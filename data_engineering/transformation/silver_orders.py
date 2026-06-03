"""
Bronze → Silver: orders (from Debezium CDC).

Reads Parquet from bronze/orders/.
Cleans, validates, enriches with region from state.
Writes to Silver Iceberg: lake.silver.orders
"""

import os
import signal
import time

import boto3
from botocore.exceptions import ClientError

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, TimestampType
)

from transformation.spark_session import get_spark
from transformation import config


def _kill_orphan_spark_jvms(label: str) -> None:
    """Kill any PySpark JVM processes left alive by a previously SIGKILL'd task.

    When Airflow sends SIGKILL to the Python process, the child JVM becomes an
    orphan (adopted by init) and keeps running — locking the streaming checkpoint.
    Reads /proc directly to avoid dependency on external binaries like pgrep.
    """
    my_pid = os.getpid()
    killed = []
    try:
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            pid = int(entry)
            if pid == my_pid:
                continue
            try:
                with open(f"/proc/{pid}/cmdline", "rb") as fh:
                    cmdline = fh.read().decode("utf-8", errors="replace")
                if "py4j" in cmdline:
                    os.kill(pid, signal.SIGKILL)
                    killed.append(pid)
            except (PermissionError, ProcessLookupError, FileNotFoundError):
                pass
    except (FileNotFoundError, PermissionError):
        pass  # /proc not available or not readable

    if killed:
        print(f"[{label}] Killed orphan Spark JVM(s): {killed}")
        time.sleep(3)


def _repair_checkpoint(checkpoint_path: str, label: str) -> None:
    """Delete uncommitted batch files and stale source-log entries.

    Orphaned batches (in offsets/ but not commits/) are deleted.  Source-log
    entries are only deleted when they exceed the logOffset referenced by the
    latest committed offset file — because a committed batch may reference a
    source-log entry whose ID is higher than the batch ID itself (the file
    source writes multiple log entries per batch when availableNow=True drains
    a large backlog).  Deleting a source-log entry still referenced by a
    committed offset causes "batches (N, M) don't exist" on the next run.
    """
    import json as _json

    path_no_proto = checkpoint_path.removeprefix("s3a://")
    bucket, _, prefix = path_no_proto.partition("/")

    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{config.MINIO_ENDPOINT}",
        aws_access_key_id=config.MINIO_ACCESS,
        aws_secret_access_key=config.MINIO_SECRET,
    )

    def _batch_ids(subdir: str) -> set:
        ids: set = set()
        try:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/{subdir}/"):
                for obj in page.get("Contents", []):
                    name = obj["Key"].rsplit("/", 1)[-1]
                    if name.isdigit():
                        ids.add(int(name))
        except ClientError:
            pass
        return ids

    def _log_offset_of_committed_batch(batch_id: int) -> int:
        """Parse the logOffset from the offset file of a committed batch.

        The offset file format is three newline-separated sections:
          v1
          {batchMetadata JSON}
          {sourceOffset JSON}   ← contains {"logOffset": N}

        Returns the logOffset value, or batch_id if parsing fails.
        """
        try:
            obj = s3.get_object(Bucket=bucket, Key=f"{prefix}/offsets/{batch_id}")
            content = obj["Body"].read().decode()
            for line in content.splitlines():
                line = line.strip()
                if '"logOffset"' in line:
                    return int(_json.loads(line).get("logOffset", batch_id))
        except (ClientError, ValueError, KeyError):
            pass
        return batch_id

    committed = _batch_ids("commits")
    orphaned = _batch_ids("offsets") - committed
    max_committed_id = max(committed) if committed else -1

    # The source-log entry referenced by the latest committed offset is the
    # true high-water mark: never delete source-log entries at or below it.
    safe_log_offset = _log_offset_of_committed_batch(max_committed_id) if max_committed_id >= 0 else -1

    stale_source_ids = {bid for bid in _batch_ids("sources/0") if bid > safe_log_offset}

    all_to_clean = orphaned | stale_source_ids
    if not all_to_clean:
        return

    # Scan the entire checkpoint tree: catches offsets/, sources/0/, and .tmp leftovers
    to_delete = []
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                name = key.rsplit("/", 1)[-1].lstrip(".")  # strip leading dot from .N.tmp
                base = name.removesuffix(".tmp")
                if base.isdigit() and int(base) in all_to_clean:
                    to_delete.append({"Key": key})
    except ClientError as exc:
        print(f"[{label}] Warning: checkpoint scan error: {exc}")

    if not to_delete:
        return

    for i in range(0, len(to_delete), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": to_delete[i:i + 1000]})
    print(f"[{label}] Repaired checkpoint: deleted {len(to_delete)} file(s) for batches {sorted(all_to_clean)}")


def _has_bronze_data(prefix: str) -> bool:
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{config.MINIO_ENDPOINT}",
        aws_access_key_id=config.MINIO_ACCESS,
        aws_secret_access_key=config.MINIO_SECRET,
    )
    try:
        resp = s3.list_objects_v2(Bucket=config.BRONZE_BUCKET, Prefix=prefix, MaxKeys=1)
        return resp.get("KeyCount", 0) > 0
    except ClientError:
        return False

BRONZE_ORDERS_PATH = f"s3a://{config.BRONZE_BUCKET}/orders/"
BRONZE_ITEMS_PATH  = f"s3a://{config.BRONZE_BUCKET}/order_items/"

# bronze/orders/ — CDC from the orders table
BRONZE_SCHEMA_ORDERS = StructType([
    StructField("order_id",           StringType()),
    StructField("customer_id",        StringType()),
    StructField("session_id",         StringType()),
    StructField("purchase_timestamp", LongType()),   # Debezium TIMESTAMP → microseconds since epoch
    StructField("state",              StringType()),
    StructField("cdc_ts_ms",          LongType()),
    StructField("ingested_at",        StringType()),
])

# bronze/order_items/ — CDC from the order_items table
BRONZE_SCHEMA_ITEMS = StructType([
    StructField("order_item_id",  StringType()),
    StructField("order_id",       StringType()),
    StructField("product_id",     StringType()),
    StructField("seller_id",      StringType()),
    StructField("price",          StringType()),   # Debezium NUMERIC → string; cast downstream
    StructField("freight_value",  StringType()),   # same
    StructField("category",       StringType()),
    StructField("cdc_ts_ms",      LongType()),
    StructField("ingested_at",    StringType()),
])


_VALID_BR_STATES = {
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA",
    "MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN",
    "RS","RO","RR","SC","SP","SE","TO",
}


def _build_region_map(spark):
    rows = [{"state": k, "region": v} for k, v in config.REGION_MAP.items()]
    return spark.createDataFrame(rows)


def run(spark=None):
    _kill_orphan_spark_jvms("silver_orders")

    if not _has_bronze_data("order_items/"):
        print("[silver_orders] No data in bronze/order_items/ yet — skipping")
        return 0

    owned_spark = spark is None
    if spark is None:
        spark = get_spark("silver_orders")

    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS lake.silver")

        spark.sql("""
            CREATE TABLE IF NOT EXISTS lake.silver.orders (
                order_id      STRING,
                session_id    STRING,
                customer_id   STRING,
                product_id    STRING,
                seller_id     STRING,
                category      STRING,
                price         DOUBLE,
                freight_value DOUBLE,
                total_value   DOUBLE,
                purchase_ts   TIMESTAMP,
                state         STRING,
                region        STRING,
                ingested_at   TIMESTAMP
            )
            USING iceberg
        """)

        region_df = _build_region_map(spark)

        # Primary stream: order_items (carries price, freight, product, seller, category)
        items_stream = (
            spark.readStream
            .schema(BRONZE_SCHEMA_ITEMS)
            .option("basePath", BRONZE_ITEMS_PATH)
            .option("maxFilesPerTrigger", 50)
            .parquet(BRONZE_ITEMS_PATH)
        )

        valid_states_list = list(_VALID_BR_STATES)

        _repair_checkpoint(config.CHECKPOINT_ORDERS, "silver_orders")

        # foreachBatch: join order_items batch with all accumulated orders (batch read),
        # apply cleaning, deduplicate, and MERGE into silver.
        def _write_batch(items_df, batch_id):
            if items_df.rdd.isEmpty():
                return

            # Batch read of all orders accumulated in Bronze so far
            orders_df = (
                spark.read
                .schema(BRONZE_SCHEMA_ORDERS)
                .option("basePath", BRONZE_ORDERS_PATH)
                .parquet(BRONZE_ORDERS_PATH)
                .select("order_id", "customer_id", "session_id", "purchase_timestamp", "state", "cdc_ts_ms")
            )

            joined = items_df.drop("cdc_ts_ms").join(orders_df, on="order_id", how="inner")

            silver = (
                joined
                .withColumn("price_d",         F.col("price").cast(DoubleType()))
                .withColumn("freight_value_d", F.col("freight_value").cast(DoubleType()))
                .filter(
                    F.col("order_id").isNotNull() &
                    F.col("customer_id").isNotNull() &
                    F.col("price_d").isNotNull() & (F.col("price_d") > 0) &
                    F.col("freight_value_d").isNotNull() & (F.col("freight_value_d") >= 0)
                )
                .withColumn("purchase_ts", (F.col("purchase_timestamp") / 1_000_000).cast(TimestampType()))
                .withColumn("ingest_ts",   F.to_timestamp("ingested_at"))
                .withColumn("total_value", F.round(F.col("price_d") + F.col("freight_value_d"), 2))
                .withColumn("state_norm",
                    F.when(
                        F.upper(F.trim(F.col("state"))).isin(valid_states_list),
                        F.upper(F.trim(F.col("state")))
                    ).otherwise(F.lit(None))
                )
                .withColumn("category_norm", F.trim(F.col("category")))
                .join(region_df, region_df["state"] == F.col("state_norm"), how="left")
                .filter(F.col("purchase_ts").isNotNull())
                .select(
                    F.col("order_id"),
                    F.col("session_id"),
                    F.col("customer_id"),
                    F.col("product_id"),
                    F.col("seller_id"),
                    F.col("category_norm").alias("category"),
                    F.col("price_d").alias("price"),
                    F.col("freight_value_d").alias("freight_value"),
                    F.col("total_value"),
                    F.col("purchase_ts"),
                    F.col("state_norm").alias("state"),
                    F.coalesce(F.col("region"), F.lit("Desconhecido")).alias("region"),
                    F.col("ingest_ts").alias("ingested_at"),
                    F.col("cdc_ts_ms"),
                )
            )

            # Within-batch dedup: keep latest CDC event per order_id
            w = Window.partitionBy("order_id").orderBy(F.col("cdc_ts_ms").desc())
            deduped = (
                silver
                .withColumn("_rn", F.row_number().over(w))
                .filter(F.col("_rn") == 1)
                .drop("_rn", "cdc_ts_ms")
            )

            deduped.createOrReplaceGlobalTempView("_silver_orders_batch")
            spark.sql("""
                MERGE INTO lake.silver.orders t
                USING global_temp._silver_orders_batch s ON t.order_id = s.order_id
                WHEN NOT MATCHED THEN INSERT *
            """)

        query = (
            items_stream.writeStream
            .foreachBatch(_write_batch)
            .option("checkpointLocation", config.CHECKPOINT_ORDERS)
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()
        count = (query.lastProgress or {}).get("numInputRows", 0)
        print(f"[silver_orders] Written {count:,} rows to lake.silver.orders")
        return count
    finally:
        if owned_spark:
            spark.stop()


if __name__ == "__main__":
    run()
