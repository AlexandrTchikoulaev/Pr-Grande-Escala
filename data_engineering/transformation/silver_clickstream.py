"""
Bronze → Silver: clickstream events.

Reads Parquet from bronze/clickstream/ (Spark file streaming).
Cleans, types, and extracts fields from the JSON properties blob.
Writes to Silver Iceberg: lake.silver.clickstream
"""

import os
import signal
import time

import boto3
from botocore.exceptions import ClientError

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
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

BRONZE_PATH = f"s3a://{config.BRONZE_BUCKET}/clickstream/"

BRONZE_SCHEMA = StructType([
    StructField("event_id",    StringType()),
    StructField("session_id",  StringType()),
    StructField("user_id",     StringType()),
    StructField("event_type",  StringType()),
    StructField("timestamp",   StringType()),
    StructField("device",      StringType()),
    StructField("properties",  StringType()),  # JSON blob
    StructField("ingested_at", StringType()),
])

PROPERTIES_SCHEMA = StructType([
    StructField("product_id", StringType()),
    StructField("category",   StringType()),
    StructField("price",      DoubleType()),
    StructField("state",      StringType()),
    StructField("query",      StringType()),
    StructField("city",       StringType()),
    StructField("reason",     StringType()),
])


def run(spark=None):
    _kill_orphan_spark_jvms("silver_clickstream")

    if not _has_bronze_data("clickstream/"):
        print("[silver_clickstream] No data in bronze/clickstream/ yet — skipping")
        return 0

    owned_spark = spark is None
    if spark is None:
        spark = get_spark("silver_clickstream")

    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS lake.silver")

        spark.sql("""
            CREATE TABLE IF NOT EXISTS lake.silver.clickstream (
                event_id    STRING,
                session_id  STRING,
                user_id     STRING,
                event_type  STRING,
                event_ts    TIMESTAMP,
                device      STRING,
                category    STRING,
                product_id  STRING,
                price       DOUBLE,
                location    STRING,
                ingested_at TIMESTAMP
            )
            USING iceberg
        """)

        raw = (
            spark.readStream
            .schema(BRONZE_SCHEMA)
            .option("basePath", BRONZE_PATH)
            .option("maxFilesPerTrigger", 50)
            .parquet(BRONZE_PATH)
        )

        props = F.from_json(F.col("properties"), PROPERTIES_SCHEMA)

        _known_devices = ["mobile", "desktop", "tablet"]

        silver = (
            raw
            .filter(
                F.col("event_id").isNotNull() &
                F.col("session_id").isNotNull() &
                (F.length(F.col("session_id")) > 0) &
                F.col("event_type").isNotNull() &
                (F.length(F.col("event_type")) > 0)
            )
            .withColumn("props",      props)
            .withColumn("event_ts",   F.to_timestamp("timestamp"))
            .withColumn("ingest_ts",  F.to_timestamp("ingested_at"))
            # Normalise device: unrecognised values (bots, crawlers, typos) → "unknown"
            .withColumn("device_norm",
                F.when(F.col("device").isin(_known_devices), F.col("device"))
                 .otherwise(F.lit("unknown"))
            )
            .select(
                F.col("event_id"),
                F.col("session_id"),
                F.col("user_id"),
                F.col("event_type"),
                F.col("event_ts"),
                F.col("device_norm").alias("device"),
                F.col("props.category").alias("category"),
                F.col("props.product_id").alias("product_id"),
                F.col("props.price").alias("price"),
                F.coalesce(F.col("props.state"), F.col("props.city")).alias("location"),
                F.col("ingest_ts").alias("ingested_at"),
            )
            .filter(F.col("event_ts").isNotNull())
        )

        _repair_checkpoint(config.CHECKPOINT_CLICKSTREAM, "silver_clickstream")

        query = (
            silver.writeStream
            .format("iceberg")
            .outputMode("append")
            .option("path", "lake.silver.clickstream")
            .option("checkpointLocation", config.CHECKPOINT_CLICKSTREAM)
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()
        count = (query.lastProgress or {}).get("numOutputRows", 0)
        print(f"[silver_clickstream] Written {count:,} rows to lake.silver.clickstream")
        return count
    finally:
        if owned_spark:
            spark.stop()


if __name__ == "__main__":
    run()
