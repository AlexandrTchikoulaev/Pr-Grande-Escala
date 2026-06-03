"""
Bronze → Silver: reviews (from MinIO .txt files).

Reads Parquet from bronze/reviews/.
Validates rating, calculates text_length.
Keeps raw message text — downstream NLP reads from silver.reviews.
Writes to Silver Iceberg: lake.silver.reviews
"""

import os
import signal
import time

import boto3
from botocore.exceptions import ClientError

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
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

BRONZE_PATH = f"s3a://{config.BRONZE_BUCKET}/reviews/"

BRONZE_SCHEMA = StructType([
    StructField("file_path",   StringType()),
    StructField("review_id",   StringType()),
    StructField("order_id",    StringType()),
    StructField("raw_content", StringType()),
    StructField("ingested_at", StringType()),
])


def run(spark=None):
    _kill_orphan_spark_jvms("silver_reviews")

    if not _has_bronze_data("reviews/"):
        print("[silver_reviews] No data in bronze/reviews/ yet — skipping")
        return 0

    owned_spark = spark is None
    if spark is None:
        spark = get_spark("silver_reviews")

    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS lake.silver")

        spark.sql("""
            CREATE TABLE IF NOT EXISTS lake.silver.reviews (
                review_id   STRING,
                order_id    STRING,
                rating      INT,
                message     STRING,
                text_length INT,
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

        # Rating appears in one of two natural-language forms generated by the simulator:
        #   Form 1: "Dou X estrelas em 5."
        #   Form 2: "Classifico este produto com X/5."
        rating_p1 = F.regexp_extract(F.col("raw_content"), r"[Dd]ou (\d) estrelas? em 5", 1)
        rating_p2 = F.regexp_extract(F.col("raw_content"), r"[Cc]lassifico este produto com (\d)/5", 1)
        rating_raw = F.coalesce(
            F.when(rating_p1 != "", rating_p1),
            F.when(rating_p2 != "", rating_p2),
        )

        silver = (
            raw
            .filter(
                F.col("review_id").isNotNull() & (F.length(F.col("review_id")) > 0) &
                F.col("order_id").isNotNull()  & (F.length(F.col("order_id"))  > 0)
            )
            .withColumn("rating_int",  rating_raw.cast(IntegerType()))
            .withColumn("message",     F.trim(F.col("raw_content")))
            .withColumn("text_length", F.size(F.split(F.col("message"), r"\s+")))
            .withColumn("ingest_ts",   F.to_timestamp("ingested_at"))
            .filter(
                F.col("rating_int").isNotNull() &
                F.col("rating_int").between(1, 5) &
                F.col("message").isNotNull() &
                (F.length(F.trim(F.col("message"))) > 0)
            )
            .select(
                F.col("review_id"),
                F.col("order_id"),
                F.col("rating_int").alias("rating"),
                F.col("message"),
                F.col("text_length"),
                F.col("ingest_ts").alias("ingested_at"),
            )
        )

        _repair_checkpoint(config.CHECKPOINT_REVIEWS, "silver_reviews")

        # Use foreachBatch to deduplicate by review_id via MERGE INTO.
        # Within each batch: keep the latest record per review_id.
        # Across batches: MERGE ensures a review_id already in Silver is never duplicated
        # (handles duplicate file submissions from the source).
        def _write_batch(batch_df, batch_id):
            if batch_df.rdd.isEmpty():
                return
            w = Window.partitionBy("review_id").orderBy(F.col("ingested_at").desc())
            deduped = (
                batch_df
                .withColumn("_rn", F.row_number().over(w))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )
            deduped.createOrReplaceGlobalTempView("_silver_reviews_batch")
            spark.sql("""
                MERGE INTO lake.silver.reviews t
                USING global_temp._silver_reviews_batch s ON t.review_id = s.review_id
                WHEN NOT MATCHED THEN INSERT *
            """)

        query = (
            silver.writeStream
            .foreachBatch(_write_batch)
            .option("checkpointLocation", config.CHECKPOINT_REVIEWS)
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()
        count = (query.lastProgress or {}).get("numInputRows", 0)
        print(f"[silver_reviews] Written {count:,} rows to lake.silver.reviews")
        return count
    finally:
        if owned_spark:
            spark.stop()


if __name__ == "__main__":
    run()
