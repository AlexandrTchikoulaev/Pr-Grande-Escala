"""
File watcher — polls MinIO raw-reviews/ for new .txt files and writes to Bronze (Parquet).

Tracks processed files in a local state file to avoid re-ingesting.
Bronze path: bronze/reviews/year=YYYY/month=MM/day=DD/batch_<ts>.parquet
"""

import io
import json
import time
import signal
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

_TZ = ZoneInfo("Europe/Lisbon")
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion import config
from reporter import record_flush, record_shutdown


def _build_minio():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{config.MINIO_ENDPOINT}",
        aws_access_key_id=config.MINIO_ACCESS,
        aws_secret_access_key=config.MINIO_SECRET,
    )


def _load_state() -> set:
    path = Path(config.WATCHER_STATE_FILE)
    if path.exists():
        return set(json.loads(path.read_text()))
    return set()


def _save_state(processed: set):
    Path(config.WATCHER_STATE_FILE).parent.mkdir(parents=True, exist_ok=True)
    Path(config.WATCHER_STATE_FILE).write_text(json.dumps(list(processed)))


def _parse_review_file(content: str, file_path: str) -> dict:
    """
    Extracts review_id and order_id from the filename (the only structured metadata).
    The file content is unstructured free text — no header parsing.

    Filename convention: {review_id}_{order_id}.txt  (or _dup.txt for duplicates)
    """
    filename = file_path.rsplit("/", 1)[-1]
    stem = filename.removesuffix("_dup.txt").removesuffix(".txt")
    review_id, _, order_id = stem.partition("_")
    return {
        "file_path":   file_path,
        "review_id":   review_id,
        "order_id":    order_id,
        "raw_content": content,
    }


def _flush(minio, buffer: list):
    if not buffer:
        return

    now = datetime.now(_TZ)
    key = (
        f"reviews/year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"batch_{int(now.timestamp())}.parquet"
    )

    table = pa.Table.from_pylist(buffer)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    minio.put_object(Bucket=config.BRONZE_BUCKET, Key=key, Body=buf.getvalue())
    print(f"[file_watcher] Flushed {len(buffer)} reviews → bronze/{key}")
    record_flush("reviews", len(buffer), key)


def main():
    minio = _build_minio()
    processed = _load_state()
    total = 0

    def _shutdown(sig, frame):
        print("\n[file_watcher] Shutting down...")
        _save_state(processed)
        print(f"[file_watcher] Total reviews ingested: {total}")
        record_shutdown("reviews", total)
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    print(f"[file_watcher] Watching bucket: {config.REVIEWS_BUCKET} every {config.WATCHER_INTERVAL}s")

    while True:
        try:
            paginator = minio.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=config.REVIEWS_BUCKET)

            batch = []
            for page in pages:
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if not key.endswith(".txt") or key in processed:
                        continue

                    response = minio.get_object(Bucket=config.REVIEWS_BUCKET, Key=key)
                    content  = response["Body"].read().decode("utf-8")
                    record   = _parse_review_file(content, key)
                    record["ingested_at"] = datetime.now(_TZ).isoformat()
                    batch.append(record)
                    processed.add(key)
                    total += 1

            if batch:
                _flush(minio, batch)
                _save_state(processed)

        except Exception as e:
            print(f"[file_watcher] Error: {e}")

        time.sleep(config.WATCHER_INTERVAL)


if __name__ == "__main__":
    main()
