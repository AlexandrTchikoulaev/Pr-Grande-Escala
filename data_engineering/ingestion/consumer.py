"""
Kafka consumer — reads clickstream_events and writes to Bronze (Parquet on MinIO).

Bronze path: bronze/clickstream/year=YYYY/month=MM/day=DD/hour=HH/batch_<ts>.parquet
"""

import io
import json
import time
import signal
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

_TZ = ZoneInfo("Europe/Lisbon")

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError

from ingestion import config
from reporter import record_flush, record_shutdown


def _build_minio():
    client = boto3.client(
        "s3",
        endpoint_url=f"http://{config.MINIO_ENDPOINT}",
        aws_access_key_id=config.MINIO_ACCESS,
        aws_secret_access_key=config.MINIO_SECRET,
    )
    try:
        client.head_bucket(Bucket=config.BRONZE_BUCKET)
    except Exception:
        client.create_bucket(Bucket=config.BRONZE_BUCKET)
    return client


def _flush(minio, buffer: list):
    if not buffer:
        return

    now = datetime.now(_TZ)
    key = (
        f"clickstream/year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"batch_{int(now.timestamp())}.parquet"
    )

    table = pa.Table.from_pylist(buffer)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    minio.put_object(Bucket=config.BRONZE_BUCKET, Key=key, Body=buf.getvalue())
    print(f"[consumer] Flushed {len(buffer)} events → bronze/{key}")
    record_flush("clickstream", len(buffer), key)


def main():
    consumer = Consumer({
        "bootstrap.servers":  config.KAFKA_BOOTSTRAP,
        "group.id":           config.GROUP_CLICKSTREAM,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([config.CLICKSTREAM_TOPIC])
    minio = _build_minio()

    buffer: list[dict] = []
    last_flush = time.time()
    total = 0

    def _shutdown(sig, frame):
        print("\n[consumer] Shutting down...")
        _flush(minio, buffer)
        consumer.close()
        print(f"[consumer] Total events ingested: {total}")
        record_shutdown("clickstream", total)
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    print(f"[consumer] Listening on topic: {config.CLICKSTREAM_TOPIC}")

    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            pass
        elif msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[consumer] Error: {msg.error()}")
        else:
            record = json.loads(msg.value().decode("utf-8"))
            record["ingested_at"] = datetime.now(_TZ).isoformat()
            record["source"]      = "kafka"
            # flatten properties to string — Bronze keeps data raw
            if isinstance(record.get("properties"), dict):
                record["properties"] = json.dumps(record["properties"])
            buffer.append(record)
            total += 1

        elapsed = time.time() - last_flush
        if len(buffer) >= config.BUFFER_SIZE or (buffer and elapsed >= config.FLUSH_INTERVAL):
            _flush(minio, buffer)
            buffer.clear()
            last_flush = time.time()


if __name__ == "__main__":
    main()
