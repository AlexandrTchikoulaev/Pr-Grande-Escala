"""
CDC consumer — reads Debezium change events from Kafka and writes to Bronze (Parquet on MinIO).

Debezium publishes PostgreSQL WAL changes to: debezium.public.simulated_orders
Each message payload contains: { before, after, op, ts_ms, source }

Bronze path: bronze/orders/year=YYYY/month=MM/day=DD/hour=HH/batch_<ts>.parquet
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


def _parse_debezium(raw: dict) -> dict | None:
    """Extracts the relevant fields from a Debezium CDC envelope."""
    payload = raw.get("payload", raw)  # handle schema-wrapped and raw formats
    op = payload.get("op")

    # Only process inserts and updates — ignore deletes and schema changes
    if op not in ("c", "u", "r"):
        return None

    after = payload.get("after")
    if not after:
        return None

    return {
        "order_id":           after.get("order_id"),
        "session_id":         after.get("session_id"),
        "customer_id":        after.get("customer_id"),
        "product_id":         after.get("product_id"),
        "seller_id":          after.get("seller_id"),
        "category":           after.get("category"),
        "price":              after.get("price"),
        "freight_value":      after.get("freight_value"),
        "purchase_timestamp": after.get("purchase_timestamp"),
        "state":              after.get("state"),
        "cdc_operation":      op,
        "cdc_ts_ms":          payload.get("ts_ms"),
    }


def _flush(minio, buffer: list):
    if not buffer:
        return

    now = datetime.now(_TZ)
    key = (
        f"orders/year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"batch_{int(now.timestamp())}.parquet"
    )

    table = pa.Table.from_pylist(buffer)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    minio.put_object(Bucket=config.BRONZE_BUCKET, Key=key, Body=buf.getvalue())
    print(f"[cdc_consumer] Flushed {len(buffer)} orders → bronze/{key}")
    record_flush("orders", len(buffer), key)


def main():
    consumer = Consumer({
        "bootstrap.servers":  config.KAFKA_BOOTSTRAP,
        "group.id":           config.GROUP_CDC,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([config.CDC_TOPIC])
    minio = _build_minio()

    buffer: list[dict] = []
    last_flush = time.time()
    total = 0

    def _shutdown(sig, frame):
        print("\n[cdc_consumer] Shutting down...")
        _flush(minio, buffer)
        consumer.close()
        print(f"[cdc_consumer] Total orders ingested: {total}")
        record_shutdown("orders", total)
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    print(f"[cdc_consumer] Listening on CDC topic: {config.CDC_TOPIC}")

    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            pass
        elif msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[cdc_consumer] Error: {msg.error()}")
        else:
            raw = json.loads(msg.value().decode("utf-8"))
            record = _parse_debezium(raw)
            if record:
                record["ingested_at"] = datetime.now(_TZ).isoformat()
                record["source"]      = "debezium"
                buffer.append(record)
                total += 1

        elapsed = time.time() - last_flush
        if len(buffer) >= config.BUFFER_SIZE or (buffer and elapsed >= config.FLUSH_INTERVAL):
            _flush(minio, buffer)
            buffer.clear()
            last_flush = time.time()


if __name__ == "__main__":
    main()
