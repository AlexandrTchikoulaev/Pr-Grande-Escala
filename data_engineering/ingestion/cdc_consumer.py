"""
CDC consumer — reads Debezium change events from Kafka and writes to Bronze (Parquet on MinIO).

Debezium publishes PostgreSQL WAL changes to two topics:
  debezium.public.orders       → bronze/orders/
  debezium.public.order_items  → bronze/order_items/

Each message payload contains: { before, after, op, ts_ms, source }

Bronze paths:
  bronze/orders/year=YYYY/month=MM/day=DD/hour=HH/batch_<ts>.parquet
  bronze/order_items/year=YYYY/month=MM/day=DD/hour=HH/batch_<ts>.parquet
"""

import io
import json
import time
import signal
import sys
from datetime import datetime
from pathlib import Path
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


def _parse_debezium(raw: dict) -> tuple[str | None, dict | None]:
    """Extracts table name and relevant fields from a Debezium CDC envelope.
    Returns (table_name, record) or (None, None) if the event should be ignored.
    """
    payload = raw.get("payload", raw)
    op = payload.get("op")

    if op not in ("c", "u", "r"):
        return None, None

    after = payload.get("after")
    if not after:
        return None, None

    source = payload.get("source", {})
    table = source.get("table")

    if table == "orders":
        record = {
            "order_id":           after.get("order_id"),
            "customer_id":        after.get("customer_id"),
            "session_id":         after.get("session_id"),
            "purchase_timestamp": after.get("purchase_timestamp"),
            "state":              after.get("state"),
            "cdc_ts_ms":          payload.get("ts_ms"),
        }
    elif table == "order_items":
        record = {
            "order_item_id": after.get("order_item_id"),
            "order_id":      after.get("order_id"),
            "product_id":    after.get("product_id"),
            "seller_id":     after.get("seller_id"),
            "price":         after.get("price"),
            "freight_value": after.get("freight_value"),
            "category":      after.get("category"),
            "cdc_ts_ms":     payload.get("ts_ms"),
        }
    else:
        return None, None

    return table, record


def _flush(minio, buffer: list, prefix: str, label: str):
    if not buffer:
        return
    now = datetime.now(_TZ)
    key = (
        f"{prefix}/year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"batch_{int(now.timestamp())}.parquet"
    )
    table = pa.Table.from_pylist(buffer)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    minio.put_object(Bucket=config.BRONZE_BUCKET, Key=key, Body=buf.getvalue())
    print(f"[cdc_consumer] Flushed {len(buffer)} {label} → bronze/{key}")
    record_flush(label, len(buffer), key)


def main():
    consumer = Consumer({
        "bootstrap.servers":  config.KAFKA_BOOTSTRAP,
        "group.id":           config.GROUP_CDC,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe(config.CDC_TOPICS)
    minio = _build_minio()

    orders_buf:      list[dict] = []
    order_items_buf: list[dict] = []
    last_flush = time.time()
    total_orders = 0
    total_items  = 0

    def _flush_all():
        _flush(minio, orders_buf,      "orders",      "orders")
        _flush(minio, order_items_buf, "order_items", "order_items")
        orders_buf.clear()
        order_items_buf.clear()

    def _shutdown(sig, frame):
        print("\n[cdc_consumer] Shutting down...")
        _flush_all()
        consumer.close()
        print(f"[cdc_consumer] Total — orders: {total_orders} | order_items: {total_items}")
        record_shutdown("orders", total_orders + total_items)
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    print(f"[cdc_consumer] Listening on CDC topics: {config.CDC_TOPICS}")

    _HEALTHY = Path("/tmp/healthy")
    _has_had_assignment = False
    _last_assigned = time.time()
    _last_assignment_check = 0.0

    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            pass
        elif msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[cdc_consumer] Error: {msg.error()}")
        else:
            raw = json.loads(msg.value().decode("utf-8"))
            table, record = _parse_debezium(raw)
            if record:
                now_str = datetime.now(_TZ).isoformat()
                record["ingested_at"] = now_str
                if table == "orders":
                    orders_buf.append(record)
                    total_orders += 1
                elif table == "order_items":
                    order_items_buf.append(record)
                    total_items += 1

        elapsed = time.time() - last_flush
        total_buf = len(orders_buf) + len(order_items_buf)
        if total_buf >= config.BUFFER_SIZE or (total_buf and elapsed >= config.FLUSH_INTERVAL):
            _flush_all()
            last_flush = time.time()

        now = time.time()
        if now - _last_assignment_check >= 5:
            _last_assignment_check = now
            if consumer.assignment():
                _has_had_assignment = True
                _last_assigned = now
                _HEALTHY.touch()
            elif _has_had_assignment and now - _last_assigned > 120:
                print("[cdc_consumer] No Kafka partition assignment for 2 min — exiting for restart")
                sys.exit(1)


if __name__ == "__main__":
    main()
