"""
Kafka producer — publishes clickstream events to the clickstream_events topic.
order_placed events are intentionally excluded (those go to PostgreSQL).
"""

import json
from confluent_kafka import Producer
from simulator import config

def build_producer() -> Producer:
    producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
    print(f"[producer] Connected to Kafka at {config.KAFKA_BOOTSTRAP}")
    return producer


def _delivery_report(err, msg):
    if err:
        print(f"[producer] Delivery failed: {err}")


def publish_events(producer: Producer, events: list[dict]) -> int:
    count = 0
    for event in events:
        producer.produce(
            topic=config.CLICKSTREAM_TOPIC,
            key=event["session_id"],
            value=json.dumps(event),
            callback=_delivery_report,
        )
        count += 1
    producer.poll(0)
    return count


def flush(producer: Producer):
    producer.flush()
