"""
data_sources — entry point.

Runs the e-commerce simulator continuously:
  - Clickstream events  → Kafka (topic: clickstream_events)
  - Purchases           → PostgreSQL (tables: customers, sellers, products, orders, order_items)
  - Reviews             → MinIO (bucket: raw-reviews, as .txt files)
"""

import time
import signal
import sys

from simulator import config
from simulator.loader import load_reference_data
from reporter import record_snapshot, record_shutdown, SNAPSHOT_EVERY
from simulator.session import simulate_session
from simulator import producer as kafka_producer
from simulator import db_writer
from simulator import review_writer
from simulator import noise


def main():
    print("=" * 60)
    print("  TrendMart — Data Sources Simulator")
    print(f"  Rate: {config.SESSIONS_PER_SECOND} sessions/sec")
    print("=" * 60)

    # Load reference data
    data = load_reference_data()

    # Build connections
    producer = kafka_producer.build_producer()
    db_conn  = db_writer.build_connection()
    db_writer.ensure_tables(db_conn)
    minio    = review_writer.build_client()

    # Counters
    total_sessions  = 0
    total_events    = 0
    total_purchases = 0
    total_reviews   = 0

    interval = 1.0 / config.SESSIONS_PER_SECOND

    def _shutdown(sig, frame):
        print("\n[main] Shutting down...")
        kafka_producer.flush(producer)
        db_conn.close()
        print(f"[main] Sessions: {total_sessions} | Events: {total_events} | Purchases: {total_purchases} | Reviews: {total_reviews}")
        record_shutdown(total_sessions, total_events, total_purchases, total_reviews)
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    print("[main] Simulator running. Press Ctrl+C to stop.\n")

    while True:
        events, purchase, review = simulate_session(data)

        # Clickstream → Kafka (with noise)
        noise.dirty_clickstream_events(events)
        sent = kafka_producer.publish_events(producer, events)
        total_events += sent

        # Purchase → PostgreSQL (with noise)
        if purchase:
            dirty_p, cdc_dup = noise.dirty_purchase(purchase)
            db_writer.write_purchase(db_conn, dirty_p)
            if cdc_dup:
                db_writer.noop_update_purchase(db_conn, dirty_p)
            total_purchases += 1

        # Review → MinIO (with noise; duplicate simulates double submission)
        if review:
            dirty_r, review_dup = noise.dirty_review(review)
            review_writer.write_review(minio, dirty_r)
            if review_dup:
                review_writer.write_review_duplicate(minio, dirty_r)
            total_reviews += 1

        total_sessions += 1

        if total_sessions % 10 == 0:
            print(
                f"[main] sessions={total_sessions} | "
                f"events={total_events} | "
                f"purchases={total_purchases} | "
                f"reviews={total_reviews}"
            )

        if total_sessions % SNAPSHOT_EVERY == 0:
            record_snapshot(total_sessions, total_events, total_purchases, total_reviews)

        time.sleep(interval)


if __name__ == "__main__":
    main()
