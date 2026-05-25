"""
PostgreSQL writer — inserts simulated purchases directly into the Olist DB.
This represents the structured data source: purchases arrive as DB records,
not as Kafka messages, to preserve data heterogeneity.
"""

import psycopg2
from psycopg2.extras import execute_values
from simulator import config


def build_connection():
    conn = psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        dbname=config.DB_NAME,
    )
    conn.autocommit = False
    print(f"[db_writer] Connected to PostgreSQL {config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}")
    return conn


def ensure_table(conn):
    """Creates the simulated_orders table if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS simulated_orders (
                order_id            TEXT PRIMARY KEY,
                session_id          TEXT        NOT NULL,
                customer_id         TEXT        NOT NULL,
                product_id          TEXT        NOT NULL,
                seller_id           TEXT        NOT NULL,
                category            TEXT,
                price               NUMERIC(10,2),
                freight_value       NUMERIC(10,2),
                purchase_timestamp  TIMESTAMP   NOT NULL,
                state               TEXT
            );
        """)
    conn.commit()
    print("[db_writer] Table simulated_orders ready.")


def write_purchase(conn, purchase: dict):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO simulated_orders (
                order_id, session_id, customer_id, product_id, seller_id,
                category, price, freight_value, purchase_timestamp, state
            ) VALUES (
                %(order_id)s, %(session_id)s, %(customer_id)s, %(product_id)s,
                %(seller_id)s, %(category)s, %(price)s, %(freight_value)s,
                %(purchase_timestamp)s, %(state)s
            )
            ON CONFLICT (order_id) DO NOTHING;
        """, purchase)
    conn.commit()


def noop_update_purchase(conn, purchase: dict):
    """Touch the row without changing data to generate a CDC 'u' event.
    Simulates Debezium at-least-once re-delivery: the Bronze receives a second
    record for the same order_id (op='u') that the Silver must deduplicate.
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE simulated_orders
            SET state = state
            WHERE order_id = %(order_id)s;
        """, purchase)
    conn.commit()
