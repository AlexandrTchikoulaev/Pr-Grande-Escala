"""
PostgreSQL writer — inserts simulated purchases into a normalised relational schema.

Schema (5 tables):
  customers   — one row per unique customer (populated on first purchase)
  sellers     — one row per unique seller   (populated on first purchase)
  products    — one row per unique product  (populated on first purchase)
  orders      — one row per purchase (FK → customers)
  order_items — one row per line item (FK → orders, products, sellers)

Dimension tables (customers, sellers, products) grow organically: each new
purchase upserts only the customer/seller/product involved, using data already
carried in the purchase dict (sourced from the Olist CSVs loaded at startup).
"""

import psycopg2
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


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id  TEXT PRIMARY KEY,
                state        TEXT,
                city         TEXT
            );

            CREATE TABLE IF NOT EXISTS sellers (
                seller_id    TEXT PRIMARY KEY
            );

            CREATE TABLE IF NOT EXISTS products (
                product_id   TEXT PRIMARY KEY,
                category     TEXT,
                photos_qty   INTEGER
            );

            CREATE TABLE IF NOT EXISTS orders (
                order_id            TEXT PRIMARY KEY,
                customer_id         TEXT NOT NULL REFERENCES customers(customer_id),
                session_id          TEXT NOT NULL,
                purchase_timestamp  TIMESTAMP NOT NULL,
                state               TEXT
            );

            CREATE TABLE IF NOT EXISTS order_items (
                order_item_id  TEXT PRIMARY KEY,
                order_id       TEXT NOT NULL REFERENCES orders(order_id),
                product_id     TEXT NOT NULL REFERENCES products(product_id),
                seller_id      TEXT NOT NULL REFERENCES sellers(seller_id),
                price          NUMERIC(10,2),
                freight_value  NUMERIC(10,2),
                category       TEXT
            );
        """)
    conn.commit()
    print("[db_writer] Relational schema ready (customers, sellers, products, orders, order_items).")


def write_purchase(conn, purchase: dict):
    with conn.cursor() as cur:
        # Upsert dimension rows with data carried from the Olist CSV pools
        cur.execute("""
            INSERT INTO customers (customer_id, state, city)
            VALUES (%(customer_id)s, %(state)s, %(city)s)
            ON CONFLICT (customer_id) DO NOTHING
        """, purchase)

        cur.execute("""
            INSERT INTO sellers (seller_id)
            VALUES (%(seller_id)s)
            ON CONFLICT (seller_id) DO NOTHING
        """, purchase)

        cur.execute("""
            INSERT INTO products (product_id, category, photos_qty)
            VALUES (%(product_id)s, %(category)s, %(photos_qty)s)
            ON CONFLICT (product_id) DO NOTHING
        """, purchase)

        cur.execute("""
            INSERT INTO orders (order_id, customer_id, session_id, purchase_timestamp, state)
            VALUES (%(order_id)s, %(customer_id)s, %(session_id)s, %(purchase_timestamp)s, %(state)s)
            ON CONFLICT (order_id) DO NOTHING
        """, purchase)

        cur.execute("""
            INSERT INTO order_items (order_item_id, order_id, product_id, seller_id, price, freight_value, category)
            VALUES (%(order_item_id)s, %(order_id)s, %(product_id)s, %(seller_id)s,
                    %(price)s, %(freight_value)s, %(category)s)
            ON CONFLICT (order_item_id) DO NOTHING
        """, purchase)

    conn.commit()


def noop_update_purchase(conn, purchase: dict):
    """Touch the orders row without changing data to generate a CDC 'u' event.
    Simulates Debezium at-least-once re-delivery: the Bronze receives a second
    record for the same order_id (op='u') that the Silver must deduplicate.
    """
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE orders SET state = state WHERE order_id = %(order_id)s
        """, purchase)
    conn.commit()
