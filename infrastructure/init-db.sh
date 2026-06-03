#!/bin/bash
set -e

# Creates the required databases if they don't already exist.
# Runs automatically on first PostgreSQL startup via docker-entrypoint-initdb.d.

create_db_if_missing() {
    local db=$1
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
        SELECT 'CREATE DATABASE "$db"'
        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$db')\gexec
EOSQL
}

create_db_if_missing Amazon_Sales
create_db_if_missing airflow
create_db_if_missing hive_metastore

# Pre-create the relational schema in Amazon_Sales so Debezium can
# register the CDC connector before the simulator starts.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "Amazon_Sales" <<-EOSQL
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
EOSQL

echo "[init-db] Databases and tables initialised."
