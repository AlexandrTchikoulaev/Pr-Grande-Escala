#!/bin/bash
set -e

# Creates the required databases if they don't already exist.
# Runs automatically on first PostgreSQL startup via docker-entrypoint-initdb.d.

create_db_if_missing() {
    local db=$1
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
        SELECT 'CREATE DATABASE $db'
        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$db')\gexec
EOSQL
}

create_db_if_missing olist_db
create_db_if_missing airflow
create_db_if_missing hive_metastore

# Pre-create the simulated_orders table in olist_db so Debezium can
# register the CDC connector before the simulator starts.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "olist_db" <<-EOSQL
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
EOSQL

echo "[init-db] Databases and tables initialised."
