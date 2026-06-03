# data_engineering — Output Contracts

Silver Iceberg tables delivered to `analytical_engineering`.
Catalog: `lake` (Hive Metastore). Warehouse: `s3a://silver/`.

## Frequência de actualização

A frequência de actualização das tabelas Silver é **horária**, determinada pelo requisito de SLA da `analytical_engineering` e implementada pela equipa `infrastructure` através da DAG `trendmart_gold_pipeline` (`infrastructure/dags/dag_trendmart.py`, `schedule_interval="0 * * * *"`).

A data_engineering é responsável pelo código de transformação (jobs Spark Silver); a infrastructure é responsável pelo agendamento e execução desse código.

---

## lake.silver.clickstream

| Column       | Type      | Description                              |
|--------------|-----------|------------------------------------------|
| event_id     | STRING    | Unique event identifier                  |
| session_id   | STRING    | User session identifier                  |
| user_id      | STRING    | Customer ID                              |
| event_type   | STRING    | e.g. product_view, add_to_cart           |
| event_ts     | TIMESTAMP | Event timestamp (UTC)                    |
| device       | STRING    | mobile / desktop / tablet                |
| category     | STRING    | Product category (English)               |
| product_id   | STRING    | Product identifier (nullable)            |
| price        | DOUBLE    | Product price (nullable)                 |
| location     | STRING    | State or city of the user                |
| ingested_at  | TIMESTAMP | When this record was ingested            |

---

## lake.silver.orders

| Column        | Type      | Description                             |
|---------------|-----------|-----------------------------------------|
| order_id      | STRING    | Unique order identifier                 |
| session_id    | STRING    | Session that generated the order        |
| customer_id   | STRING    | Customer identifier                     |
| product_id    | STRING    | Product identifier                      |
| seller_id     | STRING    | Seller identifier                       |
| category      | STRING    | Product category (English)              |
| price         | DOUBLE    | Product price                           |
| freight_value | DOUBLE    | Freight cost                            |
| total_value   | DOUBLE    | price + freight_value                   |
| purchase_ts   | TIMESTAMP | Purchase timestamp (UTC)                |
| state         | STRING    | Customer state (BR); NULL if invalid    |
| region        | STRING    | Brazilian region; "Desconhecido" if state is NULL |
| ingested_at   | TIMESTAMP | When this record was ingested           |

---

## lake.silver.reviews

| Column      | Type      | Description                              |
|-------------|-----------|------------------------------------------|
| review_id   | STRING    | Unique review identifier                 |
| order_id    | STRING    | Associated order                         |
| customer_id | STRING    | Customer who wrote the review            |
| rating      | INTEGER   | Score 1–5                                |
| title       | STRING    | Review title (may be empty)              |
| message     | STRING    | Free text review — unstructured          |
| text_length | INTEGER   | Word count of the message                |
| ingested_at | TIMESTAMP | When this record was ingested            |
