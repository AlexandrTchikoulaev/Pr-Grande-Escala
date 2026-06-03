# Pipeline TrendMart — Fluxo de Dados Completo

## Visão geral

```
DATA SOURCES  ──►  DATA ENGINEERING  ──►  ANALYTICAL ENGINEERING  ──►  CONSUMO
(Simulador)        (Ingestion)             (Silver+Gold+Views Airflow)   (Trino/BI)
                                                    │
                                                    ▼
                                           MACHINE LEARNING
                                           (Modelos diários → ML Gold Tables)
```

---

## Fluxo completo

```
╔════════════════════════════════════════════════════════════════════════════════╗
║  OLIST DATASET (CSVs)                                                          ║
║  products, customers, order_items, reviews, category_translations              ║
╚══════════════════════════════════╦═════════════════════════════════════════════╝
                                   ║ loader.py (leitura única na inicialização)
                                   ▼
╔═════════════════════════════════════════════════════════════════════════════════╗
║  DATA SOURCES — simulator/session.py  (0.5 sessões/segundo)                     ║
║                                                                                 ║
║  session_start → search/browse → product_view → add_to_cart → checkout          ║
║                                                                                 ║
║  Cada sessão gera até 3 tipos de output:                                        ║
╚══════╦════════════════════════════╦═══════════════════════════╦═════════════════╝
       ║                            ║                           ║
       ║ noise.py aplica ruído controlado antes de cada escrita (reviews > orders > clickstream)
       ║ producer.py                ║ db_writer.py              ║ review_writer.py
       ║ (todos os eventos          ║ (só order_placed,         ║ (40% das compras,
       ║  exceto order_placed)      ║  estruturado)             ║  com delay 30-300s)
       ▼                            ▼                           ▼
┌─────────────────┐      ┌───────────────── ─────┐    ┌─────────────────────────┐
│      KAFKA      │      │      POSTGRESQL       │    │         MINIO           │
│                 │      │                       │    │                         │
│ clickstream_    │      │  Amazon_Sales         │    │  raw-reviews/           │
│ events          │      │  .simulated_orders    │    │  YYYY-MM-DD/            │
│                 │      │                       │    │  {order_id}.txt         │
│ JSON por evento │      │  10 colunas           │    │                         │
│ event_id        │      │  order_id (PK)        │    │  Header estruturado:    │
│ session_id      │      │  session_id           │    │  REVIEW_ID, ORDER_ID    │
│ user_id         │      │  customer_id          │    │  CUSTOMER_ID, RATING    │
│ event_type      │      │  product_id           │    │  TIMESTAMP              │
│ timestamp       │      │  seller_id            │    │  ---                    │
│ device          │      │  category             │    │  TITLE (opcional)       │
│ properties{}    │      │  price                │    │                         │
│                 │      │  freight_value        │    │  Texto livre (NLP)      │
│ 11 event_types  │      │  purchase_timestamp   │    │                         │
│                 │      │  state                │    │                         │
└────────┬────────┘      └──────────┬────────────┘    └────────────┬────────────┘
         ║                          ║                              ║
         ║                          ║ WAL (Write-Ahead Log)        ║
         ║                          ▼                              ║
         ║               ┌──────────────────────┐                  ║
         ║               │  DEBEZIUM CDC        │                  ║
         ║               │  (Kafka Connect)     │                  ║
         ║               │                      │                  ║
         ║               │  Lê WAL do Postgres  │                  ║
         ║               │  Publica mudanças    │                  ║
         ║               │  no Kafka            │                  ║
         ║               └──────────┬───────────┘                  ║
         ║                          ║                              ║
         ║                          ▼                              ║
         ║               ┌──────────────────────┐                  ║
         ║               │  KAFKA               │                  ║
         ║               │  debezium.public.    │                  ║
         ║               │  simulated_orders    │                  ║
         ║               │                      │                  ║
         ║               │  payload: {before,   │                  ║
         ║               │   after, op, ts_ms}  │                  ║
         ║               └──────────┬───────────┘                  ║
         ║                          ║                              ║
╔════════╩══════════════════════════╩═════════════════════════════ ╩══════════════╗
║                     DATA ENGINEERING — INGESTION                                ║
╚════════╦══════════════════════════╦═════════════════════════════ ╦══════════════╝
         ║                          ║                              ║
         ▼                          ▼                              ▼
┌─────────────────┐      ┌──────────────────────┐    ┌─────────────────────────┐
│  consumer.py    │      │  cdc_consumer.py     │    │  file_watcher.py        │
│                 │      │                      │    │                         │
│  Kafka Consumer │      │  Kafka Consumer      │    │  Poll MinIO cada 30s    │
│  group:         │      │  group:              │    │                         │
│  de_clickstream │      │  de_cdc_consumer     │    │  Lê .txt novos          │
│  _consumer      │      │                      │    │  Parse do header        │
│                 │      │  Extrai payload      │    │  Mantém raw_content     │
│  Adiciona:      │      │  "after"             │    │                         │
│  ingested_at    │      │  ops: c/u/r only     │    │  Estado em              │
│  source=kafka   │      │                      │    │  .watcher_state.json    │
│                 │      │  Adiciona:           │    │  (evita re-ingestão)    │
│  Buffer:        │      │  cdc_operation       │    │                         │
│  500 rec / 30s  │      │  cdc_ts_ms           │    │  Flush imediato         │
│                 │      │  ingested_at         │    │  por batch              │
│                 │      │  source=debezium     │    │                         │
│                 │      │                      │    │  Adiciona:              │
│                 │      │  Buffer:             │    │  ingested_at            │
│                 │      │  500 rec / 30s       │    │  source=minio           │
└────────┬────────┘      └──────────┬───────────┘    └────────────┬────────────┘
         ║                          ║                              ║
         ▼                          ▼                              ▼
┌─────────────────┐      ┌──────────────────────┐    ┌─────────────────────────┐
│ BRONZE (MinIO)  │      │  BRONZE (MinIO)      │    │  BRONZE (MinIO)         │
│                 │      │                      │    │                         │
│ bronze/         │      │  bronze/             │    │  bronze/                │
│ clickstream/    │      │  orders/             │    │  reviews/               │
│ year=Y/month=M/ │      │  year=Y/month=M/     │    │  year=Y/month=M/        │
│ day=D/hour=H/   │      │  day=D/hour=H/       │    │  day=D/                 │
│ batch_<ts>      │      │  batch_<ts>.parquet  │    │  batch_<ts>.parquet     │
│ .parquet        │      │                      │    │                         │
│                 │      │  Schema: order_id,   │    │  Schema: file_path,     │
│ Schema: event_  │      │  session_id,         │    │  review_id, order_id,   │
│ id, session_id, │      │  customer_id,        │    │  customer_id, rating,   │
│ user_id,        │      │  product_id,         │    │  title, raw_content,    │
│ event_type,     │      │  seller_id,          │    │  ingested_at, source    │
│ timestamp,      │      │  category, price,    │    │                         │
│ device,         │      │  freight_value,      │    │  Dados RAW — NLP        │
│ properties      │      │  purchase_timestamp, │    │  downstream             │
│ (JSON string),  │      │  state,              │    │                         │
│ ingested_at,    │      │  cdc_operation,      │    │                         │
│ source          │      │  cdc_ts_ms,          │    │                         │
│                 │      │  ingested_at, source │    │                         │
└────────┬────────┘      └──────────┬───────────┘    └────────────┬────────────┘
         ║                          ║                              ║
╔════════╩══════════════════════════╩══════════════════════════════╩══════════════╗
║  ANALYTICAL ENGINEERING — AIRFLOW DAG: trendmart_gold_pipeline  (0 * * * *)     ║
║                                                                                 ║
║  Fase 1 — Silver (correm em paralelo)                                           ║
╚════════╦══════════════════════════╦══════════════════════════════╦══════════════╝
         ║                          ║                              ║
         ▼                          ▼                              ▼
┌─────────────────┐      ┌──────────────────────┐    ┌─────────────────────────┐
│silver_clickstream│     │  silver_orders.py    │    │  silver_reviews.py       │
│.py              │      │                      │    │                         │
│  Spark batch    │      │  Spark batch         │    │  Spark batch            │
│  availableNow   │      │  availableNow        │    │  availableNow           │
│                 │      │                      │    │                         │
│  Deserializa    │      │  Filtra:             │    │  Extrai message body    │
│  properties     │      │  order_id ≠ null     │    │  com regex após "---"   │
│  JSON → struct  │      │  price > 0           │    │                         │
│                 │      │  freight >= 0        │    │  Valida rating:         │
│  Filtra:        │      │                      │    │  extrai dígitos,        │
│  session_id ≠∅  │      │  Normaliza state:    │    │  filtra [1-5]           │
│  event_type ≠∅  │      │  UPPER+TRIM+validar  │    │                         │
│  event_ts ≠null │      │  27 códigos BR       │    │  Corrige encoding:      │
│                 │      │                      │    │  UTF-8→Latin-1          │
│  Normaliza      │      │  Trim category       │    │  (10 substituições)     │
│  device: bots/  │      │                      │    │                         │
│  crawlers →     │      │  total_value =       │    │  Filtra message vazio   │
│  "unknown"      │      │  price + freight     │    │  Calcula text_length    │
│                 │      │                      │    │                         │
│  location =     │      │  Join region_map     │    │  Dedup review_id:       │
│  coalesce(      │      │  state → região BR   │    │  foreachBatch +         │
│  state, city)   │      │                      │    │  MERGE INTO             │
│                 │      │  Dedup order_id:     │    │  (insert-only)          │
│  Cast timestamps│      │  foreachBatch +      │    │                         │
│  para TIMESTAMP │      │  MERGE INTO          │    │  Mantém message         │
│                 │      │  (insert-only)       │    │  para NLP               │
└────────┬────────┘      └──────────┬───────────┘    └────────────┬────────────┘
         ║                          ║                              ║
         ▼                          ▼                              ▼
┌─────────────────┐      ┌──────────────────────┐    ┌─────────────────────────┐
│ lake.silver.    │      │  lake.silver.orders  │    │  lake.silver.reviews    │
│ clickstream     │      │                      │    │                         │
│ (Iceberg)       │      │  (Iceberg)           │    │  (Iceberg)              │
│ s3a://silver/   │      │  s3a://silver/       │    │  s3a://silver/          │
└────────┬────────┘      └──────────┬───────────┘    └────────────┬────────────┘
         ║                          ║                             ║
╔════════╩══════════════════════════╩═════════════════════════════╩═════════════╗
║           Fase 2 — Gold (correm em paralelo após Silver terminar)             ║
╚═══════╦═══════════════╦═══════════════════════╦══════════════════════════╦════╝
        ║               ║                       ║                          ║
        ▼               ▼                       ▼                          ▼
┌──────────────┐ ┌──────────────┐  ┌─────────────────────┐  ┌──────────────────────┐
│gold_clickst  │ │ gold_sales   │  │   gold_reviews      │  │  gold_dimensions     │
│ream.py       │ │ .py          │  │   .py               │  │  .py                 │
│              │ │              │  │                     │  │                      │
│ Adiciona     │ │  Adiciona    │  │  Adiciona           │  │  dim_date            │
│ event_date   │ │  purchase_   │  │  review_date        │  │  2020→2030 (gerada)  │
│ = to_date(   │ │  date        │  │  = to_date(         │  │  year, quarter,      │
│ event_ts)    │ │  = to_date(  │  │  ingested_at)       │  │  month, week,        │
│              │ │  purchase_ts)│  │                     │  │  day_name,           │
│ createOrRepl │ │              │  │  Classifica:        │  │  is_weekend          │
│ ace()        │ │  createOrRepl│  │  rating>=4→positive │  │                      │
│              │ │  ace()       │  │  rating==3→neutral  │  │  dim_category        │
│ XCom push:   │ │              │  │  rating<=2→negative │  │  union orders +      │
│ count        │ │  XCom push:  │  │                     │  │  clickstream         │
│              │ │  count       │  │  createOrReplace()  │  │                      │
│              │ │              │  │                     │  │  dim_geography       │
│              │ │              │  │  XCom push: count   │  │  27 estados BR       │
└──────┬───────┘ └──────┬───────┘  └──────────┬──────────┘  └──────────────────────┘
       ║                ║                     ║
       ╚════════════════╬═════════════════════╝
                        ║  (todos terminam antes de init_views)
                        ▼
┌─────────────────┐   ┌──────────────────────┐   ┌──────────────────────────────┐
│lake.gold.       │   │  lake.gold.          │   │  lake.gold.fact_reviews      │
│fact_clickstream │   │  fact_sales          │   │                              │
│(Iceberg)        │   │  (Iceberg)           │   │  (Iceberg)                   │
│s3a://gold/      │   │  s3a://gold/         │   │  s3a://gold/                 │
│part: event_date │   │  part: purchase_date │   │  part: review_date           │
└─────────────────┘   └──────────────────────┘   └──────────────────────────────┘
       ║                        ║                              ║
       ╚════════════════════════╬══════════════════════════════╝
                                ║
                                ▼
                    ┌───────────────────────┐
                    │    init_views.py      │
                    │                       │
                    │  Conecta ao Trino     │
                    │  CREATE OR REPLACE    │
                    │  VIEW para cada vista │
                    └───────────┬───────────┘
                                ║
       ┌────────────────────────╬──────────────────────────────┐
       ▼                        ▼                              ▼
┌─────────────────┐   ┌──────────────────────┐   ┌────────────────────────┐
│ vw_executive    │   │ vw_sales_performance │   │  vw_funnel             │
│                 │   │                      │   │                        │
│ Por dia:        │   │ Por dia×categoria    │   │  Por hora×event_type   │
│ total_orders    │   │ ×região×estado:      │   │  ×device×category:     │
│ total_customers │   │ orders               │   │  event_count           │
│ total_revenue   │   │ customers            │   │  sessions              │
│ avg_order_value │   │ revenue              │   │  users                 │
│ avg_rating      │   │ avg_order_value      │   │                        │
│ positive_reviews│   │ product_revenue      │   │                        │
│ negative_reviews│   │ freight_revenue      │   │                        │
└─────────────────┘   └──────────────────────┘   └────────────────────────┘

                    ┌──────────────────────┐   ┌──────────────────────┐
                    │    vw_reviews        │   │    vw_trends         │
                    │                      │   │                      │
                    │  Por dia×sentimento  │   │  Por dia (global):   │
                    │  ×categoria×região:  │   │  orders_growth_pct   │
                    │  review_count        │   │  revenue_growth_pct  │
                    │  avg_rating          │   │  revenue_acceleration│
                    │  avg_text_length     │   │  anomaly_flag        │
                    └──────────────────────┘   └──────────────────────┘

                    ┌──────────────────────┐
                    │  vw_category_trends  │
                    │                      │
                    │  Por dia×categoria:  │
                    │  orders_growth_pct   │
                    │  revenue_growth_pct  │
                    └──────────┬───────────┘
                               ║
╔══════════════════════════════╩═════════════════════════════════════════════════╗
║  MACHINE LEARNING — AIRFLOW DAG: trendmart_ml_pipeline  (0 3 * * *)            ║
║  (corre após o Gold pipeline ter os dados do dia actualizados)                  ║
╚═══════════════════════════╦════════════════════════════════════════════════════╝
                            ║
                            ▼
              ┌─────────────────────────────┐
              │   demand_forecast.py        │
              │                             │
              │  Features:                  │
              │  lag_1, lag_7, lag_14       │
              │  rolling_7d_mean            │
              │  day_of_week, is_weekend    │
              │  month, quarter             │
              │                             │
              │  Modelo:                    │
              │  LinearRegression           │
              │  (Spark MLlib)              │
              │                             │
              │  Avaliação:                 │
              │  RMSE, MAE                  │
              │  (últimos 20% das datas)    │
              └─────────────┬───────────────┘
                            │  ↕ MLflow (métricas + modelo)
                            ▼
              ┌─────────────────────────────┐
              │ lake.gold.                  │
              │ ml_demand_forecast          │
              │                             │
              │ category_id                 │
              │ category_en                 │
              │ forecast_date               │
              │ predicted_orders            │
              │ model_rmse, model_mae       │
              │ scored_at                   │
              └─────────────────────────────┘
                            │
                            ║
╔═══════════════════════════╩════════════════════════════ ════════════════════════╗
║  CONSUMO                                                                        ║
║                                                                                 ║
║  Trino SQL  →  SELECT * FROM lake.gold.vw_executive ORDER BY day DESC           ║
║               SELECT * FROM lake.gold.ml_demand_forecast ORDER BY forecast_date ║
║  Dashboard  →  Dash app (Plotly) — 5 abas (Executive, Vendas, Funil,            ║
║               Reviews, ML Insights)                                             ║
║  MLflow     →  http://localhost:5001 (métricas e artefactos dos modelos)        ║
╚═════════════════════════════════════════════════════════════════════════════════╝
```

---

## Resumo por camada

| Camada | Tecnologia | Formato | Trigger | Responsável |
|---|---|---|---|---|
| **Fontes** | PostgreSQL, Kafka, MinIO | Tabela SQL, JSON, .txt | Contínuo (0.5 sess/s) | Data Sources |
| **Bronze** | MinIO (Parquet) | Parquet particionado | Buffer 500 rec / 30s | Data Engineering |
| **Silver** | Iceberg (HMS + MinIO) | Iceberg ACID | Airflow batch horário | Analytical Engineering |
| **Gold** | Iceberg (HMS + MinIO) | Iceberg ACID | Airflow batch horário (após Silver) | Analytical Engineering |
| **Views** | Trino | SQL Views (6 views) | Após cada run Gold | Analytical Engineering |
| **ML Gold** | Iceberg (HMS + MinIO) | Iceberg ACID | Diário (após Gold) | Machine Learning |
| **Consumo** | Trino / Dashboard / MLflow | SQL / HTTP | On-demand | — |

---

## Latências esperadas

```
Evento gerado pelo simulador
    │
    ├─► Kafka em < 1s
    ├─► PostgreSQL em < 1s  →  Debezium CDC → Kafka em ~10s (heartbeat)
    └─► MinIO .txt em < 1s  →  file_watcher poll em até 30s
         │
         ▼
    Bronze Parquet em até 30s (flush interval)
         │
         ▼
    Silver + Gold Iceberg em até 60min (Airflow schedule horário)
         │
         ▼
    Trino Views actualizadas após cada run
         │
         ▼
    ML Gold Tables actualizadas 1x/dia às 03:00 (demand forecast)
         │
         ▼
    Dashboard ML Insights disponível após cada run ML
```

---

## Tecnologias por camada

```
┌─────────────────────────────────────────────────────────────────────┐
│                         STACK TECNOLÓGICO                           │
├──────────────────┬──────────────────────────────────────────────────┤
│ Simulação        │ Python 3.11, confluent-kafka, psycopg2, boto3    │
├──────────────────┼──────────────────────────────────────────────────┤
│ Mensageria       │ Apache Kafka 7.6 (KRaft, sem Zookeeper)          │
│ CDC              │ Debezium 2.6 (PostgreSQL WAL → Kafka)            │
├──────────────────┼──────────────────────────────────────────────────┤
│ Object Storage   │ MinIO (S3-compatible)                            │
│ Formato Bronze   │ Apache Parquet (via PyArrow)                     │
├──────────────────┼──────────────────────────────────────────────────┤
│ Batch Silver     │ Apache Spark 3.5 (trigger availableNow)          │
│ Batch Gold       │ Apache Spark 3.5 (via PySpark)                   │
│ Formato Silver/Gold │ Apache Iceberg 1.5 (ACID, time-travel)       │
│ Catalog          │ Hive Metastore 3.1 (Thrift)                     │
├──────────────────┼──────────────────────────────────────────────────┤
│ Orquestração     │ Apache Airflow 2.8 (LocalExecutor)               │
│ Query Engine     │ Trino 430                                        │
├──────────────────┼──────────────────────────────────────────────────┤
│ ML — Modelos     │ Apache Spark MLlib 3.5 (LinearRegression)         │
│ ML — Rastreio    │ MLflow 2.13 (métricas, artefactos, modelos)      │
│ ML — Storage     │ MinIO s3://mlflow/ (modelos) + Iceberg (scores)  │
├──────────────────┼──────────────────────────────────────────────────┤
│ Infraestrutura   │ Docker + Docker Compose (12+ serviços)           │
│ Runtime          │ Python 3.11 + Java 17                            │
└──────────────────┴──────────────────────────────────────────────────┘
```
