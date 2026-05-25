# Estrutura do Projeto por Equipas

## Visão Geral

O **TrendMart** é um pipeline de dados em grande escala que simula uma plataforma de e-commerce. O projeto está dividido em **6 equipas**, cada uma responsável por uma camada da arquitetura.

```
data_sources → data_engineering → analytical_engineering → dashboard
                                          ↑                    ↑
                                    infrastructure       machine_learning
                                  (suporta tudo)         (modelos ML diários)
```

---

## 1. `data_sources/` — Simulador de E-commerce

**Responsabilidade:** Gerar dados contínuos que simulam uma plataforma de e-commerce real.

Usa dados reais do dataset Olist (CSVs com produtos, clientes, preços, reviews) carregados uma única vez na inicialização. A partir daí, simula sessões de utilizador a ~0.5 sessões/segundo, percorrendo um funnel completo:

```
session_start → search/browse → product_view → add_to_cart → checkout
```

Cada sessão produz dados para **3 destinos**:

| Destino | Ficheiro | O quê |
|---------|----------|-------|
| **Kafka** | `producer.py` | Todos os eventos de clickstream (JSON) |
| **PostgreSQL** | `db_writer.py` | Registos de compra (`simulated_orders`) |
| **MinIO** | `review_writer.py` | Reviews em ficheiros `.txt` |

**Tecnologias:** Python, Kafka, PostgreSQL, MinIO

---

## 2. `data_engineering/` — Ingestão e Transformação Bronze → Silver

**Responsabilidade:** Consumir dados das 3 fontes, armazená-los em bruto (Bronze) e transformá-los em tabelas limpas e tipadas (Silver).

### Ingestão (contínua)

Três consumers correm em paralelo e escrevem Parquet na camada Bronze:

| Consumer | Fonte | Destino |
|----------|-------|---------|
| `consumer.py` | Kafka `clickstream_events` | `bronze/clickstream/` |
| `cdc_consumer.py` | Debezium CDC (PostgreSQL WAL) | `bronze/orders/` |
| `file_watcher.py` | MinIO `raw-reviews/` | `bronze/reviews/` |

Cada consumer bufferiza 500 registos ou 30 segundos antes de escrever, e adiciona metadados (`ingested_at`, `source`).

### Transformação (batch horária via Spark)

Três jobs Spark leem os Parquets Bronze e produzem tabelas Iceberg Silver:

| Job | Entrada | Saída | O quê faz |
|-----|---------|-------|-----------|
| `silver_clickstream.py` | `bronze/clickstream/` | `lake.silver.clickstream` | Desserializa JSON, tipagem forte |
| `silver_orders.py` | `bronze/orders/` | `lake.silver.orders` | Valida registos, enriquece com região |
| `silver_reviews.py` | `bronze/reviews/` | `lake.silver.reviews` | Extrai corpo de texto com regex |

**Tecnologias:** Python, Apache Spark 3.5+, Apache Iceberg, Hive Metastore, Debezium, MinIO (S3A), PyArrow

---

## 3. `analytical_engineering/` — Silver → Gold + Vistas Trino

**Responsabilidade:** Orquestrar o pipeline completo de transformação analítica e expor vistas SQL para consumo.

Tudo é controlado por uma **DAG Airflow** que corre de hora a hora (`0 * * * *`):

```
silver_clickstream ──┐
silver_orders      ──┼──► gold_dimensions ──┬──► gold_clickstream ──┐
silver_reviews     ──┘                      ├──► gold_sales       ──┼──► init_views
                                            └──► gold_reviews     ──┘
```

### Dimensões (Spark batch)

`gold_dimensions.py` cria 3 tabelas dimensionais:

- **`dim_date`** — 3652 linhas (2020–2030): year, month, quarter, is_weekend
- **`dim_category`** — categorias únicas de orders + clickstream
- **`dim_geography`** — 27 estados brasileiros com região

### Fact Tables (Spark batch + joins às dimensões)

| Job | Entrada | Saída | Nota |
|-----|---------|-------|------|
| `gold_sales.py` | `silver.orders` | `lake.gold.fact_sales` | Joins a dim_date, dim_category, dim_geography |
| `gold_clickstream.py` | `silver.clickstream` | `lake.gold.fact_clickstream` | Joins a dim_date, dim_category, dim_geography |
| `gold_reviews.py` | `silver.reviews` | `lake.gold.fact_reviews` | Adiciona coluna `sentiment` baseada no rating |

Sentiment: rating ≥ 4 → "positive" | rating = 3 → "neutral" | rating ≤ 2 → "negative"

### Vistas Trino (SQL)

`init_views.py` cria 6 vistas analíticas no Trino após os Gold jobs:

| Vista | O quê mostra |
|-------|-------------|
| `vw_executive` | KPIs diários: revenue, orders, avg rating, reviews positivos/negativos |
| `vw_sales_performance` | Vendas por categoria, região e estado |
| `vw_funnel` | Eventos por step do funnel, taxa de conversão |
| `vw_reviews` | Sentimento por categoria e período |
| `vw_trends` | Métricas de tendência globais: crescimento WoW, aceleração da procura, anomalias |
| `vw_category_trends` | Crescimento semana a semana por categoria |

**Tecnologias:** Apache Airflow, Apache Spark 3.5+, Apache Iceberg, Trino 430, Hive Metastore, MinIO

---

---

## 4. `machine_learning/` — Modelos de Machine Learning

**Responsabilidade:** Treinar modelos ML sobre as tabelas Gold e escrever previsões de volta para novas tabelas Gold, consumíveis pelo Dashboard e por Data Scientists.

Tudo é orquestrado por uma **DAG Airflow diária** (`trendmart_ml_pipeline`, às 03:00), que corre depois do pipeline Gold ter os dados do dia actualizados.

### Modelos implementados

| Modelo | Algoritmo | Input | Output |
|---|---|---|---|
| **Previsão de Procura** | LinearRegression (Spark MLlib) + lag features | `fact_sales` + `dim_date` + `dim_category` | `gold.ml_demand_forecast` (próximos 7 dias por categoria) |
| **Previsão de Churn** | RandomForestClassifier (Spark MLlib) + RFM features | `fact_sales` + `fact_reviews` | `gold.ml_churn_scores` (probabilidade + risco por cliente) |

### Avaliação e rastreio

Ambos os modelos registam métricas e artefactos no **MLflow** (`http://localhost:5001`):
- Demand forecast: RMSE e MAE no conjunto de teste (últimos 20% das datas por categoria)
- Churn: F1 macro, AUC-ROC, precision, recall + importância das features

### DAG ML (dag_ml_pipeline.py)

```
demand_forecast ──► churn_prediction
(LinearRegression)  (RandomForest)
```

Tarefas sequenciais pelo mesmo motivo do DAG Gold: cada task lança um JVM Spark completo.

**Tecnologias:** Python, Apache Spark 3.5+ (MLlib), MLflow 2.13, Apache Airflow, Apache Iceberg, MinIO

---

## 5. `infrastructure/` — Serviços Docker

**Responsabilidade:** Definir, construir e ligar todos os serviços do sistema via Docker Compose.

### Serviços (11+ containers na rede `ge_network`)

| Container | Tecnologia | Porta | Função |
|-----------|-----------|-------|--------|
| `ge_postgres` | PostgreSQL 14 | 5434 | Base de dados principal (olist_db, airflow, hive_metastore) + WAL para CDC |
| `ge_minio` | MinIO | 9004–9005 | Object Storage (bronze/, silver/, gold/, raw-reviews/) |
| `ge_kafka` | Kafka 7.6 KRaft | 29092 | Message broker |
| `ge_kafka_connect` | Debezium Connect | 8083 | CDC connector (PostgreSQL WAL → Kafka) |
| `ge_hive_metastore` | Hive Metastore | 9083 | Catálogo de metadata (Thrift) |
| `ge_trino` | Trino 430 | 8085 | SQL query engine |
| `ge_airflow_webserver` | Airflow | 8081 | UI de orquestração |
| `ge_airflow_scheduler` | Airflow | — | Executa DAGs |
| `ge_minio_init` | Script | — | Cria buckets (run once) |
| `ge_debezium_init` | Script | — | Regista connector CDC (run once) |
| `ge_airflow_init` | Script | — | DB upgrade + cria admin (run once) |

Inclui Dockerfiles customizados (`Dockerfile.airflow`, `Dockerfile.spark`, etc.) com JARs Iceberg, S3A e Spark pré-instalados.

**Tecnologias:** Docker, Docker Compose, PostgreSQL, Kafka, MinIO, Debezium, Hive Metastore, Trino, Airflow, Spark, Java 17

---

## 6. `dashboard/` — Visualização

**Responsabilidade:** Interface web para análise dos dados em tempo quase-real.

App Dash que consulta as vistas Trino de **5 em 5 minutos** e apresenta 4 abas:

| Aba | Conteúdo |
|-----|----------|
| **Executive** | KPIs diários em série temporal: revenue, orders, avg order value, avg rating |
| **Sales** | Vendas por categoria, região e estado |
| **Funnel** | Visualização do funil de conversão (session_start → checkout) |
| **Reviews** | Sentimento por categoria, evolução temporal, rating médio |
| **ML Insights** | Previsão de procura (próximos 7 dias), distribuição de risco de churn, métricas dos modelos (RMSE, F1, AUC) |

Acesso: `http://localhost:8050`

**Tecnologias:** Dash, Plotly, Pandas, Trino SQL Client

---

## Pastas Extra

| Pasta | Conteúdo |
|-------|----------|
| `Extra/` | Scripts utilitários: `start.py`, `clean.py`, `reset.py`, `query_gold.py` |
| `explicações/` | Documentação markdown detalhada por equipa e do pipeline completo |

---

## Tabela Resumo

| Equipa | Entrada | Saída | Frequência |
|--------|---------|-------|-----------|
| `data_sources` | CSVs Olist | Kafka, PostgreSQL, MinIO | Contínua (0.5 sess/s) |
| `data_engineering` (ingestão) | Kafka, PG CDC, MinIO | Bronze (Parquet) | Contínua (3 consumers) |
| `data_engineering` (transform) | Bronze | Silver (Iceberg) | Horária (Airflow) |
| `analytical_engineering` | Silver | Gold (Iceberg) + 6 Vistas Trino | Horária (Airflow DAG) |
| `machine_learning` | Gold (Iceberg) | ML Gold Tables + MLflow experiments | Diária (Airflow DAG, 03:00) |
| `dashboard` | Vistas Trino + ML Gold Tables | Gráficos web (5 abas) | Refresh a cada 5 min |
| `infrastructure` | YAML + Dockerfiles | 12+ serviços Docker | On-demand |
