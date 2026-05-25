# Analytical Engineering — Revisão Completa

## O que faz esta equipa

A equipa de Analytical Engineering é responsável pela **orquestração completa do pipeline de transformação**: desde Bronze até às vistas SQL no Trino, passando por Silver e Gold.

Tudo corre via **Apache Airflow**, num único DAG horário que sequencia Silver → Gold → Views.

---

## Estrutura de ficheiros

```
analytical_engineering/
├── pipeline/
│   └── dag_trendmart.py          # DAG Airflow — orquestra Silver + Gold + Views
├── transformations/
│   ├── config.py                 # Configurações (MinIO, HMS, Trino, sentimentos, REGION_MAP)
│   ├── spark_session.py          # Factory SparkSession (S3A + Iceberg Gold)
│   ├── gold_sales.py             # Silver.orders → Gold.fact_sales
│   ├── gold_clickstream.py       # Silver.clickstream → Gold.fact_clickstream
│   ├── gold_reviews.py           # Silver.reviews → Gold.fact_reviews
│   └── gold_dimensions.py        # Dimensões: dim_date, dim_category, dim_geography
├── views/
│   └── init_views.py             # Cria/substitui vistas Trino sobre Gold
└── requirements.txt
```

---

## Fluxo completo

```
╔══════════════════════════════════════════════════════════════════╗
║                    BRONZE (upstream — data_engineering)          ║
║                                                                  ║
║  bronze/clickstream/  bronze/orders/  bronze/reviews/            ║
║  (Parquet — MinIO)                                               ║
╚═══╦══════════════════╦═══════════════════╦════════════════════════╝
    ║                  ║                   ║
    ▼                  ▼                   ▼
┌──────────────┐ ┌───────────────┐ ┌────────────────┐
│silver_click  │ │silver_orders  │ │silver_reviews  │
│stream.py     │ │.py            │ │.py             │
│              │ │               │ │                │
│  Spark batch │ │  Spark batch  │ │  Spark batch   │
│  availableNow│ │  availableNow │ │  availableNow  │
│              │ │               │ │                │
│  Desserializa│ │  Filtra e     │ │  Extrai message│
│  properties  │ │  enriquece    │ │  body com regex│
│  JSON → struct│ │  com região  │ │                │
└──────┬───────┘ └───────┬───────┘ └───────┬────────┘
       │                 │                 │
       ▼                 ▼                 ▼
╔══════════════════════════════════════════════════════════════════╗
║                    SILVER (Iceberg — Hive Metastore)             ║
║                                                                  ║
║  lake.silver.orders       → s3a://silver/                        ║
║  lake.silver.clickstream  → s3a://silver/                        ║
║  lake.silver.reviews      → s3a://silver/                        ║
╚═══╦══════════════════╦═══════════════════╦════════════════════════╝
    ║                  ║                   ║
    ▼                  ▼                   ▼
┌──────────────┐ ┌───────────────┐ ┌────────────────┐  ┌──────────────────┐
│gold_sales.py │ │gold_clickstream│ │gold_reviews.py │  │gold_dimensions.py│
│              │ │.py            │ │                │  │                  │
│  Lê          │ │               │ │  Lê            │  │  Gera dim_date   │
│  silver.orders│ │  Lê           │ │  silver.reviews│  │  2020-01-01 a    │
│              │ │  silver.click │ │                │  │  2030-12-31      │
│  Adiciona    │ │  stream       │ │  Adiciona      │  │  (3652 linhas)   │
│  purchase_   │ │               │ │  review_date   │  │                  │
│  date        │ │  Adiciona     │ │  = to_date(    │  │  dim_category    │
│  = to_date(  │ │  event_date   │ │  ingested_at)  │  │  union de        │
│  purchase_ts)│ │  = to_date(   │ │                │  │  categorias de   │
│              │ │  event_ts)    │ │  Classifica    │  │  orders +        │
│  Filtra      │ │               │ │  sentimento    │  │  clickstream     │
│  purchase_   │ │  Filtra       │ │  por rating:   │  │                  │
│  date ≠ null │ │  event_date   │ │  >=4 positive  │  │  dim_geography   │
│              │ │  ≠ null       │ │  ==3 neutral   │  │  27 estados BR   │
│  createOrRepl│ │               │ │  <=2 negative  │  │  com região      │
│  ace()       │ │  createOrRepl │ │                │  │                  │
│              │ │  ace()        │ │  createOrRepl  │  │  Todos com       │
│  Particionado│ │               │ │  ace()         │  │  createOrReplace │
│  days(       │ │  Particionado │ │                │  │                  │
│  purchase_   │ │  days(        │ │  Particionado  │  │                  │
│  date)       │ │  event_date)  │ │  days(review_  │  │                  │
│              │ │               │ │  date)         │  │                  │
└──────┬───────┘ └───────┬───────┘ └───────┬────────┘  └────────┬─────────┘
       │                 │                 │                     │
       ▼                 ▼                 ▼                     ▼
╔══════════════════════════════════════════════════════════════════════════╗
║                         GOLD (Iceberg — Hive Metastore)                 ║
║                                                                         ║
║  lake.gold.fact_sales        → s3a://gold/ (partição: purchase_date)    ║
║  lake.gold.fact_clickstream  → s3a://gold/ (partição: event_date)       ║
║  lake.gold.fact_reviews      → s3a://gold/ (partição: review_date)      ║
║  lake.gold.dim_date          → s3a://gold/                              ║
║  lake.gold.dim_category      → s3a://gold/                              ║
║  lake.gold.dim_geography     → s3a://gold/                              ║
╚═══════════════════════════════╦═════════════════════════════════════════╝
                                ║
                                ▼
                    ┌───────────────────────┐
                    │     init_views.py     │
                    │                       │
                    │  Conecta ao Trino     │
                    │  catalog: lake        │
                    │  schema: gold         │
                    │                       │
                    │  CREATE OR REPLACE    │
                    │  VIEW para cada vista │
                    └───────────┬───────────┘
                                │
       ┌────────────────────────┼───────────────────────┐
       ▼                        ▼                       ▼
┌─────────────┐      ┌──────────────────┐     ┌──────────────────┐
│vw_executive │      │vw_sales_performance│   │   vw_funnel      │
│             │      │                  │     │                  │
│ KPIs diários│      │ Por categoria,   │     │ Por hora,        │
│ orders,     │      │ região, estado:  │     │ event_type,      │
│ customers,  │      │ orders, customers│     │ device,          │
│ revenue,    │      │ revenue, avg_    │     │ category:        │
│ avg_order_  │      │ order_value,     │     │ event_count,     │
│ value,      │      │ product_revenue, │     │ sessions,        │
│ avg_rating, │      │ freight_revenue  │     │ users            │
│ positive/   │      │                  │     │                  │
│ negative    │      └──────────────────┘     └──────────────────┘
│ reviews     │
└─────────────┘      ┌──────────────────┐
                     │   vw_reviews     │
                     │                  │
                     │ Por data,        │
                     │ sentimento,      │
                     │ categoria,       │
                     │ região:          │
                     │ review_count,    │
                     │ avg_rating,      │
                     │ avg_text_length  │
                     └──────────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │  CONSUMIDORES         │
                    │                       │
                    │  Dashboard (Dash)     │
                    │  Data Science         │
                    │  Trino CLI / JDBC     │
                    └───────────────────────┘
```

---

## DAG Airflow (dag_trendmart.py)

**DAG ID:** `trendmart_gold_pipeline`
**Schedule:** `0 * * * *` (início de cada hora)
**Catchup:** False (não recupera horas em falta)
**Max active runs:** 1 (sem overlap)
**Retries:** 1 tentativa, delay de 5 minutos

### Grafo de dependências

```
silver_clickstream ──┐
silver_orders      ──┼──► gold_clickstream ──┐
silver_reviews     ──┘    gold_sales       ──┼──► init_views
                          gold_reviews     ──┘
                          gold_dimensions  ──── (após silver, paralelo com facts)
```

Os 3 Silver correm em paralelo primeiro. Só quando todos terminam é que os Gold arrancam.
Os 3 Gold facts correm em paralelo e só quando todos terminam é que `init_views` corre.
`gold_dimensions` corre após Silver mas em paralelo com os Gold facts — é independente de init_views.

### XCom (comunicação entre tasks)
Cada task de Silver e Gold faz `xcom_push` com o número de registos escritos:
- `silver_clickstream_count`, `silver_orders_count`, `silver_reviews_count`
- `gold_clickstream_count`, `gold_sales_count`, `gold_reviews_count`

A task `init_views` faz `xcom_pull` dos contadores Gold e imprime-os antes de criar as vistas.

---

## Descrição de cada ficheiro

### transformations/spark_session.py
- Factory da `SparkSession` partilhada pelos jobs Gold
- Warehouse aponta para `s3a://gold/` (diferente da Silver que usa `s3a://silver/`)
- Catalog `lake` via Hive Metastore (Thrift)
- Configuração S3A idêntica à DE (path style access, MinIO endpoint)

### transformations/gold_sales.py
- Lê `lake.silver.orders` (tabela Iceberg completa)
- Adiciona `purchase_date = to_date(purchase_ts)`
- Filtra registos com `purchase_date` nulo
- Seleciona: `order_id`, `session_id`, `customer_id`, `product_id`, `seller_id`, `category`, `price`, `freight_value`, `total_value`, `purchase_ts`, `purchase_date`, `state`, `region`
- Escreve com `createOrReplace()` → **idempotente** (cada run substitui tudo)
- Particionado por `days(purchase_date)`
- Iceberg format-version 2 (suporte a row-level deletes)

### transformations/gold_clickstream.py
- Lê `lake.silver.clickstream`
- Adiciona `event_date = to_date(event_ts)`
- Filtra registos com `event_date` nulo
- Seleciona: `event_id`, `session_id`, `user_id`, `event_type`, `event_ts`, `event_date`, `device`, `category`, `product_id`, `price`, `location`
- `createOrReplace()` — idempotente
- Particionado por `days(event_date)`

### transformations/gold_reviews.py
- Lê `lake.silver.reviews`
- Adiciona `review_date = to_date(ingested_at)`
- Classifica sentimento por rating:
  - `rating >= 4` → `"positive"`
  - `rating == 3` → `"neutral"`
  - `rating <= 2` → `"negative"`
- Seleciona: `review_id`, `order_id`, `customer_id`, `rating`, `sentiment`, `title`, `message`, `text_length`, `review_date`
- `createOrReplace()` — idempotente
- Particionado por `days(review_date)`

### transformations/gold_dimensions.py
- **dim_date** — gerada programaticamente de 2020-01-01 a 2030-12-31
  - Campos: `date_id` (YYYYMMDD int), `date_actual`, `year`, `quarter`, `month`, `week`, `day_of_week`, `day_name`, `month_name`, `is_weekend`
- **dim_category** — union de categorias únicas de `silver.orders` + `silver.clickstream`
  - Campos: `category_en`, `category_id` (monotonically_increasing_id)
- **dim_geography** — 27 estados brasileiros do `REGION_MAP`
  - Campos: `state`, `state_name`, `region`
- Todas com `createOrReplace()` — seguro re-correr a qualquer momento

### views/init_views.py
- Conecta ao Trino via `trino.dbapi` (catalog: `lake`, schema: `gold`)
- Executa `CREATE OR REPLACE VIEW` para cada vista definida
- 6 vistas implementadas:

| Vista | Fonte | Agrupamento | KPIs |
|---|---|---|---|
| `vw_executive` | fact_sales + fact_reviews (LEFT JOIN) | Por dia | orders, customers, revenue, avg_order_value, avg_rating, positive/negative reviews |
| `vw_sales_performance` | fact_sales | Por dia × categoria × região × estado | orders, customers, revenue, avg_order_value, product_revenue, freight_revenue |
| `vw_funnel` | fact_clickstream | Por hora × event_type × device × category | event_count, sessions, users |
| `vw_reviews` | fact_reviews + fact_sales (LEFT JOIN) | Por dia × sentimento × categoria × região | review_count, avg_rating, avg_text_length |
| `vw_trends` | fact_sales | Por dia (global) | orders_growth_pct (WoW), revenue_growth_pct (WoW), revenue_acceleration, anomaly_flag |
| `vw_category_trends` | fact_sales + dim_category | Por dia × categoria | orders_growth_pct (WoW), revenue_growth_pct (WoW) |

As vistas `vw_trends` e `vw_category_trends` usam funções de janela Trino (`LAG`, `STDDEV_POP`, `AVG OVER`) para calcular métricas de tendência exigidas pelo enunciado.

---

## Schemas Gold (output para consumo)

### lake.gold.fact_sales
| Coluna | Tipo | Descrição |
|---|---|---|
| order_id | STRING | Identificador único da encomenda |
| session_id | STRING | Sessão que gerou a encomenda |
| customer_id | STRING | Identificador do cliente |
| product_id | STRING | Identificador do produto |
| seller_id | STRING | Identificador do vendedor |
| category | STRING | Categoria do produto (inglês) |
| price | DOUBLE | Preço do produto |
| freight_value | DOUBLE | Custo de envio |
| total_value | DOUBLE | price + freight_value |
| purchase_ts | TIMESTAMP | Timestamp da compra |
| purchase_date | DATE | Data da compra (partição) |
| state | STRING | Estado brasileiro |
| region | STRING | Região brasileira |

### lake.gold.fact_clickstream
| Coluna | Tipo | Descrição |
|---|---|---|
| event_id | STRING | Identificador único do evento |
| session_id | STRING | Sessão do utilizador |
| user_id | STRING | Customer ID |
| event_type | STRING | Tipo de evento |
| event_ts | TIMESTAMP | Timestamp do evento |
| event_date | DATE | Data do evento (partição) |
| device | STRING | mobile / desktop / tablet |
| category | STRING | Categoria do produto |
| product_id | STRING | Identificador do produto |
| price | DOUBLE | Preço do produto |
| location | STRING | Estado ou cidade |

### lake.gold.fact_reviews
| Coluna | Tipo | Descrição |
|---|---|---|
| review_id | STRING | Identificador único |
| order_id | STRING | Encomenda associada |
| customer_id | STRING | Cliente |
| rating | INTEGER | Pontuação 1-5 |
| sentiment | STRING | positive / neutral / negative |
| title | STRING | Título da review |
| message | STRING | Texto livre |
| text_length | INTEGER | Contagem de palavras |
| review_date | DATE | Data da review (partição) |

### lake.gold.dim_date
| Coluna | Tipo | Descrição |
|---|---|---|
| date_id | INTEGER | YYYYMMDD (ex: 20240115) |
| date_actual | DATE | Data real |
| year | INTEGER | Ano |
| quarter | INTEGER | Trimestre 1-4 |
| month | INTEGER | Mês 1-12 |
| week | INTEGER | Semana ISO |
| day_of_week | INTEGER | 1=Segunda … 7=Domingo |
| day_name | STRING | "Monday", "Tuesday"… |
| month_name | STRING | "January", "February"… |
| is_weekend | BOOLEAN | True se Sábado ou Domingo |

---

## Configurações relevantes

```
MINIO_ENDPOINT       = localhost:9004
SILVER_BUCKET        = silver
GOLD_BUCKET          = gold
HMS_URI              = thrift://localhost:9083
TRINO_HOST           = localhost
TRINO_PORT           = 8085

DIM_DATE_START       = 2020-01-01
DIM_DATE_END         = 2030-12-31

SENTIMENT_POSITIVE   = 4   (rating >= 4)
SENTIMENT_NEUTRAL    = 3   (rating == 3)
# rating <= 2 → negative
```

---

## Como ver o resultado

Após o DAG correr, as vistas ficam disponíveis em Trino:

```sql
-- Aceder via CLI Trino (porta 8085)
SELECT * FROM lake.gold.vw_executive ORDER BY day DESC LIMIT 7;
SELECT * FROM lake.gold.vw_funnel WHERE event_type = 'add_to_cart';
SELECT * FROM lake.gold.vw_reviews WHERE sentiment = 'negative';

-- Métricas de tendência (novas)
SELECT * FROM lake.gold.vw_trends ORDER BY purchase_date DESC LIMIT 14;
SELECT * FROM lake.gold.vw_category_trends WHERE category = 'electronics' ORDER BY purchase_date DESC;
```

Airflow UI: http://localhost:8081 (admin / admin)
Trino UI:   http://localhost:8085
