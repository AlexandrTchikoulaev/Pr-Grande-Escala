# Data Engineering — Revisão Completa

## O que faz esta equipa

A equipa de Data Engineering tem **duas responsabilidades principais**:

1. **Ingestion** — consumir dados das fontes (Kafka, PostgreSQL CDC, MinIO) e escrever em Bronze (Parquet cru)
2. **Transformation** — ler Bronze com Spark e escrever tabelas limpas e tipadas em Silver (Iceberg)

Os jobs de Transformation são orquestrados pelo Airflow (juntamente com os Gold), correndo em batch horário.

---

## Estrutura de ficheiros

```
data_engineering/
├── ingestion/
│   ├── config.py          # Configurações (Kafka, MinIO, buffer)
│   ├── consumer.py        # Kafka → Bronze (clickstream)
│   ├── cdc_consumer.py    # Kafka CDC → Bronze (orders)
│   └── file_watcher.py    # MinIO .txt → Bronze (reviews)
├── transformation/
│   ├── config.py          # Configurações (MinIO, HMS, REGION_MAP)
│   ├── spark_session.py   # Factory SparkSession (S3A + Iceberg + HMS)
│   ├── silver_clickstream.py  # Bronze → Silver (clickstream)
│   ├── silver_orders.py       # Bronze → Silver (orders)
│   └── silver_reviews.py      # Bronze → Silver (reviews)
├── debezium/
│   └── connector.json     # Configuração do conector Debezium (registo automático)
└── contracts/
    ├── input.md           # O que recebe de data_sources
    └── output.md          # O que entrega a analytical_engineering
```

---

## Fluxo completo

```
╔══════════════════════════════════════════════════════════════════╗
║                    DATA SOURCES (upstream)                       ║
╠══════════════════════════════════════════════════════════════════╣
║  PostgreSQL: simulated_orders    Kafka: clickstream_events       ║
║  (WAL log — escrita pelo sim.)   (JSON — escrito pelo sim.)      ║
║                                                                  ║
║  MinIO: raw-reviews/             (ficheiros .txt)                ║
╚══════╦═══════════════════════╦══════════════════════════╦════════╝
       ║                       ║                          ║
       ▼                       ▼                          ▼
┌──────────────┐    ┌──────────────────┐    ┌──────────────────────┐
│  Debezium    │    │  consumer.py     │    │  file_watcher.py     │
│  CDC         │    │                  │    │                      │
│  connector   │    │  Lê tópico       │    │  Poll ao MinIO       │
│  (registo    │    │  clickstream_    │    │  raw-reviews/ cada   │
│  automático  │    │  events          │    │  30s. Lê .txt novos, │
│  via curl)   │    │                  │    │  faz parse ao header │
│              │    │  Buffer: 500 rec │    │  (REVIEW_ID,         │
│  Captura WAL │    │  ou 30s          │    │  ORDER_ID, RATING…)  │
│  do Postgres │    │                  │    │  guarda estado em    │
│  → publica   │    │  Adiciona        │    │  .watcher_state.json │
│  no tópico:  │    │  ingested_at     │    │  para não repetir    │
│  debezium.   │    │  e source=kafka  │    │                      │
│  public.     │    │                  │    │  Buffer: flush       │
│  simulated_  │    │                  │    │  imediato por batch  │
│  orders      │    │                  │    │                      │
└──────┬───────┘    └────────┬─────────┘    └──────────┬───────────┘
       │                     │                          │
       ▼                     ▼                          ▼
┌──────────────┐    ┌──────────────────┐    ┌──────────────────────┐
│ cdc_consumer │    │  Parquet         │    │  Parquet             │
│ .py          │    │                  │    │                      │
│              │    │  bronze/         │    │  bronze/             │
│  Lê tópico   │    │  clickstream/    │    │  reviews/            │
│  Debezium,   │    │  year=Y/month=M/ │    │  year=Y/month=M/     │
│  extrai só   │    │  day=D/hour=H/   │    │  day=D/              │
│  payload     │    │  batch_<ts>      │    │  batch_<ts>.parquet  │
│  "after"     │    │  .parquet        │    │                      │
│  (ops: c/u/r)│    │                  │    │  raw_content intacto │
│  Buffer: 500 │    │                  │    │  (NLP downstream)    │
│  rec ou 30s  │    │                  │    │                      │
└──────┬───────┘    └────────┬─────────┘    └──────────┬───────────┘
       │                     │                          │
       ▼                     ▼                          ▼
╔══════════════════════════════════════════════════════════════════╗
║                    BRONZE (MinIO)                                ║
║                                                                  ║
║  bronze/orders/year=…/month=…/day=…/hour=…/batch_<ts>.parquet   ║
║  bronze/clickstream/year=…/month=…/day=…/hour=…/batch_<ts>.parquet
║  bronze/reviews/year=…/month=…/day=…/batch_<ts>.parquet         ║
║                                                                  ║
║  Dados RAW, sem transformação, com ingested_at e source          ║
╚══════╦═══════════════════════╦══════════════════════════╦════════╝
       ║                       ║                          ║
       ▼                       ▼                          ▼
┌──────────────┐    ┌──────────────────┐    ┌──────────────────────┐
│silver_orders │    │silver_clickstream│    │  silver_reviews.py   │
│.py           │    │.py               │    │                      │
│              │    │                  │    │  Spark batch         │
│  Spark batch │    │  Spark batch     │    │  trigger:            │
│  trigger:    │    │  trigger:        │    │  availableNow        │
│  availableNow│    │  availableNow    │    │                      │
│              │    │                  │    │  Extrai message body │
│  Filtra:     │    │  Deserializa     │    │  após "---" no regex │
│  order_id ≠∅ │    │  JSON properties │    │                      │
│  price > 0   │    │  blob →          │    │  Filtra:             │
│              │    │  product_id,     │    │  rating entre 1-5    │
│  Calcula:    │    │  category,       │    │  message não vazio   │
│  total_value │    │  price, state    │    │                      │
│  = price +   │    │                  │    │  Calcula:            │
│  freight     │    │  location =      │    │  text_length         │
│              │    │  coalesce(state, │    │  (word count)        │
│  Join com    │    │  city)           │    │                      │
│  region_map  │    │                  │    │  Mantém raw message  │
│  (state →    │    │  Filtra:         │    │  para NLP downstream │
│  região BR)  │    │  event_id ≠ ∅    │    │                      │
│              │    │  event_ts ≠ ∅    │    │                      │
└──────┬───────┘    └────────┬─────────┘    └──────────┬───────────┘
       │                     │                          │
       ▼                     ▼                          ▼
╔══════════════════════════════════════════════════════════════════╗
║                    SILVER (Iceberg — Hive Metastore)             ║
║                                                                  ║
║  lake.silver.orders       → s3a://silver/                        ║
║  lake.silver.clickstream  → s3a://silver/                        ║
║  lake.silver.reviews      → s3a://silver/                        ║
║                                                                  ║
║  Checkpoints: s3a://silver/_checkpoints/{clickstream,orders,reviews}
╚══════════════════════════════════════════════════════════════════╝
                              │
                              ▼
                  analytical_engineering (downstream)
```

---

## Os 3 padrões de ingestion

| Canal | Tecnologia | Padrão |
|---|---|---|
| **Compras** | PostgreSQL WAL → Debezium → Kafka → `cdc_consumer.py` | Change Data Capture |
| **Clickstream** | Kafka → `consumer.py` | Event streaming |
| **Reviews** | MinIO `.txt` → `file_watcher.py` | File polling |

---

## Descrição de cada ficheiro

### debezium/connector.json
- Regista o conector Debezium no Kafka Connect (feito automaticamente pelo `ge_debezium_init` no Docker)
- Monitoriza a tabela `public.simulated_orders` no PostgreSQL via WAL (`pgoutput`)
- Publica mudanças no tópico Kafka `debezium.public.simulated_orders`
- Snapshot mode: `initial` — faz snapshot completo na primeira arrancada

### ingestion/consumer.py
- Consome o tópico Kafka `clickstream_events`
- Adiciona `ingested_at` e `source=kafka` a cada evento
- Faz flatten do campo `properties` (dict → JSON string) para manter Bronze sem schema enforcement
- Flush: 500 registos **ou** 30 segundos (o que acontecer primeiro)
- Escreve Parquet particionado em `bronze/clickstream/year=.../month=.../day=.../hour=.../batch_<ts>.parquet`

### ingestion/cdc_consumer.py
- Consome o tópico Debezium `debezium.public.simulated_orders`
- Extrai apenas o payload `after` (ignora `before` e deletes)
- Processa operações: `c` (create), `u` (update), `r` (read/snapshot)
- Adiciona `cdc_operation`, `cdc_ts_ms`, `ingested_at`, `source=debezium`
- Flush: 500 registos **ou** 30 segundos
- Escreve Parquet em `bronze/orders/year=.../month=.../day=.../hour=.../batch_<ts>.parquet`

### ingestion/file_watcher.py
- Faz poll ao bucket MinIO `raw-reviews/` a cada 30 segundos
- Lê apenas ficheiros `.txt` ainda não processados (estado persistido em `.watcher_state.json`)
- Parse do header estruturado do ficheiro: `REVIEW_ID`, `ORDER_ID`, `CUSTOMER_ID`, `RATING`, `TITLE`
- Preserva `raw_content` intacto para NLP downstream
- Flush imediato por batch (todos os ficheiros novos encontrados no poll)
- Escreve Parquet em `bronze/reviews/year=.../month=.../day=.../batch_<ts>.parquet`

### transformation/spark_session.py
- Factory da `SparkSession` partilhada pelos 3 jobs de Silver
- Configura S3A para MinIO (path style access, endpoint, credenciais)
- Configura Iceberg com catalog `lake` apontado para o Hive Metastore (Thrift)
- Warehouse da Silver: `s3a://silver/`
- Memória: driver 1g, maxResultSize 512m

### transformation/silver_clickstream.py
- Spark batch com `trigger(availableNow=True)` — processa todos os ficheiros Bronze novos desde o último run e termina
- Lê Parquet Bronze com schema explícito (evita inferência)
- Deserializa o campo `properties` (JSON string → struct) com schema definido
- Filtra registos com `session_id` nulo/vazio ou `event_type` nulo/vazio (ruído do simulador)
- Filtra registos sem `event_id` ou `event_ts` inválido
- Normaliza `device`: valores desconhecidos (`"bot"`, `"crawler"`, etc.) → `"unknown"`
- Normaliza `location` = `coalesce(state, city)`
- Converte timestamps de string para `TIMESTAMP`
- Escreve em append para `lake.silver.clickstream` (Iceberg)
- Checkpoint em `s3a://silver/_checkpoints/clickstream` (garante que ficheiros já processados não são repetidos)
- Expõe função `run(spark=None)` — invocada pelo Airflow DAG

### transformation/silver_orders.py
- Spark batch com `trigger(availableNow=True)`
- Lê Parquet Bronze com schema explícito
- Filtra: `order_id` não nulo, `customer_id` não nulo, `price > 0`, `freight_value >= 0`
- Normaliza `state`: `UPPER(TRIM)` + validação contra os 27 códigos de estado brasileiros; valores inválidos (ex: `"sp"` resolve para `"SP"`, `"SP0"` → `null`)
- Normaliza `category`: `TRIM` para remover espaços em volta
- Calcula `total_value = round(price + freight_value, 2)`
- Enriquece com região brasileira via join com `REGION_MAP` (27 estados → 5 regiões); estados nulos/inválidos → `"Desconhecido"`
- **Deduplicação CDC**: usa `foreachBatch` — dentro de cada batch mantém o registo com maior `cdc_ts_ms` por `order_id`; entre batches usa `MERGE INTO` (insert-only) para não duplicar ordens já existentes na Silver
- Checkpoint em `s3a://silver/_checkpoints/orders`
- Expõe função `run(spark=None)` — invocada pelo Airflow DAG

### transformation/silver_reviews.py
- Spark batch com `trigger(availableNow=True)`
- Lê Parquet Bronze com schema explícito
- Filtra: `review_id` e `order_id` não nulos **nem vazios**
- Extrai corpo da review com regex: tudo após o separador `---` no `raw_content`
- Valida rating: extrai apenas dígitos do campo (lida com `"N/A/5"`, `"6/5"`, `"0/5"`); filtra valores fora do intervalo [1,5]
- Corrige encoding: substitui sequências UTF-8 lidas como Latin-1 (`"Ã£"` → `"ã"`, etc.) — 10 pares de substituição com `regexp_replace`
- Filtra `message` vazio ou só whitespace após extração
- Calcula `text_length` (contagem de palavras com `split(\s+)`)
- Mantém `message` normalizado para análise de sentimento downstream (NLP)
- **Deduplicação de submissões duplas**: usa `foreachBatch` — dentro de cada batch mantém o registo mais recente por `review_id`; entre batches usa `MERGE INTO` (insert-only) para não duplicar reviews já existentes na Silver
- Checkpoint em `s3a://silver/_checkpoints/reviews`
- Expõe função `run(spark=None)` — invocada pelo Airflow DAG

---

## Schemas Silver (output para analytical_engineering)

### lake.silver.clickstream
| Coluna | Tipo | Descrição |
|---|---|---|
| event_id | STRING | Identificador único do evento |
| session_id | STRING | Sessão do utilizador |
| user_id | STRING | Customer ID |
| event_type | STRING | Ex: product_view, add_to_cart |
| event_ts | TIMESTAMP | Timestamp do evento (UTC) |
| device | STRING | mobile / desktop / tablet |
| category | STRING | Categoria do produto (inglês) |
| product_id | STRING | Identificador do produto (nullable) |
| price | DOUBLE | Preço do produto (nullable) |
| location | STRING | Estado ou cidade do utilizador |
| ingested_at | TIMESTAMP | Quando foi ingerido |

### lake.silver.orders
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
| purchase_ts | TIMESTAMP | Timestamp da compra (UTC) |
| state | STRING | Estado brasileiro do cliente |
| region | STRING | Região brasileira (derivada do estado) |
| ingested_at | TIMESTAMP | Quando foi ingerido |

### lake.silver.reviews
| Coluna | Tipo | Descrição |
|---|---|---|
| review_id | STRING | Identificador único da review |
| order_id | STRING | Encomenda associada |
| customer_id | STRING | Cliente que escreveu a review |
| rating | INTEGER | Pontuação 1-5 |
| title | STRING | Título da review (pode estar vazio) |
| message | STRING | Texto livre — fonte não estruturada |
| text_length | INTEGER | Contagem de palavras |
| ingested_at | TIMESTAMP | Quando foi ingerido |

---

## Configurações relevantes

### ingestion/config.py
```
KAFKA_BOOTSTRAP     = localhost:29092
CLICKSTREAM_TOPIC   = clickstream_events
CDC_TOPIC           = debezium.public.simulated_orders
GROUP_CLICKSTREAM   = de_clickstream_consumer
GROUP_CDC           = de_cdc_consumer
BRONZE_BUCKET       = bronze
REVIEWS_BUCKET      = raw-reviews
BUFFER_SIZE         = 500 registos
FLUSH_INTERVAL      = 30 segundos
WATCHER_INTERVAL    = 30 segundos
```

### transformation/config.py
```
MINIO_ENDPOINT      = localhost:9004
BRONZE_BUCKET       = bronze
SILVER_BUCKET       = silver
HMS_URI             = thrift://localhost:9083
CHECKPOINT_*        = s3a://silver/_checkpoints/{clickstream,orders,reviews}
```
