# Infrastructure — Revisão Completa

## O que faz esta equipa

A equipa de Infrastructure é responsável por **definir, construir e ligar todos os serviços** que o pipeline precisa para funcionar: bases de dados, brokers de mensagens, object storage, motores de query, orquestração e processamento distribuído.

É também responsável pelo **Apache Airflow e pelas DAGs de orquestração** (`infrastructure/dags/`). A infrastructure programa os schedules com as frequências definidas pelas equipas consumidoras: a analytical_engineering impõe cadência horária para o pipeline Silver → Gold; a machine_learning impõe cadência diária (03:00) para o pipeline ML.

Tudo corre em Docker via `docker-compose.yml` com uma rede interna partilhada (`ge_network`).

---

## Estrutura de ficheiros

```
infrastructure/
├── docker-compose.yml              # Orquestração de todos os serviços
├── init-db.sh                      # Script de inicialização do PostgreSQL
├── hive-site.xml                   # Configuração do Hive Metastore
├── dags/
│   └── dag_trendmart.py            # DAG horária: Silver + Gold + Views (owner: infrastructure)
├── dockerfiles/
│   ├── Dockerfile.airflow          # Airflow + Java 17 + PySpark + JARs Iceberg/S3A
│   ├── Dockerfile.hive             # Hive Metastore + driver PostgreSQL
│   └── Dockerfile.spark            # Python + Java 17 + PySpark + JARs Iceberg/S3A
└── trino/
    └── etc/
        └── catalog/
            └── iceberg.properties  # Catalog Iceberg do Trino (MinIO + HMS)
```

---

## Visão geral dos serviços

```
╔══════════════════════════════════════════════════════════════════╗
║                        ge_network (bridge)                       ║
║                                                                  ║
║  ┌─────────────┐   ┌──────────────┐   ┌─────────────────────┐  ║
║  │ ge_postgres │   │  ge_minio    │   │      ge_kafka       │  ║
║  │             │   │              │   │                     │  ║
║  │ PostgreSQL  │   │  MinIO S3    │   │  Kafka 7.6 KRaft    │  ║
║  │ :5434       │   │  :9004/:9005 │   │  :29092             │  ║
║  │             │   │              │   │                     │  ║
║  │  Amazon_Sales   │   │  bronze/     │   │  clickstream_events │  ║
║  │  airflow    │   │  silver/     │   │  debezium.public.   │  ║
║  │  hive_meta  │   │  gold/       │   │  simulated_orders   │  ║
║  │  store      │   │  raw-reviews/│   │                     │  ║
║  └──────┬──────┘   └──────┬───────┘   └──────────┬──────────┘  ║
║         │                 │                       │             ║
║  ┌──────▼──────┐   ┌──────▼───────┐   ┌──────────▼──────────┐  ║
║  │ge_kafka_    │   │ ge_minio_init│   │  ge_debezium_init   │  ║
║  │connect      │   │              │   │                     │  ║
║  │             │   │ Cria buckets:│   │  Regista conector   │  ║
║  │ Debezium    │   │ bronze,silver│   │  PostgreSQL CDC     │  ║
║  │ Connect     │   │ gold,        │   │  via curl POST      │  ║
║  │ :8083       │   │ raw-reviews  │   │                     │  ║
║  │             │   │ (run once)   │   │  (run once)         │  ║
║  └──────┬──────┘   └──────────────┘   └─────────────────────┘  ║
║         │                                                        ║
║  ┌──────▼──────┐   ┌──────────────┐                             ║
║  │ge_hive_     │   │  ge_trino    │                             ║
║  │metastore    │   │              │                             ║
║  │             │   │  Trino 430   │                             ║
║  │ Hive Meta.  │   │  :8085       │                             ║
║  │ :9083       │   │              │                             ║
║  │ (Thrift)    │   │  Catalog:    │                             ║
║  │             │   │  iceberg →   │                             ║
║  │  Armazena   │   │  HMS + MinIO │                             ║
║  │  metadata   │   │              │                             ║
║  │  tabelas    │   │  Expõe SQL   │                             ║
║  │  Iceberg    │   │  analítico   │                             ║
║  └─────────────┘   └──────────────┘                             ║
║                                                                  ║
║  ┌─────────────────────────────────────────────────────────┐    ║
║  │                    Airflow (3 containers)               │    ║
║  │                                                         │    ║
║  │  ge_airflow_init        ge_airflow_webserver  :8081     │    ║
║  │  (db upgrade +          ge_airflow_scheduler           │    ║
║  │   cria admin)                                           │    ║
║  │                                                         │    ║
║  │  DAG: trendmart_gold_pipeline (horário)                 │    ║
║  │  Bronze → Silver → Gold Spark batch + Trino views       │    ║
║  └─────────────────────────────────────────────────────────┘    ║
╚══════════════════════════════════════════════════════════════════╝
```

---

## Serviços em detalhe

### ge_postgres — PostgreSQL 14
- **Imagem:** `postgres:14`
- **Porta:** `5434:5432`
- **WAL configurado** para CDC: `wal_level=logical`, `max_replication_slots=5`, `max_wal_senders=5`
- **Bases de dados criadas pelo `init-db.sh`:**
  - `Amazon_Sales` — dados de e-commerce + `simulated_orders`
  - `airflow` — estado do Airflow (DAGs, tasks, logs)
  - `hive_metastore` — metadata das tabelas Iceberg
- **Volume persistente:** `ge_postgres_data`
- **Healthcheck:** `pg_isready -U postgres` a cada 5s

### ge_minio — MinIO Object Storage
- **Imagem:** `minio/minio:latest`
- **Portas:** `9004:9000` (S3 API) e `9005:9001` (Console Web)
- **Credenciais:** `minioadmin / minioadmin`
- **Alias interno:** `geminio` (usado pelo Trino em `iceberg.properties`)
- **Volume persistente:** `ge_minio_data`
- **Healthcheck:** `curl http://localhost:9000/minio/health/live`

### ge_minio_init — Criação de buckets (run once)
- **Imagem:** `minio/mc:latest`
- Cria 5 buckets: `bronze`, `silver`, `gold`, `raw-reviews`, `mlflow`
- Usa `--ignore-existing` — seguro re-correr
- Termina após criação (não é um serviço persistente)

### ge_kafka — Kafka 7.6 (KRaft)
- **Imagem:** `confluentinc/cp-kafka:7.6.0`
- **Porta:** `29092:29092` (acesso externo / localhost)
- **Modo KRaft** — sem Zookeeper
- **Auto-create topics:** ativo
- **Cluster ID:** fixo (`4L6g3nShT-eMCtK--X86sw`)
- **Listeners:**
  - `PLAINTEXT://ge_kafka:9092` — comunicação interna Docker
  - `PLAINTEXT_HOST://localhost:29092` — acesso do host
- **Heap:** 512m max, 256m inicial
- **Limit memória:** 768m

### ge_kafka_connect — Debezium 2.6
- **Imagem:** `debezium/connect:2.6`
- **Porta:** `8083:8083` (REST API)
- **Funções:**
  - Recebe o conector PostgreSQL via REST
  - Lê o WAL do PostgreSQL
  - Publica mudanças no Kafka
- **Tópicos internos de estado:** `debezium.connect.configs`, `debezium.connect.offsets`, `debezium.connect.statuses`
- **Healthcheck:** `curl http://localhost:8083/connectors`
- **Depende de:** `ge_kafka` (healthy) + `ge_postgres` (healthy)

### ge_debezium_init — Registo do conector (run once)
- **Imagem:** `curlimages/curl:latest`
- Faz POST para `http://ge_kafka_connect:8083/connectors` com o ficheiro `connector.json`
- HTTP 201 (criado) ou 409 (já existe) → sucesso
- Termina após registo

### ge_hive_metastore — Hive Metastore 3.1.3
- **Dockerfile:** `Dockerfile.hive` (apache/hive:3.1.3 + driver PostgreSQL JDBC)
- **Porta:** `9083:9083` (Thrift)
- **Alias interno:** `gehms` (usado pelo Trino em `iceberg.properties`)
- **Configuração (`hive-site.xml`):**
  - Backend: PostgreSQL (`hive_metastore` database)
  - Schema auto-init: `datanucleus.autoCreateSchema=true`
  - Warehouse dir: `/tmp/hive/warehouse` (apenas metadata; dados reais no MinIO)
- **Função:** Armazena metadata das tabelas Iceberg (schemas, partições, snapshots)
- **Depende de:** `ge_postgres` (healthy) + `ge_minio` (healthy)

### ge_trino — Trino 430
- **Imagem:** `trinodb/trino:430`
- **Porta:** `8085:8080`
- **Catalog configurado:** `iceberg` (via `iceberg.properties`)
  - Metastore: `thrift://gehms:9083`
  - Storage: MinIO via `http://geminio:9000`
  - Path style access: true
  - File format: PARQUET
- **Acesso SQL:** `SELECT * FROM lake.gold.vw_executive`
- **Depende de:** `ge_hive_metastore` + `ge_minio` (healthy)
- **Healthcheck:** `curl http://localhost:8080/v1/info`
- **Limit memória:** 1g

### ge_mlflow — MLflow Tracking Server
- **Imagem:** `ghcr.io/mlflow/mlflow:v2.13.0`
- **Porta:** `5001:5000`
- **Backend store:** SQLite em volume `/mlflow/mlruns.db` (persistente)
- **Artifact store:** `s3://mlflow/` (MinIO, bucket criado pelo `ge_minio_init`)
- **Variáveis de ambiente:** `MLFLOW_S3_ENDPOINT_URL=http://geminio:9000`, credenciais MinIO
- **Função:** Rastreio de experimentos ML (parâmetros, métricas, modelos Spark)
- **Volume persistente:** `ge_mlflow_data`
- **Depende de:** `ge_minio` (healthy)
- **Healthcheck:** `curl http://localhost:5000/health`

### ge_airflow_init — Inicialização Airflow (run once)
- Corre `airflow db upgrade` (migrações da DB)
- Cria utilizador admin: `admin / admin`
- Termina após inicialização

### ge_airflow_webserver — UI Airflow
- **Porta:** `8081:8080`
- **URL:** http://localhost:8081
- **Limit memória:** 1g
- **Healthcheck:** `curl http://localhost:8080/health`

### ge_airflow_scheduler — Scheduler Airflow
- Deteta e executa DAGs conforme schedule
- **Limit memória:** 3g
- **DAGs montadas:** `infrastructure/dags/` (principal) + `machine_learning/pipeline/` (subpasta `/ml`)
- **Project code montado:** `analytical_engineering/` + `data_engineering/` + `machine_learning/`
- **Responsável por dois pipelines:**
  - `trendmart_gold_pipeline` (horário — schedule imposto pela analytical_engineering): Silver → Gold → Views
  - `trendmart_ml_pipeline` (diário às 03:00 — schedule imposto pela machine_learning): Demand Forecast + Churn
- **MLFLOW_TRACKING_URI:** `http://ge_mlflow:5000` (variável de ambiente)

---

## Dockerfiles em detalhe

### Dockerfile.airflow
```
Base: apache/airflow:2.8.0-python3.11
+ apt: build-essential, libpq-dev, openjdk-17-jre-headless, curl
+ pip: requirements.airflow.txt (pyspark, trino, etc.)
+ JARs Iceberg + S3A copiados para o diretório jars do PySpark
```
Java e JARs são necessários porque o Airflow corre jobs PySpark tanto para Silver como para Gold.

### Dockerfile.hive
```
Base: apache/hive:3.1.3
+ wget: driver JDBC PostgreSQL 42.7.3
+ copia hive-site.xml para /opt/hive/conf/
```
O driver JDBC é necessário para o Hive Metastore persistir metadata no PostgreSQL.

### Dockerfile.spark
```
Base: python:3.11-slim
+ apt: openjdk-17-jre-headless, curl
+ pip: pyspark>=3.5,<4.0, pyarrow>=14.0, boto3>=1.34
+ JARs Iceberg + S3A copiados para o diretório jars do PySpark
```
Usado pelos workers Spark standalone se necessário; o pipeline principal corre no Airflow.

---

## Configurações de rede

Todos os serviços partilham a rede `ge_network` (bridge). Comunicação interna usa nomes DNS dos containers:

| Acesso interno | Acesso externo (localhost) |
|---|---|
| `ge_postgres:5432` | `localhost:5434` |
| `ge_minio:9000` / `geminio:9000` | `localhost:9004` |
| `ge_minio:9001` | `localhost:9005` (Console) |
| `ge_kafka:9092` | `localhost:29092` |
| `ge_kafka_connect:8083` | `localhost:8083` |
| `ge_hive_metastore:9083` / `gehms:9083` | `localhost:9083` |
| `ge_trino:8080` | `localhost:8085` |
| `ge_airflow_webserver:8080` | `localhost:8081` |

---

## Ordem de arranque (dependências)

```
ge_postgres (healthy)
    ├── ge_kafka_connect (depends on: kafka healthy + postgres healthy)
    │       └── ge_debezium_init (depends on: kafka_connect healthy)
    ├── ge_hive_metastore (depends on: postgres healthy + minio healthy)
    │       └── ge_trino (depends on: hive_metastore started + minio healthy)
    └── ge_airflow_init (depends on: postgres healthy)
            ├── ge_airflow_webserver
            └── ge_airflow_scheduler

ge_minio (healthy)
    ├── ge_minio_init (depends on: minio healthy)
    ├── ge_hive_metastore
    └── ge_trino
```

---

## Volumes persistentes

| Volume | Serviço | Conteúdo |
|---|---|---|
| `ge_postgres_data` | ge_postgres | Dados PostgreSQL (DBs, tabelas, WAL) |
| `ge_minio_data` | ge_minio | Todos os ficheiros Parquet e Iceberg (bronze/silver/gold/mlflow) |
| `ge_airflow_logs` | Airflow | Logs de execução de DAGs e tasks |
| `ge_trino_data` | ge_trino | Cache e dados internos Trino |
| `ge_mlflow_data` | ge_mlflow | Base de dados SQLite com runs, métricas e parâmetros MLflow |

---

## Como arrancar

```powershell
cd infrastructure
docker compose up -d --build   # primeira vez (--build para construir imagens)
docker compose up -d           # arranques seguintes
docker compose ps              # verificar estado
docker compose logs -f ge_airflow_scheduler  # ver logs do scheduler
docker compose down            # parar tudo (dados preservados nos volumes)
docker compose down -v         # parar e apagar volumes (reset completo)
```

---

## URLs de acesso

| Serviço | URL | Credenciais |
|---|---|---|
| Airflow UI | http://localhost:8081 | admin / admin |
| Dashboard | http://localhost:8050 | — |
| MLflow UI | http://localhost:5001 | — |
| MinIO Console | http://localhost:9005 | minioadmin / minioadmin |
| Trino UI | http://localhost:8085 | — |
| Kafka Connect REST | http://localhost:8083/connectors | — |
| Debezium connector | http://localhost:8083/connectors/simulated-orders-connector | — |
