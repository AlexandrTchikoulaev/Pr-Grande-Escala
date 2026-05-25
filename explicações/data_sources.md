# Data Sources — Revisão Completa

## O que faz esta equipa

A equipa de Data Sources é responsável por **simular um sistema de e-commerce real**, gerando dados contínuos para três destinos distintos. É a origem de todos os dados do pipeline TrendMart.

Não há dados reais — tudo é simulado a partir dos CSVs históricos do dataset Olist (e-commerce brasileiro).

---

## Estrutura de ficheiros

```
data_sources/
├── main.py                  # Entry point — loop principal do simulador
├── requirements.txt         # confluent-kafka, psycopg2-binary, boto3
├── simulator/
│   ├── config.py            # Todas as configurações e probabilidades
│   ├── loader.py            # Carrega CSVs Olist para memória
│   ├── session.py           # Simula uma sessão completa de utilizador
│   ├── noise.py             # Injeção controlada de ruído nos 3 tipos de dados
│   ├── producer.py          # Publica eventos no Kafka
│   ├── db_writer.py         # Escreve compras no PostgreSQL
│   └── review_writer.py     # Escreve reviews no MinIO (.txt)
└── contracts/
    └── README.md            # Contratos de output para data_engineering
```

---

## Fluxo completo

```
╔══════════════════════════════════════════════════════════════════╗
║                     OLIST DATASET (CSVs)                         ║
║                                                                  ║
║  olist_products_dataset.csv     → pool de produtos               ║
║  olist_customers_dataset.csv    → pool de clientes               ║
║  olist_order_items_dataset.csv  → preços e sellers               ║
║  olist_order_reviews_dataset.csv → textos de reviews reais       ║
║  product_category_name_translation.csv → PT→EN categorias        ║
╚══════════════════════╦═══════════════════════════════════════════╝
                       ║
                       ▼
              ┌─────────────────┐
              │   loader.py     │
              │                 │
              │  Lê CSVs uma vez│
              │  na inicialização│
              │                 │
              │  Constrói pools:│
              │  - products[]   │
              │  - customers[]  │
              │  - reviews[]    │
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────┐
              │    main.py      │  ← loop: 1 sessão a cada 2s (0.5/s)
              │                 │
              │  Chama          │
              │  simulate_session│
              │  em loop        │
              │                 │
              │  Distribui      │
              │  resultados por │
              │  3 destinos     │
              └────────┬────────┘
                       │
         ┌─────────────┼──────────────┐
         ▼             ▼              ▼
┌──────────────┐ ┌──────────┐ ┌────────────────┐
│  session.py  │ │          │ │                │
│              │ │          │ │                │
│  Simula o    │ │          │ │                │
│  funnel:     │ │          │ │                │
│              │ │          │ │                │
│  session_start│ │          │ │                │
│     ↓        │ │          │ │                │
│  search /    │ │          │ │                │
│  category    │ │          │ │                │
│     ↓ (70%)  │ │          │ │                │
│  product_view│ │          │ │                │
│     ↓ (40%)  │ │          │ │                │
│  review_read │ │          │ │                │
│     ↓ (35%)  │ │          │ │                │
│  add_to_cart │ │          │ │                │
│     ↓        │ │          │ │                │
│  cart_view   │ │          │ │                │
│     ↓ (55%)  │ │          │ │                │
│  checkout    │ │          │ │                │
│     ↓ (80%)  │ │          │ │                │
│  order_placed│ │          │ │                │
│     ↓ (40%)  │ │          │ │                │
│  review_submit│ │         │ │                │
│              │ │          │ │                │
│  Retorna:    │ │          │ │                │
│  events[]    ├─►producer  │ │                │
│  purchase────┼─┼──────────►db_writer         │
│  review──────┼─┼──────────┼──────────────►review_writer│
└──────────────┘ └──────────┘ └────────────────┘
         │             │              │
         ▼             ▼              ▼
  ┌────────────┐ ┌──────────┐ ┌────────────────┐
  │   Kafka    │ │PostgreSQL│ │    MinIO       │
  │            │ │          │ │                │
  │  topic:    │ │  table:  │ │  bucket:       │
  │  clickstream│ │simulated_│ │  raw-reviews/  │
  │  _events   │ │orders    │ │                │
  │            │ │          │ │  YYYY-MM-DD/   │
  │  JSON      │ │  INSERT  │ │  order_id.txt  │
  │  por evento│ │  ON      │ │                │
  │  (exceto   │ │  CONFLICT│ │  Ficheiro .txt │
  │  order_    │ │  DO      │ │  com header    │
  │  placed)   │ │  NOTHING │ │  estruturado + │
  │            │ │          │ │  body livre    │
  └────────────┘ └──────────┘ └────────────────┘
```

---

## Funnel de sessão (session.py)

Cada sessão simulada percorre este funil com probabilidades configuráveis:

```
session_start
    │
    ├─ 60% → search (query=categoria)
    └─ 40% → category_browse
         │
         ├─ 30% bounce → session_end (reason=bounce)
         └─ 70% → product_view
              │
              ├─ 40% → product_review_read (opcional)
              │
              ├─ 65% sem carrinho → session_end (reason=no_cart)
              └─ 35% → add_to_cart
                   │
                   ├─ 15% → remove_from_cart → add_to_cart (re-add)
                   │
                   └─ → cart_view
                        │
                        ├─ 45% → cart_abandon → session_end
                        └─ 55% → checkout_start
                             │
                             ├─ 20% → session_end (reason=checkout_abandon)
                             └─ 80% → order_placed ──► PostgreSQL
                                  │
                                  └─ session_end (reason=completed)
                                       │
                                       └─ 40% → review_submitted ──► MinIO
```

**Dispositivos:** mobile 55% | desktop 35% | tablet 10%

---

## Descrição de cada ficheiro

### main.py
- Entry point do simulador
- Inicializa todas as conexões (Kafka producer, PostgreSQL, MinIO)
- Loop infinito: chama `simulate_session()` → aplica ruído via `noise.py` → distribui pelos 3 writers
- Taxa: 0.5 sessões/segundo (1 sessão a cada 2 segundos)
- Para compras: chama `noop_update_purchase` quando `noise.dirty_purchase` sinaliza CDC duplicate
- Para reviews: chama `write_review_duplicate` quando `noise.dirty_review` sinaliza submissão dupla
- Report a cada 10 sessões: contadores de sessões, eventos, compras, reviews
- Shutdown gracioso com SIGINT/SIGTERM (flush Kafka + fechar DB)

### simulator/config.py
- Centraliza todos os endereços de serviços e parâmetros de simulação
- `OLIST_DATA_PATH` aponta para os CSVs do `Projeto-Grande-Escala`
- Probabilidades do funil configuráveis por variável de ambiente
- Delays entre eventos: 2-15 segundos (simula comportamento humano)
- Delay de review pós-compra: 30-300 segundos

### simulator/loader.py
- Lê 5 CSVs do Olist na inicialização (apenas uma vez)
- Constrói índices: preço/seller/freight por `product_id`
- Filtra produtos sem categoria ou sem preço de referência
- Faz parse das traduções PT→EN de categorias
- Filtra reviews sem texto (apenas reviews com `review_comment_message`)
- Expõe `pick_product()` e `pick_customer()` (random.choice)
- **Fix aplicado:** encoding `utf-8-sig` para lidar com BOM nos CSVs

### simulator/session.py
- Simula uma sessão completa de um utilizador
- Gera UUIDs únicos para `session_id`, `event_id`, `order_id`, `review_id`
- Cada evento tem: `event_id`, `session_id`, `user_id`, `event_type`, `timestamp`, `device`, `properties`
- `order_placed` é gerado mas **não** vai para Kafka — vai para PostgreSQL
- Review tem delay simulado (timestamp futuro), mas é escrita imediatamente
- Retorna tupla: `(events[], purchase | None, review | None)`

### simulator/producer.py
- Publica eventos no tópico Kafka `clickstream_events`
- Exclui explicitamente `order_placed` (vai para PostgreSQL)
- Usa `session_id` como chave Kafka (garante ordenação por sessão na mesma partição)
- Callback de delivery para logging de falhas
- `flush()` chamado no shutdown para garantir entrega de mensagens pendentes

### simulator/db_writer.py
- Insere compras na tabela `simulated_orders` do PostgreSQL (`olist_db`)
- `autocommit=False` — commit após cada INSERT
- `ON CONFLICT (order_id) DO NOTHING` — idempotente
- `noop_update_purchase()` — faz `UPDATE SET state=state` para gerar um evento CDC `u` sem alterar dados; simula re-delivery do Debezium
- `ensure_table()` cria a tabela se não existir (redundante com `init-db.sh`)

### simulator/review_writer.py
- Escreve reviews como ficheiros `.txt` no MinIO bucket `raw-reviews`
- Path: `raw-reviews/{YYYY-MM-DD}/{order_id}.txt`
- Formato do ficheiro:
  ```
  REVIEW_ID: <uuid>
  ORDER_ID: <uuid>
  CUSTOMER_ID: <string>
  RATING: <1-5>/5
  TIMESTAMP: <ISO 8601>
  ---
  TITLE: <texto opcional>

  <mensagem livre — fonte não estruturada>
  ```
- `write_review_duplicate()` — escreve o mesmo conteúdo com key `{YYYY-MM-DD}/{review_id}-dup.txt`; como o `file_watcher` rastreia por key S3, este ficheiro é processado independentemente, resultando em dois registos Bronze com o mesmo `review_id`
- Cria o bucket se não existir

### simulator/noise.py
Módulo de injeção controlada de ruído — simula problemas reais de qualidade de dados em cada tipo de fonte. É aplicado em `main.py` antes de escrever para qualquer destino.

| Fonte | Problema simulado | Probabilidade |
|---|---|---|
| **Reviews** | Rating inválido (`0`, `6`, `-1`, `N/A`) | 5% |
| **Reviews** | Campo em branco (`review_id`, `order_id`, `customer_id`) | 4% |
| **Reviews** | Mensagem vazia / whitespace | 3% |
| **Reviews** | Corrupção de encoding (UTF-8 lido como Latin-1) | 7% |
| **Reviews** | Submissão duplicada (dois ficheiros, mesmo `review_id`) | 5% |
| **Orders** | Price negativo ou zero | 4% |
| **Orders** | State com casing errado (`"sp"`) ou espaços (`" SP "`) ou typo (`"SP0"`) | 6% |
| **Orders** | Category com espaços em volta | 5% |
| **Orders** | CDC re-delivery (noop UPDATE → evento `u` duplicado) | 4% |
| **Clickstream** | `session_id` nulo | 3% |
| **Clickstream** | Device inválido (`"bot"`, `"crawler"`, `"tv"`) | 4% |
| **Clickstream** | `event_type` nulo | 2% |

A justificação por fonte é intencional: **reviews** são texto humano (mais suscetível a erros), **orders** vêm de uma BD estruturada (ruído mais pontual), **clickstream** é gerado por browsers (mínimo ruído).

---

## Os 3 outputs (contratos para data_engineering)

| Output | Destino | Formato | Conteúdo |
|---|---|---|---|
| **Clickstream** | Kafka `clickstream_events` | JSON | Todos os eventos exceto `order_placed` |
| **Compras** | PostgreSQL `olist_db.simulated_orders` | SQL INSERT | Dados estruturados da encomenda |
| **Reviews** | MinIO `raw-reviews/` | `.txt` | Header estruturado + body livre |

---

## Configurações relevantes

```
KAFKA_BOOTSTRAP      = localhost:29092
CLICKSTREAM_TOPIC    = clickstream_events

DB_HOST              = localhost
DB_PORT              = 5434
DB_NAME              = olist_db

MINIO_ENDPOINT       = localhost:9004
REVIEWS_BUCKET       = raw-reviews

SESSIONS_PER_SECOND  = 0.5
DEVICES              = mobile(55%), desktop(35%), tablet(10%)

P_SEARCH             = 0.60   (search vs category_browse)
P_PRODUCT_VIEW       = 0.70   (ver produto após navegação)
P_REVIEW_READ        = 0.40   (ler reviews do produto)
P_ADD_TO_CART        = 0.35   (adicionar ao carrinho)
P_REMOVE_FROM_CART   = 0.15   (remover do carrinho antes de re-adicionar)
P_CART_ABANDON       = 0.45   (abandonar carrinho)
P_ORDER_PLACED       = 0.80   (completar checkout)
P_REVIEW_SUBMIT      = 0.40   (submeter review após compra)

EVENT_DELAY_MIN      = 2s
EVENT_DELAY_MAX      = 15s
REVIEW_DELAY_MIN     = 30s
REVIEW_DELAY_MAX     = 300s
```

---

## Como correr

```powershell
cd data_sources
pip install -r requirements.txt
python main.py
```

Requer infraestrutura a correr (PostgreSQL, Kafka, MinIO).
