# Bronze — Sales (Orders + Order Items)

## O que é e para que serve

O Bronze Sales é responsável por consumir os eventos CDC do Debezium (que captura as alterações no PostgreSQL via WAL) e armazená-los em Parquet no MinIO. Guarda os dados das encomendas e itens em duas tabelas separadas, sem transformações.

---

## Ficheiro responsável

`data_engineering/ingestion/cdc_consumer.py`

---

## Fonte

- **Localização:** Kafka, dois tópicos Debezium:
  - `debezium.public.orders`
  - `debezium.public.order_items`
- **Formato:** JSON envelope do Debezium com `{before, after, op, ts_ms, source}`

### Operações CDC

| Operação | Significado |
|---|---|
| `c` | INSERT — nova encomenda |
| `u` | UPDATE — noop update (simulação de re-delivery) |
| `r` | Snapshot — leitura inicial da tabela |
| `d` | DELETE — **ignorado** |

---

## Como funciona

### 1. Ligação ao Kafka

```python
consumer = Consumer({
    "bootstrap.servers":  "localhost:29092",
    "group.id":           "de_cdc_consumer",
    "auto.offset.reset":  "earliest",
    "enable.auto.commit": True,
})
consumer.subscribe(["debezium.public.orders", "debezium.public.order_items"])
```

### 2. Parse do envelope Debezium

A função `_parse_debezium()` extrai os campos relevantes:

1. Verifica se `op` é `c`, `u` ou `r` — ignora `d`
2. Extrai os dados do campo `after` (estado da linha após a operação)
3. Identifica a tabela pelo campo `source.table`
4. Para `orders`: extrai `order_id`, `customer_id`, `session_id`, `purchase_timestamp`, `state`, `cdc_ts_ms`
5. Para `order_items`: extrai `order_item_id`, `order_id`, `product_id`, `seller_id`, `price`, `freight_value`, `category`, `cdc_ts_ms`

### 3. Dois buffers independentes

```python
orders_buf:      list[dict] = []
order_items_buf: list[dict] = []
```

Cada mensagem vai para o buffer correspondente à sua tabela. O flush acontece quando o total dos dois buffers atinge **500 registos** ou **30 segundos**.

---

## Armazenamento final

**Bucket:** `bronze`

**Paths:**
```
bronze/orders/year=YYYY/month=MM/day=DD/hour=HH/batch_<ts>.parquet
bronze/order_items/year=YYYY/month=MM/day=DD/hour=HH/batch_<ts>.parquet
```

**Estrutura de colunas — `bronze/orders/`:**

| Campo | Tipo | Descrição |
|---|---|---|
| `order_id` | STRING | ID da encomenda |
| `customer_id` | STRING | ID do cliente |
| `session_id` | STRING | ID da sessão |
| `purchase_timestamp` | LONG | Microsegundos desde epoch (formato Debezium) |
| `state` | STRING | Estado brasileiro (pode ter ruído) |
| `cdc_ts_ms` | LONG | Timestamp do evento no WAL |
| `ingested_at` | STRING | Timestamp de ingestão no Bronze |

**Estrutura de colunas — `bronze/order_items/`:**

| Campo | Tipo | Descrição |
|---|---|---|
| `order_item_id` | STRING | ID do item |
| `order_id` | STRING | FK para orders |
| `product_id` | STRING | ID do produto |
| `seller_id` | STRING | ID do seller |
| `price` | STRING | Preço (string — Debezium envia NUMERIC como string) |
| `freight_value` | STRING | Frete (string) |
| `category` | STRING | Categoria (pode ter espaços em volta) |
| `cdc_ts_ms` | LONG | Timestamp do evento no WAL |
| `ingested_at` | STRING | Timestamp de ingestão no Bronze |

---

## Ruído presente

O Bronze não filtra nada — os dados chegam com o ruído injetado pelo `noise.py`:

| Problema | Probabilidade |
|---|---|
| Preço negativo ou zero | 4% |
| `state` com casing errado ("sp"), espaços (" SP ") ou typo ("SP0") | 6% |
| `category` com espaços em volta | 5% |
| CDC re-delivery — mesmo `order_id` com `op=u` | 4% |

Os duplicados de CDC chegam ao Bronze como dois registos com o mesmo `order_id` — um com `op=c` e outro com `op=u`. A deduplicação é feita na Silver.
