# data_sources — Output Contracts

O `data_sources` é um **sistema simulador** que substitui a plataforma de e-commerce real. Não é uma equipa de dados — é a fonte de dados que, numa empresa real, seria o produto em produção.

## Percurso de dados

**Entrada:** CSVs Olist (referência estática — produtos, clientes, reviews)

**Processamento:** Simulação de sessões (`session.py`) + injeção de ruído intencional (`noise.py`)

**Fim de responsabilidade:** dados entregues nos 3 pontos finais abaixo. A partir daí, o `data_engineering` toma conta.

## Ruído intencional

O contrato de saída não é "dados limpos" — é **dados realistas com defeitos conhecidos**:

| Stream | Taxa de ruído | Tipos de erros injetados |
|--------|--------------|--------------------------|
| Kafka (clickstream) | ~9% | `session_id` nulo, `device` inválido ("bot", "crawler"), `event_type` nulo |
| PostgreSQL (orders) | ~19% | Preços negativos/zero, estados malformados (`" SP "`, `"sp"`), duplicados CDC (noop UPDATE) |
| MinIO (reviews) | ~24% | Ratings inválidos (0, 6, "N/A"), encoding UTF-8 corrompido (`ã→Ã£`), campos em branco, ficheiros duplicados |

---

---

## Output 1 — Kafka Topic: `clickstream_events`

**Format:** JSON  
**Producer:** `simulator/producer.py`

### Schema

```json
{
  "event_id":   "uuid",
  "session_id": "uuid",
  "user_id":    "customer_id (string)",
  "event_type": "string",
  "timestamp":  "ISO 8601 UTC",
  "device":     "mobile | desktop | tablet",
  "properties": { ... }
}
```

### Event types and their properties

| event_type            | properties                                          |
|-----------------------|-----------------------------------------------------|
| `session_start`       | state, city                                         |
| `session_end`         | reason (bounce / no_cart / cart_abandon / checkout_abandon / completed) |
| `category_browse`     | category                                            |
| `search`              | query, category                                     |
| `product_view`        | product_id, category, price, photos_qty             |
| `product_review_read` | product_id, category                                |
| `add_to_cart`         | product_id, category, price                         |
| `remove_from_cart`    | product_id                                          |
| `cart_view`           | product_id, total                                   |
| `cart_abandon`        | product_id                                          |
| `checkout_start`      | product_id, total                                   |
| `order_placed`        | order_id, product_id, total                         |

---

## Output 2 — PostgreSQL Relational Schema

**Database:** `Amazon_Sales`  
**Writer:** `simulator/db_writer.py`

Schema relacional com 5 tabelas. As tabelas de dimensão (`customers`, `sellers`, `products`) são populadas organicamente a cada compra via `INSERT ... ON CONFLICT DO NOTHING`, usando dados já presentes nos pools Olist carregados no arranque. As tabelas de factos (`orders`, `order_items`) recebem um registo por compra.

### `customers`
| Column      | Type      | Description              |
|-------------|-----------|--------------------------|
| customer_id | TEXT (PK) | Olist customer_id        |
| state       | TEXT      | Customer state (BR)      |
| city        | TEXT      | Customer city            |

### `sellers`
| Column    | Type      | Description       |
|-----------|-----------|-------------------|
| seller_id | TEXT (PK) | Olist seller_id   |

### `products`
| Column     | Type      | Description                    |
|------------|-----------|--------------------------------|
| product_id | TEXT (PK) | Olist product_id               |
| category   | TEXT      | Product category (English)     |
| photos_qty | INTEGER   | Number of product photos       |

### `orders`
| Column             | Type         | Description                       |
|--------------------|--------------|-----------------------------------|
| order_id           | TEXT (PK)    | Generated UUID                    |
| customer_id        | TEXT (FK)    | → customers.customer_id           |
| session_id         | TEXT         | Session that originated the order |
| purchase_timestamp | TIMESTAMP    | UTC timestamp of the purchase     |
| state              | TEXT         | Customer state (BR) — with noise  |

### `order_items`
| Column        | Type          | Description                    |
|---------------|---------------|--------------------------------|
| order_item_id | TEXT (PK)     | Generated UUID                 |
| order_id      | TEXT (FK)     | → orders.order_id              |
| product_id    | TEXT (FK)     | → products.product_id          |
| seller_id     | TEXT (FK)     | → sellers.seller_id            |
| price         | NUMERIC(10,2) | Product price — with noise     |
| freight_value | NUMERIC(10,2) | Freight cost                   |
| category      | TEXT          | Product category — with noise  |

---

## Output 3 — MinIO Bucket: `raw-reviews`

**Format:** Plain text (.txt)  
**Writer:** `simulator/review_writer.py`

### File path convention

```
raw-reviews/{YYYY-MM-DD}/{review_id}_{order_id}.txt
```

> Ficheiros duplicados (ruído) são escritos com sufixo `_dup`: `{review_id}_{order_id}_dup.txt`

### File content

Texto narrativo em português gerado pelo simulador. Não tem cabeçalhos estruturados — toda a metadata (review_id, order_id) está **apenas no nome do ficheiro**.

```
Boa tarde, o meu nome é João Silva e venho partilhar a minha opinião
sobre um produto da categoria electronics.
Excelente produto, chegou rápido.
Dou 5 estrelas em 5.
```
