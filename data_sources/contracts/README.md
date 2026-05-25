# data_sources — Output Contracts

This section simulates external systems producing data.
The `data_engineering` team consumes these outputs.

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

> `order_placed` is **not** published to Kafka — it goes to PostgreSQL.

---

## Output 2 — PostgreSQL Table: `simulated_orders`

**Database:** `olist_db`  
**Writer:** `simulator/db_writer.py`

### Schema

| Column               | Type         | Description                        |
|----------------------|--------------|------------------------------------|
| order_id             | TEXT (PK)    | Generated UUID                     |
| session_id           | TEXT         | Session that originated the order  |
| customer_id          | TEXT         | Olist customer_id                  |
| product_id           | TEXT         | Olist product_id                   |
| seller_id            | TEXT         | Olist seller_id                    |
| category             | TEXT         | Product category (English)         |
| price                | NUMERIC(10,2)| Product price                      |
| freight_value        | NUMERIC(10,2)| Freight cost                       |
| purchase_timestamp   | TIMESTAMP    | UTC timestamp of the purchase      |
| state                | TEXT         | Customer state (BR)                |

---

## Output 3 — MinIO Bucket: `raw-reviews`

**Format:** Plain text (.txt)  
**Writer:** `simulator/review_writer.py`

### File path convention

```
raw-reviews/{YYYY-MM-DD}/{order_id}.txt
```

### File content

```
REVIEW_ID: <uuid>
ORDER_ID: <uuid>
CUSTOMER_ID: <string>
RATING: <1-5>/5
TIMESTAMP: <ISO 8601>
---
TITLE: <optional free text>

<review message — free text, unstructured>
```
