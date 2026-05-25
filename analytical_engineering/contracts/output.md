# analytical_engineering — Output Contracts

Gold Iceberg tables and Trino views exposed to downstream consumers
(BI, Data Science, Machine Learning).

---

## Fact Tables

### lake.gold.fact_clickstream
| Column      | Type      | Description                        |
|-------------|-----------|------------------------------------|
| event_id    | STRING    | Unique event identifier            |
| session_id  | STRING    | User session                       |
| user_id     | STRING    | Customer ID                        |
| event_type  | STRING    | Event name                         |
| event_ts    | TIMESTAMP | Event timestamp                    |
| event_date  | DATE      | Partition key                      |
| device      | STRING    | mobile / desktop / tablet          |
| category    | STRING    | Product category                   |
| product_id  | STRING    | Product (nullable)                 |
| price       | DOUBLE    | Product price (nullable)           |
| location    | STRING    | State or city                      |

### lake.gold.fact_sales
| Column        | Type      | Description                      |
|---------------|-----------|----------------------------------|
| order_id      | STRING    | Unique order identifier          |
| session_id    | STRING    | Originating session              |
| customer_id   | STRING    | Customer identifier              |
| product_id    | STRING    | Product identifier               |
| seller_id     | STRING    | Seller identifier                |
| category      | STRING    | Product category                 |
| price         | DOUBLE    | Product price                    |
| freight_value | DOUBLE    | Freight cost                     |
| total_value   | DOUBLE    | price + freight                  |
| purchase_ts   | TIMESTAMP | Purchase timestamp               |
| purchase_date | DATE      | Partition key                    |
| state         | STRING    | Customer state (BR)              |
| region        | STRING    | Brazilian region                 |

### lake.gold.fact_reviews
| Column      | Type      | Description                        |
|-------------|-----------|------------------------------------|
| review_id   | STRING    | Unique review identifier           |
| order_id    | STRING    | Associated order                   |
| customer_id | STRING    | Customer                           |
| rating      | INTEGER   | Score 1–5                          |
| sentiment   | STRING    | positive / neutral / negative      |
| title       | STRING    | Review title                       |
| message     | STRING    | Free text (unstructured)           |
| text_length | INTEGER   | Word count                         |
| review_date | DATE      | Partition key                      |

---

## Dimension Tables

| Table                  | Key columns                       |
|------------------------|-----------------------------------|
| `lake.gold.dim_date`   | date_id, date_actual, year, month, quarter, is_weekend |
| `lake.gold.dim_category` | category_id, category_en        |
| `lake.gold.dim_geography` | state, state_name, region      |

---

## Trino Views

| View                    | Purpose                                     |
|-------------------------|---------------------------------------------|
| `lake.gold.vw_executive`       | Daily KPIs: revenue, orders, avg rating |
| `lake.gold.vw_sales_performance` | Sales by category, region, state      |
| `lake.gold.vw_funnel`          | Clickstream event funnel analysis       |
| `lake.gold.vw_reviews`         | Sentiment by category over time         |
