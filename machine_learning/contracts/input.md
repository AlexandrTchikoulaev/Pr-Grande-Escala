# Machine Learning — Input Contract

## Fonte de dados

Todas as tabelas lidas são Gold (Iceberg, catálogo `lake`), produzidas pela equipa de Analytical Engineering.

## Tabelas consumidas

### `lake.gold.fact_sales`
| Coluna | Tipo | Usado em |
|---|---|---|
| `customer_id` | string | churn features |
| `order_id` | string | churn features (frequency) |
| `product_id` | string | — |
| `category_id` | integer | demand features |
| `date_id` | integer | demand features (join dim_date) |
| `total_value` | double | churn (monetary), demand (revenue) |
| `purchase_date` | date | churn (recency), demand (série temporal) |

### `lake.gold.fact_reviews`
| Coluna | Tipo | Usado em |
|---|---|---|
| `customer_id` | string | churn features (avg_rating) |
| `rating` | integer | churn features |

### `lake.gold.dim_category`
| Coluna | Tipo | Usado em |
|---|---|---|
| `category_id` | integer | demand features (join) |
| `category_en` | string | demand features (label legível) |

### `lake.gold.dim_date`
| Coluna | Tipo | Usado em |
|---|---|---|
| `date_id` | integer | demand features (join) |
| `day_of_week` | integer | demand features (calendário) |
| `is_weekend` | boolean | demand features (calendário) |
| `month` | integer | demand features (calendário) |
| `quarter` | integer | demand features (calendário) |

## Pré-requisitos
- As tabelas Gold devem existir (pipeline `trendmart_gold_pipeline` executado pelo menos uma vez)
- Mínimo de 20 dias de dados por categoria para treinar o modelo de demand forecast
- O serviço MLflow (`ge_mlflow`) deve estar activo antes da execução do DAG
