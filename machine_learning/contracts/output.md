# Machine Learning — Output Contract

## Tabelas produzidas

### `lake.gold.ml_demand_forecast`
Previsão de procura para os próximos 7 dias, por categoria.

| Coluna | Tipo | Descrição |
|---|---|---|
| `category_id` | integer | FK para dim_category |
| `category_en` | string | Nome da categoria (inglês) |
| `forecast_date` | date | Data prevista (D+1 a D+7) |
| `predicted_orders` | double | Número de pedidos previstos |
| `model_rmse` | double | RMSE do modelo no conjunto de teste |
| `model_mae` | double | MAE do modelo no conjunto de teste |
| `scored_at` | timestamp | Data/hora da última inferência |

- Particionamento: nenhum (tabela pequena — máx. 7 × nº categorias linhas)
- Escrita: `createOrReplace` (substituída a cada execução do DAG)

### `lake.gold.ml_churn_scores`
Score de churn para cada cliente.

| Coluna | Tipo | Descrição |
|---|---|---|
| `customer_id` | string | Identificador do cliente |
| `churn_probability` | double | Probabilidade de churn [0.0, 1.0] |
| `risk_label` | string | `"high"` (>0.7) / `"medium"` (>0.4) / `"low"` |
| `recency` | integer | Dias desde a última compra |
| `frequency` | long | Número de pedidos distintos |
| `monetary` | double | Total gasto (€) |
| `model_f1` | double | F1 macro do modelo no conjunto de teste |
| `model_auc` | double | AUC-ROC do modelo no conjunto de teste |
| `scored_at` | timestamp | Data/hora da última inferência |

- Particionamento: nenhum
- Escrita: `createOrReplace` (substituída a cada execução do DAG)

## MLflow (artefactos)
- Experiment `demand_forecasting`: um run por categoria por execução; regista RMSE, MAE e modelo Spark
- Experiment `churn_prediction`: um run por execução; regista F1, AUC, precision, recall, importância das features e modelo Spark
- Artefactos guardados em MinIO: `s3://mlflow/`

## Consumidores
- Dashboard (`dashboard/app.py`) — tab "ML Insights"
- Analytical Engineering — as tabelas `ml_*` podem ser usadas em views Trino futuras
