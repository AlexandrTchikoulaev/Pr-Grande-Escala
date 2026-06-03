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

## MLflow (artefactos)
- Experiment `demand_forecasting`: um run por categoria por execução; regista RMSE, MAE e modelo Spark
- Artefactos guardados em MinIO: `s3://mlflow/`

## Consumidores
- Dashboard (`data_analytics/dashboard/app.py`) — tab "ML Insights"
- Analytical Engineering — a tabela `ml_demand_forecast` pode ser usada em views Trino futuras
