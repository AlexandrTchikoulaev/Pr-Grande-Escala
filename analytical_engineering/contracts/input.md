# analytical_engineering — Input Contracts

Inputs consumed from `data_engineering`. See [data_engineering/contracts/output.md](../../data_engineering/contracts/output.md).

## Tabelas consumidas

| Table | Catalog | Description |
|-------|---------|-------------|
| `lake.silver.clickstream` | Iceberg (Hive) | Cleaned clickstream events |
| `lake.silver.orders`      | Iceberg (Hive) | Validated purchase records |
| `lake.silver.reviews`     | Iceberg (Hive) | Parsed review records with free-text message |

## Requisito de frequência (SLA)

A analytical_engineering requer que as tabelas Silver estejam actualizadas **uma vez por hora**, antes do arranque de cada run do pipeline Gold.

Este requisito é comunicado à equipa **infrastructure**, que o implementa através da DAG `trendmart_gold_pipeline` com `schedule_interval="0 * * * *"`. A infrastructure é responsável por garantir que os jobs Silver correm e completam antes dos jobs Gold no âmbito da mesma DAG.
