# Machine Learning — Revisão Completa

## O que faz esta equipa

A equipa de Machine Learning é responsável por **treinar modelos preditivos sobre as tabelas Gold** e escrever os resultados de volta para novas tabelas Gold consumíveis pelo Dashboard e por Data Scientists via Trino.

Os modelos implementados respondem a dois requisitos explícitos do enunciado:
1. **Séries temporais para previsão de procura** — `demand_forecast.py`
2. **Deteção e prevenção de churn** — `churn_prediction.py`

O rastreio de modelos (métricas, parâmetros, artefactos) é feito via **MLflow**, acessível em `http://localhost:5001`.

---

## Estrutura de ficheiros

```
machine_learning/
├── config.py                       # Configurações (MLflow URI, thresholds, MinIO, HMS)
├── spark_session.py                # Factory SparkSession (idêntica ao AE)
├── requirements.txt                # pyspark, pyarrow, boto3, mlflow
├── features/
│   ├── timeseries_features.py      # Agregação diária + lag features para demand forecast
│   └── sales_features.py           # Features RFM + label de churn por cliente
├── models/
│   ├── demand_forecast.py          # Treino LinearRegression, avaliação, inferência
│   └── churn_prediction.py         # Treino RandomForest, avaliação, scoring completo
├── pipeline/
│   └── dag_ml_pipeline.py          # Airflow DAG diário (03:00)
└── contracts/
    ├── input.md                    # Tabelas Gold consumidas
    └── output.md                   # Tabelas ML produzidas + MLflow experiments
```

---

## Fluxo completo

```
╔══════════════════════════════════════════════════════════════════╗
║  GOLD (upstream — analytical_engineering)                         ║
║                                                                   ║
║  lake.gold.fact_sales        lake.gold.fact_reviews               ║
║  lake.gold.dim_date          lake.gold.dim_category               ║
╚═════╦══════════════════════════════╦════════════════════════════╝
      ║                              ║
      ▼                              ▼
┌─────────────────────────┐   ┌──────────────────────────────┐
│  timeseries_features.py │   │  sales_features.py           │
│                         │   │                              │
│  Agrega por             │   │  Por cliente:                │
│  (category, date):      │   │  recency = dias desde        │
│  orders_count           │   │  última compra               │
│  total_revenue          │   │  (vs max date do dataset)    │
│                         │   │                              │
│  Adiciona lag features  │   │  frequency = nº encomendas   │
│  via Window:            │   │  monetary = total gasto      │
│  lag_1, lag_7, lag_14   │   │  avg_rating (de reviews,     │
│  rolling_7d_mean        │   │  default 3.0 se sem reviews) │
│                         │   │                              │
│  Adiciona calendário:   │   │  Label:                      │
│  day_of_week, is_weekend│   │  churned = 1 se              │
│  month, quarter         │   │  recency > 30 dias           │
│                         │   │  churned = 0 caso contrário  │
│  Remove linhas sem lags │   │                              │
│  (primeiros 14 dias)    │   │                              │
└────────────┬────────────┘   └───────────────┬──────────────┘
             │                                │
             ▼                                ▼
┌─────────────────────────┐   ┌──────────────────────────────┐
│  demand_forecast.py     │   │  churn_prediction.py         │
│                         │   │                              │
│  Split cronológico:     │   │  Split aleatório 80/20       │
│  train = primeiros 80%  │   │  seed=42                     │
│  test  = últimos 20%    │   │                              │
│  (por categoria)        │   │  Pipeline:                   │
│                         │   │  VectorAssembler             │
│  VectorAssembler:       │   │  → StandardScaler            │
│  [lag_1, lag_7, lag_14, │   │  → RandomForestClassifier    │
│   rolling_7d_mean,      │   │     (numTrees=100)           │
│   day_of_week,          │   │                              │
│   is_weekend, month,    │   │  Avaliação no test set:      │
│   quarter]              │   │  F1 macro                    │
│                         │   │  AUC-ROC                     │
│  LinearRegression       │   │  Precision, Recall           │
│  (maxIter=100,          │   │                              │
│   regParam=0.1)         │   │  Score de TODOS os clientes  │
│                         │   │  (inferência full dataset)   │
│  Avaliação no test set: │   │                              │
│  RMSE, MAE              │   │                              │
│                         │   │                              │
│  Previsão D+1 a D+7     │   │                              │
│  (features geradas      │   │                              │
│  com calendário)        │   │                              │
└───┬─────────────────────┘   └──────────────┬───────────────┘
    │  ↕ MLflow (log params,                  │  ↕ MLflow (log params,
    │    métricas, modelo)                    │    métricas, importância,
    │                                         │    modelo)
    ▼                                         ▼
┌────────────────────────────┐  ┌──────────────────────────────────┐
│ lake.gold.ml_demand_       │  │ lake.gold.ml_churn_scores        │
│ forecast                   │  │                                  │
│                            │  │ customer_id                      │
│ category_id                │  │ churn_probability [0.0, 1.0]     │
│ category_en                │  │ risk_label:                      │
│ forecast_date              │  │   "high"   → prob > 0.7          │
│ predicted_orders           │  │   "medium" → prob > 0.4          │
│ model_rmse                 │  │   "low"    → prob ≤ 0.4          │
│ model_mae                  │  │ recency, frequency, monetary     │
│ scored_at                  │  │ model_f1, model_auc              │
└────────────────────────────┘  │ scored_at                        │
                                └──────────────────────────────────┘
              │                              │
              ╚══════════════╦══════════════╝
                             ║
                             ▼
             ┌───────────────────────────┐
             │  Dashboard — ML Insights  │
             │                           │
             │  Previsão de Procura      │
             │  Risco de Churn           │
             │  Métricas dos Modelos     │
             │                           │
             │  http://localhost:8050    │
             └───────────────────────────┘
```

---

## DAG Airflow (dag_ml_pipeline.py)

**DAG ID:** `trendmart_ml_pipeline`
**Schedule:** `0 3 * * *` (todos os dias às 03:00, após o Gold pipeline das 00:00)
**Catchup:** False
**Max active runs:** 1
**Retries:** 1 tentativa, delay de 10 minutos

### Grafo de dependências

```
demand_forecast ──► churn_prediction
```

Tarefas **sequenciais** pelo mesmo motivo do DAG Gold: cada task lança um JVM Spark completo dentro do scheduler container; correr em paralelo esgotaria a memória.

### Para triggerar manualmente

```bash
docker exec ge_airflow_scheduler airflow dags trigger trendmart_ml_pipeline
```

---

## Modelos em detalhe

### Modelo 1 — Previsão de Procura (séries temporais)

**Objetivo de negócio:** Prever quantas encomendas cada categoria de produto vai ter nos próximos 7 dias. Permite gestão de stock proactiva e alerta de sellers com antecedência.

**Abordagem:**
- Lag features capturam a autocorrelação temporal (o valor de hoje depende de ontem e da semana passada)
- Features de calendário capturam sazonalidade (fins de semana, meses, quarters)
- Um modelo por categoria para capturar padrões específicos de cada vertical

**Definição de "série temporal":** Em vez de ARIMA/Prophet (que requerem bibliotecas extra), usa-se regressão linear com lag features — abordagem equivalente a uma janela deslizante AR, compatível com Spark MLlib.

**Avaliação:**
| Métrica | Descrição |
|---|---|
| **RMSE** | Root Mean Squared Error — penaliza erros grandes |
| **MAE** | Mean Absolute Error — erro médio absoluto |

---

### Modelo 2 — Previsão de Churn

**Objetivo de negócio:** Identificar clientes em risco de abandonar a plataforma (não voltar a comprar). Permite campanhas de retenção direcionadas.

**Definição de churn:** Cliente sem compra nos últimos 30 dias (relativos ao `MAX(purchase_date)` do dataset, não à data atual — evita problemas com dados históricos simulados).

**Abordagem:**
- Features RFM (Recency, Frequency, Monetary) são o padrão industrial para análise de comportamento de clientes
- StandardScaler normaliza as escalas muito diferentes (recency em dias, monetary em euros)
- RandomForest é robusto a outliers e fornece importância das features

**Avaliação:**
| Métrica | Descrição |
|---|---|
| **F1 macro** | Média harmónica precision/recall, equilibrada entre classes |
| **AUC-ROC** | Área sob a curva ROC — capacidade discriminativa do modelo |
| **Precision** | Dos previstos como churned, quantos realmente churnam |
| **Recall** | Dos que realmente churnam, quantos foram detectados |

---

## MLflow — Rastreio de Modelos

**URL:** http://localhost:5001

**Experiments:**
- `demand_forecasting` — um run por categoria por execução diária
- `churn_prediction` — um run por execução diária

**O que é registado:**

| Tipo | Demand Forecast | Churn Prediction |
|---|---|---|
| Parâmetros | algorithm, reg_param, category | algorithm, num_trees, churn_threshold_days |
| Métricas | rmse, mae | f1, auc, precision, recall, importance_* |
| Artefactos | modelo Spark (MLlib Pipeline) | modelo Spark + importância das features |
| Storage | `s3://mlflow/` (MinIO) | `s3://mlflow/` (MinIO) |

---

## Configurações relevantes

```python
MLFLOW_TRACKING_URI  = "http://ge_mlflow:5000"   # URI interno Docker
CHURN_DAYS_THRESHOLD = 30                          # dias sem compra = churned
GOLD_BUCKET          = "gold"                      # bucket Iceberg output
```

---

## Schemas das tabelas ML

### lake.gold.ml_demand_forecast
| Coluna | Tipo | Descrição |
|---|---|---|
| category_id | INTEGER | FK para dim_category |
| category_en | STRING | Nome da categoria |
| forecast_date | DATE | Data prevista (D+1 a D+7) |
| predicted_orders | DOUBLE | Pedidos previstos (≥ 0) |
| model_rmse | DOUBLE | RMSE do modelo no conjunto de teste |
| model_mae | DOUBLE | MAE do modelo no conjunto de teste |
| scored_at | TIMESTAMP | Data/hora da inferência |

### lake.gold.ml_churn_scores
| Coluna | Tipo | Descrição |
|---|---|---|
| customer_id | STRING | Identificador do cliente |
| churn_probability | DOUBLE | Probabilidade de churn [0.0, 1.0] |
| risk_label | STRING | high / medium / low |
| recency | INTEGER | Dias desde a última compra |
| frequency | LONG | Número de encomendas distintas |
| monetary | DOUBLE | Total gasto (€) |
| model_f1 | DOUBLE | F1 macro do modelo |
| model_auc | DOUBLE | AUC-ROC do modelo |
| scored_at | TIMESTAMP | Data/hora do scoring |

---

## Como ver os resultados

```sql
-- Previsão de procura para os próximos 7 dias
SELECT * FROM lake.gold.ml_demand_forecast ORDER BY forecast_date, category_en;

-- Clientes em alto risco de churn
SELECT customer_id, churn_probability, recency, frequency, monetary
FROM lake.gold.ml_churn_scores
WHERE risk_label = 'high'
ORDER BY churn_probability DESC
LIMIT 50;

-- Distribuição de risco
SELECT risk_label, COUNT(*) AS total, AVG(churn_probability) AS avg_prob
FROM lake.gold.ml_churn_scores
GROUP BY risk_label
ORDER BY avg_prob DESC;
```

MLflow UI: http://localhost:5001
Airflow UI: http://localhost:8081 (admin / admin) → DAG `trendmart_ml_pipeline`
