"""
Modelo de previsão de procura por categoria — séries temporais.

Algoritmo : LinearRegression (Spark MLlib) com lag features e features de calendário.
Avaliação : RMSE e MAE no conjunto de teste (últimos 20% das datas por categoria).
MLflow    : regista parâmetros, métricas e modelo em experiment "demand_forecasting".
Output    : lake.gold.ml_demand_forecast
            (category_id, category_en, forecast_date, predicted_orders,
             model_rmse, model_mae, scored_at)
"""

from datetime import date, timedelta

import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from machine_learning import config
from machine_learning.features.timeseries_features import build_timeseries

FEATURE_COLS = ["lag_1", "lag_7", "lag_14", "rolling_7d_mean",
                "day_of_week", "is_weekend", "month", "quarter"]
LABEL_COL    = "orders_count"
FORECAST_DAYS = 7


def _train_evaluate(train, test, category_id):
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    lr = LinearRegression(
        featuresCol="features",
        labelCol=LABEL_COL,
        maxIter=100,
        regParam=0.1,
    )
    pipeline = Pipeline(stages=[assembler, lr])
    model = pipeline.fit(train)

    predictions = model.transform(test)
    evaluator_rmse = RegressionEvaluator(
        labelCol=LABEL_COL, predictionCol="prediction", metricName="rmse"
    )
    evaluator_mae = RegressionEvaluator(
        labelCol=LABEL_COL, predictionCol="prediction", metricName="mae"
    )
    rmse = evaluator_rmse.evaluate(predictions)
    mae  = evaluator_mae.evaluate(predictions)
    return model, rmse, mae


def _make_future_rows(spark, category_id, category_en, history: dict):
    """Gera 7 linhas com features para datas futuras.

    history: {datetime.date -> float} com os últimos ~14 dias de orders_count reais,
    usado para popular lag_1, lag_7, lag_14 e rolling_7d_mean com valores reais
    em vez de 0, o que melhora significativamente a qualidade das previsões.
    """
    today = date.today()
    rows = []
    for i in range(1, FORECAST_DAYS + 1):
        d = today + timedelta(days=i)
        lag_1  = float(history.get(today + timedelta(days=i - 1),  0) or 0)
        lag_7  = float(history.get(today + timedelta(days=i - 7),  0) or 0)
        lag_14 = float(history.get(today + timedelta(days=i - 14), 0) or 0)
        rolling_vals = [
            float(history.get(today + timedelta(days=i - j), 0) or 0)
            for j in range(1, 8)
        ]
        rolling_7d_mean = sum(rolling_vals) / 7
        rows.append({
            "category_id":     category_id,
            "category_en":     category_en,
            "forecast_date":   d.isoformat(),
            "lag_1":           lag_1,
            "lag_7":           lag_7,
            "lag_14":          lag_14,
            "rolling_7d_mean": rolling_7d_mean,
            "day_of_week":     d.isoweekday(),
            "is_weekend":      float(d.isoweekday() >= 6),
            "month":           d.month,
            "quarter":         (d.month - 1) // 3 + 1,
        })
    return spark.createDataFrame(rows)


def run(spark: SparkSession) -> int:
    mlflow.set_tracking_uri(config.MLFLOW_TRACKING_URI)
    mlflow.set_experiment("demand_forecasting")

    features = build_timeseries(spark)
    features.cache()

    categories = [
        r for r in features.select("category_id", "category_en").distinct().collect()
    ]

    forecast_parts = []
    global_rmse, global_mae, n_cats = 0.0, 0.0, 0

    for row in categories:
        cat_id, cat_en = row["category_id"], row["category_en"]
        cat_data = features.filter(F.col("category_id") == cat_id).orderBy("purchase_date")

        count = cat_data.count()
        if count < 20:
            continue

        split_idx  = int(count * 0.8)
        split_date = cat_data.orderBy("purchase_date").limit(split_idx).agg(
            F.max("purchase_date")
        ).collect()[0][0]

        train = cat_data.filter(F.col("purchase_date") <= split_date)
        test  = cat_data.filter(F.col("purchase_date") >  split_date)

        if test.count() == 0:
            continue

        with mlflow.start_run(run_name=f"demand_{cat_en}", nested=True):
            model, rmse, mae = _train_evaluate(train, test, cat_id)
            mlflow.log_param("category",  cat_en)
            mlflow.log_param("algorithm", "LinearRegression")
            mlflow.log_param("reg_param", 0.1)
            mlflow.log_metric("rmse", rmse)
            mlflow.log_metric("mae",  mae)
            mlflow.spark.log_model(model, artifact_path="model")

        global_rmse += rmse
        global_mae  += mae
        n_cats      += 1

        # construir histórico recente para popular os lag features das datas futuras
        recent = cat_data.orderBy("purchase_date").tail(14)
        history = {row["purchase_date"]: row["orders_count"] for row in recent}

        future_rows = _make_future_rows(spark, cat_id, cat_en, history)
        assembler   = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
        future_feat = assembler.transform(future_rows)
        preds = model.transform(future_feat)

        part = preds.select(
            F.col("category_id"),
            F.col("category_en"),
            F.to_date(F.col("forecast_date")).alias("forecast_date"),
            F.greatest(F.col("prediction"), F.lit(0.0)).cast("double").alias("predicted_orders"),
            F.lit(round(rmse, 4)).alias("model_rmse"),
            F.lit(round(mae,  4)).alias("model_mae"),
            F.current_timestamp().alias("scored_at"),
        )
        forecast_parts.append(part)

    features.unpersist()

    if not forecast_parts:
        print("[demand_forecast] Sem dados suficientes para treinar.")
        return 0

    avg_rmse = global_rmse / n_cats
    avg_mae  = global_mae  / n_cats
    print(f"[demand_forecast] {n_cats} categorias treinadas — RMSE médio: {avg_rmse:.2f}, MAE médio: {avg_mae:.2f}")

    forecast = forecast_parts[0]
    for part in forecast_parts[1:]:
        forecast = forecast.union(part)

    forecast.writeTo("lake.gold.ml_demand_forecast") \
        .tableProperty("format-version", "2") \
        .createOrReplace()

    return forecast.count()
