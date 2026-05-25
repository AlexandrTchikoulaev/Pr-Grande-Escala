"""
Modelo de previsão de churn de clientes.

Algoritmo : RandomForestClassifier (Spark MLlib).
Avaliação : F1 macro, AUC-ROC, precision e recall no conjunto de teste (20% aleatório).
MLflow    : regista parâmetros, métricas, importância das features e modelo
            em experiment "churn_prediction".
Output    : lake.gold.ml_churn_scores
            (customer_id, churn_probability, risk_label,
             recency, frequency, monetary, model_f1, model_auc, scored_at)
"""

import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from machine_learning import config
from machine_learning.features.sales_features import build_churn_features

FEATURE_COLS = ["recency", "frequency", "monetary", "avg_rating"]
LABEL_COL    = "churned"
NUM_TREES    = 100


def _risk_label(prob):
    if prob > 0.7:
        return "high"
    if prob > 0.4:
        return "medium"
    return "low"


_risk_label_udf = F.udf(_risk_label)


def run(spark: SparkSession) -> int:
    mlflow.set_tracking_uri(config.MLFLOW_TRACKING_URI)
    mlflow.set_experiment("churn_prediction")

    data = build_churn_features(spark)
    data.cache()

    train, test = data.randomSplit([0.8, 0.2], seed=42)

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features_raw")
    scaler    = StandardScaler(
        inputCol="features_raw", outputCol="features",
        withMean=True, withStd=True
    )
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol=LABEL_COL,
        numTrees=NUM_TREES,
        seed=42,
    )
    pipeline = Pipeline(stages=[assembler, scaler, rf])

    with mlflow.start_run(run_name="churn_rf"):
        model = pipeline.fit(train)

        test_preds = model.transform(test)

        f1_eval = MulticlassClassificationEvaluator(
            labelCol=LABEL_COL, predictionCol="prediction", metricName="f1"
        )
        auc_eval = BinaryClassificationEvaluator(
            labelCol=LABEL_COL, rawPredictionCol="rawPrediction", metricName="areaUnderROC"
        )
        precision_eval = MulticlassClassificationEvaluator(
            labelCol=LABEL_COL, predictionCol="prediction", metricName="weightedPrecision"
        )
        recall_eval = MulticlassClassificationEvaluator(
            labelCol=LABEL_COL, predictionCol="prediction", metricName="weightedRecall"
        )

        f1        = f1_eval.evaluate(test_preds)
        auc       = auc_eval.evaluate(test_preds)
        precision = precision_eval.evaluate(test_preds)
        recall    = recall_eval.evaluate(test_preds)

        rf_model = model.stages[-1]
        importances = {
            col: float(imp)
            for col, imp in zip(FEATURE_COLS, rf_model.featureImportances.toArray())
        }

        mlflow.log_param("algorithm",  "RandomForest")
        mlflow.log_param("num_trees",  NUM_TREES)
        mlflow.log_param("churn_threshold_days", config.CHURN_DAYS_THRESHOLD)
        mlflow.log_metric("f1",        f1)
        mlflow.log_metric("auc",       auc)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall",    recall)
        for feat, imp in importances.items():
            mlflow.log_metric(f"importance_{feat}", imp)
        mlflow.spark.log_model(model, artifact_path="model")

        print(f"[churn_prediction] F1={f1:.4f}  AUC={auc:.4f}  "
              f"Precision={precision:.4f}  Recall={recall:.4f}")

    # score de todos os clientes
    all_preds = model.transform(data)

    # extrair probabilidade da classe 1 (churned)
    get_prob = F.udf(lambda v: float(v[1]))

    scores = (
        all_preds
        .withColumn("churn_probability", get_prob(F.col("probability")))
        .withColumn("risk_label", _risk_label_udf(F.col("churn_probability")))
        .select(
            F.col("customer_id"),
            F.col("churn_probability"),
            F.col("risk_label"),
            F.col("recency"),
            F.col("frequency"),
            F.col("monetary"),
            F.lit(round(f1,  4)).alias("model_f1"),
            F.lit(round(auc, 4)).alias("model_auc"),
            F.current_timestamp().alias("scored_at"),
        )
    )

    data.unpersist()

    scores.writeTo("lake.gold.ml_churn_scores") \
        .tableProperty("format-version", "2") \
        .createOrReplace()

    return scores.count()
