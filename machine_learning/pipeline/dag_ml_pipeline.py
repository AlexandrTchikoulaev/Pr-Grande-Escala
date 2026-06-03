"""
TrendMart — Machine Learning DAG

Schedule : diário às 03:00 (depois do trendmart_gold_pipeline das 00:00)
Executa o modelo de demand forecast sobre as tabelas Gold e escreve os
resultados para lake.gold.ml_demand_forecast.
"""

import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner":           "machine_learning",
    "retries":         1,
    "retry_delay":     timedelta(minutes=10),
    "depends_on_past": False,
}

with DAG(
    dag_id="trendmart_ml_pipeline",
    description="Treino e inferência do modelo de demand forecast — diário",
    schedule_interval="0 3 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["machine_learning", "trendmart"],
) as dag:

    def _get_spark():
        from machine_learning.spark_session import get_spark
        return get_spark("trendmart_ml_pipeline")

    def task_demand_forecast(**ctx):
        from machine_learning.models.demand_forecast import run
        t0    = time.time()
        spark = _get_spark()
        count = run(spark)
        spark.stop()
        duration = round(time.time() - t0, 1)
        ctx["ti"].xcom_push(key="demand_count",    value=count)
        ctx["ti"].xcom_push(key="demand_duration", value=duration)
        print(f"[dag_ml] demand_forecast: {count} linhas em {duration}s")

    PythonOperator(
        task_id="demand_forecast",
        python_callable=task_demand_forecast,
    )
