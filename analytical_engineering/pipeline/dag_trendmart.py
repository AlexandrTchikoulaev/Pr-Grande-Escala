"""
TrendMart — Analytical Engineering DAG

Schedule: hourly  (every hour at minute 0)
Runs Bronze → Silver → Gold Spark batch jobs, then refreshes Trino views.
At the end of each successful run writes a Markdown report to
analytical_engineering/relatórios/batch_YYYY-MM-DD_HH-MM.md

Dependency graph:
    silver_clickstream ──┐
    silver_orders      ──┼──► gold_dimensions ──┬──► gold_clickstream ──┐
    silver_reviews     ──┘                      │    gold_sales       ──┼──► init_views
                                                └──► gold_reviews     ──┘
"""

import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner":            "analytical_engineering",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "depends_on_past":  False,
}

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="trendmart_gold_pipeline",
    description="Bronze → Silver → Gold batch + Trino views (hourly)",
    schedule_interval="0 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["analytical_engineering", "silver", "gold", "trendmart"],
) as dag:

    # ── Shared Spark session (reused across fact tasks) ───────────────────
    def _get_shared_spark():
        from transformations.spark_session import get_spark
        return get_spark("trendmart_gold_pipeline")

    # ── Silver: clickstream ───────────────────────────────────────────────
    def task_silver_clickstream(**ctx):
        from transformation.silver_clickstream import run
        t0 = time.time()
        count = run()
        duration = round(time.time() - t0, 1)
        ctx["ti"].xcom_push(key="silver_clickstream_count",    value=count)
        ctx["ti"].xcom_push(key="silver_clickstream_duration", value=duration)

    # ── Silver: orders ────────────────────────────────────────────────────
    def task_silver_orders(**ctx):
        from transformation.silver_orders import run
        t0 = time.time()
        count = run()
        duration = round(time.time() - t0, 1)
        ctx["ti"].xcom_push(key="silver_orders_count",    value=count)
        ctx["ti"].xcom_push(key="silver_orders_duration", value=duration)

    # ── Silver: reviews ───────────────────────────────────────────────────
    def task_silver_reviews(**ctx):
        from transformation.silver_reviews import run
        t0 = time.time()
        count = run()
        duration = round(time.time() - t0, 1)
        ctx["ti"].xcom_push(key="silver_reviews_count",    value=count)
        ctx["ti"].xcom_push(key="silver_reviews_duration", value=duration)

    # ── Dimensions ────────────────────────────────────────────────────────
    def task_gold_dimensions(**ctx):
        from transformations.gold_dimensions import run
        spark = _get_shared_spark()
        t0 = time.time()
        run(spark)
        ctx["ti"].xcom_push(key="gold_dimensions_duration", value=round(time.time() - t0, 1))

    # ── Fact: clickstream ────────────────────────────────────────────────
    def task_gold_clickstream(**ctx):
        from transformations.gold_clickstream import run
        spark = _get_shared_spark()
        t0 = time.time()
        count = run(spark)
        duration = round(time.time() - t0, 1)
        ctx["ti"].xcom_push(key="gold_clickstream_count",    value=count)
        ctx["ti"].xcom_push(key="gold_clickstream_duration", value=duration)

    # ── Fact: sales ──────────────────────────────────────────────────────
    def task_gold_sales(**ctx):
        from transformations.gold_sales import run
        spark = _get_shared_spark()
        t0 = time.time()
        count = run(spark)
        duration = round(time.time() - t0, 1)
        ctx["ti"].xcom_push(key="gold_sales_count",    value=count)
        ctx["ti"].xcom_push(key="gold_sales_duration", value=duration)

    # ── Fact: reviews ────────────────────────────────────────────────────
    def task_gold_reviews(**ctx):
        from transformations.gold_reviews import run
        spark = _get_shared_spark()
        t0 = time.time()
        count = run(spark)
        duration = round(time.time() - t0, 1)
        ctx["ti"].xcom_push(key="gold_reviews_count",    value=count)
        ctx["ti"].xcom_push(key="gold_reviews_duration", value=duration)

    # ── Trino views + batch report ───────────────────────────────────────
    def task_init_views(**ctx):
        from views.init_views import run

        ti = ctx["ti"]
        counts = {
            "clickstream": ti.xcom_pull(key="gold_clickstream_count", task_ids="gold_clickstream"),
            "sales":       ti.xcom_pull(key="gold_sales_count",       task_ids="gold_sales"),
            "reviews":     ti.xcom_pull(key="gold_reviews_count",     task_ids="gold_reviews"),
        }
        print(f"[dag] Gold counts: {counts}")

        t0 = time.time()
        run()
        views_duration = round(time.time() - t0, 1)

        # Collect task info for the batch report
        _task_meta = [
            ("silver_clickstream", "silver_clickstream_count"),
            ("silver_orders",      "silver_orders_count"),
            ("silver_reviews",     "silver_reviews_count"),
            ("gold_dimensions",    None),
            ("gold_clickstream",   "gold_clickstream_count"),
            ("gold_sales",         "gold_sales_count"),
            ("gold_reviews",       "gold_reviews_count"),
        ]
        tasks_info = [
            {
                "name":       task_id,
                "status":     "SUCCESS",
                "duration_s": ti.xcom_pull(key=f"{task_id}_duration", task_ids=task_id),
                "records":    ti.xcom_pull(key=count_key, task_ids=task_id) if count_key else None,
            }
            for task_id, count_key in _task_meta
        ]
        tasks_info.append({
            "name":       "init_views",
            "status":     "SUCCESS",
            "duration_s": views_duration,
            "records":    None,
        })

        try:
            from reporter import write_batch_report
            write_batch_report(
                execution_date=ctx["execution_date"],
                tasks=tasks_info,
                views_refreshed=["vw_executive", "vw_sales_performance", "vw_funnel", "vw_reviews"],
            )
        except Exception as exc:
            print(f"[dag] Aviso: falha ao escrever relatório de batch: {exc}")

    # ── Operators ─────────────────────────────────────────────────────────
    # Streaming tasks share a checkpoint location — retrying too quickly causes
    # CONCURRENT_STREAM_LOG_UPDATE if the previous JVM hasn't stopped yet.
    # A 15-minute delay gives the orphaned JVM enough time to die.
    _silver_retry_args = {"retries": 1, "retry_delay": timedelta(minutes=15)}

    op_silver_clickstream = PythonOperator(
        task_id="silver_clickstream",
        python_callable=task_silver_clickstream,
        **_silver_retry_args,
    )

    op_silver_orders = PythonOperator(
        task_id="silver_orders",
        python_callable=task_silver_orders,
        **_silver_retry_args,
    )

    op_silver_reviews = PythonOperator(
        task_id="silver_reviews",
        python_callable=task_silver_reviews,
        **_silver_retry_args,
    )

    op_clickstream = PythonOperator(
        task_id="gold_clickstream",
        python_callable=task_gold_clickstream,
    )

    op_sales = PythonOperator(
        task_id="gold_sales",
        python_callable=task_gold_sales,
    )

    op_reviews = PythonOperator(
        task_id="gold_reviews",
        python_callable=task_gold_reviews,
    )

    op_dimensions = PythonOperator(
        task_id="gold_dimensions",
        python_callable=task_gold_dimensions,
    )

    op_views = PythonOperator(
        task_id="init_views",
        python_callable=task_init_views,
    )

    # ── Dependencies ──────────────────────────────────────────────────────
    # All tasks run sequentially — each spawns a full Spark JVM inside the
    # scheduler container, so running them in parallel exhausts memory.
    # Dimensions must run before facts because facts resolve FKs via joins.
    (
        op_silver_clickstream
        >> op_silver_orders
        >> op_silver_reviews
        >> op_dimensions
        >> op_clickstream
        >> op_sales
        >> op_reviews
        >> op_views
    )
