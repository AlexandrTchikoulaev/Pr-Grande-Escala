"""
Silver → Gold: clickstream fact table.

Reads lake.silver.clickstream filtered to the Airflow processing window
(window_start <= ingested_at < window_end), resolves dimension FKs,
and appends incrementally to lake.gold.fact_clickstream partitioned by event_date.
First execution creates the table; subsequent executions append.
"""

from pyspark.sql import functions as F
from transformations.spark_session import get_spark


def _table_exists(spark, table: str) -> bool:
    return spark.catalog.tableExists(table)


def run(spark=None, window_start=None, window_end=None):
    if spark is None:
        spark = get_spark("gold_clickstream")

    spark.sql("CREATE DATABASE IF NOT EXISTS lake.gold")

    silver   = spark.table("lake.silver.clickstream")
    dim_date = spark.table("lake.gold.dim_date").select("date_id", "date_actual")
    dim_cat  = spark.table("lake.gold.dim_category").select("category_id", "category_en")

    base = (
        silver
        .withColumn("event_date", F.to_date("event_ts"))
        .filter(F.col("event_date").isNotNull())
    )

    if window_start is not None and window_end is not None:
        base = base.filter(
            (F.col("ingested_at") >= F.lit(window_start)) &
            (F.col("ingested_at") <  F.lit(window_end))
        )

    gold = (
        base
        .join(dim_date, base["event_date"] == dim_date["date_actual"], "left")
        .join(dim_cat,  base["category"]   == dim_cat["category_en"], "left")
        .select(
            F.col("event_id"),
            F.col("session_id"),
            F.col("user_id"),
            F.col("event_type"),
            F.col("event_ts"),
            F.col("event_date"),
            F.col("device"),
            F.col("date_id"),
            F.col("category_id"),
        )
    )

    gold.cache()
    count = gold.count()
    if count == 0:
        gold.unpersist()
        print(f"[gold_clickstream] No new records in window [{window_start}, {window_end}[.")
        return 0

    writer = (
        gold.writeTo("lake.gold.fact_clickstream")
        .tableProperty("format-version", "2")
        .partitionedBy(F.days("event_date"))
    )

    if _table_exists(spark, "lake.gold.fact_clickstream"):
        writer.append()
        print(f"[gold_clickstream] Appended {count:,} rows to lake.gold.fact_clickstream")
    else:
        writer.createOrReplace()
        print(f"[gold_clickstream] Created lake.gold.fact_clickstream with {count:,} rows")

    gold.unpersist()

    return count


if __name__ == "__main__":
    run()
