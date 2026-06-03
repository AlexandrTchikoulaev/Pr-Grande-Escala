"""
Silver → Gold: sales fact table.

Reads lake.silver.orders filtered to the Airflow processing window
(window_start <= ingested_at < window_end), resolves dimension FKs,
and appends incrementally to lake.gold.fact_sales partitioned by purchase_date.
First execution creates the table; subsequent executions append.
"""

from pyspark.sql import functions as F
from transformations.spark_session import get_spark


def _table_exists(spark, table: str) -> bool:
    return spark.catalog.tableExists(table)


def run(spark=None, window_start=None, window_end=None):
    if spark is None:
        spark = get_spark("gold_sales")

    spark.sql("CREATE DATABASE IF NOT EXISTS lake.gold")

    silver   = spark.table("lake.silver.orders")
    dim_cat  = spark.table("lake.gold.dim_category").select("category_id", "category_en")
    dim_geo  = spark.table("lake.gold.dim_geography").select("geo_id", F.col("state").alias("geo_state"))

    base = (
        silver
        .withColumn("purchase_date", F.to_date("purchase_ts"))
        .filter(F.col("purchase_date").isNotNull())
    )

    if window_start is not None and window_end is not None:
        base = base.filter(
            (F.col("ingested_at") >= F.lit(window_start)) &
            (F.col("ingested_at") <  F.lit(window_end))
        )

    gold = (
        base
        .join(dim_cat, base["category"] == dim_cat["category_en"], "left")
        .join(dim_geo, base["state"]    == dim_geo["geo_state"],   "left")
        .select(
            F.col("order_id"),
            F.col("customer_id"),
            F.col("category_id"),
            F.col("geo_id"),
            F.col("price"),
            F.col("freight_value"),
            F.col("total_value"),
            F.col("purchase_date"),
        )
    )

    gold.cache()
    count = gold.count()
    if count == 0:
        gold.unpersist()
        print(f"[gold_sales] No new records in window [{window_start}, {window_end}[.")
        return 0

    writer = (
        gold.writeTo("lake.gold.fact_sales")
        .tableProperty("format-version", "2")
        .partitionedBy(F.days("purchase_date"))
    )

    if _table_exists(spark, "lake.gold.fact_sales"):
        writer.append()
        print(f"[gold_sales] Appended {count:,} rows to lake.gold.fact_sales")
    else:
        writer.createOrReplace()
        print(f"[gold_sales] Created lake.gold.fact_sales with {count:,} rows")

    gold.unpersist()

    return count


if __name__ == "__main__":
    run()
