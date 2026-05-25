"""
Silver → Gold: clickstream fact table.

Reads lake.silver.clickstream, resolves dimension FKs (date_id, category_id),
writes to lake.gold.fact_clickstream partitioned by event_date. Idempotent via createOrReplace.
"""

from pyspark.sql import functions as F
from transformations.spark_session import get_spark


def run(spark=None):
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
            F.col("product_id"),
            F.col("price"),
            F.col("location"),
        )
    )

    gold.writeTo("lake.gold.fact_clickstream") \
        .tableProperty("format-version", "2") \
        .partitionedBy(F.days("event_date")) \
        .createOrReplace()

    count = gold.count()
    print(f"[gold_clickstream] Written {count:,} rows to lake.gold.fact_clickstream")
    return count


if __name__ == "__main__":
    run()
