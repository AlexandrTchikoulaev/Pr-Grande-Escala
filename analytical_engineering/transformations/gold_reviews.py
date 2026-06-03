"""
Silver → Gold: reviews fact table.

Reads lake.silver.reviews filtered to the Airflow processing window
(window_start <= ingested_at < window_end), adds sentiment label from rating,
resolves date_id, category_id and geo_id FKs (via join with silver.orders),
and appends incrementally to lake.gold.fact_reviews partitioned by review_date.
First execution creates the table; subsequent executions append.

geo_id is resolved here (not in the view) so that vw_reviews can join
dim_geography directly on fact_reviews.geo_id without going through fact_sales.
This avoids NULL geography when a review exists but the matching order hasn't
yet landed in fact_sales.
"""

from pyspark.sql import functions as F
from transformations.spark_session import get_spark
from transformations import config


def _table_exists(spark, table: str) -> bool:
    return spark.catalog.tableExists(table)


def run(spark=None, window_start=None, window_end=None):
    if spark is None:
        spark = get_spark("gold_reviews")

    spark.sql("CREATE DATABASE IF NOT EXISTS lake.gold")

    silver        = spark.table("lake.silver.reviews")
    silver_orders = spark.table("lake.silver.orders") \
        .select("order_id",
                F.col("category").alias("order_category"),
                F.col("state").alias("order_state"))
    dim_cat       = spark.table("lake.gold.dim_category").select("category_id", "category_en")
    dim_geo       = spark.table("lake.gold.dim_geography").select("geo_id", F.col("state").alias("geo_state"))

    sentiment = (
        F.when(F.col("rating") >= config.SENTIMENT_POSITIVE, "positive")
         .when(F.col("rating") == config.SENTIMENT_NEUTRAL,  "neutral")
         .otherwise("negative")
    )

    base = (
        silver
        .withColumn("review_date", F.to_date(F.col("ingested_at")))
        .withColumn("sentiment",   sentiment)
        .filter(F.col("review_date").isNotNull())
    )

    if window_start is not None and window_end is not None:
        base = base.filter(
            (F.col("ingested_at") >= F.lit(window_start)) &
            (F.col("ingested_at") <  F.lit(window_end))
        )

    base = base.join(silver_orders, "order_id", "left")

    gold = (
        base
        .join(dim_cat, F.col("order_category") == dim_cat["category_en"], "left")
        .join(dim_geo, F.col("order_state")    == dim_geo["geo_state"],   "left")
        .select(
            F.col("review_id"),
            F.col("order_id"),
            F.col("rating"),
            F.col("sentiment"),
            F.col("text_length"),
            F.col("category_id"),
            F.col("geo_id"),
            F.col("review_date"),
        )
    )

    gold.cache()
    count = gold.count()
    if count == 0:
        gold.unpersist()
        print(f"[gold_reviews] No new records in window [{window_start}, {window_end}[.")
        return 0

    writer = (
        gold.writeTo("lake.gold.fact_reviews")
        .tableProperty("format-version", "2")
        .partitionedBy(F.days("review_date"))
    )

    if _table_exists(spark, "lake.gold.fact_reviews"):
        writer.append()
        print(f"[gold_reviews] Appended {count:,} rows to lake.gold.fact_reviews")
    else:
        writer.createOrReplace()
        print(f"[gold_reviews] Created lake.gold.fact_reviews with {count:,} rows")

    gold.unpersist()

    return count


if __name__ == "__main__":
    run()
