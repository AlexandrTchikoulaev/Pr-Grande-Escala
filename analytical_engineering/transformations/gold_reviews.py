"""
Silver → Gold: reviews fact table.

Reads lake.silver.reviews, adds sentiment label from rating, resolves date_id FK,
writes to lake.gold.fact_reviews partitioned by review_date.
"""

from pyspark.sql import functions as F
from transformations.spark_session import get_spark
from transformations import config


def run(spark=None):
    if spark is None:
        spark = get_spark("gold_reviews")

    spark.sql("CREATE DATABASE IF NOT EXISTS lake.gold")

    silver   = spark.table("lake.silver.reviews")
    dim_date = spark.table("lake.gold.dim_date").select("date_id", "date_actual")

    sentiment = (
        F.when(F.col("rating") >= config.SENTIMENT_POSITIVE, "positive")
         .when(F.col("rating") == config.SENTIMENT_NEUTRAL,  "neutral")
         .otherwise("negative")
    )

    base = (
        silver
        .withColumn("review_date", F.to_date("ingested_at"))
        .withColumn("sentiment",   sentiment)
        .filter(F.col("review_date").isNotNull())
    )

    gold = (
        base
        .join(dim_date, base["review_date"] == dim_date["date_actual"], "left")
        .select(
            F.col("review_id"),
            F.col("order_id"),
            F.col("customer_id"),
            F.col("rating"),
            F.col("sentiment"),
            F.col("title"),
            F.col("message"),
            F.col("text_length"),
            F.col("date_id"),
            F.col("review_date"),
        )
    )

    gold.writeTo("lake.gold.fact_reviews") \
        .tableProperty("format-version", "2") \
        .partitionedBy(F.days("review_date")) \
        .createOrReplace()

    count = gold.count()
    print(f"[gold_reviews] Written {count:,} rows to lake.gold.fact_reviews")
    return count


if __name__ == "__main__":
    run()
