"""
Silver → Gold: sales fact table.

Reads lake.silver.orders, resolves dimension FKs (date_id, category_id, geo_id),
writes to lake.gold.fact_sales partitioned by purchase_date. Idempotent via createOrReplace.
"""

from pyspark.sql import functions as F
from transformations.spark_session import get_spark


def run(spark=None):
    if spark is None:
        spark = get_spark("gold_sales")

    spark.sql("CREATE DATABASE IF NOT EXISTS lake.gold")

    silver   = spark.table("lake.silver.orders")
    dim_date = spark.table("lake.gold.dim_date").select("date_id", "date_actual")
    dim_cat  = spark.table("lake.gold.dim_category").select("category_id", "category_en")
    dim_geo  = spark.table("lake.gold.dim_geography").select("geo_id", F.col("state").alias("geo_state"))

    base = (
        silver
        .withColumn("purchase_date", F.to_date("purchase_ts"))
        .filter(F.col("purchase_date").isNotNull())
    )

    gold = (
        base
        .join(dim_date, base["purchase_date"] == dim_date["date_actual"], "left")
        .join(dim_cat,  base["category"]       == dim_cat["category_en"], "left")
        .join(dim_geo,  base["state"]           == dim_geo["geo_state"],  "left")
        .select(
            F.col("order_id"),
            F.col("session_id"),
            F.col("customer_id"),
            F.col("product_id"),
            F.col("seller_id"),
            F.col("date_id"),
            F.col("category_id"),
            F.col("geo_id"),
            F.col("price"),
            F.col("freight_value"),
            F.col("total_value"),
            F.col("purchase_ts"),
            F.col("purchase_date"),
        )
    )

    gold.writeTo("lake.gold.fact_sales") \
        .tableProperty("format-version", "2") \
        .partitionedBy(F.days("purchase_date")) \
        .createOrReplace()

    count = gold.count()
    print(f"[gold_sales] Written {count:,} rows to lake.gold.fact_sales")
    return count


if __name__ == "__main__":
    run()
