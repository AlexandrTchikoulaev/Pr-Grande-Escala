"""
Dimension tables — generated or derived from Silver data.

  dim_date        : calendar 2020–2030 (generated)
  dim_category    : unique categories from silver.orders + silver.clickstream
  dim_geography   : Brazilian states with region (from config)

Runs independently of the fact pipelines; safe to re-run at any time.
"""

from datetime import date, timedelta

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DateType, BooleanType
)

from transformations.spark_session import get_spark
from transformations import config


# ---------------------------------------------------------------------------
# dim_date
# ---------------------------------------------------------------------------

def _generate_dates(spark, start: str, end: str):
    start_date = date.fromisoformat(start)
    end_date   = date.fromisoformat(end)

    rows = []
    current = start_date
    while current <= end_date:
        rows.append({
            "date_id":     int(current.strftime("%Y%m%d")),
            "date_actual": current.isoformat(),
            "year":        current.year,
            "quarter":     (current.month - 1) // 3 + 1,
            "month":       current.month,
            "week":        current.isocalendar()[1],
            "day_of_week": current.isoweekday(),   # 1=Mon … 7=Sun
            "day_name":    current.strftime("%A"),
            "month_name":  current.strftime("%B"),
            "is_weekend":  current.isoweekday() >= 6,
        })
        current += timedelta(days=1)

    return spark.createDataFrame(rows)


def run_dim_date(spark):
    df = _generate_dates(spark, config.DIM_DATE_START, config.DIM_DATE_END)
    df = df.withColumn("date_actual", F.to_date("date_actual"))

    df.writeTo("lake.gold.dim_date") \
        .tableProperty("format-version", "2") \
        .createOrReplace()

    print(f"[dim_date] {df.count():,} rows written.")


# ---------------------------------------------------------------------------
# dim_category
# ---------------------------------------------------------------------------

def run_dim_category(spark):
    cats_orders = spark.table("lake.silver.orders") \
        .select(F.col("category").alias("category_en")) \
        .filter(F.col("category_en").isNotNull())

    cats_clicks = spark.table("lake.silver.clickstream") \
        .select(F.col("category").alias("category_en")) \
        .filter(F.col("category_en").isNotNull())

    df = cats_orders.union(cats_clicks).distinct() \
        .withColumn("category_id", F.abs(F.hash(F.col("category_en"))))

    df.writeTo("lake.gold.dim_category") \
        .tableProperty("format-version", "2") \
        .createOrReplace()

    print(f"[dim_category] {df.count():,} categories written.")


# ---------------------------------------------------------------------------
# dim_geography
# ---------------------------------------------------------------------------

def run_dim_geography(spark):
    rows = [
        {"state": state, "state_name": names[0], "region": names[1]}
        for state, names in config.REGION_MAP.items()
    ]
    df = spark.createDataFrame(rows) \
        .withColumn("geo_id", F.abs(F.hash(F.col("state"))))

    df.writeTo("lake.gold.dim_geography") \
        .tableProperty("format-version", "2") \
        .createOrReplace()

    print(f"[dim_geography] {df.count():,} states written.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(spark=None):
    if spark is None:
        spark = get_spark("gold_dimensions")

    spark.sql("CREATE DATABASE IF NOT EXISTS lake.gold")

    run_dim_date(spark)
    run_dim_category(spark)
    run_dim_geography(spark)
    print("[gold_dimensions] All dimension tables updated.")


if __name__ == "__main__":
    run()
