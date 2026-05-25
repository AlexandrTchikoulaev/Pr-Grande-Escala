"""
Constrói a tabela de features para o modelo de previsão de procura.

Input:  lake.gold.fact_sales + dim_date + dim_category
Output: DataFrame com agregação diária por categoria + lag features + features de calendário
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_timeseries(spark: SparkSession) -> DataFrame:
    daily = spark.sql("""
        SELECT
            s.category_id,
            dc.category_en,
            s.purchase_date,
            COUNT(DISTINCT s.order_id)         AS orders_count,
            CAST(SUM(s.total_value) AS DOUBLE) AS total_revenue,
            d.day_of_week,
            d.is_weekend,
            d.month,
            d.quarter
        FROM lake.gold.fact_sales s
        LEFT JOIN lake.gold.dim_category dc ON s.category_id = dc.category_id
        LEFT JOIN lake.gold.dim_date     d  ON s.date_id     = d.date_id
        GROUP BY s.category_id, dc.category_en, s.purchase_date,
                 d.day_of_week, d.is_weekend, d.month, d.quarter
    """)

    w = Window.partitionBy("category_id").orderBy("purchase_date")
    w_rolling = Window.partitionBy("category_id").orderBy("purchase_date").rowsBetween(-6, 0)

    features = (
        daily
        .withColumn("lag_1",  F.lag("orders_count", 1).over(w))
        .withColumn("lag_7",  F.lag("orders_count", 7).over(w))
        .withColumn("lag_14", F.lag("orders_count", 14).over(w))
        .withColumn("rolling_7d_mean", F.avg("orders_count").over(w_rolling))
        # remover linhas onde os lags não estão ainda disponíveis
        .filter(F.col("lag_14").isNotNull())
    )

    return features
