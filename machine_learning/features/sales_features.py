"""
Constrói as features RFM para o modelo de previsão de churn.

Input:  lake.gold.fact_sales + lake.gold.fact_reviews
Output: DataFrame com uma linha por cliente:
        customer_id, recency, frequency, monetary, avg_rating, churned (label)

Definição de churn: cliente sem compra nos últimos CHURN_DAYS_THRESHOLD dias
relativamente à data mais recente do dataset (não à data atual, para evitar
problemas com dados simulados históricos).
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from machine_learning import config


def build_churn_features(spark: SparkSession) -> DataFrame:
    rfm = spark.sql("""
        WITH max_date AS (
            SELECT MAX(purchase_date) AS ref_date FROM lake.gold.fact_sales
        ),
        sales_agg AS (
            SELECT
                s.customer_id,
                DATEDIFF(m.ref_date, MAX(s.purchase_date)) AS recency,
                COUNT(DISTINCT s.order_id)                 AS frequency,
                CAST(SUM(s.total_value) AS DOUBLE)         AS monetary
            FROM lake.gold.fact_sales s
            CROSS JOIN max_date m
            GROUP BY s.customer_id, m.ref_date
        ),
        reviews_agg AS (
            SELECT
                customer_id,
                AVG(CAST(rating AS DOUBLE)) AS avg_rating
            FROM lake.gold.fact_reviews
            GROUP BY customer_id
        )
        SELECT
            s.customer_id,
            s.recency,
            s.frequency,
            s.monetary,
            COALESCE(r.avg_rating, 3.0) AS avg_rating,
            CASE WHEN s.recency > {threshold} THEN 1 ELSE 0 END AS churned
        FROM sales_agg s
        LEFT JOIN reviews_agg r ON s.customer_id = r.customer_id
    """.format(threshold=config.CHURN_DAYS_THRESHOLD))

    return rfm
