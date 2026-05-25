"""
TrendMart Dashboard — camada de dados.
Queries às views Trino (lake.gold.*) e devolve DataFrames Pandas.
"""

import os
import pandas as pd
import trino

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8085"))


def _conn():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="dashboard",
        catalog="lake",
        schema="gold",
    )


def _fetch(sql: str) -> pd.DataFrame:
    try:
        conn = _conn()
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
        cur.close()
        conn.close()
        return pd.DataFrame(rows, columns=cols)
    except Exception as exc:
        print(f"[data] Trino query failed: {exc}")
        return pd.DataFrame()


def get_executive(days: int = 90) -> pd.DataFrame:
    return _fetch(f"""
        SELECT
            "day", total_orders, total_customers,
            total_revenue, avg_order_value, avg_rating,
            positive_reviews, negative_reviews
        FROM lake.gold.vw_executive
        WHERE "day" >= current_date - INTERVAL '{days}' DAY
        ORDER BY "day"
    """)


def get_sales_performance(days: int = 90) -> pd.DataFrame:
    return _fetch(f"""
        SELECT
            purchase_date, category, state, region,
            orders, customers, revenue, avg_order_value,
            product_revenue, freight_revenue
        FROM lake.gold.vw_sales_performance
        WHERE purchase_date >= current_date - INTERVAL '{days}' DAY
    """)


def get_funnel(days: int = 7) -> pd.DataFrame:
    return _fetch(f"""
        SELECT
            hour, event_date, event_type, device, category,
            event_count, sessions, users
        FROM lake.gold.vw_funnel
        WHERE event_date >= current_date - INTERVAL '{days}' DAY
    """)


def get_reviews(days: int = 90) -> pd.DataFrame:
    return _fetch(f"""
        SELECT
            review_date, sentiment, category, region,
            review_count, avg_rating, avg_text_length
        FROM lake.gold.vw_reviews
        WHERE review_date >= current_date - INTERVAL '{days}' DAY
    """)


def get_demand_forecast() -> pd.DataFrame:
    return _fetch("""
        SELECT
            category_en AS category,
            forecast_date,
            predicted_orders,
            model_rmse,
            model_mae,
            scored_at
        FROM lake.gold.ml_demand_forecast
        ORDER BY category_en, forecast_date
    """)


def get_churn_scores() -> pd.DataFrame:
    return _fetch("""
        SELECT
            risk_label,
            COUNT(*)                  AS total_customers,
            AVG(churn_probability)    AS avg_churn_prob,
            AVG(recency)              AS avg_recency,
            AVG(frequency)            AS avg_frequency,
            AVG(monetary)             AS avg_monetary,
            MAX(model_f1)             AS model_f1,
            MAX(model_auc)            AS model_auc
        FROM lake.gold.ml_churn_scores
        GROUP BY risk_label
    """)
