"""
Creates (or replaces) all analytical views in Trino.
Views are defined over lake.gold.* tables and are the
consumption layer for BI, Data Science, and ML teams.

Called as the final task in the Airflow DAG after all Gold jobs complete.
"""

import trino
from transformations import config


def _get_conn():
    return trino.dbapi.connect(
        host=config.TRINO_HOST,
        port=config.TRINO_PORT,
        user="airflow",
        catalog="lake",
        schema="gold",
    )


VIEWS = {

    # ------------------------------------------------------------------
    # Executive KPIs — one row per day
    # ------------------------------------------------------------------
    "vw_executive": """
        SELECT
            s.purchase_date                             AS day,
            COUNT(DISTINCT s.order_id)                  AS total_orders,
            COUNT(DISTINCT s.customer_id)               AS total_customers,
            CAST(SUM(s.total_value) AS DOUBLE)          AS total_revenue,
            CAST(AVG(s.total_value) AS DOUBLE)          AS avg_order_value
        FROM lake.gold.fact_sales s
        GROUP BY s.purchase_date
    """,

    # ------------------------------------------------------------------
    # Sales performance — by category and region
    # ------------------------------------------------------------------
    "vw_sales_performance": """
        SELECT
            s.purchase_date,
            dc.category_en                       AS category,
            dg.state,
            dg.region,
            COUNT(DISTINCT s.order_id)           AS orders,
            COUNT(DISTINCT s.customer_id)        AS customers,
            CAST(SUM(s.total_value) AS DOUBLE)   AS revenue,
            CAST(AVG(s.total_value) AS DOUBLE)   AS avg_order_value,
            CAST(SUM(s.price) AS DOUBLE)         AS product_revenue,
            CAST(SUM(s.freight_value) AS DOUBLE) AS freight_revenue
        FROM lake.gold.fact_sales s
        LEFT JOIN lake.gold.dim_category  dc ON s.category_id = dc.category_id
        LEFT JOIN lake.gold.dim_geography dg ON s.geo_id      = dg.geo_id
        GROUP BY s.purchase_date, dc.category_en, dg.state, dg.region
    """,

    # ------------------------------------------------------------------
    # Clickstream funnel — conversion analysis (RF3.1–RF3.4)
    # order_placed is now published to Kafka alongside all other events,
    # so the funnel is complete directly from fact_clickstream (RF3.1).
    # ------------------------------------------------------------------
    "vw_funnel": """
        SELECT
            CAST(DATE_TRUNC('hour', fc.event_ts) AS TIMESTAMP) AS hour,
            fc.event_date,
            dd.is_weekend,
            dd.day_of_week,
            fc.event_type,
            fc.device,
            dc.category_en                  AS category,
            COUNT(*)                        AS event_count,
            COUNT(DISTINCT fc.session_id)   AS sessions,
            COUNT(DISTINCT fc.user_id)      AS users
        FROM lake.gold.fact_clickstream fc
        LEFT JOIN lake.gold.dim_date     dd ON fc.date_id     = dd.date_id
        LEFT JOIN lake.gold.dim_category dc ON fc.category_id = dc.category_id
        GROUP BY
            DATE_TRUNC('hour', fc.event_ts),
            fc.event_date,
            dd.is_weekend,
            dd.day_of_week,
            fc.event_type,
            fc.device,
            dc.category_en
    """,

    # ------------------------------------------------------------------
    # Reviews sentiment — by category and region over time (RF4.1–RF4.4)
    # category_id and geo_id are now columns of fact_reviews itself,
    # so no join through fact_sales is needed.
    # ------------------------------------------------------------------
    "vw_reviews": """
        SELECT
            r.review_date,
            r.sentiment,
            dc.category_en                                     AS category,
            dg.state,
            dg.region,
            COUNT(r.review_id)                                 AS review_count,
            CAST(AVG(CAST(r.rating AS DOUBLE)) AS DOUBLE)      AS avg_rating,
            CAST(AVG(CAST(r.text_length AS DOUBLE)) AS DOUBLE) AS avg_text_length
        FROM lake.gold.fact_reviews r
        LEFT JOIN lake.gold.dim_category  dc ON r.category_id = dc.category_id
        LEFT JOIN lake.gold.dim_geography dg ON r.geo_id      = dg.geo_id
        GROUP BY r.review_date, r.sentiment, dc.category_en, dg.state, dg.region
    """,

    # ------------------------------------------------------------------
    # Global trend metrics — WoW growth and demand acceleration
    # ------------------------------------------------------------------
    "vw_trends": """
        WITH daily AS (
            SELECT
                purchase_date,
                COUNT(DISTINCT order_id)         AS orders,
                CAST(SUM(total_value) AS DOUBLE) AS revenue
            FROM lake.gold.fact_sales
            GROUP BY purchase_date
        ),
        with_lags AS (
            SELECT
                purchase_date,
                orders,
                revenue,
                LAG(orders,  7) OVER (ORDER BY purchase_date) AS orders_7d_ago,
                LAG(revenue, 7) OVER (ORDER BY purchase_date) AS revenue_7d_ago,
                LAG(orders,  14) OVER (ORDER BY purchase_date) AS orders_14d_ago,
                LAG(revenue, 14) OVER (ORDER BY purchase_date) AS revenue_14d_ago
            FROM daily
        )
        SELECT
            purchase_date,
            orders,
            revenue,
            CASE
                WHEN orders_7d_ago > 0
                THEN CAST((orders - orders_7d_ago) AS DOUBLE) / orders_7d_ago * 100
            END AS orders_growth_pct,
            CASE
                WHEN revenue_7d_ago > 0
                THEN (revenue - revenue_7d_ago) / revenue_7d_ago * 100
            END AS revenue_growth_pct,
            CASE
                WHEN revenue_7d_ago > 0 AND revenue_14d_ago > 0
                THEN ((revenue - revenue_7d_ago) / revenue_7d_ago)
                   - ((revenue_7d_ago - revenue_14d_ago) / revenue_14d_ago)
            END AS revenue_acceleration
        FROM with_lags
    """,

    # ------------------------------------------------------------------
    # Per-category trend metrics — WoW growth by category
    # ------------------------------------------------------------------
    "vw_category_trends": """
        WITH base AS (
            SELECT
                s.purchase_date,
                dc.category_en                       AS category,
                COUNT(DISTINCT s.order_id)           AS orders,
                CAST(SUM(s.total_value) AS DOUBLE)   AS revenue
            FROM lake.gold.fact_sales s
            LEFT JOIN lake.gold.dim_category dc ON s.category_id = dc.category_id
            GROUP BY s.purchase_date, dc.category_en
        ),
        with_lags AS (
            SELECT
                purchase_date,
                category,
                orders,
                revenue,
                LAG(orders,  7) OVER (PARTITION BY category ORDER BY purchase_date) AS orders_7d_ago,
                LAG(revenue, 7) OVER (PARTITION BY category ORDER BY purchase_date) AS revenue_7d_ago
            FROM base
        )
        SELECT
            purchase_date,
            category,
            orders,
            revenue,
            CASE
                WHEN orders_7d_ago > 0
                THEN CAST((orders - orders_7d_ago) AS DOUBLE) / orders_7d_ago * 100
            END AS orders_growth_pct,
            CASE
                WHEN revenue_7d_ago > 0
                THEN (revenue - revenue_7d_ago) / revenue_7d_ago * 100
            END AS revenue_growth_pct
        FROM with_lags
    """,
}


def run():
    conn = _get_conn()
    cursor = conn.cursor()
    created, skipped = [], []

    for name, sql in VIEWS.items():
        ddl = f"CREATE OR REPLACE VIEW lake.gold.{name} AS {sql.strip()}"
        try:
            cursor.execute(ddl)
            print(f"[init_views] View created/replaced: {name}")
            created.append(name)
        except Exception as exc:
            if "TABLE_NOT_FOUND" in str(exc) or "does not exist" in str(exc):
                print(f"[init_views] Skipped {name}: tabela base ainda não existe")
                skipped.append(name)
            else:
                cursor.close()
                conn.close()
                raise

    cursor.close()
    conn.close()
    print(f"[init_views] Criadas: {len(created)}, ignoradas: {len(skipped)}/{len(VIEWS)} views.")


if __name__ == "__main__":
    run()
