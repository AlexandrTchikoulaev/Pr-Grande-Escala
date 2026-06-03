"""
seed_demo.py
============
Popula as tabelas gold com 95 dias de dados realistas para demonstração.

Executa NO container ge_airflow_scheduler (tem trino + env vars correctas):

    docker exec ge_airflow_scheduler python /opt/airflow/project/seed_demo.py

Seguro re-executar: só insere datas que ainda não existem em cada tabela.
Lê os IDs actuais de dim_category / dim_geography para não quebrar JOINs
com dados já inseridos pelo pipeline.
"""

import os
import random
import uuid
import hashlib
import datetime
from datetime import date, timedelta

import trino

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8085"))

TODAY = date(2026, 5, 29)
DAYS  = 95          # 90 dias visíveis + 5 de margem para os lags WoW (14 dias)
START = TODAY - timedelta(days=DAYS - 1)

random.seed(42)     # dados reprodutíveis entre execuções

# ─────────────────────────────────────────────────────────────────────────────
# Domínio
# ─────────────────────────────────────────────────────────────────────────────
CATEGORIES = [
    "health_beauty", "computers_accessories", "auto", "bed_bath_table",
    "sports_leisure", "furniture_decor", "housewares", "watches_gifts",
    "telephony", "garden_tools", "toys", "cool_stuff", "electronics",
    "stationery", "fashion_bags_accessories",
]
CAT_W = [12, 10, 8, 9, 8, 7, 7, 6, 6, 4, 5, 4, 5, 3, 6]

STATES = {
    "SP": ("São Paulo",          "Sudeste"),
    "RJ": ("Rio de Janeiro",     "Sudeste"),
    "MG": ("Minas Gerais",       "Sudeste"),
    "ES": ("Espírito Santo",     "Sudeste"),
    "RS": ("Rio Grande do Sul",  "Sul"),
    "PR": ("Paraná",             "Sul"),
    "SC": ("Santa Catarina",     "Sul"),
    "BA": ("Bahia",              "Nordeste"),
    "PE": ("Pernambuco",         "Nordeste"),
    "CE": ("Ceará",              "Nordeste"),
    "MA": ("Maranhão",           "Nordeste"),
    "PA": ("Pará",               "Norte"),
    "AM": ("Amazonas",           "Norte"),
    "DF": ("Distrito Federal",   "Centro-Oeste"),
    "GO": ("Goiás",              "Centro-Oeste"),
}
STATE_LIST = list(STATES.keys())
STATE_W    = [30, 13, 9, 2, 6, 5, 4, 5, 3, 3, 2, 2, 2, 3, 2]

# Funil: (event_type, probabilidade de ocorrer numa sessão)
FUNNEL_RATES = [
    ("session_start",       1.00),
    ("search",              0.84),
    ("category_browse",     0.72),
    ("product_view",        0.58),
    ("product_review_read", 0.32),
    ("add_to_cart",         0.19),
    ("remove_from_cart",    0.06),
    ("cart_view",           0.17),
    ("cart_abandon",        0.08),
    ("checkout_start",      0.09),
    ("order_placed",        0.06),
    ("session_end",         0.94),
]

DEVICES = ["mobile", "desktop", "tablet"]
DEV_W   = [58, 35, 7]
HOUR_W  = [1,1,1,1,1,1,2,4,6,7,7,7,6,6,7,8,8,8,7,6,5,4,3,2]

# ─────────────────────────────────────────────────────────────────────────────
# Helpers gerais
# ─────────────────────────────────────────────────────────────────────────────

def _stable_id(s: str) -> int:
    """ID inteiro estável (não usa hash() do Python, que muda entre runs)."""
    return int(hashlib.md5(s.encode()).hexdigest()[:8], 16)


def _q(v) -> str:
    """Formata valor Python como literal SQL Trino."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return f"{v:.4f}"
    if isinstance(v, datetime.datetime):           # datetime antes de date!
        return f"TIMESTAMP '{v:%Y-%m-%d %H:%M:%S}'"
    if isinstance(v, date):
        return f"DATE '{v.isoformat()}'"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return str(v)


def _insert(cur, table: str, cols: list, rows: list, batch: int = 400):
    if not rows:
        print(f"    (sem linhas novas para {table})")
        return
    col_str = ", ".join(cols)
    total = 0
    for i in range(0, len(rows), batch):
        chunk = rows[i : i + batch]
        vals  = ", ".join(
            "(" + ", ".join(_q(v) for v in row) + ")"
            for row in chunk
        )
        cur.execute(f"INSERT INTO lake.gold.{table} ({col_str}) VALUES {vals}")
        cur.fetchall()
        total += len(chunk)
    print(f"    ✓ {table}: {total:,} linhas inseridas")


def _table_exists(cur, table: str) -> bool:
    try:
        cur.execute(f"SELECT 1 FROM lake.gold.{table} LIMIT 1")
        cur.fetchall()
        return True
    except Exception:
        return False


def _count(cur, table: str) -> int:
    try:
        cur.execute(f"SELECT COUNT(*) FROM lake.gold.{table}")
        return (cur.fetchone() or [0])[0]
    except Exception:
        return 0


def _existing_dates(cur, table: str, col: str) -> set:
    try:
        cur.execute(f"SELECT DISTINCT {col} FROM lake.gold.{table}")
        return {r[0] for r in cur.fetchall()}
    except Exception:
        return set()


def _missing_dates(cur, table: str, col: str) -> list:
    existing = _existing_dates(cur, table, col)
    all_days = [START + timedelta(days=i) for i in range(DAYS)]
    missing  = [d for d in all_days if d not in existing]
    print(f"    {table}: {len(existing)} dias já existentes, {len(missing)} a inserir")
    return missing


# ─────────────────────────────────────────────────────────────────────────────
# Leitura dos IDs de dimensão existentes
# (garante consistência com dados já inseridos pelo pipeline Spark)
# ─────────────────────────────────────────────────────────────────────────────

def _read_cat_ids(cur) -> dict:
    """Devolve {category_en: category_id} lido de dim_category (ou IDs estáveis)."""
    try:
        cur.execute("SELECT category_en, category_id FROM lake.gold.dim_category")
        rows = cur.fetchall()
        if rows:
            return {r[0]: r[1] for r in rows}
    except Exception:
        pass
    return {c: _stable_id(c) for c in CATEGORIES}


def _read_geo_ids(cur) -> dict:
    """Devolve {state: geo_id} lido de dim_geography (ou IDs estáveis)."""
    try:
        cur.execute("SELECT state, geo_id FROM lake.gold.dim_geography")
        rows = cur.fetchall()
        if rows:
            return {r[0]: r[1] for r in rows}
    except Exception:
        pass
    return {s: _stable_id(s) for s in STATE_LIST}


# ─────────────────────────────────────────────────────────────────────────────
# Dimensões
# ─────────────────────────────────────────────────────────────────────────────

def seed_dim_category(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lake.gold.dim_category (
            category_id BIGINT,
            category_en VARCHAR
        ) WITH (format = 'PARQUET')
    """)
    cur.fetchall()

    existing = _read_cat_ids(cur)
    missing  = [c for c in CATEGORIES if c not in existing]
    if not missing:
        print("    ↩ dim_category: todas as categorias já existem")
        return
    rows = [(_stable_id(c), c) for c in missing]
    _insert(cur, "dim_category", ["category_id", "category_en"], rows)


def seed_dim_geography(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lake.gold.dim_geography (
            geo_id     BIGINT,
            state      VARCHAR,
            state_name VARCHAR,
            region     VARCHAR
        ) WITH (format = 'PARQUET')
    """)
    cur.fetchall()

    existing = _read_geo_ids(cur)
    missing  = [s for s in STATE_LIST if s not in existing]
    if not missing:
        print("    ↩ dim_geography: todos os estados já existem")
        return
    rows = [(_stable_id(s), s, STATES[s][0], STATES[s][1]) for s in missing]
    _insert(cur, "dim_geography", ["geo_id", "state", "state_name", "region"], rows)


def seed_dim_date(cur):
    if _table_exists(cur, "dim_date") and _count(cur, "dim_date") > 365:
        print("    ↩ dim_date: já populada pelo pipeline")
        return
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lake.gold.dim_date (
            date_id     INTEGER,
            date_actual DATE,
            year        INTEGER,
            quarter     INTEGER,
            month       INTEGER,
            week        INTEGER,
            day_of_week INTEGER,
            day_name    VARCHAR,
            month_name  VARCHAR,
            is_weekend  BOOLEAN
        ) WITH (format = 'PARQUET')
    """)
    cur.fetchall()
    rows = []
    for i in range(DAYS + 20):          # margem para lags
        d = START + timedelta(days=i)
        rows.append((
            int(d.strftime("%Y%m%d")), d,
            d.year, (d.month - 1) // 3 + 1, d.month,
            d.isocalendar()[1], d.isoweekday(),
            d.strftime("%A"), d.strftime("%B"),
            d.isoweekday() >= 6,
        ))
    _insert(cur, "dim_date",
            ["date_id","date_actual","year","quarter","month","week",
             "day_of_week","day_name","month_name","is_weekend"], rows)


# ─────────────────────────────────────────────────────────────────────────────
# Factos: Vendas
# ─────────────────────────────────────────────────────────────────────────────

def seed_fact_sales(cur, cat_ids: dict, geo_ids: dict):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lake.gold.fact_sales (
            order_id      VARCHAR,
            customer_id   VARCHAR,
            category_id   BIGINT,
            geo_id        BIGINT,
            price         DOUBLE,
            freight_value DOUBLE,
            total_value   DOUBLE,
            purchase_date DATE
        ) WITH (
            format       = 'PARQUET',
            partitioning = ARRAY['day(purchase_date)']
        )
    """)
    cur.fetchall()

    missing = _missing_dates(cur, "fact_sales", "purchase_date")
    if not missing:
        return

    cat_list = [(cat_ids.get(c, _stable_id(c)), w) for c, w in zip(CATEGORIES, CAT_W)]
    geo_list = [(geo_ids.get(s, _stable_id(s)), w) for s, w in zip(STATE_LIST, STATE_W)]

    rows = []
    for d in missing:
        dow    = d.isoweekday()
        week_i = (d - START).days // 7
        # Crescimento semanal gradual + sazonalidade dia-da-semana
        base = int(260 * (1 + week_i * 0.009))
        if   dow >= 6:   base = int(base * 1.22)
        elif dow == 1:   base = int(base * 0.86)
        elif dow == 5:   base = int(base * 1.05)
        n = max(80, int(base + random.gauss(0, base * 0.10)))

        for _ in range(n):
            ci = random.choices([x[0] for x in cat_list], weights=[x[1] for x in cat_list])[0]
            gi = random.choices([x[0] for x in geo_list], weights=[x[1] for x in geo_list])[0]
            p  = round(random.uniform(14.0, 290.0), 2)
            f  = round(random.uniform(5.0, min(p * 0.28, 42.0)), 2)
            rows.append((
                str(uuid.uuid4()), str(uuid.uuid4()),
                ci, gi,
                p, f, round(p + f, 2),
                d,
            ))

    _insert(cur, "fact_sales",
            ["order_id","customer_id","category_id","geo_id",
             "price","freight_value","total_value","purchase_date"],
            rows, batch=400)


# ─────────────────────────────────────────────────────────────────────────────
# Factos: Clickstream
# ─────────────────────────────────────────────────────────────────────────────

def seed_fact_clickstream(cur, cat_ids: dict):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lake.gold.fact_clickstream (
            event_id    VARCHAR,
            session_id  VARCHAR,
            user_id     VARCHAR,
            event_type  VARCHAR,
            event_ts    TIMESTAMP,
            event_date  DATE,
            device      VARCHAR,
            date_id     INTEGER,
            category_id BIGINT
        ) WITH (
            format       = 'PARQUET',
            partitioning = ARRAY['day(event_date)']
        )
    """)
    cur.fetchall()

    missing = _missing_dates(cur, "fact_clickstream", "event_date")
    if not missing:
        return

    cat_list = [(cat_ids.get(c, _stable_id(c)), w) for c, w in zip(CATEGORIES, CAT_W)]
    rows = []

    for d in missing:
        dow    = d.isoweekday()
        week_i = (d - START).days // 7
        base   = int(130 * (1 + week_i * 0.006))
        if dow >= 6: base = int(base * 1.16)
        n_sessions = max(40, int(base + random.gauss(0, base * 0.09)))
        did = int(d.strftime("%Y%m%d"))

        for _ in range(n_sessions):
            sid = str(uuid.uuid4())
            uid = str(uuid.uuid4())
            dev = random.choices(DEVICES, weights=DEV_W)[0]
            ci  = random.choices([x[0] for x in cat_list], weights=[x[1] for x in cat_list])[0]

            for ev_type, rate in FUNNEL_RATES:
                if random.random() < rate:
                    h  = random.choices(range(24), weights=HOUR_W)[0]
                    ts = datetime.datetime(d.year, d.month, d.day, h,
                                           random.randint(0, 59), random.randint(0, 59))
                    rows.append((
                        str(uuid.uuid4()), sid, uid,
                        ev_type, ts, d, dev, did, ci,
                    ))

    _insert(cur, "fact_clickstream",
            ["event_id","session_id","user_id","event_type","event_ts","event_date",
             "device","date_id","category_id"],
            rows, batch=350)


# ─────────────────────────────────────────────────────────────────────────────
# Factos: Reviews
# ─────────────────────────────────────────────────────────────────────────────

def seed_fact_reviews(cur, cat_ids: dict, geo_ids: dict):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lake.gold.fact_reviews (
            review_id   VARCHAR,
            order_id    VARCHAR,
            rating      INTEGER,
            sentiment   VARCHAR,
            text_length INTEGER,
            category_id BIGINT,
            geo_id      BIGINT,
            review_date DATE
        ) WITH (
            format       = 'PARQUET',
            partitioning = ARRAY['day(review_date)']
        )
    """)
    cur.fetchall()

    missing = _missing_dates(cur, "fact_reviews", "review_date")
    if not missing:
        return

    cat_list = [(cat_ids.get(c, _stable_id(c)), w) for c, w in zip(CATEGORIES, CAT_W)]
    geo_list = [(geo_ids.get(s, _stable_id(s)), w) for s, w in zip(STATE_LIST, STATE_W)]

    RATINGS  = [1, 2, 3, 4, 5]
    RATING_W = [6, 8, 16, 30, 40]      # skewed positivo (realista)

    def _sent(r: int) -> str:
        if r >= 4: return "positive"
        if r == 3: return "neutral"
        return "negative"

    rows = []
    for d in missing:
        for _ in range(random.randint(50, 88)):
            ci = random.choices([x[0] for x in cat_list], weights=[x[1] for x in cat_list])[0]
            gi = random.choices([x[0] for x in geo_list], weights=[x[1] for x in geo_list])[0]
            r  = random.choices(RATINGS, weights=RATING_W)[0]
            rows.append((
                str(uuid.uuid4()), str(uuid.uuid4()),
                r, _sent(r), random.randint(10, 330),
                ci, gi, d,
            ))

    _insert(cur, "fact_reviews",
            ["review_id","order_id","rating","sentiment","text_length",
             "category_id","geo_id","review_date"],
            rows, batch=400)


# ─────────────────────────────────────────────────────────────────────────────
# ML Forecast
# ─────────────────────────────────────────────────────────────────────────────

def seed_ml_forecast(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lake.gold.ml_demand_forecast (
            category_en      VARCHAR,
            forecast_date    DATE,
            predicted_orders DOUBLE,
            model_rmse       DOUBLE,
            model_mae        DOUBLE,
            scored_at        TIMESTAMP
        ) WITH (format = 'PARQUET')
    """)
    cur.fetchall()

    # Só insere se não houver previsões para amanhã ou mais
    try:
        cur.execute(
            f"SELECT COUNT(*) FROM lake.gold.ml_demand_forecast "
            f"WHERE forecast_date > DATE '{TODAY.isoformat()}'"
        )
        if (cur.fetchone() or [0])[0] > 0:
            print("    ↩ ml_demand_forecast: previsões futuras já existem")
            return
    except Exception:
        pass

    scored_at = datetime.datetime(TODAY.year, TODAY.month, TODAY.day, 3, 0, 0)
    rows = []
    for cat in CATEGORIES:
        rmse = round(random.uniform(3.5, 14.0), 2)
        mae  = round(rmse * random.uniform(0.58, 0.78), 2)
        base = random.randint(25, 110)
        for i in range(1, 8):
            fd  = TODAY + timedelta(days=i)
            adj = 1.18 if fd.isoweekday() >= 6 else 1.0
            pred = round(base * adj * random.uniform(0.88, 1.14), 1)
            rows.append((cat, fd, pred, rmse, mae, scored_at))

    _insert(cur, "ml_demand_forecast",
            ["category_en","forecast_date","predicted_orders",
             "model_rmse","model_mae","scored_at"], rows)


# ─────────────────────────────────────────────────────────────────────────────
# Views (inline — não depende de init_views.py)
# ─────────────────────────────────────────────────────────────────────────────

VIEWS = {
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
    "vw_reviews": """
        SELECT
            r.review_date,
            r.sentiment,
            dc.category_en                                     AS category,
            dg.region,
            COUNT(r.review_id)                                 AS review_count,
            CAST(AVG(CAST(r.rating AS DOUBLE)) AS DOUBLE)      AS avg_rating,
            CAST(AVG(CAST(r.text_length AS DOUBLE)) AS DOUBLE) AS avg_text_length
        FROM lake.gold.fact_reviews r
        LEFT JOIN lake.gold.dim_category  dc ON r.category_id = dc.category_id
        LEFT JOIN lake.gold.dim_geography dg ON r.geo_id      = dg.geo_id
        GROUP BY r.review_date, r.sentiment, dc.category_en, dg.region
    """,
    "vw_trends": """
        WITH daily AS (
            SELECT purchase_date,
                   COUNT(DISTINCT order_id)         AS orders,
                   CAST(SUM(total_value) AS DOUBLE) AS revenue
            FROM lake.gold.fact_sales
            GROUP BY purchase_date
        ),
        with_lags AS (
            SELECT purchase_date, orders, revenue,
                   LAG(orders,  7) OVER (ORDER BY purchase_date) AS orders_7d_ago,
                   LAG(revenue, 7) OVER (ORDER BY purchase_date) AS revenue_7d_ago,
                   LAG(orders, 14) OVER (ORDER BY purchase_date) AS orders_14d_ago,
                   LAG(revenue,14) OVER (ORDER BY purchase_date) AS revenue_14d_ago
            FROM daily
        )
        SELECT purchase_date, orders, revenue,
            CASE WHEN orders_7d_ago  > 0
                 THEN CAST((orders  - orders_7d_ago) AS DOUBLE) / orders_7d_ago  * 100 END AS orders_growth_pct,
            CASE WHEN revenue_7d_ago > 0
                 THEN (revenue - revenue_7d_ago) / revenue_7d_ago * 100 END AS revenue_growth_pct,
            CASE WHEN revenue_7d_ago > 0 AND revenue_14d_ago > 0
                 THEN ((revenue - revenue_7d_ago) / revenue_7d_ago)
                    - ((revenue_7d_ago - revenue_14d_ago) / revenue_14d_ago)
            END AS revenue_acceleration
        FROM with_lags
    """,
    "vw_category_trends": """
        WITH base AS (
            SELECT s.purchase_date,
                   dc.category_en                       AS category,
                   COUNT(DISTINCT s.order_id)           AS orders,
                   CAST(SUM(s.total_value) AS DOUBLE)   AS revenue
            FROM lake.gold.fact_sales s
            LEFT JOIN lake.gold.dim_category dc ON s.category_id = dc.category_id
            GROUP BY s.purchase_date, dc.category_en
        ),
        with_lags AS (
            SELECT purchase_date, category, orders, revenue,
                   LAG(orders,  7) OVER (PARTITION BY category ORDER BY purchase_date) AS orders_7d_ago,
                   LAG(revenue, 7) OVER (PARTITION BY category ORDER BY purchase_date) AS revenue_7d_ago
            FROM base
        )
        SELECT purchase_date, category, orders, revenue,
            CASE WHEN orders_7d_ago  > 0
                 THEN CAST((orders - orders_7d_ago) AS DOUBLE) / orders_7d_ago * 100 END AS orders_growth_pct,
            CASE WHEN revenue_7d_ago > 0
                 THEN (revenue - revenue_7d_ago) / revenue_7d_ago * 100 END AS revenue_growth_pct
        FROM with_lags
    """,
}


def recreate_views(cur):
    for name, sql in VIEWS.items():
        try:
            cur.execute(f"CREATE OR REPLACE VIEW lake.gold.{name} AS {sql.strip()}")
            cur.fetchall()
            print(f"    ✓ view {name}")
        except Exception as e:
            print(f"    ✗ {name}: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "=" * 60)
    print("  TrendMart Demo Seeder")
    print(f"  Trino  : {TRINO_HOST}:{TRINO_PORT}")
    print(f"  Período: {START} → {TODAY}  ({DAYS} dias)")
    print("=" * 60)

    conn = trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT,
        user="seeder", catalog="lake", schema="gold",
    )
    cur = conn.cursor()

    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS lake.gold")
        cur.fetchall()
    except Exception:
        pass

    # Lê IDs existentes antes de qualquer inserção
    cat_ids = _read_cat_ids(cur)
    geo_ids = _read_geo_ids(cur)

    print("\n[1/8] dim_category");     seed_dim_category(cur)
    print("[2/8] dim_geography");      seed_dim_geography(cur)
    print("[3/8] dim_date");           seed_dim_date(cur)

    # Re-lê após seed (pode ter mudado)
    cat_ids = _read_cat_ids(cur)
    geo_ids = _read_geo_ids(cur)

    print("[4/8] fact_sales");         seed_fact_sales(cur, cat_ids, geo_ids)
    print("[5/8] fact_clickstream");   seed_fact_clickstream(cur, cat_ids)
    print("[6/8] fact_reviews");       seed_fact_reviews(cur, cat_ids, geo_ids)
    print("[7/8] ml_demand_forecast"); seed_ml_forecast(cur)
    print("[8/8] views");              recreate_views(cur)

    cur.close()
    conn.close()

    print("\n" + "=" * 60)
    print("  Concluído!")
    print("  Abre http://localhost:8050 e faz refresh ao browser.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
