"""
data_demo.py — dados sintéticos para demonstração.
Mesma interface de data.py, sem Trino / MinIO.
Activar: python demo.py start  |  Reverter: python demo.py clean
"""

import datetime
from datetime import date, timedelta

import numpy as np
import pandas as pd

# ── Configuração ─────────────────────────────────────────────────────────────
_TODAY     = date(2026, 5, 29)
_SEED      = 42
_BASE_DAYS = 850          # ~2.4 anos — cobre Ano/Mês/Dia nos gráficos temporais

# ── Domínio ──────────────────────────────────────────────────────────────────
CATEGORIES = [
    "health_beauty", "computers_accessories", "auto", "bed_bath_table",
    "sports_leisure", "furniture_decor", "housewares", "watches_gifts",
    "telephony", "garden_tools", "toys", "cool_stuff", "electronics",
    "stationery", "fashion_bags_accessories",
]
_CAT_W = np.array([12,10,8,9,8,7,7,6,6,4,5,4,5,3,6], dtype=float)
_CAT_W /= _CAT_W.sum()

_STATES = {
    "SP": "Sudeste",      "RJ": "Sudeste",    "MG": "Sudeste",    "ES": "Sudeste",
    "RS": "Sul",          "PR": "Sul",         "SC": "Sul",
    "BA": "Nordeste",     "PE": "Nordeste",    "CE": "Nordeste",   "MA": "Nordeste",
    "PA": "Norte",        "AM": "Norte",
    "DF": "Centro-Oeste", "GO": "Centro-Oeste",
}
_STATE_LIST = list(_STATES.keys())
_STATE_W    = np.array([30,13,9,2,6,5,4,5,3,3,2,2,2,3,2], dtype=float)
_STATE_W   /= _STATE_W.sum()
_REGIONS    = sorted(set(_STATES.values()))

_FUNNEL = [
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
_DEVICES = ["mobile", "desktop", "tablet"]
_DEV_W   = np.array([58, 35, 7], dtype=float) / 100

# ── Cache (calculado uma vez por processo) ───────────────────────────────────
_CACHE: dict = {}


def _rng() -> np.random.Generator:
    """Novo gerador com semente fixa — garante dados iguais a cada refresh."""
    return np.random.default_rng(_SEED)


def _all_dates() -> list:
    start = _TODAY - timedelta(days=_BASE_DAYS - 1)
    return [start + timedelta(days=i) for i in range(_BASE_DAYS)]


def _day_factor(d: date, start: date) -> float:
    """Tendência semanal crescente + sazonalidade por dia da semana."""
    wk  = (d - start).days // 7
    dow = d.isoweekday()
    t   = 1 + wk * 0.009
    s   = 1.22 if dow >= 6 else (0.86 if dow == 1 else (1.05 if dow == 5 else 1.0))
    return t * s


# ── Série temporal base ───────────────────────────────────────────────────────

def _daily() -> pd.DataFrame:
    """110 dias de receita/pedidos diários — base para todas as outras funções."""
    if "daily" in _CACHE:
        return _CACHE["daily"]

    rng   = _rng()
    dates = _all_dates()
    start = dates[0]
    idx   = np.arange(len(dates))

    factors  = np.array([_day_factor(d, start) for d in dates])
    base_n   = (260 * factors).astype(int)
    n_orders = np.maximum(80, (base_n * (1 + rng.normal(0, 0.10, len(dates)))).astype(int))

    avg_price = 148.0 + idx * 0.05 + rng.normal(0, 8, len(dates))
    revenue   = np.round(n_orders * avg_price, 2)

    df = pd.DataFrame({
        "day":             dates,
        "n_orders":        n_orders,
        "total_revenue":   revenue,
        "avg_order_value": np.round(avg_price, 2),
    })
    _CACHE["daily"] = df
    return df


# ── Funções públicas (mesma interface de data.py) ─────────────────────────────

def get_executive(days: int = 90) -> pd.DataFrame:
    key = f"exec_{days}"
    if key in _CACHE:
        return _CACHE[key]

    rng    = _rng()
    b      = _daily().copy()
    cutoff = _TODAY - timedelta(days=days - 1)
    b      = b[b["day"] >= cutoff].copy()

    b["total_customers"] = np.maximum(
        1, (b["n_orders"] * rng.uniform(0.82, 0.97, len(b))).astype(int)
    )
    result = b.rename(columns={"n_orders": "total_orders"})[
        ["day", "total_orders", "total_customers", "total_revenue", "avg_order_value"]
    ]
    _CACHE[key] = result
    return result


def get_sales_performance(days: int = 90) -> pd.DataFrame:
    key = f"sales_{days}"
    if key in _CACHE:
        return _CACHE[key]

    rng    = _rng()
    b      = _daily()
    cutoff = _TODAY - timedelta(days=days - 1)
    b      = b[b["day"] >= cutoff]

    rows = []
    for _, row in b.iterrows():
        d       = row["day"]
        day_rev = float(row["total_revenue"])
        day_ord = int(row["n_orders"])

        cat_rev_sh = rng.dirichlet(_CAT_W * 20)
        cat_ord_sh = rng.dirichlet(_CAT_W * 20)

        for ci, cat in enumerate(CATEGORIES):
            cat_rev = day_rev * float(cat_rev_sh[ci])
            cat_ord = max(0, int(round(day_ord * float(cat_ord_sh[ci]))))
            if cat_ord == 0:
                continue

            st_sh = rng.dirichlet(_STATE_W * 15)
            for si, state in enumerate(_STATE_LIST):
                s_rev = cat_rev * float(st_sh[si])
                s_ord = max(0, int(round(cat_ord * float(st_sh[si]))))
                if s_ord == 0:
                    continue
                ff = float(rng.uniform(0.08, 0.22))
                rows.append({
                    "purchase_date":   d,
                    "category":        cat,
                    "state":           state,
                    "region":          _STATES[state],
                    "orders":          s_ord,
                    "customers":       max(1, int(s_ord * float(rng.uniform(0.82, 0.97)))),
                    "revenue":         round(s_rev, 2),
                    "avg_order_value": round(s_rev / s_ord, 2),
                    "product_revenue": round(s_rev * (1 - ff), 2),
                    "freight_revenue": round(s_rev * ff, 2),
                })

    result = pd.DataFrame(rows)
    _CACHE[key] = result
    return result


def get_funnel(days: int = 7) -> pd.DataFrame:
    key = f"funnel_{days}"
    if key in _CACHE:
        return _CACHE[key]

    rng    = _rng()
    cutoff = _TODAY - timedelta(days=days - 1)
    dates  = [d for d in _all_dates() if d >= cutoff]
    start  = _all_dates()[0]
    rows   = []

    for d in dates:
        f         = _day_factor(d, start)
        base_sess = int(130 * f)
        is_wknd   = d.isoweekday() >= 6

        for ev_type, rate in _FUNNEL:
            n_ev = max(1, int(base_sess * rate * (1 + float(rng.normal(0, 0.08)))))
            dev_counts = np.round(rng.dirichlet(_DEV_W * 30) * n_ev).astype(int)

            for di, dev in enumerate(_DEVICES):
                n_dev = int(dev_counts[di])
                if n_dev == 0:
                    continue
                sess  = max(1, int(n_dev * float(rng.uniform(0.55, 0.85))))
                users = max(1, int(sess  * float(rng.uniform(0.80, 1.00))))
                rows.append({
                    "hour":        datetime.datetime(d.year, d.month, d.day, 12),
                    "event_date":  d,
                    "is_weekend":  is_wknd,
                    "event_type":  ev_type,
                    "device":      dev,
                    "category":    CATEGORIES[int(rng.integers(0, len(CATEGORIES)))],
                    "event_count": n_dev,
                    "sessions":    sess,
                    "users":       users,
                })

    result = pd.DataFrame(rows)
    _CACHE[key] = result
    return result


def get_reviews(days: int = 90) -> pd.DataFrame:
    key = f"reviews_{days}"
    if key in _CACHE:
        return _CACHE[key]

    rng    = _rng()
    cutoff = _TODAY - timedelta(days=days - 1)
    dates  = [d for d in _all_dates() if d >= cutoff]

    SENT_W    = np.array([70, 16, 14], dtype=float) / 100
    SENT_BASE = {"positive": 4.3, "neutral": 3.0, "negative": 1.8}
    sentiments = ["positive", "neutral", "negative"]
    rows = []

    for d in dates:
        n  = int(rng.integers(50, 90))
        sc = np.round(rng.dirichlet(SENT_W * 30) * n).astype(int)

        for si, sent in enumerate(sentiments):
            if sc[si] == 0:
                continue
            cat_sh = rng.dirichlet(_CAT_W * 10)

            for ci, cat in enumerate(CATEGORIES):
                cc = max(0, int(round(sc[si] * float(cat_sh[ci]))))
                if cc == 0:
                    continue
                st_sh = rng.dirichlet(_STATE_W * 10)

                for sti, state in enumerate(_STATE_LIST):
                    s_rc = max(0, int(round(cc * float(st_sh[sti]))))
                    if s_rc == 0:
                        continue
                    avg_r = float(SENT_BASE[sent]) + float(rng.normal(0, 0.3))
                    rows.append({
                        "review_date":     d,
                        "sentiment":       sent,
                        "category":        cat,
                        "state":           state,
                        "region":          _STATES[state],
                        "review_count":    s_rc,
                        "avg_rating":      round(max(1.0, min(5.0, avg_r)), 2),
                        "avg_text_length": round(float(rng.uniform(40, 280)), 1),
                    })

    result = pd.DataFrame(rows)
    _CACHE[key] = result
    return result


def get_trends(days: int = 60) -> pd.DataFrame:
    key = f"trends_{days}"
    if key in _CACHE:
        return _CACHE[key]

    b = _daily().copy().sort_values("day").reset_index(drop=True)
    b["orders"]  = b["n_orders"]
    b["revenue"] = b["total_revenue"]

    b["orders_7d_ago"]   = b["orders"].shift(7)
    b["revenue_7d_ago"]  = b["revenue"].shift(7)
    b["revenue_14d_ago"] = b["revenue"].shift(14)

    b["orders_growth_pct"] = np.where(
        b["orders_7d_ago"] > 0,
        (b["orders"] - b["orders_7d_ago"]) / b["orders_7d_ago"] * 100,
        np.nan)
    b["revenue_growth_pct"] = np.where(
        b["revenue_7d_ago"] > 0,
        (b["revenue"] - b["revenue_7d_ago"]) / b["revenue_7d_ago"] * 100,
        np.nan)
    b["revenue_acceleration"] = np.where(
        (b["revenue_7d_ago"] > 0) & (b["revenue_14d_ago"] > 0),
        ((b["revenue"] - b["revenue_7d_ago"]) / b["revenue_7d_ago"]) -
        ((b["revenue_7d_ago"] - b["revenue_14d_ago"]) / b["revenue_14d_ago"]),
        np.nan)

    cutoff = _TODAY - timedelta(days=days - 1)
    b = b[b["day"] >= cutoff].rename(columns={"day": "purchase_date"})
    result = b[["purchase_date", "orders", "revenue",
                "orders_growth_pct", "revenue_growth_pct", "revenue_acceleration"]]
    _CACHE[key] = result
    return result


def get_category_trends(days: int = 30) -> pd.DataFrame:
    key = f"cat_trends_{days}"
    if key in _CACHE:
        return _CACHE[key]

    rng      = _rng()
    need_from = _TODAY - timedelta(days=days + 6)  # +7 para o lag
    b        = _daily()[_daily()["day"] >= need_from]

    rows = []
    for _, row in b.iterrows():
        d       = row["day"]
        day_rev = float(row["total_revenue"])
        day_ord = int(row["n_orders"])
        cat_rev = rng.dirichlet(_CAT_W * 20) * day_rev
        cat_ord = np.round(rng.dirichlet(_CAT_W * 20) * day_ord).astype(int)
        for ci, cat in enumerate(CATEGORIES):
            rows.append({
                "purchase_date": d,
                "category":      cat,
                "orders":        int(cat_ord[ci]),
                "revenue":       round(float(cat_rev[ci]), 2),
            })

    df = pd.DataFrame(rows).sort_values(["category", "purchase_date"])
    df["orders_7d_ago"]  = df.groupby("category")["orders"].shift(7)
    df["revenue_7d_ago"] = df.groupby("category")["revenue"].shift(7)
    df["orders_growth_pct"] = np.where(
        df["orders_7d_ago"] > 0,
        (df["orders"] - df["orders_7d_ago"]) / df["orders_7d_ago"] * 100,
        np.nan)
    df["revenue_growth_pct"] = np.where(
        df["revenue_7d_ago"] > 0,
        (df["revenue"] - df["revenue_7d_ago"]) / df["revenue_7d_ago"] * 100,
        np.nan)

    cutoff = _TODAY - timedelta(days=days - 1)
    df = df[df["purchase_date"] >= cutoff]
    result = df[["purchase_date", "category", "orders", "revenue",
                 "orders_growth_pct", "revenue_growth_pct"]]
    _CACHE[key] = result
    return result


def get_demand_forecast() -> pd.DataFrame:
    if "forecast" in _CACHE:
        return _CACHE["forecast"]

    rng       = _rng()
    scored_at = datetime.datetime(_TODAY.year, _TODAY.month, _TODAY.day, 3, 0, 0)
    rows = []
    for cat in CATEGORIES:
        rmse = round(float(rng.uniform(3.5, 14.0)), 2)
        mae  = round(rmse * float(rng.uniform(0.58, 0.78)), 2)
        base = int(rng.integers(25, 110))
        for i in range(1, 8):
            fd  = _TODAY + timedelta(days=i)
            adj = 1.18 if fd.isoweekday() >= 6 else 1.0
            pred = round(base * adj * float(rng.uniform(0.88, 1.14)), 1)
            rows.append({
                "category":         cat,
                "forecast_date":    fd,
                "predicted_orders": pred,
                "model_rmse":       rmse,
                "model_mae":        mae,
                "scored_at":        scored_at,
            })

    result = pd.DataFrame(rows)
    _CACHE["forecast"] = result
    return result
