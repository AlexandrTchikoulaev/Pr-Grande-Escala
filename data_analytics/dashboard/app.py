"""
TrendMart Analytics Dashboard.

Dash + Plotly app que consulta as views Trino (lake.gold.*) e apresenta
4 abas de análise: Executivo, Vendas, Funil de Eventos, Reviews.

Acesso local: http://localhost:8050
"""

import os

import dash
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from dash import dcc, html, Input, Output

from data import (
    get_executive, get_sales_performance, get_funnel, get_reviews,
    get_demand_forecast, get_churn_scores,
)

# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------
REFRESH_MS = 5 * 60 * 1000  # auto-refresh a cada 5 minutos

FUNNEL_EVENT_ORDER = [
    "session_start", "search", "product_view",
    "add_to_cart", "remove_from_cart", "checkout",
    "order_placed", "payment_failed",
    "wishlist_add", "product_review", "session_end",
]

SENTIMENT_COLORS = {
    "positive": "#198754",
    "neutral":  "#ffc107",
    "negative": "#dc3545",
}

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    title="TrendMart Analytics",
)

app.layout = dbc.Container([

    # ── Cabeçalho ────────────────────────────────────────────────────────
    dbc.Row([
        dbc.Col([
            html.H2("TrendMart Analytics", className="text-primary mb-0"),
            html.Small(
                "Kafka · Debezium · Spark · Iceberg · Trino",
                className="text-muted",
            ),
        ], md=8),
        dbc.Col([
            html.Div(id="last-refresh", className="text-end text-muted small mt-2"),
        ], md=4),
    ], className="py-3 border-bottom mb-3"),

    # ── Auto-refresh ─────────────────────────────────────────────────────
    dcc.Interval(id="tick", interval=REFRESH_MS, n_intervals=0),

    # ── Tabs ─────────────────────────────────────────────────────────────
    dbc.Tabs([
        dbc.Tab(label="Executivo",   tab_id="executive"),
        dbc.Tab(label="Vendas",      tab_id="sales"),
        dbc.Tab(label="Funil",       tab_id="funnel"),
        dbc.Tab(label="Reviews",     tab_id="reviews"),
        dbc.Tab(label="ML Insights", tab_id="ml"),
    ], id="tabs", active_tab="executive", className="mb-3"),

    html.Div(id="tab-content"),

], fluid=True)


# ---------------------------------------------------------------------------
# Callback principal
# ---------------------------------------------------------------------------
@app.callback(
    Output("tab-content",    "children"),
    Output("last-refresh",   "children"),
    Input("tabs",  "active_tab"),
    Input("tick",  "n_intervals"),
)
def render_tab(active_tab, _):
    import datetime
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    label = f"Actualizado às {ts}"

    renderers = {
        "executive": _tab_executive,
        "sales":     _tab_sales,
        "funnel":    _tab_funnel,
        "reviews":   _tab_reviews,
        "ml":        _tab_ml,
    }
    content = renderers.get(active_tab, lambda: html.Div())()
    return content, label


# ---------------------------------------------------------------------------
# Helpers UI
# ---------------------------------------------------------------------------
def _no_data():
    return dbc.Alert(
        "Sem dados disponíveis — o pipeline ainda não gerou registos para este período.",
        color="warning", className="mt-3",
    )


def _kpi(title: str, value: str, color: str = "primary"):
    return dbc.Card(dbc.CardBody([
        html.P(title, className="text-muted small mb-1"),
        html.H4(value, className=f"text-{color} mb-0"),
    ]), className="text-center shadow-sm h-100")


# ---------------------------------------------------------------------------
# Aba: Executivo
# ---------------------------------------------------------------------------
def _tab_executive():
    df = get_executive(90)
    if df.empty:
        return _no_data()

    df["day"] = pd.to_datetime(df["day"])
    for col in ("total_orders", "total_customers", "positive_reviews", "negative_reviews"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    for col in ("total_revenue", "avg_order_value", "avg_rating"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # KPIs
    rev       = f"€ {df['total_revenue'].sum():,.0f}"
    orders    = f"{int(df['total_orders'].sum()):,}"
    aov       = f"€ {df['avg_order_value'].mean():.2f}"
    rating    = f"{df['avg_rating'].mean():.2f} / 5"
    customers = f"{int(df['total_customers'].sum()):,}"

    kpis = dbc.Row([
        dbc.Col(_kpi("Receita Total",        rev,       "success"), md=True, className="mb-3"),
        dbc.Col(_kpi("Pedidos",              orders,    "primary"), md=True, className="mb-3"),
        dbc.Col(_kpi("Valor Médio Pedido",   aov,       "info"),    md=True, className="mb-3"),
        dbc.Col(_kpi("Rating Médio",         rating,    "warning"), md=True, className="mb-3"),
        dbc.Col(_kpi("Clientes Únicos",      customers, "secondary"), md=True, className="mb-3"),
    ], className="g-3 mb-3")

    # Receita diária
    fig_rev = px.area(
        df, x="day", y="total_revenue",
        title="Receita Diária (€)",
        labels={"total_revenue": "Receita (€)", "day": ""},
        color_discrete_sequence=["#0d6efd"],
    )
    fig_rev.update_layout(showlegend=False, margin=dict(t=40, b=20))

    # Pedidos por dia
    fig_ord = px.bar(
        df, x="day", y="total_orders",
        title="Pedidos por Dia",
        labels={"total_orders": "Pedidos", "day": ""},
        color_discrete_sequence=["#198754"],
    )
    fig_ord.update_layout(showlegend=False, margin=dict(t=40, b=20))

    # Reviews positivas vs negativas
    df_rev = df[["day", "positive_reviews", "negative_reviews"]].melt(
        id_vars="day", var_name="tipo", value_name="count"
    )
    df_rev["tipo"] = df_rev["tipo"].map({
        "positive_reviews": "Positivas",
        "negative_reviews": "Negativas",
    })
    fig_reviews = px.bar(
        df_rev, x="day", y="count", color="tipo",
        title="Reviews por Dia",
        labels={"count": "Reviews", "day": "", "tipo": ""},
        color_discrete_map={"Positivas": "#198754", "Negativas": "#dc3545"},
        barmode="stack",
    )
    fig_reviews.update_layout(margin=dict(t=40, b=20))

    # Rating médio diário
    fig_rating = px.line(
        df, x="day", y="avg_rating",
        title="Rating Médio Diário",
        labels={"avg_rating": "Rating", "day": ""},
        color_discrete_sequence=["#fd7e14"],
    )
    fig_rating.update_layout(
        yaxis=dict(range=[0, 5]),
        showlegend=False,
        margin=dict(t=40, b=20),
    )

    return html.Div([
        kpis,
        dbc.Row([
            dbc.Col(dcc.Graph(figure=fig_rev),     md=8),
            dbc.Col(dcc.Graph(figure=fig_rating),  md=4),
        ], className="mb-2"),
        dbc.Row([
            dbc.Col(dcc.Graph(figure=fig_ord),     md=6),
            dbc.Col(dcc.Graph(figure=fig_reviews), md=6),
        ]),
    ])


# ---------------------------------------------------------------------------
# Aba: Vendas
# ---------------------------------------------------------------------------
def _tab_sales():
    df = get_sales_performance(90)
    if df.empty:
        return _no_data()

    df["purchase_date"] = pd.to_datetime(df["purchase_date"])
    for col in ("orders", "customers"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    for col in ("revenue", "avg_order_value", "product_revenue", "freight_revenue"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Top 10 categorias por receita
    cat_df = (
        df.groupby("category", as_index=False)["revenue"]
        .sum()
        .nlargest(10, "revenue")
        .sort_values("revenue")
    )
    fig_cat = px.bar(
        cat_df, x="revenue", y="category", orientation="h",
        title="Top 10 Categorias por Receita",
        labels={"revenue": "Receita (€)", "category": ""},
        color_discrete_sequence=["#0d6efd"],
    )
    fig_cat.update_layout(margin=dict(t=40, b=20))

    # Receita por região (donut)
    region_df = df.groupby("region", as_index=False)["revenue"].sum()
    fig_region = px.pie(
        region_df, values="revenue", names="region",
        title="Receita por Região",
        hole=0.45,
    )
    fig_region.update_layout(margin=dict(t=40, b=20))

    # Evolução temporal
    daily_df = (
        df.groupby("purchase_date", as_index=False)
        .agg(revenue=("revenue", "sum"), orders=("orders", "sum"))
        .sort_values("purchase_date")
    )
    fig_trend = px.line(
        daily_df, x="purchase_date", y="revenue",
        title="Evolução da Receita",
        labels={"revenue": "Receita (€)", "purchase_date": ""},
        color_discrete_sequence=["#6610f2"],
    )
    fig_trend.update_layout(showlegend=False, margin=dict(t=40, b=20))

    # Top 10 estados
    state_df = (
        df.groupby("state", as_index=False)["revenue"]
        .sum()
        .nlargest(10, "revenue")
        .sort_values("revenue", ascending=False)
    )
    fig_state = px.bar(
        state_df, x="state", y="revenue",
        title="Top 10 Estados por Receita",
        labels={"revenue": "Receita (€)", "state": "Estado"},
        color_discrete_sequence=["#20c997"],
    )
    fig_state.update_layout(margin=dict(t=40, b=20))

    return html.Div([
        dbc.Row([
            dbc.Col(dcc.Graph(figure=fig_cat),    md=7),
            dbc.Col(dcc.Graph(figure=fig_region), md=5),
        ], className="mb-2"),
        dbc.Row([
            dbc.Col(dcc.Graph(figure=fig_trend),  md=8),
            dbc.Col(dcc.Graph(figure=fig_state),  md=4),
        ]),
    ])


# ---------------------------------------------------------------------------
# Aba: Funil de Eventos
# ---------------------------------------------------------------------------
def _tab_funnel():
    df = get_funnel(7)
    if df.empty:
        return _no_data()

    df["event_date"] = pd.to_datetime(df["event_date"])
    for col in ("event_count", "sessions", "users"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Funil por tipo de evento (na ordem do journey)
    funnel_df = df.groupby("event_type", as_index=False)["event_count"].sum()
    funnel_df["_order"] = funnel_df["event_type"].map(
        {e: i for i, e in enumerate(FUNNEL_EVENT_ORDER)}
    )
    funnel_df = funnel_df.dropna(subset=["_order"]).sort_values("_order")
    fig_funnel = go.Figure(go.Funnel(
        y=funnel_df["event_type"],
        x=funnel_df["event_count"],
        textinfo="value+percent initial",
        marker=dict(color="#0d6efd"),
    ))
    fig_funnel.update_layout(title="Funil de Eventos (7 dias)", margin=dict(t=40, b=20))

    # Distribuição por dispositivo
    dev_df = df.groupby("device", as_index=False)["event_count"].sum()
    fig_dev = px.pie(
        dev_df, values="event_count", names="device",
        title="Eventos por Dispositivo",
        hole=0.45,
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig_dev.update_layout(margin=dict(t=40, b=20))

    # Top 5 eventos ao longo do tempo
    time_df = (
        df.groupby(["event_date", "event_type"], as_index=False)["event_count"]
        .sum()
    )
    top5 = (
        time_df.groupby("event_type")["event_count"]
        .sum()
        .nlargest(5)
        .index
    )
    time_df = time_df[time_df["event_type"].isin(top5)].sort_values("event_date")
    fig_time = px.line(
        time_df, x="event_date", y="event_count", color="event_type",
        title="Top 5 Eventos ao Longo do Tempo",
        labels={"event_count": "Eventos", "event_date": "", "event_type": "Evento"},
    )
    fig_time.update_layout(margin=dict(t=40, b=20))

    # Sessions vs Users por dia
    su_df = (
        df.groupby("event_date", as_index=False)
        .agg(sessions=("sessions", "sum"), users=("users", "sum"))
        .sort_values("event_date")
    )
    fig_su = px.line(
        su_df, x="event_date", y=["sessions", "users"],
        title="Sessões e Utilizadores por Dia",
        labels={"value": "Contagem", "event_date": "", "variable": ""},
    )
    fig_su.update_layout(margin=dict(t=40, b=20))

    return html.Div([
        dbc.Row([
            dbc.Col(dcc.Graph(figure=fig_funnel), md=6),
            dbc.Col(dcc.Graph(figure=fig_dev),    md=6),
        ], className="mb-2"),
        dbc.Row([
            dbc.Col(dcc.Graph(figure=fig_time),   md=8),
            dbc.Col(dcc.Graph(figure=fig_su),     md=4),
        ]),
    ])


# ---------------------------------------------------------------------------
# Aba: Reviews
# ---------------------------------------------------------------------------
def _tab_reviews():
    df = get_reviews(90)
    if df.empty:
        return _no_data()

    df["review_date"] = pd.to_datetime(df["review_date"])
    for col in ("review_count",):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    for col in ("avg_rating", "avg_text_length"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Distribuição de sentimento
    sent_df = df.groupby("sentiment", as_index=False)["review_count"].sum()
    fig_sent = px.pie(
        sent_df, values="review_count", names="sentiment",
        title="Distribuição de Sentimento",
        color="sentiment",
        color_discrete_map=SENTIMENT_COLORS,
        hole=0.45,
    )
    fig_sent.update_layout(margin=dict(t=40, b=20))

    # Rating médio ao longo do tempo (média ponderada)
    daily_df = (
        df.groupby("review_date")
        .apply(lambda x: pd.Series({
            "avg_rating":  (x["avg_rating"] * x["review_count"]).sum() / x["review_count"].sum()
                           if x["review_count"].sum() > 0 else 0,
            "review_count": x["review_count"].sum(),
        }))
        .reset_index()
        .sort_values("review_date")
    )
    fig_rating = px.line(
        daily_df, x="review_date", y="avg_rating",
        title="Rating Médio ao Longo do Tempo",
        labels={"avg_rating": "Rating", "review_date": ""},
        color_discrete_sequence=["#fd7e14"],
    )
    fig_rating.update_layout(yaxis=dict(range=[1, 5]), showlegend=False, margin=dict(t=40, b=20))
    fig_rating.add_hline(y=3, line_dash="dot", line_color="gray", annotation_text="Neutro (3)")

    # Top 10 categorias por nº de reviews (colorido por rating)
    cat_df = (
        df.groupby("category", as_index=False)
        .agg(review_count=("review_count", "sum"), avg_rating=("avg_rating", "mean"))
        .nlargest(10, "review_count")
        .sort_values("review_count")
    )
    fig_cat = px.bar(
        cat_df, x="review_count", y="category", orientation="h",
        color="avg_rating",
        color_continuous_scale="RdYlGn",
        range_color=[1, 5],
        title="Top 10 Categorias por Nº de Reviews",
        labels={"review_count": "Reviews", "category": "", "avg_rating": "Rating"},
    )
    fig_cat.update_layout(margin=dict(t=40, b=20))

    # Sentimento por região
    region_df = df.groupby(["region", "sentiment"], as_index=False)["review_count"].sum()
    fig_region = px.bar(
        region_df, x="region", y="review_count", color="sentiment",
        title="Sentimento por Região",
        color_discrete_map=SENTIMENT_COLORS,
        labels={"review_count": "Reviews", "region": "Região", "sentiment": ""},
    )
    fig_region.update_layout(margin=dict(t=40, b=20))

    return html.Div([
        dbc.Row([
            dbc.Col(dcc.Graph(figure=fig_sent),   md=4),
            dbc.Col(dcc.Graph(figure=fig_rating), md=8),
        ], className="mb-2"),
        dbc.Row([
            dbc.Col(dcc.Graph(figure=fig_cat),    md=6),
            dbc.Col(dcc.Graph(figure=fig_region), md=6),
        ]),
    ])


# ---------------------------------------------------------------------------
# Aba: ML Insights
# ---------------------------------------------------------------------------
def _tab_ml():
    df_forecast = get_demand_forecast()
    df_churn    = get_churn_scores()

    sections = []

    # ── Previsão de Procura ───────────────────────────────────────────────
    if df_forecast.empty:
        sections.append(dbc.Alert(
            "Previsão de procura ainda não disponível — aguardar o pipeline ML (trendmart_ml_pipeline).",
            color="info", className="mt-2",
        ))
    else:
        df_forecast["forecast_date"] = pd.to_datetime(df_forecast["forecast_date"])
        df_forecast["predicted_orders"] = pd.to_numeric(
            df_forecast["predicted_orders"], errors="coerce"
        ).fillna(0.0)

        fig_forecast = px.line(
            df_forecast, x="forecast_date", y="predicted_orders", color="category",
            title="Previsão de Procura — Próximos 7 Dias (por Categoria)",
            labels={"predicted_orders": "Pedidos Previstos", "forecast_date": "Data", "category": "Categoria"},
            markers=True,
        )
        fig_forecast.update_layout(margin=dict(t=40, b=20))

        rmse = df_forecast["model_rmse"].iloc[0] if "model_rmse" in df_forecast.columns else None
        mae  = df_forecast["model_mae"].iloc[0]  if "model_mae"  in df_forecast.columns else None
        metric_text = []
        if rmse is not None:
            metric_text.append(f"RMSE: {float(rmse):.2f}")
        if mae is not None:
            metric_text.append(f"MAE: {float(mae):.2f}")

        sections.append(html.Div([
            html.H5("Previsão de Procura por Categoria", className="mt-3 mb-1"),
            html.Small(
                " · ".join(metric_text) if metric_text else "",
                className="text-muted",
            ),
            dcc.Graph(figure=fig_forecast),
        ]))

    # ── Risco de Churn ────────────────────────────────────────────────────
    if df_churn.empty:
        sections.append(dbc.Alert(
            "Scores de churn ainda não disponíveis — aguardar o pipeline ML.",
            color="info", className="mt-2",
        ))
    else:
        df_churn["total_customers"] = pd.to_numeric(df_churn["total_customers"], errors="coerce").fillna(0)
        df_churn["avg_churn_prob"]  = pd.to_numeric(df_churn["avg_churn_prob"],  errors="coerce").fillna(0.0)
        df_churn["model_f1"]        = pd.to_numeric(df_churn["model_f1"],        errors="coerce").fillna(0.0)
        df_churn["model_auc"]       = pd.to_numeric(df_churn["model_auc"],       errors="coerce").fillna(0.0)

        risk_colors = {"high": "#dc3545", "medium": "#ffc107", "low": "#198754"}

        fig_donut = px.pie(
            df_churn, values="total_customers", names="risk_label",
            title="Distribuição de Risco de Churn",
            color="risk_label",
            color_discrete_map=risk_colors,
            hole=0.45,
        )
        fig_donut.update_layout(margin=dict(t=40, b=20))

        fig_bar = px.bar(
            df_churn.sort_values("total_customers", ascending=False),
            x="risk_label", y="total_customers",
            color="risk_label",
            color_discrete_map=risk_colors,
            title="Clientes por Nível de Risco",
            labels={"total_customers": "Clientes", "risk_label": "Risco"},
            text="total_customers",
        )
        fig_bar.update_layout(showlegend=False, margin=dict(t=40, b=20))

        f1  = df_churn["model_f1"].iloc[0]
        auc = df_churn["model_auc"].iloc[0]

        kpi_row = dbc.Row([
            dbc.Col(_kpi("Modelo F1",  f"{f1:.4f}",  "primary"),  md=3, className="mb-3"),
            dbc.Col(_kpi("Modelo AUC", f"{auc:.4f}", "success"),  md=3, className="mb-3"),
            dbc.Col(_kpi("Clientes High Risk",
                         f"{int(df_churn[df_churn['risk_label']=='high']['total_customers'].sum()):,}",
                         "danger"), md=3, className="mb-3"),
            dbc.Col(_kpi("Clientes Low Risk",
                         f"{int(df_churn[df_churn['risk_label']=='low']['total_customers'].sum()):,}",
                         "success"), md=3, className="mb-3"),
        ], className="g-3")

        sections.append(html.Div([
            html.H5("Previsão de Churn de Clientes", className="mt-4 mb-2"),
            kpi_row,
            dbc.Row([
                dbc.Col(dcc.Graph(figure=fig_donut), md=5),
                dbc.Col(dcc.Graph(figure=fig_bar),   md=7),
            ]),
        ]))

    return html.Div(sections)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    port  = int(os.getenv("DASHBOARD_PORT", "8050"))
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=port, debug=debug)
