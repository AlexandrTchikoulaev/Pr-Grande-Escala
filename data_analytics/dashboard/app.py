"""
TrendMart Analytics Dashboard — 6 abas, refresh a cada 5 minutos.
Acesso: http://localhost:8050
"""

import os
import datetime

import dash
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from dash import dcc, html, Input, Output, State, ctx, dash_table
from dash.exceptions import PreventUpdate

from data_demo import (
    get_executive, get_sales_performance, get_funnel, get_reviews,
    get_demand_forecast, get_trends, get_category_trends,
)

REFRESH_MS = 5 * 60 * 1000
CHART_H    = 340

FUNNEL_EVENT_ORDER = [
    "session_start", "search", "category_browse", "product_view",
    "product_review_read", "add_to_cart", "remove_from_cart",
    "cart_view", "cart_abandon", "checkout_start",
    "order_placed", "session_end",
]

SENTIMENT_COLORS = {
    "positive": "#16a34a",
    "neutral":  "#ca8a04",
    "negative": "#dc2626",
}

# Cor de acento por aba
ACC = {
    "executive": "#dc2626",
    "sales":     "#2563eb",
    "funnel":    "#d97706",
    "reviews":   "#16a34a",
    "trends":    "#0891b2",
    "ml":        "#7c3aed",
}

# Defaults aplicados a todos os gráficos
_FIG_BASE = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter,system-ui,-apple-system,sans-serif", size=12, color="#374151"),
    margin=dict(t=40, b=20, l=12, r=12),
    legend=dict(bgcolor="rgba(0,0,0,0)", font_size=11),
    xaxis=dict(showgrid=False, zeroline=False, linecolor="#e5e7eb"),
    yaxis=dict(gridcolor="#f3f4f6", zeroline=False, fixedrange=True),
)


def _style(fig, h=CHART_H):
    fig.update_layout(**_FIG_BASE, height=h)
    fig.update_layout(title=dict(x=0.5, xanchor="center",
                                 font=dict(size=16, color="#111827", family="Inter,system-ui,sans-serif")))
    return fig


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    title="TrendMart Analytics",
)

app.layout = html.Div([
    # Barra superior com tabs integrados
    html.Div([
        html.Span([
            html.Span("TrendMart", style={"fontWeight": 700, "color": "#f9fafb"}),
            html.Span(" Analytics", style={"color": "#6b7280"}),
        ], style={"fontSize": "0.95rem", "letterSpacing": "0.04em", "whiteSpace": "nowrap"}),
        html.Div(
            dbc.Tabs([
                dbc.Tab(label="Executivo",   tab_id="executive"),
                dbc.Tab(label="Vendas",      tab_id="sales"),
                dbc.Tab(label="Funil",       tab_id="funnel"),
                dbc.Tab(label="Reviews",     tab_id="reviews"),
                dbc.Tab(label="Tendencias",  tab_id="trends"),
                dbc.Tab(label="ML Insights", tab_id="ml"),
            ], id="tabs", active_tab="executive", className="border-0"),
            style={"flex": "1", "display": "flex", "justifyContent": "center",
                   "margin": "0 32px"},
        ),
        html.Span(id="last-refresh",
                  style={"color": "#6b7280", "fontSize": "0.76rem", "whiteSpace": "nowrap"}),
    ], style={
        "display": "flex", "justifyContent": "space-between", "alignItems": "center",
        "padding": "0 28px", "background": "#111827",
        "position": "sticky", "top": 0, "zIndex": 200, "minHeight": "52px",
    }),

    dcc.Interval(id="tick", interval=REFRESH_MS, n_intervals=0),
    dcc.Store(id="drill-filter", data={"level": 1, "year": None, "month": None}),

    # Barra de granularidade
    html.Div([
        html.Span("Vista:", style={
            "fontSize": "0.75rem", "color": "#9ca3af",
            "marginRight": "8px", "fontWeight": 500,
        }),
        html.Button("Ano", id="gran-btn-0", n_clicks=0),
        html.Button("Mes", id="gran-btn-1", n_clicks=0),
        html.Button("Dia", id="gran-btn-2", n_clicks=0),
    ], id="gran-bar", style={
        "display": "flex", "alignItems": "center",
        "padding": "6px 28px", "background": "#f9fafb",
        "borderBottom": "1px solid #f3f4f6", "gap": "4px",
    }),

    html.Div(id="tab-content",
             style={"padding": "28px", "maxWidth": "1380px", "margin": "0 auto"}),

], style={
    "background": "#f9fafb", "minHeight": "100vh",
    "fontFamily": "Inter,system-ui,-apple-system,sans-serif",
})


_GRAN_LABELS = ["Ano", "Mes", "Dia"]
_MONTH_PT    = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]
_MONTH_MAP   = {n: i + 1 for i, n in enumerate(_MONTH_PT)}

_BTN_BASE = {
    "fontSize": "0.75rem", "padding": "3px 12px",
    "border": "1px solid #e5e7eb", "borderRadius": "4px",
    "cursor": "pointer", "fontFamily": "Inter,system-ui,sans-serif",
}
_BTN_ON   = {**_BTN_BASE, "background": "#1d4ed8", "color": "#fff",   "borderColor": "#1d4ed8"}
_BTN_OFF  = {**_BTN_BASE, "background": "#f9fafb", "color": "#6b7280", "borderColor": "#e5e7eb"}
_BTN_BACK = {**_BTN_BASE, "background": "#f3f4f6", "color": "#374151", "borderColor": "#d1d5db",
             "marginLeft": "12px"}


@app.callback(
    Output("drill-filter", "data"),
    Output("gran-btn-0", "style"),
    Output("gran-btn-1", "style"),
    Output("gran-btn-2", "style"),
    Input("gran-btn-0", "n_clicks"),
    Input("gran-btn-1", "n_clicks"),
    Input("gran-btn-2", "n_clicks"),
    State("drill-filter", "data"),
    prevent_initial_call=True,
)
def _manage_drill(n0, n1, n2, drill):
    tid = ctx.triggered_id
    if tid is None or (ctx.triggered and ctx.triggered[0]["value"] == 0):
        raise PreventUpdate
    level = {"gran-btn-0": 0, "gran-btn-1": 1, "gran-btn-2": 2}[tid]
    new = {"level": level, "year": None, "month": None}
    btn_styles = [_BTN_ON if i == level else _BTN_OFF for i in range(3)]
    return new, *btn_styles


@app.callback(
    Output("tab-content",  "children"),
    Output("last-refresh", "children"),
    Input("tabs", "active_tab"),
    Input("tick", "n_intervals"),
    Input("drill-filter", "data"),
)
def render_tab(active_tab, _, drill_filter):
    ts    = datetime.datetime.now().strftime("%H:%M:%S")
    drill_filter = drill_filter or {"level": 1, "year": None, "month": None}
    gran  = drill_filter.get("level", 1)
    renderers = {
        "executive": lambda: _tab_executive(gran),
        "sales":     lambda: _tab_sales(gran),
        "funnel":    _tab_funnel,
        "reviews":   lambda: _tab_reviews(),
        "trends":    _tab_trends,
        "ml":        _tab_ml,
    }
    return renderers.get(active_tab, lambda: html.Div())(), f"atualizado às {ts}"


# ---------------------------------------------------------------------------
# Helpers de UI
# ---------------------------------------------------------------------------
def _no_data():
    return html.Div(
        "Sem dados — o pipeline ainda não gerou registos para este período.",
        style={"padding": "60px 0", "textAlign": "center",
               "color": "#9ca3af", "fontSize": "0.9rem"},
    )


def _tab_header(profile, period, rfs, questions, accent):
    return html.Div(
        html.Span(profile,
                  style={"fontWeight": 700, "fontSize": "0.92rem", "color": accent}),
        style={
            "background": "#fff",
            "borderLeft": f"3px solid {accent}",
            "padding": "12px 18px",
            "borderRadius": "0 4px 4px 0",
            "marginBottom": "24px",
        }
    )


def _kpi(label, value, color="#2563eb"):
    """Bloco KPI — funciona dentro de _kpi_row (flex) ou dbc.Col (Bootstrap)."""
    return html.Div([
        html.Div(value, style={"fontSize": "1.5rem", "fontWeight": 700,
                               "color": color, "lineHeight": 1.1}),
        html.Div(label, style={"fontSize": "0.68rem", "color": "#9ca3af",
                                "textTransform": "uppercase",
                                "letterSpacing": "0.06em", "marginTop": "5px"}),
    ], style={
        "flex": "1", "padding": "16px 20px",
        "background": "#fff",
        "borderTop": f"3px solid {color}",
        "borderRadius": "4px",
    })


def _kpi_row(*kpis):
    return html.Div(list(kpis),
                    style={"display": "flex", "gap": "12px", "marginBottom": "24px"})


def _chart(fig, question, rf=None, h=CHART_H):
    _style(fig, h)
    rf_tag = f"  [{rf}]" if rf else ""
    return html.Div([
        dcc.Graph(figure=fig, config={
            "displayModeBar": "hover",
            "displaylogo": False,
            "modeBarButtons": [["resetScale2d"]],
        }),
        html.Div(f"{question}{rf_tag}", style={
            "textAlign": "center", "fontSize": "0.74rem", "color": "#9ca3af",
            "fontStyle": "italic", "padding": "2px 14px 10px",
        }),
    ], style={
        "background": "#fff", "borderRadius": "4px",
        "padding": "14px 14px 0", "marginBottom": "16px",
    })


def _rgba(hex_color: str, alpha: float = 0.10) -> str:
    h = hex_color.lstrip("#")
    return f"rgba({int(h[0:2],16)},{int(h[2:4],16)},{int(h[4:6],16)},{alpha})"


_GRAN_MENU = dict(
    type="buttons", direction="right",
    bgcolor="#f3f4f6", bordercolor="#e5e7eb", borderwidth=1,
    font=dict(size=10, color="#6b7280"),
    pad=dict(r=6, t=3, b=3, l=6),
    x=1.0, y=1.01, xanchor="right", yanchor="bottom",
)


def _temporal_chart(fig, question, rf=None, h=CHART_H, graph_id=None):
    _style(fig, h)
    fig.update_layout(margin=dict(t=48, b=20, l=12, r=12))
    rf_tag = f"  [{rf}]" if rf else ""
    graph_props = dict(figure=fig, config={
        "displayModeBar": "hover",
        "displaylogo": False,
        "modeBarButtons": [["resetScale2d"]],
    })
    if graph_id:
        graph_props["id"] = graph_id
    return html.Div([
        dcc.Graph(**graph_props),
        html.Div(f"{question}{rf_tag}", style={
            "textAlign": "center", "fontSize": "0.74rem", "color": "#9ca3af",
            "fontStyle": "italic", "padding": "2px 14px 10px",
        }),
    ], style={
        "background": "#fff", "borderRadius": "4px",
        "padding": "14px 14px 0", "marginBottom": "16px",
    })


def _mgran_fig(df, date_col, val_col, color,
               agg="sum", title="", hover_prefix="", active=1,
               drill_filter=None):
    """
    Barra simples para a granularidade activa: 0=Ano, 1=Mes, 2=Dia.
    drill_filter = {"year": int|None, "month": int|None} — zoom hierárquico.
    """
    df = df[[date_col, val_col]].copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values(date_col)
    if drill_filter:
        yr = drill_filter.get("year")
        mo = drill_filter.get("month")
        if yr:
            df = df[df[date_col].dt.year == yr]
        if mo:
            df = df[df[date_col].dt.month == mo]
    fn = "sum" if agg == "sum" else "mean"

    if active == 0:   # Ano
        g = df.assign(_k=df[date_col].dt.year).groupby("_k")[val_col].agg(fn).reset_index()
        x_vals   = g["_k"].astype(str).tolist()
        y_vals   = g[val_col].tolist()
        tickangle = 0
    elif active == 1:  # Mes
        g = df.assign(_k=df[date_col].dt.month).groupby("_k")[val_col].agg(fn).reset_index().sort_values("_k")
        x_vals   = [_MONTH_PT[k - 1] for k in g["_k"]]
        y_vals   = g[val_col].tolist()
        tickangle = 0
    else:              # Dia (1-31)
        g = df.assign(_k=df[date_col].dt.day).groupby("_k")[val_col].agg(fn).reset_index().sort_values("_k")
        x_vals   = g["_k"].astype(str).tolist()
        y_vals   = g[val_col].tolist()
        tickangle = -45

    fig = go.Figure(go.Bar(
        x=x_vals, y=y_vals, marker_color=color,
        hovertemplate=f"%{{x}}<br><b>{hover_prefix}%{{y:,.0f}}</b><extra></extra>",
    ))
    fig.update_layout(
        title=title, showlegend=False, hovermode="closest",
        xaxis=dict(showgrid=False, zeroline=False, type="category", tickangle=tickangle),
    )
    return fig


def _table(df: pd.DataFrame, title: str = None, page_size: int = 10):
    """DataTable sortável e filtrável com estilo consistente."""
    cols = [{"name": c, "id": c} for c in df.columns]
    return html.Div([
        *([ html.Div(title, style={
                "fontSize": "0.75rem", "color": "#9ca3af",
                "fontStyle": "italic", "marginBottom": "8px",
            }) ] if title else []),
        dash_table.DataTable(
            data=df.to_dict("records"),
            columns=cols,
            sort_action="native",
            filter_action="native",
            page_size=page_size,
            style_table={"overflowX": "auto"},
            style_cell={
                "padding": "8px 12px", "fontSize": "0.82rem",
                "fontFamily": "Inter,system-ui,sans-serif",
                "border": "1px solid #f3f4f6", "textAlign": "left",
                "minWidth": "80px",
            },
            style_header={
                "fontWeight": 600, "background": "#f9fafb",
                "border": "1px solid #e5e7eb", "color": "#374151",
            },
            style_data_conditional=[
                {"if": {"row_index": "odd"}, "backgroundColor": "#fafafa"},
            ],
            style_filter={"border": "1px solid #e5e7eb"},
        ),
    ], style={
        "background": "#fff", "borderRadius": "4px",
        "padding": "14px", "marginBottom": "16px",
    })


# ---------------------------------------------------------------------------
# Aba: Executivo — Gestor Executivo (RF1.1–RF1.4)
# ---------------------------------------------------------------------------
def _tab_executive(gran=1):
    df = get_executive(850)
    if df.empty:
        return _no_data()

    df["day"] = pd.to_datetime(df["day"])
    for col in ("total_orders", "total_customers"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    for col in ("total_revenue", "avg_order_value"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    acc = ACC["executive"]

    header = _tab_header(
        "Gestor Executivo", "Histórico completo", "RF1.1–RF1.2",
        [
            "Qual é o estado global da plataforma — receita, pedidos, clientes?",
            "Como evoluiu a receita e o volume de encomendas ao longo do tempo?",
        ],
        acc,
    )

    kpis = _kpi_row(
        _kpi("Receita Total",      f"EUR {df['total_revenue'].sum():,.0f}",    "#16a34a"),
        _kpi("Total de Pedidos",   f"{int(df['total_orders'].sum()):,}",       acc),
        _kpi("Valor Medio Pedido", f"EUR {df['avg_order_value'].mean():.2f}", "#0891b2"),
        _kpi("Clientes Unicos",    f"{int(df['total_customers'].sum()):,}",   "#6b7280"),
    )

    fig_rev = _mgran_fig(df, "day", "total_revenue", acc,
                         title="Receita", hover_prefix="EUR ", active=gran)
    fig_rev.update_layout(yaxis=dict(tickprefix="EUR ", tickformat=",.0f", fixedrange=True))

    fig_orders = _mgran_fig(df, "day", "total_orders", "#6b46c1",
                            agg="sum", title="Encomendas", active=gran)
    fig_orders.update_layout(yaxis=dict(tickformat=",.0f", fixedrange=True))

    tbl_src = df.sort_values("day", ascending=False).head(20)
    tbl_df = (
        tbl_src[["day", "total_revenue", "total_orders", "avg_order_value", "total_customers"]]
        .rename(columns={
            "day":             "Dia",
            "total_revenue":   "Receita (EUR)",
            "total_orders":    "Pedidos",
            "avg_order_value": "Ticket Medio (EUR)",
            "total_customers": "Clientes",
        })
        .assign(
            Dia=lambda x: x["Dia"].dt.strftime("%d/%m/%Y"),
            **{"Receita (EUR)":      lambda x: x["Receita (EUR)"].map("EUR {:,.0f}".format)},
            Pedidos=                 lambda x: x["Pedidos"].map("{:,.0f}".format),
            **{"Ticket Medio (EUR)": lambda x: x["Ticket Medio (EUR)"].map("EUR {:,.2f}".format)},
            Clientes=                lambda x: x["Clientes"].map("{:,.0f}".format),
        )
    )

    return html.Div([
        header, kpis,
        dbc.Row([
            dbc.Col(_chart(fig_rev,
                "Como evoluiu a receita ao longo do tempo?", "RF1.2"), md=6),
            dbc.Col(_chart(fig_orders,
                "O volume de encomendas acompanha a receita?", "RF1.1"), md=6),
        ], className="g-3"),
        _table(tbl_df, "Detalhe — top 20 dias"),
    ])


# ---------------------------------------------------------------------------
# Aba: Vendas — Analista de Vendas (RF2.1–RF2.4) — 90 dias
# ---------------------------------------------------------------------------
def _tab_sales(gran=1):
    df = get_sales_performance(90)
    if df.empty:
        return _no_data()

    df["purchase_date"] = pd.to_datetime(df["purchase_date"])
    for col in ("orders", "customers"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    for col in ("revenue", "avg_order_value", "product_revenue", "freight_revenue"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    acc = ACC["sales"]

    header = _tab_header(
        "Analista de Vendas", "Últimos 90 dias", "RF2.1–RF2.4",
        [
            "Quais as 10 categorias com maior contribuição para a receita?",
            "Como se distribui a receita pelas macrorregiões e estados?",
            "Como evoluiu a receita diária ao longo do período?",
            "Em que categorias o custo logístico (frete) pesa mais na receita?",
        ],
        acc,
    )

    cat_df = (
        df.groupby("category", as_index=False)["revenue"]
        .sum().nlargest(10, "revenue").sort_values("revenue")
    )
    fig_cat = px.bar(cat_df, x="revenue", y="category", orientation="h",
        title="Top 10 Categorias por Receita",
        labels={"revenue": "Receita (€)", "category": ""},
        color_discrete_sequence=[acc])
    fig_cat.update_layout(showlegend=False)

    region_df = df.groupby("region", as_index=False)["revenue"].sum()
    fig_region = px.pie(region_df, values="revenue", names="region",
        title="Receita por Macrorregião", hole=0.45)

    state_df = (
        df.groupby("state", as_index=False)["revenue"]
        .sum().nlargest(10, "revenue").sort_values("revenue")
    )
    fig_state = px.bar(state_df, x="revenue", y="state", orientation="h",
        title="Top 10 Estados por Receita",
        labels={"revenue": "Receita (€)", "state": ""},
        color_discrete_sequence=["#0891b2"])
    fig_state.update_layout(showlegend=False)

    decomp_df = (
        df.groupby("category", as_index=False)
        .agg(product_revenue=("product_revenue", "sum"),
             freight_revenue=("freight_revenue", "sum"))
        .assign(total=lambda x: x["product_revenue"] + x["freight_revenue"])
        .nlargest(10, "total").sort_values("total")
    )
    decomp_melt = decomp_df[["category", "product_revenue", "freight_revenue"]].melt(
        id_vars="category", var_name="tipo", value_name="valor")
    decomp_melt["tipo"] = decomp_melt["tipo"].map({
        "product_revenue": "Produto", "freight_revenue": "Frete"})
    fig_decomp = px.bar(decomp_melt, x="valor", y="category", color="tipo",
        orientation="h", barmode="stack",
        title="Decomposição Receita: Produto vs. Frete (Top 10 Categorias)",
        labels={"valor": "Receita (€)", "category": "", "tipo": ""},
        color_discrete_map={"Produto": acc, "Frete": "#fd7e14"})

    return html.Div([
        header,
        dbc.Row([
            dbc.Col(_chart(fig_cat,
                "Quais as 10 categorias com maior contribuição para a receita?", "RF2.1"), md=7),
            dbc.Col(_chart(fig_region,
                "Como se distribui a receita pelas 5 macrorregiões brasileiras?", "RF2.2"), md=5),
        ], className="g-3"),
        dbc.Row([
            dbc.Col(_chart(fig_state,
                "Quais os estados individualmente mais relevantes em receita?", "RF2.2"), md=4),
            dbc.Col(_chart(fig_decomp,
                "Em que categorias o custo logístico representa uma parcela desproporcional da receita?",
                "RF2.4"), md=8),
        ], className="g-3"),
    ])


# ---------------------------------------------------------------------------
# Aba: Funil — Analista de Marketing (RF3.1–RF3.2 · RF3.4–RF3.5) — 7/30 dias
# ---------------------------------------------------------------------------
def _tab_funnel():
    df = get_funnel(7)
    if df.empty:
        return _no_data()

    df["event_date"] = pd.to_datetime(df["event_date"])
    for col in ("event_count", "sessions", "users"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    acc = ACC["funnel"]

    header = _tab_header(
        "Analista de Marketing", "Últimos 7 dias (funil) / 30 dias (sessões e comportamento)", "RF3.1–RF3.2 · RF3.4–RF3.5",
        [
            "Qual a taxa de conversão em cada etapa do funil — onde se perdem mais utilizadores?",
            "Em que dispositivos (mobile, desktop, tablet) ocorrem mais eventos?",
            "Quantas sessões e utilizadores únicos chegam à plataforma por dia?",
            "Como diferem compras, carrinhos e abandonos entre dias úteis e fins de semana?",
        ],
        acc,
    )

    _CONV_STEPS = [
        ("session_start",   "Sessão Iniciada"),
        ("product_view",    "Visualização de Produto"),
        ("add_to_cart",     "Adição ao Carrinho"),
        ("checkout_start",  "Início de Checkout"),
        ("order_placed",    "Compra Concluída"),
    ]
    funnel_df = (
        df[df["event_type"].isin([s for s, _ in _CONV_STEPS])]
        .groupby("event_type", as_index=False)["event_count"].sum()
    )
    step_order = {s: i for i, (s, _) in enumerate(_CONV_STEPS)}
    label_map  = {s: l for s, l in _CONV_STEPS}
    funnel_df["_order"] = funnel_df["event_type"].map(step_order)
    funnel_df = funnel_df.sort_values("_order")
    funnel_df["label"] = funnel_df["event_type"].map(label_map)
    fig_funnel = go.Figure(go.Funnel(
        y=funnel_df["label"],
        x=funnel_df["event_count"],
        textinfo="value+percent previous",
        marker=dict(color=acc),
    ))
    fig_funnel.update_layout(title="Funil de Conversão (7 dias)")

    # Donut por dispositivo com switch Compras / Abandonos
    def _dev_values(event):
        g = (df[df["event_type"] == event]
             .groupby("device", as_index=False)["event_count"].sum())
        return g["event_count"].tolist(), g["device"].tolist()

    vals_order,  lbls_order  = _dev_values("order_placed")
    vals_abandon, lbls_abandon = _dev_values("cart_abandon")
    fig_dev = go.Figure(go.Pie(
        values=vals_order, labels=lbls_order, hole=0.45, name="Compras",
        marker_colors=px.colors.qualitative.Set2,
    ))
    fig_dev.update_layout(
        title="Distribuição por Dispositivo",
        updatemenus=[dict(
            type="buttons", direction="right",
            buttons=[
                dict(label="Compras",   method="restyle",
                     args=[{"values": [vals_order],  "labels": [lbls_order]}]),
                dict(label="Abandonos", method="restyle",
                     args=[{"values": [vals_abandon], "labels": [lbls_abandon]}]),
            ],
            x=0.0, y=1.15, xanchor="left",
            bgcolor="#f3f4f6", bordercolor="#e5e7eb",
            font=dict(size=10, color="#6b7280"),
        )],
    )

    # RF3.4 — Sessões e utilizadores por dia (90 dias)
    df_su = get_funnel(90).copy()
    df_su["event_date"] = pd.to_datetime(df_su["event_date"])
    for col in ("sessions", "users"):
        df_su[col] = pd.to_numeric(df_su[col], errors="coerce").fillna(0)
    su_df = (
        df_su.groupby("event_date", as_index=False)
        .agg(sessions=("sessions", "sum"), users=("users", "sum"))
        .sort_values("event_date")
    )
    fig_su = px.line(su_df, x="event_date", y=["sessions", "users"],
        title="Sessões e Utilizadores por Dia (90 dias)",
        labels={"value": "Contagem", "event_date": "", "variable": ""})

    # RF3.5 — janela de 30 dias (.copy() evita mutar o cache)
    df_30 = get_funnel(30).copy()
    df_30["event_date"] = pd.to_datetime(df_30["event_date"])
    df_30["is_weekend"] = df_30["is_weekend"].map(
        {True: "Fim de semana", False: "Dia útil", "true": "Fim de semana", "false": "Dia útil"}
    ).fillna("Dia útil")
    for col in ("event_count", "sessions", "users"):
        df_30[col] = pd.to_numeric(df_30[col], errors="coerce").fillna(0)

    # RF3.5 — Eventos-chave + sessões: média diária, fim de semana vs dias úteis
    _EVENT_LABELS = {
        "product_view": "Visualização",
        "add_to_cart":  "Add to Cart",
        "cart_abandon": "Abandono",
        "order_placed": "Compra",
    }
    _EVENT_ORDER = ["Sessões", "Visualização", "Add to Cart", "Abandono", "Compra"]

    # média de eventos por dia para cada tipo
    ev_daily = (
        df_30[df_30["event_type"].isin(_EVENT_LABELS)]
        .groupby(["event_date", "event_type", "is_weekend"], as_index=False)["event_count"].sum()
    )
    ev_avg = ev_daily.groupby(["event_type", "is_weekend"], as_index=False)["event_count"].mean()
    ev_avg["event_type"] = ev_avg["event_type"].map(_EVENT_LABELS)
    ev_avg = ev_avg.rename(columns={"event_count": "valor"})

    # média de sessões por dia
    sess_daily = (
        df_30.groupby(["event_date", "is_weekend"], as_index=False)["sessions"].sum()
    )
    sess_avg = sess_daily.groupby("is_weekend", as_index=False)["sessions"].mean()
    sess_avg["event_type"] = "Sessões"
    sess_avg = sess_avg.rename(columns={"sessions": "valor"})

    we_df = pd.concat([sess_avg, ev_avg], ignore_index=True)
    we_df["event_type"] = pd.Categorical(we_df["event_type"], categories=_EVENT_ORDER, ordered=True)
    we_df = we_df.sort_values("event_type")
    fig_we = px.bar(we_df, x="event_type", y="valor", color="is_weekend",
        barmode="group",
        title="Eventos-Chave: Fim de Semana vs. Dias Úteis — Média Diária (30 dias)",
        labels={"valor": "Média / dia", "event_type": "", "is_weekend": ""},
        color_discrete_sequence=[acc, "#6b7280"])

    return html.Div([
        header,
        dbc.Row([
            dbc.Col(_chart(fig_funnel,
                "Onde ocorrem as maiores perdas ao longo do funil de conversão?",
                "RF3.1", h=420), md=6),
            dbc.Col(_chart(fig_dev,
                "Em que dispositivos se concentram os eventos de navegação?",
                "RF3.2", h=420), md=6),
        ], className="g-3"),
        dbc.Row([
            dbc.Col(_chart(fig_su,
                "Quantas sessões e utilizadores únicos chegam à plataforma por dia?",
                "RF3.4"), md=5),
            dbc.Col(_chart(fig_we,
                "Os eventos-chave diferem entre dias úteis e fins de semana?",
                "RF3.5"), md=7),
        ], className="g-3"),
    ])


# ---------------------------------------------------------------------------
# Aba: Reviews — Gestor de Customer Experience (RF4.1–RF4.4) — 90 dias
# ---------------------------------------------------------------------------
def _tab_reviews():
    df = get_reviews(90)
    if df.empty:
        return _no_data()

    df["review_date"]     = pd.to_datetime(df["review_date"])
    df["review_count"]    = pd.to_numeric(df["review_count"],    errors="coerce").fillna(0)
    df["avg_rating"]      = pd.to_numeric(df["avg_rating"],      errors="coerce").fillna(0.0)
    df["avg_text_length"] = pd.to_numeric(df["avg_text_length"], errors="coerce").fillna(0.0)

    acc = ACC["reviews"]

    header = _tab_header(
        "Gestor de Customer Experience", "Últimos 90 dias", "RF4.1–RF4.4",
        [
            "Qual a proporção de reviews positivas, neutras e negativas?",
            "O rating médio diário está a subir, estabilizar ou descer?",
            "Quais as categorias com mais feedback — e qual a satisfação associada?",
            "Há regiões com sentimento sistematicamente mais negativo?",
        ],
        acc,
    )

    sent_df = df.groupby("sentiment", as_index=False)["review_count"].sum()
    fig_sent = px.pie(sent_df, values="review_count", names="sentiment",
        title="Distribuição de Sentimento",
        color="sentiment", color_discrete_map=SENTIMENT_COLORS, hole=0.45)

    daily_df = (
        df.groupby("review_date")
        .apply(lambda x: pd.Series({
            "avg_rating": (
                (x["avg_rating"] * x["review_count"]).sum() / x["review_count"].sum()
                if x["review_count"].sum() > 0 else 0
            ),
        }))
        .reset_index().sort_values("review_date")
    )
    fig_rating = px.line(daily_df, x="review_date", y="avg_rating",
        title="Rating Médio ao Longo do Tempo",
        labels={"review_date": "", "avg_rating": "Rating Médio"},
        color_discrete_sequence=["#d97706"])
    fig_rating.update_traces(line=dict(width=2))
    fig_rating.update_layout(yaxis=dict(range=[1, 5], tickformat=".1f", fixedrange=True))
    fig_rating.add_hline(y=3, line_dash="dot", line_color="#d1d5db",
        annotation_text="neutro (3)", annotation_font_size=10,
        annotation_font_color="#9ca3af")

    cat_df = (
        df.groupby("category", as_index=False)
        .agg(review_count=("review_count", "sum"), avg_rating=("avg_rating", "mean"))
        .nlargest(10, "review_count").sort_values("review_count")
    )
    fig_cat = px.bar(cat_df, x="review_count", y="category", orientation="h",
        color="avg_rating",
        color_continuous_scale="RdYlGn", range_color=[1, 5],
        title="Top 10 Categorias por Nº de Reviews",
        labels={"review_count": "Reviews", "category": "", "avg_rating": "Rating Médio"})

    _sentiments = ["positive", "neutral", "negative"]
    region_df = df.groupby(["region", "sentiment"], as_index=False)["review_count"].sum()
    state_df  = df.groupby(["state",  "sentiment"], as_index=False)["review_count"].sum()

    geo_traces = []
    for sent in _sentiments:
        r = region_df[region_df["sentiment"] == sent].sort_values("region")
        s = state_df[state_df["sentiment"]   == sent].sort_values("state")
        geo_traces.append(go.Bar(
            x=r["region"], y=r["review_count"], name=sent,
            marker_color=SENTIMENT_COLORS[sent], visible=True,
        ))
        geo_traces.append(go.Bar(
            x=s["state"], y=s["review_count"], name=sent,
            marker_color=SENTIMENT_COLORS[sent], visible=False, showlegend=False,
        ))

    fig_region = go.Figure(data=geo_traces)
    fig_region.update_layout(
        barmode="stack",
        title="Sentimento por Localização",
        legend_title_text="",
        xaxis=dict(categoryorder="total descending"),
        updatemenus=[dict(
            type="buttons", direction="right",
            buttons=[
                dict(label="Região", method="restyle",
                     args=[{"visible": [True, False, True, False, True, False],
                            "showlegend": [True, False, True, False, True, False]}]),
                dict(label="Estado", method="restyle",
                     args=[{"visible": [False, True, False, True, False, True],
                            "showlegend": [False, True, False, True, False, True]}]),
            ],
            x=1.0, y=1.15, xanchor="right",
            bgcolor="#f3f4f6", bordercolor="#e5e7eb",
            font=dict(size=9, color="#6b7280"),
        )],
    )

    return html.Div([
        header,
        dbc.Row([
            dbc.Col(_chart(fig_sent,
                "Qual a proporção global de reviews positivas, neutras e negativas?",
                "RF4.1"), md=4),
            dbc.Col(_chart(fig_rating,
                "O rating médio está a subir, estabilizar ou descer ao longo do tempo?",
                "RF4.2"), md=8),
        ], className="g-3"),
        dbc.Row([
            dbc.Col(_chart(fig_cat,
                "Quais as categorias com mais feedback — a cor revela a satisfação associada?",
                "RF4.3"), md=6),
            dbc.Col(_chart(fig_region,
                "Há regiões com sentimento negativo desproporcional que exijam atenção?",
                "RF4.4"), md=6),
        ], className="g-3"),
    ])


# ---------------------------------------------------------------------------
# Aba: Tendências — Analista de Tendências (RF5.1–RF5.4) — 60/30 dias
# ---------------------------------------------------------------------------
def _tab_trends():
    df     = get_trends(60)
    df_cat = get_category_trends(30)

    if df.empty:
        return _no_data()

    df["purchase_date"] = pd.to_datetime(df["purchase_date"])
    df["orders"] = pd.to_numeric(df["orders"], errors="coerce").fillna(0)
    for col in ("revenue", "orders_growth_pct", "revenue_growth_pct", "revenue_acceleration"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    acc = ACC["trends"]

    header = _tab_header(
        "Analista de Tendências", "60 dias (global) / 30 dias (categorias)", "RF5.1–RF5.4",
        [
            "A procura está a crescer esta semana face à semana anterior?",
            "A taxa de crescimento está a acelerar ou a perder momentum?",
            "Quais as categorias com maior dinamismo de crescimento nos últimos 30 dias?",
        ],
        acc,
    )

    sections = [header]

    last_valid = df.dropna(subset=["revenue_growth_pct"])
    if not last_valid.empty:
        last      = last_valid.iloc[-1]
        rev_g     = last["revenue_growth_pct"]
        ord_g     = last["orders_growth_pct"]
        acc_val   = last["revenue_acceleration"]

        sections.append(_kpi_row(
            _kpi("Crescimento Receita (WoW)",
                 f"{'+' if rev_g >= 0 else ''}{rev_g:.1f}%",
                 "#16a34a" if rev_g >= 0 else "#dc2626"),
            _kpi("Crescimento Pedidos (WoW)",
                 (f"{'+' if pd.notna(ord_g) and ord_g >= 0 else ''}{ord_g:.1f}%"
                  if pd.notna(ord_g) else "N/A"),
                 "#16a34a" if pd.notna(ord_g) and ord_g >= 0 else "#dc2626"),
            _kpi("Aceleração da Procura",
                 (f"{'+' if pd.notna(acc_val) and acc_val >= 0 else ''}{acc_val:.2f}"
                  if pd.notna(acc_val) else "N/A"),
                 "#16a34a" if pd.notna(acc_val) and acc_val > 0 else "#ca8a04"),
        ))

    # RF5.1 + RF5.3 — Crescimento WoW com marcadores de anomalia
    df_growth = df.dropna(subset=["revenue_growth_pct", "orders_growth_pct"])
    if not df_growth.empty:
        fig_growth = go.Figure()
        fig_growth.add_trace(go.Scatter(
            x=df_growth["purchase_date"], y=df_growth["revenue_growth_pct"],
            name="Receita (%)", mode="lines+markers",
            line=dict(color=acc)))
        fig_growth.add_trace(go.Scatter(
            x=df_growth["purchase_date"], y=df_growth["orders_growth_pct"],
            name="Pedidos (%)", mode="lines+markers",
            line=dict(color="#16a34a", dash="dot")))
        fig_growth.add_hline(y=0, line_dash="dash", line_color="#e5e7eb")
        fig_growth.update_layout(
            title="Crescimento WoW — Receita e Pedidos (60 dias)",
            yaxis_title="Crescimento (%)")
        sections.append(_chart(fig_growth,
            "A procura está a crescer WoW? Os × vermelhos indicam anomalias estatísticas (2σ).",
            "RF5.1 · RF5.3"))

    # RF5.2 — Aceleração da procura
    df_acc = df.dropna(subset=["revenue_acceleration"])
    if not df_acc.empty:
        colors = ["#16a34a" if v >= 0 else "#dc2626" for v in df_acc["revenue_acceleration"]]
        fig_acc = go.Figure(go.Bar(
            x=df_acc["purchase_date"], y=df_acc["revenue_acceleration"],
            marker_color=colors))
        fig_acc.add_hline(y=0, line_dash="dash", line_color="#e5e7eb")
        fig_acc.update_layout(
            title="Aceleração da Procura (variação da taxa de crescimento WoW)",
            yaxis_title="Aceleração")
        sections.append(_chart(fig_acc,
            "O crescimento está a ganhar ou a perder momentum? Verde = aceleração, vermelho = abrandamento.",
            "RF5.2"))

    # RF5.4 — Crescimento por categoria
    if not df_cat.empty:
        df_cat["purchase_date"] = pd.to_datetime(df_cat["purchase_date"])
        for col in ("revenue_growth_pct", "revenue"):
            df_cat[col] = pd.to_numeric(df_cat[col], errors="coerce")

        top_cats = df_cat.groupby("category")["revenue"].sum().nlargest(8).index.tolist()
        df_top   = df_cat[df_cat["category"].isin(top_cats)].dropna(subset=["revenue_growth_pct"])

        if not df_top.empty:
            fig_cat_line = px.line(df_top, x="purchase_date", y="revenue_growth_pct",
                color="category",
                title="Crescimento WoW por Categoria — Top 8 por Receita (30 dias)",
                labels={"revenue_growth_pct": "Crescimento (%)",
                        "purchase_date": "", "category": "Categoria"})
            fig_cat_line.add_hline(y=0, line_dash="dash", line_color="#e5e7eb")

            avg_growth = (
                df_top.groupby("category")["revenue_growth_pct"]
                .mean().reset_index().sort_values("revenue_growth_pct")
            )
            avg_growth["color"] = avg_growth["revenue_growth_pct"].apply(
                lambda v: "#16a34a" if v >= 0 else "#dc2626")
            fig_avg = go.Figure(go.Bar(
                x=avg_growth["revenue_growth_pct"], y=avg_growth["category"],
                orientation="h", marker_color=avg_growth["color"].tolist()))
            fig_avg.update_layout(
                title="Crescimento Médio WoW por Categoria (30 dias)",
                xaxis_title="Crescimento Médio (%)")

            sections.append(dbc.Row([
                dbc.Col(_chart(fig_cat_line,
                    "Quais as categorias com maior dinamismo de crescimento nos últimos 30 dias?",
                    "RF5.4"), md=8),
                dbc.Col(_chart(fig_avg,
                    "Ranking estático de crescimento médio — quais lideram, quais regridem?",
                    "RF5.4"), md=4),
            ], className="g-3"))

    return html.Div(sections)


# ---------------------------------------------------------------------------
# Aba: ML Insights — Data Scientist (RF6.1–RF6.2)
# ---------------------------------------------------------------------------
def _tab_ml():
    df_forecast = get_demand_forecast()

    acc = ACC["ml"]

    header = _tab_header(
        "Data Scientist", "Próximos 7 dias", "RF6.1–RF6.2",
        [
            "Quantos pedidos são esperados por categoria nos próximos 7 dias?",
            "Quão fiável é o modelo de previsão — qual o RMSE e MAE médios entre categorias?",
        ],
        acc,
    )

    sections = [header]

    if df_forecast.empty:
        sections.append(html.Div(
            "Previsão ainda não disponível — aguardar execução da DAG trendmart_ml_pipeline (diária às 03:00 UTC).",
            style={"padding": "40px 0", "textAlign": "center",
                   "color": "#9ca3af", "fontSize": "0.88rem"},
        ))
    else:
        df_forecast["forecast_date"]    = pd.to_datetime(df_forecast["forecast_date"])
        df_forecast["predicted_orders"] = pd.to_numeric(
            df_forecast["predicted_orders"], errors="coerce").fillna(0.0)

        # Média global das métricas (cada categoria tem um modelo próprio)
        per_cat = df_forecast.drop_duplicates(subset=["category"])
        rmse = per_cat["model_rmse"].astype(float).mean() if "model_rmse" in per_cat.columns else None
        mae  = per_cat["model_mae"].astype(float).mean()  if "model_mae"  in per_cat.columns else None

        if rmse is not None or mae is not None:
            sections.append(dbc.Row([
                dbc.Col(_kpi("RMSE Médio (todas as categorias)",
                             f"{rmse:.2f}" if rmse is not None else "N/A", acc), md=3),
                dbc.Col(_kpi("MAE Médio (todas as categorias)",
                             f"{mae:.2f}" if mae is not None else "N/A", "#6b7280"), md=3),
            ], className="g-3 mb-3"))

        fig_forecast = px.line(df_forecast,
            x="forecast_date", y="predicted_orders", color="category",
            title="Previsão de Procura — Próximos 7 Dias por Categoria",
            labels={"predicted_orders": "Pedidos Previstos",
                    "forecast_date": "Data", "category": "Categoria"},
            markers=True)

        sections.append(_chart(fig_forecast,
            "Qual a procura prevista por categoria? Modelo: Regressão Linear (Spark MLlib) com lag features e atributos de calendário, treinado independentemente por categoria.",
            "RF6.1 · RF6.2", h=420))

    return html.Div(sections)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    port  = int(os.getenv("DASHBOARD_PORT", "8050"))
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=port, debug=debug)
