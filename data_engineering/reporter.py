"""
Ingestion reporter — appends one line per flush to a daily Markdown report in
data_engineering/relatórios/.

Three separate files are kept (one per consumer) to avoid concurrent write conflicts:
    ingestion_clickstream_YYYY-MM-DD.md
    ingestion_orders_YYYY-MM-DD.md
    ingestion_reviews_YYYY-MM-DD.md
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_TZ = ZoneInfo("Europe/Lisbon")
_REPORTS_DIR = Path(__file__).parent / "relatórios"

_SOURCE_LABELS = {
    "clickstream": "Kafka `clickstream_events`",
    "orders":      "Debezium CDC (PostgreSQL WAL)",
    "reviews":     "MinIO `raw-reviews/`",
}
_DEST_LABELS = {
    "clickstream": "bronze/clickstream/",
    "orders":      "bronze/orders/",
    "reviews":     "bronze/reviews/",
}


def _daily_path(consumer: str) -> Path:
    today = datetime.now(_TZ).strftime("%Y-%m-%d")
    return _REPORTS_DIR / f"ingestion_{consumer}_{today}.md"


def _ensure_header(path: Path, consumer: str) -> None:
    if path.exists():
        return
    header = (
        f"# Relatório de Ingestão — {consumer}\n\n"
        f"**Fonte:** {_SOURCE_LABELS.get(consumer, consumer)}  \n"
        f"**Destino:** `{_DEST_LABELS.get(consumer, 'bronze/')}`\n\n"
        "| Hora | Registos | Caminho Bronze |\n"
        "|------|----------|----------------|\n"
    )
    path.write_text(header, encoding="utf-8")


def record_flush(consumer: str, count: int, bronze_key: str) -> None:
    """Append a flush entry to the consumer's daily report."""
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = _daily_path(consumer)
    _ensure_header(path, consumer)

    now = datetime.now(_TZ).strftime("%H:%M:%S")
    with path.open("a", encoding="utf-8") as f:
        f.write(f"| {now} | {count} | `{bronze_key}` |\n")


def record_shutdown(consumer: str, total: int) -> None:
    """Append a shutdown summary line to the consumer's daily report."""
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = _daily_path(consumer)
    _ensure_header(path, consumer)

    now = datetime.now(_TZ).strftime("%H:%M:%S")
    summary = (
        f"\n## Encerramento — {now}\n\n"
        f"**Total de registos ingeridos nesta sessão:** {total}\n"
    )
    with path.open("a", encoding="utf-8") as f:
        f.write(summary)
