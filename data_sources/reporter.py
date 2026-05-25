"""
Simulator reporter — appends periodic snapshots and a final summary to a daily
Markdown report in data_sources/relatórios/simulator_YYYY-MM-DD.md.

Snapshots are written every SNAPSHOT_EVERY sessions (default 50 ≈ 100 s at 0.5 sess/s).
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_TZ = ZoneInfo("Europe/Lisbon")
_REPORTS_DIR = Path(__file__).parent / "relatórios"

SNAPSHOT_EVERY = 50


def _daily_path() -> Path:
    today = datetime.now(_TZ).strftime("%Y-%m-%d")
    return _REPORTS_DIR / f"simulator_{today}.md"


def _ensure_header(path: Path) -> None:
    if path.exists():
        return
    date_str = datetime.now(_TZ).strftime("%Y-%m-%d")
    header = (
        f"# Relatório do Simulador — {date_str}\n\n"
        "**Processo:** TrendMart E-commerce Simulator  \n"
        "**Taxa:** ~0.5 sessões/segundo  \n"
        "**Destinos:** Kafka (clickstream), PostgreSQL (orders), MinIO (reviews)\n\n"
        "| Hora | Sessões | Eventos Kafka | Compras PG | Reviews MinIO |\n"
        "|------|---------|---------------|------------|---------------|\n"
    )
    path.write_text(header, encoding="utf-8")


def record_snapshot(sessions: int, events: int, purchases: int, reviews: int) -> None:
    """Append a counter snapshot — call every SNAPSHOT_EVERY sessions."""
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = _daily_path()
    _ensure_header(path)

    now = datetime.now(_TZ).strftime("%H:%M:%S")
    with path.open("a", encoding="utf-8") as f:
        f.write(f"| {now} | {sessions} | {events} | {purchases} | {reviews} |\n")


def record_shutdown(sessions: int, events: int, purchases: int, reviews: int) -> None:
    """Append a final shutdown summary block."""
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = _daily_path()
    _ensure_header(path)

    now = datetime.now(_TZ).strftime("%H:%M:%S")
    summary = (
        f"\n## Encerramento — {now}\n\n"
        f"| Métrica | Total |\n"
        f"|---------|-------|\n"
        f"| Sessões simuladas | {sessions} |\n"
        f"| Eventos Kafka | {events} |\n"
        f"| Compras PostgreSQL | {purchases} |\n"
        f"| Reviews MinIO | {reviews} |\n"
    )
    with path.open("a", encoding="utf-8") as f:
        f.write(summary)
