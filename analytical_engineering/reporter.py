"""
Batch reporter — writes one Markdown report per Airflow DAG run to
analytical_engineering/relatórios/batch_YYYY-MM-DD_HH-MM.md

Inside the Airflow container, analytical_engineering/ is mounted at
/opt/airflow/project, so reports land there and are visible on the host.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_TZ = ZoneInfo("Europe/Lisbon")
_REPORTS_DIR = Path(os.environ.get("AE_PROJECT_PATH", "/opt/airflow/project")) / "relatórios"


def write_batch_report(
    execution_date: datetime,
    tasks: list[dict],
    views_refreshed: list[str] | None = None,
) -> None:
    """
    Write a complete Markdown report for one pipeline batch run.

    Each dict in `tasks` must have:
        name        str
        status      "SUCCESS" | "FAILED"
        duration_s  float | None
        records     int | None
    """
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    ts = execution_date.strftime("%Y-%m-%d_%H-%M")
    path = _REPORTS_DIR / f"batch_{ts}.md"

    total_records = sum(t.get("records") or 0 for t in tasks)
    total_duration = sum(t.get("duration_s") or 0.0 for t in tasks)
    all_ok = all(t.get("status") == "SUCCESS" for t in tasks)
    overall = "SUCCESS" if all_ok else "FAILED"

    lines = [
        f"# Relatório de Batch — {execution_date.strftime('%Y-%m-%d %H:%M')}",
        "",
        f"**Pipeline:** trendmart_gold_pipeline  ",
        f"**Execução:** {execution_date.strftime('%Y-%m-%d %H:%M:%S')}  ",
        f"**Estado:** {overall}",
        "",
        "## Tarefas",
        "",
        "| Tarefa | Estado | Duração | Registos |",
        "|--------|--------|---------|----------|",
    ]

    for t in tasks:
        dur = f"{t['duration_s']:.0f}s" if t.get("duration_s") is not None else "—"
        rec = str(t["records"]) if t.get("records") is not None else "—"
        lines.append(f"| {t['name']} | {t.get('status', 'UNKNOWN')} | {dur} | {rec} |")

    lines += [
        "",
        "## Resumo",
        "",
        f"- **Registos totais processados:** {total_records}",
        f"- **Duração total:** {total_duration:.0f}s",
    ]

    if views_refreshed:
        views_str = ", ".join(f"`{v}`" for v in views_refreshed)
        lines.append(f"- **Vistas Trino actualizadas:** {views_str}")

    now_str = datetime.now(_TZ).strftime("%Y-%m-%d %H:%M:%S")
    lines += ["", f"*Gerado em {now_str}*", ""]

    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"[reporter] Relatório escrito: relatórios/{path.name}")
