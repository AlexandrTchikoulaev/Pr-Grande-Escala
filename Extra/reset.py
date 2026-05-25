"""
TrendMart — Reset completo do sistema.

Modos:
  python reset.py          → hard reset (apaga volumes Docker + recria tudo)
  python reset.py --soft   → soft reset (apenas reinicia containers, mantém dados)
"""

import subprocess
import sys
import shutil
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT          = Path(__file__).parent.parent.resolve()
INFRA         = ROOT / "infrastructure"
WATCHER_STATE = ROOT / "data_engineering" / "ingestion" / ".watcher_state.json"

SOFT = "--soft" in sys.argv

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def header(text: str):
    print(f"\n{'─' * 60}")
    print(f"  {text}")
    print(f"{'─' * 60}")

def run(*cmd: str, cwd: Path = INFRA, check: bool = True):
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(list(cmd), cwd=cwd)
    if check and result.returncode != 0:
        print(f"\n[reset] Erro: comando falhou com código {result.returncode}")
        sys.exit(result.returncode)
    return result.returncode

# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------
def main():
    mode = "SOFT (mantém volumes)" if SOFT else "HARD (apaga volumes)"
    print(f"\n{'=' * 60}")
    print(f"  TrendMart — Reset do Sistema [{mode}]")
    print(f"{'=' * 60}")

    if not SOFT:
        print("\n  AVISO: isto vai apagar TODOS os dados (Bronze, Silver, Gold,")
        print("  PostgreSQL, Airflow history, Hive Metastore).")
        confirm = input("\n  Continuar? (s/N): ").strip().lower()
        if confirm != "s":
            print("  Cancelado.")
            sys.exit(0)

    # ── 1. Parar containers ──────────────────────────────────────────────
    header("1/4 — A parar containers Docker")
    if SOFT:
        run("docker", "compose", "down", "--remove-orphans")
    else:
        run("docker", "compose", "down", "-v", "--remove-orphans")

    # Força remoção de containers órfãos que possam ter ficado para trás
    containers = [
        "ge_postgres", "ge_minio", "ge_minio_init", "ge_kafka",
        "ge_kafka_connect", "ge_debezium_init", "ge_hive_metastore",
        "ge_trino", "ge_airflow_init", "ge_airflow_webserver",
        "ge_airflow_scheduler", "ge_spark_de", "ge_dashboard",
    ]
    for c in containers:
        subprocess.run(["docker", "rm", "-f", c], capture_output=True)

    # ── 2. Limpar estado local ───────────────────────────────────────────
    header("2/4 — A limpar estado local")

    if WATCHER_STATE.exists():
        WATCHER_STATE.unlink()
        print(f"  ✓ Apagado: {WATCHER_STATE.relative_to(ROOT)}")
    else:
        print(f"  (sem estado de watcher para apagar)")

    # ── 3. Reiniciar infra ───────────────────────────────────────────────
    header("3/4 — A iniciar infra Docker")
    if SOFT:
        run("docker", "compose", "up", "-d")
    else:
        run("docker", "compose", "up", "-d", "--build")

    # ── 4. Resumo ────────────────────────────────────────────────────────
    header("4/4 — Concluído")
    print("""
  Infra pronta. Todos os serviços correm como containers Docker:

    ge_simulator      — simulador de dados (clickstream, orders, reviews)
    ge_consumer       — ingestion clickstream → bronze
    ge_cdc_consumer   — ingestion CDC orders → bronze
    ge_file_watcher   — ingestion reviews → bronze
    ge_spark_de       — transformações Bronze → Silver (streaming)

  Não é necessário correr nada localmente.

  Para disparar o pipeline Gold manualmente (sem esperar pela hora):
    docker exec ge_airflow_scheduler airflow dags trigger trendmart_gold_pipeline

  Airflow UI:    http://localhost:8081  (admin / admin)
  Dashboard:     http://localhost:8050
    """)


if __name__ == "__main__":
    main()
