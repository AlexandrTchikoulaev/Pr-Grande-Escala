"""
TrendMart — Limpar e reiniciar o sistema.

Apaga todos os volumes Docker (Bronze, Silver, Gold, PostgreSQL, Airflow)
e reinicia tudo do zero.

Uso: python clean.py
"""

import subprocess
import sys
from pathlib import Path

ROOT          = Path(__file__).parent.parent.resolve()
INFRA         = ROOT / "infrastructure"
WATCHER_STATE = ROOT / "data_engineering" / "ingestion" / ".watcher_state.json"

CONTAINERS = [
    "ge_postgres", "ge_minio", "ge_minio_init", "ge_kafka",
    "ge_kafka_connect", "ge_debezium_init", "ge_hive_metastore",
    "ge_trino", "ge_airflow_init", "ge_airflow_webserver",
    "ge_airflow_scheduler", "ge_spark_de", "ge_dashboard",
    "ge_simulator", "ge_consumer", "ge_cdc_consumer", "ge_file_watcher",
    "ge_mlflow",
]

VOLUMES = [
    "trendmart_ge_postgres_data",
    "trendmart_ge_minio_data",
    "trendmart_ge_airflow_logs",
    "trendmart_ge_trino_data",
    "trendmart_ge_mlflow_data",
]


def header(text: str):
    print(f"\n{'─' * 60}")
    print(f"  {text}")
    print(f"{'─' * 60}")


def run(*cmd: str):
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(list(cmd), cwd=INFRA)
    if result.returncode != 0:
        print(f"\n[clean] Erro: comando falhou com código {result.returncode}")
        sys.exit(result.returncode)


def main():
    print("\n" + "=" * 60)
    print("  TrendMart — Limpeza do sistema")
    print("=" * 60)
    print("\n  AVISO: isto vai apagar TODOS os dados (Bronze, Silver, Gold,")
    print("  PostgreSQL, Airflow history, Hive Metastore).")

    confirm = input("\n  Continuar? (s/N): ").strip().lower()
    if confirm != "s":
        print("  Cancelado.")
        sys.exit(0)

    header("1/3 — A parar e remover containers e volumes")

    # 1. matar containers imediatamente (kill ignora restart policies)
    print("  $ docker compose kill")
    subprocess.run(["docker", "compose", "kill"], cwd=INFRA)
    for c in CONTAINERS:
        subprocess.run(["docker", "rm", "-f", c], capture_output=True)

    # 2. agora sem containers activos, remover volumes
    for v in VOLUMES:
        r = subprocess.run(["docker", "volume", "rm", v], capture_output=True)
        if r.returncode == 0:
            print(f"  ✓ Volume removido: {v}")
        else:
            print(f"  (volume não encontrado ou já removido: {v})")

    # 3. limpar redes e estado do compose
    run("docker", "compose", "down", "--remove-orphans")

    header("2/3 — A limpar estado local")
    if WATCHER_STATE.exists():
        WATCHER_STATE.unlink()
        print(f"  ✓ Apagado: {WATCHER_STATE.relative_to(ROOT)}")
    else:
        print("  (sem estado de watcher para apagar)")

    header("3/3 — Concluído")
    print("""
  Sistema limpo. Para iniciar:
    python start.py
    """)


if __name__ == "__main__":
    main()
