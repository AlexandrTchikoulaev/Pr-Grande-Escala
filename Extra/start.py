"""
TrendMart — Iniciar o sistema.
Uso: python start.py
"""

import subprocess
import sys
from pathlib import Path

ROOT  = Path(__file__).parent.parent.resolve()
INFRA = ROOT / "infrastructure"


def run(*cmd: str):
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(list(cmd), cwd=INFRA)
    if result.returncode != 0:
        print(f"\n[start] Erro: comando falhou com código {result.returncode}")
        sys.exit(result.returncode)


def main():
    print("\n" + "=" * 60)
    print("  TrendMart — A iniciar o sistema")
    print("=" * 60)

    run("docker", "compose", "up", "-d")

    print("""
  Sistema iniciado. Serviços disponíveis:

    Airflow UI:  http://localhost:8081  (admin / admin)
    Dashboard:   http://localhost:8050
    MLflow UI:   http://localhost:5001

  Para disparar o pipeline Gold manualmente:
    docker exec ge_airflow_scheduler airflow dags trigger trendmart_gold_pipeline

  Para disparar o pipeline ML manualmente:
    docker exec ge_airflow_scheduler airflow dags trigger trendmart_ml_pipeline
    """)


if __name__ == "__main__":
    main()
