"""
demo.py - Modo demonstracao do TrendMart Analytics

Uso (a partir da raiz do projecto):
    python demo.py start    - inicia APENAS o dashboard com dados sinteticos
    python demo.py clean    - reverte import + inicia stack completo

Workflow tipico:
    python demo.py start    # abre http://localhost:8050, tira screenshots
    python demo.py clean    # volta ao normal com pipeline real
"""

import sys
import time
import subprocess
import urllib.request
from pathlib import Path

# Caminhos
ROOT      = Path(__file__).parent
APP_PY    = ROOT / "data_analytics" / "dashboard" / "app.py"
COMPOSE   = ROOT / "infrastructure" / "docker-compose.yml"
CONTAINER = "ge_dashboard"
URL       = "http://localhost:8050"

FROM_REAL = "from data import ("
FROM_DEMO = "from data_demo import ("


def _patch(src, dst):
    """Substitui src por dst em app.py. Devolve True se algo mudou."""
    content = APP_PY.read_text(encoding="utf-8")
    if dst in content:
        return False
    if src not in content:
        raise ValueError("Padrao nao encontrado em app.py: " + repr(src))
    APP_PY.write_text(content.replace(src, dst, 1), encoding="utf-8")
    return True


def _run(cmd, **kwargs):
    subprocess.run(cmd, check=True, **kwargs)


def _wait(timeout=60, label="dashboard"):
    print(f"  A aguardar {label} em {URL}", end="", flush=True)
    for _ in range(timeout):
        try:
            urllib.request.urlopen(URL, timeout=2)
            print(" OK")
            return True
        except Exception:
            print(".", end="", flush=True)
            time.sleep(1)
    print(f"\n  [!] Timeout ({timeout}s) -- verifica manualmente: {URL}")
    return False


def start():
    """Inicia APENAS o dashboard em modo demo (sem Trino, sem MinIO)."""
    print("\n--- Demo Start -------------------------------------------")

    changed = _patch(FROM_REAL, FROM_DEMO)
    print("  [1/2] app.py -> data_demo " + ("activado" if changed else "ja estava activo"))

    print(f"  [2/2] A iniciar {CONTAINER}...")
    try:
        # restart funciona em containers parados E a correr
        _run(["docker", "restart", CONTAINER], capture_output=True)
    except subprocess.CalledProcessError:
        # Container nao existe (docker-compose down foi usado) -- criar sem deps
        _run(
            ["docker-compose", "-f", str(COMPOSE), "up", "-d", "--no-deps", CONTAINER],
            cwd=str(ROOT),
        )

    _wait(timeout=40)
    print(f"--- DEMO ACTIVO -> {URL}\n")


def clean():
    """Reverte import e inicia o stack completo (pipeline real)."""
    print("\n--- Demo Clean -------------------------------------------")

    changed = _patch(FROM_DEMO, FROM_REAL)
    print("  [1/3] app.py -> import original " + ("reposto" if changed else "ja estava normal"))

    print(f"  [2/3] A parar {CONTAINER}...")
    try:
        _run(["docker", "stop", CONTAINER], capture_output=True)
    except subprocess.CalledProcessError:
        pass

    # postgres -> minio -> hive -> trino -> dashboard + pipeline
    print("  [3/3] A subir stack completo (pode demorar ~2 min)...")
    _run(
        ["docker-compose", "-f", str(COMPOSE), "up", "-d"],
        cwd=str(ROOT),
    )

    _wait(timeout=180, label="stack completo")
    print(f"--- MODO NORMAL RESTAURADO -> {URL}\n")


if __name__ == "__main__":
    cmds = {"start": start, "clean": clean}
    if len(sys.argv) != 2 or sys.argv[1] not in cmds:
        print(__doc__)
        sys.exit(1)
    try:
        cmds[sys.argv[1]]()
    except KeyboardInterrupt:
        print("\nInterrompido.")
    except Exception as e:
        print("\n  Erro: " + str(e))
        sys.exit(1)
