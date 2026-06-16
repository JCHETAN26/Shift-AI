"""Shared helpers for the Shift.ai Airflow DAGs.

The DAGs orchestrate the *already-tested* job entrypoints that live in the
spark / dbt containers, invoked over `docker exec`. Airflow has the docker CLI
and the docker socket mounted, so a task is a thin wrapper around the same
command you'd run by hand (`python -m src.ingestion.cdc_merge orders --once`).
"""
from __future__ import annotations

import json
import re
import subprocess

# The migrated tables (kept local — the Airflow container doesn't mount `src`).
TABLES = ["customers", "products", "orders", "inventory", "shipments"]


def run_in(container: str, *cmd: str, timeout: int = 2400) -> str:
    """Run a command in a running container; raise on non-zero exit."""
    full = ["docker", "exec", "-e", "PYTHONPATH=/app", container, *cmd]
    res = subprocess.run(full, capture_output=True, text=True, timeout=timeout)
    if res.returncode != 0:
        raise RuntimeError(
            f"`{' '.join(cmd)}` in {container} failed (exit {res.returncode}):\n"
            f"{res.stderr[-1500:]}"
        )
    return res.stdout


def spark(*cmd: str, timeout: int = 2400) -> str:
    return run_in("shift-spark", "python", *cmd, timeout=timeout)


def psql(sql: str) -> str:
    """Run SQL in the source Postgres and return stdout (tab/aligned off)."""
    return run_in("shift-postgres", "psql", "-U", "shift", "-d", "shift", "-tAc", sql)


def delta_count(layer: str, table: str) -> int:
    """Count rows in a Bronze/Silver Delta table (via Spark)."""
    code = (
        "from src.ingestion.cdc_merge import _build_spark;"
        "s=_build_spark('airflow-count');s.sparkContext.setLogLevel('ERROR');"
        f"print('COUNT=' + str(s.read.format('delta').load('/data/delta/{layer}/{table}').count()));"
        "s.stop()"
    )
    out = spark("-c", code, timeout=600)
    m = re.search(r"COUNT=(\d+)", out)
    if not m:
        raise RuntimeError(f"could not read {layer}/{table} count from:\n{out[-500:]}")
    return int(m.group(1))


def parse_metrics(stdout: str) -> dict:
    """Pull the last `{...}` JSON-ish dict a job printed (best-effort)."""
    matches = re.findall(r"\{[^{}]*\}", stdout)
    for m in reversed(matches):
        try:
            return json.loads(m.replace("'", '"'))
        except Exception:
            continue
    return {}
