"""DAG 3 — shift_gold_promotion (every 30 minutes).

Depends on shift_silver_promotion succeeding.
  run_dbt_models (load Silver→Snowflake RAW, then dbt run)
  → run_great_expectations_gold_suite (dbt test — Gold quality gate)
  → run_reconciliation_check (source↔target)
  → update_migration_dashboard (refresh gold_migration_summary)

Requires the dbt container up:  docker compose --profile snowflake up -d dbt
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta

from _helpers import run_in, spark


def dbt(*cmd: str, timeout: int = 1800) -> str:
    return run_in("shift-dbt", *cmd, timeout=timeout)


@dag(
    dag_id="shift_gold_promotion",
    schedule="*/30 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["shift", "gold", "snowflake", "dbt"],
)
def shift_gold_promotion():

    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver_promotion",
        external_dag_id="shift_silver_promotion",
        external_task_id=None,
        allowed_states=["success"],
        mode="reschedule",
        timeout=900,
        poke_interval=30,
    )

    @task(retries=2, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True,
          sla=timedelta(minutes=20))
    def run_dbt_models() -> str:
        spark("-m", "src.transformation.load_snowflake")    # Silver → RAW
        out = dbt("dbt", "run")
        return out.strip().splitlines()[-1] if out.strip() else ""

    @task
    def run_great_expectations_gold_suite() -> str:
        out = dbt("dbt", "test")
        if "ERROR" in out or "Failure" in out:
            raise ValueError("Gold dbt tests failed")
        return "gold tests passed"

    @task
    def run_reconciliation_check() -> str:
        out = run_in("shift-dbt", "python", "-m", "src.transformation.reconcile", timeout=1200)
        return out.strip().splitlines()[-1] if out.strip() else ""

    @task
    def update_migration_dashboard() -> str:
        out = dbt("dbt", "run", "--select", "gold_migration_summary")
        return out.strip().splitlines()[-1] if out.strip() else ""

    models = run_dbt_models()
    ge = run_great_expectations_gold_suite()
    recon = run_reconciliation_check()
    dash = update_migration_dashboard()

    wait_for_silver >> models >> ge >> recon >> dash


shift_gold_promotion()
