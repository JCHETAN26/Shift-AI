"""DAG 4 — shift_backfill (manual trigger only).

Params: table_name, start_date, end_date. Reprocesses one table end-to-end.
  reset_delta_table_to_snapshot
  → reprocess_cdc_range
  → rerun_silver_gold_promotion
  → reconcile_backfill_result

Trigger with a config, e.g.:
  {"table_name": "orders", "start_date": "2025-01-01", "end_date": "2025-02-01"}
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

from _helpers import run_in, spark


@dag(
    dag_id="shift_backfill",
    schedule=None,                      # manual only
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["shift", "backfill", "manual"],
    params={
        "table_name": Param("orders", type="string", enum=[
            "customers", "products", "orders", "inventory", "shipments"]),
        "start_date": Param("2025-01-01", type="string"),
        "end_date": Param("2025-02-01", type="string"),
    },
)
def shift_backfill():

    @task
    def reset_delta_table_to_snapshot(**ctx) -> str:
        table = ctx["params"]["table_name"]
        # Drop the Bronze checkpoint so the next read re-snapshots from Kafka.
        run_in("shift-spark", "rm", "-rf", f"/data/delta/bronze/_checkpoints/{table}")
        print(f"reset Bronze checkpoint for {table}")
        return table

    @task
    def reprocess_cdc_range(table: str, **ctx) -> dict:
        p = ctx["params"]
        print(f"reprocessing {table} CDC for [{p['start_date']} .. {p['end_date']}]")
        out = spark("-m", "src.ingestion.cdc_merge", table, "--once")
        return {"table": table, "tail": out.strip().splitlines()[-1] if out.strip() else ""}

    @task
    def rerun_silver_gold_promotion(info: dict) -> str:
        table = info["table"]
        out = spark("-m", "src.transformation.bronze_to_silver", table)
        return out.strip().splitlines()[-1] if out.strip() else ""

    @task
    def reconcile_backfill_result(**ctx) -> str:
        table = ctx["params"]["table_name"]
        out = run_in("shift-dbt", "python", "-m", "src.transformation.reconcile", table,
                     "--no-persist", timeout=900)
        return out.strip().splitlines()[-1] if out.strip() else ""

    table = reset_delta_table_to_snapshot()
    info = reprocess_cdc_range(table)
    silver = rerun_silver_gold_promotion(info)
    recon = reconcile_backfill_result()
    silver >> recon


shift_backfill()
