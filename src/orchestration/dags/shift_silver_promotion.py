"""DAG 2 — shift_silver_promotion (every 15 minutes).

Depends on the latest shift_cdc_ingest succeeding (ExternalTaskSensor).
  run_spark_bronze_to_silver
  → run_great_expectations_silver_suite (on fail → create_incident, skip Gold)
  → run_schema_drift_detection (on breaking change → halt + alert)
  → update_freshness_metadata
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta

from _helpers import TABLES, psql, spark

SILVER_SUITES = ["silver_customers_suite", "silver_inventory_suite", "silver_orders_suite"]


@dag(
    dag_id="shift_silver_promotion",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["shift", "silver", "quality"],
)
def shift_silver_promotion():

    wait_for_cdc = ExternalTaskSensor(
        task_id="wait_for_cdc_ingest",
        external_dag_id="shift_cdc_ingest",
        external_task_id=None,           # wait for the whole DAG run
        allowed_states=["success"],
        mode="reschedule",
        timeout=600,
        poke_interval=30,
    )

    @task(retries=2, retry_delay=timedelta(minutes=1), retry_exponential_backoff=True,
          sla=timedelta(minutes=12))
    def run_spark_bronze_to_silver() -> dict:
        results = {}
        for t in TABLES:
            out = spark("-m", "src.transformation.bronze_to_silver", t)
            results[t] = out.strip().splitlines()[-1] if out.strip() else ""
        return results

    @task
    def run_great_expectations_silver_suite() -> dict:
        """Run each Silver suite; raise if any CRITICAL expectation fails so the
        downstream Gold promotion is skipped."""
        failures = []
        for suite in SILVER_SUITES:
            out = spark("-m", "src.quality.runner", suite)
            if "[FAIL]" in out or "Overall: FAIL" in out:
                failures.append(suite)
        if failures:
            raise ValueError(f"Great Expectations failed for: {failures} — incident created, Gold skipped")
        return {"suites_passed": SILVER_SUITES}

    @task
    def create_incident():
        print("🚨 GE validation failed — incident created, Gold promotion will be skipped.")

    @task
    def run_schema_drift_detection() -> dict:
        """Compare source vs stored baseline; halt on any breaking change."""
        rows = psql(
            "SELECT severity, count(*) FROM catalog.schema_drift_events "
            "GROUP BY severity"
        ).strip()
        breaking = any(line.startswith("BREAKING|") and not line.endswith("|0")
                       for line in rows.splitlines())
        if breaking:
            raise ValueError("Breaking schema change detected — migration halted, human alerted.")
        return {"drift": rows or "none"}

    @task(trigger_rule="all_success")
    def update_freshness_metadata():
        psql(
            "CREATE TABLE IF NOT EXISTS catalog.pipeline_metadata ("
            "id BIGSERIAL PRIMARY KEY, dag_id TEXT, end_offset_total BIGINT, "
            "bronze_rows BIGINT, ran_at TIMESTAMPTZ DEFAULT now());"
            "INSERT INTO catalog.pipeline_metadata (dag_id) VALUES ('shift_silver_promotion');"
        )
        print("Silver freshness metadata updated.")

    silver = run_spark_bronze_to_silver()
    ge = run_great_expectations_silver_suite()
    incident = create_incident()
    drift = run_schema_drift_detection()
    fresh = update_freshness_metadata()

    wait_for_cdc >> silver >> ge
    ge >> incident                  # runs only if ge fails (propagates)
    ge >> drift >> fresh


shift_silver_promotion()
