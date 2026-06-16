"""DAG 1 — shift_cdc_ingest (every 5 minutes).

check_kafka_consumer_lag → branch (alert if lag>10000 else continue)
  → run_pyspark_cdc_job (retries + exp backoff, SLA 4m)
  → validate_bronze_rowcount (fail if 0 rows)
  → update_pipeline_metadata
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from datetime import timedelta

from _helpers import TABLES, delta_count, parse_metrics, psql, run_in, spark

LAG_THRESHOLD = 10_000


@dag(
    dag_id="shift_cdc_ingest",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["shift", "cdc", "bronze"],
)
def shift_cdc_ingest():

    @task
    def check_kafka_consumer_lag() -> int:
        """Lag = current total topic end-offsets minus the last watermark."""
        total = 0
        for t in TABLES:
            out = run_in(
                "shift-kafka", "kafka-run-class", "kafka.tools.GetOffsetShell",
                "--broker-list", "localhost:9092", "--topic", f"shift.public.{t}",
            )
            total += sum(int(line.split(":")[-1]) for line in out.strip().splitlines() if ":" in line)
        out = psql(
            "CREATE SCHEMA IF NOT EXISTS catalog;"
            "CREATE TABLE IF NOT EXISTS catalog.pipeline_metadata ("
            "id BIGSERIAL PRIMARY KEY, dag_id TEXT, end_offset_total BIGINT, "
            "bronze_rows BIGINT, ran_at TIMESTAMPTZ DEFAULT now());"
            "SELECT coalesce(max(end_offset_total),0) FROM catalog.pipeline_metadata "
            "WHERE dag_id='shift_cdc_ingest'"
        )
        # psql echoes CREATE command tags; the SELECT result is the last line.
        last = out.strip().splitlines()[-1].strip() if out.strip() else "0"
        lag = total - int(last or 0)
        print(f"end_offsets_total={total} last_watermark={last} lag={lag}")
        return max(lag, 0)

    @task.branch
    def branch_on_lag(lag: int) -> str:
        return "trigger_alert" if lag > LAG_THRESHOLD else "run_pyspark_cdc_job"

    @task
    def trigger_alert(lag: int):
        print(f"⚠️  Consumer lag {lag} exceeds {LAG_THRESHOLD} — alerting on-call, skipping ingest.")

    @task(
        retries=3,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=5),
        sla=timedelta(minutes=4),
    )
    def run_pyspark_cdc_job() -> dict:
        """Run the hand-written CDC merge (availableNow) for each table."""
        metrics = {}
        for t in TABLES:
            out = spark("-m", "src.ingestion.cdc_merge", t, "--once")
            metrics[t] = parse_metrics(out)
        return metrics

    @task
    def validate_bronze_rowcount() -> dict:
        counts = {t: delta_count("bronze", t) for t in TABLES}
        empty = [t for t, c in counts.items() if c == 0]
        if empty:
            raise ValueError(f"Bronze tables empty — ingest broke: {empty}")
        print(f"bronze counts: {counts}")
        return counts

    @task(trigger_rule="none_failed_min_one_success")
    def update_pipeline_metadata(counts: dict):
        total = 0
        for t in TABLES:
            out = run_in(
                "shift-kafka", "kafka-run-class", "kafka.tools.GetOffsetShell",
                "--broker-list", "localhost:9092", "--topic", f"shift.public.{t}",
            )
            total += sum(int(l.split(":")[-1]) for l in out.strip().splitlines() if ":" in l)
        rows = sum(counts.values())
        psql(
            "CREATE TABLE IF NOT EXISTS catalog.pipeline_metadata ("
            "id BIGSERIAL PRIMARY KEY, dag_id TEXT, end_offset_total BIGINT, "
            "bronze_rows BIGINT, ran_at TIMESTAMPTZ DEFAULT now());"
            f"INSERT INTO catalog.pipeline_metadata (dag_id,end_offset_total,bronze_rows) "
            f"VALUES ('shift_cdc_ingest',{total},{rows});"
        )
        print(f"pipeline_metadata updated: end_offset_total={total} bronze_rows={rows}")

    lag = check_kafka_consumer_lag()
    branch = branch_on_lag(lag)
    alert = trigger_alert(lag)
    cdc = run_pyspark_cdc_job()
    counts = validate_bronze_rowcount()
    meta = update_pipeline_metadata(counts)

    branch >> [alert, cdc]
    cdc >> counts >> meta


shift_cdc_ingest()
