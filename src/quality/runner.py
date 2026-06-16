"""Run expectation suites against the Silver layer and persist results.

Results land in Postgres (`validation_results`) for the dashboard's Data
Quality view. A suite fails if any CRITICAL expectation fails — that's the
signal the Airflow Silver→Gold branch uses to skip Gold promotion.
"""
from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass

from src.quality.expectations.base import ExpectationResult, ValidationContext
from src.quality.expectations.suites import SUITES

_DDL = """
CREATE TABLE IF NOT EXISTS validation_results (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL, suite TEXT NOT NULL, table_name TEXT NOT NULL,
    expectation TEXT NOT NULL, severity TEXT NOT NULL, success BOOLEAN NOT NULL,
    observed JSONB, detail TEXT, validated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


@dataclass
class SuiteResult:
    suite: str
    run_id: str
    results: list[ExpectationResult]

    @property
    def success(self) -> bool:
        return all(r.success for r in self.results if r.severity == "critical")

    @property
    def failed(self) -> list[ExpectationResult]:
        return [r for r in self.results if not r.success]


def _load_silver(spark, table: str, silver_root: str):
    return spark.read.format("delta").load(f"{silver_root}/{table}")


def run_suite(
    spark,
    suite_name: str,
    silver_root: str,
    *,
    persist: bool = True,
    dsn: str | None = None,
) -> SuiteResult:
    if suite_name not in SUITES:
        raise KeyError(f"unknown suite '{suite_name}'; known: {sorted(SUITES)}")
    builder, tables = SUITES[suite_name]
    ctx = ValidationContext({t: _load_silver(spark, t, silver_root) for t in tables})

    results = [exp.validate(ctx) for exp in builder()]
    run_id = uuid.uuid4().hex[:12]

    if persist:
        persist_results(dsn or os.environ["POSTGRES_DSN"], run_id, suite_name, results)
    return SuiteResult(suite=suite_name, run_id=run_id, results=results)


def persist_results(dsn: str, run_id: str, suite: str, results: list[ExpectationResult]) -> None:
    import psycopg

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL)
            for r in results:
                cur.execute(
                    """INSERT INTO validation_results
                       (run_id, suite, table_name, expectation, severity, success, observed, detail)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (run_id, suite, r.table, r.expectation, r.severity, r.success,
                     json.dumps(r.observed), r.detail),
                )
        conn.commit()


def _print(result: SuiteResult) -> None:
    status = "PASS" if result.success else "FAIL"
    print(f"\n=== {result.suite}  [{status}]  run={result.run_id} ===")
    for r in result.results:
        mark = "✓" if r.success else "✗"
        print(f"  {mark} [{r.severity}] {r.expectation}: {r.detail}")


def main() -> int:
    import argparse

    from src.ingestion.cdc_merge import _build_spark

    ap = argparse.ArgumentParser(description="Run Silver expectation suites.")
    ap.add_argument("suite", nargs="?", default="all",
                    help="suite name or 'all' (default)")
    ap.add_argument("--silver", default=os.environ.get("DELTA_SILVER_PATH", "/data/delta/silver"))
    ap.add_argument("--no-persist", action="store_true", help="don't write to Postgres")
    args = ap.parse_args()

    spark = _build_spark("shift-quality")
    spark.sparkContext.setLogLevel("ERROR")

    names = sorted(SUITES) if args.suite == "all" else [args.suite]
    overall_ok = True
    for name in names:
        result = run_suite(spark, name, args.silver, persist=not args.no_persist)
        _print(result)
        overall_ok = overall_ok and result.success

    print(f"\nOverall: {'PASS' if overall_ok else 'FAIL'}")
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
