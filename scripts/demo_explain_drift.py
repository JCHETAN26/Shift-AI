#!/usr/bin/env python3
"""Explain a breaking schema change with the LLM (Phase 3 + Phase 9).

Simulates dropping a column from the source, classifies the drift with the
rules engine, then asks the LLM analyzer for impact + migration strategy.
"""
from __future__ import annotations

import json
import os

from src.ai.drift_analyzer import analyze
from src.transformation.schema_drift import classify_drift, postgres_schema

TABLE = os.environ.get("DRIFT_TABLE", "products")
DROP_COLUMN = os.environ.get("DRIFT_COLUMN", "category")


def _baseline() -> dict[str, str]:
    """Current source schema = the baseline we built from."""
    import psycopg

    with psycopg.connect(os.environ["POSTGRES_DSN"]) as conn:
        return postgres_schema(conn, TABLE)


def main() -> int:
    baseline = _baseline()
    current = {k: v for k, v in baseline.items() if k != DROP_COLUMN}  # simulate the drop
    report = classify_drift(current, baseline, TABLE)

    print(f"Rules engine: action={report.recommended_action}, "
          f"breaking={report.has_breaking}")
    for c in report.changes:
        print(f"  [{c.severity.value}] {c.change_type.value} {c.column}: {c.detail}")

    print("\nLLM analysis:")
    print(json.dumps(analyze(report, use_cache=True), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
