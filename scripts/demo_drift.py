#!/usr/bin/env python3
"""Demonstrate schema-drift detection against the live source.

Establishes a baseline from the current Postgres schema, then injects schema
changes inside *rolled-back* transactions so the real source (and Debezium) are
never actually modified — while information_schema still reflects the change
within the transaction, so detection sees it.

    python scripts/demo_drift.py            # default table: products
    python scripts/demo_drift.py orders
"""
from __future__ import annotations

import os
import sys

import psycopg

from src.common.tables import TABLES
from src.transformation.schema_drift import (
    detect_drift,
    postgres_schema,
    save_baseline,
)

DSN = os.environ.get("POSTGRES_DSN", "postgresql://shift:shift_dev_password@localhost:5435/shift")
BASELINE_DIR = os.environ.get("SCHEMA_BASELINE_DIR", "data/schema_baselines")


def _print(report) -> None:
    print(f"  action: {report.recommended_action.upper()}  "
          f"(breaking={report.has_breaking}, changes={len(report.changes)})")
    for c in report.changes:
        print(f"    [{c.severity.value:12}] {c.change_type.value:13} {c.column}: {c.detail}")


def main() -> int:
    table = TABLES[sys.argv[1]] if len(sys.argv) > 1 else TABLES["products"]
    print(f"Table: {table.name}  |  baseline dir: {BASELINE_DIR}\n")

    # 1) Establish baseline from the current (committed) source schema.
    with psycopg.connect(DSN) as conn:
        save_baseline(BASELINE_DIR, table.name, postgres_schema(conn, table.name))
        print("① Baseline captured from current source schema.")
        _print(detect_drift(conn, table, BASELINE_DIR))

    # 2) BREAKING: drop a column (rolled back).
    print("\n② Inject BREAKING change (DROP COLUMN) — rolled back after detection:")
    drop_col = "category" if table.name == "products" else table.columns[-1]
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(f"ALTER TABLE {table.name} DROP COLUMN {drop_col}")
        _print(detect_drift(conn, table, BASELINE_DIR))
        conn.rollback()

    # 3) NON_BREAKING: add a column (rolled back).
    print("\n③ Inject NON_BREAKING change (ADD COLUMN) — rolled back after detection:")
    with psycopg.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(f"ALTER TABLE {table.name} ADD COLUMN promo_code varchar(20)")
        _print(detect_drift(conn, table, BASELINE_DIR))
        conn.rollback()

    print("\nSource schema untouched (all changes rolled back).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
