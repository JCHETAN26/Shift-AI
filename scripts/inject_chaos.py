#!/usr/bin/env python3
"""Inject bad data into Silver to trip a Great Expectations validation failure.

Demonstrates that the quality gate catches corruption the Bronze→Silver
quarantine does not (here: a negative inventory quantity written straight into
Silver). Runs the inventory suite afterward so the failure — and its row in
`validation_results` — is visible immediately.

    PYTHONPATH=/app python scripts/inject_chaos.py

Restore clean Silver with:  make silver TABLE=inventory
"""
from __future__ import annotations

import os
from datetime import datetime

from src.ingestion.cdc_merge import _build_spark
from src.quality.runner import _print, run_suite

SILVER = os.environ.get("DELTA_SILVER_PATH", "/data/delta/silver")


def main() -> int:
    spark = _build_spark("shift-chaos")
    spark.sparkContext.setLogLevel("ERROR")

    path = f"{SILVER}/inventory"
    target = spark.read.format("delta").load(path)

    # One physically-impossible row: negative on-hand quantity.
    chaos_row = (999_999_999, 1, 1, -42, 10, datetime.now())  # naive ts (Spark-picklable)
    bad = spark.createDataFrame([chaos_row], schema=target.schema)
    bad.write.format("delta").mode("append").save(path)
    print("💥 Injected 1 chaos row into Silver inventory (quantity = -42).")

    result = run_suite(spark, "silver_inventory_suite", SILVER)
    _print(result)
    print("\nRestore clean Silver with:  make silver TABLE=inventory")
    spark.stop()
    return 0 if result.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
