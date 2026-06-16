#!/usr/bin/env python3
"""Seed catalog.schema_drift_events for the dashboard's Schema Drift Monitor.

These mirror the kinds of changes the Phase-3 detector classifies (a breaking
drop, a non-breaking add, an ambiguous rename) so the view shows the full
RED / YELLOW / GREEN range and the click-to-explain flow has content.
"""
from __future__ import annotations

import os

import psycopg

DDL = """
CREATE SCHEMA IF NOT EXISTS catalog;
CREATE TABLE IF NOT EXISTS catalog.schema_drift_events (
    id BIGSERIAL PRIMARY KEY,
    table_name TEXT NOT NULL, column_name TEXT NOT NULL,
    change_type TEXT NOT NULL, severity TEXT NOT NULL, detail TEXT,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

EVENTS = [
    ("products", "category", "DROPPED", "BREAKING",
     "column category (text) dropped from source"),
    ("orders", "promo_code", "ADDED", "NON_BREAKING",
     "new column promo_code varchar(20)"),
    ("customers", "loyalty_tier", "RENAMED", "AMBIGUOUS",
     "'tier' may have been renamed to 'loyalty_tier' (same type varchar, name similarity 0.62)"),
]


def main() -> int:
    dsn = os.environ["POSTGRES_DSN"]
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute("DELETE FROM catalog.schema_drift_events")
            for ev in EVENTS:
                cur.execute(
                    """INSERT INTO catalog.schema_drift_events
                       (table_name, column_name, change_type, severity, detail)
                       VALUES (%s,%s,%s,%s,%s)""",
                    ev,
                )
        conn.commit()
    print(f"Seeded {len(EVENTS)} schema-drift events.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
