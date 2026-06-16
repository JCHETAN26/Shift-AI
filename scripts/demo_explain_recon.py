#!/usr/bin/env python3
"""Explain the latest reconciliation results in plain English (LLM).

Reads the most recent `reconciliation_results` rows from Postgres (written by
Phase 7) and runs each through the AI reconciliation reporter. Highlights the
shipments discrepancy.
"""
from __future__ import annotations

import json
import os

import psycopg

from src.ai.reconciliation_reporter import explain

DSN = os.environ["POSTGRES_DSN"]


def main() -> int:
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT max(run_id) FROM reconciliation_results")
        run_id = cur.fetchone()[0]
        cur.execute(
            """SELECT table_name, source_rows, target_rows, count_match, checksum_match,
                      sample_size, sample_mismatches, status, discrepancies
               FROM reconciliation_results WHERE run_id = %s ORDER BY status DESC, table_name""",
            (run_id,),
        )
        names = [d[0] for d in cur.description]
        rows = [dict(zip(names, r)) for r in cur.fetchall()]

    for row in rows:
        if row["status"] == "RECONCILED":
            print(f"\n### {row['table_name']}: RECONCILED "
                  f"({row['source_rows']}={row['target_rows']}) — no explanation needed")
            continue
        print(f"\n### {row['table_name']}: {row['status']} "
              f"(source={row['source_rows']} target={row['target_rows']})")
        result = explain({
            "table": row["table_name"],
            "source_rows": row["source_rows"], "target_rows": row["target_rows"],
            "count_match": row["count_match"], "checksum_match": row["checksum_match"],
            "sample_size": row["sample_size"], "sample_mismatches": row["sample_mismatches"],
            "status": row["status"], "discrepancies": row["discrepancies"],
        })
        print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
