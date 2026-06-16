"""Shift.ai dashboard API — FastAPI (REST + SSE).

Serves the 4 dashboard views from everything the pipeline populated:
  /api/overview   — migration scorecard (reconciliation_results)
  /api/quality    — Great Expectations suites + reconciliation (validation_results)
  /api/drift      — schema-drift events + LLM explanation
  /api/catalog    — RAG catalog search + column profiles
  /api/stream     — SSE live migration progress
"""
from __future__ import annotations

import asyncio
import json

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse

from src.api import db

app = FastAPI(title="Shift.ai API", version="1.0.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)


# ── Health ─────────────────────────────────────────────────────────────
@app.get("/api/health")
def health():
    return {"status": "ok"}


# ── View 1: Migration Overview ─────────────────────────────────────────
def _latest_recon() -> list[dict]:
    run = db.query_one("SELECT max(run_id) AS run_id FROM reconciliation_results")
    if not run or not run["run_id"]:
        return []
    return db.query(
        """SELECT table_name, source_rows, target_rows, count_match, checksum_match,
                  sample_size, sample_mismatches, status, reconciled_at
           FROM reconciliation_results WHERE run_id = %s ORDER BY table_name""",
        (run["run_id"],),
    )


@app.get("/api/overview")
def overview():
    rows = _latest_recon()
    tables = [
        {
            "table": r["table_name"],
            "source_rows": r["source_rows"],
            "target_rows": r["target_rows"],
            "delta": (r["source_rows"] or 0) - (r["target_rows"] or 0),
            "count_match": r["count_match"],
            "checksum_match": r["checksum_match"],
            "sample_mismatches": r["sample_mismatches"],
            "status": r["status"],
            "last_synced": r["reconciled_at"].isoformat() if r["reconciled_at"] else None,
        }
        for r in rows
    ]
    reconciled = sum(1 for t in tables if t["status"] == "RECONCILED")
    if any(t["status"] != "RECONCILED" for t in tables):
        overall = "DRIFTED" if tables else "PENDING"
    else:
        overall = "HEALTHY"
    return {
        "overall_status": overall,
        "tables_reconciled": reconciled,
        "tables_total": len(tables),
        "total_source_rows": sum(t["source_rows"] or 0 for t in tables),
        "total_target_rows": sum(t["target_rows"] or 0 for t in tables),
        "tables": tables,
    }


# ── View 3: Data Quality ───────────────────────────────────────────────
@app.get("/api/quality")
def quality():
    # latest run per suite
    runs = db.query(
        """SELECT DISTINCT ON (suite) suite, run_id, validated_at
           FROM validation_results ORDER BY suite, validated_at DESC"""
    )
    suites = []
    for r in runs:
        expectations = db.query(
            """SELECT expectation, table_name, severity, success, observed, detail
               FROM validation_results WHERE suite = %s AND run_id = %s
               ORDER BY success, expectation""",
            (r["suite"], r["run_id"]),
        )
        suites.append({
            "suite": r["suite"],
            "validated_at": r["validated_at"].isoformat() if r["validated_at"] else None,
            "passed": sum(1 for e in expectations if e["success"]),
            "total": len(expectations),
            "success": all(e["success"] for e in expectations if e["severity"] == "critical"),
            "expectations": expectations,
        })
    return {"suites": suites, "reconciliation": _latest_recon()}


@app.get("/api/recon/explain")
def recon_explain(table: str = Query(...)):
    from src.ai.reconciliation_reporter import explain

    rows = _latest_recon()
    row = next((r for r in rows if r["table_name"] == table), None)
    if not row:
        return {"error": f"no reconciliation result for {table}"}
    return explain({
        "table": row["table_name"], "source_rows": row["source_rows"],
        "target_rows": row["target_rows"], "count_match": row["count_match"],
        "checksum_match": row["checksum_match"], "sample_size": row["sample_size"],
        "sample_mismatches": row["sample_mismatches"], "status": row["status"],
    })


# ── View 2: Schema Drift Monitor ───────────────────────────────────────
@app.get("/api/drift")
def drift():
    events = db.query(
        """SELECT id, table_name, column_name, change_type, severity, detail, detected_at
           FROM catalog.schema_drift_events ORDER BY
             CASE severity WHEN 'BREAKING' THEN 0 WHEN 'AMBIGUOUS' THEN 1 ELSE 2 END,
             detected_at DESC"""
    )
    for e in events:
        e["detected_at"] = e["detected_at"].isoformat() if e["detected_at"] else None
    return {"events": events}


@app.get("/api/drift/explain")
def drift_explain(table: str = Query(...)):
    from src.transformation.schema_drift import (
        ChangeType, Severity, SchemaChange, SchemaDriftReport,
    )
    from src.ai.drift_analyzer import analyze

    rows = db.query(
        "SELECT column_name, change_type, severity, detail FROM catalog.schema_drift_events WHERE table_name=%s",
        (table,),
    )
    if not rows:
        return {"error": f"no drift events for {table}"}
    changes = [
        SchemaChange(
            column=r["column_name"], change_type=ChangeType(r["change_type"]),
            severity=Severity(r["severity"]), detail=r["detail"],
        )
        for r in rows
    ]
    report = SchemaDriftReport(table=table, changes=changes)
    return analyze(report, use_cache=True)


# ── View 4: AI Data Catalog ────────────────────────────────────────────
@app.get("/api/catalog/search")
def catalog_search(q: str = Query(...), limit: int = 5):
    from src.ai.catalog import search

    return {"query": q, "results": search(q, limit=limit)}


@app.get("/api/catalog/profile")
def catalog_profile(table: str = Query(...), column: str = Query(...)):
    row = db.query_one(
        """SELECT table_name, column_name, data_type, row_count, null_count, null_rate,
                  distinct_count, min_value, max_value, mean_value, sample_values
           FROM catalog.column_profiles WHERE table_name=%s AND column_name=%s""",
        (table, column),
    )
    return row or {"error": "profile not found"}


# ── SSE: live migration progress ───────────────────────────────────────
@app.get("/api/stream/progress")
async def stream_progress():
    async def gen():
        while True:
            yield {"event": "progress", "data": json.dumps(overview())}
            await asyncio.sleep(2)

    return EventSourceResponse(gen())
