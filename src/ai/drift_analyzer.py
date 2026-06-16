"""LLM schema-drift analyzer: explain a breaking change + suggest a strategy.

Takes a SchemaDriftReport (Phase 3) and asks the LLM for impact, affected
downstream dbt models, and a recommended migration action. Responses are cached
by drift pattern — the same change set yields the same explanation without a
second API call.
"""
from __future__ import annotations

import hashlib
import json
import os

from src.ai.llm_client import LLMClient
from src.transformation.schema_drift import SchemaDriftReport

# Which Gold/dbt models depend on each source table (context for impact analysis).
DOWNSTREAM_MODELS = {
    "orders": ["stg_orders", "gold_revenue_daily", "gold_customer_segments", "gold_migration_summary"],
    "customers": ["stg_customers", "gold_customer_segments"],
    "products": ["stg_products", "gold_inventory_health"],
    "inventory": ["stg_inventory", "gold_inventory_health"],
    "shipments": ["gold_migration_summary"],
}

SYSTEM = (
    "You are a senior data engineer reviewing a PostgreSQL→Snowflake migration. "
    "A schema change was detected on the source. Explain the impact concisely and "
    "recommend a concrete migration strategy. Be specific and pragmatic."
)

_CACHE_DIR = os.environ.get("LLM_CACHE_DIR", "data/llm_cache")


def _pattern_key(report: SchemaDriftReport) -> str:
    sig = json.dumps(
        [(c.column, c.change_type.value, c.severity.value) for c in report.changes],
        sort_keys=True,
    )
    return hashlib.sha256(f"{report.table}:{sig}".encode()).hexdigest()[:16]


def analyze(report: SchemaDriftReport, *, client: LLMClient | None = None, use_cache: bool = True) -> dict:
    key = _pattern_key(report)
    cache_path = os.path.join(_CACHE_DIR, f"drift_{key}.json")
    if use_cache and os.path.exists(cache_path):
        with open(cache_path) as fh:
            return json.load(fh)

    downstream = DOWNSTREAM_MODELS.get(report.table, [])
    changes_text = "\n".join(
        f"- {c.change_type.value} [{c.severity.value}] {c.column}: {c.detail}"
        for c in report.changes
    )
    user = (
        f"Source table: {report.table}\n"
        f"Recommended action from rules engine: {report.recommended_action}\n"
        f"Detected changes:\n{changes_text}\n\n"
        f"Downstream dbt models that read this table: {', '.join(downstream) or 'none'}\n\n"
        "Return JSON with keys: impact_summary (string, 2-3 sentences), "
        "affected_downstream_models (array of strings, only those genuinely at risk), "
        "recommended_action (string), severity (one of LOW, MEDIUM, HIGH)."
    )

    client = client or LLMClient()
    result = client.complete_json(SYSTEM, user, max_tokens=900)

    if use_cache:
        os.makedirs(_CACHE_DIR, exist_ok=True)
        with open(cache_path, "w") as fh:
            json.dump(result, fh, indent=2)
    return result
