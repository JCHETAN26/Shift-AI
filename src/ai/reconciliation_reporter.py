"""AI reconciliation reporter: explain reconciliation discrepancies in English.

Takes a reconciliation result (the Phase 7 report, or a row from
`reconciliation_results`) and asks the LLM for a plain-English explanation,
the probable root cause, and recommended next steps.
"""
from __future__ import annotations

from src.ai.llm_client import LLMClient

SYSTEM = (
    "You are a data engineer explaining data-reconciliation results between a "
    "PostgreSQL source and a Snowflake target to a stakeholder. Be clear, concise, "
    "and concrete about likely causes and next steps."
)


def explain(result: dict, *, client: LLMClient | None = None) -> dict:
    """``result`` keys: table, source_rows, target_rows, count_match,
    checksum_match, sample_size, sample_mismatches, status, and optionally
    discrepancies (list)."""
    delta = (result.get("source_rows") or 0) - (result.get("target_rows") or 0)
    disc = result.get("discrepancies") or []
    disc_text = ""
    if disc:
        disc_text = "Sample discrepancy examples:\n" + "\n".join(
            f"- {d}" for d in disc[:5]
        )

    user = (
        f"Table: {result['table']}\n"
        f"Status: {result.get('status')}\n"
        f"Source rows: {result.get('source_rows')}, Target rows: {result.get('target_rows')}, "
        f"delta: {delta}\n"
        f"Row count match: {result.get('count_match')}\n"
        f"Checksum match: {result.get('checksum_match')}\n"
        f"Sample comparison: {result.get('sample_mismatches')} mismatches "
        f"out of {result.get('sample_size')}\n"
        f"{disc_text}\n\n"
        "Return JSON with keys: explanation (string, 2-4 sentences in plain English), "
        "probable_root_cause (string), recommended_action (string), "
        "severity (one of OK, LOW, MEDIUM, HIGH)."
    )

    client = client or LLMClient()
    return client.complete_json(SYSTEM, user, max_tokens=900)
