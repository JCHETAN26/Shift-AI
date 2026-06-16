"""Unit tests for the pure AI-layer helpers (no Spark / Qdrant / LLM calls)."""
from __future__ import annotations

import pytest

from src.ai.catalog import build_catalog_documents
from src.ai.llm_client import extract_json

PROFILES = [
    {"table_name": "orders", "column_name": "total_amount", "data_type": "double",
     "null_rate": 0.0, "distinct_count": 480000, "sample_values": ["19.99", "250.00"]},
    {"table_name": "orders", "column_name": "customer_id", "data_type": "bigint",
     "null_rate": 0.0, "distinct_count": 99000, "sample_values": ["1", "2"]},
    {"table_name": "customers", "column_name": "email", "data_type": "string",
     "null_rate": 0.0, "distinct_count": 100000, "sample_values": ["a@b.com"]},
]


def test_build_documents_has_one_per_column_plus_table_summaries():
    docs = build_catalog_documents(PROFILES)
    cols = [d for d in docs if d.payload["kind"] == "column"]
    tables = [d for d in docs if d.payload["kind"] == "table"]
    assert len(cols) == 3
    assert {d.payload["table"] for d in tables} == {"orders", "customers"}


def test_column_doc_text_mentions_table_column_and_samples():
    doc = next(d for d in build_catalog_documents(PROFILES)
               if d.payload.get("column") == "total_amount")
    assert "orders" in doc.text and "total_amount" in doc.text
    assert "19.99" in doc.text          # sample values embedded for retrieval
    assert "total amount" in doc.text   # humanized for semantic match


def test_point_ids_are_unique():
    ids = [d.point_id for d in build_catalog_documents(PROFILES)]
    assert len(ids) == len(set(ids))


def test_table_summary_lists_columns():
    doc = next(d for d in build_catalog_documents(PROFILES)
               if d.payload["kind"] == "table" and d.payload["table"] == "orders")
    assert "total_amount" in doc.text and "customer_id" in doc.text


def test_extract_json_plain():
    assert extract_json('{"a": 1, "b": "x"}') == {"a": 1, "b": "x"}


def test_extract_json_from_markdown_fence():
    text = 'Here is the result:\n```json\n{"impact": "high", "models": ["a"]}\n```\nDone.'
    assert extract_json(text) == {"impact": "high", "models": ["a"]}


def test_extract_json_with_surrounding_prose():
    assert extract_json('Sure! {"x": [1, 2]} hope that helps') == {"x": [1, 2]}


def test_extract_json_raises_when_absent():
    with pytest.raises(ValueError):
        extract_json("no json here")
