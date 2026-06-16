"""Unit tests for the schema-drift classifier (pure — no Spark/Postgres)."""
from __future__ import annotations

from src.transformation.schema_drift import (
    ChangeType,
    Severity,
    classify_drift,
    compare_types,
    parse_type,
)

# A realistic baseline schema (what the Delta target currently has).
BASE = {
    "order_id": "bigint",
    "customer_id": "bigint",
    "quantity": "int",
    "unit_price": "decimal(12,2)",
    "status": "string",
    "created_at": "timestamp",
}


def _by_col(report):
    return {c.column: c for c in report.changes}


def test_no_change_returns_empty_report():
    report = classify_drift(dict(BASE), dict(BASE), "orders")
    assert report.is_empty
    assert report.changes == []
    assert report.recommended_action == "none"
    assert not report.has_breaking


def test_added_column_is_non_breaking():
    source = dict(BASE, discount_pct="decimal(5,2)")
    report = classify_drift(source, dict(BASE), "orders")
    change = _by_col(report)["discount_pct"]
    assert change.change_type == ChangeType.ADDED
    assert change.severity == Severity.NON_BREAKING
    assert report.recommended_action == "auto_evolve"


def test_dropped_column_is_breaking():
    target = dict(BASE)
    source = {k: v for k, v in BASE.items() if k != "status"}  # source lost 'status'
    report = classify_drift(source, target, "orders")
    change = _by_col(report)["status"]
    assert change.change_type == ChangeType.DROPPED
    assert change.severity == Severity.BREAKING
    assert report.has_breaking
    assert report.recommended_action == "halt"


def test_type_widened_is_non_breaking():
    target = dict(BASE)
    source = dict(BASE, quantity="bigint")  # int → bigint
    report = classify_drift(source, target, "orders")
    change = _by_col(report)["quantity"]
    assert change.change_type == ChangeType.TYPE_WIDENED
    assert change.severity == Severity.NON_BREAKING


def test_type_narrowed_is_breaking():
    target = dict(BASE)
    source = dict(BASE, order_id="int")  # bigint → int
    report = classify_drift(source, target, "orders")
    change = _by_col(report)["order_id"]
    assert change.change_type == ChangeType.TYPE_NARROWED
    assert change.severity == Severity.BREAKING


def test_cross_family_change_is_breaking():
    target = dict(BASE)
    source = dict(BASE, quantity="string")  # int → string
    report = classify_drift(source, target, "orders")
    change = _by_col(report)["quantity"]
    assert change.change_type == ChangeType.TYPE_CHANGED
    assert change.severity == Severity.BREAKING


def test_decimal_widening_and_narrowing():
    assert compare_types(parse_type("decimal(14,2)"), parse_type("decimal(12,2)")).value == "WIDENED"
    assert compare_types(parse_type("decimal(10,2)"), parse_type("decimal(12,2)")).value == "NARROWED"
    assert compare_types(parse_type("decimal(12,2)"), parse_type("decimal(12,2)")).value == "SAME"


def test_rename_detected_as_ambiguous():
    # 'discount' dropped, 'discount_code' added, same type, similar name.
    target = dict(BASE, discount="string")
    source = dict(BASE, discount_code="string")
    report = classify_drift(source, target, "orders")
    changes = _by_col(report)
    assert "discount_code" in changes
    rename = changes["discount_code"]
    assert rename.change_type == ChangeType.RENAMED
    assert rename.severity == Severity.AMBIGUOUS
    assert rename.renamed_from == "discount"
    # Not also reported as a breaking drop / plain add.
    assert "discount" not in changes
    assert report.recommended_action == "review"


def test_dissimilar_names_not_treated_as_rename():
    # Same type but unrelated names → must stay DROP (breaking) + ADD, not rename.
    target = dict(BASE, legacy_blob="string")
    source = dict(BASE, shipping_notes="string")
    report = classify_drift(source, target, "orders")
    changes = _by_col(report)
    assert changes["legacy_blob"].change_type == ChangeType.DROPPED
    assert changes["shipping_notes"].change_type == ChangeType.ADDED
    assert report.has_breaking


def test_multiple_simultaneous_changes():
    target = dict(BASE)
    source = dict(BASE)
    source["region"] = "string"        # ADDED
    del source["status"]               # DROPPED
    source["quantity"] = "bigint"      # WIDENED
    source["order_id"] = "int"         # NARROWED
    report = classify_drift(source, target, "orders")
    changes = _by_col(report)
    assert changes["region"].severity == Severity.NON_BREAKING
    assert changes["status"].change_type == ChangeType.DROPPED
    assert changes["quantity"].change_type == ChangeType.TYPE_WIDENED
    assert changes["order_id"].change_type == ChangeType.TYPE_NARROWED
    assert len(report.changes) == 4
    assert report.has_breaking
    # Breaking changes sort first for triage.
    assert report.changes[0].severity == Severity.BREAKING
