"""Unit tests for reconciliation comparison logic + checksum SQL (pure)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from src.transformation.reconcile import (
    ReconciliationReport,
    _checksum_sql,
    compare_row,
)

COLS = ["order_id", "quantity", "status", "unit_price", "created_at"]


def _report(**kw):
    base = dict(table="orders", source_count=10, target_count=10, count_match=True,
                checksum_match=True, source_checksum=1, target_checksum=1,
                sample_size=1000, sample_mismatches=0)
    base.update(kw)
    return ReconciliationReport(**base)


def test_identical_rows_have_no_diffs():
    t = datetime(2025, 1, 1, tzinfo=timezone.utc)
    row = {"order_id": 1, "quantity": 3, "status": "pending", "unit_price": Decimal("10.00"), "created_at": t}
    assert compare_row(COLS, row, dict(row)) == {}


def test_string_and_int_mismatch_detected():
    a = {"order_id": 1, "quantity": 3, "status": "pending", "unit_price": Decimal("10.0"), "created_at": None}
    b = {**a, "status": "confirmed"}
    diffs = compare_row(COLS, a, b)
    assert "status" in diffs and "quantity" not in diffs


def test_int_vs_decimal_same_value_matches():
    # Snowflake often returns NUMBER as Decimal; source returns int.
    a = {"order_id": 1, "quantity": 3}
    b = {"order_id": Decimal("1"), "quantity": Decimal("3")}
    assert compare_row(["order_id", "quantity"], a, b) == {}


def test_decimal_within_tolerance_matches_but_outside_differs():
    a = {"unit_price": Decimal("10.00")}
    assert compare_row(["unit_price"], a, {"unit_price": Decimal("10.004")}) == {}
    assert "unit_price" in compare_row(["unit_price"], a, {"unit_price": Decimal("10.05")})


def test_timestamp_within_tolerance_matches():
    t = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    naive = t.replace(tzinfo=None)                      # target stored tz-naive UTC
    assert compare_row(["created_at"], {"created_at": t}, {"created_at": naive}) == {}
    far = (t + timedelta(seconds=5)).replace(tzinfo=None)
    assert "created_at" in compare_row(["created_at"], {"created_at": t}, {"created_at": far})


def test_timestamp_iso_string_target_matches_datetime_source():
    # Debezium encodes timestamps as ISO strings -> Snowflake VARCHAR; the
    # source side is a native datetime. Same instant must reconcile.
    t = datetime(2025, 11, 11, 0, 9, 23, 426196, tzinfo=timezone.utc)
    assert compare_row(["created_at"], {"created_at": t},
                       {"created_at": "2025-11-11T00:09:23.426196Z"}) == {}


def test_null_vs_value_is_a_diff():
    assert "status" in compare_row(["status"], {"status": None}, {"status": "x"})


def test_report_status_transitions():
    assert _report().status == "RECONCILED"
    assert _report(count_match=False).status == "COUNT_MISMATCH"
    assert _report(checksum_match=False).status == "CHECKSUM_MISMATCH"
    assert _report(sample_mismatches=4).status == "SAMPLE_MISMATCH"


def test_checksum_sql_uses_engine_specific_hex_conversion():
    pg = _checksum_sql("orders", ["order_id", "status"], engine="postgres")
    sf = _checksum_sql("orders", ["order_id", "status"], engine="snowflake")
    assert "::bit(60)::bigint" in pg and "public.orders" in pg
    assert "repeat('X', 15)" in sf and "RAW.ORDERS" in sf
    assert "order_id::text" in pg and "to_varchar(order_id)" in sf
