"""Unit tests for Bronze→Silver cleaning + validation/quarantine."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.tables import TABLES
from src.transformation.bronze_to_silver import split_clean_and_quarantine

PAST = datetime(2025, 1, 1, 12, 0, 0)
LATER = PAST + timedelta(days=1)
FUTURE = datetime(2999, 1, 1, 0, 0, 0)

# Spark types per column, used to build explicit schemas (None defaults need it).
_COLTYPES = {
    "customer_id": LongType(), "order_id": LongType(), "product_id": LongType(),
    "inventory_id": LongType(), "shipment_id": LongType(), "warehouse_id": IntegerType(),
    "quantity": IntegerType(), "reorder_point": IntegerType(),
    "unit_price": DoubleType(), "unit_cost": DoubleType(),
    "name": StringType(), "email": StringType(), "segment": StringType(),
    "region": StringType(), "category": StringType(), "status": StringType(),
    "channel": StringType(), "carrier": StringType(),
    "is_active": BooleanType(),
    "created_at": TimestampType(), "updated_at": TimestampType(),
    "shipped_at": TimestampType(), "delivered_at": TimestampType(),
    "estimated_delivery": TimestampType(),
}

_DEFAULTS = {
    "customers": dict(customer_id=1, name="n", email="a@b.com", segment="consumer",
                      region="NA", created_at=PAST, updated_at=PAST),
    "products": dict(product_id=1, name="p", category="electronics", unit_cost=1.0,
                     is_active=True, created_at=PAST),
    "orders": dict(order_id=1, customer_id=1, product_id=1, quantity=2, unit_price=10.0,
                   status="pending", channel="web", created_at=PAST, updated_at=PAST),
    "inventory": dict(inventory_id=1, product_id=1, warehouse_id=1, quantity=5,
                      reorder_point=10, updated_at=PAST),
    "shipments": dict(shipment_id=1, order_id=1, carrier="ups", status="delivered",
                      shipped_at=PAST, delivered_at=LATER, estimated_delivery=LATER),
}


def _df(spark, table_name, rows):
    """Build a Bronze-shaped DataFrame; each row dict overrides table defaults."""
    cols = TABLES[table_name].columns
    schema = StructType([StructField(c, _COLTYPES[c], True) for c in cols])
    data = []
    for i, overrides in enumerate(rows):
        base = dict(_DEFAULTS[table_name])
        base[TABLES[table_name].primary_key] = i + 1  # unique pk unless overridden
        base.update(overrides)
        data.append(tuple(base[c] for c in cols))
    return spark.createDataFrame(data, schema=schema)


def _split(spark, table_name, rows):
    clean, quar = split_clean_and_quarantine(table_name, _df(spark, table_name, rows))
    clean_rows = [r.asDict() for r in clean.collect()]
    quar_rows = [r.asDict() for r in quar.collect()]
    return clean_rows, quar_rows


def test_orders_total_amount_is_derived(spark):
    clean, quar = _split(spark, "orders", [{"quantity": 3, "unit_price": 4.5}])
    assert quar == []
    assert clean[0]["total_amount"] == 13.5


def test_orders_negative_price_quarantined(spark):
    clean, quar = _split(spark, "orders", [
        {"order_id": 1, "unit_price": 10.0},
        {"order_id": 2, "unit_price": -5.0},
    ])
    assert {r["order_id"] for r in clean} == {1}
    assert quar[0]["order_id"] == 2
    assert "negative_unit_price" in quar[0]["_violations"]


def test_orders_non_positive_quantity_quarantined(spark):
    _, quar = _split(spark, "orders", [{"quantity": 0}])
    assert "non_positive_quantity" in quar[0]["_violations"]


def test_customers_invalid_email_quarantined_and_segment_normalized(spark):
    clean, quar = _split(spark, "customers", [
        {"customer_id": 1, "email": "Good@Example.com", "segment": " B2C "},
        {"customer_id": 2, "email": "not-an-email"},
    ])
    assert clean[0]["email"] == "good@example.com"      # lowercased
    assert clean[0]["segment"] == "consumer"            # normalized from B2C
    assert quar[0]["customer_id"] == 2
    assert "invalid_email" in quar[0]["_violations"]


def test_inventory_negative_quantity_quarantined(spark):
    _, quar = _split(spark, "inventory", [{"quantity": -1}])
    assert "negative_quantity" in quar[0]["_violations"]


def test_shipments_illegal_status_quarantined(spark):
    _, quar = _split(spark, "shipments", [{"status": "teleported"}])
    assert "illegal_status" in quar[0]["_violations"]


def test_shipments_delivered_without_timestamp_quarantined(spark):
    _, quar = _split(spark, "shipments", [{"status": "delivered", "delivered_at": None}])
    assert "delivered_without_timestamp" in quar[0]["_violations"]


def test_null_primary_key_quarantined(spark):
    _, quar = _split(spark, "orders", [{"order_id": None}])
    assert "null_primary_keys" in quar[0]["_violations"]


def test_duplicate_primary_keys_quarantined(spark):
    _, quar = _split(spark, "orders", [{"order_id": 7}, {"order_id": 7}])
    assert len(quar) == 2
    assert all("duplicate_pks" in r["_violations"] for r in quar)


def test_future_timestamp_quarantined(spark):
    _, quar = _split(spark, "orders", [{"updated_at": FUTURE}])
    assert "future_timestamps" in quar[0]["_violations"]


def test_all_valid_rows_pass_through_clean(spark):
    clean, quar = _split(spark, "orders", [{"order_id": 1}, {"order_id": 2}, {"order_id": 3}])
    assert len(clean) == 3
    assert quar == []


def test_row_can_trip_multiple_violations(spark):
    _, quar = _split(spark, "inventory", [{"quantity": -1, "reorder_point": -2}])
    v = quar[0]["_violations"]
    assert "negative_quantity" in v and "negative_reorder_point" in v
