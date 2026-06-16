"""Unit tests for the custom + column expectations and suite aggregation."""
from __future__ import annotations

from src.quality.expectations.base import ExpectationResult, ValidationContext
from src.quality.expectations.custom_expectations import (
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToBeUnique,
    ExpectColumnValuesToNotBeNull,
    ExpectInventoryNonNegative,
    ExpectNoOrphanedOrders,
    ExpectOrderAmountsToMatchLineItems,
    ExpectTimestampMonotonicity,
)
from src.quality.runner import SuiteResult

ORDERS_DDL = ("order_id long, customer_id long, product_id long, "
              "quantity int, unit_price double, total_amount double")


def _ctx(spark, **tables):
    return ValidationContext(tables)


def test_order_amounts_match_passes_when_consistent(spark):
    orders = spark.createDataFrame(
        [(1, 1, 1, 2, 10.0, 20.0), (2, 1, 1, 3, 5.0, 15.0)], ORDERS_DDL)
    res = ExpectOrderAmountsToMatchLineItems().validate(_ctx(spark, orders=orders))
    assert res.success
    assert res.observed["unexpected_count"] == 0


def test_order_amounts_match_fails_on_mismatch(spark):
    orders = spark.createDataFrame(
        [(1, 1, 1, 2, 10.0, 20.0), (2, 1, 1, 3, 5.0, 99.0)], ORDERS_DDL)  # 99 != 15
    res = ExpectOrderAmountsToMatchLineItems().validate(_ctx(spark, orders=orders))
    assert not res.success
    assert res.observed["unexpected_count"] == 1


def test_no_orphaned_orders_passes_when_fks_resolve(spark):
    orders = spark.createDataFrame([(1, 10, 100, 1, 1.0, 1.0)], ORDERS_DDL)
    customers = spark.createDataFrame([(10,)], "customer_id long")
    products = spark.createDataFrame([(100,)], "product_id long")
    res = ExpectNoOrphanedOrders().validate(
        _ctx(spark, orders=orders, customers=customers, products=products))
    assert res.success


def test_no_orphaned_orders_fails_on_missing_customer(spark):
    orders = spark.createDataFrame([(1, 999, 100, 1, 1.0, 1.0)], ORDERS_DDL)
    customers = spark.createDataFrame([(10,)], "customer_id long")  # 999 missing
    products = spark.createDataFrame([(100,)], "product_id long")
    res = ExpectNoOrphanedOrders().validate(
        _ctx(spark, orders=orders, customers=customers, products=products))
    assert not res.success
    assert res.observed["orphan_customer_fk"] == 1


def test_inventory_non_negative(spark):
    ok = spark.createDataFrame([(1, 0), (2, 5)], "inventory_id long, quantity int")
    bad = spark.createDataFrame([(1, -3), (2, 5)], "inventory_id long, quantity int")
    assert ExpectInventoryNonNegative().validate(_ctx(spark, inventory=ok)).success
    res = ExpectInventoryNonNegative().validate(_ctx(spark, inventory=bad))
    assert not res.success and res.observed["unexpected_count"] == 1


def test_timestamp_monotonicity(spark):
    ddl = "customer_id long, created_at timestamp, updated_at timestamp"
    from datetime import datetime
    c, u = datetime(2025, 1, 1), datetime(2025, 1, 2)
    ok = spark.createDataFrame([(1, c, u)], ddl)
    bad = spark.createDataFrame([(1, u, c)], ddl)  # updated < created
    assert ExpectTimestampMonotonicity("customers").validate(_ctx(spark, customers=ok)).success
    assert not ExpectTimestampMonotonicity("customers").validate(_ctx(spark, customers=bad)).success


def test_column_expectations(spark):
    df = spark.createDataFrame(
        [(1, "web"), (2, "web"), (2, None)],
        "id long, channel string")
    ctx = _ctx(spark, t=df)
    assert not ExpectColumnValuesToBeUnique("t", "id").validate(ctx).success      # 2 repeats
    assert not ExpectColumnValuesToNotBeNull("t", "channel").validate(ctx).success  # a null
    assert ExpectColumnValuesToBeInSet("t", "id", {1, 2}).validate(ctx).success
    assert not ExpectColumnValuesToBeBetween("t", "id", max_value=1).validate(ctx).success


def test_suite_result_success_ignores_warnings_but_not_critical():
    crit_fail = ExpectationResult("X", "orders", success=False, severity="critical")
    warn_fail = ExpectationResult("Y", "orders", success=False, severity="warning")
    ok = ExpectationResult("Z", "orders", success=True, severity="critical")

    assert SuiteResult("s", "r", [ok, warn_fail]).success      # warning failure tolerated
    assert not SuiteResult("s", "r", [ok, crit_fail]).success  # critical failure fails suite
    assert SuiteResult("s", "r", [ok]).success
