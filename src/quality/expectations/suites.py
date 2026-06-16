"""Expectation suites per table (mirrors the build plan's suite shapes)."""
from __future__ import annotations

from src.quality.expectations.base import Expectation
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

ORDER_STATUS = {"pending", "confirmed", "shipped", "delivered", "cancelled"}
CHANNELS = {"web", "mobile", "retail", "partner"}
SEGMENTS = {"consumer", "smb", "enterprise"}
REGIONS = {"NA", "EMEA", "APAC", "LATAM"}


def silver_orders_suite() -> list[Expectation]:
    """8 expectations, 2 custom (cross-table)."""
    return [
        ExpectColumnValuesToNotBeNull("orders", "order_id"),
        ExpectColumnValuesToBeUnique("orders", "order_id"),
        ExpectColumnValuesToBeInSet("orders", "status", ORDER_STATUS),
        ExpectColumnValuesToBeInSet("orders", "channel", CHANNELS),
        ExpectColumnValuesToBeBetween("orders", "quantity", min_value=1),
        ExpectColumnValuesToBeBetween("orders", "unit_price", min_value=0),
        ExpectOrderAmountsToMatchLineItems(),          # custom
        ExpectNoOrphanedOrders(),                      # custom
    ]


def silver_customers_suite() -> list[Expectation]:
    """6 expectations, 1 custom."""
    return [
        ExpectColumnValuesToNotBeNull("customers", "customer_id"),
        ExpectColumnValuesToBeUnique("customers", "customer_id"),
        ExpectColumnValuesToNotBeNull("customers", "email"),
        ExpectColumnValuesToBeInSet("customers", "segment", SEGMENTS),
        ExpectColumnValuesToBeInSet("customers", "region", REGIONS),
        ExpectTimestampMonotonicity("customers"),      # custom
    ]


def silver_inventory_suite() -> list[Expectation]:
    return [
        ExpectColumnValuesToNotBeNull("inventory", "inventory_id"),
        ExpectColumnValuesToBeUnique("inventory", "inventory_id"),
        ExpectInventoryNonNegative(),                  # custom
    ]


# suite name -> (builder, tables the suite needs loaded into context)
SUITES: dict[str, tuple] = {
    "silver_orders_suite": (silver_orders_suite, ["orders", "customers", "products"]),
    "silver_customers_suite": (silver_customers_suite, ["customers"]),
    "silver_inventory_suite": (silver_inventory_suite, ["inventory"]),
}
