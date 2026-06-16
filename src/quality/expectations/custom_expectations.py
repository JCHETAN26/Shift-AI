"""Expectations: 4 custom (cross-table / business-rule) + reusable column ones.

Custom expectations (the depth):
  ExpectOrderAmountsToMatchLineItems — total_amount == quantity * unit_price
  ExpectNoOrphanedOrders             — every order FK resolves (customers/products)
  ExpectInventoryNonNegative         — physical inventory can't go negative
  ExpectTimestampMonotonicity        — updated_at >= created_at
"""
from __future__ import annotations

from pyspark.sql import functions as F

from src.quality.expectations.base import Expectation, ExpectationResult, ValidationContext


# ── Custom expectations ────────────────────────────────────────────────
class ExpectOrderAmountsToMatchLineItems(Expectation):
    """orders.total_amount must equal round(quantity * unit_price, 2)."""

    tolerance = 0.01

    def validate(self, ctx: ValidationContext) -> ExpectationResult:
        df = ctx.table("orders")
        expected = F.round(F.col("quantity") * F.col("unit_price"), 2)
        mismatched = df.filter(F.abs(F.col("total_amount") - expected) > self.tolerance)
        bad = mismatched.count()
        total = df.count()
        return self._result(
            "orders", success=(bad == 0),
            observed={"unexpected_count": bad, "element_count": total},
            detail=f"{bad}/{total} orders where total_amount != quantity*unit_price",
        )


class ExpectNoOrphanedOrders(Expectation):
    """Every orders.customer_id and product_id must exist in its parent table."""

    def validate(self, ctx: ValidationContext) -> ExpectationResult:
        orders = ctx.table("orders")
        customers = ctx.table("customers").select("customer_id")
        products = ctx.table("products").select("product_id")

        orphan_customers = orders.join(customers, "customer_id", "left_anti").count()
        orphan_products = orders.join(products, "product_id", "left_anti").count()
        total = orphan_customers + orphan_products
        return self._result(
            "orders", success=(total == 0),
            observed={
                "orphan_customer_fk": orphan_customers,
                "orphan_product_fk": orphan_products,
            },
            detail=f"{orphan_customers} orders with missing customer, "
                   f"{orphan_products} with missing product",
        )


class ExpectInventoryNonNegative(Expectation):
    """inventory.quantity must be >= 0 (physical stock can't be negative)."""

    def validate(self, ctx: ValidationContext) -> ExpectationResult:
        df = ctx.table("inventory")
        bad = df.filter(F.col("quantity") < 0).count()
        total = df.count()
        return self._result(
            "inventory", success=(bad == 0),
            observed={"unexpected_count": bad, "element_count": total},
            detail=f"{bad}/{total} inventory rows with negative quantity",
        )


class ExpectTimestampMonotonicity(Expectation):
    """updated_at must be >= created_at for every row."""

    def __init__(self, table: str, *, created="created_at", updated="updated_at"):
        self._table = table
        self._created = created
        self._updated = updated

    @property
    def name(self) -> str:
        return f"ExpectTimestampMonotonicity({self._table})"

    def validate(self, ctx: ValidationContext) -> ExpectationResult:
        df = ctx.table(self._table)
        bad = df.filter(F.col(self._updated) < F.col(self._created)).count()
        total = df.count()
        return self._result(
            self._table, success=(bad == 0),
            observed={"unexpected_count": bad, "element_count": total},
            detail=f"{bad}/{total} rows where {self._updated} < {self._created}",
        )


# ── Reusable column expectations (GE built-in equivalents) ─────────────
class ExpectColumnValuesToNotBeNull(Expectation):
    def __init__(self, table: str, column: str):
        self._table, self._column = table, column

    @property
    def name(self) -> str:
        return f"ExpectColumnValuesToNotBeNull({self._column})"

    def validate(self, ctx: ValidationContext) -> ExpectationResult:
        df = ctx.table(self._table)
        bad = df.filter(F.col(self._column).isNull()).count()
        return self._result(self._table, bad == 0,
                            {"unexpected_count": bad, "element_count": df.count()},
                            f"{bad} null values in {self._column}")


class ExpectColumnValuesToBeUnique(Expectation):
    def __init__(self, table: str, column: str):
        self._table, self._column = table, column

    @property
    def name(self) -> str:
        return f"ExpectColumnValuesToBeUnique({self._column})"

    def validate(self, ctx: ValidationContext) -> ExpectationResult:
        df = ctx.table(self._table)
        dupes = (
            df.groupBy(self._column).count().filter(F.col("count") > 1).count()
        )
        return self._result(self._table, dupes == 0,
                            {"duplicate_value_count": dupes},
                            f"{dupes} duplicate values in {self._column}")


class ExpectColumnValuesToBeInSet(Expectation):
    def __init__(self, table: str, column: str, allowed: set[str]):
        self._table, self._column, self._allowed = table, column, set(allowed)

    @property
    def name(self) -> str:
        return f"ExpectColumnValuesToBeInSet({self._column})"

    def validate(self, ctx: ValidationContext) -> ExpectationResult:
        df = ctx.table(self._table)
        bad = df.filter(~F.col(self._column).isin(*self._allowed)).count()
        return self._result(self._table, bad == 0,
                            {"unexpected_count": bad, "allowed": sorted(self._allowed)},
                            f"{bad} values in {self._column} outside {sorted(self._allowed)}")


class ExpectColumnValuesToBeBetween(Expectation):
    def __init__(self, table: str, column: str, min_value=None, max_value=None):
        self._table, self._column = table, column
        self._min, self._max = min_value, max_value

    @property
    def name(self) -> str:
        return f"ExpectColumnValuesToBeBetween({self._column})"

    def validate(self, ctx: ValidationContext) -> ExpectationResult:
        df = ctx.table(self._table)
        cond = F.lit(False)
        if self._min is not None:
            cond = cond | (F.col(self._column) < self._min)
        if self._max is not None:
            cond = cond | (F.col(self._column) > self._max)
        bad = df.filter(cond).count()
        return self._result(self._table, bad == 0,
                            {"unexpected_count": bad, "min": self._min, "max": self._max},
                            f"{bad} values in {self._column} outside [{self._min}, {self._max}]")
