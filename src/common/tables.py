"""Central registry of the migrated tables.

Primary keys + column lists live here so every phase (CDC merge, schema
drift, reconciliation, profiling) shares one source of truth instead of
re-declaring schemas. Mirrors infra/postgres/01_schema.sql.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TableSpec:
    name: str
    primary_key: str
    columns: tuple[str, ...]

    @property
    def topic(self) -> str:
        """Debezium topic for this table (topic.prefix = 'shift')."""
        return f"shift.public.{self.name}"

    @property
    def value_subject(self) -> str:
        """Schema Registry subject holding the Avro value (envelope) schema."""
        return f"{self.topic}-value"

    @property
    def business_columns(self) -> list[str]:
        return list(self.columns)


TABLES: dict[str, TableSpec] = {
    "customers": TableSpec(
        "customers", "customer_id",
        ("customer_id", "name", "email", "segment", "region", "created_at", "updated_at"),
    ),
    "products": TableSpec(
        "products", "product_id",
        ("product_id", "name", "category", "unit_cost", "is_active", "created_at"),
    ),
    "orders": TableSpec(
        "orders", "order_id",
        ("order_id", "customer_id", "product_id", "quantity", "unit_price",
         "status", "channel", "created_at", "updated_at"),
    ),
    "inventory": TableSpec(
        "inventory", "inventory_id",
        ("inventory_id", "product_id", "warehouse_id", "quantity", "reorder_point", "updated_at"),
    ),
    "shipments": TableSpec(
        "shipments", "shipment_id",
        ("shipment_id", "order_id", "carrier", "status", "shipped_at",
         "delivered_at", "estimated_delivery"),
    ),
}
