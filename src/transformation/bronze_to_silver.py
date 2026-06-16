"""Bronze → Silver promotion: table-specific cleaning + Spark-native validation.

Each table gets cleaning rules (normalize/derive) and validation rules. A row
that fails any validation is *quarantined* — written to `quarantine/<table>`
with the list of violations it tripped — and the pipeline continues with the
clean rows. We do NOT halt here (that's reserved for breaking schema drift);
bad data is logged and set aside.

Cleaning (transform):
  customers : lowercase/trim email, normalize segment values
  orders    : derive total_amount = quantity * unit_price
  products  : coerce is_active to boolean
  shipments : lowercase/trim status
  inventory : (none)

Validation (quarantine on failure):
  generic   : null primary keys, duplicate primary keys, future timestamps,
              type consistency (required columns non-null)
  customers : email must look like an address
  orders    : quantity > 0, unit_price >= 0
  inventory : quantity >= 0, reorder_point >= 0
  products  : is_active not null
  shipments : status in legal set, status/timestamp consistency
"""
from __future__ import annotations

from dataclasses import dataclass, field
from functools import reduce
from typing import Callable

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.tables import TABLES

EMAIL_RE = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
LEGAL_SHIP_STATUS = ("label_created", "in_transit", "delivered", "returned")


# ── Per-table cleaners ─────────────────────────────────────────────────
def _clean_customers(df: DataFrame) -> DataFrame:
    seg = F.lower(F.trim(F.col("segment")))
    seg = (
        F.when(seg.isin("consumer", "b2c", "retail"), F.lit("consumer"))
        .when(seg.isin("smb", "sme", "small", "small business"), F.lit("smb"))
        .when(seg.isin("enterprise", "ent", "corp", "corporate"), F.lit("enterprise"))
        .otherwise(seg)
    )
    return df.withColumn("email", F.lower(F.trim(F.col("email")))).withColumn("segment", seg)


def _clean_orders(df: DataFrame) -> DataFrame:
    # Recompute the order total from line economics rather than trusting any
    # stored value — this is what the GE ExpectOrderAmountsToMatchLineItems
    # check validates downstream.
    return df.withColumn(
        "total_amount", F.round(F.col("quantity") * F.col("unit_price"), 2)
    )


def _clean_products(df: DataFrame) -> DataFrame:
    return df.withColumn("is_active", F.col("is_active").cast("boolean"))


def _clean_shipments(df: DataFrame) -> DataFrame:
    return df.withColumn("status", F.lower(F.trim(F.col("status"))))


def _identity(df: DataFrame) -> DataFrame:
    return df


# ── Silver specs ───────────────────────────────────────────────────────
@dataclass(frozen=True)
class SilverSpec:
    clean: Callable[[DataFrame], DataFrame]
    timestamps: tuple[str, ...]
    nullable: frozenset[str] = frozenset()
    # (rule_name, thunk returning a Column that is TRUE for a BAD row).
    # The conditions are thunks because building a Column needs an active
    # SparkContext, which does not exist at module-import time.
    rules: tuple[tuple[str, Callable[[], Column]], ...] = field(default_factory=tuple)


SILVER_SPECS: dict[str, SilverSpec] = {
    "customers": SilverSpec(
        clean=_clean_customers,
        timestamps=("created_at", "updated_at"),
        rules=(("invalid_email", lambda: ~F.col("email").rlike(EMAIL_RE)),),
    ),
    "products": SilverSpec(
        clean=_clean_products,
        timestamps=("created_at",),
        rules=(("null_is_active", lambda: F.col("is_active").isNull()),),
    ),
    "orders": SilverSpec(
        clean=_clean_orders,
        timestamps=("created_at", "updated_at"),
        rules=(
            ("non_positive_quantity", lambda: F.col("quantity") <= 0),
            ("negative_unit_price", lambda: F.col("unit_price") < 0),
        ),
    ),
    "inventory": SilverSpec(
        clean=_identity,
        timestamps=("updated_at",),
        rules=(
            ("negative_quantity", lambda: F.col("quantity") < 0),
            ("negative_reorder_point", lambda: F.col("reorder_point") < 0),
        ),
    ),
    "shipments": SilverSpec(
        clean=_clean_shipments,
        # estimated_delivery is intentionally a FUTURE date, so it's excluded
        # from the no-future-timestamps check (shipped_at/delivered_at are past
        # events and must not be in the future).
        timestamps=("shipped_at", "delivered_at"),
        nullable=frozenset({"shipped_at", "delivered_at", "estimated_delivery"}),
        rules=(
            ("illegal_status", lambda: ~F.col("status").isin(*LEGAL_SHIP_STATUS)),
            ("delivered_without_timestamp",
             lambda: (F.col("status") == "delivered") & F.col("delivered_at").isNull()),
            ("shipped_without_timestamp",
             lambda: F.col("status").isin("in_transit", "delivered") & F.col("shipped_at").isNull()),
            ("delivered_before_shipped",
             lambda: F.col("delivered_at").isNotNull() & F.col("shipped_at").isNotNull()
             & (F.col("delivered_at") < F.col("shipped_at"))),
        ),
    ),
}


@dataclass(frozen=True)
class SilverMetrics:
    table: str
    input_rows: int
    clean_rows: int
    quarantined_rows: int
    violations: dict[str, int]

    def as_dict(self) -> dict:
        return {**self.__dict__}


def _generic_rules(table_name: str) -> list[tuple[str, Column]]:
    spec = SILVER_SPECS[table_name]
    pk = TABLES[table_name].primary_key
    rules: list[tuple[str, Column]] = [("null_primary_keys", F.col(pk).isNull())]

    if spec.timestamps:
        future = reduce(
            lambda a, b: a | b,
            [F.coalesce(F.col(c) > F.current_timestamp(), F.lit(False)) for c in spec.timestamps],
        )
        rules.append(("future_timestamps", future))

    required = [c for c in TABLES[table_name].columns if c != pk and c not in spec.nullable]
    if required:
        any_null = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in required])
        rules.append(("type_consistency", any_null))

    return rules


def split_clean_and_quarantine(table_name: str, df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Apply cleaning, then partition rows into (clean, quarantined).

    Quarantined rows keep all original columns plus `_violations` (array of
    failed rule names) and `_quarantined_at`.
    """
    spec = SILVER_SPECS[table_name]
    pk = TABLES[table_name].primary_key
    cleaned = spec.clean(df)

    # Duplicate-PK detection needs a window; everything else is row-local.
    counts = cleaned.withColumn("_pk_count", F.count(F.lit(1)).over(Window.partitionBy(pk)))

    rule_exprs = _generic_rules(table_name) + [(name, fn()) for name, fn in spec.rules]
    flags = [F.when(cond, F.lit(name)) for name, cond in rule_exprs]
    flags.append(F.when(F.col("_pk_count") > 1, F.lit("duplicate_pks")))

    violations = F.filter(F.array(*flags), lambda x: x.isNotNull())
    tagged = counts.withColumn("_violations", violations).drop("_pk_count")

    clean_df = tagged.filter(F.size("_violations") == 0).drop("_violations")
    quarantine_df = tagged.filter(F.size("_violations") > 0).withColumn(
        "_quarantined_at", F.current_timestamp()
    )
    return clean_df, quarantine_df


def bronze_to_silver(
    spark: SparkSession,
    table_name: str,
    bronze_path: str,
    silver_path: str,
    quarantine_path: str,
) -> SilverMetrics:
    """Promote a Bronze Delta table to Silver, quarantining invalid rows."""
    df = spark.read.format("delta").load(bronze_path)
    input_rows = df.count()

    clean_df, quarantine_df = split_clean_and_quarantine(table_name, df)
    clean_df = clean_df.persist()
    quarantine_df = quarantine_df.persist()
    try:
        clean_rows = clean_df.count()
        quarantined_rows = quarantine_df.count()

        # Silver is a clean, idempotent rebuild each promotion.
        clean_df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(silver_path)

        breakdown: dict[str, int] = {}
        if quarantined_rows:
            quarantine_df.write.format("delta").mode("append").option(
                "mergeSchema", "true"
            ).save(quarantine_path)
            breakdown = {
                r["v"]: r["n"]
                for r in quarantine_df.select(F.explode("_violations").alias("v"))
                .groupBy("v").count().withColumnRenamed("count", "n").collect()
            }
    finally:
        clean_df.unpersist()
        quarantine_df.unpersist()

    return SilverMetrics(
        table=table_name,
        input_rows=input_rows,
        clean_rows=clean_rows,
        quarantined_rows=quarantined_rows,
        violations=breakdown,
    )


def _build_spark():
    from src.ingestion.cdc_merge import _build_spark as build
    return build("shift-bronze-to-silver")


def main() -> int:
    import argparse
    import os

    ap = argparse.ArgumentParser(description="Promote a Bronze Delta table to Silver.")
    ap.add_argument("table", choices=sorted(TABLES))
    ap.add_argument("--bronze", default=os.environ.get("DELTA_BRONZE_PATH", "/data/delta/bronze"))
    ap.add_argument("--silver", default=os.environ.get("DELTA_SILVER_PATH", "/data/delta/silver"))
    args = ap.parse_args()

    spark = _build_spark()
    spark.sparkContext.setLogLevel("WARN")
    metrics = bronze_to_silver(
        spark, args.table,
        bronze_path=f"{args.bronze}/{args.table}",
        silver_path=f"{args.silver}/{args.table}",
        quarantine_path=f"{args.bronze}/quarantine/{args.table}",
    )
    print(f"[bronze→silver {args.table}] {metrics.as_dict()}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
