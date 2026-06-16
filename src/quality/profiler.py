"""Column profiler — pure statistics over the Silver layer (no LLM).

For every column computes: data type, row/null counts + null rate, cardinality
(approx distinct), min/max, mean (numeric), and a few sample values. Results
land in Postgres `catalog.column_profiles`, which feeds both the RAG data
catalog (Phase 9) and the dashboard's catalog view.
"""
from __future__ import annotations

import os

from pyspark.sql import functions as F

from src.common.tables import TABLES

_NUMERIC_PREFIXES = ("int", "bigint", "smallint", "double", "float", "decimal", "long")


def _is_numeric(dtype: str) -> bool:
    return dtype.startswith(_NUMERIC_PREFIXES)


def profile_table(spark, table_name: str, silver_root: str) -> list[dict]:
    df = spark.read.format("delta").load(f"{silver_root}/{table_name}")
    row_count = df.count()

    # One aggregation pass for the cheap stats across every column.
    agg_exprs = []
    for f in df.schema.fields:
        c = f.name
        agg_exprs.append(F.count(F.when(F.col(c).isNull(), 1)).alias(f"{c}__nulls"))
        agg_exprs.append(F.approx_count_distinct(c).alias(f"{c}__distinct"))
        agg_exprs.append(F.min(c).cast("string").alias(f"{c}__min"))
        agg_exprs.append(F.max(c).cast("string").alias(f"{c}__max"))
        if _is_numeric(f.dataType.simpleString()):
            agg_exprs.append(F.avg(c).cast("double").alias(f"{c}__mean"))
    stats = df.agg(*agg_exprs).collect()[0].asDict()

    profiles = []
    for f in df.schema.fields:
        c, dtype = f.name, f.dataType.simpleString()
        nulls = int(stats[f"{c}__nulls"])
        # Cheap sample: a handful of non-null values, deduped in Python.
        seen, samples = set(), []
        for r in df.select(c).where(F.col(c).isNotNull()).limit(50).collect():
            v = r[0]
            if v not in seen:
                seen.add(v)
                samples.append(str(v))
            if len(samples) >= 5:
                break
        profiles.append({
            "table_name": table_name,
            "column_name": c,
            "data_type": dtype,
            "row_count": row_count,
            "null_count": nulls,
            "null_rate": (nulls / row_count) if row_count else 0.0,
            "distinct_count": int(stats[f"{c}__distinct"]),
            "min_value": stats[f"{c}__min"],
            "max_value": stats[f"{c}__max"],
            "mean_value": stats.get(f"{c}__mean"),
            "sample_values": samples,
        })
    return profiles


_DDL = """
CREATE SCHEMA IF NOT EXISTS catalog;
CREATE TABLE IF NOT EXISTS catalog.column_profiles (
    table_name TEXT NOT NULL, column_name TEXT NOT NULL, data_type TEXT NOT NULL,
    row_count BIGINT NOT NULL, null_count BIGINT NOT NULL, null_rate DOUBLE PRECISION NOT NULL,
    distinct_count BIGINT NOT NULL, min_value TEXT, max_value TEXT, mean_value DOUBLE PRECISION,
    sample_values TEXT[], description TEXT, profiled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, column_name)
)
"""


def persist(dsn: str, profiles: list[dict]) -> None:
    import psycopg

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(_DDL)
            for p in profiles:
                cur.execute(
                    """INSERT INTO catalog.column_profiles
                       (table_name, column_name, data_type, row_count, null_count, null_rate,
                        distinct_count, min_value, max_value, mean_value, sample_values, profiled_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
                       ON CONFLICT (table_name, column_name) DO UPDATE SET
                         data_type=EXCLUDED.data_type, row_count=EXCLUDED.row_count,
                         null_count=EXCLUDED.null_count, null_rate=EXCLUDED.null_rate,
                         distinct_count=EXCLUDED.distinct_count, min_value=EXCLUDED.min_value,
                         max_value=EXCLUDED.max_value, mean_value=EXCLUDED.mean_value,
                         sample_values=EXCLUDED.sample_values, profiled_at=now()""",
                    (p["table_name"], p["column_name"], p["data_type"], p["row_count"],
                     p["null_count"], p["null_rate"], p["distinct_count"], p["min_value"],
                     p["max_value"], p["mean_value"], p["sample_values"]),
                )
        conn.commit()


def main() -> int:
    import argparse

    from src.ingestion.cdc_merge import _build_spark

    ap = argparse.ArgumentParser(description="Profile Silver columns into catalog.column_profiles.")
    ap.add_argument("tables", nargs="*", help="tables to profile (default: all)")
    ap.add_argument("--silver", default=os.environ.get("DELTA_SILVER_PATH", "/data/delta/silver"))
    args = ap.parse_args()

    spark = _build_spark("shift-profiler")
    spark.sparkContext.setLogLevel("ERROR")
    dsn = os.environ["POSTGRES_DSN"]

    for t in (args.tables or sorted(TABLES)):
        profiles = profile_table(spark, t, args.silver)
        persist(dsn, profiles)
        print(f"[profiler] {t}: {len(profiles)} columns profiled", flush=True)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
