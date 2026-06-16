"""Load the Silver (Delta) layer into Snowflake RAW so dbt can build Gold.

dbt transforms tables that already live in Snowflake; it does not move data
from Delta. This job bridges that gap: read each Silver Delta table with Spark
and write it to ``SNOWFLAKE_DATABASE.RAW.<table>`` via the Spark-Snowflake
connector (overwrite each run — Silver is the source of truth).
"""
from __future__ import annotations

import os
import sys

from src.common.tables import TABLES

# The 3.x connector line is unified (no -spark_X.X suffix) and supports Spark
# 3.4/3.5; it pulls its matching snowflake-jdbc transitively.
SF_PACKAGES = ["net.snowflake:spark-snowflake_2.12:3.1.9"]


def _pem_private_key_body(path: str) -> str:
    """The Spark-Snowflake connector wants the PKCS8 key body (no PEM header/
    footer, no newlines)."""
    with open(path) as fh:
        lines = [ln for ln in fh.read().splitlines() if "PRIVATE KEY" not in ln]
    return "".join(lines)


def _snowflake_options() -> dict[str, str]:
    for k in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER"):
        if not os.environ.get(k):
            raise SystemExit(f"Missing Snowflake env var: {k} (set it in .env)")
    options = {
        "sfURL": f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
        "sfUser": os.environ["SNOWFLAKE_USER"],
        "sfDatabase": os.environ.get("SNOWFLAKE_DATABASE", "SHIFT"),
        "sfSchema": "RAW",
        "sfWarehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "SHIFT_WH"),
        "sfRole": os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    }
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
    if key_path:
        options["pem_private_key"] = _pem_private_key_body(key_path)
    elif os.environ.get("SNOWFLAKE_PASSWORD"):
        options["sfPassword"] = os.environ["SNOWFLAKE_PASSWORD"]
    else:
        raise SystemExit("Set SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD in .env")
    return options


def _build_spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("shift-load-snowflake")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder, extra_packages=SF_PACKAGES).getOrCreate()


def load_table(spark, table_name: str, silver_root: str, options: dict) -> int:
    df = spark.read.format("delta").load(f"{silver_root}/{table_name}")
    n = df.count()
    (
        df.write.format("net.snowflake.spark.snowflake")
        .options(**options)
        .option("dbtable", f"RAW.{table_name.upper()}")
        .mode("overwrite")
        .save()
    )
    return n


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Load Silver Delta tables into Snowflake RAW.")
    ap.add_argument("tables", nargs="*", default=None,
                    help="tables to load (default: all)")
    ap.add_argument("--silver", default=os.environ.get("DELTA_SILVER_PATH", "/data/delta/silver"))
    args = ap.parse_args()

    options = _snowflake_options()
    spark = _build_spark()
    spark.sparkContext.setLogLevel("WARN")

    tables = args.tables or sorted(TABLES)
    for t in tables:
        n = load_table(spark, t, args.silver, options)
        print(f"[load-snowflake] RAW.{t.upper()} <- {n:,} rows", flush=True)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
