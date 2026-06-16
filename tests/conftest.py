"""Shared pytest fixtures: a local Delta-enabled Spark session + helpers."""
from __future__ import annotations

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.common.tables import TableSpec

# A tiny synthetic table to exercise the merge logic in isolation. The merge
# is table-agnostic, so a 3-column "widget" keeps the tests focused.
WIDGET = TableSpec("widget", "id", ("id", "name", "amount"))

EVENT_SCHEMA = StructType(
    [
        StructField("op", StringType(), False),
        StructField("lsn", LongType(), False),
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("amount", DoubleType(), True),
    ]
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("shift-cdc-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def make_events(spark):
    """Build a flat CDC-event DataFrame from (op, lsn, id, name, amount) tuples."""

    def _make(rows: list[tuple]):
        return spark.createDataFrame(rows, schema=EVENT_SCHEMA)

    return _make
