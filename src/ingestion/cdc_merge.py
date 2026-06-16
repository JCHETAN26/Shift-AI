"""CDC merge: apply a batch of Debezium change events to a Delta table.

THE most important file in Shift.ai. The contract:

    Given a batch of CDC events (possibly duplicated, out-of-order, mixing
    inserts/updates/deletes for the same key), converge the target Delta table
    to the *latest* state per primary key — idempotently, with exactly-once
    semantics regardless of arrival order.

Design split so it is unit-testable without Kafka:

  apply_cdc_events()  — pure: (events DataFrame, Delta path, TableSpec) -> metrics
  stream_table()      — production wiring: Kafka readStream + Avro decode +
                         foreachBatch(apply_cdc_events). Not unit tested.

How correctness is achieved
---------------------------
1. NORMALIZE the Debezium envelope into a flat row image. For create/update/
   read(snapshot) the image is `after`; for delete it is `before` (which is a
   complete row because the source uses REPLICA IDENTITY FULL). This gives a
   primary key for deletes too.
2. DEDUPLICATE within the batch: a single key can appear many times (e.g.
   insert→update→delete). Keep only the highest-LSN event per key via a window
   function. LSN is Postgres's monotonic log sequence number, so "highest LSN"
   == "most recent", independent of Kafka ordering. This is what makes the
   merge idempotent and out-of-order-safe.
3. MERGE INTO the Delta table:
     WHEN MATCHED   AND op = 'd'  THEN DELETE
     WHEN MATCHED   AND op <> 'd' THEN UPDATE SET <business columns>
     WHEN NOT MATCHED AND op <> 'd' THEN INSERT <business columns>
   A delete of a row that isn't present is simply an unmatched no-op.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.tables import TABLES, TableSpec

# Debezium op codes that carry a forward row image (insert / update / snapshot).
UPSERT_OPS = ("c", "u", "r")
DELETE_OP = "d"


@dataclass(frozen=True)
class CDCMergeMetrics:
    rows_inserted: int
    rows_updated: int
    rows_deleted: int
    rows_skipped: int          # collapsed duplicates + no-op deletes
    input_events: int
    deduped_events: int

    def as_dict(self) -> dict[str, int]:
        return self.__dict__.copy()


class SchemaMismatchError(RuntimeError):
    """Raised when incoming event columns diverge from the Delta target.

    Phase 3 (schema_drift.py) classifies *why* (add/drop/widen/narrow) and
    decides whether to auto-evolve or halt; at the merge layer we fail loudly
    rather than silently corrupt the target.
    """

    def __init__(self, table: str, target_cols: set[str], source_cols: set[str]):
        self.table = table
        self.added = source_cols - target_cols
        self.dropped = target_cols - source_cols
        super().__init__(
            f"Schema mismatch for '{table}': "
            f"added={sorted(self.added)} dropped={sorted(self.dropped)}"
        )


# ──────────────────────────────────────────────────────────────────────
# Core (pure) logic
# ──────────────────────────────────────────────────────────────────────
def _normalize(events_df: DataFrame, business_cols: list[str]) -> DataFrame:
    """Flatten a batch into [<business cols>, op, lsn, ts_ms].

    Accepts either the Debezium envelope form (columns `before`/`after`
    structs + `op`/`lsn`) or an already-flat form (business cols + op + lsn),
    so tests can build flat rows and production can pass the envelope.
    """
    cols = set(events_df.columns)
    has_envelope = "after" in cols or "before" in cols

    if has_envelope:
        select_exprs = []
        for c in business_cols:
            after_c = F.col(f"after.{c}") if "after" in cols else F.lit(None)
            before_c = F.col(f"before.{c}") if "before" in cols else F.lit(None)
            # delete carries its image in `before`; everything else in `after`
            select_exprs.append(
                F.when(F.col("op") == F.lit(DELETE_OP), before_c).otherwise(after_c).alias(c)
            )
        ts = F.col("ts_ms") if "ts_ms" in cols else F.lit(0).cast("long")
        return events_df.select(*select_exprs, F.col("op"), F.col("lsn"), ts.alias("ts_ms"))

    out = events_df
    if "ts_ms" not in cols:
        out = out.withColumn("ts_ms", F.lit(0).cast("long"))
    return out.select(*business_cols, "op", "lsn", "ts_ms")


def _dedupe_latest(normalized_df: DataFrame, primary_key: str) -> DataFrame:
    """Keep the highest-LSN event per primary key within the batch."""
    window = Window.partitionBy(primary_key).orderBy(
        F.col("lsn").desc(), F.col("ts_ms").desc()
    )
    return (
        normalized_df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def _merge_metrics(delta_table: DeltaTable) -> tuple[int, int, int]:
    """Read inserted/updated/deleted counts from the latest MERGE history row."""
    metrics = delta_table.history(1).select("operationMetrics").collect()[0][0] or {}

    def g(key: str) -> int:
        return int(metrics.get(key, "0") or 0)

    return (
        g("numTargetRowsInserted"),
        g("numTargetRowsUpdated"),
        g("numTargetRowsDeleted"),
    )


def apply_cdc_events(
    spark: SparkSession,
    events_df: DataFrame,
    target_path: str,
    table: TableSpec,
    *,
    allow_schema_evolution: bool = False,
) -> CDCMergeMetrics:
    """Apply one batch of CDC events to the Delta table at ``target_path``.

    Handles empty batches, first-run table creation, dedup, and the upsert/
    delete MERGE. Returns row-level metrics.
    """
    pk = table.primary_key
    business_cols = table.business_columns

    input_events = events_df.count()
    if input_events == 0:
        return CDCMergeMetrics(0, 0, 0, 0, 0, 0)

    # Fail fast on drift before doing any work: if the target already exists and
    # its columns diverge from this table's spec, halt rather than corrupt it.
    target_exists = DeltaTable.isDeltaTable(spark, target_path)
    if target_exists:
        target_cols = set(DeltaTable.forPath(spark, target_path).toDF().columns)
        if set(business_cols) != target_cols and not allow_schema_evolution:
            raise SchemaMismatchError(table.name, target_cols, set(business_cols))

    normalized = _normalize(events_df, business_cols).filter(F.col(pk).isNotNull())
    deduped = _dedupe_latest(normalized, pk).persist()
    try:
        deduped_events = deduped.count()

        # First run: materialize an empty Delta table with the business schema
        # so the MERGE below has a target. Deletes against it become no-ops.
        if not target_exists:
            deduped.select(*business_cols).limit(0).write.format("delta").save(target_path)

        delta_table = DeltaTable.forPath(spark, target_path)
        update_map = {c: f"s.`{c}`" for c in business_cols if c != pk}
        insert_map = {c: f"s.`{c}`" for c in business_cols}

        (
            delta_table.alias("t")
            .merge(deduped.alias("s"), f"t.`{pk}` = s.`{pk}`")
            .whenMatchedDelete(condition=f"s.op = '{DELETE_OP}'")
            .whenMatchedUpdate(condition=f"s.op <> '{DELETE_OP}'", set=update_map)
            .whenNotMatchedInsert(condition=f"s.op <> '{DELETE_OP}'", values=insert_map)
            .execute()
        )

        inserted, updated, deleted = _merge_metrics(delta_table)
    finally:
        deduped.unpersist()

    skipped = input_events - (inserted + updated + deleted)
    return CDCMergeMetrics(
        rows_inserted=inserted,
        rows_updated=updated,
        rows_deleted=deleted,
        rows_skipped=max(skipped, 0),
        input_events=input_events,
        deduped_events=deduped_events,
    )


# ──────────────────────────────────────────────────────────────────────
# Production wiring: Kafka readStream + Confluent-Avro decode
# ──────────────────────────────────────────────────────────────────────
def _fetch_value_schema(registry_url: str, subject: str) -> str:
    """Pull the latest Avro writer schema JSON from Schema Registry.

    We decode the Confluent wire format ourselves (depth rule) rather than
    leaning on a magic connector: strip the 5-byte prefix (magic + schema id)
    and hand the Avro bytes to Spark's from_avro with this schema.
    """
    import requests

    resp = requests.get(f"{registry_url}/subjects/{subject}/versions/latest", timeout=10)
    resp.raise_for_status()
    return resp.json()["schema"]


def stream_table(
    spark: SparkSession,
    table: TableSpec,
    *,
    target_path: str,
    checkpoint_path: str,
    bootstrap_servers: str | None = None,
    registry_url: str | None = None,
    starting_offsets: str = "earliest",
    available_now: bool = False,
):
    """Start a streaming query that merges this table's CDC topic into Delta.

    ``available_now=True`` processes everything currently in the topic and then
    stops — used for bounded batch runs (and the Airflow ingest DAG); otherwise
    it runs continuously on a 10s micro-batch trigger.
    """
    from pyspark.sql.avro.functions import from_avro

    bootstrap_servers = bootstrap_servers or os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    registry_url = registry_url or os.environ["SCHEMA_REGISTRY_URL"]
    value_schema = _fetch_value_schema(registry_url, table.value_subject)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", table.topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )

    # Confluent wire format: byte 0 = magic, bytes 1-4 = schema id, then Avro.
    # substring is 1-indexed, so start at byte 6.
    avro_bytes = F.expr("substring(value, 6, length(value) - 5)")
    envelope = from_avro(avro_bytes, value_schema, {"mode": "PERMISSIVE"}).alias("e")

    events = raw.select(envelope).select(
        F.col("e.op").alias("op"),
        F.col("e.source.lsn").alias("lsn"),
        F.col("e.source.ts_ms").alias("ts_ms"),
        F.col("e.before").alias("before"),
        F.col("e.after").alias("after"),
    ).filter(F.col("op").isNotNull())  # drop tombstones (null value)

    def _batch(batch_df: DataFrame, batch_id: int) -> None:
        metrics = apply_cdc_events(spark, batch_df, target_path, table)
        print(f"[{table.name} batch {batch_id}] {metrics.as_dict()}", flush=True)

    writer = events.writeStream.foreachBatch(_batch).option(
        "checkpointLocation", checkpoint_path
    )
    writer = writer.trigger(availableNow=True) if available_now else writer.trigger(
        processingTime="10 seconds"
    )
    return writer.start()


def _build_spark(app_name: str = "shift-cdc-merge") -> SparkSession:
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    extra = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        "org.apache.spark:spark-avro_2.12:3.5.3",
    ]
    return configure_spark_with_delta_pip(builder, extra_packages=extra).getOrCreate()


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Stream a table's CDC topic into Delta Bronze.")
    ap.add_argument("table", choices=sorted(TABLES), help="table to stream")
    ap.add_argument("--bronze", default=os.environ.get("DELTA_BRONZE_PATH", "/data/delta/bronze"))
    ap.add_argument("--once", action="store_true",
                    help="process all currently-available events, then stop (batch mode)")
    args = ap.parse_args()

    table = TABLES[args.table]
    spark = _build_spark()
    spark.sparkContext.setLogLevel("WARN")
    target = f"{args.bronze}/{table.name}"
    checkpoint = f"{args.bronze}/_checkpoints/{table.name}"
    query = stream_table(
        spark, table, target_path=target, checkpoint_path=checkpoint,
        available_now=args.once,
    )
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
