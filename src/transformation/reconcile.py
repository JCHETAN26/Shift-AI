"""Reconciliation framework: validate the migration source↔target.

Three levels (the build plan's design):
  1. Row count parity (Postgres source vs Snowflake target)
  2. Checksum — order-independent SUM over a per-row MD5 of canonical column
     values, computed independently on each engine and compared. Built from
     representation-stable columns (ints + strings) so the same string hashes
     identically on both engines (verified: PG `('x'||substr(md5,1,15))::bit(60)::bigint`
     == Snowflake `to_number(substr(md5,1,15), repeat('X',15))`).
  3. Sample comparison — 1,000 random PKs, field-by-field diff with type-aware
     tolerance for decimals/timestamps.

Results are written to Snowflake `RAW.MIGRATION_METADATA` (enriches the
gold_migration_summary mart) and to Postgres `reconciliation_results` (dashboard).
"""
from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal

from src.common.tables import TABLES

# Representation-stable columns (integers + plain strings) for the checksum.
CHECKSUM_COLUMNS = {
    "customers": ["customer_id", "name", "email", "segment", "region"],
    "products": ["product_id", "name", "category"],
    "orders": ["order_id", "customer_id", "product_id", "quantity", "status", "channel"],
    "inventory": ["inventory_id", "product_id", "warehouse_id", "quantity", "reorder_point"],
    "shipments": ["shipment_id", "order_id", "carrier", "status"],
}

# Columns compared with numeric / timestamp tolerance in the sample diff.
DECIMAL_COLUMNS = {"unit_price", "unit_cost", "total_amount"}
TIMESTAMP_COLUMNS = {"created_at", "updated_at", "shipped_at", "delivered_at", "estimated_delivery"}

SAMPLE_SIZE = 1000
COUNT_DELTA_THRESHOLD = 0.01   # checksum only runs if |Δ|/source < 1%


# ── Pure comparison helpers (unit-tested without DBs) ──────────────────
def _to_epoch(value) -> float | None:
    """Epoch seconds from a datetime OR an ISO-8601 string.

    Debezium encodes timestamps as ISO strings, so they land in Snowflake as
    VARCHAR (e.g. '2025-11-11T00:09:23.426196Z') while the Postgres source
    returns native datetimes — both must reduce to the same instant.
    """
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    return None


def _num(value):
    try:
        return Decimal(str(value))
    except Exception:
        return None


def compare_row(
    columns: list[str],
    source: dict,
    target: dict,
    *,
    ts_tolerance: float = 1.0,
    dec_tolerance: float = 0.01,
) -> dict[str, tuple]:
    """Return {column: (source_value, target_value)} for every field that differs.

    Stable columns compare exactly; decimals within ``dec_tolerance``;
    timestamps within ``ts_tolerance`` seconds (absorbs ms-precision / tz).
    """
    diffs: dict[str, tuple] = {}
    for col in columns:
        sv, tv = source.get(col), target.get(col)
        if col in TIMESTAMP_COLUMNS:
            se, te = _to_epoch(sv), _to_epoch(tv)
            if (se is None) != (te is None) or (
                se is not None and abs(se - te) > ts_tolerance
            ):
                diffs[col] = (sv, tv)
        elif col in DECIMAL_COLUMNS:
            sn, tn = _num(sv), _num(tv)
            if (sn is None) != (tn is None) or (
                sn is not None and abs(sn - tn) > Decimal(str(dec_tolerance))
            ):
                diffs[col] = (sv, tv)
        else:
            if _norm(sv) != _norm(tv):
                diffs[col] = (sv, tv)
    return diffs


def _norm(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    return str(value)


@dataclass
class ReconciliationReport:
    table: str
    source_count: int
    target_count: int
    count_match: bool
    checksum_match: bool | None
    source_checksum: int | None
    target_checksum: int | None
    sample_size: int
    sample_mismatches: int
    discrepancies: list[dict] = field(default_factory=list)
    reconciled_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def status(self) -> str:
        if not self.count_match:
            return "COUNT_MISMATCH"
        if self.checksum_match is False:
            return "CHECKSUM_MISMATCH"
        if self.sample_mismatches > 0:
            return "SAMPLE_MISMATCH"
        return "RECONCILED"


# ── SQL builders ───────────────────────────────────────────────────────
def _checksum_sql(table: str, cols: list[str], *, engine: str) -> str:
    if engine == "postgres":
        parts = ", ".join(f"{c}::text" for c in cols)
        row_hash = f"('x' || substr(md5(concat_ws('|', {parts})), 1, 15))::bit(60)::bigint"
        return f"SELECT COALESCE(SUM({row_hash}), 0) FROM public.{table}"
    # snowflake
    parts = ", ".join(f"to_varchar({c})" for c in cols)
    row_hash = f"to_number(substr(md5(concat_ws('|', {parts})), 1, 15), repeat('X', 15))"
    return f"SELECT COALESCE(SUM({row_hash}), 0) FROM RAW.{table.upper()}"


class ReconciliationEngine:
    def __init__(self, pg_conn, sf_conn):
        self.pg = pg_conn
        self.sf = sf_conn

    def _pg_scalar(self, sql, params=None):
        with self.pg.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.fetchone()[0]

    def _sf_scalar(self, sql):
        cur = self.sf.cursor()
        cur.execute(sql)
        return cur.fetchone()[0]

    def reconcile_table(self, table_name: str) -> ReconciliationReport:
        spec = TABLES[table_name]
        pk = spec.primary_key

        source_count = self._pg_scalar(f"SELECT count(*) FROM public.{table_name}")
        target_count = self._sf_scalar(f"SELECT count(*) FROM RAW.{table_name.upper()}")
        count_match = source_count == target_count

        # Level 2: checksum, only if counts are close enough to be worth it.
        source_ck = target_ck = None
        checksum_match = None
        delta_ratio = abs(source_count - target_count) / source_count if source_count else 1
        if delta_ratio < COUNT_DELTA_THRESHOLD:
            cols = CHECKSUM_COLUMNS[table_name]
            source_ck = int(self._pg_scalar(_checksum_sql(table_name, cols, engine="postgres")))
            target_ck = int(self._sf_scalar(_checksum_sql(table_name, cols, engine="snowflake")))
            checksum_match = source_ck == target_ck

        # Level 3: sample comparison.
        sample_mismatches, discrepancies = self._sample_compare(table_name, pk, spec.columns)

        return ReconciliationReport(
            table=table_name,
            source_count=source_count, target_count=target_count, count_match=count_match,
            checksum_match=checksum_match, source_checksum=source_ck, target_checksum=target_ck,
            sample_size=SAMPLE_SIZE, sample_mismatches=sample_mismatches, discrepancies=discrepancies,
        )

    def _sample_compare(self, table, pk, columns):
        pks = [r[0] for r in self._pg_rows(
            f"SELECT {pk} FROM public.{table} ORDER BY random() LIMIT {SAMPLE_SIZE}")]
        if not pks:
            return 0, []
        in_list = ", ".join(str(int(p)) for p in pks)
        cols = ", ".join(columns)
        src = {r[pk]: r for r in self._pg_dictrows(
            f"SELECT {cols} FROM public.{table} WHERE {pk} IN ({in_list})")}
        tgt = self._sf_dictrows(
            f"SELECT {cols} FROM RAW.{table.upper()} WHERE {pk} IN ({in_list})", pk)

        mismatches, discrepancies = 0, []
        for key, s_row in src.items():
            t_row = tgt.get(key)
            if t_row is None:
                mismatches += 1
                discrepancies.append({"pk": key, "issue": "missing_in_target"})
                continue
            diffs = compare_row(list(columns), s_row, t_row)
            if diffs:
                mismatches += 1
                if len(discrepancies) < 20:  # cap stored examples
                    discrepancies.append({
                        "pk": key,
                        "fields": {k: [str(a), str(b)] for k, (a, b) in diffs.items()},
                    })
        return mismatches, discrepancies

    # -- row fetch helpers --
    def _pg_rows(self, sql):
        with self.pg.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

    def _pg_dictrows(self, sql):
        with self.pg.cursor() as cur:
            cur.execute(sql)
            names = [d[0] for d in cur.description]
            return [dict(zip(names, row)) for row in cur.fetchall()]

    def _sf_dictrows(self, sql, pk):
        cur = self.sf.cursor()
        cur.execute(sql)
        names = [d[0].lower() for d in cur.description]  # Snowflake returns UPPERCASE
        return {(d := dict(zip(names, row)))[pk]: d for row in cur.fetchall()}


# ── Connections (container-internal hostnames) ─────────────────────────
def _pg_connect():
    import psycopg

    return psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "shift"),
        user=os.environ.get("POSTGRES_USER", "shift"),
        password=os.environ.get("POSTGRES_PASSWORD", "shift_dev_password"),
    )


def _sf_connect():
    import snowflake.connector

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "SHIFT_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "SHIFT"),
    )


_SF_DDL = """
CREATE TABLE IF NOT EXISTS RAW.MIGRATION_METADATA (
    table_name STRING, source_rows NUMBER, target_rows NUMBER,
    count_match BOOLEAN, checksum_match BOOLEAN, sample_size NUMBER,
    sample_mismatches NUMBER, reconciliation_status STRING,
    schema_version STRING, reconciled_at TIMESTAMP_NTZ
)
"""

_PG_DDL = """
CREATE TABLE IF NOT EXISTS reconciliation_results (
    id BIGSERIAL PRIMARY KEY, run_id TEXT, table_name TEXT,
    source_rows BIGINT, target_rows BIGINT, count_match BOOLEAN,
    checksum_match BOOLEAN, sample_size INT, sample_mismatches INT,
    status TEXT, discrepancies JSONB, reconciled_at TIMESTAMPTZ DEFAULT now()
)
"""


def persist_snowflake(sf, reports: list[ReconciliationReport]) -> None:
    cur = sf.cursor()
    cur.execute(_SF_DDL)
    for r in reports:
        cur.execute("DELETE FROM RAW.MIGRATION_METADATA WHERE table_name = %s", (r.table,))
        cur.execute(
            """INSERT INTO RAW.MIGRATION_METADATA
               (table_name, source_rows, target_rows, count_match, checksum_match,
                sample_size, sample_mismatches, reconciliation_status, schema_version, reconciled_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, current_timestamp())""",
            (r.table, r.source_count, r.target_count, r.count_match, r.checksum_match,
             r.sample_size, r.sample_mismatches, r.status, "v1"),
        )


def persist_postgres(pg, run_id: str, reports: list[ReconciliationReport]) -> None:
    with pg.cursor() as cur:
        cur.execute(_PG_DDL)
        for r in reports:
            cur.execute(
                """INSERT INTO reconciliation_results
                   (run_id, table_name, source_rows, target_rows, count_match,
                    checksum_match, sample_size, sample_mismatches, status, discrepancies)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (run_id, r.table, r.source_count, r.target_count, r.count_match,
                 r.checksum_match, r.sample_size, r.sample_mismatches, r.status,
                 json.dumps(r.discrepancies, default=str)),
            )
    pg.commit()


def _print(r: ReconciliationReport) -> None:
    ck = {True: "match", False: "MISMATCH", None: "skipped"}[r.checksum_match]
    print(f"  {r.table:11} [{r.status:16}] source={r.source_count:>7} target={r.target_count:>7} "
          f"checksum={ck} sample_mismatches={r.sample_mismatches}/{r.sample_size}")


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Reconcile source Postgres vs target Snowflake.")
    ap.add_argument("tables", nargs="*", help="tables to reconcile (default: all)")
    ap.add_argument("--no-persist", action="store_true")
    args = ap.parse_args()

    tables = args.tables or sorted(TABLES)
    run_id = uuid.uuid4().hex[:12]
    pg, sf = _pg_connect(), _sf_connect()
    try:
        engine = ReconciliationEngine(pg, sf)
        reports = [engine.reconcile_table(t) for t in tables]
        print(f"\n=== Reconciliation run {run_id} ===")
        for r in reports:
            _print(r)
        if not args.no_persist:
            persist_snowflake(sf, reports)
            persist_postgres(pg, run_id, reports)
            print("\nPersisted to Snowflake RAW.MIGRATION_METADATA and Postgres reconciliation_results.")
        all_ok = all(r.status == "RECONCILED" for r in reports)
        print(f"Overall: {'ALL RECONCILED' if all_ok else 'DISCREPANCIES FOUND'}")
        return 0
    finally:
        pg.close()
        sf.close()


if __name__ == "__main__":
    raise SystemExit(main())
