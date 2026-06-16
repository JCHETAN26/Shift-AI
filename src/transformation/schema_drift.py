"""Schema drift detection: classify how the source schema has diverged from
the Delta target, and decide whether to auto-evolve or halt the migration.

The classifier is PURE — it compares two ``{column: type_string}`` maps and
needs neither Postgres nor Spark, so the classification rules are unit-tested
in milliseconds. Thin adapters (`postgres_schema`, `delta_schema`) read the
real schemas and feed the classifier via `detect_drift`.

Classification (source = current Postgres, target = existing Delta table):
  column only in source              → ADDED         NON_BREAKING (auto-evolve)
  column only in target              → DROPPED       BREAKING     (halt)
  same family, source type wider     → TYPE_WIDENED  NON_BREAKING (e.g. INT→BIGINT)
  same family, source type narrower  → TYPE_NARROWED BREAKING     (e.g. BIGINT→INT)
  different family                   → TYPE_CHANGED  BREAKING
  a drop + add with same type & a
    similar name                     → RENAMED       AMBIGUOUS    (human review)
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from enum import Enum

# ── Canonical type model ───────────────────────────────────────────────
# Integer widths and float widths give a comparable "rank" within a family,
# which is how we tell widening (safe) from narrowing (breaking) apart.
_INT_RANKS = {
    "smallint": 16, "int2": 16, "short": 16,
    "integer": 32, "int": 32, "int4": 32, "serial": 32,
    "bigint": 64, "int8": 64, "long": 64, "bigserial": 64,
}
_FLOAT_RANKS = {"real": 32, "float4": 32, "float": 32, "double precision": 64, "float8": 64, "double": 64}
_TIMESTAMP = {"timestamp", "timestamp without time zone", "timestamp with time zone", "timestamptz"}
_STRING_BOUNDED = {"character varying", "varchar", "character", "char", "bpchar"}
_STRING_UNBOUNDED = {"text", "string", "citext", "name"}
_UNBOUNDED_LEN = 10 ** 9  # treat text/unbounded varchar as "very long"


@dataclass(frozen=True)
class CType:
    """Canonical type: a family plus a comparable size rank (and decimal scale)."""
    family: str
    rank: int = 0
    scale: int = 0
    raw: str = ""


def parse_type(raw: str) -> CType:
    """Map a Postgres or Spark type string onto a canonical type."""
    raw = (raw or "").strip().lower()
    m = re.match(r"^([a-z0-9_ ]+?)\s*(?:\((\d+)(?:\s*,\s*(\d+))?\))?\s*$", raw)
    name = (m.group(1).strip() if m else raw)
    p1 = int(m.group(2)) if (m and m.group(2)) else None
    p2 = int(m.group(3)) if (m and m.group(3)) else None

    if name in _INT_RANKS:
        return CType("INTEGER", _INT_RANKS[name], raw=raw)
    if name in _FLOAT_RANKS:
        return CType("FLOAT", _FLOAT_RANKS[name], raw=raw)
    if name in ("numeric", "decimal"):
        return CType("DECIMAL", p1 if p1 is not None else 38, p2 or 0, raw=raw)
    if name in ("boolean", "bool"):
        return CType("BOOLEAN", raw=raw)
    if name in _STRING_UNBOUNDED:
        return CType("STRING", _UNBOUNDED_LEN, raw=raw)
    if name in _STRING_BOUNDED:
        return CType("STRING", p1 if p1 is not None else _UNBOUNDED_LEN, raw=raw)
    if name in _TIMESTAMP:
        return CType("TIMESTAMP", raw=raw)
    if name == "date":
        return CType("DATE", raw=raw)
    if name in ("bytea", "binary"):
        return CType("BINARY", raw=raw)
    if name in ("json", "jsonb"):
        return CType("JSON", raw=raw)
    if name == "uuid":
        return CType("UUID", raw=raw)
    return CType("OTHER", raw=raw)


class TypeRelation(str, Enum):
    SAME = "SAME"
    WIDENED = "WIDENED"
    NARROWED = "NARROWED"
    CHANGED = "CHANGED"


# Families whose rank is meaningfully ordered (widening vs narrowing applies).
_RANKED_FAMILIES = {"INTEGER", "FLOAT", "STRING"}


def compare_types(source: CType, target: CType) -> TypeRelation:
    """Relation of the *source* (new) type to the *target* (existing) type."""
    if source.family != target.family:
        return TypeRelation.CHANGED
    if source.family in _RANKED_FAMILIES:
        if source.rank == target.rank:
            return TypeRelation.SAME
        return TypeRelation.WIDENED if source.rank > target.rank else TypeRelation.NARROWED
    if source.family == "DECIMAL":
        if (source.rank, source.scale) == (target.rank, target.scale):
            return TypeRelation.SAME
        if source.rank >= target.rank and source.scale >= target.scale:
            return TypeRelation.WIDENED
        return TypeRelation.NARROWED
    return TypeRelation.SAME  # unranked families: same family == same type


# ── Drift report model ─────────────────────────────────────────────────
class ChangeType(str, Enum):
    ADDED = "ADDED"
    DROPPED = "DROPPED"
    TYPE_WIDENED = "TYPE_WIDENED"
    TYPE_NARROWED = "TYPE_NARROWED"
    TYPE_CHANGED = "TYPE_CHANGED"
    RENAMED = "RENAMED"


class Severity(str, Enum):
    NON_BREAKING = "NON_BREAKING"
    BREAKING = "BREAKING"
    AMBIGUOUS = "AMBIGUOUS"


_SEVERITY = {
    ChangeType.ADDED: Severity.NON_BREAKING,
    ChangeType.TYPE_WIDENED: Severity.NON_BREAKING,
    ChangeType.DROPPED: Severity.BREAKING,
    ChangeType.TYPE_NARROWED: Severity.BREAKING,
    ChangeType.TYPE_CHANGED: Severity.BREAKING,
    ChangeType.RENAMED: Severity.AMBIGUOUS,
}


@dataclass(frozen=True)
class SchemaChange:
    column: str
    change_type: ChangeType
    severity: Severity
    detail: str
    from_type: str | None = None
    to_type: str | None = None
    renamed_from: str | None = None


@dataclass
class SchemaDriftReport:
    table: str
    changes: list[SchemaChange] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not self.changes

    @property
    def has_breaking(self) -> bool:
        return any(c.severity == Severity.BREAKING for c in self.changes)

    @property
    def has_ambiguous(self) -> bool:
        return any(c.severity == Severity.AMBIGUOUS for c in self.changes)

    @property
    def recommended_action(self) -> str:
        """halt | review | auto_evolve | none."""
        if self.has_breaking:
            return "halt"
        if self.has_ambiguous:
            return "review"
        if self.changes:
            return "auto_evolve"
        return "none"

    def breaking_changes(self) -> list[SchemaChange]:
        return [c for c in self.changes if c.severity == Severity.BREAKING]


def _name_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def classify_drift(
    source: dict[str, str],
    target: dict[str, str],
    table: str,
    *,
    rename_threshold: float = 0.6,
) -> SchemaDriftReport:
    """Classify how ``source`` (current Postgres) diverged from ``target`` (Delta)."""
    changes: list[SchemaChange] = []

    added = [c for c in source if c not in target]
    dropped = [c for c in target if c not in source]
    common = [c for c in source if c in target]

    # Type changes on columns present in both.
    for col in common:
        s, t = parse_type(source[col]), parse_type(target[col])
        rel = compare_types(s, t)
        if rel == TypeRelation.SAME:
            continue
        ct = {
            TypeRelation.WIDENED: ChangeType.TYPE_WIDENED,
            TypeRelation.NARROWED: ChangeType.TYPE_NARROWED,
            TypeRelation.CHANGED: ChangeType.TYPE_CHANGED,
        }[rel]
        changes.append(SchemaChange(
            column=col, change_type=ct, severity=_SEVERITY[ct],
            detail=f"{target[col]} → {source[col]} ({rel.value.lower()})",
            from_type=target[col], to_type=source[col],
        ))

    # Rename detection: pair a dropped column with an added column when their
    # canonical types match and names are similar enough. Heuristic on purpose —
    # flagged AMBIGUOUS for human review, never auto-applied. A safe threshold
    # avoids masking a genuine breaking drop as a rename.
    paired_added: set[str] = set()
    for d in list(dropped):
        best, best_sim = None, rename_threshold
        for a in added:
            if a in paired_added:
                continue
            if compare_types(parse_type(source[a]), parse_type(target[d])) != TypeRelation.SAME:
                continue
            sim = _name_similarity(d, a)
            if sim >= best_sim:
                best, best_sim = a, sim
        if best is not None:
            paired_added.add(best)
            dropped.remove(d)
            changes.append(SchemaChange(
                column=best, change_type=ChangeType.RENAMED, severity=Severity.AMBIGUOUS,
                detail=f"'{d}' may have been renamed to '{best}' "
                       f"(same type {source[best]}, name similarity {best_sim:.2f})",
                from_type=target[d], to_type=source[best], renamed_from=d,
            ))

    for col in added:
        if col in paired_added:
            continue
        changes.append(SchemaChange(
            column=col, change_type=ChangeType.ADDED, severity=Severity.NON_BREAKING,
            detail=f"new column {col} {source[col]}", to_type=source[col],
        ))
    for col in dropped:
        changes.append(SchemaChange(
            column=col, change_type=ChangeType.DROPPED, severity=Severity.BREAKING,
            detail=f"column {col} ({target[col]}) dropped from source", from_type=target[col],
        ))

    changes.sort(key=lambda c: (c.severity != Severity.BREAKING, c.column))
    return SchemaDriftReport(table=table, changes=changes)


# ── Adapters (lazy deps so the pure classifier imports with stdlib only) ─
def _pg_raw_type(data_type: str, char_len, num_prec, num_scale) -> str:
    d = data_type.lower()
    if d in ("numeric", "decimal") and num_prec is not None:
        return f"decimal({num_prec},{num_scale or 0})"
    if d in ("character varying", "character") and char_len is not None:
        return f"varchar({char_len})"
    return d


def postgres_schema(conn, table: str, schema: str = "public") -> dict[str, str]:
    """Read ``{column: type_string}`` from information_schema for a table."""
    query = """
        SELECT column_name, data_type, character_maximum_length,
               numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    out: dict[str, str] = {}
    with conn.cursor() as cur:
        cur.execute(query, (schema, table))
        for name, dtype, char_len, num_prec, num_scale in cur.fetchall():
            out[name] = _pg_raw_type(dtype, char_len, num_prec, num_scale)
    return out


def delta_schema(spark, path: str) -> dict[str, str]:
    """Read ``{column: type_string}`` from a Delta table's schema.

    NOTE: comparing these types *directly* to Postgres types yields false
    positives, because Debezium re-encodes some types on the wire (e.g.
    ``timestamptz`` → ISO string, so Bronze stores it as ``string``). Drift is
    therefore tracked against a stored *source-schema baseline* (below) rather
    than against the serialized Delta types.
    """
    df = spark.read.format("delta").load(path)
    return {f.name: f.dataType.simpleString() for f in df.schema.fields}


# ── Source-schema baseline (the schema each Delta table was built from) ──
import json
import os


def _baseline_file(baseline_dir: str, table: str) -> str:
    return os.path.join(baseline_dir, f"{table}.json")


def load_baseline(baseline_dir: str, table: str) -> dict[str, str] | None:
    path = _baseline_file(baseline_dir, table)
    if not os.path.exists(path):
        return None
    with open(path) as fh:
        return json.load(fh)


def save_baseline(baseline_dir: str, table: str, schema: dict[str, str]) -> None:
    os.makedirs(baseline_dir, exist_ok=True)
    with open(_baseline_file(baseline_dir, table), "w") as fh:
        json.dump(schema, fh, indent=2, sort_keys=True)


def detect_drift(conn, table, baseline_dir: str) -> SchemaDriftReport:
    """Compare the live Postgres schema against the stored source baseline.

    First run for a table establishes the baseline and reports no drift; later
    runs classify how the source has diverged from what Bronze was built on.
    """
    current = postgres_schema(conn, table.name)
    baseline = load_baseline(baseline_dir, table.name)
    if baseline is None:
        save_baseline(baseline_dir, table.name, current)
        return SchemaDriftReport(table=table.name, changes=[])
    return classify_drift(current, baseline, table.name)
