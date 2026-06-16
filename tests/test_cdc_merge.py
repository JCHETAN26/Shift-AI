"""Unit tests for the CDC merge — the cases the build plan calls out:

  - insert followed by update of the same row (within a batch)
  - delete of a non-existent row (no-op)
  - out-of-order events (older LSN arriving after newer)
  - duplicate events (idempotency)
  - schema mismatch between event and Delta table
  - create→update→delete across separate batches (full lifecycle)
"""
from __future__ import annotations

import pytest

from src.common.tables import TableSpec
from src.ingestion.cdc_merge import SchemaMismatchError, apply_cdc_events
from tests.conftest import WIDGET


def _rows(spark, path):
    """Return target Delta rows keyed by id, as plain dicts."""
    df = spark.read.format("delta").load(path)
    return {r["id"]: r.asDict() for r in df.collect()}


def test_insert_then_update_same_row(spark, make_events, tmp_path):
    path = str(tmp_path / "widget")
    events = make_events([
        ("c", 1, 1, "alpha", 10.0),
        ("u", 2, 1, "alpha-v2", 20.0),   # same key, higher LSN
    ])

    metrics = apply_cdc_events(spark, events, path, WIDGET)

    rows = _rows(spark, path)
    assert set(rows) == {1}
    assert rows[1]["name"] == "alpha-v2"
    assert rows[1]["amount"] == 20.0
    # Two events collapsed to one insert; the create was skipped by dedup.
    assert metrics.rows_inserted == 1
    assert metrics.deduped_events == 1
    assert metrics.rows_skipped == 1


def test_delete_of_nonexistent_row_is_noop(spark, make_events, tmp_path):
    path = str(tmp_path / "widget")
    events = make_events([("d", 5, 99, "ghost", 0.0)])

    metrics = apply_cdc_events(spark, events, path, WIDGET)

    assert _rows(spark, path) == {}
    assert metrics.rows_deleted == 0
    assert metrics.rows_inserted == 0


def test_out_of_order_events_keep_latest_lsn(spark, make_events, tmp_path):
    path = str(tmp_path / "widget")
    # The newer event (lsn=5) arrives BEFORE the older one (lsn=1) in the batch.
    events = make_events([
        ("u", 5, 1, "final", 99.0),
        ("c", 1, 1, "initial", 1.0),
    ])

    apply_cdc_events(spark, events, path, WIDGET)

    rows = _rows(spark, path)
    assert rows[1]["name"] == "final"
    assert rows[1]["amount"] == 99.0


def test_idempotent_on_duplicate_application(spark, make_events, tmp_path):
    path = str(tmp_path / "widget")
    events = make_events([("c", 1, 1, "alpha", 10.0)])

    first = apply_cdc_events(spark, events, path, WIDGET)
    second = apply_cdc_events(spark, events, path, WIDGET)  # replay same batch

    rows = _rows(spark, path)
    assert set(rows) == {1}
    assert rows[1]["name"] == "alpha" and rows[1]["amount"] == 10.0
    assert first.rows_inserted == 1
    # Replay must not duplicate the row; state is unchanged.
    assert second.rows_inserted == 0


def test_create_update_delete_across_batches(spark, make_events, tmp_path):
    path = str(tmp_path / "widget")

    m1 = apply_cdc_events(spark, make_events([("c", 1, 7, "a", 5.0)]), path, WIDGET)
    assert m1.rows_inserted == 1
    assert _rows(spark, path)[7]["name"] == "a"

    m2 = apply_cdc_events(spark, make_events([("u", 2, 7, "b", 6.0)]), path, WIDGET)
    assert m2.rows_updated == 1
    assert _rows(spark, path)[7]["name"] == "b"

    m3 = apply_cdc_events(spark, make_events([("d", 3, 7, "b", 6.0)]), path, WIDGET)
    assert m3.rows_deleted == 1
    assert _rows(spark, path) == {}


def test_create_then_delete_in_same_batch_leaves_no_row(spark, make_events, tmp_path):
    path = str(tmp_path / "widget")
    events = make_events([
        ("c", 1, 3, "tmp", 1.0),
        ("d", 2, 3, "tmp", 1.0),   # higher LSN delete wins
    ])

    apply_cdc_events(spark, events, path, WIDGET)

    assert _rows(spark, path) == {}


def test_empty_batch_returns_zero_metrics(spark, make_events, tmp_path):
    path = str(tmp_path / "widget")
    metrics = apply_cdc_events(spark, make_events([]), path, WIDGET)
    assert metrics.input_events == 0
    assert metrics.rows_inserted == 0


def test_schema_mismatch_raises(spark, make_events, tmp_path):
    path = str(tmp_path / "widget")
    # Establish the table with the standard 3-column widget.
    apply_cdc_events(spark, make_events([("c", 1, 1, "alpha", 10.0)]), path, WIDGET)

    # Now pretend the source evolved: a spec with an extra column. _normalize
    # would reference a column the Delta target lacks, so we must fail loudly.
    evolved = TableSpec("widget", "id", ("id", "name", "amount", "discount_code"))
    events = make_events([("u", 2, 1, "alpha", 10.0)])

    with pytest.raises(SchemaMismatchError) as exc:
        apply_cdc_events(spark, events, path, evolved)
    assert "discount_code" in exc.value.added
