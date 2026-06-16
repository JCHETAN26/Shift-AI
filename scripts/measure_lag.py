#!/usr/bin/env python3
"""Measure CDC replication lag (Postgres commit → Debezium emit).

For each change event Debezium stamps two timestamps from the same Docker VM
clock: `source.ts_ms` (when the row committed in Postgres) and the envelope
`ts_ms` (when the connector produced the event). Their difference is the
replication lag, with no host/container clock skew.

This drives a live UPDATE workload on `orders`, consumes the resulting CDC
events, and reports lag percentiles over the incremental changes.

    python scripts/measure_lag.py --n 300 --interval 0.02
"""
from __future__ import annotations

import argparse
import os
import random
import threading
import time

import psycopg
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:29092")
SCHEMA_REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL_HOST", "http://localhost:8081")
DSN = os.environ.get("POSTGRES_DSN", "postgresql://shift:shift_dev_password@localhost:5435/shift")
TOPIC = "shift.public.orders"


def percentile(values: list[float], p: float) -> float:
    s = sorted(values)
    return s[min(len(s) - 1, int(round(p / 100 * (len(s) - 1))))]


def run_workload(n: int, interval: float):
    """UPDATE n distinct orders, each commit producing one CDC event."""
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT order_id FROM orders ORDER BY random() LIMIT %s", (n,))
        ids = [r[0] for r in cur.fetchall()]
        for oid in ids:
            cur.execute(
                "UPDATE orders SET updated_at = now(), quantity = quantity WHERE order_id = %s",
                (oid,),
            )
            time.sleep(interval)
    print(f"workload done: {len(ids)} updates committed", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--n", type=int, default=300, help="number of updates")
    ap.add_argument("--interval", type=float, default=0.02, help="seconds between updates")
    args = ap.parse_args()

    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY})
    val_de = AvroDeserializer(sr)
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"lag-{os.getpid()}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])

    # Wait for partition assignment so we don't miss events.
    print("waiting for partition assignment...", flush=True)
    for _ in range(20):
        consumer.poll(0.5)
        if consumer.assignment():
            break
    time.sleep(1.0)

    # Generate the workload in a background thread while we consume.
    worker = threading.Thread(target=run_workload, args=(args.n, args.interval), daemon=True)
    worker.start()

    lags: list[float] = []
    deadline_idle = 25.0
    last_msg = time.time()
    while len(lags) < args.n and (time.time() - last_msg) < deadline_idle:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        value = val_de(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        if not value or value.get("op") != "u":
            continue
        src = value["source"]["ts_ms"]
        emit = value["ts_ms"]
        lags.append(emit - src)
        last_msg = time.time()
    consumer.close()

    if not lags:
        print("No streaming events captured — is the connector RUNNING?")
        return 1

    print(f"\n=== CDC replication lag (Postgres commit → Debezium emit), n={len(lags)} ===")
    print(f"  p50 = {percentile(lags, 50):>7.0f} ms")
    print(f"  p95 = {percentile(lags, 95):>7.0f} ms")
    print(f"  p99 = {percentile(lags, 99):>7.0f} ms")
    print(f"  min = {min(lags):>7.0f} ms   max = {max(lags):>7.0f} ms   mean = {sum(lags)/len(lags):>7.0f} ms")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
