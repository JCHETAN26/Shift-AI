#!/usr/bin/env python3
"""Read and pretty-print raw Debezium CDC events off a Kafka topic.

This is the Phase-1 *depth checkpoint*: before writing any merge logic,
prove you understand exactly what Debezium emits. It decodes the Avro
envelope via Schema Registry and prints the op / before / after / source
for each message, so an insert→update→delete sequence is legible.

Usage:
    python scripts/peek_topic.py shift.public.orders [--max 20] [--from-beginning]
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:29092")
SCHEMA_REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL_HOST", "http://localhost:8081")

OP_LABELS = {"c": "CREATE", "u": "UPDATE", "d": "DELETE", "r": "READ(snapshot)"}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("topic", help="e.g. shift.public.orders")
    ap.add_argument("--max", type=int, default=20, help="messages to print before exiting")
    ap.add_argument("--from-beginning", action="store_true", help="read from offset 0")
    ap.add_argument("--timeout", type=float, default=20.0, help="seconds to wait for messages")
    args = ap.parse_args()

    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY})
    key_de = AvroDeserializer(sr)
    val_de = AvroDeserializer(sr)

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"peek-{os.getpid()}",
            "auto.offset.reset": "earliest" if args.from_beginning else "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.topic])
    print(f"Subscribed to {args.topic} @ {BOOTSTRAP} (registry {SCHEMA_REGISTRY})")
    print(f"Waiting up to {args.timeout}s for up to {args.max} messages...\n")

    seen = 0
    try:
        while seen < args.max:
            msg = consumer.poll(args.timeout)
            if msg is None:
                print("No more messages within timeout.")
                break
            if msg.error():
                print(f"  consumer error: {msg.error()}", file=sys.stderr)
                continue

            ctx = SerializationContext(msg.topic(), MessageField.VALUE)
            key = key_de(msg.key(), SerializationContext(msg.topic(), MessageField.KEY))
            value = val_de(msg.value(), ctx)

            seen += 1
            if value is None:
                # tombstone (Debezium emits these after delete)
                print(f"[{seen}] TOMBSTONE  key={key}  (partition {msg.partition()} offset {msg.offset()})")
                continue

            op = value.get("op")
            source = value.get("source", {})
            print(f"[{seen}] {OP_LABELS.get(op, op):16} table={source.get('table')} "
                  f"lsn={source.get('lsn')} ts_ms={source.get('ts_ms')}")
            print(f"     before: {json.dumps(value.get('before'), default=str)}")
            print(f"     after : {json.dumps(value.get('after'), default=str)}\n")
    finally:
        consumer.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
