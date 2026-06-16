#!/usr/bin/env python3
"""Seed the Shift.ai source Postgres with realistic fake data.

Volume target (overridable via env): 100K customers, 10K products,
50K inventory, 500K orders, 200K shipments — ~860K rows total.

Uses server-side COPY (psycopg3) and streams rows from generators so
the whole dataset never lives in memory at once. Faker is used only
where human-readable PII matters (names, emails, product names);
high-volume fact tables use plain randomness for speed.

Referential integrity is guaranteed by construction:
  orders.customer_id  in [1 .. n_customers]
  orders.product_id   in [1 .. n_products]
  inventory.product_id in [1 .. n_products]
  shipments.order_id  in [1 .. n_orders]
"""
from __future__ import annotations

import os
import random
import sys
from datetime import datetime, timedelta, timezone

import psycopg
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

DSN = os.environ.get("POSTGRES_DSN", "postgresql://shift:shift_dev_password@localhost:5432/shift")

N_CUSTOMERS = int(os.environ.get("SEED_CUSTOMERS", 100_000))
N_PRODUCTS = int(os.environ.get("SEED_PRODUCTS", 10_000))
N_INVENTORY = int(os.environ.get("SEED_INVENTORY", 50_000))
N_ORDERS = int(os.environ.get("SEED_ORDERS", 500_000))
N_SHIPMENTS = int(os.environ.get("SEED_SHIPMENTS", 200_000))

SEGMENTS = ["consumer", "smb", "enterprise"]
REGIONS = ["NA", "EMEA", "APAC", "LATAM"]
CATEGORIES = ["electronics", "apparel", "home", "grocery", "toys", "sports", "beauty", "automotive"]
ORDER_STATUS = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
CHANNELS = ["web", "mobile", "retail", "partner"]
CARRIERS = ["ups", "fedex", "usps", "dhl"]
SHIP_STATUS = ["label_created", "in_transit", "delivered", "returned"]

NOW = datetime.now(timezone.utc)
START = NOW - timedelta(days=730)  # 2-year window


def _ts_pair() -> tuple[datetime, datetime]:
    """A (created_at, updated_at) pair where updated_at >= created_at.

    Guarantees the ExpectTimestampMonotonicity invariant at the source.
    """
    created = START + timedelta(seconds=random.randint(0, int((NOW - START).total_seconds())))
    updated = created + timedelta(seconds=random.randint(0, 60 * 60 * 24 * 30))
    if updated > NOW:
        updated = NOW
    return created, updated


def gen_customers():
    for cid in range(1, N_CUSTOMERS + 1):
        created, updated = _ts_pair()
        yield (
            cid,
            fake.name(),
            f"user{cid}@{fake.free_email_domain()}",
            random.choice(SEGMENTS),
            random.choice(REGIONS),
            created,
            updated,
        )


def gen_products():
    for pid in range(1, N_PRODUCTS + 1):
        created, _ = _ts_pair()
        yield (
            pid,
            fake.catch_phrase(),
            random.choice(CATEGORIES),
            round(random.uniform(1.5, 800.0), 2),
            random.random() > 0.05,  # ~5% inactive
            created,
        )


def gen_inventory():
    for iid in range(1, N_INVENTORY + 1):
        qty = random.randint(0, 5000)
        yield (
            iid,
            random.randint(1, N_PRODUCTS),
            random.randint(1, 25),               # warehouse_id
            qty,
            random.randint(50, 500),             # reorder_point
            START + timedelta(seconds=random.randint(0, int((NOW - START).total_seconds()))),
        )


def gen_orders():
    for oid in range(1, N_ORDERS + 1):
        created, updated = _ts_pair()
        yield (
            oid,
            random.randint(1, N_CUSTOMERS),
            random.randint(1, N_PRODUCTS),
            random.randint(1, 25),                # quantity
            round(random.uniform(2.0, 1200.0), 2),  # unit_price
            random.choice(ORDER_STATUS),
            random.choice(CHANNELS),
            created,
            updated,
        )


def gen_shipments():
    for sid in range(1, N_SHIPMENTS + 1):
        order_id = random.randint(1, N_ORDERS)
        status = random.choice(SHIP_STATUS)
        shipped = START + timedelta(seconds=random.randint(0, int((NOW - START).total_seconds())))
        estimated = shipped + timedelta(days=random.randint(1, 10))
        delivered = None
        if status in ("delivered", "returned"):
            delivered = shipped + timedelta(days=random.randint(1, 14))
        # label_created shipments may not have shipped yet
        if status == "label_created":
            shipped_val = None
        else:
            shipped_val = shipped
        yield (sid, order_id, random.choice(CARRIERS), status, shipped_val, delivered, estimated)


COPY_SPECS = [
    ("products",  "product_id, name, category, unit_cost, is_active, created_at", gen_products, N_PRODUCTS),
    ("customers", "customer_id, name, email, segment, region, created_at, updated_at", gen_customers, N_CUSTOMERS),
    ("inventory", "inventory_id, product_id, warehouse_id, quantity, reorder_point, updated_at", gen_inventory, N_INVENTORY),
    ("orders",    "order_id, customer_id, product_id, quantity, unit_price, status, channel, created_at, updated_at", gen_orders, N_ORDERS),
    ("shipments", "shipment_id, order_id, carrier, status, shipped_at, delivered_at, estimated_delivery", gen_shipments, N_SHIPMENTS),
]


def main() -> int:
    print(f"Connecting to {DSN.rsplit('@', 1)[-1]} ...", flush=True)
    with psycopg.connect(DSN, autocommit=False) as conn:
        for table, columns, gen, total in COPY_SPECS:
            with conn.cursor() as cur:
                cur.execute(f"SELECT count(*) FROM {table}")
                existing = cur.fetchone()[0]
                if existing >= total:
                    print(f"  {table}: already has {existing:,} rows — skipping", flush=True)
                    continue
                if existing > 0:
                    print(f"  {table}: truncating {existing:,} partial rows", flush=True)
                    cur.execute(f"TRUNCATE {table} CASCADE")

            print(f"  {table}: loading {total:,} rows via COPY ...", flush=True)
            with conn.cursor() as cur:
                with cur.copy(f"COPY {table} ({columns}) FROM STDIN") as copy:
                    for i, row in enumerate(gen(), 1):
                        copy.write_row(row)
                        if i % 100_000 == 0:
                            print(f"    {table}: {i:,}/{total:,}", flush=True)
            conn.commit()
            print(f"  {table}: done", flush=True)

        with conn.cursor() as cur:
            for table, *_ in COPY_SPECS:
                cur.execute(f"ANALYZE {table}")
    print("Seed complete.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
