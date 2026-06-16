-- Shift.ai source schema (PostgreSQL 16).
-- Five tables migrated to Snowflake via CDC. REPLICA IDENTITY FULL so
-- Debezium emits complete `before` images on UPDATE/DELETE — the CDC
-- merge logic relies on this to resolve out-of-order events.

CREATE SCHEMA IF NOT EXISTS public;

-- ─────────────────────────────────────────────────────────────
-- customers
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customers (
    customer_id  BIGINT PRIMARY KEY,
    name         TEXT        NOT NULL,
    email        TEXT        NOT NULL,
    segment      TEXT        NOT NULL,   -- consumer | smb | enterprise
    region       TEXT        NOT NULL,   -- NA | EMEA | APAC | LATAM
    created_at   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL
);

-- ─────────────────────────────────────────────────────────────
-- products
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS products (
    product_id  BIGINT PRIMARY KEY,
    name        TEXT          NOT NULL,
    category    TEXT          NOT NULL,
    unit_cost   NUMERIC(12,2) NOT NULL,
    is_active   BOOLEAN       NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ   NOT NULL
);

-- ─────────────────────────────────────────────────────────────
-- orders
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS orders (
    order_id     BIGINT PRIMARY KEY,
    customer_id  BIGINT        NOT NULL REFERENCES customers(customer_id),
    product_id   BIGINT        NOT NULL REFERENCES products(product_id),
    quantity     INTEGER       NOT NULL,
    unit_price   NUMERIC(12,2) NOT NULL,
    status       TEXT          NOT NULL,   -- pending | confirmed | shipped | delivered | cancelled
    channel      TEXT          NOT NULL,   -- web | mobile | retail | partner
    created_at   TIMESTAMPTZ   NOT NULL,
    updated_at   TIMESTAMPTZ   NOT NULL
);

-- ─────────────────────────────────────────────────────────────
-- inventory
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS inventory (
    inventory_id   BIGINT PRIMARY KEY,
    product_id     BIGINT      NOT NULL REFERENCES products(product_id),
    warehouse_id   INTEGER     NOT NULL,
    quantity       INTEGER     NOT NULL,
    reorder_point  INTEGER     NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL
);

-- ─────────────────────────────────────────────────────────────
-- shipments
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS shipments (
    shipment_id        BIGINT PRIMARY KEY,
    order_id           BIGINT      NOT NULL REFERENCES orders(order_id),
    carrier            TEXT        NOT NULL,   -- ups | fedex | usps | dhl
    status             TEXT        NOT NULL,   -- label_created | in_transit | delivered | returned
    shipped_at         TIMESTAMPTZ,
    delivered_at       TIMESTAMPTZ,
    estimated_delivery TIMESTAMPTZ
);

-- Indexes that matter for reconciliation sampling + FK joins
CREATE INDEX IF NOT EXISTS idx_orders_customer   ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_product    ON orders(product_id);
CREATE INDEX IF NOT EXISTS idx_inventory_product ON inventory(product_id);
CREATE INDEX IF NOT EXISTS idx_shipments_order   ON shipments(order_id);

-- Debezium needs full row images to emit complete before/after states.
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE products  REPLICA IDENTITY FULL;
ALTER TABLE orders    REPLICA IDENTITY FULL;
ALTER TABLE inventory REPLICA IDENTITY FULL;
ALTER TABLE shipments REPLICA IDENTITY FULL;

-- Dedicated publication for the connector (pgoutput plugin reads this).
DROP PUBLICATION IF EXISTS shift_pub;
CREATE PUBLICATION shift_pub FOR TABLE
    customers, products, orders, inventory, shipments;
