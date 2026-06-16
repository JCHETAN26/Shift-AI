# Shift.ai — Intelligent Data Warehouse Migration Platform
## Build Plan + Claude Code System Prompt

---

## SYSTEM PROMPT (paste into Claude Code project instructions)

```
You are helping build Shift.ai, an intelligent data warehouse migration
platform that combines genuine data engineering depth with an AI layer
that makes migration faster and safer.

TAGLINE:
"Shift.ai migrates your data warehouse from PostgreSQL to Snowflake
with CDC streaming, PySpark transformations, automated data quality
validation, and AI-assisted schema analysis."

CORE PHILOSOPHY:
The DE work is primary. The AI layer is additive, not decorative.
Every AI feature must solve a real migration problem:
- RAG catalog: find tables without reading docs
- LLM schema analysis: understand breaking changes immediately
- AI reconciliation report: explain discrepancies in plain English
- Column profiler: generate descriptions from sample data automatically

THE DEPTH RULE:
If a tool can do it automatically, find the layer beneath it and write
that yourself. Examples:
- Don't just call spark.read.format("kafka") — write the CDC merge
  logic that handles op:c/u/d events correctly yourself
- Don't just run ge.validate() — write custom expectations that check
  referential integrity and business rules specific to your schema
- Don't just configure Debezium defaults — understand and tune
  snapshot.mode, offset.storage, max.batch.size for your workload

TECH STACK (non-negotiable):
- PostgreSQL 16 (source — WAL-enabled, seeded with realistic data)
- Debezium 2.5 (CDC connector for Postgres)
- Apache Kafka 3.7 + Confluent Schema Registry (Avro schemas)
- Apache Spark 3.5 / PySpark (local mode + Delta Lake)
- Delta Lake 3.0 (open source — Bronze/Silver layers)
- Snowflake (Gold layer — use free trial)
- dbt Core 1.8 (Gold transformations on Snowflake)
- Great Expectations 1.0 (data quality validation)
- Apache Airflow 2.8 (orchestration)
- Qdrant (RAG catalog vector store)
- OpenAI API or Anthropic Claude API (LLM calls)
- FastAPI (REST API + SSE)
- React + Recharts (migration dashboard)
- Docker Compose (full local stack except Snowflake)

SOURCE DATABASE SCHEMA (PostgreSQL — seed with realistic data):
Tables to migrate:
  customers     (customer_id, name, email, segment, region, created_at, updated_at)
  orders        (order_id, customer_id, product_id, quantity, unit_price,
                 status, channel, created_at, updated_at)
  products      (product_id, name, category, unit_cost, is_active, created_at)
  inventory     (inventory_id, product_id, warehouse_id, quantity,
                 reorder_point, updated_at)
  shipments     (shipment_id, order_id, carrier, status, shipped_at,
                 delivered_at, estimated_delivery)

Seed: 100K customers, 500K orders, 10K products, 50K inventory records.
Use Faker to generate realistic but fake PII.

CDC EVENTS (Debezium Postgres connector output format):
{
  "op": "c" | "u" | "d" | "r",   // create, update, delete, read(snapshot)
  "before": {...} | null,          // previous row state
  "after": {...} | null,           // new row state
  "source": {
    "table": "orders",
    "lsn": 12345,                  // log sequence number
    "ts_ms": 1234567890
  }
}

PYSPARK DEPTH REQUIREMENTS:
Write ALL of the following yourself (do not use AutoLoader magic):

1. CDC MERGE LOGIC (most important):
   def apply_cdc_events(events_df, target_delta_path):
       # Separate inserts, updates, deletes
       inserts = events_df.filter(col("op").isin("c", "r"))
       updates = events_df.filter(col("op") == "u")
       deletes = events_df.filter(col("op") == "d")

       # Handle deduplication: keep latest event per primary key
       # using window functions (row_number over partition by pk order by lsn desc)

       # Apply as Delta MERGE:
       # WHEN MATCHED AND op=d THEN DELETE
       # WHEN MATCHED AND op=u THEN UPDATE SET *
       # WHEN NOT MATCHED AND op in (c,r) THEN INSERT *

       # Return: rows_inserted, rows_updated, rows_deleted, rows_skipped

2. DEDUPLICATION WITH WINDOW FUNCTIONS:
   # Within a micro-batch, same row might have multiple events
   # Keep only the latest event per primary key per batch
   window = Window.partitionBy("primary_key").orderBy(desc("lsn"))
   deduped = events_df.withColumn("rank", row_number().over(window))
                      .filter(col("rank") == 1)
                      .drop("rank")

3. SCHEMA EVOLUTION HANDLER:
   def handle_schema_evolution(source_schema, target_schema):
       # Returns: SchemaChangeReport with:
       # - added_columns: list of new columns (non-breaking)
       # - dropped_columns: list of removed columns (BREAKING)
       # - type_changes: columns where type changed (check compatibility)
       # - renamed_columns: detected by name similarity + type match
       # Applies: ALTER TABLE for additions, halts for breaking changes

4. DATA QUALITY CHECK (Spark-native, not just GE):
   # Before writing to Silver, validate:
   def validate_silver(df, table_name):
       checks = {
           "null_primary_keys": df.filter(col("pk").isNull()).count(),
           "duplicate_pks": df.groupBy("pk").count().filter(col("count") > 1).count(),
           "future_timestamps": df.filter(col("created_at") > current_timestamp()).count(),
           "negative_amounts": df.filter(col("unit_price") < 0).count() if "unit_price" in df.columns else 0,
       }
       # Fail fast if any check has count > 0

5. RECONCILIATION FRAMEWORK:
   def reconcile(source_conn, target_conn, table_name):
       # Step 1: Row count comparison
       # Step 2: Checksum comparison (MD5 of concatenated row values, sorted by PK)
       # Step 3: Sample comparison (1000 random PKs, field-by-field diff)
       # Returns: ReconciliationReport with discrepancies

GREAT EXPECTATIONS DEPTH:
Write custom expectations, not just built-ins:

class ExpectOrderAmountsToMatchLineItems(CustomQueryExpectation):
    # Sum of (quantity * unit_price) for each order should equal order.total_amount
    # This validates referential integrity across tables

class ExpectNoOrphanedOrders(CustomQueryExpectation):
    # Every order.customer_id must exist in customers table

class ExpectInventoryNonNegative(ColumnExpectation):
    # quantity_on_hand must be >= 0 (physical inventory can't be negative)

class ExpectTimestampMonotonicity(ColumnExpectation):
    # updated_at >= created_at for every row

AIRFLOW DAG DESIGN:
DAG 1: shift_cdc_ingest (every 5 minutes)
  task_1: check_kafka_consumer_lag
    → branch: if lag > 10000 → trigger_alert, else → continue
  task_2: run_pyspark_cdc_job
    → on_retry_callback: exponential_backoff(max_retries=3)
    → sla=timedelta(minutes=4)  # alert if this takes > 4 min
  task_3: validate_bronze_rowcount
    → fail if count == 0 (nothing ingested = something broke)
  task_4: update_pipeline_metadata

DAG 2: shift_silver_promotion (every 15 minutes)
  depends_on: shift_cdc_ingest (last run succeeded)
  task_1: run_spark_bronze_to_silver
  task_2: run_great_expectations_silver_suite
    → on_failure: skip_gold_promotion, create_incident
  task_3: run_schema_drift_detection
    → on_breaking_change: halt_migration, alert_human
  task_4: update_freshness_metadata

DAG 3: shift_gold_promotion (every 30 minutes)
  depends_on: shift_silver_promotion
  task_1: run_dbt_models
  task_2: run_great_expectations_gold_suite
  task_3: run_reconciliation_check
  task_4: update_migration_dashboard

DAG 4: shift_backfill (manual trigger only)
  params: table_name, start_date, end_date
  task_1: reset_delta_table_to_snapshot
  task_2: reprocess_cdc_range
  task_3: rerun_silver_gold_promotion
  task_4: reconcile_backfill_result

AI LAYER (build after DE core is complete):

1. RAG DATA CATALOG:
   # After migration, embed every table + column with:
   # "{table_name}: {column_name} — {inferred_description} — sample: {sample_values}"
   # Store in Qdrant with metadata: {table, column, data_type, null_rate, cardinality}
   # API: GET /catalog/search?q="which table has customer email addresses"
   # Returns: ranked table+column matches with similarity scores

2. LLM SCHEMA DRIFT ANALYZER:
   # When schema drift detected, send to LLM:
   # "Source table 'orders' dropped column 'discount_code' (type: VARCHAR).
   #  Target table has 50K rows with this column populated.
   #  Downstream dbt model 'gold_revenue' references this column.
   #  Explain the impact and suggest a migration strategy."

3. AI RECONCILIATION REPORTER:
   # After reconciliation run, send discrepancies to LLM:
   # "Row count mismatch: source=500123, target=499891, delta=232.
   #  Checksum mismatch in orders table, 14 rows differ.
   #  Sample diff shows updated_at timestamps differ by 1 second.
   #  Explain likely cause and recommended action."

4. COLUMN PROFILER:
   # For each column, compute: data_type, null_rate, cardinality,
   # min/max/mean (numeric), sample values (string)
   # Send to LLM: generate a one-sentence description
   # Store result in data catalog

DASHBOARD (React — 4 views):

View 1: Migration Overview
  - Progress bars: tables migrated X/Y
  - Row count comparison per table (source vs target)
  - Last sync timestamp per table
  - Overall status: HEALTHY / DRIFTED / RECONCILIATION_FAILED

View 2: Schema Drift Monitor
  - Table of detected schema changes with severity badge
  - Breaking changes: RED (pipeline halted)
  - Non-breaking changes: YELLOW (auto-evolved)
  - Click → LLM explanation + recommended action

View 3: Data Quality
  - Great Expectations validation results per suite
  - Custom expectation pass/fail with row counts
  - Reconciliation report with discrepancy visualization

View 4: AI Data Catalog
  - Search bar: "which table has order revenue?"
  - Results: ranked table+column matches
  - Click → column profile (type, nulls, cardinality, sample values, AI description)

DOCKER COMPOSE SERVICES:
  postgres:16 (source database, WAL enabled)
  zookeeper
  kafka + schema-registry
  debezium-connect
  spark (PySpark local mode with Delta Lake)
  airflow-webserver + airflow-scheduler
  qdrant
  api (FastAPI)
  ui (React)
  # Snowflake: external service (free trial)

REPO STRUCTURE:
shift-ai/
├── docker-compose.yml
├── .env.example
├── Makefile
├── README.md
├── src/
│   ├── ingestion/
│   │   ├── debezium_config.json
│   │   ├── kafka_consumer.py
│   │   └── cdc_merge.py          ← THE MOST IMPORTANT FILE
│   ├── transformation/
│   │   ├── bronze_to_silver.py
│   │   ├── schema_drift.py
│   │   └── reconciliation.py
│   ├── quality/
│   │   ├── expectations/
│   │   │   ├── custom_expectations.py
│   │   │   └── suites/
│   │   └── profiler.py
│   ├── orchestration/
│   │   └── dags/
│   ├── ai/
│   │   ├── catalog.py            ← RAG data catalog
│   │   ├── drift_analyzer.py     ← LLM schema analysis
│   │   └── reconciliation_reporter.py
│   └── api/
│       ├── main.py
│       └── routers/
├── dbt/
│   └── models/
└── ui/
    └── src/

WHEN WRITING CODE:
- cdc_merge.py is the most important file — write it first, test it thoroughly
- Every PySpark job must handle: empty DataFrames, schema mismatches, partial failures
- Use Delta Lake MERGE INTO syntax, not overwrite
- Airflow DAGs use TaskFlow API exclusively
- All LLM calls go through a single provider-agnostic client (same pattern as Veyra)
- RAG retrieval uses the same pattern as Veyra's operational RAG
- Zero hardcoded credentials — .env file for everything
- README must include: one-command setup, architecture diagram, demo GIF or screenshots
```

---

## BUILD PLAN

### Phase 0: Repo + Infrastructure (Day 1, 3 hours)
Set up docker-compose with all services.
Enable WAL logging on Postgres: `wal_level=logical`
Seed source database with 100K+ realistic rows using Faker.
Verify Kafka + Schema Registry running.
Verify Debezium can connect to Postgres.

---

### Phase 1: Debezium CDC Setup (Day 1, 2 hours)
Configure Debezium Postgres connector:
- `snapshot.mode=initial` (full snapshot first, then streaming)
- `plugin.name=pgoutput` (native Postgres logical replication)
- `decimal.handling.mode=precise`
- One topic per table: `shift.public.orders`, etc.

Register Avro schemas in Schema Registry for each table.
Verify CDC events arriving in Kafka with correct op/before/after structure.

**Depth checkpoint:** Read raw Kafka messages in Python and parse a
complete insert/update/delete event sequence for the orders table.
Make sure you understand exactly what Debezium emits before writing
the merge logic.

---

### Phase 2: PySpark CDC Merge Logic (Day 2-3, 8 hours)
**This is the most important phase. Take your time here.**

Write `cdc_merge.py`:

Step 1: Spark reads from Kafka topic using readStream
Step 2: Parse Avro-encoded Debezium envelope
Step 3: Deduplicate within micro-batch using window functions
Step 4: Separate creates/updates/deletes
Step 5: Load existing Delta table (or create if first run)
Step 6: Execute MERGE INTO Delta table
Step 7: Return metrics (rows_inserted, rows_updated, rows_deleted)

Write unit tests for cdc_merge.py with synthetic DataFrames:
- Test: insert followed by update of same row
- Test: delete of non-existent row (should be no-op)
- Test: out-of-order events (older LSN arriving after newer)
- Test: duplicate events (idempotency)
- Test: schema mismatch between event and Delta table

---

### Phase 3: Schema Drift Detection (Day 3, 3 hours)
Write `schema_drift.py`:

Compare source Postgres schema (via information_schema) against
Delta Lake table schema on each ingestion run.

Classify every change:
- Column added → NON_BREAKING (auto-evolve with mergeSchema=true)
- Column dropped → BREAKING (halt, alert human)
- Type widened (INT → BIGINT) → NON_BREAKING
- Type narrowed (BIGINT → INT) → BREAKING
- Column renamed → AMBIGUOUS (flag for human review)

Write unit tests:
- Test each change type classification
- Test multiple simultaneous changes
- Test no-change scenario (should be fast, return empty report)

---

### Phase 4: Bronze to Silver Transformation (Day 4, 4 hours)
Write `bronze_to_silver.py`:

For each table, apply table-specific cleaning rules:
- customers: validate email format, normalize segment values
- orders: recalculate total_amount from quantity * unit_price
- products: ensure is_active is boolean
- inventory: ensure quantity >= 0
- shipments: validate status transitions are legal

Write Spark-native validation (not Great Expectations — that comes later):
- null_primary_keys check
- duplicate_pks check
- future_timestamps check
- type_consistency check

If any validation fails: write bad rows to `bronze.quarantine_{table}`
and continue with clean rows (don't halt the pipeline for bad rows
at this stage — log and quarantine).

---

### Phase 5: Great Expectations Custom Suites (Day 4-5, 4 hours)
Write 4 custom expectations:
- `ExpectOrderAmountsToMatchLineItems`
- `ExpectNoOrphanedOrders` (FK validation)
- `ExpectInventoryNonNegative`
- `ExpectTimestampMonotonicity`

Build suites per table:
- `silver_orders_suite`: 8 expectations including 2 custom
- `silver_customers_suite`: 6 expectations including 1 custom
- `gold_revenue_suite`: 5 expectations on Snowflake

Connect GE to Airflow: task fails if any Critical expectation fails.
Store validation results in PostgreSQL for dashboard display.

---

### Phase 6: dbt Gold Models on Snowflake (Day 5, 3 hours)
```
models/
├── staging/
│   ├── stg_orders.sql
│   ├── stg_customers.sql
│   ├── stg_products.sql
│   └── stg_inventory.sql
└── marts/
    ├── gold_revenue_daily.sql      (incremental)
    ├── gold_customer_segments.sql  (incremental)
    ├── gold_inventory_health.sql   (full refresh)
    └── gold_migration_summary.sql  (migration metadata)
```

Write `gold_migration_summary.sql` — this is unique to Shift.ai:
shows per-table: source rows, target rows, delta, last sync,
reconciliation status, schema version.

---

### Phase 7: Reconciliation Framework (Day 6, 4 hours)
Write `reconciliation.py`:

```python
class ReconciliationEngine:
    def reconcile_table(self, table_name) -> ReconciliationReport:
        # Level 1: Row count
        source_count = self.count_source(table_name)
        target_count = self.count_target(table_name)

        # Level 2: Checksum (only if counts match or delta < 1%)
        if abs(source_count - target_count) / source_count < 0.01:
            source_checksum = self.checksum_table(source, table_name)
            target_checksum = self.checksum_table(target, table_name)

        # Level 3: Sample comparison (1000 random PKs)
        sample_pks = self.random_sample_pks(table_name, n=1000)
        source_rows = self.fetch_rows(source, table_name, sample_pks)
        target_rows = self.fetch_rows(target, table_name, sample_pks)
        discrepancies = self.compare_rows(source_rows, target_rows)

        return ReconciliationReport(
            table=table_name,
            source_count=source_count,
            target_count=target_count,
            count_match=source_count == target_count,
            checksum_match=...,
            sample_discrepancies=discrepancies,
            reconciled_at=datetime.now()
        )
```

---

### Phase 8: Airflow DAGs (Day 6-7, 4 hours)
Write all 4 DAGs with proper:
- TaskFlow API (@task decorator)
- Retry logic with exponential backoff
- SLA callbacks (alert if task exceeds time limit)
- XCom for passing metrics between tasks
- Branching logic (skip Gold if Silver validation fails)
- Backfill DAG with parameterized date range

---

### Phase 9: AI Layer (Day 7-8, 4 hours)
Build in this order:

**Column Profiler first** (no LLM needed — just statistics):
Compute null_rate, cardinality, min/max/mean, sample values for
every column in Silver layer. Store in `catalog.column_profiles`.

**RAG Data Catalog second:**
Embed each table+column description using OpenAI/Anthropic embeddings.
Store vectors in Qdrant with metadata.
FastAPI endpoint: GET /catalog/search?q=...
Test: "which table has customer purchase history?" should return orders.

**LLM Schema Drift Analyzer third:**
When breaking change detected, send structured context to LLM.
Return: impact_summary, affected_downstream_models, recommended_action.
Cache responses (same drift pattern → same explanation).

**AI Reconciliation Reporter fourth:**
After reconciliation, send discrepancy report to LLM.
Return: plain English explanation + probable root cause + next steps.

---

### Phase 10: FastAPI + React Dashboard (Day 8-9, 4 hours)
4 views as specified in system prompt.
SSE endpoint for live migration progress updates.
Migration overview is the landing page.

---

### Phase 11: README + Demo (Day 9-10, 3 hours)
```bash
make setup    # docker-compose up + seed postgres + register debezium connector
make migrate  # trigger first full snapshot + DAG run
make drift    # inject schema change (DROP COLUMN) to show drift detection
make chaos    # inject bad data to trigger GE validation failure
make catalog  # demo natural language catalog search
```

README sections:
- What Shift.ai is (2 sentences)
- Architecture diagram (ASCII)
- The 5 deep DE skills demonstrated (explicitly listed)
- The 4 AI features
- Screenshot of each dashboard view
- How to run

---

## KEY METRICS FOR RESUME

"Migrated 650K+ rows across 5 tables from PostgreSQL to Snowflake
using Debezium CDC with PySpark MERGE logic handling insert, update,
and delete events with idempotent exactly-once semantics."

"Built a reconciliation framework validating row-count parity,
MD5 checksums, and 1,000-row sample comparison across source and
target, detecting a 0.04% discrepancy rate in sample validation."

"Detected breaking schema changes (column drops, type narrowing) in
real-time using schema comparison on every CDC batch, halting
migration automatically to prevent data loss."

"Implemented 4 custom Great Expectations expectations validating
cross-table referential integrity and business rules,
achieving 100% validation pass rate on clean Silver data."

"Built a RAG-powered data catalog over Qdrant, enabling natural
language column discovery across 35 columns in under 200ms."

---

## ESTIMATED BUILD TIME
Focused: 10-12 days
Parallel with job searching: 3-4 weeks

## BUILD ORDER
Phase 2 (CDC Merge) is the most important.
If you only have time for one phase, it's Phase 2.
Everything else supports it.
