# Shift.ai — developer entrypoints.
# `make setup` brings the stack up, seeds Postgres, and registers Debezium.

SHELL := /bin/bash
COMPOSE := docker compose
CONNECT_URL ?= http://localhost:8083
PYTHON ?= python3
# Host-side scripts (seed, peek, drift) run from the project venv created by `make venv`.
VENV_PY ?= ./.venv/bin/python

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ── Environment ────────────────────────────────────────────────
.PHONY: env
env: ## Create .env from .env.example if missing
	@test -f .env || (cp .env.example .env && echo "Created .env from template")

.PHONY: venv
venv: ## Create local Python venv and install host tooling
	$(PYTHON) -m venv .venv
	./.venv/bin/pip install --upgrade pip
	./.venv/bin/pip install -r requirements.txt

# ── Stack lifecycle ────────────────────────────────────────────
.PHONY: up
up: env ## Start all containers and wait for health
	$(COMPOSE) up -d
	@echo "Waiting for services to become healthy..."
	@$(COMPOSE) ps

.PHONY: down
down: ## Stop containers (keep volumes)
	$(COMPOSE) down

.PHONY: clean
clean: ## Stop containers AND delete volumes (full reset)
	$(COMPOSE) down -v

.PHONY: ps
ps: ## Show container status
	$(COMPOSE) ps

.PHONY: logs
logs: ## Tail logs for all services
	$(COMPOSE) logs -f --tail=100

# ── Data ───────────────────────────────────────────────────────
.PHONY: seed
seed: ## Seed Postgres with ~860K realistic rows (Faker + COPY)
	$(VENV_PY) scripts/seed_postgres.py

.PHONY: psql
psql: ## Open a psql shell against the source database
	$(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-shift} -d $${POSTGRES_DB:-shift}

# ── Debezium ───────────────────────────────────────────────────
.PHONY: register-connector
register-connector: ## Register/update the Postgres CDC connector with Debezium
	@# The PUT /config endpoint is idempotent but wants just the inner config
	@# object, so we extract it from the {name, config} envelope.
	$(PYTHON) -c "import json; print(json.dumps(json.load(open('src/ingestion/debezium_config.json'))['config']))" \
		| curl -fsS -X PUT -H "Content-Type: application/json" --data @- \
		  $(CONNECT_URL)/connectors/shift-postgres-connector/config | $(PYTHON) -m json.tool

.PHONY: connector-status
connector-status: ## Show Debezium connector + task status
	curl -fsS $(CONNECT_URL)/connectors/shift-postgres-connector/status | $(PYTHON) -m json.tool

.PHONY: topics
topics: ## List Kafka topics
	$(COMPOSE) exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# ── One-command setup + demo flow (later phases fill in stubs) ──
.PHONY: setup
setup: up seed ## Full setup: stack up + seed + register connector
	@echo "Waiting 15s for Debezium Connect to be ready..."
	@sleep 15
	@$(MAKE) register-connector
	@echo "Setup complete. Inspect CDC events with: make peek-orders"

.PHONY: peek-orders
peek-orders: ## Read raw CDC events off the orders topic (depth checkpoint)
	$(VENV_PY) scripts/peek_topic.py shift.public.orders

# Demo targets — implemented as their phases land.
.PHONY: migrate drift chaos catalog
migrate: ## (Phase 2+) Trigger snapshot + run CDC merge job
	@echo "TODO: implemented in Phase 2 (cdc_merge.py)"
drift: ## Demo schema-drift detection (rolled-back DDL; source untouched)
	PYTHONPATH=. $(VENV_PY) scripts/demo_drift.py $(TABLE)
chaos: ## Inject bad data into Silver to trip a Great Expectations failure
	$(COMPOSE) exec -e PYTHONPATH=/app spark python scripts/inject_chaos.py
catalog: ## Demo natural-language catalog search (make catalog Q="which table has revenue")
	$(COMPOSE) exec -e PYTHONPATH=/app api python -m src.ai.catalog search "$(Q)"

# ── Spark / CDC merge ──────────────────────────────────────────
.PHONY: spark-build
spark-build: ## Build the Spark image (PySpark 3.5 + Delta 3.0)
	$(COMPOSE) build spark

.PHONY: test
test: ## Run the PySpark unit tests inside the Spark container
	$(COMPOSE) run --rm spark python -m pytest tests/ -v

.PHONY: stream
stream: ## Stream a table's CDC topic into Delta Bronze (e.g. make stream TABLE=orders)
	$(COMPOSE) exec spark python -m src.ingestion.cdc_merge $(TABLE)

.PHONY: silver
silver: ## Promote a table Bronze→Silver with cleaning + quarantine (e.g. make silver TABLE=products)
	$(COMPOSE) exec spark python -m src.transformation.bronze_to_silver $(TABLE)

# ── Gold layer (Snowflake + dbt) ───────────────────────────────
.PHONY: snowflake-bootstrap
snowflake-bootstrap: ## Create Snowflake warehouse/database/schemas (idempotent)
	$(COMPOSE) run --rm dbt python /app/scripts/snowflake_bootstrap.py

.PHONY: load-snowflake
load-snowflake: ## Load Silver Delta tables into Snowflake RAW (make load-snowflake or TABLE=orders)
	$(COMPOSE) exec spark python -m src.transformation.load_snowflake $(TABLE)

.PHONY: dbt-run
dbt-run: ## Build dbt staging + Gold models on Snowflake
	$(COMPOSE) run --rm dbt dbt run

.PHONY: dbt-test
dbt-test: ## Run dbt tests
	$(COMPOSE) run --rm dbt dbt test

.PHONY: gold
gold: snowflake-bootstrap load-snowflake dbt-run ## Full Gold build: bootstrap + load + dbt
	@echo "Gold layer built on Snowflake."

.PHONY: reconcile
reconcile: ## Reconcile source Postgres vs target Snowflake (count/checksum/sample)
	$(COMPOSE) run --rm -e PYTHONPATH=/app dbt python -m src.transformation.reconcile $(TABLE)
	$(COMPOSE) run --rm dbt dbt run --select gold_migration_summary

# ── Orchestration (Airflow) ────────────────────────────────────
.PHONY: airflow
airflow: ## Start Airflow scheduler + webserver (http://localhost:8080, admin/admin)
	$(COMPOSE) --profile airflow up -d
	@echo "Airflow → http://localhost:8080   (admin / admin)"

.PHONY: airflow-dags
airflow-dags: ## List DAGs + any import errors
	$(COMPOSE) exec airflow-scheduler airflow dags list
	$(COMPOSE) exec airflow-scheduler airflow dags list-import-errors

.PHONY: airflow-trigger
airflow-trigger: ## Trigger a DAG run (make airflow-trigger DAG=shift_cdc_ingest)
	$(COMPOSE) exec airflow-scheduler airflow dags trigger $(DAG)

.PHONY: airflow-down
airflow-down: ## Stop Airflow services
	$(COMPOSE) --profile airflow down

# ── Dashboard (FastAPI + React) ────────────────────────────────
.PHONY: dashboard
dashboard: ## Start the API + React dashboard
	$(COMPOSE) up -d api ui
	@echo "Dashboard → http://localhost:5173   API → http://localhost:8000/api/overview"

.PHONY: seed-drift
seed-drift: ## Seed demo schema-drift events for the dashboard
	$(COMPOSE) exec -e PYTHONPATH=/app api python scripts/seed_drift_events.py

# ── AI layer (profiler, RAG catalog, LLM analyzers) ────────────
.PHONY: profile
profile: ## Profile Silver columns into catalog.column_profiles (Spark)
	$(COMPOSE) exec spark python -m src.quality.profiler $(TABLE)

.PHONY: catalog-index
catalog-index: ## Build the RAG catalog in Qdrant from column profiles
	$(COMPOSE) exec -e PYTHONPATH=/app api python -m src.ai.catalog index

.PHONY: explain-recon
explain-recon: ## LLM explanation of reconciliation discrepancies
	$(COMPOSE) exec -e PYTHONPATH=/app api python scripts/demo_explain_recon.py

.PHONY: explain-drift
explain-drift: ## LLM explanation of a breaking schema change
	$(COMPOSE) exec -e PYTHONPATH=/app api python scripts/demo_explain_drift.py

# ── Data quality (Great Expectations-style suites) ─────────────
.PHONY: quality
quality: ## Run Silver expectation suites + persist results (make quality SUITE=silver_orders_suite)
	$(COMPOSE) exec spark python -m src.quality.runner $(SUITE)

.PHONY: spark-shell
spark-shell: ## Open a shell in the Spark container
	$(COMPOSE) exec spark bash
