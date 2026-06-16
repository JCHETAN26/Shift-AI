# dbt Core 1.8 + Snowflake adapter (Python 3.11; the host runs 3.14 which dbt
# doesn't support yet). Connection is env-driven via DBT_PROFILES_DIR.
FROM python:3.11-slim

# dbt-snowflake brings snowflake-connector-python; psycopg lets the same
# container run the reconciliation job (source Postgres + target Snowflake).
RUN pip install --no-cache-dir "dbt-core==1.8.7" "dbt-snowflake==1.8.4" "psycopg[binary]==3.2.13"

ENV DBT_PROFILES_DIR=/app/dbt
WORKDIR /app/dbt
CMD ["sleep", "infinity"]
