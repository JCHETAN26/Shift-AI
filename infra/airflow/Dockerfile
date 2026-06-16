# Airflow 2.8 + docker CLI. DAG tasks orchestrate the job containers
# (spark, dbt, postgres, kafka) via `docker exec` over the mounted socket.
FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
USER airflow
