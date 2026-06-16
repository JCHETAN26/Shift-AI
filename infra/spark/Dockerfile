# PySpark 3.5 + Delta Lake 3.0 on Python 3.11 (local mode).
# Runs the CDC merge, Bronze→Silver, quality, reconciliation, and AI jobs.
# Python is pinned to 3.11 because PySpark 3.5 predates 3.14 (the host's Python).
FROM python:3.11-slim

# Spark needs a JVM; default-jdk on bookworm is OpenJDK 17 (Spark 3.5 supports it).
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk-headless procps curl \
    && rm -rf /var/lib/apt/lists/*

ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

# Layer 1: Spark + Delta (stable — rarely changes).
RUN pip install --no-cache-dir pyspark==3.5.3 delta-spark==3.0.0

# Layer 2: pre-fetch the Delta + Kafka + Avro JVM packages into the Ivy cache so
# jobs and tests start fast and offline. Depends only on layer 1.
RUN python -c "from pyspark.sql import SparkSession; \
spark = SparkSession.builder.master('local[1]') \
  .config('spark.jars.packages', \
    'io.delta:delta-spark_2.12:3.0.0,' \
    'org.apache.spark:spark-avro_2.12:3.5.3,' \
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3') \
  .getOrCreate(); spark.stop()"

# Layer 3: app-level Python deps (change more often than Spark/JARs).
COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN pip install --no-cache-dir -r /tmp/requirements-spark.txt

WORKDIR /app
CMD ["sleep", "infinity"]
