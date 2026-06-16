# Shift.ai API + AI layer (Python 3.11). Hosts the RAG catalog, LLM analyzers,
# and (Phase 10) the FastAPI dashboard backend.
FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-api.txt /tmp/requirements-api.txt
RUN pip install --no-cache-dir -r /tmp/requirements-api.txt

WORKDIR /app
CMD ["sleep", "infinity"]
