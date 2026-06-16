"""RAG data catalog: embed table+column descriptions into Qdrant, search them.

Pipeline:
  Silver → profiler → catalog.column_profiles (Postgres)
         → build_catalog_documents() → embed → Qdrant
  GET /catalog/search?q=... → embed query → Qdrant search → ranked matches

`build_catalog_documents` is pure (no Qdrant/embeddings), so the text that gets
embedded is unit-tested directly.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

QDRANT_URL = os.environ.get("QDRANT_URL", "http://localhost:6333")
COLLECTION = os.environ.get("QDRANT_COLLECTION", "shift_catalog")


@dataclass(frozen=True)
class CatalogDoc:
    point_id: int
    text: str
    payload: dict


def _humanize(column: str) -> str:
    return column.replace("_", " ")


def _column_doc_text(p: dict) -> str:
    samples = ", ".join((p.get("sample_values") or [])[:5])
    parts = [
        f"Table {p['table_name']}, column {p['column_name']} ({p['data_type']}).",
        f"Represents {_humanize(p['column_name'])} in the {p['table_name']} table.",
        f"Cardinality {p['distinct_count']}, null rate {p['null_rate']:.1%}.",
    ]
    if samples:
        parts.append(f"Sample values: {samples}.")
    return " ".join(parts)


def build_catalog_documents(profiles: list[dict]) -> list[CatalogDoc]:
    """One doc per column, plus one summary doc per table (helps table-level
    queries like 'which table has order revenue')."""
    docs: list[CatalogDoc] = []
    pid = 0
    by_table: dict[str, list[dict]] = {}
    for p in profiles:
        by_table.setdefault(p["table_name"], []).append(p)
        docs.append(CatalogDoc(
            point_id=pid,
            text=_column_doc_text(p),
            payload={
                "table": p["table_name"], "column": p["column_name"],
                "data_type": p["data_type"], "null_rate": p["null_rate"],
                "distinct_count": p["distinct_count"],
                "sample_values": (p.get("sample_values") or [])[:5],
                "kind": "column",
            },
        ))
        pid += 1

    for table, cols in by_table.items():
        col_names = ", ".join(c["column_name"] for c in cols)
        docs.append(CatalogDoc(
            point_id=pid,
            text=f"Table {table} contains columns: {col_names}. "
                 f"It holds {table} records in the migrated warehouse.",
            payload={"table": table, "column": None, "kind": "table"},
        ))
        pid += 1
    return docs


# ── Qdrant wiring ──────────────────────────────────────────────────────
def _qdrant():
    from qdrant_client import QdrantClient

    return QdrantClient(url=QDRANT_URL)


def index_profiles(profiles: list[dict]) -> int:
    from qdrant_client.models import Distance, PointStruct, VectorParams

    from src.ai.embeddings import Embedder

    docs = build_catalog_documents(profiles)
    embedder = Embedder()
    vectors = embedder.embed([d.text for d in docs])

    client = _qdrant()
    client.recreate_collection(
        collection_name=COLLECTION,
        vectors_config=VectorParams(size=embedder.dim, distance=Distance.COSINE),
    )
    client.upsert(
        collection_name=COLLECTION,
        points=[
            PointStruct(id=d.point_id, vector=v, payload={**d.payload, "text": d.text})
            for d, v in zip(docs, vectors)
        ],
    )
    return len(docs)


def search(query: str, *, limit: int = 5) -> list[dict]:
    from src.ai.embeddings import Embedder

    vec = Embedder().embed_one(query)
    client = _qdrant()
    hits = client.search(collection_name=COLLECTION, query_vector=vec, limit=limit)
    return [
        {
            "score": round(h.score, 4),
            "table": h.payload.get("table"),
            "column": h.payload.get("column"),
            "kind": h.payload.get("kind"),
            "data_type": h.payload.get("data_type"),
            "sample_values": h.payload.get("sample_values"),
        }
        for h in hits
    ]


def _load_profiles_from_pg(dsn: str) -> list[dict]:
    import psycopg

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT table_name, column_name, data_type, null_rate, distinct_count, sample_values
               FROM catalog.column_profiles ORDER BY table_name, column_name"""
        )
        names = [d[0] for d in cur.description]
        return [dict(zip(names, row)) for row in cur.fetchall()]


def main() -> int:
    import argparse
    import json

    ap = argparse.ArgumentParser(description="Index or search the RAG data catalog.")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("index", help="(re)build the Qdrant catalog from column_profiles")
    s = sub.add_parser("search", help="natural-language search")
    s.add_argument("query")
    s.add_argument("--limit", type=int, default=5)
    args = ap.parse_args()

    if args.cmd == "index":
        profiles = _load_profiles_from_pg(os.environ["POSTGRES_DSN"])
        n = index_profiles(profiles)
        print(f"[catalog] indexed {n} documents into Qdrant collection '{COLLECTION}'")
    else:
        for r in search(args.query, limit=args.limit):
            print(json.dumps(r))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
