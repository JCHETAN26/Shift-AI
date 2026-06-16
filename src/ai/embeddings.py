"""Local text embeddings via fastembed (ONNX — no torch, no API key).

Anthropic has no embeddings endpoint, so the RAG catalog embeds locally with a
small, good-quality model. Same role Veyra's embedder plays for its RAG layer.
"""
from __future__ import annotations

import os

DEFAULT_MODEL = "BAAI/bge-small-en-v1.5"
EMBED_DIM = 384  # bge-small-en-v1.5


class Embedder:
    def __init__(self, model: str | None = None):
        from fastembed import TextEmbedding

        self.model_name = model or os.environ.get("EMBEDDING_MODEL", DEFAULT_MODEL)
        self._model = TextEmbedding(model_name=self.model_name)
        self.dim = EMBED_DIM

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [vec.tolist() for vec in self._model.embed(list(texts))]

    def embed_one(self, text: str) -> list[float]:
        return self.embed([text])[0]
