"""Embedding (vector) retriever — the dense counterpart to BM25.

Ranks resident MemRecords by cosine similarity between their stored
``embedding`` and the query embedding.  Drop-in alongside
:class:`forgetit.retrieval.lexical.BM25Retriever`: same ``topk(items, query, k)``
contract, same tie-breaks (score → newer → id).

The query vector is resolved in this order:
  1. ``query.embedding`` if already set (e.g. the workload pre-embedded it);
  2. else, if an ``embedder`` was supplied, embed ``query.text`` on the fly.

Because this is a VECTOR retriever, running it without vectors is a configuration
error, not a degenerate-but-acceptable case (the way an empty BM25 query is). So
by default (``strict=True``) it RAISES when it can't get a query vector or when no
resident item carries an embedding — rather than silently degrading to
newest-first and producing plausible-looking-but-meaningless rankings. Run with
``--dense`` (which wires the embedder and attaches item vectors) or pick
bm25/lexical. ``strict=False`` restores the newest-first fallback for ablations.

Items are assumed L2-normalised at ingestion (the embeddings engine default), so
cosine is a dot product; we normalise defensively anyway in case a caller mixed
in un-normalised vectors.
"""
from __future__ import annotations

from typing import List, Mapping, Optional

import numpy as np

from forgetit.core.schema import MemRecord, Query
from forgetit.core.timing import phase
from forgetit.retrieval.base import Retriever


class EmbeddingRetriever(Retriever):
    def __init__(self, embedder=None, *, strict: bool = True) -> None:
        # embedder: lets the retriever embed the query text itself. Items are
        # expected to already carry vectors from ingestion (attach_embeddings).
        # strict: raise (vs newest-first fallback) when vectors are missing — the
        # default, so a misconfigured vector run fails loudly instead of silently.
        self.embedder = embedder
        self.strict = strict

    def _query_vec(self, query: Query) -> Optional[np.ndarray]:
        if query.embedding is not None:
            return np.asarray(query.embedding, dtype=np.float32)
        if self.embedder is not None and query.text:
            with phase("query_embed"):
                return np.asarray(self.embedder.embed_one(query.text), dtype=np.float32)
        return None

    @staticmethod
    def _newest(items: Mapping[str, MemRecord], k: int) -> List[MemRecord]:
        return sorted(items.values(), key=lambda it: (it.created_at, it.id),
                      reverse=True)[:k]

    def topk(self, items: Mapping[str, MemRecord], query: Query, k: int) -> List[MemRecord]:
        if k <= 0 or not items:
            return []

        qv = self._query_vec(query)
        if qv is None:
            if self.strict:
                raise ValueError(
                    "EmbeddingRetriever has no query vector (query.embedding unset and "
                    "no embedder). A vector retriever needs embeddings — run with "
                    "--dense, or use bm25/lexical. Pass strict=False to fall back to "
                    "newest-first.")
            return self._newest(items, k)
        qn = np.linalg.norm(qv) or 1.0
        qv = qv / qn

        embedded = [(rid, rec) for rid, rec in items.items() if rec.embedding is not None]
        if not embedded:
            if self.strict:
                raise ValueError(
                    f"EmbeddingRetriever: none of the {len(items)} resident items carry "
                    "an embedding. Attach item vectors (attach_embeddings / --dense) "
                    "before using a vector retriever, or pass strict=False.")
            return self._newest(items, k)

        mat = np.asarray([rec.embedding for _r, rec in embedded], dtype=np.float32)
        norms = np.linalg.norm(mat, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        sims = (mat / norms) @ qv                              # cosine, shape (M,)

        # score → newer → id  (mirrors BM25Retriever tie-breaks)
        scored = [(float(sims[i]), embedded[i][1].created_at, embedded[i][0], embedded[i][1])
                  for i in range(len(embedded))]
        scored.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
        return [x[3] for x in scored[:k]]
