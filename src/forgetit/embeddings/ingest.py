"""Ingestion-side embedding: attach a vector to each memory item.

Used two ways, same code path:
  * runtime — a batched pre-pass over an example's records before they are
    replayed into the store, so every resident MemRecord carries ``embedding``;
  * offline — embedding a corpus for the redundancy / separability analysis.

Budget note: whether the vector counts toward a record's stored cost is the one
global switch ``settings.COUNT_VECTOR_BYTES`` (default off — the embedding is a
side index, like BM25's term stats, not content). We pin ``record.bytes`` here
via the single source of truth ``core.cost.record_cost`` so ingestion, the
store's used_bytes, the footprint, and compression all agree. Pass
``count_vector_bytes=`` to override the global flag for a specific pass.
"""
from __future__ import annotations

from forgetit.core import MemRecord
from forgetit.core.cost import record_cost
from .base import Embedder


def attach_embeddings(
    records: list[MemRecord],
    embedder: Embedder,
    *,
    count_vector_bytes: bool | None = None,
    skip_existing: bool = True,
) -> list[MemRecord]:
    """Fill ``.embedding`` for each record (in place) using one batched call.

    Returns the same list for convenience.  Records whose ``embedding`` is
    already set are skipped when ``skip_existing`` (the default).

    ``count_vector_bytes`` defaults to the global ``COUNT_VECTOR_BYTES`` setting;
    pass True/False to force a specific cost model for this pass."""
    todo = [r for r in records if not (skip_existing and r.embedding is not None)]
    if todo:
        vecs = embedder.embed([r.text or "" for r in todo])
        for r, v in zip(todo, vecs):
            r.embedding = v

    for r in records:
        # Pin the authoritative cost through the shared resolver so the chosen
        # model (text-only vs text+vector) is identical everywhere downstream.
        r.bytes = record_cost(r, count_vector_bytes=count_vector_bytes)
    return records
