"""Single source of truth for a memory record's storage cost in bytes.

Every byte consumer in the codebase — the store's accounting (used_bytes), the
relative-budget footprint, and the LLM compression policy — resolves cost
through :func:`record_cost`, so they can never disagree about what a record
"costs". The choice of model is one global switch, ``settings.COUNT_VECTOR_BYTES``:

  * False (default) — cost = TEXT bytes only. The embedding is a side index
    (like BM25's postings), not stored content. Keeps budgets identical to the
    no-embedding runs already characterised.
  * True            — cost = TEXT bytes + VECTOR bytes. The embedding is modelled
    as part of the stored cost.

Cost splits into a *text* term (what compression can shrink) and a *vector* term
(fixed; survives compression). Compression therefore credits savings against the
text term only and preserves the vector term — see
``RetentionManager._do_compress`` and ``LLMCompressionPolicy``.

``record.bytes`` remains the authoritative per-record cost once set (at ingestion
via ``attach_embeddings``, or normalised on insert); these helpers are how that
number is computed, and the fallback whenever ``bytes`` was never set.
"""
from __future__ import annotations


def text_bytes(item) -> int:
    """UTF-8 byte size of a record's text — the compressible part of its cost."""
    return len(item.text.encode("utf-8"))


def vector_bytes(item) -> int:
    """Byte size of a record's embedding (float32 = 4 bytes/dim); 0 if absent.

    This is the fixed, non-compressible part of the cost — counted only when the
    global switch is on.
    """
    emb = getattr(item, "embedding", None)
    return 0 if emb is None else 4 * len(emb)


def record_cost(item, *, count_vector_bytes: bool | None = None) -> int:
    """Storage cost of ``item`` under the global byte-cost model.

    ``count_vector_bytes`` overrides the global ``settings.COUNT_VECTOR_BYTES``
    when given (used by ingestion and tests); otherwise the global flag decides.
    Resolved lazily so importing this module never requires settings/.env.
    """
    if count_vector_bytes is None:
        from forgetit.settings import COUNT_VECTOR_BYTES
        count_vector_bytes = COUNT_VECTOR_BYTES
    cost = text_bytes(item)
    if count_vector_bytes:
        cost += vector_bytes(item)
    return cost
