"""Embeddings: provider-agnostic vectors for memory items + similarity tooling.

Mirrors :mod:`forgetit.llm`: :func:`get_embedder` is the single factory the rest
of the codebase calls, so the provider/model is swapped from settings alone.
"""
import os

from .base import Embedder
from .openai_embedder import OpenAiEmbedder
from .cache import EmbeddingCache, CachingEmbedder
from .ingest import attach_embeddings
from . import similarity

from forgetit.settings import (
    EMBED_BASE_URL, EMBED_MODEL, EMBED_API_KEY, EMBED_BATCH,
    EMBED_NORMALIZE, EMBED_CACHE_PATH, EMBED_MAX_INPUT_CHARS,
)


def get_embedder(*, cache: bool = True) -> Embedder:
    """Build the configured embedder.

    ``EMBED_MAX_CONNECTIONS`` sizes the httpx pool to the concurrent in-flight
    embedding count (the embedder is a separate endpoint from the chat LLM); it
    falls back to ``LLM_MAX_CONNECTIONS`` so the existing launcher wiring still
    sizes it, and to the SDK default (100) if neither is set.  When ``cache`` is
    True and ``EMBED_CACHE_PATH`` is set, vectors are memoised to disk so corpus
    passes and reruns don't re-hit the provider.
    """
    # Encoder pre-flight: settings.py silently falls back to its default model
    # when .env is absent, and post-hoc provenance only catches that AFTER a
    # run has burned compute. A launcher that pins the encoder exports
    # EXPECTED_EMBED_MODEL; any mismatch fails loudly before the first vector.
    expected = os.environ.get("EXPECTED_EMBED_MODEL")
    if expected and expected != EMBED_MODEL:
        raise RuntimeError(
            f"Encoder pin mismatch: EMBED_MODEL={EMBED_MODEL!r} but the launcher "
            f"pinned EXPECTED_EMBED_MODEL={expected!r}. Refusing to embed — check "
            f".env / environment (settings.py silently defaults when unset)."
        )

    mc = os.environ.get("EMBED_MAX_CONNECTIONS") or os.environ.get("LLM_MAX_CONNECTIONS")
    inner: Embedder = OpenAiEmbedder(
        base_url=EMBED_BASE_URL,
        api_key=EMBED_API_KEY,
        model=EMBED_MODEL,
        batch_size=EMBED_BATCH,
        normalize=EMBED_NORMALIZE,
        max_connections=int(mc) if mc and mc.isdigit() else None,
        max_input_chars=EMBED_MAX_INPUT_CHARS,
    )
    if cache and EMBED_CACHE_PATH:
        return CachingEmbedder(inner, EmbeddingCache(EMBED_CACHE_PATH))
    return inner


__all__ = [
    "Embedder",
    "OpenAiEmbedder",
    "EmbeddingCache",
    "CachingEmbedder",
    "attach_embeddings",
    "get_embedder",
    "similarity",
]
