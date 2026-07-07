"""Tests for OpenAiEmbedder — offline (``_embed_batch`` stubbed, no network).

Covers the two review-driven changes: the input-cap is part of the cache identity
(``name``), and progress logging is DEBUG-gated (silent by default, no behaviour
change to the returned vectors)."""
from __future__ import annotations

import logging

from forgetit.embeddings.openai_embedder import OpenAiEmbedder


def _stub(batch_size: int = 2) -> OpenAiEmbedder:
    e = OpenAiEmbedder(base_url="http://x/v1", api_key="k", model="m", batch_size=batch_size)
    e._embed_batch = lambda batch: [[float(len(t))] for t in batch]   # deterministic, offline
    return e


def test_input_cap_in_name():
    a = OpenAiEmbedder(base_url="http://x/v1", api_key="k", model="m", max_input_chars=8000)
    b = OpenAiEmbedder(base_url="http://x/v1", api_key="k", model="m", max_input_chars=2000)
    c = OpenAiEmbedder(base_url="http://x/v1", api_key="k", model="m", max_input_chars=None)
    assert a.name.endswith("@8000") and b.name.endswith("@2000") and c.name.endswith("@full")
    assert a.name != b.name != c.name


def test_progress_is_debug_gated_and_lossless(caplog):
    e = _stub(batch_size=2)
    texts = ["aa", "bbbb", "cccccc", "dd", "ee"]

    with caplog.at_level(logging.INFO, logger="forgetit.embeddings.openai_embedder"):
        v_info = e.embed(texts)
    assert not [r for r in caplog.records if "[embed]" in r.message]   # silent at INFO

    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger="forgetit.embeddings.openai_embedder"):
        v_debug = e.embed(texts)
    progress = [r for r in caplog.records if "[embed]" in r.message]
    assert len(progress) == 3                     # ceil(5/2) batches
    assert v_info == v_debug                       # DEBUG changes nothing about the output
