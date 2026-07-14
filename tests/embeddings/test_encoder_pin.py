"""
Encoder pre-flight: a launcher that pins the encoder (EXPECTED_EMBED_MODEL)
must fail loudly on mismatch BEFORE any vector is computed — settings.py
silently falls back to its default model when .env is absent, and post-hoc
provenance only catches that after a run has burned compute.
"""
from __future__ import annotations

import pytest

import forgetit.embeddings as embeddings


def test_pin_mismatch_fails_loudly(monkeypatch):
    monkeypatch.setenv("EXPECTED_EMBED_MODEL", "qwen3-embedding-0.6b")
    monkeypatch.setattr(embeddings, "EMBED_MODEL", "nomic-embed-text")
    with pytest.raises(RuntimeError, match="Encoder pin mismatch"):
        embeddings.get_embedder()


def test_matching_pin_builds(monkeypatch):
    monkeypatch.setenv("EXPECTED_EMBED_MODEL", "pinned-model")
    monkeypatch.setattr(embeddings, "EMBED_MODEL", "pinned-model")
    assert embeddings.get_embedder(cache=False) is not None


def test_no_pin_keeps_current_behavior(monkeypatch):
    monkeypatch.delenv("EXPECTED_EMBED_MODEL", raising=False)
    assert embeddings.get_embedder(cache=False) is not None
