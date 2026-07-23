"""OBSERVING tests for the embed-cache key namespace (verification R-3).

These tests do NOT assert the desired behaviour — they PIN the behaviour that is
actually there today, so the two known namespace holes are visible in CI instead
of being rediscovered from a corrupted vector store. Nothing in src/ is changed.

Key construction under test:

    cache.py:_key(model_name, text) -> f"{model_name}\\x00{sha1(text)}"
    model_name = CachingEmbedder.name -> inner.name
    openai_embedder.py:name -> f"{model}{'+l2' if normalize else ''}@{cap or 'full'}"

HOLD (verified good): the three configured axes — model, normalization, input
char-cap — each move the namespace.

HOLE 1 (``test_namespace_separators_are_forgeable``): the identity is an
UNESCAPED concatenation, so a model id that itself contains ``+l2`` collides with
the normalised variant of the shorter name.

HOLE 2 (``test_runtime_cap_shrink_is_invisible_to_the_key``): ``_embed_batch``
halves the input cap in a LOCAL variable on a context-length 400 and retries.
``self.max_input_chars`` — and therefore ``name`` — never moves, so the
differently-truncated vector is written under the un-shrunk cap's key. A stale
vector is NOT impossible by key construction on this path.

If either hole is ever closed, these tests fail — which is the intended signal to
delete them and assert the fixed behaviour instead.
"""
from __future__ import annotations

import hashlib

import pytest
from openai import BadRequestError

from forgetit.embeddings.base import Embedder
from forgetit.embeddings.cache import CachingEmbedder, EmbeddingCache, _key
from forgetit.embeddings.openai_embedder import OpenAiEmbedder


def _emb(model: str, normalize: bool = True, cap: int | None = None) -> OpenAiEmbedder:
    return OpenAiEmbedder(base_url="http://x/v1", api_key="k", model=model,
                          normalize=normalize, max_input_chars=cap)


# ── HOLD: the three configured axes are each in the namespace ────────────────

@pytest.mark.parametrize("a,b,axis", [
    (_emb("bge-m3", True, 2000), _emb("e5-large", True, 2000), "model"),
    (_emb("bge-m3", True, 2000), _emb("bge-m3", False, 2000), "normalize"),
    (_emb("bge-m3", True, 2000), _emb("bge-m3", True, 8000), "char-cap"),
])
def test_each_configured_axis_moves_the_cache_key(a, b, axis):
    assert a.name != b.name, f"{axis} is absent from the cache identity"
    assert _key(a.name, "hello") != _key(b.name, "hello"), (
        f"{axis} does not separate the cache-key namespace")


# ── HOLE 1: unescaped separators ────────────────────────────────────────────

def test_namespace_separators_are_forgeable():
    """A model id ending in '+l2' collides with the L2-normalised shorter name.

    Same key, but one embedder returns raw vectors and the other L2-normalised
    ones. Pins today's behaviour (collision); flip to != when it is fixed.
    """
    raw = _emb("bge-m3+l2", normalize=False, cap=2000)     # -> 'bge-m3+l2@2000'
    l2 = _emb("bge-m3", normalize=True, cap=2000)          # -> 'bge-m3+l2@2000'
    assert raw.name == l2.name == "bge-m3+l2@2000"
    assert _key(raw.name, "hello") == _key(l2.name, "hello")
    assert raw.normalize is not l2.normalize               # yet they differ


# ── HOLE 2: runtime cap shrink is invisible to the key ───────────────────────

class _FakeEmbeddings:
    """Vector is a function of the text ACTUALLY SENT, so it reveals truncation."""

    def __init__(self, fail_first: bool):
        self.fail_first = fail_first
        self.calls = 0
        self.sent_lengths: list[int] = []

    def create(self, model, input):  # noqa: A002 — mirrors the OpenAI SDK name
        self.calls += 1
        if self.fail_first and self.calls == 1:
            raise BadRequestError(
                message="This model's maximum context length is 512 tokens",
                response=type("R", (), {"status_code": 400, "headers": {},
                                        "request": None})(),
                body=None)
        self.sent_lengths.extend(len(t) for t in input)
        data = []
        for i, t in enumerate(input):
            h = hashlib.sha1(t.encode()).digest()
            data.append(type("D", (), {"index": i,
                                       "embedding": [b / 255.0 for b in h[:4]]})())
        return type("Resp", (), {"data": data})()


def _wired(fail_first: bool, cap: int = 4000) -> OpenAiEmbedder:
    e = _emb("demo", normalize=True, cap=cap)
    e.client = type("C", (), {"embeddings": _FakeEmbeddings(fail_first)})()
    return e


def test_runtime_cap_shrink_is_invisible_to_the_key(tmp_path):
    text = "z" * 9000

    shrunk = _wired(fail_first=True)     # server 400s once -> cap halved to 2000
    clean = _wired(fail_first=False)     # server never 400s -> full 4000

    v_shrunk = shrunk.embed([text])[0]
    v_clean = clean.embed([text])[0]

    assert shrunk.client.embeddings.sent_lengths == [2000]
    assert clean.client.embeddings.sent_lengths == [4000]
    assert v_shrunk != v_clean, "the two vectors describe different prefixes"

    # ...yet the namespace never moved.
    assert shrunk.max_input_chars == 4000
    assert shrunk.name == clean.name == "demo+l2@4000"
    assert _key(shrunk.name, text) == _key(clean.name, text)


def test_stale_vector_is_served_across_the_shared_cache(tmp_path):
    """End-to-end consequence of HOLE 2: the truncated vector is written first
    and a later, correctly-configured embedder reads it back without a call."""
    text = "z" * 9000
    cache = EmbeddingCache(tmp_path / "emb.sqlite")

    writer = CachingEmbedder(_wired(fail_first=True), cache)
    v_written = writer.embed([text])[0]

    reader = CachingEmbedder(_wired(fail_first=False), cache)
    v_read = reader.embed([text])[0]

    assert reader.inner.client.embeddings.calls == 0, "cache hit expected"
    assert v_read == v_written, "the stale, differently-truncated vector is served"
    cache.close()


# ── Latent: the base-class default namespace drops two of the three axes ─────

def test_base_embedder_name_defaults_to_model_only():
    """Embedder.name falls back to the bare model id. Latent today (only
    OpenAiEmbedder ships and it overrides), but any new Embedder that forgets to
    override gets a namespace with no normalization and no cap in it."""

    class _Plain(Embedder):
        model = "plain"

        def embed(self, texts):
            return [[0.0] for _ in texts]

    assert _Plain().name == "plain"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
