"""Tests for the embedding cache — the concurrency gate (review finding G4).

The original production bug was ``sqlite3.InterfaceError`` from two threads driving
one connection concurrently; the lock fixes it. These tests pin that fix (a
16-thread hammer on ``put_many``/``get_many``), the float32 round-trip determinism,
and the input-cap cache-key namespacing.
"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from forgetit.embeddings.cache import EmbeddingCache, CachingEmbedder, _key, _pack, _unpack
from forgetit.embeddings.base import Embedder


class _StubEmbedder(Embedder):
    """Deterministic offline embedder — vector is a function of the text length so
    tests never hit a network. Counts calls to prove cache misses vs hits."""
    model = "stub"

    def __init__(self, dim: int = 8):
        self._dim = dim
        self.calls = 0
        self._lock = threading.Lock()

    def embed(self, texts):
        with self._lock:
            self.calls += len(texts)
        return [[float((len(t) + i) % 7) for i in range(self._dim)] for t in texts]

    @property
    def name(self) -> str:
        return self.model


def test_roundtrip_determinism():
    v = [0.1, -0.2, 3.14159, 2.7e-3]
    assert _unpack(_pack(v)) == _unpack(_pack(_unpack(_pack(v))))


def test_put_get_roundtrip(tmp_path):
    c = EmbeddingCache(tmp_path / "e.sqlite")
    c.put_many([("a", [1.0, 2.0]), ("b", [3.0, 4.0])])
    got = c.get_many(["a", "b", "missing"])
    assert got["a"] == [1.0, 2.0] and got["b"] == [3.0, 4.0] and "missing" not in got
    c.close()


def test_concurrent_put_and_get_no_interface_error(tmp_path):
    """16 threads hammering one connection must not raise sqlite3.InterfaceError
    (the exact production regression the lock fixes)."""
    c = EmbeddingCache(tmp_path / "hammer.sqlite")
    errors: list[Exception] = []

    def worker(w: int):
        try:
            for j in range(50):
                k = f"k{w}_{j}"
                c.put_many([(k, [float(w), float(j)])])
                c.get_many([k, f"k{(w + 1) % 16}_{j}"])  # read own + a neighbour's
        except Exception as e:  # noqa: BLE001 — the whole point is to catch it
            errors.append(e)

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(worker, range(16)))
    assert not errors, f"concurrent access raised: {errors[:3]}"
    # every write is durable and readable afterwards
    got = c.get_many([f"k{w}_{j}" for w in range(16) for j in range(50)])
    assert len(got) == 16 * 50
    c.close()


def test_caching_embedder_hits_miss_once(tmp_path):
    stub = _StubEmbedder()
    ce = CachingEmbedder(stub, EmbeddingCache(tmp_path / "c.sqlite"))
    texts = ["alpha", "beta", "alpha"]          # duplicate within the call
    v1 = ce.embed(texts)
    assert stub.calls == 2                        # unique misses only
    v2 = ce.embed(texts)                          # all hits now
    assert stub.calls == 2 and v1 == v2           # no new provider calls, stable


def test_input_cap_is_in_the_cache_identity():
    """The cache key namespace must include the input-length cap, or two configs
    with different EMBED_MAX_INPUT_CHARS silently collide (review finding)."""
    from forgetit.embeddings.openai_embedder import OpenAiEmbedder
    a = OpenAiEmbedder(base_url="http://x/v1", api_key="k", model="m", max_input_chars=8000)
    b = OpenAiEmbedder(base_url="http://x/v1", api_key="k", model="m", max_input_chars=2000)
    assert a.name != b.name
    assert _key(a.name, "hello") != _key(b.name, "hello")


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
