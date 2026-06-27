"""Content-addressable embedding cache.

Embedding ~124k LongMemEval items (and re-running the redundancy / separability
passes) is the expensive part of Family A.  This caches every (model, text)
vector to a single SQLite file keyed by a content hash, so:

  * reruns and multiple analyses share one corpus pass,
  * the cache key includes the embedder ``name`` (model + normalisation), so
    vectors from different models never collide,
  * storage is compact float32 BLOBs, not JSON.

``CachingEmbedder`` wraps any :class:`Embedder`: cache hits are served from disk,
only misses hit the provider, and misses are written back in one transaction.
"""
from __future__ import annotations

import hashlib
import sqlite3
import struct
from pathlib import Path

from .base import Embedder


def _key(model_name: str, text: str) -> str:
    h = hashlib.sha1(text.encode("utf-8")).hexdigest()
    return f"{model_name}\x00{h}"


def _pack(vec: list[float]) -> bytes:
    return struct.pack(f"<{len(vec)}f", *vec)


def _unpack(blob: bytes) -> list[float]:
    return list(struct.unpack(f"<{len(blob) // 4}f", blob))


class EmbeddingCache:
    """Tiny SQLite key->vector store. Safe for the thread-pool runner: each
    connection is opened per-process with WAL so concurrent readers don't block."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._con = sqlite3.connect(str(self.path), check_same_thread=False)
        self._con.execute("PRAGMA journal_mode=WAL;")
        self._con.execute("PRAGMA synchronous=NORMAL;")
        self._con.execute(
            "CREATE TABLE IF NOT EXISTS emb (k TEXT PRIMARY KEY, v BLOB NOT NULL)"
        )
        self._con.commit()

    def get_many(self, keys: list[str]) -> dict[str, list[float]]:
        out: dict[str, list[float]] = {}
        cur = self._con.cursor()
        # chunk the IN() to stay under SQLite's variable limit
        for i in range(0, len(keys), 900):
            chunk = keys[i:i + 900]
            q = f"SELECT k, v FROM emb WHERE k IN ({','.join('?' * len(chunk))})"
            for k, v in cur.execute(q, chunk):
                out[k] = _unpack(v)
        return out

    def put_many(self, items: list[tuple[str, list[float]]]) -> None:
        self._con.executemany(
            "INSERT OR REPLACE INTO emb (k, v) VALUES (?, ?)",
            [(k, _pack(vec)) for k, vec in items],
        )
        self._con.commit()

    def close(self) -> None:
        self._con.close()


class CachingEmbedder(Embedder):
    def __init__(self, inner: Embedder, cache: EmbeddingCache):
        self.inner = inner
        self.cache = cache
        self.model = inner.model

    @property
    def name(self) -> str:
        return self.inner.name

    @property
    def dim(self) -> int | None:           # delegate so callers see resolved dim
        return self.inner.dim

    @dim.setter
    def dim(self, value: int | None) -> None:
        self.inner.dim = value

    def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        keys = [_key(self.name, t) for t in texts]
        hits = self.cache.get_many(list(set(keys)))

        # Embed the unique misses once, even if a text repeats in the batch.
        miss_texts: list[str] = []
        miss_keys: list[str] = []
        seen: set[str] = set()
        for k, t in zip(keys, texts):
            if k not in hits and k not in seen:
                seen.add(k)
                miss_keys.append(k)
                miss_texts.append(t)

        if miss_texts:
            new_vecs = self.inner.embed(miss_texts)
            self.cache.put_many(list(zip(miss_keys, new_vecs)))
            # Round-trip freshly-computed vectors through the same float32
            # (un)packing the cache uses, so a value is byte-identical whether it
            # was just computed or read back later — results don't drift on rerun.
            for k, v in zip(miss_keys, new_vecs):
                hits[k] = _unpack(_pack(v))

        return [hits[k] for k in keys]
