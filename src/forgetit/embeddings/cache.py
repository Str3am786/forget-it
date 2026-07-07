"""Content-addressable embedding cache.

Embedding ~124k LongMemEval items (and re-running the redundancy / separability
passes) is the expensive part of Family A.  This caches every (embedder-identity,
text) vector to a SQLite file keyed by a content hash, so:

  * reruns and multiple analyses within a process share one corpus pass,
  * the cache key includes the embedder ``name`` (model + normalisation + input
    cap — see identity note below), so vectors from different configs never collide,
  * storage is compact float32 BLOBs, not JSON.

``CachingEmbedder`` wraps any :class:`Embedder`: cache hits are served from disk,
only misses hit the provider, and misses are written back.

SCOPE — this is a PER-PROCESS, per-shard, node-local cache; it is NOT a shared
cross-job store:

  * Intra-process safety comes from a ``threading.Lock``: the across-example runner
    shares ONE ``CachingEmbedder`` / one sqlite connection across a
    ThreadPoolExecutor, and a single connection is NOT safe under concurrent use
    even with ``check_same_thread=False`` — two threads issuing
    ``executemany``/``commit`` on it raise ``sqlite3.InterfaceError``. The lock
    serialises every connection touch; the provider ``embed`` call stays OUTSIDE
    the lock so embedding remains parallel. The lock is the intra-process guarantee.

  * ``journal_mode=WAL`` is set for the case of a second *process* sharing the file,
    but do NOT rely on it: WAL needs the ``-wal``/``-shm`` shared-memory mmap that
    network filesystems (Lustre ``/scratch``) don't provide — SQLite can silently
    fall back to rollback-journal mode and still *report* success, so cross-process
    sharing on such a filesystem is unsafe regardless of this PRAGMA. We fetch and
    log the resulting mode (a silent fallback is at least visible), but the honest
    posture is: keep the cache node-local (``/tmp``) and per-shard. WAL buys little
    intra-process anyway — the lock already serialises readers and writers.

  * For a PERSISTENT, cross-run/cross-node corpus, don't use this at all: build an
    immutable per-(dataset, itemiser, model, input-cap) vector file offline and load
    it read-only. Concurrent reads of an immutable file need no locking and are safe
    on any filesystem. Note that runtime-generated texts (e.g. LLMCompression's
    summaries, re-embedded mid-sweep) cannot live in such a file, so an offline
    corpus must be a PREFERRED source with a live-provider fallback, not a
    hard-error-on-miss store.
"""
from __future__ import annotations

import hashlib
import logging
import sqlite3
import struct
import threading
from pathlib import Path

from .base import Embedder

log = logging.getLogger(__name__)


def _key(model_name: str, text: str) -> str:
    h = hashlib.sha1(text.encode("utf-8")).hexdigest()
    return f"{model_name}\x00{h}"


def _pack(vec: list[float]) -> bytes:
    return struct.pack(f"<{len(vec)}f", *vec)


def _unpack(blob: bytes) -> list[float]:
    return list(struct.unpack(f"<{len(blob) // 4}f", blob))


class EmbeddingCache:
    """SQLite key->vector store, safe under the thread-pool runner (one process).

    One connection per process plus a process-local lock so the pool's threads
    can't drive the same connection object concurrently. ``busy_timeout`` lets it
    wait rather than error if a second process ever contends on the file. See the
    module docstring for why WAL is not a cross-process safety guarantee here.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._con = sqlite3.connect(str(self.path), check_same_thread=False)
        # Fetch the RESULTING journal mode: PRAGMA journal_mode=WAL returns the mode
        # actually in force, and on a filesystem without -shm support (Lustre) it can
        # silently stay in rollback-journal mode. Surfacing it turns a silent,
        # unsafe-cross-process fallback into a visible warning.
        self.journal_mode = self._con.execute("PRAGMA journal_mode=WAL;").fetchone()[0]
        if str(self.journal_mode).lower() != "wal":
            log.warning("[embed-cache] %s: journal_mode is %r, not 'wal' (filesystem may "
                        "lack -shm support). Safe for THIS process via the lock; do NOT "
                        "share this file across processes here.", self.path, self.journal_mode)
        self._con.execute("PRAGMA synchronous=NORMAL;")
        self._con.execute("PRAGMA busy_timeout=30000;")
        self._con.execute(
            "CREATE TABLE IF NOT EXISTS emb (k TEXT PRIMARY KEY, v BLOB NOT NULL)"
        )
        self._con.commit()

    def get_many(self, keys: list[str]) -> dict[str, list[float]]:
        # Hold the lock only for the SQLite fetch; unpack (CPU-bound struct.unpack
        # over potentially 100k+ rows) OUTSIDE the lock so it doesn't block other
        # threads' cache access.
        rows: list[tuple[str, bytes]] = []
        with self._lock:
            cur = self._con.cursor()
            # chunk the IN() to stay under SQLite's variable limit
            for i in range(0, len(keys), 900):
                chunk = keys[i:i + 900]
                q = f"SELECT k, v FROM emb WHERE k IN ({','.join('?' * len(chunk))})"
                rows.extend(cur.execute(q, chunk).fetchall())
        return {k: _unpack(v) for k, v in rows}

    def put_many(self, items: list[tuple[str, list[float]]]) -> None:
        rows = [(k, _pack(vec)) for k, vec in items]
        with self._lock:
            self._con.executemany(
                "INSERT OR REPLACE INTO emb (k, v) VALUES (?, ?)",
                rows,
            )
            self._con.commit()

    def close(self) -> None:
        with self._lock:
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
        # NB: dedup is per-CALL. Under the thread-pool runner two concurrent embed()
        # calls whose miss sets overlap will each embed the shared text — harmless
        # (INSERT OR REPLACE is idempotent and the float32 round-trip below makes the
        # two results byte-identical), just a few redundant provider calls.
        miss_texts: list[str] = []
        miss_keys: list[str] = []
        seen: set[str] = set()
        for k, t in zip(keys, texts):
            if k not in hits and k not in seen:
                seen.add(k)
                miss_keys.append(k)
                miss_texts.append(t)

        if miss_texts:
            # NB: provider call is OUTSIDE the cache lock — embedding stays parallel.
            new_vecs = self.inner.embed(miss_texts)
            self.cache.put_many(list(zip(miss_keys, new_vecs)))
            # Round-trip freshly-computed vectors through the same float32
            # (un)packing the cache uses, so a value is byte-identical whether it
            # was just computed or read back later — results don't drift on rerun.
            for k, v in zip(miss_keys, new_vecs):
                hits[k] = _unpack(_pack(v))

        return [hits[k] for k in keys]
