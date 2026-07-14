from __future__ import annotations

import random as _random
from typing import TYPE_CHECKING

from forgetit.policies.base import SimpleEvictionPolicy

if TYPE_CHECKING:
    from forgetit.memory_system import RetentionManager


class FIFOPolicy(SimpleEvictionPolicy):
    """Evict the item with the oldest created_at.

    Ties keep the FIRST-encountered item (strict <), which for the in-memory
    backend is insertion order — the true FIFO tiebreak when several items
    share a timestamp (e.g. turn-pairs itemised from one session)."""
    name = "FIFO"

    def select_victim(self, store: RetentionManager, candidates: set[str] | None = None):
        oldest_id: str | None = None
        oldest_ts: float | None = None

        n = 0
        for feat in store.iter_features():
            if candidates is not None and feat.id not in candidates:
                continue
            n += 1
            created = feat.created_at if feat.created_at is not None else 0.0
            if oldest_ts is None or created < oldest_ts:
                oldest_ts = created
                oldest_id = feat.id

        if oldest_id is None:
            raise RuntimeError("No records to evict")

        self._note_decision(oldest_ts, n)
        return oldest_id

class RandomPolicy(SimpleEvictionPolicy):
    name = "Random"

    def __init__(self, seed):
        self._rng = _random.Random(seed)


    def select_victim(self, store, candidates: set[str] | None = None):
        ids = list(store.iter_ids())
        if candidates is not None:
            ids = [i for i in ids if i in candidates]
        if not ids:
            raise RuntimeError("No records to evict")

        draw = self._rng.randrange(len(ids))     # the logged "score" IS the draw
        self._note_decision(draw, len(ids))
        return ids[draw]


class LFUPolicy(SimpleEvictionPolicy):
    name = "lfu"

    def select_victim(self, store, candidates: set[str] | None = None):
        victim_id = None
        best = None

        n = 0
        for it in store.iter_features():
            if candidates is not None and it.id not in candidates:
                continue
            n += 1
            created = it.created_at if it.created_at is not None else 0.0
            score = (it.access_count, it.last_access, created, it.id)
            if best is None or score < best:
                best = score
                victim_id = it.id

        if victim_id is None:
            raise RuntimeError("LFU: store empty but eviction requested.")
        self._note_decision(best[0], n)
        return victim_id


class LRUPolicy(SimpleEvictionPolicy):
    name = "lru"

    def select_victim(self, store, candidates: set[str] | None = None):
        victim_id = None
        best = None

        n = 0
        for it in store.iter_features():
            if candidates is not None and it.id not in candidates:
                continue
            n += 1
            created = it.created_at if it.created_at is not None else 0.0
            score = (it.last_access, created, it.id)
            if best is None or score < best:
                best = score
                victim_id = it.id

        if victim_id is None:
            raise RuntimeError("LRU: store empty but eviction requested.")
        self._note_decision(best[0], n)
        return victim_id


class KeepSmallestPolicy(SimpleEvictionPolicy):
    """Evict the LARGEST resident item so the budget holds more (smaller) items.

    The content-agnostic size heuristic with a knapsack flavour: shed the few big
    items first to maximise the *count* retained. The trace-only `keep_smallest`
    baseline in oracle_size_analysis.py made this concrete; this is the runnable
    policy. Ties: largest, then oldest, then id (deterministic)."""
    name = "keep_smallest"

    def select_victim(self, store, candidates: set[str] | None = None):
        victim_id = None
        best = None
        n = 0
        for it in store.iter_features():
            if candidates is not None and it.id not in candidates:
                continue
            n += 1
            created = it.created_at if it.created_at is not None else 0.0
            # min of (-bytes, created, id) == largest, then oldest, then lowest id
            score = (-it.bytes, created, it.id)
            if best is None or score < best:
                best = score
                victim_id = it.id
        if victim_id is None:
            raise RuntimeError("keep_smallest: store empty but eviction requested.")
        self._note_decision(best[0], n)
        return victim_id


class EvictSmallestPolicy(SimpleEvictionPolicy):
    """Evict the SMALLEST resident item (keep the large ones).

    The opposite size extreme — the worst size heuristic when small items carry
    gold, and the natural pessimistic bracket for the size axis. Ties: smallest,
    then oldest, then id."""
    name = "evict_smallest"

    def select_victim(self, store, candidates: set[str] | None = None):
        victim_id = None
        best = None
        n = 0
        for it in store.iter_features():
            if candidates is not None and it.id not in candidates:
                continue
            n += 1
            created = it.created_at if it.created_at is not None else 0.0
            score = (it.bytes, created, it.id)   # smallest, then oldest, then id
            if best is None or score < best:
                best = score
                victim_id = it.id
        if victim_id is None:
            raise RuntimeError("evict_smallest: store empty but eviction requested.")
        self._note_decision(best[0], n)
        return victim_id
