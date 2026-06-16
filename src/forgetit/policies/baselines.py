from __future__ import annotations

import random as _random
from typing import TYPE_CHECKING

from forgetit.policies.base import SimpleEvictionPolicy

if TYPE_CHECKING:
    from forgetit.memory_system import RetentionManager


class FIFOPolicy(SimpleEvictionPolicy):
    name = "FIFO"

    def select_victim(self, store: RetentionManager, candidates: set[str] | None = None):
        oldest_id: str | None = None
        oldest_ts: float | None = None

        for feat in store.iter_features():
            if candidates is not None and feat.id not in candidates:
                continue
            if oldest_ts is None or feat.created_at < oldest_ts:
                oldest_ts = feat.created_at
                oldest_id = feat.id

        if oldest_id is None:
            raise RuntimeError("No records to evict")

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

        return self._rng.choice(ids)


class LFUPolicy(SimpleEvictionPolicy):
    name = "lfu"

    def select_victim(self, store, candidates: set[str] | None = None):
        victim_id = None
        best = None

        for it in store.iter_features():
            if candidates is not None and it.id not in candidates:
                continue
            created = it.created_at if it.created_at is not None else 0.0
            score = (it.access_count, it.last_access, created, it.id)
            if best is None or score < best:
                best = score
                victim_id = it.id

        if victim_id is None:
            raise RuntimeError("LFU: store empty but eviction requested.")
        return victim_id


class LRUPolicy(SimpleEvictionPolicy):
    name = "lru"

    def select_victim(self, store, candidates: set[str] | None = None):
        victim_id = None
        best = None

        for it in store.iter_features():
            if candidates is not None and it.id not in candidates:
                continue
            created = it.created_at if it.created_at is not None else 0.0
            score = (it.last_access, created, it.id)
            if best is None or score < best:
                best = score
                victim_id = it.id

        if victim_id is None:
            raise RuntimeError("LRU: store empty but eviction requested.")
        return victim_id
