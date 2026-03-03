from __future__ import annotations
from forgetit.memory_system import RetentionManager

class LRUPolicy:
    name = "lru"


    def select_victim(self, store: RetentionManager) -> str:

        if not store:
            raise ValueError("Invalid Storage given to evict")
        
        victim_id: str | None = None
        best = float("inf")

        for it in store.iter_features():
            score = it.last_access

            if score < best:
                best = score
                victim_id = it.id
                

        if victim_id is None:
            raise RuntimeError("LRU: store empty but eviction requested.")

        return victim_id