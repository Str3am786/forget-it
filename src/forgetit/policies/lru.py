from __future__ import annotations

class LRUPolicy:
    name = "lru"

    def select_victim(self, store):
        victim_id = None
        best = None

        for it in store.iter_features():
            created = it.created_at if it.created_at is not None else 0.0
            score = (it.last_access, created, it.id)
            if best is None or score < best:
                best = score
                victim_id = it.id

        if victim_id is None:
            raise RuntimeError("LRU: store empty but eviction requested.")
        return victim_id
    

