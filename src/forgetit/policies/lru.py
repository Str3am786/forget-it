from __future__ import annotations

class LRUPolicy:
    name = "lru"


    def select_victim(self, store) -> str:

        if not store:
            raise ValueError("Invalid Storage given to evict")
        
        victim_id = ""
        best = float("inf")

        for it in store.iter_items():
            score = it.last_access

            if score < best:
                best = score
                victim_id = it.id
                

        if victim_id is None:
            raise RuntimeError("LRU: store empty but eviction requested.")

        return victim_id