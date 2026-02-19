from __future__ import annotations


class LFUPolicy:
    name = "lfu"

    def select_victim(self, store) -> str:

        victim_id = None

        best = (float("inf"), float("inf"))

        for it in store.iter_items():
            score = (it.access_count, it.last_access)

            if score < best:
                best = score
                victim_id = it.id
                
        if victim_id is None:
            raise RuntimeError("LFU: store empty but eviction requested.")
        
        return victim_id
    

    
    