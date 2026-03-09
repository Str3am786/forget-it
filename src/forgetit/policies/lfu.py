from __future__ import annotations

from forgetit.memory_system import RetentionManager

class LFUPolicy:
    name = "lfu"

    def select_victim(self, store):
        victim_id = None
        best = None

        for it in store.iter_features():
            score = (it.access_count, it.last_access, it.created_at, it.id)
            if best is None or score < best:
                best = score
                victim_id = it.id

        if victim_id is None:
            raise RuntimeError("LFU: store empty but eviction requested.")
        return victim_id
