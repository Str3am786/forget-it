from __future__ import annotations

from typing import Optional, Iterable, TYPE_CHECKING

from forgetit.core import MemRecord, Query, MemFeatures
from forgetit.policies import Action, Evict, Compress, Prune
from forgetit.memory_system.base import MemorySystem

if TYPE_CHECKING:
    from forgetit.policies import EvictionPolicy
    from forgetit.backend import Backend

def estimate_bytes(item: MemRecord) -> int:

    text_bytes = len(item.text.encode("utf-8"))
    emb_bytes = 0 if item.embedding is None else 4 * len(item.embedding)  

    return text_bytes + emb_bytes

# TODO maybe look at calling it "ForgettingController"
class RetentionManager(MemorySystem):

    def __init__(self, budget_bytes: int, backend: Backend, policy: EvictionPolicy, logger):
        self.budget_bytes = budget_bytes
        self.backend = backend
        self.policy = policy
        self.logger = logger

        self.used_bytes = 0
        self._bytes_by_id: dict[str, int] = {}  

    def connect(self) -> None:
        self.backend.connect()
        # TODO remove once cheap
        if self.backend.is_persistent:
            self.rebuild_accounting()
        
    def close(self) -> None:
        self.backend.close()
 
    def insert(self, item: MemRecord) -> None:
        # Ensure bytes exists deterministically
        b = item.bytes if item.bytes > 0 else estimate_bytes(item)
        item.bytes = b  # normalize

        if b > self.budget_bytes:
            raise ValueError("Single item larger than budget.")

        existing = self.backend.get(item.id)
        if existing is not None:
            old_b = self._bytes_by_id.get(item.id, existing.bytes)
            self.used_bytes -= old_b
            self._bytes_by_id.pop(item.id, None)

        self._ensure_budget(incoming_bytes=b)
        
        # if new put the creation time as last access LRU vs LFU differentiation.

        if item.last_access == 0.0:
            item.last_access = item.created_at
        self.backend.upsert(item)
        self._bytes_by_id[item.id] = b
        self.used_bytes += b

        self.logger.event(
            "insert",
            {"id": item.id, "bytes": b, "used_bytes": self.used_bytes, "budget_bytes": self.budget_bytes},
        )

    def retrieve(self, query: Query, k: int) -> list[MemRecord]:
        results = self.backend.search(query, k)
        now = query.timestamp

        # TODO change to a multiple upsert
        for it in results:
            it.access_count += 1
            it.last_access = now
            self.backend.upsert(it)

        self.logger.event("retrieve", {"q": query.id, "hits": [r.id for r in results]})
        return results

    def get(self, item_id: str) -> Optional[MemRecord]:
        return self.backend.get(item_id)

    def delete(self, item_id: str) -> None:
        # Maintain accounting
        freed = self._remove_record(item_id)
        self.logger.event("delete", {
            "id": item_id,
            "freed_bytes": freed,
            "used_bytes": self.used_bytes,
        })
    
    def clear(self) -> None:
        """Wipes the backend and resets all byte accounting"""
        
        self.backend.clear()
        self.used_bytes = 0 
        self._bytes_by_id.clear()

        if hasattr(self.logger, "event"):
            self.logger.event("clear", {"budget_bytes": self.budget_bytes})


    def _execute_action(self, action: Action) -> None:
        """
        Dispatch a policy Action and update byte accounting
        """
        if isinstance(action, Evict):
            self._do_evict(action.id)
        elif isinstance(action, Compress):
            self._do_compress(action.id, action.new_text)
        elif isinstance(action, Prune):
            raise NotImplementedError("Not Implemented yet and requires a graph backend")
        else:
            raise TypeError(f" Unknown action type: {type(action)}")

    def _do_evict(self, record_id: str) -> None:
        """Policy-driven eviction."""
        freed = self._remove_record(record_id)
        self.logger.event("evict", {
            "id": record_id,
            "freed_bytes": freed,
            "used_bytes": self.used_bytes,
        })
    
    def _do_compress(self, record_id: str, new_text: str) -> None:
        record = self.backend.get(record_id)

        if record is None:
            # TODO re-evaluate, this only happens if the record disappears mid way.
            return 
        old_bytes = self._bytes_by_id.get(record_id, 0)
        new_bytes = len(new_text.encode("utf-8"))

        # Compression didn't help
        if new_bytes >= old_bytes:
            
            self.logger.event("compress_skipped", {
                "id": record_id,
                "old_bytes": old_bytes,
                "new_bytes": new_bytes,
            })
            return
        
        # Compression worked:
        freed = old_bytes - new_bytes
        # Mutate the record
        record.text = new_text
        record.meta["compressed"] = True
        record.meta["compression_saved_bytes"] = freed

        # Upsert back into 
        self.backend.upsert(record)

        self._bytes_by_id[record_id] = new_bytes
        self.used_bytes += (new_bytes - old_bytes)

    
        self.logger.event("compress", {
            "id": record_id,
            "old_bytes": old_bytes,
            "new_bytes": new_bytes,
            "freed_bytes": freed,
            "used_bytes": self.used_bytes,
        })


    def _ensure_budget(self, incoming_bytes: int) -> None:

        while self.used_bytes + incoming_bytes > self.budget_bytes:
            action = self.policy.select_action(self)
            self._execute_action(action=action)
    
    def _remove_record(self, record_id: str) -> int:
        """Remove a record from backend and accounting. Returns bytes freed.
        
        No logging — callers decide what event name and payload to use.
        """
        freed = self._bytes_by_id.get(record_id, 0)
        self.backend.delete(record_id)              # if this throws, nothing changed
        self._bytes_by_id.pop(record_id, None)
        self.used_bytes = max(0, self.used_bytes - freed)
        return freed

    def rebuild_accounting(self) -> None:
        self.used_bytes = 0
        self._bytes_by_id.clear()

        for item_id, b in self.backend.scan_accounting():
            self._bytes_by_id[item_id] = b
            self.used_bytes += b

    
    def iter_ids(self):
        return self.backend.iter_ids()


    def iter_features(self) -> Iterable[MemFeatures]:
        return self.backend.scan_features()
    

    def iter_items(self):
        # TODO add 'feature cache' or policy-specific indexes.
        for item_id in self.backend.iter_ids():
            it = self.backend.get(item_id)
            if it is not None:
                yield it
