from __future__ import annotations

from typing import Optional

from forgetit.core.schema import MemRecord, Query
from forgetit.backend.backend import Backend


def estimate_bytes(item: MemRecord) -> int:

    text_bytes = len(item.text.encode("utf-8"))
    emb_bytes = 0 if item.embedding is None else 4 * len(item.embedding)  

    return text_bytes + emb_bytes

# TODO maybe look at calling it "ForgettingController"
class RetentionManager:


    def __init__(self, budget_bytes: int, backend: Backend, policy, logger):
        self.budget_bytes = budget_bytes
        self.backend = backend
        self.policy = policy
        self.logger = logger

        self.used_bytes = 0
        self._bytes_by_id: dict[str, int] = {}  

    def connect(self) -> None:
        self.backend.connect()

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

        self._evict_until_fits(incoming_bytes=b)

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

        # Update access stats in backend
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
        b = self._bytes_by_id.pop(item_id, None)
        if b is None:
            # If we don't know, try backend
            it = self.backend.get(item_id)
            b = it.bytes if it else 0

        self.backend.delete(item_id)
        self.used_bytes = max(0, self.used_bytes - b)

        self.logger.event("delete", {"id": item_id, "used_bytes": self.used_bytes})

    def _evict_until_fits(self, incoming_bytes: int) -> None:
        while self.used_bytes + incoming_bytes > self.budget_bytes:
            victim_id = self.policy.select_victim(store=self)
            self._evict(victim_id)

    def _evict(self, victim_id: str) -> None:
        # Get bytes before delete for correct accounting
        b = self._bytes_by_id.pop(victim_id, None)
        if b is None:
            it = self.backend.get(victim_id)
            b = it.bytes if it else 0

        self.backend.delete(victim_id)
        self.used_bytes = max(0, self.used_bytes - b)

        self.logger.event("evict", {"id": victim_id, "bytes": b, "used_bytes": self.used_bytes})

    # ---- Backend-agnostic iteration helpers for policies ----

    def iter_ids(self):
        return self.backend.iter_ids()

    def iter_items(self):
        """
        Policies often need item attributes.
        This is backend-agnostic but can be expensive for DB backends.
        For now, fine. Later you can add a 'feature cache' or policy-specific indexes.
        """
        for item_id in self.backend.iter_ids():
            it = self.backend.get(item_id)
            if it is not None:
                yield it
