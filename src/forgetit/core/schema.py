# core/types.py
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set
import time

@dataclass
class MemRecord:
    id: str
    text: str
    embedding: Optional[list[float]] = None
    created_at: float = field(default_factory=lambda: time.time())
    last_access: float = field(default_factory=lambda: 0.0)
    access_count: int = 0
    bytes: int = 0
    meta: Dict[str, Any] = field(default_factory=dict)

    # optional graph hooks
    neighbors: Set[str] = field(default_factory=set)



# core/store.py
class RetentionManager:
    def __init__(self, budget_bytes: int, retriever, policy, logger):
        self.budget_bytes = budget_bytes
        self.retriever = retriever
        self.policy = policy
        self.logger = logger
        self.items = {}  # id -> MemRecord
        # TODO check: self.used_bytes = 0

    def insert(self, item: MemRecord):
        # enforce budget by evicting until fits
        if item.bytes > self.budget_bytes:
            raise ValueError("Single item larger than budget.")
        self._evict_until_fits(item.bytes)
        self.items[item.id] = item
        self.used_bytes += item.bytes
        self.logger.event("insert", {"id": item.id, "used_bytes": self.used_bytes})

    def retrieve(self, query, k: int):
        results = self.retriever.topk(self.items, query, k)
        now = query.timestamp
        for it in results:
            it.access_count += 1
            it.last_access = now
        self.logger.event("retrieve", {"q": query.id, "hits": [r.id for r in results]})
        return results

    def _evict_until_fits(self, incoming_bytes: int):
        while self.used_bytes + incoming_bytes > self.budget_bytes:
            victim = self.policy.select_victim(store=self)
            self._evict(victim)

    def _evict(self, victim_id: str):
        v = self.items.pop(victim_id)
        self.used_bytes -= v.bytes
        self.logger.event("evict", {"id": victim_id, "used_bytes": self.used_bytes})

# TODO placeholder 
@dataclass
class Query:
    id: str
    text: str
    timestamp: float = field(default_factory=lambda: time.time())
    embedding: Optional[list[float]] = None
    meta: Dict[str, Any] = field(default_factory=dict)