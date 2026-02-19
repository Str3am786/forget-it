# src/forgetit/experiments/run_toy.py
from __future__ import annotations

import time
import uuid

from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.schema import MemRecord, Query
from forgetit.memory_system.store import RetentionManager
from forgetit.policies.lru import LRUPolicy
from forgetit.retrieval.lexical import LexicalOverlapRetriever


class PrintLogger:
    def event(self, name: str, payload: dict):
        print(f"[{name}] {payload}")


def make_item(i: int, text: str) -> MemRecord:
    return MemRecord(
        id=f"m{i}",
        text=text,
        # bytes=0 so store estimates deterministically
    )


def main():
    logger = PrintLogger()
    retriever = LexicalOverlapRetriever()
    backend = InMemoryBackend(retriever=retriever)
    policy = LRUPolicy()

    # Small budget to force evictions
    store = RetentionManager(budget_bytes=250, backend=backend, policy=policy, logger=logger)
    store.connect()

    # Insert some items
    store.insert(make_item(1, "Cats are small animals that like to nap."))
    store.insert(make_item(2, "Dogs are loyal animals and like to play fetch."))
    store.insert(make_item(3, "Graph databases store nodes and edges."))

    # Retrieve updates LRU stats
    t = time.time()
    q1 = Query(id="q1", text="animals that nap", timestamp=t)
    hits = store.retrieve(q1, k=2)
    print("Hits:", [h.id for h in hits])

    # Insert more to trigger evictions
    store.insert(make_item(4, "Vector databases enable similarity search using embeddings."))
    store.insert(make_item(5, "LRU evicts the least recently accessed item."))

    # Final state
    remaining = [it.id for it in store.iter_items()]
    print("Remaining IDs:", remaining)
    print("Used bytes:", store.used_bytes, "/", store.budget_bytes)

    store.close()


if __name__ == "__main__":
    main()
