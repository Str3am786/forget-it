from __future__ import annotations

from dataclasses import replace
from typing import Dict, Iterable, Optional, List

from forgetit.retrieval import Retriever
from forgetit.backend import Backend
from forgetit.core.schema import MemRecord, Query, MemFeatures


class InMemoryBackend(Backend):
    """
    Simple in-memory backend for experiments.
    - Stores MemRecord objects by id
    - Does not do any embedding computation
    - search() delegates to a retriever passed at construction time

    Notes:
    - connect/close are no-ops, but kept for interface parity with DB backends.
    """

    def __init__(self, retriever: Retriever):
        self._retriever = retriever
        self._items: Dict[str, MemRecord] = {}
        self._connected = False

    def connect(self) -> None:
        self._connected = True

    def close(self) -> None:
        self._connected = False

    def upsert(self, item: MemRecord) -> None:
        self._items[item.id] = item

    def get(self, item_id: str) -> Optional[MemRecord]:
        return self._items.get(item_id)

    def delete(self, item_id: str) -> None:
        self._items.pop(item_id, None)

    def search(self, query: Query, k: int) -> List[MemRecord]:
        return self._retriever.topk(self._items, query, k)

    def iter_ids(self) -> Iterable[str]:
        return self._items.keys()

    def count(self) -> int:
        return len(self._items)

    def all_items(self) -> Dict[str, MemRecord]:
        return self._items
    
    def scan_accounting(self) -> list[tuple[str, int]]:
        out: list[tuple[str, int]] = []
        for it in self._items.values():
            b = it.bytes if it.bytes and it.bytes > 0 else len(it.text.encode("utf-8"))
            out.append((it.id, b))
        return out

    def scan_features(self):
        for it in self._items.values():
            b = it.bytes if it.bytes and it.bytes > 0 else len(it.text.encode("utf-8"))
            yield MemFeatures(
                id=it.id,
                bytes=b,
                last_access=it.last_access,
                access_count=it.access_count,
                created_at=it.created_at,
            )
