
from __future__ import annotations

from typing import Protocol, Mapping, List

from forgetit.core.schema import MemRecord, Query


class Retriever(Protocol):
    """
    Ranks MemRecords for a Query and returns the top-k.
    Backend/search can delegate to a Retriever (in-memory baseline).
    """

    def topk(self, items: Mapping[str, MemRecord], query: Query, k: int) -> List[MemRecord]:
        ...
