from __future__ import annotations

from typing import Iterable, Optional

from forgetit.core import MemRecord, Query


class EmptyStore:
    """The E2b parametric-knowledge floor: a store that retains nothing.

    Every scored query reaches the reader with ZERO evidence items, so the
    graded answers measure what the reader model knows (or guesses) without
    memory — the floor every end-to-end number is reported as lift over.
    It doubles as the contamination check: floor ≈ policy accuracy on a
    question type means that type isn't measuring memory at all.

    ``insert`` is a no-op (ingestion skipped for speed — nothing to index,
    no eviction path to simulate), and ``retrieve`` returns []. The store is
    introspectable on purpose (iter_ids/get), so ``gold_survived=False`` and
    the byte fractions are real zeros rather than opaque-store sentinels.
    """

    def insert(self, item: MemRecord) -> None:
        pass

    def retrieve(self, query: Query, k: int | None = None) -> list[MemRecord]:
        return []

    def get(self, item_id: str) -> Optional[MemRecord]:
        return None

    def get_used_bytes(self) -> int:
        return 0

    def iter_ids(self) -> Iterable[str]:
        return iter(())

    def clear(self) -> None:
        pass
