# benchmark/workloads/longmemeval.py
from __future__ import annotations
from typing import Iterator
from forgetit.benchmark.schema import WorkloadEvent
from forgetit.core.schema import MemRecord, Query

class LongMemEvalWorkload:
    name = "longmemeval"

    def __init__(self, path: str, itemiser: Itemiser):
        self.path = path
        self.itemiser = itemiser   # ← injected, not hardcoded

    def _load(self):
        return
    
    def events(self) -> Iterator[WorkloadEvent]:
        for example in self._load():
            for session_id, date, turns in zip(
                example["haystack_session_ids"],
                example["haystack_dates"],
                example["haystack_sessions"],
            ):
                items = self.itemiser.itemise(session_id, date, turns)
                for item in items:
                    yield WorkloadEvent(type="insert", item=item, gold_item_ids=[])

            yield WorkloadEvent(
                type="query",
                item=None,
                query=Query(id=example["question_id"], text=example["question"]),
                gold_item_ids=self._resolve_gold(example),
            )