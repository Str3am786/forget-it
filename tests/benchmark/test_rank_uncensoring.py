"""S1 — gold's TRUE rank is logged unconditionally (rank uncensoring).

Sprint-1 D2 had to carry interval bounds because ``gold_ranks`` was only
evaluated to depth 10: a surviving gold ranked 11+ was right-censored. S1 adds
``gold_ranks_true`` — exact 1-based ranks over the FULL resident ranking —
while every legacy column keeps its historical VALUE (computed from the same
depth-10 prefix as before), so old and new CSVs stay comparable.

Also pins: events.jsonl ``retrieve`` hits are capped at 50 ids (byte-identical
for every existing call site — nothing requested more than 10 before), and
``gold_rank_percentile`` now comes from the TRUE ranks (exact, no censoring —
the column is sprint-1-new, so no historical artifact changes meaning).
"""
from __future__ import annotations

import json

from forgetit.backend.in_memory import InMemoryBackend
from forgetit.benchmark.runner_core import run_one_example
from forgetit.benchmark.schema import WorkloadEvent
from forgetit.core.schema import MemRecord, Query
from forgetit.memory_system import RetentionManager
from forgetit.policies import LRUPolicy
from forgetit.retrieval import BM25Retriever


class RecordingLogger:
    def __init__(self):
        self.events = []

    def event(self, name, payload=None):
        self.events.append((name, dict(payload or {})))


def _store(logger=None):
    return RetentionManager(budget_bytes=1 << 62,
                            backend=InMemoryBackend(retriever=BM25Retriever()),
                            policy=LRUPolicy(), logger=logger or RecordingLogger())


def _events_gold_beyond_depth(n_distractors=12):
    """12 distractors match the query; gold shares no query token and is the
    OLDEST item, so the zero-score tie-break (created_at desc) puts it LAST:
    exact rank 13 — beyond the legacy depth-10 horizon."""
    evs = [WorkloadEvent(type="insert", timestamp=0.0,
                         item=MemRecord(id="gold", text="quiet unrelated note",
                                        created_at=1.0, bytes=30),
                         query=None, gold_item_ids=[])]
    for i in range(n_distractors):
        evs.append(WorkloadEvent(
            type="insert", timestamp=float(i + 1),
            item=MemRecord(id=f"d{i:02d}", text="alpha topic memo",
                           created_at=float(i + 2), bytes=30),
            query=None, gold_item_ids=[]))
    evs.append(WorkloadEvent(
        type="query", timestamp=99.0, item=None,
        query=Query(id="q1", text="alpha topic", timestamp=99.0, meta={}),
        gold_item_ids=["gold"], ref_answer="x", question_type="t"))
    return evs


def test_true_rank_logged_beyond_legacy_depth():
    results, _, _, _ = run_one_example(
        _events_gold_beyond_depth(), _store(), k=5, dataset="t", store_label="t")
    (row,) = results
    assert row.gold_survived
    assert json.loads(row.gold_ranks) == []           # legacy: censored, unchanged
    assert json.loads(row.gold_ranks_true) == [13]    # S1: exact rank, unconditional
    # legacy columns keep their historical values (depth-10 prefix)
    assert row.n_retrieved == 10
    assert row.mrr == 0.0
    assert row.hit_at_10 == 0.0
    # the D2 columns become exact: resident gold is never censored any more
    assert row.gold_rank_censored == 0.0
    assert abs(row.gold_rank_percentile - 13 / 13) < 1e-12
    assert row.n_resident_at_query == 13


def test_small_store_true_and_legacy_ranks_agree():
    evs = _events_gold_beyond_depth(n_distractors=2)   # 3 residents, gold rank 3
    results, _, _, _ = run_one_example(evs, _store(), k=5, dataset="t", store_label="t")
    (row,) = results
    assert json.loads(row.gold_ranks) == [3]
    assert json.loads(row.gold_ranks_true) == [3]
    assert row.n_retrieved == 3


def test_retrieve_event_hits_capped_at_50():
    logger = RecordingLogger()
    store = _store(logger)
    for i in range(60):
        store.insert(MemRecord(id=f"i{i:02d}", text=f"tok{i:02d} filler",
                               created_at=float(i + 1), bytes=20))
    results = store.retrieve(Query(id="q", text="filler", timestamp=1.0, meta={}), k=60)
    assert len(results) == 60                          # caller gets everything
    (payload,) = [p for n, p in logger.events if n == "retrieve"]
    assert len(payload["hits"]) == 50                  # the LOG is capped at 50
    assert payload["hits"] == [r.id for r in results[:50]]
