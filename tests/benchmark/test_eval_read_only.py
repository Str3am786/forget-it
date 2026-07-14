"""
Eval retrieval is READ-ONLY (rerun-prep §1: "eval must be read-only — assert").

Two retrieval paths exist by design:
  * probes (ingestion access signal)  → mutating ``retrieve``: bumps
    access_count / last_access — the food LRU/LFU/ebbinghaus rank on;
  * scored eval queries               → read-only ``search``: identical
    ranking, zero side-effects — the measurement never rewrites the policy
    state being measured.

Also pins that the two paths log distinct event names ("retrieve" vs
"search"), so probe hits and eval hits stay distinguishable offline.
"""
from __future__ import annotations

from forgetit.backend.in_memory import InMemoryBackend
from forgetit.benchmark.runner_core import _StoreView, _evaluate_scored_query
from forgetit.core.logging import BenchmarkLogger
from forgetit.core.schema import MemRecord, Query
from forgetit.memory_system.custom_manager import RetentionManager
from forgetit.policies import LFUPolicy
from forgetit.retrieval.lexical import LexicalOverlapRetriever


def _populated_store(n: int = 6) -> RetentionManager:
    store = RetentionManager(
        budget_bytes=1 << 30,
        backend=InMemoryBackend(retriever=LexicalOverlapRetriever()),
        policy=LFUPolicy(),
        logger=BenchmarkLogger(),
    )
    store.connect()
    for i in range(n - 1):
        store.insert(MemRecord(id=f"distractor_{i}",
                               text=f"alpha beta gamma filler_{i}", created_at=float(i)))
    store.insert(MemRecord(id="gold", text="alpha beta gamma target", created_at=float(n)))
    return store


def test_store_search_mutates_nothing():
    store = _populated_store()
    before = {f.id: (f.access_count, f.last_access) for f in store.iter_features()}
    tick_before = store._tick

    hits = store.search(Query(text="alpha beta gamma"), k=3)

    assert hits, "search should still rank and return items"
    after = {f.id: (f.access_count, f.last_access) for f in store.iter_features()}
    assert after == before, "search must not touch access_count/last_access"
    assert store._tick == tick_before, "search must not advance the store tick"


def test_search_and_retrieve_rank_identically():
    store = _populated_store()
    q = Query(text="alpha beta gamma")
    assert [r.id for r in store.search(q, k=4)] == [r.id for r in store.retrieve(q, k=4)]


def test_probe_retrieve_still_feeds_access_signal():
    # The mutating path must stay mutating — probes ARE the access signal.
    store = _populated_store()
    hits = store.retrieve(Query(text="alpha beta gamma"), k=3)
    assert hits
    assert all(h.access_count == 1 for h in hits)
    assert all(h.last_access > 0 for h in hits)


def test_eval_query_path_is_read_only():
    # Through the runner: a scored query must leave every resident untouched.
    store = _populated_store()
    before = {f.id: (f.access_count, f.last_access) for f in store.iter_features()}

    base = _evaluate_scored_query(
        view=_StoreView(store),
        query=Query(text="alpha beta gamma"),
        gold_ids={"gold"},
        k=5,
    )

    assert base.hit_at_k == 1.0                      # the query itself works
    after = {f.id: (f.access_count, f.last_access) for f in store.iter_features()}
    assert after == before, "scored eval query must not mutate access state"


def test_probe_and_eval_events_distinguishable():
    store = _populated_store()
    store.search(Query(text="alpha beta gamma"), k=3)     # eval
    store.retrieve(Query(text="alpha beta gamma"), k=3)   # probe
    assert store.logger.counts.get("search", 0) == 1
    assert store.logger.counts.get("retrieve", 0) == 1
