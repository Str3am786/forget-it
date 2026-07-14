"""
gold_ranks is UNCENSORED (rerun-prep §1: full-depth ranks + n_residents).

Introspectable stores rank EVERY resident on a scored query, so gold sitting
beyond the old fixed depth (10) gets its exact rank recorded instead of
silently vanishing from gold_ranks — rank 11 and rank 300 are now different
rows. n_residents (store size at query time) rides along as the denominator
for the rank-percentile / k∝N analyses built downstream.
"""
from __future__ import annotations

from forgetit.backend.in_memory import InMemoryBackend
from forgetit.benchmark.runner_core import _StoreView, _evaluate_scored_query
from forgetit.core.logging import BenchmarkLogger
from forgetit.core.schema import MemRecord, Query
from forgetit.memory_system.custom_manager import RetentionManager
from forgetit.policies import LFUPolicy
from forgetit.retrieval.lexical import LexicalOverlapRetriever


def _make_store() -> RetentionManager:
    store = RetentionManager(
        budget_bytes=1 << 30,
        backend=InMemoryBackend(retriever=LexicalOverlapRetriever()),
        policy=LFUPolicy(),
        logger=BenchmarkLogger(),
    )
    store.connect()
    return store


def _deep_gold_store(n: int = 15) -> RetentionManager:
    """n residents where GOLD ranks LAST: distractors overlap the query on
    three tokens, gold on one — so its rank is n, well past the old depth."""
    store = _make_store()
    for i in range(n - 1):
        store.insert(MemRecord(id=f"distractor_{i:02d}",
                               text=f"alpha beta gamma filler_{i}", created_at=float(i)))
    store.insert(MemRecord(id="gold", text="alpha only here", created_at=float(n)))
    return store


def test_gold_rank_beyond_old_depth_is_recorded_exactly():
    store = _deep_gold_store(n=15)
    base = _evaluate_scored_query(
        view=_StoreView(store),
        query=Query(text="alpha beta gamma"),
        gold_ids={"gold"},
        k=5,
    )
    assert base.n_residents == 15
    assert base.n_retrieved == 15, "introspectable stores rank every resident"
    assert base.gold_ranks == [15], "rank 15 must be exact, not censored away"
    assert base.hit_at_k == 0.0            # still outside the top-5
    assert base.mrr == 1.0 / 15            # uncensored: was 0.0 at depth 10


def test_shallow_gold_unchanged():
    # Gold inside the old depth: identical row values to the censored regime.
    store = _make_store()
    store.insert(MemRecord(id="gold", text="alpha beta gamma target", created_at=1.0))
    store.insert(MemRecord(id="d1", text="alpha filler", created_at=2.0))
    store.insert(MemRecord(id="d2", text="beta filler", created_at=3.0))
    base = _evaluate_scored_query(
        view=_StoreView(store),
        query=Query(text="alpha beta gamma"),
        gold_ids={"gold"},
        k=5,
    )
    assert base.gold_ranks == [1]
    assert base.hit_at_k == 1.0
    assert base.mrr == 1.0
    assert base.n_residents == 3


def test_eval_k_never_below_configured_k():
    # Fewer residents than k: retrieval still asks for at least k (harmless —
    # the backend just returns what exists) and n_residents records the truth.
    store = _make_store()
    store.insert(MemRecord(id="gold", text="alpha beta gamma", created_at=1.0))
    base = _evaluate_scored_query(
        view=_StoreView(store),
        query=Query(text="alpha beta gamma"),
        gold_ids={"gold"},
        k=5,
    )
    assert base.n_residents == 1
    assert base.n_retrieved == 1
    assert base.gold_ranks == [1]


def test_full_depth_does_not_mutate_access_state():
    # The pairing that makes full depth safe: ranking ALL residents must not
    # touch any of them (fragment 1's read-only eval, exercised at depth N).
    store = _deep_gold_store(n=15)
    before = {f.id: (f.access_count, f.last_access) for f in store.iter_features()}
    _evaluate_scored_query(
        view=_StoreView(store),
        query=Query(text="alpha beta gamma"),
        gold_ids={"gold"},
        k=5,
    )
    after = {f.id: (f.access_count, f.last_access) for f in store.iter_features()}
    assert after == before
