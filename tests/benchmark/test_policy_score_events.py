"""
Decision provenance on eviction events (rerun-prep §1, event streams):

Every eviction event in events.jsonl carries the score the policy used to
select the victim (``policy_score``) and how many items competed
(``candidate_count``) — logged AT DECISION TIME by the policy itself, never
reconstructed offline (stateful policies make replay a bug farm). Random logs
its draw. The compression policy surfaces its selector's decision as its own.
"""
from __future__ import annotations

import pytest

from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.logging import BenchmarkLogger
from forgetit.core.schema import MemRecord
from forgetit.memory_system.custom_manager import RetentionManager
from forgetit.policies import (
    EbbinghausPolicy, EvictSmallestPolicy, FIFOPolicy, KeepSmallestPolicy,
    LFUPolicy, LRUPolicy, RandomPolicy, SemanticRedundancyPolicy,
    SemanticCoveragePolicy,
)
from forgetit.retrieval.lexical import LexicalOverlapRetriever


# Every registered policy that can run without an LLM endpoint. The semantic
# pair runs here through its content-agnostic fallback (no embeddings in this
# store), which must record decisions exactly like the primary path.
ALL_POLICIES = [
    ("lru", lambda: LRUPolicy()),
    ("lfu", lambda: LFUPolicy()),
    ("fifo", lambda: FIFOPolicy()),
    ("random", lambda: RandomPolicy(seed=7)),
    ("keep_smallest", lambda: KeepSmallestPolicy()),
    ("evict_smallest", lambda: EvictSmallestPolicy()),
    ("ebbinghaus", lambda: EbbinghausPolicy()),
    ("semantic_fallback", lambda: SemanticRedundancyPolicy()),
    ("semantic_coverage_fallback", lambda: SemanticCoveragePolicy()),
]


def _pressured_store(policy) -> RetentionManager:
    store = RetentionManager(
        budget_bytes=250,
        backend=InMemoryBackend(retriever=LexicalOverlapRetriever()),
        policy=policy, logger=BenchmarkLogger(),
    )
    store.connect()
    for i in range(4):                       # 4 × 100B > 250B → ≥1 eviction
        store.insert(MemRecord(id=f"i{i}", text="x" * 100, bytes=100,
                               created_at=float(i)))
    return store


@pytest.mark.parametrize("name,factory", ALL_POLICIES)
def test_every_eviction_event_carries_policy_score(name, factory):
    store = _pressured_store(factory())
    evicts = [e for e in store.logger.log if e.name == "evict"]
    assert evicts, f"{name}: no eviction happened"
    for e in evicts:
        assert isinstance(e.payload.get("policy_score"), float), \
            f"{name}: eviction event missing numeric policy_score: {e.payload}"
        assert e.payload.get("candidate_count", 0) >= 1, \
            f"{name}: eviction event missing candidate_count"


@pytest.mark.parametrize("name,factory", ALL_POLICIES)
def test_decision_events_carry_score_too(name, factory):
    store = _pressured_store(factory())
    decisions = [e for e in store.logger.log if e.name == "decision"]
    assert decisions, f"{name}: no decision event"
    assert all("policy_score" in e.payload for e in decisions)


def test_random_logs_its_draw_and_stays_seeded():
    # Same seed → same draws; the logged score IS the draw index.
    def draws(seed):
        store = _pressured_store(RandomPolicy(seed=seed))
        return [e.payload["policy_score"] for e in store.logger.log if e.name == "evict"]

    assert draws(7) == draws(7), "seeded random must stay reproducible"
    assert all(isinstance(d, float) for d in draws(7))


def test_compression_policy_propagates_selector_decision():
    from forgetit.policies import LLMCompressionPolicy

    class StubLLM:
        def generate(self, prompt, *, max_tokens=128, temperature=0.0):
            return "Tiny summary."

    policy = LLMCompressionPolicy(llm=StubLLM(), selector=LFUPolicy(),
                                  min_compress_bytes=100)
    store = RetentionManager(
        budget_bytes=900,
        backend=InMemoryBackend(retriever=LexicalOverlapRetriever()),
        policy=policy, logger=BenchmarkLogger(),
    )
    store.connect()
    store.insert(MemRecord(id="a", text="lorem ipsum " * 60, bytes=0))    # ~720B
    store.insert(MemRecord(id="b", text="dolor sit amet " * 40, bytes=0)) # overflow → compress

    decisions = [e for e in store.logger.log if e.name == "decision"]
    assert decisions, "no decision event"
    assert all("policy_score" in e.payload for e in decisions), \
        "compression policy must surface its selector's decision"
