"""
Tests for the value-per-byte family: SalienceDecayPolicy and GDSFPolicy.

Covers the rerun-prep §4 asserts: GDSF's inflation clock is monotone
nondecreasing, and access genuinely changes H (via the RetentionManager
hooks). freeze_L pins the clock at 0 = pure frequency-density. Salience
decay: frequency, decay, and size terms each order victims; log-space
scoring survives extreme ages (the ebbinghaus underflow class).
"""
from __future__ import annotations

import pytest

from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.logging import BenchmarkLogger
from forgetit.core.schema import MemFeatures, MemRecord, Query
from forgetit.memory_system.custom_manager import RetentionManager
from forgetit.policies.density import GDSFPolicy, SalienceDecayPolicy
from forgetit.retrieval.lexical import LexicalOverlapRetriever


def _feat(id_: str, *, bytes_: int = 100, last_access: float = 0.0,
          access_count: int = 0) -> MemFeatures:
    return MemFeatures(id=id_, bytes=bytes_, last_access=last_access,
                       access_count=access_count, created_at=0.0)


class FakeStore:
    def __init__(self, *features: MemFeatures):
        self._features = list(features)

    def iter_features(self):
        return iter(self._features)


# ── SalienceDecayPolicy ─────────────────────────────────────────────

class TestSalienceDecay:

    def test_stale_item_evicted_first(self):
        store = FakeStore(
            _feat("stale", last_access=0.0),
            _feat("fresh", last_access=1000.0),
        )
        assert SalienceDecayPolicy(half_life_ticks=100).select_victim(store) == "stale"

    def test_frequency_protects_a_stale_item(self):
        # Equal staleness: the accessed item scores higher and survives.
        store = FakeStore(
            _feat("stale_hot", last_access=0.0, access_count=7),
            _feat("stale_cold", last_access=0.0),
            _feat("fresh", last_access=1000.0),
        )
        assert SalienceDecayPolicy(half_life_ticks=100).select_victim(store) == "stale_cold"

    def test_decay_eventually_beats_frequency(self):
        # A hot-but-ancient item loses to a cold-but-recent one once the age
        # gap exceeds what log2(1 + count) can buy: with half_life=10 the
        # 100-tick gap costs 10 doublings vs count 7 → ~3 doublings.
        store = FakeStore(
            _feat("ancient_hot", last_access=0.0, access_count=7),
            _feat("recent_cold", last_access=100.0),
            _feat("fresh", last_access=200.0),
        )
        assert SalienceDecayPolicy(half_life_ticks=10).select_victim(store) == "ancient_hot"

    def test_larger_item_evicted_on_equal_value(self):
        store = FakeStore(
            _feat("big", bytes_=1000, last_access=50.0),
            _feat("small", bytes_=10, last_access=50.0),
            _feat("fresh", last_access=100.0),
        )
        assert SalienceDecayPolicy().select_victim(store) == "big"

    def test_extreme_age_keeps_frequency_ordering(self):
        # Underflow regression class: ages of millions of ticks must not
        # collapse the frequency separation (log-space never underflows).
        store = FakeStore(
            _feat("ancient_hot", last_access=0.0, access_count=1000),
            _feat("ancient_cold", last_access=1000.0),
            _feat("fresh", last_access=10_000_000.0),
        )
        # log2(1001) ≈ 10 doublings vs the hot item's extra 1000 ticks of age
        # (≈2 half-lives at hl=512) — the cold one must still go first.
        assert SalienceDecayPolicy().select_victim(store) == "ancient_cold"

    def test_candidates_filter(self):
        store = FakeStore(_feat("worst", last_access=0.0), _feat("ok", last_access=50.0))
        assert SalienceDecayPolicy().select_victim(store, candidates={"ok"}) == "ok"

    def test_empty_store_raises(self):
        with pytest.raises(RuntimeError):
            SalienceDecayPolicy().select_victim(FakeStore())

    def test_invalid_half_life_raises(self):
        with pytest.raises(ValueError):
            SalienceDecayPolicy(half_life_ticks=0.0)


# ── GDSFPolicy ──────────────────────────────────────────────────────

class TestGDSFSelection:

    def test_evicts_lowest_density_and_inflates_L(self):
        policy = GDSFPolicy()
        store = FakeStore(
            _feat("cheap_dense", bytes_=10),          # H = 0 + 1/10
            _feat("big_sparse", bytes_=1000),         # H = 0 + 1/1000  ← victim
        )
        assert policy.select_victim(store) == "big_sparse"
        assert policy.L == pytest.approx(1 / 1000)

    def test_L_monotone_nondecreasing_across_evictions(self):
        policy = GDSFPolicy()
        remaining = [_feat(f"i{n}", bytes_=100 + n) for n in range(6)]
        prev_L = policy.L
        while len(remaining) > 1:
            victim = policy.select_victim(FakeStore(*remaining))
            remaining = [f for f in remaining if f.id != victim]
            assert policy.L >= prev_L, "inflation clock must never decrease"
            prev_L = policy.L
        assert prev_L > 0.0

    def test_inflation_ages_out_early_hammered_item(self):
        # Cache-pollution fix: an item with early frequency (count 5, credited
        # at L=0) must eventually lose to a fresh arrival credited after
        # inflation has raised L past its old credit.
        policy = GDSFPolicy()
        early_hot = MemRecord(id="early_hot", text="x" * 100, bytes=100, access_count=5)
        policy.on_insert(early_hot)                   # H = 0 + 6/100 = 0.06

        policy.L = 0.10                               # evictions elsewhere inflated L
        fresh = MemRecord(id="fresh", text="x" * 100, bytes=100, access_count=0)
        policy.on_insert(fresh)                       # H = 0.10 + 1/100 = 0.11

        store = FakeStore(
            _feat("early_hot", bytes_=100, access_count=5),
            _feat("fresh", bytes_=100),
        )
        assert policy.select_victim(store) == "early_hot"

    def test_freeze_L_is_frequency_density(self):
        policy = GDSFPolicy(freeze_L=True)
        assert policy.name == "gdsf_frozen"
        # (count+1)/bytes: a=1/100=0.01, b=6/100=0.06, c=1/10=0.1 → evict a
        store = FakeStore(_feat("a", bytes_=100), _feat("b", bytes_=100, access_count=5),
                          _feat("c", bytes_=10))
        assert policy.select_victim(store) == "a"
        assert policy.L == 0.0, "freeze_L must pin the inflation clock at 0"
        store2 = FakeStore(_feat("b", bytes_=100, access_count=5), _feat("c", bytes_=10))
        assert policy.select_victim(store2) == "b"
        assert policy.L == 0.0

    def test_backfill_uses_current_L(self):
        policy = GDSFPolicy()
        policy.L = 5.0
        store = FakeStore(_feat("unseen", bytes_=100), _feat("unseen2", bytes_=200))
        assert policy.select_victim(store) == "unseen2"   # lower density loses
        assert policy.L >= 5.0

    def test_empty_store_raises(self):
        with pytest.raises(RuntimeError):
            GDSFPolicy().select_victim(FakeStore())


class TestGDSFHooksThroughRetentionManager:
    """§4 assert: access must actually change H (hooks wired in the store)."""

    def _store(self, policy: GDSFPolicy, budget: int = 1 << 30) -> RetentionManager:
        store = RetentionManager(
            budget_bytes=budget,
            backend=InMemoryBackend(retriever=LexicalOverlapRetriever()),
            policy=policy, logger=BenchmarkLogger(),
        )
        store.connect()
        return store

    def test_insert_sets_credit(self):
        policy = GDSFPolicy()
        store = self._store(policy)
        store.insert(MemRecord(id="a", text="alpha beta", bytes=0))
        assert "a" in policy._H
        assert policy._H["a"] == pytest.approx(1 / max(store.backend.get("a").bytes, 1))

    def test_access_changes_H(self):
        policy = GDSFPolicy()
        store = self._store(policy)
        store.insert(MemRecord(id="a", text="alpha beta gamma", bytes=0))
        h_before = policy._H["a"]
        policy.L = 3.0                                 # simulate inflation
        hits = store.retrieve(Query(text="alpha beta gamma"), k=1)
        assert [h.id for h in hits] == ["a"]
        assert policy._H["a"] > h_before, "retrieval touch must re-credit H at current L"

    def test_eval_search_does_not_change_H(self):
        # The fragment-1 pairing: the read-only eval path must not re-credit.
        policy = GDSFPolicy()
        store = self._store(policy)
        store.insert(MemRecord(id="a", text="alpha beta gamma", bytes=0))
        h_before = policy._H["a"]
        policy.L = 3.0
        store.search(Query(text="alpha beta gamma"), k=1)
        assert policy._H["a"] == h_before, "eval search must not touch GDSF credit"

    def test_eviction_drops_bookkeeping_and_makes_progress(self):
        policy = GDSFPolicy()
        store = self._store(policy, budget=250)
        store.insert(MemRecord(id="a", text="x" * 100, bytes=100))
        store.insert(MemRecord(id="b", text="y" * 100, bytes=100))
        store.insert(MemRecord(id="c", text="z" * 100, bytes=100))  # forces eviction
        assert store.backend.count() == 2
        assert set(policy._H.keys()) == set(store.iter_ids()), "evicted item must leave _H"
