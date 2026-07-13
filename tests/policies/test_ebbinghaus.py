"""
Tests for EbbinghausPolicy (MemoryBank forgetting-curve eviction).

The policy ranks items by log-retention -dt/S where dt is age since
created_at and S = base_strength + strength_growth * access_count.
Ranking in log space is order-identical to the paper's exp(-dt/S) but
cannot underflow, so the rehearsal signal keeps separating items no
matter how stale they are (the regression test below fails on the
exp-based implementation).

Pure select_victim tests use the FakeStore pattern from test_lru.py;
behavioral tests drive a real RetentionManager + InMemoryBackend; the
last group exercises EbbinghausPolicy as the MemoryBankCompressionPolicy
selector (candidates filtering under the two-phase compress/evict loop).
"""
from __future__ import annotations

import pytest

from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.logging import BenchmarkLogger
from forgetit.core.schema import MemFeatures, MemRecord, Query
from forgetit.memory_system.custom_manager import RetentionManager
from forgetit.policies.actions import Compress, Evict
from forgetit.policies.ebbinghaus import EbbinghausPolicy, MemoryBankCompressionPolicy
from forgetit.retrieval.lexical import LexicalOverlapRetriever


DAY = 86_400.0
T0 = 1_600_000_000.0  # arbitrary "now" anchor (2020-09-13), well past epoch


def _feat(id_: str, days_ago: float, access_count: int = 0) -> MemFeatures:
    return MemFeatures(
        id=id_,
        bytes=100,
        last_access=0.0,
        access_count=access_count,
        created_at=T0 - days_ago * DAY,
    )


class FakeStore:
    def __init__(self, *features: MemFeatures):
        self._features = list(features)

    def iter_features(self):
        return iter(self._features)


# ── pure select_victim ───────────────────────────────────────────────

class TestSelectVictim:

    def test_evicts_oldest_when_all_cold(self):
        store = FakeStore(_feat("old", 90), _feat("mid", 30), _feat("new", 0))
        assert EbbinghausPolicy().select_victim(store) == "old"

    def test_rehearsal_protects_old_item(self):
        # old is 90 days stale but rehearsed 50x → dt/S = 90/51 ≈ 1.8
        # mid is 30 days stale and cold        → dt/S = 30/1  = 30
        store = FakeStore(
            _feat("old_rehearsed", 90, access_count=50),
            _feat("mid_cold", 30),
            _feat("new", 0),
        )
        assert EbbinghausPolicy().select_victim(store) == "mid_cold"

    def test_newest_item_has_max_retention(self):
        # dt = 0 → log-retention 0, the maximum; never the victim while
        # anything staler is resident.
        store = FakeStore(_feat("new", 0), _feat("older", 1))
        assert EbbinghausPolicy().select_victim(store) == "older"

    def test_underflow_regression_strength_separates_very_old_items(self):
        # Both items are stale enough that exp(-dt/S) underflows to exactly
        # 0.0 (dt/S > ~745): A at 3000d with 3 accesses (dt/S = 750), B at
        # 2900d cold (dt/S = 2900). The exp-based implementation tied them
        # at retention 0.0 and fell through to the created_at tiebreak,
        # evicting A (older) despite its rehearsal. Log-space ranking keeps
        # -750 > -2900 and correctly evicts B.
        store = FakeStore(
            _feat("A_rehearsed", 3000, access_count=3),
            _feat("B_cold", 2900),
            _feat("anchor_now", 0),
        )
        assert EbbinghausPolicy().select_victim(store) == "B_cold"

    def test_tie_breaks_by_created_at_then_id(self):
        # Same age, same strength → same score; older created_at wins the
        # first tiebreak, id the second.
        store = FakeStore(_feat("b", 10), _feat("a", 10), _feat("new", 0))
        assert EbbinghausPolicy().select_victim(store) == "a"

    def test_candidates_filter_restricts_victim_set(self):
        store = FakeStore(_feat("worst", 90), _feat("better", 30), _feat("new", 0))
        victim = EbbinghausPolicy().select_victim(store, candidates={"better"})
        assert victim == "better"

    def test_now_is_computed_over_all_items_not_just_candidates(self):
        # The newest item defines "now" even when it is not a candidate.
        # A: 100d stale, 9 accesses → dt/S = 100/10 = 10
        # B:  50d stale, cold       → dt/S =  50/1  = 50  → victim
        # If "now" were wrongly taken over candidates only (= B's ts),
        # A would score 50/10 = 5 vs B's 0 and A would be evicted.
        store = FakeStore(
            _feat("A", 100, access_count=9),
            _feat("B", 50),
            _feat("newest_non_candidate", 0),
        )
        victim = EbbinghausPolicy().select_victim(store, candidates={"A", "B"})
        assert victim == "B"

    def test_missing_created_at_treated_as_infinitely_stale(self):
        none_ts = MemFeatures(id="unknown_age", bytes=100, last_access=0.0,
                              access_count=0, created_at=None)
        store = FakeStore(none_ts, _feat("dated", 10))
        assert EbbinghausPolicy().select_victim(store) == "unknown_age"

    def test_empty_store_raises(self):
        with pytest.raises(RuntimeError):
            EbbinghausPolicy().select_victim(FakeStore())

    def test_empty_candidate_set_raises(self):
        store = FakeStore(_feat("a", 1))
        with pytest.raises(RuntimeError):
            EbbinghausPolicy().select_victim(store, candidates=set())

    def test_select_action_wraps_victim_in_evict(self):
        store = FakeStore(_feat("old", 90), _feat("new", 0))
        action = EbbinghausPolicy().select_action(store)
        assert action == Evict(id="old")


class TestParameters:

    def test_time_unit_does_not_change_the_victim(self):
        # Ordering is by dt/S; a global rescale of dt cannot reorder items.
        store = FakeStore(
            _feat("old_rehearsed", 90, access_count=50),
            _feat("mid_cold", 30),
            _feat("new", 0),
        )
        for unit in (1.0, 3600.0, DAY, 7 * DAY):
            assert EbbinghausPolicy(time_unit_seconds=unit).select_victim(store) == "mid_cold"

    def test_zero_strength_growth_ignores_rehearsal(self):
        # With growth 0 every item has strength base → pure age ordering.
        store = FakeStore(
            _feat("old_rehearsed", 90, access_count=50),
            _feat("mid_cold", 30),
            _feat("new", 0),
        )
        policy = EbbinghausPolicy(strength_growth=0.0)
        assert policy.select_victim(store) == "old_rehearsed"

    def test_invalid_parameters_raise(self):
        with pytest.raises(ValueError):
            EbbinghausPolicy(base_strength=0.0)
        with pytest.raises(ValueError):
            EbbinghausPolicy(base_strength=-1.0)
        with pytest.raises(ValueError):
            EbbinghausPolicy(strength_growth=-0.5)
        with pytest.raises(ValueError):
            EbbinghausPolicy(time_unit_seconds=0.0)


# ── behavioral: real RetentionManager + InMemoryBackend ─────────────

def _make_store(budget: int) -> RetentionManager:
    backend = InMemoryBackend(retriever=LexicalOverlapRetriever())
    store = RetentionManager(
        budget_bytes=budget, backend=backend,
        policy=EbbinghausPolicy(), logger=BenchmarkLogger(),
    )
    store.connect()
    return store


def _rec(id_: str, days_ago: float, text: str = "", nbytes: int = 100) -> MemRecord:
    text = text or ("x" * nbytes)
    rec = MemRecord(id=id_, text=text, created_at=T0 - days_ago * DAY)
    rec.bytes = len(text.encode("utf-8"))
    return rec


class TestRetentionManagerIntegration:

    def test_insert_over_budget_evicts_oldest_cold_item(self):
        store = _make_store(budget=350)
        store.insert(_rec("old", 90))
        store.insert(_rec("mid", 30))
        store.insert(_rec("new", 10))
        store.insert(_rec("newest", 0))  # 4 × 100B > 350B → one eviction
        assert sorted(store.iter_ids()) == ["mid", "new", "newest"]

    def test_retrieval_rehearsal_protects_item_from_eviction(self):
        store = _make_store(budget=350)
        store.insert(_rec("old", 90, text="alpha beta gamma " * 6))   # distinctive tokens
        store.insert(_rec("mid", 30))
        store.insert(_rec("new", 10))

        # 20 retrieval touches → access_count 20 → strength 21:
        # old scores 90/21 ≈ 4.3 vs cold mid's 30/1 = 30 → mid is evicted.
        for _ in range(20):
            hits = store.retrieve(Query(text="alpha beta gamma"), k=1)
            assert [h.id for h in hits] == ["old"]

        store.insert(_rec("newest", 0))
        assert sorted(store.iter_ids()) == ["new", "newest", "old"]


# ── MemoryBankCompressionPolicy with the Ebbinghaus selector ────────

class StubLLM:
    """Returns scripted outputs from a queue (pattern from test_compression_2)."""

    def __init__(self, outputs):
        self.outputs = list(outputs)
        self.call_count = 0

    def generate(self, prompt, *, max_tokens=128, temperature=0.0):
        self.call_count += 1
        if not self.outputs:
            raise RuntimeError("StubLLM exhausted — test asked for too many calls")
        return self.outputs.pop(0)


def _mb_store(budget: int, policy) -> RetentionManager:
    backend = InMemoryBackend(retriever=LexicalOverlapRetriever())
    store = RetentionManager(
        budget_bytes=budget, backend=backend, policy=policy, logger=BenchmarkLogger(),
    )
    store.connect()
    return store


def _add(store: RetentionManager, rec: MemRecord) -> None:
    """Place a record directly into store + accounting (bypasses _ensure_budget)."""
    store.backend.upsert(rec)
    store._bytes_by_id[rec.id] = rec.bytes
    store.used_bytes += rec.bytes


class TestMemoryBankWithEbbinghausSelector:

    def test_compress_phase_picks_lowest_retention_uncompressed(self):
        llm = StubLLM(["Old summary."])
        policy = MemoryBankCompressionPolicy(
            llm=llm, selector=EbbinghausPolicy(), min_compress_bytes=100,
        )
        store = _mb_store(10_000, policy)
        _add(store, _rec("old", 90, text="Lorem ipsum dolor sit amet. " * 20))
        _add(store, _rec("new", 0, text="Recens textus novus est hic. " * 20))

        action = policy.select_action(store)
        assert action == Compress(id="old", new_text="Old summary.")

    def test_compressed_items_leave_compress_candidates(self):
        # "old" is already compressed → the compress phase must skip it and
        # summarise the remaining uncompressed item, even though old has the
        # lower retention overall.
        llm = StubLLM(["New summary."])
        policy = MemoryBankCompressionPolicy(
            llm=llm, selector=EbbinghausPolicy(), min_compress_bytes=100,
        )
        store = _mb_store(10_000, policy)
        old = _rec("old", 90, text="Lorem ipsum dolor sit amet. " * 20)
        old.meta["compressed"] = True
        _add(store, old)
        _add(store, _rec("new", 0, text="Recens textus novus est hic. " * 20))

        action = policy.select_action(store)
        assert action == Compress(id="new", new_text="New summary.")

    def test_all_compressed_falls_back_to_ebbinghaus_evict(self):
        llm = StubLLM([])  # must not be called
        policy = MemoryBankCompressionPolicy(llm=llm, selector=EbbinghausPolicy())
        store = _mb_store(10_000, policy)
        for id_, days in (("old", 90), ("mid", 30), ("new", 0)):
            rec = _rec(id_, days, text=f"summary of {id_}")
            rec.meta["compressed"] = True
            _add(store, rec)

        action = policy.select_action(store)
        assert action == Evict(id="old")
        assert llm.call_count == 0

    def test_full_budget_loop_compresses_then_survives(self):
        # Drive the real _ensure_budget loop: three fat records at a budget
        # that forces pressure; scripted summaries shrink them in place.
        llm = StubLLM(["S1.", "S2.", "S3."])
        policy = MemoryBankCompressionPolicy(
            llm=llm, selector=EbbinghausPolicy(),
            min_compress_bytes=100, min_saving_ratio=0.5,
        )
        store = _mb_store(1_500, policy)
        store.insert(_rec("a", 90, text="Lorem ipsum dolor sit amet. " * 25))  # 700B
        store.insert(_rec("b", 30, text="Consectetur adipiscing elit. " * 25))  # 725B
        store.insert(_rec("c", 0, text="Sed do eiusmod tempor incidunt. " * 25))  # 800B

        resident = sorted(store.iter_ids())
        assert store.used_bytes <= 1_500
        # Compression (not eviction) should have absorbed the pressure:
        # everything is still resident, oldest items now summarised.
        assert resident == ["a", "b", "c"]
        assert store.backend.get("a").meta.get("compressed") is True
