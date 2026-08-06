"""
Tests for OraclePolicy — the §4 assert "oracle never evicts gold", the
forced-violation fallback, and the runner's gold-id injection contract
(wants_gold_ids → run_one_example fills protected_ids; the streaming run()
refuses rather than running an unprotected oracle).
"""
from __future__ import annotations

import pytest

from forgetit.backend.in_memory import InMemoryBackend
from forgetit.benchmark.runner_core import run, run_one_example
from forgetit.benchmark.schema import WorkloadEvent
from forgetit.core.logging import BenchmarkLogger
from forgetit.core.schema import MemFeatures, MemRecord, Query
from forgetit.memory_system.custom_manager import RetentionManager
from forgetit.policies.oracle import OraclePolicy
from forgetit.retrieval.lexical import LexicalOverlapRetriever


def _feat(id_: str, created_at: float) -> MemFeatures:
    return MemFeatures(id=id_, bytes=100, last_access=0.0, access_count=0,
                       created_at=created_at)


class FakeStore:
    def __init__(self, *features: MemFeatures):
        self._features = list(features)

    def iter_features(self):
        return iter(self._features)


class TestOracleSelection:

    def test_never_picks_protected_while_others_remain(self):
        policy = OraclePolicy()
        policy.protected_ids = {"gold"}
        store = FakeStore(_feat("gold", 1.0), _feat("later", 5.0), _feat("latest", 9.0))
        # gold is the OLDEST — FIFO would take it; the oracle must not.
        assert policy.select_victim(store) == "later"
        assert policy.forced_gold_evictions == 0

    def test_forced_to_evict_gold_only_when_nothing_else_left(self):
        policy = OraclePolicy()
        policy.protected_ids = {"g1", "g2"}
        store = FakeStore(_feat("g1", 1.0), _feat("g2", 2.0))
        assert policy.select_victim(store) == "g1"     # oldest protected
        assert policy.forced_gold_evictions == 1

    def test_candidates_filter_respected(self):
        policy = OraclePolicy()
        policy.protected_ids = {"gold"}
        store = FakeStore(_feat("gold", 1.0), _feat("a", 2.0), _feat("b", 3.0))
        assert policy.select_victim(store, candidates={"b", "gold"}) == "b"

    def test_unprotected_ordering_is_fifo(self):
        policy = OraclePolicy()          # nothing protected
        store = FakeStore(_feat("b", 2.0), _feat("a", 1.0))
        assert policy.select_victim(store) == "a"

    def test_records_decision_provenance(self):
        policy = OraclePolicy()
        policy.protected_ids = {"gold"}
        store = FakeStore(_feat("gold", 1.0), _feat("a", 2.0))
        policy.select_victim(store)
        assert policy.last_decision == {"policy_score": 2.0, "candidate_count": 2}

    def test_empty_store_raises(self):
        with pytest.raises(RuntimeError):
            OraclePolicy().select_victim(FakeStore())


def _events():
    def rec(id_: str, ts: float) -> MemRecord:
        return MemRecord(id=id_, text="x" * 100, bytes=100, created_at=ts)

    return [
        WorkloadEvent(type="insert", timestamp=1.0, item=rec("gold", 1.0),
                      query=None, gold_item_ids=[]),
        WorkloadEvent(type="insert", timestamp=2.0, item=rec("d1", 2.0),
                      query=None, gold_item_ids=[]),
        WorkloadEvent(type="insert", timestamp=3.0, item=rec("d2", 3.0),
                      query=None, gold_item_ids=[]),
        WorkloadEvent(type="insert", timestamp=4.0, item=rec("d3", 4.0),
                      query=None, gold_item_ids=[]),
        WorkloadEvent(type="query", timestamp=5.0, item=None,
                      query=Query(text="anything"), gold_item_ids=["gold"]),
    ]


def _store(budget: int = 250) -> RetentionManager:
    return RetentionManager(
        budget_bytes=budget,   # holds 2 of 4 items → 2 evictions
        backend=InMemoryBackend(retriever=LexicalOverlapRetriever()),
        policy=OraclePolicy(), logger=BenchmarkLogger(),
    )


class TestOracleThroughRunner:

    def test_runner_injects_gold_and_oracle_protects_it(self):
        store = _store()
        results, *_ = run_one_example(
            _events(), store, k=5, dataset="unit", store_label="oracle_test",
        )
        assert store.policy.protected_ids == {"gold"}, "runner must inject gold ids"
        assert "gold" in set(store.iter_ids()), "oracle must keep the OLDEST item alive"
        assert store.policy.forced_gold_evictions == 0
        assert len(results) == 1
        assert results[0].gold_id_retained is True

    def test_streaming_run_refuses_wants_gold_ids(self):
        class OneShotWorkload:
            def events(self):
                return iter(_events())

        with pytest.raises(ValueError, match="wants_gold_ids"):
            run(workload=OneShotWorkload(), store=_store(), k=5,
                dataset="unit", store_label="oracle_test")

class TestOracleSacrificeOrder:
    """When every candidate is protected the oracle must choose WHICH answers to
    lose, not fall back on age.

    The defect this guards: ordering the protected branch by (created_at, id)
    makes it the FIFO rule over the same candidate set, so a saturated oracle
    produced fifo's exact victim sequence and its exact final byte count. It
    stopped being a ceiling and nothing noticed — locomo/session saturates on
    100% of examples, beam_batch on 77%.
    """

    @staticmethod
    def _fifo_victim(store) -> str:
        """FIFO, computed independently, so the canary compares against the
        real rule rather than an assumption about it."""
        return min((f.created_at or 0.0, f.id) for f in store.iter_features())[1]

    def test_saturated_does_not_reproduce_fifo(self):
        policy = OraclePolicy()
        policy.protected_ids = {"old_many", "mid", "new_one"}
        policy.gold_counts = {"old_many": 9, "mid": 4, "new_one": 1}
        store = FakeStore(_feat("old_many", 1.0), _feat("mid", 2.0),
                          _feat("new_one", 3.0))

        victim = policy.select_victim(store)
        assert victim == "new_one", "must sacrifice the fewest questions"
        assert victim != self._fifo_victim(store), (
            "the oracle chose fifo's victim under saturation — the protected "
            "branch is ordered by age again")
        assert policy.forced_gold_evictions == 1

    def test_stake_ties_fall_back_to_age(self):
        policy = OraclePolicy()
        policy.protected_ids = {"newer", "older"}
        policy.gold_counts = {"newer": 3, "older": 3}
        store = FakeStore(_feat("newer", 9.0), _feat("older", 1.0))
        assert policy.select_victim(store) == "older"

    def test_forced_decision_is_marked_per_decision(self):
        """A forced sacrifice must be identifiable in the event log row, not
        only as a run total. policy_score stays in age units on both branches
        so the column keeps one meaning."""
        policy = OraclePolicy()
        policy.protected_ids = {"gold"}
        policy.gold_counts = {"gold": 7}

        policy.select_victim(FakeStore(_feat("gold", 1.0), _feat("junk", 4.0)))
        assert "forced_gold" not in policy.last_decision

        policy.select_victim(FakeStore(_feat("gold", 1.0)))
        assert policy.last_decision["forced_gold"] is True
        assert policy.last_decision["questions_at_stake"] == 7
        assert policy.last_decision["policy_score"] == 1.0

    def test_counter_counts_only_compelled_sacrifices(self):
        policy = OraclePolicy()
        policy.protected_ids = {"gold"}
        policy.gold_counts = {"gold": 2}

        policy.select_victim(FakeStore(_feat("gold", 1.0), _feat("junk", 2.0)))
        assert policy.forced_gold_evictions == 0, "counted a choice as compelled"

        policy.select_victim(FakeStore(_feat("gold", 1.0)))
        policy.select_victim(FakeStore(_feat("gold", 1.0)))
        assert policy.forced_gold_evictions == 2

    def test_protected_without_counts_still_protected(self):
        """gold_counts missing an id must not except — the item is protected
        with a default stake of one."""
        policy = OraclePolicy()
        policy.protected_ids = {"gold"}      # counts deliberately not injected
        assert policy.select_victim(
            FakeStore(_feat("gold", 1.0), _feat("junk", 2.0))) == "junk"
        assert policy.select_victim(FakeStore(_feat("gold", 1.0))) == "gold"


class TestOracleInjectionContract:
    """run_one_example owns protected_ids AND gold_counts AND the reset."""

    def test_runner_injects_counts_not_just_ids(self):
        events = _events()
        events.append(WorkloadEvent(
            type="query", timestamp=6.0, item=None,
            query=Query(text="again"), gold_item_ids=["gold"]))

        store = _store()
        run_one_example(events, store, k=5, dataset="unit", store_label="oracle_test")

        assert store.policy.protected_ids == {"gold"}
        assert store.policy.gold_counts == {"gold": 2}, (
            "the stake is the number of scored queries naming the item; "
            "without it the sacrifice branch cannot rank")

    def test_counter_resets_between_examples(self):
        """The store factory builds per config, so a reused policy instance
        would carry the previous example's count and make every downstream
        'was it forced' test pass vacuously."""
        store = _store(budget=150)          # 1 item fits, 4 inserted
        run_one_example(_events(), store, k=5, dataset="unit", store_label="t")
        first = store.policy.forced_gold_evictions

        run_one_example(_events(), store, k=5, dataset="unit", store_label="t")
        assert store.policy.forced_gold_evictions <= first, (
            "forced_gold_evictions accumulated across examples")

    def test_forced_count_reaches_the_result_row(self):
        """Counted-but-never-persisted is how this went undetected for a whole
        campaign, so assert the column exists and carries the value."""
        store = _store()
        results, *_ = run_one_example(
            _events(), store, k=5, dataset="unit", store_label="t")
        assert results[0].oracle_forced_gold_evictions == \
            store.policy.forced_gold_evictions