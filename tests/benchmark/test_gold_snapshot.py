"""Tests for the gold answer-survival snapshot (_collect_gold_snapshot).

`gold_id_retained` is set intersection on identifiers: it says the gold item is
still resident, nothing about whether the text still carries the answer. A
policy that rewrites items in place therefore scores 1.000 by construction —
compression posted exactly that on locomo/session at every budget. The snapshot
is what separates the two, and it has two ordering constraints that are easy to
break and silent when broken:

  O_g must be captured AT INSERT. Compression reassigns record.text on the same
  object the insert event holds, so reading "the original" after replay returns
  the compressed text and every answer looks perfectly retained — the exact
  blindness the metric removes.

  R_g must be read AT FINAL STORE STATE, before disposal. Any earlier and it is
  pre-compression text, same failure.

The eviction-only invariant (R_g == O_g on every retained row) is asserted here
AND at merge time in scripts/concat_results.py: once before the run, once on
data anyone else produces.
"""
from __future__ import annotations

import pytest

from forgetit.backend.in_memory import InMemoryBackend
from forgetit.benchmark.runner_core import run_one_example
from forgetit.benchmark.schema import WorkloadEvent
from forgetit.core.logging import BenchmarkLogger
from forgetit.core.schema import MemRecord, Query
from forgetit.memory_system.custom_manager import RetentionManager
from forgetit.policies.actions import Compress, Evict
from forgetit.policies.base import EvictionPolicy
from forgetit.policies.baselines import FIFOPolicy
from forgetit.retrieval.lexical import LexicalOverlapRetriever

ORIGINAL = "the passport number is ZX12345 and it expires in June 2030"
SHRUNK = "passport details"


class ShrinkPolicy(EvictionPolicy):
    """Deterministic stand-in for llm_compression: rewrites in place, no LLM.

    Compresses the oldest not-yet-compressed item; once everything is shrunk and
    the store is still over budget it evicts, so the budget loop always makes
    progress.
    """
    name = "shrink"

    def __init__(self, new_text: str = SHRUNK) -> None:
        self.new_text = new_text
        self.seen: set[str] = set()

    def select_action(self, store):
        feats = sorted(store.iter_features(), key=lambda f: (f.created_at or 0.0, f.id))
        for f in feats:
            if f.id not in self.seen:
                self.seen.add(f.id)
                self._note_decision(f.created_at or 0.0, len(feats))
                return Compress(id=f.id, new_text=self.new_text)
        self._note_decision(feats[0].created_at or 0.0, len(feats))
        return Evict(id=feats[0].id)


def _events(n_filler: int = 3):
    """One gold item inserted first, then filler, then one scored query."""
    evs = [WorkloadEvent(
        type="insert", timestamp=1.0, query=None, gold_item_ids=[],
        item=MemRecord(id="gold", text=ORIGINAL, bytes=len(ORIGINAL), created_at=1.0))]
    for i in range(n_filler):
        text = "filler " * 20
        evs.append(WorkloadEvent(
            type="insert", timestamp=2.0 + i, query=None, gold_item_ids=[],
            item=MemRecord(id=f"d{i}", text=text, bytes=len(text), created_at=2.0 + i)))
    evs.append(WorkloadEvent(
        type="query", timestamp=9.0, item=None, query=Query(text="passport number"),
        gold_item_ids=["gold"], ref_answer="ZX12345", question_type="single_hop"))
    return evs


def _store(policy, budget: int) -> RetentionManager:
    return RetentionManager(
        budget_bytes=budget,
        backend=InMemoryBackend(retriever=LexicalOverlapRetriever()),
        policy=policy, logger=BenchmarkLogger(),
    )


def _snapshot(policy, budget: int, events=None) -> list[dict]:
    _, _, _, gold_rows = run_one_example(
        events or _events(), _store(policy, budget), k=5,
        dataset="unit", store_label="snap", example_id="ex1",
        collect_gold_snapshot=True,
    )
    return gold_rows


class TestSnapshotShape:

    def test_one_row_per_query_gold_pair(self):
        rows = _snapshot(FIFOPolicy(), budget=10_000)
        assert len(rows) == 1
        r = rows[0]
        assert (r["example_id"], r["gold_id"]) == ("ex1", "gold")
        assert r["answer"] == "ZX12345"
        assert r["question_type"] == "single_hop"

    def test_not_collected_unless_requested(self):
        _, _, _, gold_rows = run_one_example(
            _events(), _store(FIFOPolicy(), 10_000), k=5,
            dataset="unit", store_label="snap", example_id="ex1",
        )
        assert gold_rows == []


class TestEvictionOnlyPolicies:
    """These never touch text, so the snapshot must be able to prove it."""

    def test_retained_gold_returns_original_text_unchanged(self):
        r = _snapshot(FIFOPolicy(), budget=10_000)[0]
        assert r["was_evicted"] is False
        assert r["was_compressed"] is False
        assert r["resident_text"] == r["original_text"] == ORIGINAL

    def test_evicted_gold_gives_empty_resident_text(self):
        """FIFO evicts oldest-first and gold is inserted first, so a budget that
        holds one filler item drops it. Eviction loss and compression loss then
        unify: both score zero on any content test."""
        r = _snapshot(FIFOPolicy(), budget=200)[0]
        assert r["was_evicted"] is True
        assert r["resident_text"] == ""
        assert r["resident_bytes"] == 0
        assert r["original_text"] == ORIGINAL, "O_g survives the eviction of R_g"


class TestCompressionCapture:
    """The ordering constraints. Both failures are silent."""

    def test_original_text_is_pre_compression(self):
        """THE CANARY. Compression reassigns record.text on the object the
        insert event holds. If O_g is read back after replay instead of
        captured at insert, this returns SHRUNK and every answer looks
        retained."""
        r = _snapshot(ShrinkPolicy(), budget=300)[0]
        assert r["original_text"] == ORIGINAL, (
            "O_g is post-compression — it is being read back from the store "
            "instead of captured at insert time")

    def test_resident_text_is_post_compression(self):
        r = _snapshot(ShrinkPolicy(), budget=300)[0]
        assert r["resident_text"] == SHRUNK, (
            "R_g is pre-compression — the snapshot is not reading final state")
        assert r["was_evicted"] is False

    def test_answer_is_lost_while_the_id_is_retained(self):
        """The whole point: identifier retention says survived, content says
        otherwise. Without this separation compression scores 1.000 on a
        metric named survival."""
        r = _snapshot(ShrinkPolicy(), budget=300)[0]
        assert r["was_evicted"] is False              # gold_id_retained is True
        assert "ZX12345" in r["original_text"]
        assert "ZX12345" not in r["resident_text"]

    def test_byte_counts_track_the_rewrite(self):
        r = _snapshot(ShrinkPolicy(), budget=300)[0]
        assert r["orig_bytes"] == len(ORIGINAL)
        assert r["resident_bytes"] < r["orig_bytes"]

    def test_compressed_flag_is_set(self):
        """Depends on RetentionManager stamping meta['compressed'] on a
        successful compress. If this is the only failure, the flag is the bug,
        not the snapshot."""
        r = _snapshot(ShrinkPolicy(), budget=300)[0]
        assert r["was_compressed"] is True


class TestMultiGoldAndRepeatedQueries:

    def test_one_row_per_pair_when_two_queries_share_gold(self):
        """Rows are per (query, gold-id), so a shared item appears once per
        query. The analysis collapses to the query before comparing against
        gold_id_retained; conflating the two scopes reads a surviving
        non-extractive item as a preservation failure."""
        evs = _events()
        evs.append(WorkloadEvent(
            type="query", timestamp=10.0, item=None, query=Query(text="expiry"),
            gold_item_ids=["gold"], ref_answer="June 2030", question_type="temporal"))
        rows = _snapshot(FIFOPolicy(), budget=10_000, events=evs)
        assert len(rows) == 2
        assert {r["gold_id"] for r in rows} == {"gold"}
        assert {r["answer"] for r in rows} == {"ZX12345", "June 2030"}

    def test_unscored_probes_produce_no_rows(self):
        evs = _events()
        evs.insert(-1, WorkloadEvent(
            type="query", timestamp=8.0, item=None,
            query=Query(text="probe"), gold_item_ids=[]))
        assert len(_snapshot(FIFOPolicy(), budget=10_000, events=evs)) == 1