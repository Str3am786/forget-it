"""R8 enforcement: the eval path must be read-only, and must rank identically
to the mutating path.

Three invariants, each a separate test so a failure names its mechanism:

  1. ``search`` (the EVAL path) leaves every policy-visible counter untouched
     — asserted via RetentionManager.counter_state_hash over
     (id, access_count, last_access, bytes, tick, n_inserts).
  2. ``retrieve`` (the PROBE path) does mutate — the positive control that
     proves the hash is not vacuously constant.
  3. ``search`` and ``retrieve`` return the same ranking over the same
     resident set — the read-only path is the same retrieval, minus the
     side effects. (search runs FIRST, since retrieve perturbs state.)

NOTE ON FIXTURES: records/queries are built with the minimal constructor
below. If your MemRecord / Query signatures differ, swap _mk_record/_mk_query
for the factory your existing tests use — the assertions are what matter.
"""
from __future__ import annotations

import pytest

from forgetit.core import MemRecord, Query
from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.logging import BenchmarkLogger
from forgetit.memory_system import RetentionManager
from forgetit.retrieval import BM25Retriever
from forgetit.policies import LFUPolicy

K = 5

TEXTS = [
    "Alice adopted a golden retriever named Biscuit in March.",
    "Bob started a pottery class at the community centre.",
    "Alice and Bob discussed the pottery class over dinner.",
    "The community centre is closed on public holidays.",
    "Biscuit chewed through Bob's favourite pottery apron.",
    "Alice booked a vet appointment for Biscuit next Tuesday.",
    "Bob glazed his first bowl and gave it to Alice.",
    "The vet recommended a new diet for golden retrievers.",
]


def _mk_record(i: int, text: str) -> MemRecord:
    return MemRecord(id=f"it{i:03d}", text=text)


def _mk_query(qid: str, text: str) -> Query:
    return Query(id=qid, text=text)


@pytest.fixture()
def manager() -> RetentionManager:
    # LFU on purpose: it is the access-consuming policy the leak fed, so the
    # counters under test are exactly the ones that were contaminated.
    m = RetentionManager(
        budget_bytes=1 << 20,
        backend=InMemoryBackend(retriever=BM25Retriever()),
        policy=LFUPolicy(),
        logger=BenchmarkLogger(),
    )
    for i, t in enumerate(TEXTS):
        m.insert(_mk_record(i, t))
    return m


def test_search_is_read_only(manager: RetentionManager) -> None:
    """A scored query must leave every policy-visible signal untouched."""
    before = manager.counter_state_hash()
    for qid, qtext in [("q1", "who adopted a dog"),
                       ("q2", "pottery class community centre"),
                       ("q3", "vet appointment Tuesday")]:
        manager.search(_mk_query(qid, qtext), k=K)
    assert manager.counter_state_hash() == before, (
        "search() mutated policy-visible state — the eval path is feeding "
        "the access-consuming policies (R8 violation)."
    )


def test_search_leaves_every_field_untouched(manager: RetentionManager) -> None:
    """Field-level twin of the hash test: sharper failure message."""
    snap = {
        rid: (manager.get(rid).access_count, manager.get(rid).last_access)
        for rid in manager.iter_ids()
    }
    manager.search(_mk_query("q", "golden retriever diet"), k=K)
    for rid, (count, last) in snap.items():
        rec = manager.get(rid)
        assert rec.access_count == count, f"{rid}: access_count bumped by search()"
        assert rec.last_access == last, f"{rid}: last_access bumped by search()"


def test_retrieve_mutates(manager: RetentionManager) -> None:
    """Positive control: the probe path MUST write the usage signal.

    Without this, the read-only tests could pass vacuously (e.g. if counter
    updates were accidentally removed from BOTH paths, starving LRU/LFU)."""
    before = manager.counter_state_hash()
    hits = manager.retrieve(_mk_query("probe", "pottery class"), k=K)
    assert hits, "probe retrieved nothing — fixture broken"
    assert manager.counter_state_hash() != before, (
        "retrieve() no longer mutates — probes have stopped generating the "
        "access signal LRU/LFU consume."
    )
    assert all(h.access_count >= 1 for h in hits)


def test_search_and_retrieve_rank_identically(manager: RetentionManager) -> None:
    """The eval path is the same retrieval as the probe path, minus writes.

    search() runs first: retrieve() perturbs recency/frequency state, which
    may legitimately change subsequent rankings, but never this one."""
    for qtext in ["who adopted a dog", "pottery class", "vet appointment",
                  "public holidays", "glazed bowl"]:
        q_ro = _mk_query("ro::" + qtext, qtext)
        q_mut = _mk_query("mut::" + qtext, qtext)
        ro_ids = [r.id for r in manager.search(q_ro, k=K)]
        mut_ids = [r.id for r in manager.retrieve(q_mut, k=K)]
        assert ro_ids == mut_ids, (
            f"ranking diverged for {qtext!r}: search={ro_ids} retrieve={mut_ids}"
        )