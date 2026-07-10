"""D1: FIFO as a first-class policy + the equivalence property (VERIFICATION.md S3).

Three claims, each a permanent regression test:

1. FIFO evicts in INSERTION order, ignoring touches AND ignoring data
   timestamps. (`created_at` is a data-clock — LongMemEval session dates are
   non-monotonic across the insert stream — so FIFO keyed on created_at is
   "oldest-data-timestamp", a different policy. The store's insert tick is the
   only honest FIFO clock.)
2. `fifo` is registered in both experiment runners' default policy specs.
3. Equivalence property: on BEAM-shaped traces (access-only probes interleave
   with inserts; scored queries fire after the last insert), the eviction
   sequences of `fifo`, `lru --probe-touch-mode probes_blind`, and
   `lfu --probe-touch-mode probes_blind` are byte-identical — the E3 finding
   as a property test. A negative control asserts `lru` under `original`
   touches DOES diverge, proving the test can detect divergence.

The property's scope condition (why "BEAM-shaped"): probes_blind still lets
SCORED queries touch, so equivalence only holds when no eviction pressure
follows a touch. Both real workloads satisfy this (scored queries fire at an
example's end); the generator reproduces that shape and the real-trace replay
(scripts/replay_equivalence.py) asserts it on real events.
"""
from __future__ import annotations

import random

import pytest

from forgetit.policies import FIFOPolicy, LRUPolicy, LFUPolicy
from forgetit.memory_system import RetentionManager
from forgetit.backend.in_memory import InMemoryBackend
from forgetit.retrieval.lexical import LexicalOverlapRetriever
from forgetit.core.schema import MemRecord, Query


class RecordingLogger:
    def __init__(self):
        self.events = []

    def event(self, name, payload=None):
        self.events.append((name, dict(payload or {})))

    def evictions(self):
        return [p["id"] for n, p in self.events if n == "evict"]


def make_store(policy, *, budget, mode="original"):
    logger = RecordingLogger()
    store = RetentionManager(
        budget_bytes=budget,
        backend=InMemoryBackend(retriever=LexicalOverlapRetriever()),
        policy=policy,
        logger=logger,
        probe_touch_mode=mode,
    )
    store.connect()
    return store, logger


def rec(item_id, token, *, bytes_=40, created_at=1.0):
    return MemRecord(id=item_id, text=f"{token} filler", bytes=bytes_,
                     created_at=created_at)


# ── 1. FIFO = insertion order, not data-timestamp order ─────────────


def test_fifo_evicts_in_insertion_order_despite_nonmonotonic_created_at():
    """LongMemEval shape: data timestamps disagree with insertion order.
    First-inserted (A) must be the victim, even though B carries the oldest
    created_at. Fails when FIFO is keyed on created_at."""
    store, logger = make_store(FIFOPolicy(), budget=100)
    store.insert(rec("A", "alpha", created_at=500.0))   # inserted first, NEWEST data ts... no —
    store.insert(rec("B", "beta", created_at=100.0))    # oldest data ts, inserted second
    store.insert(rec("C", "gamma", created_at=300.0))   # forces one eviction
    assert logger.evictions() == ["A"], (
        f"FIFO evicted {logger.evictions()} — data-timestamp order, not insertion order"
    )


def test_fifo_ignores_touches():
    """e006 unit-scale: A touched between inserts must still be first victim."""
    store, logger = make_store(FIFOPolicy(), budget=100)
    store.insert(rec("A", "alpha"))
    store.insert(rec("B", "beta"))
    store.retrieve(Query(id="q1", text="alpha", timestamp=2.0, meta={}), k=1)  # touch A
    store.insert(rec("C", "gamma"))
    store.insert(rec("D", "delta"))
    assert logger.evictions() == ["A", "B"]


# ── 2. fifo registered in both runners ───────────────────────────────


@pytest.mark.parametrize("runner_module", [
    "forgetit.experiments.run_beam",
    "forgetit.experiments.run_longmemeval_qa",
])
def test_fifo_registered_in_runner_policy_specs(runner_module):
    import importlib
    mod = importlib.import_module(runner_module)
    names = [name for name, _ in mod.default_policy_specs(llm=None)]
    assert "fifo" in names, f"{runner_module}: fifo missing from {names}"


# ── 3. the equivalence property ──────────────────────────────────────


def beam_shaped_trace(seed: int, n_items: int = 60):
    """Deterministic trace in the BEAM shape: insert → probe after every
    insert (query for a random earlier item's unique token), scored queries
    only at the very end. Sizes vary; created_at is deliberately shuffled so
    equivalence cannot be an accident of monotone data timestamps."""
    rng = random.Random(seed)
    created = list(range(1, n_items + 1))
    rng.shuffle(created)
    events = []          # ("insert", record) | ("probe", query) | ("scored", query)
    tokens = []
    for i in range(n_items):
        tok = f"tok{i:03d}"
        tokens.append(tok)
        events.append(("insert", rec(f"i{i:03d}", tok, bytes_=rng.randint(20, 80),
                                     created_at=float(created[i]))))
        target = rng.choice(tokens)
        events.append(("probe", Query(id=f"e::probe::{i}", text=target,
                                      timestamp=float(i), meta={"probe": True})))
    for j in range(5):
        target = rng.choice(tokens)
        events.append(("scored", Query(id=f"e::q::{j}", text=target,
                                       timestamp=float(n_items + j), meta={})))
    return events


def replay(policy, mode, events, *, budget=600, k=3):
    store, logger = make_store(policy, budget=budget, mode=mode)
    for kind, payload in events:
        if kind == "insert":
            # fresh copy: replays must not share mutable records
            store.insert(MemRecord(id=payload.id, text=payload.text,
                                   bytes=payload.bytes, created_at=payload.created_at))
        else:
            store.retrieve(payload, k=k)
    return logger.evictions()


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_fifo_blind_lru_blind_lfu_identical_eviction_sequences(seed):
    events = beam_shaped_trace(seed)
    fifo_orig = replay(FIFOPolicy(), "original", events)
    fifo_blind = replay(FIFOPolicy(), "probes_blind", events)
    lru_blind = replay(LRUPolicy(), "probes_blind", events)
    lfu_blind = replay(LFUPolicy(), "probes_blind", events)

    assert len(fifo_orig) > 10, "trace produced too little pressure to be meaningful"
    # fifo is touch-blind by construction: mode must not matter.
    assert fifo_orig == fifo_blind
    assert fifo_blind == lru_blind, "FIFO != blind-LRU"
    assert lru_blind == lfu_blind, "blind-LRU != blind-LFU (tie-break drift?)"


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_negative_control_original_lru_diverges(seed):
    """Probe touches must change LRU's eviction order somewhere in the trace —
    otherwise the equivalence test above is vacuously green."""
    events = beam_shaped_trace(seed)
    lru_orig = replay(LRUPolicy(), "original", events)
    lru_blind = replay(LRUPolicy(), "probes_blind", events)
    assert lru_orig != lru_blind


# ── same property on a REAL recorded trace (capped for test time) ────
# The FULL trace is covered by scripts/replay_equivalence.py (35/35 examples,
# 14,444 evictions/policy, byte-identical — report/01_fifo_equivalence.md).

from pathlib import Path

_REAL_EVENTS = (Path(__file__).resolve().parents[2] / "artifacts" / "benchmark_full"
                / "beam" / "beam_turn_pair" / "retention" / "lru" / "bm25"
                / "budget_025pct" / "PlainTextReader" / "events.jsonl")


@pytest.mark.skipif(not _REAL_EVENTS.exists(),
                    reason="benchmark_full BEAM trace not present on this machine")
def test_equivalence_on_real_beam_trace_first_examples():
    from scripts.replay_equivalence import parse_examples, replay_policy
    n_evicts = 0
    for inserts, budget, shape_ok in parse_examples(_REAL_EVENTS, max_examples=5):
        assert shape_ok, "scored query interleaved with pressure — shape violated"
        fifo = replay_policy(FIFOPolicy(), inserts, budget)
        lru = replay_policy(LRUPolicy(), inserts, budget)
        lfu = replay_policy(LFUPolicy(), inserts, budget)
        assert fifo == lru == lfu
        n_evicts += len(fifo)
    assert n_evicts > 100, "real trace produced too little pressure to be meaningful"
