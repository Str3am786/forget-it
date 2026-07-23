"""MAIN-ADAPTED rescue of tests/policies/test_fifo_equivalence.py (manifest P-1/P-2).

WHY THIS FILE EXISTS — read before trusting any green here.
=============================================================
The rescued branch test constructs its store with
``RetentionManager(..., probe_touch_mode=<mode>)``. That kwarg does not exist on
main (verification/reports/INVENTORY_CORRECTIONS.md C-E), so all 9 of its tests
die at construction time with TypeError and prove nothing.

Main implements the same touch axis by CALL-SITE DISPATCH instead of a store
mode:

  * ``RetentionManager.retrieve(q, k)``  -> ranks AND touches (access_count += 1,
    last_access = ++tick).  Equivalent to branch ``touch=True``.
  * ``RetentionManager.search(q, k)``    -> ranks, touches nothing, does not even
    advance ``_tick``.  Equivalent to branch ``touch=False``.

So the branch's three modes map onto main as follows, at the TEST layer:

  branch mode      | probe call | scored call | reachable on main?
  -----------------+------------+-------------+-----------------------------
  original         | retrieve   | retrieve    | yes (test layer)
  probes_blind     | search     | retrieve    | yes (test layer)
  probes_only      | retrieve   | search      | yes (test layer) == what
                   |            |             | runner_core.py ALREADY does
                   |            |             | in production

MEASURED DELTA vs the branch implementation (not a guess -- asserted below by
test_tick_skew_does_not_change_eviction_order): branch ``retrieve`` increments
``_tick`` unconditionally even when it does not touch; main's ``search`` does
not increment ``_tick`` at all.  The absolute ``last_access`` values therefore
differ between "search-as-blind-probe" and branch ``probes_blind``.  Eviction
order depends only on the ORDER of ``last_access``, which tick-skipping
preserves, so eviction traces are identical.  That is asserted, not assumed.

WHAT IS STILL NOT COVERED HERE (genuinely BLOCKED-BY-DRIFT, no substitute):
  * probe auto-detection from ``meta={"probe": True}`` / the ``::probe::`` id
    convention -- main has no such branch anywhere in src/.
  * ``ValueError`` on an unregistered mode string.
  * ``retrieve`` log event carrying ``probe`` / ``touched`` booleans -- main's
    payload is exactly ``{"q", "hits"}``.
  * a CLI/pipeline flag that selects probes_blind for a real run.
"""
from __future__ import annotations

import random

import pytest

from forgetit.policies import FIFOPolicy, LRUPolicy, LFUPolicy
from forgetit.policies.baselines import (KeepSmallestPolicy, RandomPolicy,
                                         EvictSmallestPolicy)
from forgetit.policies.density import SalienceDecayPolicy, GDSFPolicy
from forgetit.policies.ebbinghaus import EbbinghausPolicy
from forgetit.policies.oracle import OraclePolicy
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


def make_store(policy, *, budget):
    logger = RecordingLogger()
    store = RetentionManager(
        budget_bytes=budget,
        backend=InMemoryBackend(retriever=LexicalOverlapRetriever()),
        policy=policy,
        logger=logger,
    )
    store.connect()
    return store, logger


def rec(item_id, token, *, bytes_=40, created_at=1.0):
    return MemRecord(id=item_id, text=f"{token} filler", bytes=bytes_,
                     created_at=created_at)


# ── main's FIFO clock: MEASURED, and it is NOT the insert ordinal ─────
#
# The branch test asserts insertion order and documents why (LongMemEval
# session dates are non-monotonic w.r.t. the insert stream).  Main's
# FIFOPolicy.select_victim ranks on MemFeatures.created_at; MemFeatures on main
# has no t_insert field at all.  These two tests PIN the actual main behaviour
# so the divergence is a tracked fact, not a silent one.


def test_main_fifo_is_keyed_on_created_at_not_insertion_order():
    """DIVERGENCE PIN. The branch test asserts ['A']; main yields ['B'].

    A is inserted first but carries created_at=500; B is inserted second with
    created_at=100.  Insertion-order FIFO evicts A.  created_at FIFO evicts B.
    """
    store, logger = make_store(FIFOPolicy(), budget=100)
    store.insert(rec("A", "alpha", created_at=500.0))
    store.insert(rec("B", "beta", created_at=100.0))
    store.insert(rec("C", "gamma", created_at=300.0))
    assert logger.evictions() == ["B"], (
        "main FIFOPolicy changed clock; this pin needs updating"
    )


def test_main_fifo_matches_insertion_order_only_when_created_at_is_monotone():
    """The condition under which main's FIFO joins the E3 equivalence class."""
    store, logger = make_store(FIFOPolicy(), budget=100)
    store.insert(rec("A", "alpha", created_at=1.0))
    store.insert(rec("B", "beta", created_at=2.0))
    store.insert(rec("C", "gamma", created_at=3.0))
    assert logger.evictions() == ["A"]


def test_main_fifo_ignores_touches():
    """Rescued test_fifo_ignores_touches, verbatim behaviour, kwarg dropped.

    created_at is constant here, so the created_at-vs-t_insert divergence is
    inert and the original assertion stands unchanged.
    """
    store, logger = make_store(FIFOPolicy(), budget=100)
    store.insert(rec("A", "alpha"))
    store.insert(rec("B", "beta"))
    store.retrieve(Query(id="q1", text="alpha", timestamp=2.0, meta={}), k=1)
    store.insert(rec("C", "gamma"))
    store.insert(rec("D", "delta"))
    assert logger.evictions() == ["A", "B"]


# ── 2. fifo registered in both runners (rescued verbatim; already green) ──


@pytest.mark.parametrize("runner_module", [
    "forgetit.experiments.run_beam",
    "forgetit.experiments.run_longmemeval_qa",
])
def test_fifo_registered_in_runner_policy_specs(runner_module):
    import importlib
    mod = importlib.import_module(runner_module)
    names = [name for name, _ in mod.default_policy_specs(llm=None)]
    assert "fifo" in names, f"{runner_module}: fifo missing from {names}"


# ── the BEAM-shaped generator (rescued verbatim) ─────────────────────


def beam_shaped_trace(seed: int, n_items: int = 60, *, shuffle_created=True):
    rng = random.Random(seed)
    created = list(range(1, n_items + 1))
    if shuffle_created:
        rng.shuffle(created)
    events = []
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


def replay(policy, events, *, budget=600, k=3, probe_touches=True,
           scored_touches=True, tick_on_blind=False):
    """Replay with the touch axis selected per call site (main's API).

    ``tick_on_blind`` reproduces the BRANCH's tick bookkeeping: branch
    ``retrieve`` bumps ``_tick`` even when it suppresses the touch.  Main's
    ``search`` does not.  Exposed so the delta can be measured.
    """
    store, logger = make_store(policy, budget=budget)
    for kind, payload in events:
        if kind == "insert":
            store.insert(MemRecord(id=payload.id, text=payload.text,
                                   bytes=payload.bytes,
                                   created_at=payload.created_at))
            continue
        touches = probe_touches if kind == "probe" else scored_touches
        if touches:
            store.retrieve(payload, k=k)
        else:
            store.search(payload, k=k)
            if tick_on_blind:
                store._tick += 1        # branch-parity tick bookkeeping
    return logger.evictions()


def blind_oracle(inserts, budget):
    """Insertion-order FIFO replay. Imports nothing from forgetit.

    Byte-for-byte the same algorithm as the verification campaign's
    e3_invariants._blind_replay (local-only, not part of the repo).
    """
    q, used, evicted = [], 0, []
    for item_id, b in inserts:
        while used + b > budget and q:
            vid, vb = q.pop(0)
            evicted.append(vid)
            used -= vb
        q.append((item_id, b))
        used += b
    return evicted


# ── 3. the equivalence property, main-adapted ────────────────────────


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_blind_lru_equals_blind_lfu_equals_insertion_order_oracle(seed):
    """E3 class collapse. Substitution: probes_blind == probes routed to
    search().  main's FIFOPolicy is deliberately EXCLUDED from the identity —
    see test_main_fifo_is_outside_the_equivalence_class below."""
    events = beam_shaped_trace(seed)
    inserts = [(p.id, p.bytes) for kind, p in events if kind == "insert"]
    oracle = blind_oracle(inserts, 600)

    lru_blind = replay(LRUPolicy(), events, probe_touches=False)
    lfu_blind = replay(LFUPolicy(), events, probe_touches=False)

    assert len(oracle) > 10, "trace produced too little pressure to be meaningful"
    assert lru_blind == oracle, "blind-LRU != insertion-order oracle"
    assert lfu_blind == oracle, "blind-LFU != insertion-order oracle"


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_main_fifo_is_outside_the_equivalence_class(seed):
    """The branch's claim `fifo == blind-LRU == blind-LFU` is FALSE on main
    whenever created_at is non-monotone w.r.t. insertion order -- which is the
    exact shape the branch docstring cites LongMemEval for."""
    events = beam_shaped_trace(seed, shuffle_created=True)
    inserts = [(p.id, p.bytes) for kind, p in events if kind == "insert"]
    oracle = blind_oracle(inserts, 600)
    fifo_main = replay(FIFOPolicy(), events)
    assert fifo_main != oracle


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_main_fifo_joins_the_class_when_created_at_is_monotone(seed):
    events = beam_shaped_trace(seed, shuffle_created=False)
    inserts = [(p.id, p.bytes) for kind, p in events if kind == "insert"]
    oracle = blind_oracle(inserts, 600)
    assert replay(FIFOPolicy(), events) == oracle


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_negative_control_touching_lru_diverges_from_blind_lru(seed):
    """Without this, the equivalence test above could be vacuously green."""
    events = beam_shaped_trace(seed)
    lru_touching = replay(LRUPolicy(), events, probe_touches=True)
    lru_blind = replay(LRUPolicy(), events, probe_touches=False)
    assert lru_touching != lru_blind


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_tick_skew_does_not_change_eviction_order(seed):
    """MEASURES the one named delta between main's search() and the branch's
    probes_blind: the branch advances _tick on a non-touching retrieve, main's
    search does not.  Eviction order must be invariant to that."""
    events = beam_shaped_trace(seed)
    for policy_cls in (LRUPolicy, LFUPolicy):
        no_tick = replay(policy_cls(), events, probe_touches=False,
                         tick_on_blind=False)
        with_tick = replay(policy_cls(), events, probe_touches=False,
                           tick_on_blind=True)
        assert no_tick == with_tick, f"{policy_cls.__name__} tick-sensitive"


# ── P-2: touch-invariance of NON-HISTORY policies ────────────────────
#
# Manifest P-2 says "probes are byte-identical no-ops for non-history
# policies".  On main this is demonstrable WITHOUT probe_touch_mode: a
# non-history policy's select_victim reads no touch-derived field, so routing
# probes through retrieve() or search() cannot change what it evicts.
# The scope of "byte-identical" is stated exactly: the EVICTION TRACE and the
# END STATE are identical; the retrieve/search LOG EVENT NAME and the residents'
# access_count/last_access are NOT (they are what the touch changes).

# Membership is SOURCE-DERIVED, not assumed: a policy is "history" iff its
# select_victim reads last_access / access_count / on_access. Asserted by
# test_history_partition_is_source_derived below, so the split cannot silently
# drift when a policy is added.
NON_HISTORY = [
    ("fifo", lambda: FIFOPolicy()),
    ("keep_smallest", lambda: KeepSmallestPolicy()),
    ("evict_smallest", lambda: EvictSmallestPolicy()),
    ("random", lambda: RandomPolicy(seed=7)),
    ("oracle", lambda: OraclePolicy()),
]
HISTORY = [
    ("lru", lambda: LRUPolicy()),
    ("lfu", lambda: LFUPolicy()),
    ("gdsf", lambda: GDSFPolicy()),
    ("salience_decay", lambda: SalienceDecayPolicy()),
    ("ebbinghaus", lambda: EbbinghausPolicy()),
]


def test_history_partition_is_source_derived():
    """Pins the NON_HISTORY / HISTORY split to the actual source, so P-2's
    scope cannot rot when a policy is added or its scoring changes."""
    import inspect
    touch_symbols = ("last_access", "access_count", "on_access")
    for name, factory in NON_HISTORY:
        src = inspect.getsource(type(factory()))
        found = [t for t in touch_symbols if t in src]
        assert not found, f"{name} listed non-history but reads {found}"
    for name, factory in HISTORY:
        src = inspect.getsource(type(factory()))
        found = [t for t in touch_symbols if t in src]
        assert found, f"{name} listed history but reads no touch state"


@pytest.mark.parametrize("name,factory", NON_HISTORY)
@pytest.mark.parametrize("seed", [1, 2, 3])
def test_touch_invariance_non_history_policies(name, factory, seed):
    events = beam_shaped_trace(seed)
    touching = replay(factory(), events, probe_touches=True)
    blind = replay(factory(), events, probe_touches=False)
    assert len(touching) > 10, "insufficient pressure"
    assert touching == blind, f"{name} eviction trace changed under probe touches"


@pytest.mark.parametrize("name,factory", HISTORY)
@pytest.mark.parametrize("seed", [1, 2, 3])
def test_touch_variance_history_policies_negative_control(name, factory, seed):
    events = beam_shaped_trace(seed)
    touching = replay(factory(), events, probe_touches=True)
    blind = replay(factory(), events, probe_touches=False)
    assert touching != blind, f"{name} is touch-invariant -- control is vacuous"


# ── main's search() really is a no-op on policy state ────────────────


def test_search_mutates_no_policy_signal_while_retrieve_does():
    store, _ = make_store(LRUPolicy(), budget=1_000_000)
    store.insert(rec("a", "alpha"))
    before = (store.get("a").access_count, store.get("a").last_access)

    hits = store.search(Query(id="q::probe::1", text="alpha", timestamp=2.0, meta={}), k=1)
    assert [h.id for h in hits] == ["a"]
    assert (store.get("a").access_count, store.get("a").last_access) == before

    store.retrieve(Query(id="q1", text="alpha", timestamp=2.0, meta={}), k=1)
    after = (store.get("a").access_count, store.get("a").last_access)
    assert after[0] == before[0] + 1 and after[1] > before[1]


def test_retrieval_results_identical_between_search_and_retrieve():
    ranked = {}
    for path in ("retrieve", "search"):
        store, _ = make_store(LRUPolicy(), budget=1_000_000)
        for i, tok in enumerate(["alpha", "beta", "gamma"]):
            store.insert(rec(f"i{i}", tok))
        q = Query(id="q::probe::1", text="beta", timestamp=2.0, meta={})
        hits = getattr(store, path)(q, k=3)
        ranked[path] = [h.id for h in hits]
    assert ranked["retrieve"] == ranked["search"]


def test_lru_eviction_order_differs_between_touching_and_blind_probe():
    """Unit-scale version of the smoke gate (rescued
    test_lru_eviction_order_differs_between_original_and_probes_blind)."""
    def evictions(probe_touches):
        store, logger = make_store(LRUPolicy(), budget=20)
        store.insert(rec("a", "alpha", bytes_=10))
        store.insert(rec("b", "beta", bytes_=10))
        q = Query(id="q::probe::1", text="alpha", timestamp=2.0, meta={"probe": True})
        (store.retrieve if probe_touches else store.search)(q, k=1)
        store.insert(rec("c", "gamma", bytes_=10))
        return logger.evictions()

    assert evictions(True) == ["b"]     # probe refreshed a -> b is least-recent
    assert evictions(False) == ["a"]    # probe left a stale -> a is least-recent


# ── what main's PRODUCTION pipeline actually does ────────────────────


def test_production_call_site_dispatch_is_probes_only_semantics():
    """runner_core routes unscored queries to view.retrieve (touch) and scored
    queries to view.search (no touch).  In branch vocabulary that is
    probe_touch_mode='probes_only', NOT the branch default 'original'.
    Pinned here because two rescue documents describe main as 'original'."""
    import inspect
    from forgetit.benchmark import runner_core
    src = inspect.getsource(runner_core)
    assert "view.retrieve(event.query, k=k)" in src, \
        "unscored-query path no longer uses the touching call"
    assert "view.search(query, eval_k)" in src, \
        "scored-query path no longer uses the read-only call"
