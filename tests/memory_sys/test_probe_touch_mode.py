"""E3 probes-blind machinery: probe_touch_mode on RetentionManager.

Covers the Phase-1 contract:
  - probes_blind: probe retrievals do NOT touch access_count/last_access;
    scored retrievals touch as before.
  - probes_only: the inverse.
  - original: every retrieval touches (legacy behavior, the default).
  - retrieval RESULTS are identical across modes — only the touch differs.
  - the retrieve log event is self-describing (probe/touched booleans).
  - LRU eviction order actually differs between original and probes_blind
    (the unit-level version of the smoke-run gate).
"""
import pytest

from forgetit.policies import LRUPolicy
from forgetit.memory_system import RetentionManager
from forgetit.retrieval.lexical import LexicalOverlapRetriever
from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.schema import MemRecord, Query


class RecordingLogger:
    def __init__(self):
        self.events = []

    def event(self, name, payload=None):
        self.events.append((name, dict(payload or {})))

    def retrieves(self):
        return [p for n, p in self.events if n == "retrieve"]


def make_store(probe_touch_mode="original", budget_bytes=1_000_000):
    logger = RecordingLogger()
    store = RetentionManager(
        budget_bytes=budget_bytes,
        backend=InMemoryBackend(retriever=LexicalOverlapRetriever()),
        policy=LRUPolicy(),
        logger=logger,
        probe_touch_mode=probe_touch_mode,
    )
    store.connect()
    return store, logger


def rec(item_id, token, bytes_=10):
    # One unique token per record so a query for that token hits exactly it.
    return MemRecord(id=item_id, text=f"{token} filler", created_at=1.0, bytes=bytes_)


def probe_query(qid_stem, token):
    return Query(id=f"{qid_stem}::probe::1", text=token, timestamp=2.0,
                 meta={"probe": True})


def scored_query(qid, token):
    return Query(id=qid, text=token, timestamp=2.0, meta={})


def signal(store, item_id):
    r = store.get(item_id)
    return (r.access_count, r.last_access)


# ── touch semantics per mode ─────────────────────────────────────────


def test_probes_blind_probe_does_not_touch_scored_does():
    store, _ = make_store("probes_blind")
    store.insert(rec("a", "alpha"))
    before = signal(store, "a")

    hits = store.retrieve(probe_query("q1", "alpha"), k=1)
    assert [h.id for h in hits] == ["a"]          # results unchanged by mode
    assert signal(store, "a") == before           # no touch

    store.retrieve(scored_query("q1", "alpha"), k=1)
    count, last = signal(store, "a")
    assert count == before[0] + 1                 # scored query touches
    assert last > before[1]


def test_probes_only_probe_touches_scored_does_not():
    store, _ = make_store("probes_only")
    store.insert(rec("a", "alpha"))
    before = signal(store, "a")

    store.retrieve(scored_query("q1", "alpha"), k=1)
    assert signal(store, "a") == before           # scored query does NOT touch

    store.retrieve(probe_query("q1", "alpha"), k=1)
    count, last = signal(store, "a")
    assert count == before[0] + 1                 # probe touches
    assert last > before[1]


def test_original_touches_on_both_and_is_default():
    store, _ = make_store()                       # no arg → default mode
    assert store.probe_touch_mode == "original"
    store.insert(rec("a", "alpha"))
    c0, _ = signal(store, "a")

    store.retrieve(probe_query("q1", "alpha"), k=1)
    c1, l1 = signal(store, "a")
    assert c1 == c0 + 1

    store.retrieve(scored_query("q1", "alpha"), k=1)
    c2, l2 = signal(store, "a")
    assert c2 == c1 + 1 and l2 > l1


def test_probe_detected_by_id_substring_without_meta_flag():
    # Fallback path: no meta flag, but the id carries the ::probe:: convention.
    store, _ = make_store("probes_blind")
    store.insert(rec("a", "alpha"))
    before = signal(store, "a")
    q = Query(id="x::probe::7", text="alpha", timestamp=2.0, meta={})
    store.retrieve(q, k=1)
    assert signal(store, "a") == before


def test_invalid_mode_rejected():
    with pytest.raises(ValueError):
        make_store("blind")                       # not a registered mode


# ── log event self-description ───────────────────────────────────────


@pytest.mark.parametrize("mode,probe_touched,scored_touched", [
    ("original",     True,  True),
    ("probes_blind", False, True),
    ("probes_only",  True,  False),
])
def test_retrieve_event_carries_probe_and_touched_flags(mode, probe_touched, scored_touched):
    store, logger = make_store(mode)
    store.insert(rec("a", "alpha"))

    store.retrieve(probe_query("q1", "alpha"), k=1)
    store.retrieve(scored_query("q1", "alpha"), k=1)

    probe_ev, scored_ev = logger.retrieves()
    assert probe_ev["q"] == "q1::probe::1"
    assert probe_ev["hits"] == ["a"]
    assert probe_ev["probe"] is True
    assert probe_ev["touched"] is probe_touched
    assert scored_ev["probe"] is False
    assert scored_ev["touched"] is scored_touched


# ── results identical across modes, eviction order not ───────────────


def test_retrieval_results_identical_across_modes():
    hits_by_mode = {}
    for mode in ("original", "probes_blind", "probes_only"):
        store, _ = make_store(mode)
        for i, tok in enumerate(["alpha", "beta", "gamma"]):
            store.insert(rec(f"i{i}", tok))
        hits = store.retrieve(probe_query("q", "beta"), k=3)
        hits_by_mode[mode] = [h.id for h in hits]
    assert hits_by_mode["original"] == hits_by_mode["probes_blind"] == hits_by_mode["probes_only"]


def _evictions_under(mode):
    """Insert a,b; probe-touch a; insert c under a 2-item budget → who dies?"""
    store, logger = make_store(mode, budget_bytes=20)
    store.insert(rec("a", "alpha"))
    store.insert(rec("b", "beta"))
    store.retrieve(probe_query("q", "alpha"), k=1)   # probe touches (or not) a
    store.insert(rec("c", "gamma"))                  # forces one eviction
    return [p["id"] for n, p in logger.events if n == "evict"]


def test_lru_eviction_order_differs_between_original_and_probes_blind():
    # original: the probe refreshed a, so b is least-recent → b evicted.
    assert _evictions_under("original") == ["b"]
    # probes_blind: the probe left a stale → a is least-recent → a evicted.
    assert _evictions_under("probes_blind") == ["a"]
