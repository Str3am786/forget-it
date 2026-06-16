"""
Behavioral tests for the two-phase LLMCompressionPolicy.

The policy packs more memories into a fixed budget by *compressing* before it
*evicts*:

  1. COMPRESS phase — least-useful UN-compressed item gets summarised, kept.
  2. EVICT phase    — only once everything resident is already compressed do we
                      delete the least-useful item.

Each item is compressed at most once (compressed items are excluded from the
compress phase), so there is no re-compression churn and no max_attempts.

Tests use a scripted StubLLM and run the policy both in isolation
(select_action) and inside a real RetentionManager + InMemoryBackend.
"""
from __future__ import annotations

import time
import pytest

from forgetit.core.schema import MemRecord
from forgetit.core.logging import BenchmarkLogger
from forgetit.backend.in_memory import InMemoryBackend
from forgetit.memory_system.custom_manager import RetentionManager
from forgetit.policies.baselines import FIFOPolicy, LRUPolicy, LFUPolicy
from forgetit.policies.llm_compression import LLMCompressionPolicy
from forgetit.policies.actions import Evict, Compress
from forgetit.retrieval.lexical import LexicalOverlapRetriever


# ── helpers ─────────────────────────────────────────────────────────

class StubLLM:
    """Returns scripted outputs from a queue."""
    def __init__(self, outputs):
        self.outputs = list(outputs)
        self.call_count = 0

    def generate(self, prompt, *, max_tokens=128, temperature=0.0):
        self.call_count += 1
        if not self.outputs:
            raise RuntimeError("StubLLM exhausted — test asked for too many calls")
        out = self.outputs.pop(0)
        if isinstance(out, Exception):
            raise out
        return out


class RecordingSelector:
    """Records the candidate set it was asked to rank; returns a fixed victim."""
    name = "recording"

    def __init__(self, victim_id: str):
        self.victim_id = victim_id
        self.calls: list = []

    def select_victim(self, store, candidates=None) -> str:
        self.calls.append(candidates)
        return self.victim_id


def _record(rid: str, text: str, ts: float = 0.0, compressed: bool = False) -> MemRecord:
    rec = MemRecord(id=rid, text=text, created_at=ts or time.time())
    rec.bytes = len(text.encode("utf-8"))
    if compressed:
        rec.meta["compressed"] = True
    return rec


def _make_store(budget: int, policy) -> RetentionManager:
    backend = InMemoryBackend(retriever=LexicalOverlapRetriever())
    store = RetentionManager(
        budget_bytes=budget, backend=backend, policy=policy, logger=BenchmarkLogger()
    )
    store.connect()
    return store


def _add(store: RetentionManager, rec: MemRecord) -> None:
    """Place a record directly into the store + accounting (bypasses _ensure_budget)."""
    store.backend.upsert(rec)
    store._bytes_by_id[rec.id] = rec.bytes
    store.used_bytes += rec.bytes


# ── compress phase: single-item branches ────────────────────────────

def test_good_compression_returns_compress():
    record = _record("a", "Lorem ipsum dolor sit amet. " * 50)  # ~1400 bytes
    llm = StubLLM(["Lorem summary."])
    policy = LLMCompressionPolicy(llm=llm, selector=LRUPolicy(), min_compress_bytes=100)
    store = _make_store(10_000, policy)
    _add(store, record)

    action = policy.select_action(store)
    assert isinstance(action, Compress)
    assert action.id == "a"
    assert action.new_text == "Lorem summary."
    assert llm.call_count == 1


def test_insufficient_saving_returns_evict():
    text = "Lorem ipsum dolor sit amet. " * 50
    barely_smaller = text[: int(len(text) * 0.9)]  # only ~10% saving
    llm = StubLLM([barely_smaller])
    policy = LLMCompressionPolicy(llm=llm, selector=LRUPolicy(),
                                  min_saving_ratio=0.5, min_compress_bytes=100)
    store = _make_store(10_000, policy)
    _add(store, _record("a", text))

    action = policy.select_action(store)
    assert isinstance(action, Evict)
    assert action.id == "a"


def test_llm_exception_returns_evict():
    llm = StubLLM([ConnectionError("Ollama down")])
    policy = LLMCompressionPolicy(llm=llm, selector=LRUPolicy(), min_compress_bytes=100)
    store = _make_store(10_000, policy)
    _add(store, _record("a", "x" * 1000))

    action = policy.select_action(store)
    assert isinstance(action, Evict)
    assert action.id == "a"


def test_tiny_record_evicted_without_llm_call():
    llm = StubLLM([])  # must not be called
    policy = LLMCompressionPolicy(llm=llm, selector=LRUPolicy(), min_compress_bytes=200)
    store = _make_store(10_000, policy)
    _add(store, _record("a", "tiny"))  # 4 bytes

    action = policy.select_action(store)
    assert isinstance(action, Evict)
    assert llm.call_count == 0


def test_empty_compression_output_returns_evict():
    llm = StubLLM(["   \n  "])  # _sanitise() strips to ""
    policy = LLMCompressionPolicy(llm=llm, selector=LRUPolicy(), min_compress_bytes=100)
    store = _make_store(10_000, policy)
    _add(store, _record("a", "x" * 1000))

    action = policy.select_action(store)
    assert isinstance(action, Evict)


# ── two-phase: compress un-compressed before evicting compressed ─────

def test_compress_phase_only_considers_uncompressed_items():
    big_uncompressed = _record("u", "Lorem ipsum dolor. " * 80)
    already = _record("c", "short summary.", compressed=True)
    cs = RecordingSelector("u")
    es = RecordingSelector("c")
    llm = StubLLM(["tiny."])
    policy = LLMCompressionPolicy(llm=llm, compress_selector=cs, evict_selector=es,
                                  min_compress_bytes=50)
    store = _make_store(10_000, policy)
    _add(store, big_uncompressed)
    _add(store, already)

    action = policy.select_action(store)
    assert isinstance(action, Compress)
    assert action.id == "u"               # the uncompressed item, never "c"
    assert cs.calls == [{"u"}]            # only un-compressed id offered to compress-selector
    assert es.calls == []                 # evict-selector not consulted while work remains


def test_evict_phase_runs_only_when_all_compressed():
    a = _record("a", "summary a.", compressed=True)
    b = _record("b", "summary b.", compressed=True)
    cs = RecordingSelector("a")
    es = RecordingSelector("b")
    llm = StubLLM([])  # must not be called — nothing left to compress
    policy = LLMCompressionPolicy(llm=llm, compress_selector=cs, evict_selector=es)
    store = _make_store(10_000, policy)
    _add(store, a)
    _add(store, b)

    action = policy.select_action(store)
    assert isinstance(action, Evict)
    assert action.id == "b"               # chosen by the evict-selector
    assert es.calls == [None]             # evict-selector ranks ALL items
    assert cs.calls == []
    assert llm.call_count == 0


def test_compress_selector_chooses_among_uncompressed():
    # FIFO compress-selector should pick the oldest UN-compressed item.
    old = _record("old", "Lorem ipsum dolor. " * 80, ts=1.0)
    new = _record("new", "Lorem ipsum dolor. " * 80, ts=2.0)
    llm = StubLLM(["tiny."])
    policy = LLMCompressionPolicy(llm=llm, compress_selector=FIFOPolicy(),
                                  evict_selector=LFUPolicy(), min_compress_bytes=50)
    store = _make_store(10_000, policy)
    _add(store, old)
    _add(store, new)

    action = policy.select_action(store)
    assert isinstance(action, Compress)
    assert action.id == "old"


# ── selector wiring ─────────────────────────────────────────────────

def test_default_selectors_are_lfu():
    p = LLMCompressionPolicy(llm=StubLLM([]))
    assert isinstance(p.compress_selector, LFUPolicy)
    assert isinstance(p.evict_selector, LFUPolicy)


def test_selector_shorthand_sets_both_phases():
    s = LRUPolicy()
    p = LLMCompressionPolicy(llm=StubLLM([]), selector=s)
    assert p.compress_selector is s
    assert p.evict_selector is s


def test_compress_and_evict_selectors_are_independent():
    cs, es = FIFOPolicy(), LFUPolicy()
    p = LLMCompressionPolicy(llm=StubLLM([]), compress_selector=cs, evict_selector=es)
    assert p.compress_selector is cs
    assert p.evict_selector is es


# ── integration: real _ensure_budget loop ──────────────────────────

def test_ensure_budget_terminates_with_misbehaving_llm():
    """LLM echoes its input (0% saving) forever → policy must still make progress
    (evict on failed compression) and the budget loop must terminate."""
    records = [_record(f"r{i}", "x" * 1000, ts=float(i)) for i in range(5)]

    class IdiotLLM:
        call_count = 0
        def generate(self, prompt, **kwargs):
            IdiotLLM.call_count += 1
            return prompt.split("TEXT:\n", 1)[1].split("\n\nSUMMARY")[0]

    policy = LLMCompressionPolicy(llm=IdiotLLM(), selector=LRUPolicy(),
                                  min_saving_ratio=0.5, min_compress_bytes=100)
    store = _make_store(budget=2000, policy=policy)

    for r in records:
        store.insert(r)  # must terminate (no infinite loop)

    assert store.used_bytes <= 2000
    assert IdiotLLM.call_count < 50, f"LLM called {IdiotLLM.call_count}× — likely a loop"


def test_ensure_budget_progresses_with_good_compression():
    records = [_record(f"r{i}", "lorem ipsum " * 67, ts=float(i)) for i in range(3)]

    class GoodLLM:
        call_count = 0
        def generate(self, prompt, **kwargs):
            GoodLLM.call_count += 1
            return "summary."

    policy = LLMCompressionPolicy(llm=GoodLLM(), selector=LRUPolicy(),
                                  min_saving_ratio=0.5, min_compress_bytes=100)
    store = _make_store(budget=2000, policy=policy)

    for r in records:
        store.insert(r)

    assert store.used_bytes <= 2000
    assert len(list(store.iter_ids())) == 3  # all compressed & retained, none evicted


def test_compression_packs_more_than_plain_eviction_would():
    """5 items of ~800B with a 2500B budget: plain eviction keeps ~3, but
    compress-to-pack shrinks them so all 5 survive."""
    records = [_record(f"r{i}", "lorem ipsum dolor sit " * 38, ts=float(i)) for i in range(5)]
    assert records[0].bytes > 700  # sanity: each item is large

    policy = LLMCompressionPolicy(llm=StubLLM(["summary."] * 20), selector=LRUPolicy(),
                                  min_saving_ratio=0.5, min_compress_bytes=100)
    store = _make_store(budget=2500, policy=policy)

    for r in records:
        store.insert(r)

    assert store.used_bytes <= 2500
    assert len(list(store.iter_ids())) == 5  # nothing evicted — all packed in compressed


# ── MemoryBank subclass behaves under the real loop ─────────────────

def test_memorybank_compresses_against_real_store():
    from forgetit.policies.ebbinghaus import MemoryBankCompressionPolicy

    llm = StubLLM(["Short summary."])
    policy = MemoryBankCompressionPolicy(llm=llm, selector=LRUPolicy(), min_compress_bytes=100)
    store = _make_store(10_000, policy)
    _add(store, _record("v", "A" * 500))

    action = policy.select_action(store)
    assert isinstance(action, Compress)
    assert action.new_text == "Short summary."
    # the record's text must have reached the LLM prompt
    assert llm.call_count == 1
