"""Every compress-phase decision must say what it did and why, in the event log.

`compressions` in results.csv counts SUCCESSES, so a zero is ambiguous between
"never attempted" and "attempted and failed every time" — opposite findings.
beam_batch posted 0.0 across the whole grid because 90% of its items are 50k-98k
tokens against a 32k window: every call raised, the handler evicted, and the only
trace was a log line nobody reads.

The outcome belongs on the DECISION, not on EvalResult. RetentionManager spreads
`policy.last_decision` into the evict and decision events, so annotating there
costs no schema columns, puts nothing on the rows of policies that never
compress, and gives Prune the same channel when it lands.
"""
from __future__ import annotations

import pytest

from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.logging import BenchmarkLogger
from forgetit.core.schema import MemRecord
from forgetit.memory_system.custom_manager import RetentionManager
from forgetit.policies.actions import Compress, Evict
from forgetit.policies.baselines import LFUPolicy
from forgetit.policies.llm_compression import LLMCompressionPolicy
from forgetit.retrieval.lexical import LexicalOverlapRetriever

BIG = "sentence one about the passport. " * 60          # ~2000 B


class StubLLM:
    model = "stub"

    def __init__(self, out):
        self.out = out
        self.calls = 0

    def generate(self, prompt, **kwargs):
        self.calls += 1
        if isinstance(self.out, Exception):
            raise self.out
        return self.out


def _store(policy) -> RetentionManager:
    s = RetentionManager(budget_bytes=10_000, backend=InMemoryBackend(
        retriever=LexicalOverlapRetriever()), policy=policy, logger=BenchmarkLogger())
    s.connect()
    return s


def _add(store, rid: str, text: str, compressed: bool = False) -> None:
    rec = MemRecord(id=rid, text=text, created_at=1.0)
    rec.bytes = len(text.encode("utf-8"))
    if compressed:
        rec.meta["compressed"] = True
    store.backend.upsert(rec)
    store._bytes_by_id[rid] = rec.bytes
    store.used_bytes += rec.bytes


def _decide(out, text: str = BIG, **kw):
    p = LLMCompressionPolicy(llm=StubLLM(out), selector=LFUPolicy(), **kw)
    s = _store(p)
    _add(s, "x", text)
    return p, p.select_action(s)


class TestOutcomeIsRecorded:

    def test_applied(self):
        p, a = _decide("A short summary.")
        assert isinstance(a, Compress)
        assert p.last_decision["outcome"] == "applied"
        assert p.last_decision["orig_bytes"] > p.last_decision["out_bytes"]

    def test_low_saving(self):
        p, a = _decide(BIG[: int(len(BIG) * 0.8)])
        assert isinstance(a, Evict)
        assert p.last_decision["outcome"] == "low_saving"

    def test_empty_output_is_distinct_from_low_saving(self):
        """_sanitise can strip a response to nothing. Folding that into
        low_saving would hide a prompt or model failure inside a threshold."""
        p, a = _decide("   \n  ")
        assert isinstance(a, Evict)
        assert p.last_decision["outcome"] == "empty_output"

    def test_too_small_skips_the_call(self):
        p = LLMCompressionPolicy(llm=StubLLM("unused"), selector=LFUPolicy(),
                                 min_compress_bytes=200)
        s = _store(p)
        _add(s, "x", "tiny")
        a = p.select_action(s)
        assert isinstance(a, Evict)
        assert p.last_decision["outcome"] == "too_small"
        assert p.llm.calls == 0

    def test_llm_error_names_the_exception(self):
        p, a = _decide(ConnectionError("context length exceeded"))
        assert isinstance(a, Evict)
        assert p.last_decision["outcome"] == "llm_error"
        assert p.last_decision["error"] == "ConnectionError"

    def test_evict_phase_when_everything_is_compressed(self):
        p = LLMCompressionPolicy(llm=StubLLM("unused"), selector=LFUPolicy())
        s = _store(p)
        _add(s, "x", BIG, compressed=True)
        a = p.select_action(s)
        assert isinstance(a, Evict)
        assert p.last_decision["outcome"] == "all_compressed"
        assert p.llm.calls == 0


class TestContextCeiling:
    """The beam_batch regression: oversized items must be a declared skip."""

    def test_oversized_item_is_skipped_without_a_call(self):
        p = LLMCompressionPolicy(llm=StubLLM("summary."), selector=LFUPolicy(),
                                 max_compress_bytes=500)
        s = _store(p)
        _add(s, "x", BIG)
        a = p.select_action(s)
        assert isinstance(a, Evict)
        assert p.last_decision["outcome"] == "too_large"
        assert p.llm.calls == 0, "an unservable prompt must not be sent"

    def test_ceiling_defaults_to_off(self):
        p, a = _decide("A short summary.")
        assert p.max_compress_bytes is None
        assert isinstance(a, Compress)

    def test_item_under_the_ceiling_still_compresses(self):
        p = LLMCompressionPolicy(llm=StubLLM("summary."), selector=LFUPolicy(),
                                 max_compress_bytes=100_000)
        s = _store(p)
        _add(s, "x", BIG)
        assert isinstance(p.select_action(s), Compress)


class TestDecisionProvenance:

    def test_selector_score_survives_the_annotation(self):
        """The selector's own provenance is why last_decision exists; the
        outcome is added to it, not instead of it."""
        p, _ = _decide("A short summary.")
        assert "policy_score" in p.last_decision
        assert "candidate_count" in p.last_decision

    def test_annotating_does_not_mutate_the_selector(self):
        """last_decision used to be ASSIGNED from the selector by reference, so
        annotating would have written into the selector's own state."""
        sel = LFUPolicy()
        p = LLMCompressionPolicy(llm=StubLLM("A short summary."), selector=sel)
        s = _store(p)
        _add(s, "x", BIG)
        p.select_action(s)
        assert "outcome" not in (sel.last_decision or {})

    def test_outcome_reaches_the_event_log(self):
        """The end-to-end claim: run the real budget loop and read it back off
        the logger, because an annotation the store never spreads is useless."""
        p = LLMCompressionPolicy(llm=StubLLM("tiny summary."), selector=LFUPolicy(),
                                 min_compress_bytes=100, min_saving_ratio=0.5)
        s = RetentionManager(budget_bytes=2_500, backend=InMemoryBackend(
            retriever=LexicalOverlapRetriever()), policy=p, logger=BenchmarkLogger())
        s.connect()
        for i in range(5):
            rec = MemRecord(id=f"r{i}", text=BIG, created_at=float(i))
            rec.bytes = len(BIG.encode("utf-8"))
            s.insert(rec)

        outcomes = [e.payload.get("outcome") for e in s.logger.log
                    if e.name in ("decision", "evict", "compress")
                    and e.payload.get("outcome")]
        assert outcomes, "no decision carried an outcome into the event log"
        assert "applied" in outcomes

    def test_rejected_compression_carries_its_cache_key(self):
        p, a = _decide(BIG[: int(len(BIG) * 0.8)])
        assert p.last_decision["outcome"] == "low_saving"
        assert p.last_decision["cache_key"], (
            "without the key the model's output is unreachable, and rejected "
            "compressions are the compressor's failure cases")

    def test_no_key_when_no_call_was_made(self):
        p = LLMCompressionPolicy(llm=StubLLM("unused"), selector=LFUPolicy(),
                                 min_compress_bytes=200)
        s = _store(p)
        _add(s, "x", "tiny")
        p.select_action(s)
        assert "cache_key" not in p.last_decision