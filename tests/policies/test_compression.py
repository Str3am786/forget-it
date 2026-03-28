"""
Tests for LLMCompressionPolicy.

All tests use mocks — no LLM server, no backend, no disk.
Run with: pytest tests/test_llm_compression.py -v
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock, PropertyMock
from dataclasses import dataclass, field

# ── minimal fakes (avoid importing the real modules) ────────────────

@dataclass
class FakeRecord:
    id: str
    text: str
    bytes: int = 0
    meta: dict = field(default_factory=dict)

    def __post_init__(self):
        if self.bytes == 0:
            self.bytes = len(self.text.encode("utf-8"))


class FakeSelector:
    """Always picks the victim_id you tell it to."""
    def __init__(self, victim_id: str):
        self.victim_id = victim_id

    def select_victim(self, store) -> str:
        return self.victim_id


class FakeLLM:
    """Returns a canned response. Can be set to raise."""
    def __init__(self, response: str = "", should_raise: bool = False):
        self.response = response
        self.should_raise = should_raise
        self.call_count = 0

    def generate(self, prompt: str, *, max_tokens: int = 512, temperature: float = 0.0) -> str:
        self.call_count += 1
        if self.should_raise:
            raise RuntimeError("LLM crashed")
        return self.response


def make_store(records: dict[str, FakeRecord]) -> MagicMock:
    """Build a mock store whose backend.get returns from the dict."""
    store = MagicMock()
    store.backend.get.side_effect = lambda rid: records.get(rid)
    return store


# ── import the real code under test ─────────────────────────────────

from forgetit.policies.llm_compression import LLMCompressionPolicy, _sanitise
from forgetit.policies.actions import Evict, Compress


# ====================================================================
# _sanitise tests
# ====================================================================

class TestSanitise:

    def test_strips_markdown_fences(self):
        text = "```json\n{\"a\": 1}\n```\nReal content here."
        assert "```" not in _sanitise(text)
        assert "Real content here." in _sanitise(text)

    def test_strips_preamble_here_is(self):
        assert _sanitise("Here is a summary: The key fact.") == "The key fact."

    def test_strips_preamble_sure(self):
        assert _sanitise("Sure! The key fact.") == "The key fact."

    def test_strips_preamble_okay(self):
        assert _sanitise("Okay, The key fact.") == "The key fact."

    def test_deduplicates_repeated_lines(self):
        text = "Line one.\nLine one.\nLine one.\nLine two."
        result = _sanitise(text)
        assert result.count("Line one.") == 1
        assert "Line two." in result

    def test_trims_trailing_incomplete_sentence(self):
        text = "Complete sentence.\nThis is incomple"
        result = _sanitise(text)
        assert result == "Complete sentence."

    def test_keeps_complete_trailing_sentence(self):
        text = "First sentence.\nSecond sentence."
        assert _sanitise(text) == text

    def test_empty_input(self):
        assert _sanitise("") == ""

    def test_only_preamble_returns_empty(self):
        assert _sanitise("Here is a summary:") == ""

    def test_real_world_ramble(self):
        """Simulate what a 4B model might actually spit out."""
        raw = (
            "Sure, here is the summary:\n"
            "```\n"
            "User moved to Berlin in March.\n"
            "```\n"
            "User moved to Berlin in March.\n"  # repeated
            "User moved to Berlin in March.\n"  # repeated
            "I hope this helps you with your"    # incomplete
        )
        result = _sanitise(raw)
        assert result == "User moved to Berlin in March."


# ====================================================================
# LLMCompressionPolicy.select_action tests
# ====================================================================

class TestSelectAction:

    def _make_policy(self, llm, victim_id="vic-1", **kwargs):
        selector = FakeSelector(victim_id)
        return LLMCompressionPolicy(
            llm=llm,
            selector=selector,
            **kwargs,
        )

    # ── happy path: compression works ───────────────────────────

    def test_good_compression_returns_compress(self):
        """LLM returns text that's < 50% of original → Compress."""
        original = "A" * 500  # 500 bytes
        compressed = "Short summary."  # ~14 bytes

        llm = FakeLLM(response=compressed)
        policy = self._make_policy(llm)
        store = make_store({"vic-1": FakeRecord(id="vic-1", text=original)})

        action = policy.select_action(store)

        assert isinstance(action, Compress)
        assert action.id == "vic-1"
        assert action.new_text == compressed

    # ── compression not good enough ─────────────────────────────

    def test_insufficient_saving_returns_evict(self):
        """LLM output is 80% of original → doesn't meet 50% threshold → Evict."""
        original = "A" * 500
        barely_smaller = "B" * 400  # 80% of original

        llm = FakeLLM(response=barely_smaller)
        policy = self._make_policy(llm, min_saving_ratio=0.5)
        store = make_store({"vic-1": FakeRecord(id="vic-1", text=original)})

        action = policy.select_action(store)

        assert isinstance(action, Evict)
        assert action.id == "vic-1"

    def test_custom_saving_ratio(self):
        """With min_saving_ratio=0.2, a 50% reduction is fine."""
        original = "A" * 500
        half = "B" * 250

        llm = FakeLLM(response=half)
        policy = self._make_policy(llm, min_saving_ratio=0.2)
        store = make_store({"vic-1": FakeRecord(id="vic-1", text=original)})

        action = policy.select_action(store)

        assert isinstance(action, Compress)

    # ── tiny record → skip compression ──────────────────────────

    def test_tiny_record_evicted_directly(self):
        """Records under min_compress_bytes get evicted, not compressed."""
        llm = FakeLLM(response="should not be called")
        policy = self._make_policy(llm, min_compress_bytes=200)
        store = make_store({"vic-1": FakeRecord(id="vic-1", text="tiny")})

        action = policy.select_action(store)

        assert isinstance(action, Evict)
        assert llm.call_count == 0  # LLM was never called

    # ── LLM crashes → evict ─────────────────────────────────────

    def test_llm_exception_returns_evict(self):
        llm = FakeLLM(should_raise=True)
        policy = self._make_policy(llm)
        store = make_store({"vic-1": FakeRecord(id="vic-1", text="A" * 500)})

        action = policy.select_action(store)

        assert isinstance(action, Evict)

    # ── record vanished → evict ─────────────────────────────────

    def test_missing_record_returns_evict(self):
        llm = FakeLLM()
        policy = self._make_policy(llm)
        store = make_store({})  # record doesn't exist

        action = policy.select_action(store)

        assert isinstance(action, Evict)
        assert action.id == "vic-1"

    # ── LLM returns empty after sanitise → evict ────────────────

    def test_empty_compression_returns_evict(self):
        llm = FakeLLM(response="Here is a summary:")  # sanitise strips this to ""
        policy = self._make_policy(llm)
        store = make_store({"vic-1": FakeRecord(id="vic-1", text="A" * 500)})

        action = policy.select_action(store)

        assert isinstance(action, Evict)

    # ── failure tracking and max_attempts ────────────────────────

    def test_max_attempts_then_evict(self):
        """After max_attempts failed compressions, evict without calling LLM."""
        llm = FakeLLM(response="B" * 400)  # too big — will fail min_saving_ratio
        policy = self._make_policy(llm, max_attempts=2, min_saving_ratio=0.5)
        store = make_store({"vic-1": FakeRecord(id="vic-1", text="A" * 500)})

        # First two calls: compression fails, failure count increments
        action1 = policy.select_action(store)
        assert isinstance(action1, Evict)
        assert policy._failures.get("vic-1") == 1

        action2 = policy.select_action(store)
        assert isinstance(action2, Evict)
        assert policy._failures.get("vic-1") == 2

        # Third call: max_attempts reached, evicts immediately
        calls_before = llm.call_count
        action3 = policy.select_action(store)
        assert isinstance(action3, Evict)
        assert llm.call_count == calls_before  # LLM was NOT called
        # Failure count cleared after forced eviction
        assert "vic-1" not in policy._failures

    def test_successful_compression_resets_failures(self):
        """A successful compression clears the failure counter."""
        policy = self._make_policy(FakeLLM(response="B" * 400), max_attempts=5)
        store = make_store({"vic-1": FakeRecord(id="vic-1", text="A" * 500)})

        # Accumulate some failures
        policy._failures["vic-1"] = 3

        # Now swap in a good LLM response
        policy.llm = FakeLLM(response="Short.")
        action = policy.select_action(store)

        assert isinstance(action, Compress)
        assert "vic-1" not in policy._failures


# ====================================================================
# _sanitise edge cases
# ====================================================================

class TestSanitiseEdgeCases:

    def test_question_mark_ending_preserved(self):
        assert _sanitise("Is this a fact?") == "Is this a fact?"

    def test_exclamation_ending_preserved(self):
        assert _sanitise("Important fact!") == "Important fact!"

    def test_quoted_ending_preserved(self):
        assert _sanitise('He said "hello."') == 'He said "hello."'

    def test_multiple_fences(self):
        text = "Before. ```code1``` Middle. ```code2``` After."
        result = _sanitise(text)
        assert "```" not in result
        assert "Before." in result
        assert "After." in result

    def test_whitespace_only(self):
        assert _sanitise("   \n\n  ") == ""