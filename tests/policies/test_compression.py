"""
Unit tests for LLMCompressionPolicy — pure / store-independent.

Covers _sanitise(), the Action dataclasses, SimpleEvictionPolicy.select_action
wrapping, and MemoryBankCompressionPolicy construction/prompt wiring.

The behavioral tests that exercise the two-phase compress/evict loop against a
real RetentionManager live in test_compression_2.py.

All tests here are pure — no LLM server, no backend, no disk.
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from forgetit.policies.llm_compression import LLMCompressionPolicy, _sanitise
from forgetit.policies.actions import Evict, Compress


# ── minimal construction-only fakes ─────────────────────────────────

class FakeSelector:
    """Always picks the victim_id you tell it to. Accepts the candidates filter."""
    def __init__(self, victim_id: str):
        self.victim_id = victim_id

    def select_victim(self, store, candidates=None) -> str:
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


class TestSanitisePreambleStacking:
    """The preamble-strip loop runs until stable, so stacked preambles unwind."""

    def test_sure_then_here_is_stacked(self):
        # "Sure," is stripped first, leaving "here is the summary:" which is then stripped.
        result = _sanitise("Sure, here is the summary: The key fact.")
        assert result == "The key fact."

    def test_preamble_case_insensitive_here(self):
        assert _sanitise("HERE IS A SUMMARY: The key fact.") == "The key fact."

    def test_preamble_case_insensitive_sure(self):
        assert _sanitise("SURE! The key fact.") == "The key fact."

    def test_preamble_case_insensitive_okay(self):
        assert _sanitise("OKAY: The key fact.") == "The key fact."


class TestSanitiseFences:

    def test_fence_only_returns_empty(self):
        assert _sanitise("```some code here```") == ""

    def test_text_before_fence_preserved(self):
        result = _sanitise("Before fact. ```ignored code``` After fact.")
        assert "Before fact." in result
        assert "After fact." in result
        assert "ignored" not in result

    def test_preamble_inside_fence_removed_with_fence(self):
        # The fence is removed wholesale; content inside it does not leak.
        result = _sanitise("```Sure, here is a summary: secret```\nReal fact.")
        assert "secret" not in result
        assert "Real fact." in result


class TestSanitiseTrailingTrim:

    def test_single_line_incomplete_is_never_trimmed(self):
        # The trailing-trim guard fires only when len(lines) > 1.
        # A one-line input is the whole content — silently dropping it would be wrong.
        assert _sanitise("Incomplete fragment") == "Incomplete fragment"

    def test_single_quote_ending_preserved(self):
        text = "First line.\nShe said 'the answer'."
        assert "She said 'the answer'." in _sanitise(text)

    def test_trailing_line_ending_with_period_preserved(self):
        text = "First sentence.\nSecond sentence."
        assert _sanitise(text) == text

    def test_trailing_incomplete_multiline_dropped(self):
        result = _sanitise("Full sentence.\nincomplete without punctuation")
        assert result == "Full sentence."

    def test_blank_trailing_line_not_treated_as_fragment(self):
        # A blank last line has no rstrip content → the guard doesn't fire.
        result = _sanitise("Full sentence.\n")
        assert "Full sentence." in result


class TestSanitiseDeduplication:

    def test_non_identical_similar_lines_not_collapsed(self):
        # The regex collapses exact repeats only; variant lines survive.
        text = "Line one.\nLine one.\nLine one different."
        result = _sanitise(text)
        assert result.count("Line one.") == 1
        assert "Line one different." in result

    def test_triple_repeat_collapses_to_one(self):
        text = "Fact.\nFact.\nFact.\nOther."
        result = _sanitise(text)
        assert result.count("Fact.") == 1
        assert "Other." in result


class TestSanitiseUnicode:

    def test_non_ascii_content_passes_through(self):
        text = "Jürgen moved to München in März."
        assert _sanitise(text) == text

    def test_multibyte_cjk_content_passes_through(self):
        text = "東京のオフィスは2024年に閉鎖された。"
        assert _sanitise(text) == text


# ====================================================================
# Action dataclass design
# ====================================================================

class TestActionsDesign:

    def test_evict_is_frozen(self):
        e = Evict(id="x")
        with pytest.raises((AttributeError, TypeError)):
            e.id = "y"  # type: ignore[misc]

    def test_compress_is_frozen(self):
        c = Compress(id="x", new_text="text")
        with pytest.raises((AttributeError, TypeError)):
            c.new_text = "other"  # type: ignore[misc]

    def test_evict_equality(self):
        assert Evict(id="x") == Evict(id="x")
        assert Evict(id="x") != Evict(id="y")

    def test_compress_equality(self):
        assert Compress(id="x", new_text="t") == Compress(id="x", new_text="t")
        assert Compress(id="x", new_text="t") != Compress(id="x", new_text="u")

    def test_evict_is_hashable(self):
        s = {Evict(id="x"), Evict(id="x")}
        assert len(s) == 1

    def test_compress_is_hashable(self):
        s = {Compress(id="x", new_text="t"), Compress(id="x", new_text="t")}
        assert len(s) == 1

    def test_compress_accepts_empty_new_text(self):
        # The dataclass has no validation — policy logic is responsible for
        # rejecting empty compressions before creating a Compress action.
        c = Compress(id="x", new_text="")
        assert c.new_text == ""


# ====================================================================
# SimpleEvictionPolicy design
# ====================================================================

class TestSimpleEvictionPolicyDesign:

    def _concrete_policy(self, victim_id: str):
        from forgetit.policies.base import SimpleEvictionPolicy

        class ConcretePolicy(SimpleEvictionPolicy):
            name = "concrete"
            def select_victim(self, store, candidates=None):
                return victim_id

        return ConcretePolicy()

    def test_select_action_wraps_select_victim_result_in_evict(self):
        policy = self._concrete_policy("target")
        action = policy.select_action(MagicMock())
        assert isinstance(action, Evict)
        assert action.id == "target"

    def test_select_action_propagates_any_victim_id(self):
        for victim_id in ["a", "uuid-style-1234", "long-id-" + "x" * 50]:
            policy = self._concrete_policy(victim_id)
            action = policy.select_action(MagicMock())
            assert action.id == victim_id


# ====================================================================
# MemoryBankCompressionPolicy — construction / prompt wiring
# ====================================================================

class TestMemoryBankConstruction:

    @staticmethod
    def _import():
        from forgetit.policies.ebbinghaus import MemoryBankCompressionPolicy, _MEMORYBANK_PROMPT
        return MemoryBankCompressionPolicy, _MEMORYBANK_PROMPT

    def test_is_subclass_of_llm_compression_policy(self):
        MB, _ = self._import()
        assert isinstance(MB(llm=FakeLLM(), selector=FakeSelector("v")), LLMCompressionPolicy)

    def test_default_prompt_is_memorybank_prompt(self):
        MB, _MEMORYBANK_PROMPT = self._import()
        policy = MB(llm=FakeLLM(), selector=FakeSelector("v"))
        assert policy.prompt_template == _MEMORYBANK_PROMPT

    def test_default_prompt_differs_from_base_compress_prompt(self):
        from forgetit.policies.llm_compression import _COMPRESS_PROMPT
        _, _MEMORYBANK_PROMPT = self._import()
        assert _MEMORYBANK_PROMPT != _COMPRESS_PROMPT

    def test_custom_prompt_overrides_default(self):
        MB, _ = self._import()
        custom = "Summarise: {text}"
        policy = MB(llm=FakeLLM(), selector=FakeSelector("v"), prompt_template=custom)
        assert policy.prompt_template == custom

    def test_name_attribute(self):
        MB, _ = self._import()
        assert MB.name == "memorybank_compression"
