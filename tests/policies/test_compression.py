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


# ====================================================================
# _sanitise — stacking, locale, and structural edge cases
# ====================================================================

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
# Savings threshold boundary conditions
# ====================================================================

class TestSavingsThreshold:

    def _policy_and_store(self, ratio, response, original_size=500):
        llm = FakeLLM(response=response)
        policy = LLMCompressionPolicy(
            llm=llm, selector=FakeSelector("v"), min_saving_ratio=ratio
        )
        store = make_store({"v": FakeRecord(id="v", text="A" * original_size)})
        return policy, store

    def test_saving_exactly_at_threshold_allows_compress(self):
        # saving == min_saving_ratio: condition is `saving < threshold`, so equal → Compress.
        # Original=500, need exactly 50%: compressed=250 bytes.
        policy, store = self._policy_and_store(ratio=0.5, response="B" * 250)
        assert isinstance(policy.select_action(store), Compress)

    def test_saving_one_byte_below_threshold_evicts(self):
        # 251/500 → saving=0.498 < 0.5 → Evict.
        policy, store = self._policy_and_store(ratio=0.5, response="B" * 251)
        assert isinstance(policy.select_action(store), Evict)

    def test_zero_saving_evicts(self):
        policy, store = self._policy_and_store(ratio=0.5, response="A" * 500)
        assert isinstance(policy.select_action(store), Evict)

    def test_record_bytes_field_used_not_recomputed_from_text(self):
        # The policy reads record.bytes; if the caller set it to an unusual value,
        # savings are relative to that, not the text's actual UTF-8 length.
        llm = FakeLLM(response="Short.")  # 6 bytes
        policy = LLMCompressionPolicy(
            llm=llm, selector=FakeSelector("v"), min_saving_ratio=0.5
        )
        # bytes forced to 1000 even though text is short
        record = FakeRecord(id="v", text="Twelve chars", bytes=1000)
        store = make_store({"v": record})
        # saving = 1 - 6/1000 = 0.994 → Compress
        assert isinstance(policy.select_action(store), Compress)

    def test_utf8_multibyte_savings_counted_correctly(self):
        # Japanese text: each char is 3 UTF-8 bytes.
        original_text = "日本語テキスト。" * 30   # 8 chars × 3 bytes × 30 = 720 bytes
        compressed_text = "Short English summary."  # 22 ASCII bytes
        saving = 1 - len(compressed_text.encode()) / len(original_text.encode())

        llm = FakeLLM(response=compressed_text)
        policy = LLMCompressionPolicy(
            llm=llm, selector=FakeSelector("v"), min_saving_ratio=0.5
        )
        store = make_store({"v": FakeRecord(id="v", text=original_text)})
        action = policy.select_action(store)
        assert isinstance(action, Compress), f"saving={saving:.3f} should exceed 0.5"


# ====================================================================
# min_compress_bytes boundary conditions
# ====================================================================

class TestMinCompressBytesThreshold:

    def test_record_one_byte_under_threshold_evicts_without_llm(self):
        llm = FakeLLM()
        policy = LLMCompressionPolicy(
            llm=llm, selector=FakeSelector("v"), min_compress_bytes=200
        )
        record = FakeRecord(id="v", text="X", bytes=199)
        store = make_store({"v": record})
        action = policy.select_action(store)
        assert isinstance(action, Evict)
        assert llm.call_count == 0

    def test_record_exactly_at_threshold_calls_llm(self):
        llm = FakeLLM(response="S")  # 1 byte — saving = 199/200 > 0.5
        policy = LLMCompressionPolicy(
            llm=llm, selector=FakeSelector("v"), min_compress_bytes=200
        )
        record = FakeRecord(id="v", text="X", bytes=200)
        store = make_store({"v": record})
        policy.select_action(store)
        assert llm.call_count == 1

    def test_zero_bytes_record_evicts_without_llm(self):
        # text="" → bytes stays 0 after FakeRecord.__post_init__
        llm = FakeLLM()
        policy = LLMCompressionPolicy(
            llm=llm, selector=FakeSelector("v"), min_compress_bytes=1
        )
        store = make_store({"v": FakeRecord(id="v", text="")})
        action = policy.select_action(store)
        assert isinstance(action, Evict)
        assert llm.call_count == 0


# ====================================================================
# Failure tracking
# ====================================================================

class TestFailureTracking:

    def _fail_policy(self, **kwargs):
        """Policy whose LLM always returns text too large to save (saving=0.2 < 0.5)."""
        llm = FakeLLM(response="B" * 400)
        policy = LLMCompressionPolicy(
            llm=llm,
            selector=FakeSelector("v"),
            min_saving_ratio=0.5,
            **kwargs,
        )
        store = make_store({"v": FakeRecord(id="v", text="A" * 500)})
        return policy, store

    def test_each_failed_compression_increments_counter(self):
        policy, store = self._fail_policy(max_attempts=10)
        for expected in range(1, 4):
            policy.select_action(store)
            assert policy._failures.get("v") == expected

    def test_failure_counters_are_independent_per_victim(self):
        llm = FakeLLM(response="B" * 400)
        policy = LLMCompressionPolicy(
            llm=llm, selector=FakeSelector("placeholder"),
            min_saving_ratio=0.5, max_attempts=5,
        )
        store_a = make_store({"a": FakeRecord(id="a", text="A" * 500)})
        store_b = make_store({"b": FakeRecord(id="b", text="A" * 500)})

        policy.selector = FakeSelector("a")
        policy.select_action(store_a)
        policy.select_action(store_a)

        policy.selector = FakeSelector("b")
        policy.select_action(store_b)

        assert policy._failures.get("a") == 2
        assert policy._failures.get("b") == 1

    def test_max_attempts_one_second_call_evicts_without_llm(self):
        policy, store = self._fail_policy(max_attempts=1)
        policy.select_action(store)  # failure 1 — failure count reaches limit
        calls_before = policy.llm.call_count
        action = policy.select_action(store)
        assert isinstance(action, Evict)
        assert policy.llm.call_count == calls_before  # LLM skipped

    def test_forced_eviction_clears_failure_counter(self):
        policy, store = self._fail_policy(max_attempts=2)
        policy.select_action(store)
        policy.select_action(store)
        # Next call: failures == max_attempts → forced evict → counter deleted
        policy.select_action(store)
        assert "v" not in policy._failures

    def test_llm_exception_does_not_increment_failure_counter(self):
        # Exception path returns Evict immediately — it's a transient fault,
        # not a failed compression attempt, so the counter must stay at zero.
        llm = FakeLLM(should_raise=True)
        policy = LLMCompressionPolicy(
            llm=llm, selector=FakeSelector("v"), max_attempts=3
        )
        store = make_store({"v": FakeRecord(id="v", text="A" * 500)})

        policy.select_action(store)
        policy.select_action(store)

        assert policy._failures.get("v", 0) == 0

    def test_fresh_policy_has_empty_failure_dict(self):
        policy = LLMCompressionPolicy(llm=FakeLLM(), selector=FakeSelector("v"))
        assert policy._failures == {}

    def test_failure_state_is_instance_level(self):
        """Two policy instances never share failure state."""
        llm = FakeLLM(response="B" * 400)
        p1 = LLMCompressionPolicy(
            llm=llm, selector=FakeSelector("v"), min_saving_ratio=0.5
        )
        p2 = LLMCompressionPolicy(
            llm=llm, selector=FakeSelector("v"), min_saving_ratio=0.5
        )
        store = make_store({"v": FakeRecord(id="v", text="A" * 500)})
        p1.select_action(store)
        assert p1._failures.get("v") == 1
        assert p2._failures.get("v", 0) == 0


# ====================================================================
# LLM call parameter forwarding
# ====================================================================

class TestLLMForwarding:

    class RecordingLLM:
        def __init__(self, response="Short summary."):
            self.calls: list[dict] = []
            self._response = response

        def generate(self, prompt, *, max_tokens, temperature):
            self.calls.append(
                {"prompt": prompt, "max_tokens": max_tokens, "temperature": temperature}
            )
            return self._response

    def _setup(self, response="Short summary.", **policy_kwargs):
        llm = self.RecordingLLM(response=response)
        policy = LLMCompressionPolicy(llm=llm, selector=FakeSelector("v"), **policy_kwargs)
        store = make_store({"v": FakeRecord(id="v", text="A" * 500)})
        return policy, store, llm

    def test_record_text_appears_in_prompt(self):
        policy, store, llm = self._setup()
        policy.select_action(store)
        assert "A" * 500 in llm.calls[0]["prompt"]

    def test_max_tokens_forwarded(self):
        policy, store, llm = self._setup(max_tokens=64)
        policy.select_action(store)
        assert llm.calls[0]["max_tokens"] == 64

    def test_temperature_is_always_zero(self):
        policy, store, llm = self._setup()
        policy.select_action(store)
        assert llm.calls[0]["temperature"] == 0.0

    def test_custom_prompt_template_used(self):
        policy, store, llm = self._setup(prompt_template="CUSTOM: {text} :END")
        policy.select_action(store)
        prompt = llm.calls[0]["prompt"]
        assert prompt.startswith("CUSTOM:")
        assert prompt.endswith(":END")

    def test_llm_not_called_for_missing_record(self):
        llm = FakeLLM()
        policy = LLMCompressionPolicy(llm=llm, selector=FakeSelector("v"))
        policy.select_action(make_store({}))
        assert llm.call_count == 0

    def test_llm_not_called_when_max_attempts_already_reached(self):
        llm = FakeLLM()
        policy = LLMCompressionPolicy(
            llm=llm, selector=FakeSelector("v"), max_attempts=1
        )
        store = make_store({"v": FakeRecord(id="v", text="A" * 500)})
        policy._failures["v"] = 1  # already at the limit
        policy.select_action(store)
        assert llm.call_count == 0


# ====================================================================
# Architectural / design invariants
# ====================================================================

class TestPolicyArchitecture:

    def test_llm_compression_policy_is_an_eviction_policy(self):
        from forgetit.policies.base import EvictionPolicy
        policy = LLMCompressionPolicy(llm=FakeLLM(), selector=FakeSelector("v"))
        assert isinstance(policy, EvictionPolicy)

    def test_select_action_only_returns_evict_or_compress(self):
        scenarios = [
            ("Short.", {"v": FakeRecord(id="v", text="A" * 500)}),  # compress path
            ("B" * 400, {"v": FakeRecord(id="v", text="A" * 500)}),  # saving too small
            ("Short.", {}),                                            # record missing
        ]
        for response, records in scenarios:
            policy = LLMCompressionPolicy(
                llm=FakeLLM(response=response), selector=FakeSelector("v")
            )
            action = policy.select_action(make_store(records))
            assert isinstance(action, (Evict, Compress)), f"Unexpected action for {response!r}"

    def test_selector_called_exactly_once_per_action(self):
        """Policy never calls select_victim more than once — no retry inside select_action."""
        call_count = [0]

        class CountingSelector:
            def select_victim(self, store):
                call_count[0] += 1
                return "v"

        policy = LLMCompressionPolicy(llm=FakeLLM(), selector=CountingSelector())
        store = make_store({"v": FakeRecord(id="v", text="A" * 500)})
        policy.select_action(store)
        assert call_count[0] == 1

    def test_max_attempts_zero_evicts_immediately_without_llm(self):
        # _failures.get(id, 0) == 0 >= max_attempts=0 → forced evict on first call.
        llm = FakeLLM()
        policy = LLMCompressionPolicy(
            llm=llm, selector=FakeSelector("v"), max_attempts=0
        )
        store = make_store({"v": FakeRecord(id="v", text="A" * 500)})
        action = policy.select_action(store)
        assert isinstance(action, Evict)
        assert llm.call_count == 0

    def test_policy_does_not_write_to_store(self):
        """select_action is read-only. The RetentionManager, not the policy, executes mutations."""
        llm = FakeLLM(response="Short summary.")
        policy = LLMCompressionPolicy(llm=llm, selector=FakeSelector("v"))
        store = make_store({"v": FakeRecord(id="v", text="A" * 500)})
        policy.select_action(store)
        store.backend.upsert.assert_not_called()
        store.backend.delete.assert_not_called()

    def test_has_name_attribute(self):
        assert LLMCompressionPolicy.name == "llm_compression"


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
            def select_victim(self, store):
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
# MemoryBankCompressionPolicy
# ====================================================================

class TestMemoryBankCompressionPolicy:

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

    def test_compresses_successfully(self):
        MB, _ = self._import()
        policy = MB(llm=FakeLLM(response="Short summary."), selector=FakeSelector("v"))
        store = make_store({"v": FakeRecord(id="v", text="A" * 500)})
        action = policy.select_action(store)
        assert isinstance(action, Compress)
        assert action.new_text == "Short summary."

    def test_prompt_contains_record_text(self):
        MB, _ = self._import()
        calls: list[str] = []

        class Rec:
            def generate(self, prompt, *, max_tokens, temperature):
                calls.append(prompt)
                return "Short."

        policy = MB(llm=Rec(), selector=FakeSelector("v"))
        store = make_store({"v": FakeRecord(id="v", text="A" * 500)})
        policy.select_action(store)
        assert "A" * 500 in calls[0]

    def test_inherits_failure_tracking(self):
        MB, _ = self._import()
        llm = FakeLLM(response="B" * 400)  # saving insufficient
        policy = MB(llm=llm, selector=FakeSelector("v"), min_saving_ratio=0.5)
        store = make_store({"v": FakeRecord(id="v", text="A" * 500)})
        policy.select_action(store)
        assert policy._failures.get("v") == 1