"""D4 fix ii — answer-cleaning mode, legacy pinned.

Both modes share one noise-stripping stage; they differ only in whether the
first line or the whole stripped answer survives. The last section is a
CHARACTERIZATION test: it records what abstention detection currently does
with multi-line answers under mode=full. It documents the behavior so a change
is visible in the diff — it is not an endorsement of it. See REPORT notes on
the full+loose interaction.
"""
from forgetit.llm.readers.text_reader import (
    PlainTextReader,
    _clean_answer,
    _is_abstention,
)

LIST_ANSWER = "1. freeze the branch\n2. run the dry-run\n3. apply\n4. restart workers"
NOISY = 'Sure, **May 14th**\n```\nfenced block\n```'


class _NoLLM:
    def generate(self, *a, **k):
        raise AssertionError("cleaning test: the LLM must not be called")


# ── mode behavior ────────────────────────────────────────────────────


def test_firstline_keeps_only_the_first_line():
    assert _clean_answer(LIST_ANSWER) == "1. freeze the branch"
    assert _clean_answer(LIST_ANSWER, mode="firstline") == "1. freeze the branch"


def test_full_keeps_every_line():
    cleaned = _clean_answer(LIST_ANSWER, mode="full")
    assert cleaned.splitlines() == LIST_ANSWER.splitlines()
    for step in ("freeze", "dry-run", "apply", "restart"):
        assert step in cleaned


def test_noise_stripping_is_common_to_both_modes():
    """The shared stage is the invariant: fences, asterisks and preamble go in
    BOTH modes, so switching modes changes only how much survives, never how
    it is scrubbed."""
    first, full = _clean_answer(NOISY), _clean_answer(NOISY, mode="full")
    assert first == full                       # single-line input: modes agree
    assert "**" not in full and "```" not in full and "fenced block" not in full
    assert "May 14th" in full


def test_default_is_firstline_and_name_encodes_full():
    assert PlainTextReader(_NoLLM()).clean_mode == "firstline"
    assert PlainTextReader(_NoLLM()).name == "PlainTextReader"
    assert PlainTextReader(_NoLLM(), clean_mode="full").name == "PlainTextReader_cleanfull"
    combined = PlainTextReader(_NoLLM(), token_budget=1500,
                               prompt_version="v2", clean_mode="full")
    assert combined.name == "PlainTextReader_rtok1500_pv2_cleanfull"


def test_unknown_mode_is_rejected():
    import pytest
    with pytest.raises(ValueError):
        PlainTextReader(_NoLLM(), clean_mode="partial")


# ── characterization: abstention detection sees the CLEANED text ─────


HEDGED_LIST = "1. freeze the branch\n2. run the dry-run\n3. apply\nIDK if there are more."
IDK_THEN_CONTENT = "IDK\nBut the vendor listed is Blue Corp."


def test_full_mode_exposes_later_lines_to_loose_abstention_matching():
    """Measured flip: a list answer whose LAST line hedges is not an abstention
    under firstline (the hedge is discarded with the tail) but IS one under
    full + loose, because loose matches the phrase anywhere in the longer text.
    Recorded because it moves the answer from tier=miss to tier=abstain."""
    assert _is_abstention(_clean_answer(HEDGED_LIST), "loose") is False
    assert _is_abstention(_clean_answer(HEDGED_LIST, mode="full"), "loose") is True
    # exact matching is unaffected — the whole answer is not an abstention phrase
    assert _is_abstention(_clean_answer(HEDGED_LIST, mode="full"), "exact") is False


def test_full_mode_stops_exact_matching_flagging_idk_prefixed_answers():
    """The mirror flip: 'IDK' followed by content normalizes to a bare 'IDK'
    under firstline (exact flags it), but stays a longer string under full
    (exact does not)."""
    assert _is_abstention(_clean_answer(IDK_THEN_CONTENT), "exact") is True
    assert _is_abstention(_clean_answer(IDK_THEN_CONTENT, mode="full"), "exact") is False
    # loose flags it either way
    assert _is_abstention(_clean_answer(IDK_THEN_CONTENT, mode="full"), "loose") is True
