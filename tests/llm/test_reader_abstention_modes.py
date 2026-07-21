"""D4 fix vii (detection half) — abstention matching mode, legacy pinned.

Detection answers "did the reader refuse?". Whether refusing was CORRECT is a
grading concern and lives elsewhere. The last section is a CHARACTERIZATION
test of the detection→grading coupling: it records what a loose false positive
costs, so the cost is visible in a diff if anything moves. It does not endorse
the behavior and changes nothing on the grading side.
"""
from forgetit.benchmark.grading import grade
from forgetit.llm.readers.text_reader import (
    PlainTextReader,
    _clean_answer,
    _is_abstention,
)

# The measured false-positive class: informative answer, trailing hedge.
HEDGED = "IDK if there were others, but the rent was $2,100"
BARE = "IDK"
PLAIN = "The rent was $2,100"
IDK_THEN_CONTENT = "IDK\nBut the lease shows the rent was $2,100 per month."


class _NoLLM:
    def generate(self, *a, **k):
        raise AssertionError("detection test: the LLM must not be called")


# ── mode behavior ────────────────────────────────────────────────────


def test_bare_refusal_flags_under_both_modes():
    """The form both prompts instruct. If exact ever stopped catching it,
    every genuine refusal would silently become a hallucination in the grades."""
    assert _is_abstention(BARE) is True
    assert _is_abstention(BARE, mode="loose") is True
    assert _is_abstention(BARE, mode="exact") is True
    assert _is_abstention("idk.", mode="exact") is True     # normalization holds


def test_loose_flags_hedged_informative_answer_the_defect():
    assert _is_abstention(HEDGED) is True                    # legacy default
    assert _is_abstention(HEDGED, mode="loose") is True


def test_exact_keeps_hedged_informative_answer():
    assert _is_abstention(HEDGED, mode="exact") is False


def test_plain_answer_flags_under_neither_mode():
    assert _is_abstention(PLAIN, mode="loose") is False
    assert _is_abstention(PLAIN, mode="exact") is False


def test_default_is_loose_and_name_encodes_exact():
    assert PlainTextReader(_NoLLM()).abstention_match == "loose"
    assert PlainTextReader(_NoLLM()).name == "PlainTextReader"
    assert PlainTextReader(_NoLLM(), abstention_match="exact").name == "PlainTextReader_absexact"
    every = PlainTextReader(_NoLLM(), token_budget=1500, prompt_version="v2",
                            clean_mode="full", abstention_match="exact")
    assert every.name == "PlainTextReader_rtok1500_pv2_absexact_cleanfull"


def test_unknown_mode_is_rejected():
    import pytest
    with pytest.raises(ValueError):
        PlainTextReader(_NoLLM(), abstention_match="fuzzy")


# ── characterization: clean_mode × abstention_match ──────────────────


def test_only_full_plus_exact_recovers_an_idk_prefixed_answer():
    """Measured 2x2 on 'IDK\\nBut ... $2,100'. firstline discards the content
    before detection ever sees it, so no abstention mode can recover it; under
    full, loose still matches the leading IDK. Exactly one cell keeps the
    answer — the two flags are coupled, not independent."""
    verdicts = {
        (clean, match): _is_abstention(_clean_answer(IDK_THEN_CONTENT, mode=clean), mode=match)
        for clean in ("firstline", "full")
        for match in ("loose", "exact")
    }
    assert verdicts == {
        ("firstline", "loose"): True,
        ("firstline", "exact"): True,
        ("full", "loose"): True,
        ("full", "exact"): False,       # the only cell that keeps the content
    }


# ── characterization: detection → grading coupling ───────────────────


def test_loose_false_positive_zeroes_an_otherwise_correct_row():
    """grade() short-circuits on abstained= before any matching runs, so a
    detection false positive does not merely mislabel the row: it forces
    correct=False, zeroes f1, and files the row under tier='abstain', where it
    is indistinguishable from a genuine refusal. Direction of the v1 bias."""
    ref = "$2,100"
    as_loose = grade(HEDGED, ref, abstained=_is_abstention(HEDGED, mode="loose"))
    as_exact = grade(HEDGED, ref, abstained=_is_abstention(HEDGED, mode="exact"))

    assert as_loose == {"correct": False, "f1": 0.0, "tier": "abstain",
                        "similarity": as_loose["similarity"]}
    assert as_exact["correct"] is True
    assert as_exact["tier"] == "contains"
    assert as_exact["f1"] > 0.0

    # The answer text survives either way, so v1 rows stay re-gradeable offline.
    assert "2,100" in HEDGED
