"""Abstention-aware grading conventions.

Rescued from the deleted tests/llm/test_reader_fixes.py, which died with the
reader's version switches. These three cover grading.py, not the reader, and
nothing else asserts them: an unanswerable question is graded on whether the
reader DECLINED, not on string overlap with an empty reference.
"""
from forgetit.benchmark.grading import grade_with_abstention, is_answerable


def test_is_answerable_conventions():
    assert not is_answerable("q17_abs", "temporal-reasoning")   # LongMemEval suffix
    assert not is_answerable("1::q::3::abstention", "abstention")  # BEAM ability
    assert is_answerable("q17", "temporal-reasoning")


def test_unanswerable_correct_iff_abstained():
    g = grade_with_abstention("IDK", "", abstained=True, answerable=False)
    assert g["correct"] and g["tier"] == "abstain_correct"
    g = grade_with_abstention("The Hilton", "", abstained=False, answerable=False)
    assert not g["correct"] and g["tier"] == "abstain_missed"


def test_answerable_defers_to_legacy_grade():
    g = grade_with_abstention("May 14th", "May 14th", abstained=False, answerable=True)
    assert g["correct"] and g["tier"] == "exact"
    g = grade_with_abstention("IDK", "May 14th", abstained=True, answerable=True)
    assert not g["correct"] and g["tier"] == "abstain"
