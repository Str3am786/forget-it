"""An infrastructure failure is not a wrong answer.

Before this, a reader call that raised was recorded with abstained=True and
qa_correct=False, so a flaky endpoint depressed qa_accuracy and — on an
unanswerable question — a timeout scored as a correct refusal. Errored rows now
leave both the numerator and the denominator of every grade, and get their own
rate so the exclusion is visible rather than silent.
"""
from __future__ import annotations

import math

from forgetit.benchmark.metrics import (
    abstention_rate, accuracy_given_gold_retrieved, hallucination_rate,
    reader_error_rate, summarize,
)
from forgetit.benchmark.schema import EvalResult


def _row(*, correct=True, abstained=False, errored=False, hit=1.0, f1=1.0):
    return EvalResult(
        dataset="d", query_id="q", policy="p", budget_bytes=0,
        k=5, itemiser="i", retriever="r", reader="reader_m_rtok16000_gen256",
        seed=0, gold_id_retained=True, useful_byte_fraction=0.0,
        non_gold_byte_fraction=0.0, evictions_total_at_query_time=0,
        compressions=0, hit_at_k=hit, hit_at_k_given_retained=hit, mrr=1.0,
        used_bytes_end=0, latency_sec=0.0, introspectable=True, store="s",
        n_retrieved=5, qa_correct=correct, qa_f1=f1, abstained=abstained,
        reader_errored=errored,
    )


def test_errored_rows_leave_the_accuracy_denominator():
    """Two correct answers and two failed calls is 100% accuracy, not 50%."""
    rows = [_row(correct=True), _row(correct=True),
            _row(correct=False, errored=True), _row(correct=False, errored=True)]
    assert summarize(rows)["qa_accuracy"] == 1.0
    assert summarize(rows)["mean_qa_f1"] == 1.0


def test_error_rate_is_reported_over_every_row():
    rows = [_row(), _row(), _row(errored=True), _row(errored=True)]
    s = summarize(rows)
    assert s["reader_error_rate"] == 0.5
    assert s["n_queries"] == 4.0        # nothing is hidden from the row count
    assert s["n_scored"] == 2.0


def test_errored_rows_do_not_count_as_refusals():
    """The exact defect: an errored row carries abstained=False, so leaving it
    in the denominator understated abstention instead of overstating it — and
    either way the number described a different population than the accuracy."""
    rows = [_row(abstained=True), _row(abstained=False), _row(errored=True)]
    assert abstention_rate(rows) == 0.5          # 1 of the 2 scored rows
    assert reader_error_rate(rows) == 1 / 3


def test_errored_rows_leave_the_conditional_and_hallucination_rates():
    rows = [_row(correct=True, hit=1.0),
            _row(correct=False, errored=True, hit=1.0)]
    # Given gold was in context, the reader got the one answer it produced right.
    assert accuracy_given_gold_retrieved(rows) == 1.0
    # An unanswered row is not "confidently wrong".
    assert hallucination_rate(rows) == 0.0


def test_all_errored_is_nan_or_zero_not_a_false_zero_accuracy():
    """A config whose endpoint was down must not report 0.0 accuracy as though
    the reader answered everything wrong."""
    rows = [_row(correct=False, errored=True) for _ in range(3)]
    s = summarize(rows)
    assert s["reader_error_rate"] == 1.0
    assert s["n_scored"] == 0.0
    assert s["qa_accuracy"] == 0.0 or math.isnan(s["qa_accuracy"])
    assert math.isnan(accuracy_given_gold_retrieved(rows))


def test_rows_predating_the_column_are_not_errored():
    """Old CSVs have no reader_errored; missing must read as False, not as a
    crash and not as 'everything failed'."""
    rows = [{"reader_errored": None}, {}, {"reader_errored": "false"}]
    # dicts: getattr would return the default unconditionally, so the helper
    # has to branch on type. Only the last is truthy-as-string.
    assert reader_error_rate(rows) == 1 / 3


def test_no_errors_leaves_every_number_unchanged():
    # An abstention grades as wrong, so 1 of 3 is correct.
    rows = [_row(correct=True), _row(correct=False),
            _row(correct=False, abstained=True)]
    s = summarize(rows)
    assert s["reader_error_rate"] == 0.0
    assert s["n_scored"] == s["n_queries"] == 3.0
    assert s["qa_accuracy"] == 1 / 3
    assert abstention_rate(rows) == 1 / 3
