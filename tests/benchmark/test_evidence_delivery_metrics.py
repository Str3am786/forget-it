"""Reader-context summary stats: evidence_delivery_rate / truncation rate.

These are the gate on every abstention number in the same summary row. A
config with hit@5 = 0.86 and delivery = 0.2 did not measure a memory policy's
answer quality — it measured a prompt budget. The stats must therefore appear
whenever a reader ran, and stay NaN (not 0.0, which reads as "nothing was
delivered") when one did not.
"""
from __future__ import annotations

import math

from forgetit.benchmark.metrics import evidence_delivery_rate, summarize
from forgetit.benchmark.schema import EvalResult


def _row(*, delivered=5, n_retrieved=5, truncated=False, reader="PlainTextReader_rtok1500"):
    return EvalResult(
        dataset="d", query_id="q", policy="p", budget_bytes=0,
        k=5, itemiser="i", retriever="r", reader=reader, seed=0,
        gold_id_retained=True, useful_byte_fraction=0.0, non_gold_byte_fraction=0.0,
        evictions_total_at_query_time=0, compressions=0,
        hit_at_k=1.0, hit_at_k_given_retained=1.0, mrr=1.0,
        used_bytes_end=0, latency_sec=0.0, introspectable=True, store="s",
        n_retrieved=n_retrieved,
        reader_evidence_delivered=delivered,
        reader_evidence_truncated=truncated,
    )


class TestEvidenceDeliveryRate:

    def test_full_delivery(self):
        assert evidence_delivery_rate([_row(), _row()]) == 1.0

    def test_partial_delivery_is_per_row_then_averaged(self):
        # 1-of-5 and 5-of-5 → (0.2 + 1.0) / 2
        rate = evidence_delivery_rate([_row(delivered=1), _row(delivered=5)])
        assert rate == 0.6

    def test_delivery_is_capped_at_the_retrieved_count(self):
        # A reader can't deliver more than it was handed; a miscount must not
        # push the rate above 1.0 and hide a shortfall elsewhere.
        assert evidence_delivery_rate([_row(delivered=9, n_retrieved=5)]) == 1.0

    def test_no_retrieval_leaves_no_denominator(self):
        assert math.isnan(evidence_delivery_rate([_row(n_retrieved=0)]))


class TestSummaryColumns:

    def test_reported_when_a_reader_ran(self):
        s = summarize([_row(delivered=1, truncated=True), _row(delivered=5)])
        assert s["evidence_delivery_rate"] == 0.6
        assert s["mean_evidence_delivered"] == 3.0
        assert s["evidence_truncation_rate"] == 0.5

    def test_nan_on_retrieval_only_runs(self):
        s = summarize([_row(delivered=0, reader="none")])
        assert math.isnan(s["evidence_delivery_rate"])
        assert math.isnan(s["mean_evidence_delivered"])
        assert math.isnan(s["evidence_truncation_rate"])
