"""
B-bucket metrics: rank-percentile + k∝N (B1), censored bounds for legacy
CSVs (B2), coupled-identity aggregation (B3), and the grader embedding tier
plumbing through _run_reader (B4).
"""
from __future__ import annotations

import json

import pytest

from forgetit.benchmark.metrics import (
    censored_bounds_row, coupled_identity, rank_percentile, ranking_columns,
    summarize,
)
from forgetit.benchmark.schema import EvalResult


def _row(ranks, n_res, survived=True, introspectable=True):
    return EvalResult(
        dataset="d", query_id="q", policy="p", budget_bytes=0,
        k=5, itemiser="i", retriever="r", reader="none", seed=0,
        gold_id_retained=survived, useful_byte_fraction=0.0,
        non_gold_byte_fraction=0.0, evictions_total_at_query_time=0,
        compressions=0, hit_at_k=0.0, hit_at_k_given_retained=0.0,
        mrr=0.0, used_bytes_end=0, latency_sec=0.0,
        introspectable=introspectable, store="s",
        n_gold=1, n_retrieved=n_res, n_residents=n_res,
        gold_ranks=json.dumps(ranks),
    )


# ── B1: rank percentile ──────────────────────────────────────────────

class TestRankPercentile:

    def test_first_of_n(self):
        assert rank_percentile(1, 100) == 1.0

    def test_last_of_n(self):
        assert rank_percentile(100, 100) == 0.0

    def test_middle(self):
        assert rank_percentile(51, 101) == pytest.approx(0.5)

    def test_sole_resident(self):
        assert rank_percentile(1, 1) == 1.0

    def test_ranking_columns_conditioned_on_retention(self):
        obs = ranking_columns([20], 1, True, True, n_residents=39)
        assert obs["gold_rank_best"] == 20.0
        assert obs["rank_pct"] == pytest.approx(1 - 19 / 38)
        # evicted gold / opaque store / unknown N → NaN, not zero
        for cols in (ranking_columns([], 1, True, False, n_residents=39),
                     ranking_columns([20], 1, False, False, n_residents=39),
                     ranking_columns([20], 1, True, True, n_residents=0)):
            assert cols["gold_rank_best"] != cols["gold_rank_best"]
            assert cols["rank_pct"] != cols["rank_pct"]

    def test_summary_columns(self):
        # gold at rank 20 of 39 residents → pct = 1 - 19/38 = 0.5
        s = summarize([_row([20], 39)])
        assert s["rank_pct_mean"] == pytest.approx(0.5)
        # k∝N at median N (single row → k=5): rank 20 > 5 → no hit
        assert s["hit_at_5_prop_n"] == 0.0
        assert s["deployment_view_hit_at_5"] == s["hit_at_5"]

    def test_k_prop_n_scales_with_store_size(self):
        # Median store size = 10 → k_row(N=10) = 5, k_row(N=100) = 50.
        # gold at rank 30 in the big store misses fixed k=5 but hits scaled k.
        rows = [_row([3], 10), _row([3], 10), _row([30], 100)]
        s = summarize(rows)
        assert s["hit_at_5"] == pytest.approx(2 / 3)   # fixed k: big store misses
        assert s["hit_at_5_prop_n"] == 1.0             # scaled k: all hit


# ── B2: censored bounds ──────────────────────────────────────────────

class TestCensoredBounds:

    def test_observed_rank_is_exact(self):
        b = censored_bounds_row([3], 1, True, True)
        assert b == {"legacy_censored": 0.0,
                     "gold_rank_best_lo": 3.0, "gold_rank_best_hi": 3.0}

    def test_retained_unobserved_bracketed(self):
        b = censored_bounds_row([], 1, True, True, depth=10)
        assert b["legacy_censored"] == 1.0
        assert b["gold_rank_best_lo"] == 11.0
        assert b["gold_rank_best_hi"] != b["gold_rank_best_hi"]   # NaN: N unknown

    def test_evicted_gold_has_no_bounds(self):
        b = censored_bounds_row([], 1, False, True)
        assert all(v != v for v in b.values())                    # all NaN


# ── B3: coupled identity ─────────────────────────────────────────────

class TestCoupledIdentity:

    def test_history_policies_couple_to_retriever(self):
        assert coupled_identity("lru", "bm25") == "lru×bm25"
        assert coupled_identity("gdsf", "lexical") == "gdsf×lexical"
        assert coupled_identity("gdsf_frozen", "bm25") == "gdsf_frozen×bm25"
        assert coupled_identity("salience_decay", "bm25") == "salience_decay×bm25"
        assert coupled_identity("compression", "bm25") == "compression×bm25"

    def test_non_history_policies_stay_bare(self):
        for p in ("fifo", "random", "oracle", "keep_smallest", "evict_smallest",
                  "semantic", "semantic_coverage", "semantic_keep"):
            assert coupled_identity(p, "bm25") == p


# ── B4: grader embedding tier through _run_reader ────────────────────

class TestGraderEmbeddingTier:

    def _fake_reader(self, answer_text: str):
        from forgetit.core.schema import AnswerResult

        class FakeReader:
            def answer(self, question, records, *, max_tokens=64):
                return AnswerResult(query_id=question.id, answer=answer_text,
                                    abstained=False, latency_sec=0.0)

        return FakeReader()

    def _run(self, answer, ref, embedder=None):
        from forgetit.benchmark.runner_core import _run_reader
        from forgetit.core.schema import Query
        return _run_reader(reader_impl=self._fake_reader(answer),
                           query=Query(text="q?"), retrieved=[],
                           ref_answer=ref,
                           question_type="t", grader_embedder=embedder)

    def test_default_stays_deterministic_only(self):
        # Paraphrase, no embedder → deterministic tiers say wrong (current behavior).
        qa = self._run("a heart doctor", "cardiologist")
        assert qa.qa_correct is False

    def test_embedder_recovers_paraphrase(self):
        class FakeEmbedder:
            def embed_one(self, text):
                # identical vectors → cosine 1.0 for any pair
                return [1.0, 0.0]

        qa = self._run("a heart doctor", "cardiologist", embedder=FakeEmbedder())
        assert qa.qa_correct is True


# ── B3 completeness: partial identities are flagged ──────────────────

class TestGridRaggedness:

    def test_complete_grid_is_silent(self):
        from forgetit.benchmark.metrics import grid_raggedness
        cells = {"lru×bm25": {("lme", "s", "050"), ("lme", "s", "025")},
                 "fifo": {("lme", "s", "050"), ("lme", "s", "025")}}
        assert grid_raggedness(cells) == []

    def test_partial_identity_is_flagged(self):
        from forgetit.benchmark.metrics import grid_raggedness
        # the "new policy, old CSVs lack it" shape: ebbinghaus at one budget only
        cells = {"lru×bm25": {("lme", "s", "050"), ("lme", "s", "025")},
                 "ebbinghaus×bm25": {("lme", "s", "050")}}
        lines = grid_raggedness(cells)
        assert len(lines) == 1
        assert "ebbinghaus×bm25" in lines[0]
        assert "1/2" in lines[0]

    def test_partial_across_datasets_is_flagged(self):
        from forgetit.benchmark.metrics import grid_raggedness
        # present everywhere in LME, absent from the (older) BEAM half
        cells = {"lru×bm25": {("lme", "s", "050"), ("beam", "t", "050")},
                 "evict_smallest": {("lme", "s", "050")}}
        lines = grid_raggedness(cells)
        assert len(lines) == 1 and "evict_smallest" in lines[0]

    def test_empty_grid_is_silent(self):
        from forgetit.benchmark.metrics import grid_raggedness
        assert grid_raggedness({}) == []
