"""
Provenance columns: every results row is self-describing — two rows from
different configs (or different runs of the code) are distinguishable by
columns alone (git_commit, config_hash, model tags, prompt shas).
"""
from __future__ import annotations

from forgetit.benchmark.pipeline import _config_hash, _git_commit, _stamp_provenance
from forgetit.benchmark.schema import EvalResult


def _row() -> EvalResult:
    return EvalResult(
        dataset="d", query_id="q", policy="p", budget_bytes=0,
        k=5, itemiser="i", retriever="r", reader="none", seed=0,
        gold_survived=True, useful_byte_fraction=0.0, non_gold_byte_fraction=0.0,
        evictions_total_at_query_time=0, compressions=0,
        hit_at_k=0.0, hit_at_k_given_retained=0.0, mrr=0.0,
        used_bytes_end=0, latency_sec=0.0, introspectable=True, store="s",
    )


class TestConfigHash:

    def test_stable_for_identical_configs(self):
        a = _config_hash(dataset="lme", itemiser="session", k=5, seed=42)
        b = _config_hash(dataset="lme", itemiser="session", k=5, seed=42)
        assert a == b

    def test_field_order_does_not_matter(self):
        a = _config_hash(dataset="lme", k=5)
        b = _config_hash(k=5, dataset="lme")
        assert a == b

    def test_any_science_field_changes_the_hash(self):
        base = _config_hash(dataset="lme", itemiser="session", k=5, seed=42,
                            budget_frac=0.5, n_examples=500)
        assert _config_hash(dataset="beam", itemiser="session", k=5, seed=42,
                            budget_frac=0.5, n_examples=500) != base
        assert _config_hash(dataset="lme", itemiser="turn_pair", k=5, seed=42,
                            budget_frac=0.5, n_examples=500) != base
        assert _config_hash(dataset="lme", itemiser="session", k=10, seed=42,
                            budget_frac=0.5, n_examples=500) != base
        assert _config_hash(dataset="lme", itemiser="session", k=5, seed=43,
                            budget_frac=0.5, n_examples=500) != base
        assert _config_hash(dataset="lme", itemiser="session", k=5, seed=42,
                            budget_frac=0.25, n_examples=500) != base
        # a --limit smoke and the full run are different experiments
        assert _config_hash(dataset="lme", itemiser="session", k=5, seed=42,
                            budget_frac=0.5, n_examples=2) != base


class TestStampProvenance:

    def test_fills_every_column(self):
        class FakeLLM:
            model = "llama-test:1b"

        class FakeReader:
            llm = FakeLLM()

        class FakePolicy:
            prompt_template = "Compress: {text}"
            llm = FakeLLM()

        class FakeEmbedder:
            model = "encoder-test:0.6b"

        class FakeStore:
            policy = FakePolicy()
            embedder = FakeEmbedder()

        rows = [_row(), _row()]
        _stamp_provenance(rows, config_hash="abc123",
                          reader_impl=FakeReader(), store_probe=FakeStore())
        for r in rows:
            assert r.config_hash == "abc123"
            assert r.git_commit and r.git_commit != ""
            assert r.llm_model == "llama-test:1b"
            assert r.embed_model == "encoder-test:0.6b"
            assert len(r.prompt_compress_sha) == 12
            assert len(r.prompt_reader_sha) == 12

    def test_reader_off_compression_config_still_records_llm(self):
        # --no-reader compression runs use the chat LLM for summarisation;
        # the tag must come from the policy when there is no reader.
        class FakeLLM:
            model = "llama-test:1b"

        class FakePolicy:
            prompt_template = "Compress: {text}"
            llm = FakeLLM()

        class FakeStore:
            policy = FakePolicy()
            embedder = None

        rows = [_row()]
        _stamp_provenance(rows, config_hash="x",
                          reader_impl=None, store_probe=FakeStore())
        assert rows[0].llm_model == "llama-test:1b"
        assert rows[0].prompt_reader_sha == ""      # no reader ran
        assert rows[0].embed_model == ""

    def test_plain_policy_leaves_llm_fields_blank(self):
        class FakeStore:
            policy = object()      # no prompt_template, no llm
            embedder = None

        rows = [_row()]
        _stamp_provenance(rows, config_hash="x", reader_impl=None,
                          store_probe=FakeStore())
        assert rows[0].llm_model == ""
        assert rows[0].prompt_compress_sha == ""


def test_git_commit_is_cached_and_nonempty():
    a = _git_commit()
    assert a and isinstance(a, str)
    assert _git_commit() is a       # cached
