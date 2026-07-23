"""D3 — coupled identity: the (policy, retriever) aggregation unit.

The regression the audit mandates: feed per-cell scores where LRU@bm25 and
LRU@dense differ wildly and assert no UNLABELED output contains their mean.
"""
from __future__ import annotations

from forgetit.benchmark.metrics import (
    by_policy, hit_at_k, is_history_policy, policy_unit, NAME_LEVEL_PREFIX,
)
from forgetit.benchmark.schema import EvalResult


def _row(policy: str, retriever: str, hit: float) -> EvalResult:
    return EvalResult(
        dataset="synthetic", query_id=f"{policy}-{retriever}-{hit}",
        policy=policy, budget_bytes=100, itemiser="t", retriever=retriever,
        reader="none", seed=0, k=5, gold_survived=True,
        useful_byte_fraction=0.0, non_gold_byte_fraction=0.0,
        evictions_total_at_query_time=0, hit_at_k=hit,
        hit_at_k_given_retained=hit, mrr=hit, used_bytes_end=0,
        latency_sec=0.0, introspectable=True,
    )


def _synthetic_results() -> list[EvalResult]:
    # LRU: perfect under bm25, useless under dense — the interaction the
    # aggregation must not launder. Random: a genuinely blind policy.
    rows = []
    rows += [_row("lru", "bm25", 1.0) for _ in range(10)]
    rows += [_row("lru", "dense", 0.0) for _ in range(10)]
    rows += [_row("random", "bm25", 1.0) for _ in range(10)]
    rows += [_row("random", "dense", 0.0) for _ in range(10)]
    return rows


def test_history_policy_keys_carry_the_retriever():
    out = by_policy(_synthetic_results(), hit_at_k)
    assert out["lru@bm25"] == 1.0
    assert out["lru@dense"] == 0.0
    assert "lru" not in out, "bare-name lru key = unlabeled cross-retriever mean"
    # blind policies aggregate under their bare name (their state doesn't
    # depend on the retriever, so name-level is their real identity)
    assert out["random"] == 0.5


def test_no_unlabeled_output_contains_the_laundered_mean():
    out = by_policy(_synthetic_results(), hit_at_k)
    for key, value in out.items():
        if "lru" in key and not key.startswith(NAME_LEVEL_PREFIX):
            assert value in (0.0, 1.0), (
                f"unlabeled key {key!r} holds {value} — the cross-retriever "
                f"mean (0.5) leaked into an unlabeled output"
            )


def test_name_level_view_is_labeled_in_every_key():
    out = by_policy(_synthetic_results(), hit_at_k, name_level_view=True)
    assert all(k.startswith(NAME_LEVEL_PREFIX) for k in out)
    assert out[NAME_LEVEL_PREFIX + "lru"] == 0.5   # the mean exists ONLY here


def test_unit_helpers():
    assert is_history_policy("lru") and is_history_policy("LFU")
    assert not is_history_policy("random")
    assert not is_history_policy("compression")
    assert policy_unit("lru", "bm25") == "lru@bm25"
    assert policy_unit("lru", "") == "lru@unknown"     # missing retriever is loud
    assert policy_unit("keep_smallest", "bm25") == "keep_smallest"
