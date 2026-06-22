from __future__ import annotations

from collections import defaultdict
from itertools import groupby

from forgetit.benchmark.schema import EvalResult


# ── primitive aggregates ──────────────────────────────────────────
# Retention-specific ones (gold_survival, useful_byte_fraction, non_gold_byte_fraction,
# hit_at_k_given_retained) only average over rows where introspectable=True,
# because opaque stores (Letta, Mem0) carry sentinel values in those fields.
# Retrieval and latency metrics are meaningful for all stores.


def _introspectable(results: list[EvalResult]) -> list[EvalResult]:
    return [r for r in results if r.introspectable]


def gold_survival_rate(results: list[EvalResult]) -> float:
    introspectable = _introspectable(results)
    if not introspectable:
        return 0.0
    return sum(1.0 for r in introspectable if r.gold_survived) / len(introspectable)


def useful_byte_fraction(results: list[EvalResult]) -> float:
    introspectable = _introspectable(results)
    if not introspectable:
        return 0.0
    return sum(r.useful_byte_fraction for r in introspectable) / len(introspectable)


def non_gold_byte_fraction(results: list[EvalResult]) -> float:
    introspectable = _introspectable(results)
    if not introspectable:
        return 0.0
    return sum(r.non_gold_byte_fraction for r in introspectable) / len(introspectable)


def hit_at_k(results: list[EvalResult]) -> float:
    if not results:
        return 0.0
    return sum(r.hit_at_k for r in results) / len(results)


def hit_at_k_given_retained(results: list[EvalResult]) -> float:
    # Only meaningful for introspectable rows — for opaque stores we can't
    # tell whether the gold item is still resident.
    retained = [r for r in results if r.introspectable and r.gold_survived]
    if not retained:
        return 0.0
    return sum(r.hit_at_k for r in retained) / len(retained)


def mean_mrr(results: list[EvalResult]) -> float:
    if not results:
        return 0.0
    return sum(r.mrr for r in results) / len(results)


def mean_latency(results: list[EvalResult]) -> float:
    if not results:
        return 0.0
    return sum(r.latency_sec for r in results) / len(results)


def mean_qa_latency(results: list[EvalResult]) -> float:
    """Reader latency (separate from retrieval latency)."""
    if not results:
        return 0.0
    return sum(r.qa_latency_sec for r in results) / len(results)


def abstention_rate(results: list[EvalResult]) -> float:
    if not results:
        return 0.0
    return sum(1.0 for r in results if r.abstained) / len(results)


# ── breakdowns ────────────────────────────────────────────────────


def budget_quality_frontier(
    results: list[EvalResult],
    metric_fn,
) -> dict[int, float]:
    """Group by budget_bytes and apply a metric per group."""
    results_sorted = sorted(results, key=lambda r: r.budget_bytes)
    return {
        budget: metric_fn(list(group))
        for budget, group in groupby(results_sorted, key=lambda r: r.budget_bytes)
    }


def by_question_type(
    results: list[EvalResult],
    metric_fn,
) -> dict[str, float]:
    """
    Group by ``question_type`` and apply a metric per group.

    For BEAM this gives you the 10-way memory-ability breakdown
    (abstention / contradiction_resolution / event_ordering / ...).
    For LongMemEval it gives you the dataset's own question-type split.
    """
    buckets: dict[str, list[EvalResult]] = defaultdict(list)
    for r in results:
        buckets[r.question_type or "_unknown"].append(r)
    return {qt: metric_fn(rs) for qt, rs in sorted(buckets.items())}


def by_store(
    results: list[EvalResult],
    metric_fn,
) -> dict[str, float]:
    """Group by ``store`` label — useful when comparing RetentionManager vs Letta."""
    buckets: dict[str, list[EvalResult]] = defaultdict(list)
    for r in results:
        buckets[r.store or "_unknown"].append(r)
    return {s: metric_fn(rs) for s, rs in sorted(buckets.items())}


# ── summary ───────────────────────────────────────────────────────


def summarize(results: list[EvalResult]) -> dict[str, float]:
    """
    One-row summary for a single ``run_pipeline`` call.  Safe for both
    introspectable and opaque stores — retention-specific fields are
    averaged only over introspectable rows, so an all-opaque run returns
    0.0 for those and the reader-layer / retrieval-layer metrics remain
    meaningful.
    """
    intro = _introspectable(results)
    n = len(results)
    mean = lambda f: (sum(f(r) for r in results) / n) if n else 0.0
    return {
        "n_queries": float(n),
        "n_introspectable": float(len(intro)),
        "gold_survival_rate": gold_survival_rate(results),
        "useful_byte_fraction": useful_byte_fraction(results),
        "non_gold_byte_fraction": non_gold_byte_fraction(results),
        "hit_at_k": hit_at_k(results),
        "hit_at_k_given_retained": hit_at_k_given_retained(results),
        "mean_mrr": mean_mrr(results),
        # per-query activity (deltas), averaged across queries
        "mean_inserts_per_query": mean(lambda r: r.inserts_since_last_query),
        "mean_evictions_per_query": mean(lambda r: r.evictions_since_last_query),
        "mean_compressions_per_query": mean(lambda r: r.compressions_since_last_query),
        "mean_latency_sec": mean_latency(results),
        "mean_qa_latency_sec": mean_qa_latency(results),
        "abstention_rate": abstention_rate(results),
    }