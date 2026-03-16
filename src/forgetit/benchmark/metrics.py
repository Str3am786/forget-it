from __future__ import annotations

from itertools import groupby

from forgetit.benchmark.schema import EvalResult


def gold_survival_rate(results: list[EvalResult]) -> float:
    if not results:
        return 0.0
    return sum(1.0 for r in results if r.gold_survived) / len(results)


def useful_byte_fraction(results: list[EvalResult]) -> float:
    if not results:
        return 0.0
    return sum(r.useful_byte_fraction for r in results) / len(results)


def waste_rate(results: list[EvalResult]) -> float:
    if not results:
        return 0.0
    return sum(r.waste_rate for r in results) / len(results)


def hit_at_k(results: list[EvalResult]) -> float:
    if not results:
        return 0.0
    return sum(r.hit_at_k for r in results) / len(results)


def hit_at_k_given_retained(results: list[EvalResult]) -> float:
    retained = [r for r in results if r.gold_survived]
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


def budget_quality_frontier(
    results: list[EvalResult],
    metric_fn,
) -> dict[int, float]:
    results_sorted = sorted(results, key=lambda r: r.budget_bytes)
    return {
        budget: metric_fn(list(group))
        for budget, group in groupby(results_sorted, key=lambda r: r.budget_bytes)
    }


def summarize(results: list[EvalResult]) -> dict[str, float]:
    return {
        "n_queries": float(len(results)),
        "gold_survival_rate": gold_survival_rate(results),
        "useful_byte_fraction": useful_byte_fraction(results),
        "waste_rate": waste_rate(results),
        "hit_at_k": hit_at_k(results),
        "hit_at_k_given_retained": hit_at_k_given_retained(results),
        "mean_mrr": mean_mrr(results),
        "mean_latency_sec": mean_latency(results),
    }