from __future__ import annotations

import json
import math
from collections import defaultdict
from itertools import groupby

from forgetit.benchmark.schema import EvalResult


# Cutoffs for the retrieval curve. Reporting Hit@k / nDCG@k as a curve (not one
# fixed k) is diagnostic: if Hit@1 is low but Hit@10 is high, gold is retrieved
# but ranked poorly (a ranking problem); if even Hit@10 is low, gold isn't
# surfacing at all (a recall/candidate problem — likely forgetting, not the
# retriever). The runner retrieves to max(EVAL_K_CURVE) so all cutoffs are
# computable from one ranked list.
EVAL_K_CURVE: tuple[int, ...] = (1, 3, 5, 10)


# ── ranking curve (Hit@k / nDCG@k / Recall@k from stored gold ranks) ──
# All four derive from one per-row sufficient statistic: gold_ranks, the 1-based
# ranks of gold items in the ranked list (stored as a JSON string in EvalResult).
# Binary relevance (gold vs not). Computed here so the metric set / cutoffs can
# change without re-running the benchmark.


def _gold_ranks(r: EvalResult) -> list[int]:
    gr = getattr(r, "gold_ranks", None)
    if not gr:
        return []
    if isinstance(gr, list):
        return gr
    try:
        return [int(x) for x in json.loads(gr)]
    except (ValueError, TypeError):
        return []


# Primitive math on a single query's (ranks, n_gold). The per-row columns in
# results.csv, the per-row helpers, and the summary aggregates all route through
# these three, so the materialised columns and the computed curves can't diverge.

def _hit(ranks: list[int], k: int) -> float:
    """1 if any gold item is in the top-k, else 0 (== Recall@k for single-gold)."""
    return 1.0 if any(rank <= k for rank in ranks) else 0.0


def _ndcg(ranks: list[int], n_gold: int, k: int) -> float:
    """nDCG@k, binary relevance, log2 position discount (Järvelin & Kekäläinen).
    Well-defined for single-gold (IDCG@k = 1, so it reduces to 1/log2(rank+1) — a
    log-discounted reciprocal rank, never N/A); its real value is BEAM multi-gold."""
    if n_gold <= 0:
        return 0.0
    dcg = sum(1.0 / math.log2(rank + 1) for rank in ranks if rank <= k)
    idcg = sum(1.0 / math.log2(i + 1) for i in range(1, min(k, n_gold) + 1))
    return (dcg / idcg) if idcg > 0 else 0.0


def _recall(ranks: list[int], n_gold: int, k: int) -> float:
    """Fraction of this query's gold items found in the top-k (raw — multi-gold
    scoping happens downstream)."""
    if n_gold <= 0:
        return 0.0
    return sum(1 for rank in ranks if rank <= k) / n_gold


def ranking_columns(gold_ranks: list[int], n_gold: int,
                    introspectable: bool, gold_survived: bool) -> dict[str, float]:
    """The per-(metric, k) scalar columns for ONE results.csv row.

    A row is a single query, so the "curve" isn't a per-row object — it's these
    flat scalars that become a curve when averaged across rows at plot time.
    Hit@k / nDCG@k / Recall@k are computed for every row (Recall raw — its
    multi-gold scoping is a downstream filter on question_type, not stored here).
    hit_at_k_given_retained is NaN unless the gold was resident, mirroring the
    existing scalar — averaging the non-NaN rows gives the conditioned curve.
    """
    ranks = gold_ranks or []
    n_gold = n_gold or 0
    cols: dict[str, float] = {}
    for k in EVAL_K_CURVE:
        h = _hit(ranks, k)
        cols[f"hit_at_{k}"] = h
        cols[f"hit_at_{k}_given_retained"] = h if (introspectable and gold_survived) else float("nan")
        cols[f"ndcg_at_{k}"] = _ndcg(ranks, n_gold, k)
        cols[f"recall_at_{k}"] = _recall(ranks, n_gold, k)
    return cols


# ── composition-invariant rank metrics (the k-confound fix) ─────────
# At equal BYTE budget, different policies hold different ITEM counts; every
# fixed-k cutoff above therefore penalizes item-rich stores (compression,
# keep-smallest) with more distractors — it measures store composition, not
# policy merit. Two invariant alternatives, both derived from the same
# per-row sufficient statistic (gold_ranks) plus the resident count:
#
#   gold_rank_percentile — best gold rank ÷ n_resident: WHERE the gold sits in
#     the ranking, normalized by how many items were being ranked.
#   hit@kprop — hit under a PROPORTIONAL cutoff k = max(k_floor, ceil(c·n)):
#     the sensitivity analysis of the same idea (k scales with the store).
#
# Censoring: gold_ranks is only logged to the eval depth (max(EVAL_K_CURVE)).
# A surviving gold that surfaced nowhere within depth has an UNKNOWN rank in
# (depth, n_resident] — percentile is right-censored, not zero and not 1.
# Aggregations must carry bounds (censored→depth+1/n optimistic, →1.0
# pessimistic) or report the censored fraction alongside.


def gold_rank_percentile(ranks: list[int], n_resident: int) -> float:
    """Best-gold rank ÷ resident count ∈ (0, 1]; lower = better. NaN when no
    gold rank was observed (censored or evicted) or n_resident is unknown."""
    if not ranks or n_resident <= 0:
        return float("nan")
    return min(ranks) / n_resident


def kprop(n_resident: int, c: float, k_floor: int = 5) -> int:
    """Proportional cutoff k = max(k_floor, ceil(c·n_resident)). Calibrate c so
    the MEDIAN store of the comparison population gets k = k_floor
    (c = k_floor / median(n_resident))."""
    if n_resident <= 0:
        return k_floor
    return max(k_floor, math.ceil(c * n_resident))


def composition_columns(gold_ranks: list[int], n_resident: int,
                        introspectable: bool, gold_survived: bool) -> dict[str, float]:
    """Per-row columns: n_resident_at_query / gold_rank_percentile /
    gold_rank_censored (see EvalResult docs). hit@kprop is NOT materialised
    per-row because its c is a property of the comparison population, not of
    one run — compute it offline from these columns."""
    ranks = gold_ranks or []
    pct = gold_rank_percentile(ranks, n_resident)
    if not introspectable or not gold_survived:
        censored = float("nan")      # evicted / unknowable — not a rank question
    elif ranks:
        censored = 0.0               # observed
    else:
        censored = 1.0               # survived but beyond the eval depth
    return {
        "n_resident_at_query": n_resident,
        "gold_rank_percentile": pct,
        "gold_rank_censored": censored,
    }


def _row_hit_at(r: EvalResult, k: int) -> float:
    return _hit(_gold_ranks(r), k)


def _row_ndcg_at(r: EvalResult, k: int) -> float:
    return _ndcg(_gold_ranks(r), getattr(r, "n_gold", 0) or 0, k)


def _row_recall_at(r: EvalResult, k: int) -> float:
    return _recall(_gold_ranks(r), getattr(r, "n_gold", 0) or 0, k)


def hit_at(results: list[EvalResult], k: int) -> float:
    return sum(_row_hit_at(r, k) for r in results) / len(results) if results else 0.0


def ndcg_at(results: list[EvalResult], k: int) -> float:
    return sum(_row_ndcg_at(r, k) for r in results) / len(results) if results else 0.0


def recall_at(results: list[EvalResult], k: int) -> float:
    """Recall@k averaged over MULTI-GOLD rows only (n_gold > 1).

    With a single relevant item Recall@k == Hit@k, so pooling single-gold queries
    here would just duplicate Hit@k. Returns NaN when there are no multi-gold
    rows (e.g. LongMemEval, which is single-ref) — that's "not applicable", not 0.
    """
    multi = [r for r in results if (getattr(r, "n_gold", 0) or 0) > 1]
    if not multi:
        return float("nan")
    return sum(_row_recall_at(r, k) for r in multi) / len(multi)


def hit_at_given_retained(results: list[EvalResult], k: int) -> float:
    """Hit@k conditioned on the gold item still being resident (gold_survived).

    This is the star metric: plain Hit@k conflates two failures — the policy
    evicted the gold, and the retriever missed gold that was still there.
    Conditioning on survival isolates *retriever quality given the item is kept*.
    Reported as a curve PAIRED with hit_at(): the gap (hit_at − this) is the
    forgetting cost; the level of this one is the retriever's quality.
    NaN when nothing survived (can't measure retrieval over an empty kept set).
    """
    retained = [r for r in results if r.introspectable and r.gold_survived]
    if not retained:
        return float("nan")
    return sum(_row_hit_at(r, k) for r in retained) / len(retained)


# ── primitive aggregates ──────────────────────────────────────────
# Retention-specific ones (gold_survival, useful_byte_fraction, non_gold_byte_fraction,
# hit_at_k_given_retained) only average over rows where introspectable=True,
# because opaque stores (Letta, Mem0) carry sentinel values in those fields.
# Retrieval and latency metrics are meaningful for all stores.


def _introspectable(results: list[EvalResult]) -> list[EvalResult]:
    return [r for r in results if r.introspectable]


def _has_reader(results: list[EvalResult]) -> bool:
    """True if a real reader ran (so QA grades are meaningful). The reader name is
    "" / "none" for retrieval-only runs — both are falsey for grading."""
    return any(r.reader and r.reader != "none" for r in results)


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


def accuracy_given_gold_retrieved(results: list[EvalResult]) -> float:
    """Reader correctness conditioned on the gold item being IN the reader's
    context (hit_at_k > 0). The reader's analogue of hit_at_k_given_retained:
    it isolates pure reader quality from upstream forgetting/retrieval — given
    the answer was actually in front of the reader, did it get it right? The most
    important reader metric in this harness. NaN if gold never reached context."""
    ctx = [r for r in results if r.introspectable and r.hit_at_k > 0]
    if not ctx:
        return float("nan")
    return sum(1.0 for r in ctx if r.qa_correct) / len(ctx)


def hallucination_rate(results: list[EvalResult]) -> float:
    """Fraction of queries answered (not abstained) but WRONG — confidently wrong.
    The failure mode that lowering abstention hides."""
    if not results:
        return float("nan")
    return sum(1.0 for r in results if (not r.abstained) and (not r.qa_correct)) / len(results)


def abstention_answerable_rate(results: list[EvalResult]) -> float:
    """Of abstentions, the fraction that were actually answerable (gold WAS
    retrieved into context). High == the reader is declining on present answers
    (missed answers); low == abstaining mostly when the answer wasn't there
    (honest 'I don't know'). NaN if there were no abstentions."""
    ab = [r for r in results if r.abstained]
    if not ab:
        return float("nan")
    return sum(1.0 for r in ab if r.hit_at_k > 0) / len(ab)


def mean_query_coverage(results: list[EvalResult]) -> float:
    """Mean pre-retrieval retrievability (cosine query→nearest resident item),
    over rows where it's defined (dense configs). NaN when no row has it — the
    embedding-native signal of whether the kept set can answer, independent of
    the ranker. A policy that keeps high-coverage residents will retrieve well;
    one that evicts by recency won't — measurable without running the retriever.
    """
    vals = [r.query_coverage for r in results
            if isinstance(r.query_coverage, (int, float)) and not math.isnan(r.query_coverage)]
    return (sum(vals) / len(vals)) if vals else float("nan")


# ── policy aggregation unit (coupled identity, D3) ──────────────────
# LRU/LFU build their state from retrieval TOUCHES, and touches come from the
# retriever — so "lru under bm25" and "lru under dense" are different policies
# sharing a name. Averaging a history policy's scores across retrievers
# launders a policy×retriever interaction into a "policy" score. The
# aggregation unit for these policies is therefore (policy, retriever);
# name-level pooling is allowed ONLY in outputs explicitly labeled
# name_level_view (and by_policy stamps that label into every key so it
# travels with the numbers into any table they land in).

# Every touch-based policy must be listed here (exact lowercase name as it
# appears in the results `policy` column). Blind/content/size policies
# aggregate under their bare name.
HISTORY_POLICIES = frozenset({"lru", "lfu"})

NAME_LEVEL_PREFIX = "name_level_view:"


def is_history_policy(policy: str) -> bool:
    return (policy or "").strip().lower() in HISTORY_POLICIES


def policy_unit(policy: str, retriever: str) -> str:
    """The key a policy's scores may be pooled under: ``lru@bm25`` for history
    policies (their identity includes the touch source), bare name otherwise."""
    if is_history_policy(policy):
        return f"{policy}@{retriever or 'unknown'}"
    return policy


def by_policy(results: list[EvalResult], metric_fn,
              *, name_level_view: bool = False) -> dict[str, float]:
    """Group by the POLICY AGGREGATION UNIT and apply a metric per group.

    Default: history policies (LRU/LFU) key as ``policy@retriever`` — no
    unlabeled cross-retriever mean can exist in the output. With
    ``name_level_view=True`` every key is prefixed ``name_level_view:`` and
    history policies pool across retrievers — the label is part of the key
    precisely so it survives into whatever CSV/table the dict is written to.
    """
    buckets: dict[str, list[EvalResult]] = defaultdict(list)
    for r in results:
        if name_level_view:
            key = NAME_LEVEL_PREFIX + (r.policy or "_unknown")
        else:
            key = policy_unit(r.policy or "_unknown", r.retriever or "")
        buckets[key].append(r)
    return {k: metric_fn(rs) for k, rs in sorted(buckets.items())}


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
    n_multigold = sum(1 for r in results if (getattr(r, "n_gold", 0) or 0) > 1)
    mean = lambda f: (sum(f(r) for r in results) / n) if n else 0.0
    out = {
        "n_queries": float(n),
        "n_introspectable": float(len(intro)),
        "n_multigold": float(n_multigold),
        "gold_survival_rate": gold_survival_rate(results),
        "useful_byte_fraction": useful_byte_fraction(results),
        "non_gold_byte_fraction": non_gold_byte_fraction(results),
        "hit_at_k": hit_at_k(results),                       # at the configured k
        "hit_at_k_given_retained": hit_at_k_given_retained(results),
        "mean_mrr": mean_mrr(results),
        "mean_n_gold": mean(lambda r: getattr(r, "n_gold", 0) or 0),
        # per-query activity (deltas), averaged across queries
        "mean_inserts_per_query": mean(lambda r: r.inserts_since_last_query),
        "mean_evictions_per_query": mean(lambda r: r.evictions_since_last_query),
        "mean_compressions_per_query": mean(lambda r: r.compressions_since_last_query),
        "mean_latency_sec": mean_latency(results),
        "mean_qa_latency_sec": mean_qa_latency(results),
        "abstention_rate": abstention_rate(results),
        "mean_query_coverage": mean_query_coverage(results),   # NaN unless dense
        # Reader correctness — deterministic headline (normalized EM/containment).
        # Conservative lower bound: abstentions count as wrong; paraphrase/judge
        # tiers (offline) measure how much it under-counts. NaN when no reader ran.
        "qa_accuracy": (mean(lambda r: 1.0 if r.qa_correct else 0.0)
                        if _has_reader(results) else float("nan")),
        "mean_qa_f1": (mean(lambda r: r.qa_f1)
                       if _has_reader(results) else float("nan")),
        # Reader-vs-memory separation + abstention axis (NaN unless a reader ran).
        # accuracy_given_gold_retrieved is the reader's hit_at_k_given_retained:
        # pure reader quality once the answer is in context.
        "accuracy_given_gold_retrieved": (accuracy_given_gold_retrieved(results)
                                          if _has_reader(results) else float("nan")),
        "hallucination_rate": (hallucination_rate(results)
                               if _has_reader(results) else float("nan")),
        "abstention_answerable_rate": (abstention_answerable_rate(results)
                                       if _has_reader(results) else float("nan")),
    }
    # ── Layer-A eviction-activity diagnostics (cheap arithmetic on columns) ──
    # Behaviour, not quality: how hard is the policy working, is it using its
    # budget, and when under pressure does it delete or compact? The quality
    # verdict (regret) is a separate offline pass (scripts/eviction_regret.py).
    tot_inserts = sum(r.inserts_since_last_query for r in results)
    tot_evicts = sum(r.evictions_since_last_query for r in results)
    tot_compress = sum(r.compressions_since_last_query for r in results)
    util = [r.used_bytes_end / r.budget_bytes for r in results if r.budget_bytes > 0]
    pressure = tot_evicts + tot_compress
    out["mean_utilization"] = (sum(util) / len(util)) if util else float("nan")
    out["churn_evictions_per_insert"] = (tot_evicts / tot_inserts) if tot_inserts else 0.0
    # Action mix: of pressure-events resolved, the evict-vs-compact split.
    out["evict_fraction"] = (tot_evicts / pressure) if pressure else float("nan")
    out["compress_fraction"] = (tot_compress / pressure) if pressure else float("nan")
    # Retrieval curves over k. Hit@k + nDCG@k apply to every query (single- and
    # multi-gold); hit_at_k_given_retained is the paired curve that isolates
    # retriever quality from forgetting; Recall@k is reported only on the
    # multi-gold subset (NaN otherwise, since it duplicates Hit@k for single-gold).
    for kk in EVAL_K_CURVE:
        out[f"hit_at_{kk}"] = hit_at(results, kk)
        out[f"hit_at_{kk}_given_retained"] = hit_at_given_retained(results, kk)
        out[f"ndcg_at_{kk}"] = ndcg_at(results, kk)
        out[f"recall_at_{kk}_multigold"] = recall_at(results, kk)
    return out


def summarize_by_question_type(results: list[EvalResult]) -> dict[str, dict[str, float]]:
    """``{question_type: summarize(rows of that type)}``.

    The validity gate for the set-based metrics: this codebase's workloads are
    single-gold (LongMemEval) except for a few BEAM abilities (event_ordering,
    contradiction_resolution, ...). Pooled nDCG/Recall@k average meaningful
    multi-gold numbers with single-gold ones where they collapse to Hit@k — so
    they only mean something split by type. Each value is a full ``summarize``
    dict (so per-type Hit@k/nDCG/Recall curves, support counts, and abstention
    all fall out); ``n_queries``/``n_multigold`` per type tell you the support.
    """
    buckets: dict[str, list[EvalResult]] = defaultdict(list)
    for r in results:
        buckets[r.question_type or "_unknown"].append(r)
    return {qt: summarize(rs) for qt, rs in sorted(buckets.items())}