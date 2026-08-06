from __future__ import annotations

import json
import math
import re
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


# ── coupled identity (decision #10) ─────────────────────────────────
# History policies form their state THROUGH the ingest retriever (probe hits
# feed access_count/last_access, and for GDSF the credit trajectory), so
# "lru under bm25" and "lru under lexical" are different policies. Every
# ranking / mean-rank / matrix aggregation must treat (policy × retriever) as
# the unit; name-level cross-retriever averages are only allowed in views
# explicitly labeled as such. Enforced at the merge layer
# (scripts/concat_results.py adds ``policy_identity``) — not by memory.
HISTORY_POLICIES = frozenset({
    "lru", "lfu", "ebbinghaus", "salience_decay", "gdsf", "gdsf_frozen",
    "compression", "memorybank",   # their selectors are history policies
})


_SEED_SUFFIX = re.compile(r"_s\d+$")


def canonical_policy(policy: str) -> str:
    """``random_s47`` -> ``random``. Seeds are REPLICATES OF ONE POLICY.

    Left as distinct names the ten seeds of the stochastic control occupy ten
    of the roster's twenty-three entries, so every pooled per-policy average,
    mean rank and rank matrix gives the blind baseline ~43% of the weight and
    drags the ranking toward it. No roster policy ends in ``_s<digits>``, so
    the suffix is unambiguous.

    NOT for existence claims. Each seed is a separate deterministic run, so a
    bound violation by ANY seed is a real counterexample; averaging first turns
    ten booleans into a fraction and either manufactures violations against a
    boolean reference or hides them. Collapse for REPORTING, keep separate for
    per-run assertions.
    """
    return _SEED_SUFFIX.sub("", (policy or "").strip())


def coupled_identity(policy: str, retriever: str, *,
                     collapse_seeds: bool = True) -> str:
    """Aggregation identity: ``policy×retriever`` for history policies (whose
    state is formed by the ingest retriever's probe hits), plain ``policy``
    for retriever-independent ones (fifo/random/size/semantic/oracle).

    Seeded replicates collapse to their policy by default (see
    :func:`canonical_policy`). Pass ``collapse_seeds=False`` where per-run
    resolution is the point — the raggedness gate needs it, because a merge in
    which one seed covers forty cells and another ninety-six is unequal support
    that the collapsed view cannot see.
    """
    p = (policy or "").strip()
    if collapse_seeds:
        p = canonical_policy(p)
    if p.lower() in HISTORY_POLICIES:
        return f"{p}×{(retriever or '').strip()}"
    return p


def grid_raggedness(cells_by_identity: dict[str, set]) -> list[str]:
    """Completeness check for the merged grid: one warning line per coupled
    identity that covers only PART of the grid.

    A partially-present identity is worse than an absent one: it silently
    skews every pooled per-policy average through unequal support (e.g. a
    policy added to NEW runs whose rows exist only for the cells that were
    re-run, merged with old CSVs that lack it — mean-rank and name-level
    comparisons then compare policies on different budget/dataset mixes).
    Entirely-absent identities never reach this function (they have no rows),
    which is exactly right: not being in the sweep is a roster choice; being
    half-in is a data defect.

    ``cells_by_identity``: {policy_identity: set of grid-cell tuples it covers}
    where a cell is whatever granularity the caller merges over — the concat
    script uses (dataset, itemiser, budget). Returns [] for a complete grid.
    """
    if not cells_by_identity:
        return []
    all_cells: set = set().union(*cells_by_identity.values())
    lines: list[str] = []
    for ident, cells in sorted(cells_by_identity.items()):
        missing = all_cells - cells
        if missing:
            sample = ", ".join(map(str, sorted(missing)[:3]))
            more = f" (+{len(missing) - 3} more)" if len(missing) > 3 else ""
            lines.append(
                f"RAGGED grid: {ident} covers {len(cells)}/{len(all_cells)} cells "
                f"— missing {sample}{more}; pooled per-policy averages over this "
                f"merge compare unequal supports")
    return lines


def censored_bounds_row(gold_ranks: list[int], n_gold: int, gold_id_retained: bool,
                        introspectable: bool, depth: int = 10) -> dict[str, float]:
    """Interval bounds for LEGACY (depth-censored) rows.

    Rows written before 2026-07-14 observed ranks only to ``depth``; a
    retained gold with no observed rank is somewhere in [depth+1, N] — and N
    (n_residents) was not logged then, so the upper bound is unknown (NaN).
    Emits: legacy_censored (1.0 when the row's best rank is only bounded, not
    observed), gold_rank_best_lo, gold_rank_best_hi. Never apply to
    uncensored rows — their gold_rank_best is exact.
    """
    ranks = [r for r in (gold_ranks or []) if r <= depth]
    if not (introspectable and gold_id_retained):
        return {"legacy_censored": float("nan"),
                "gold_rank_best_lo": float("nan"), "gold_rank_best_hi": float("nan")}
    if ranks:      # best rank observed within depth → exact
        best = float(min(ranks))
        return {"legacy_censored": 0.0,
                "gold_rank_best_lo": best, "gold_rank_best_hi": best}
    # retained but unobserved → bracketed below by depth+1, unbounded above
    return {"legacy_censored": 1.0,
            "gold_rank_best_lo": float(depth + 1), "gold_rank_best_hi": float("nan")}


def rank_percentile(best_rank: int, n_residents: int) -> float:
    """Store-size-normalized rank: pct = 1 - (r-1)/(N-1) ∈ [0, 1].

    1.0 = gold ranked first, 0.0 = ranked last among N residents. The fixed-k
    hit confounds ranking quality with store size (hit@5 of a 10-item store is
    a different bar than of a 400-item store); the percentile is the
    size-invariant headline. N == 1 → 1.0 (the sole resident is rank 1)."""
    if n_residents <= 1:
        return 1.0
    return 1.0 - (best_rank - 1) / (n_residents - 1)


def ranking_columns(gold_ranks: list[int], n_gold: int,
                    introspectable: bool, gold_survived: bool,
                    n_residents: int = 0) -> dict[str, float]:
    """The per-(metric, k) scalar columns for ONE results.csv row.

    A row is a single query, so the "curve" isn't a per-row object — it's these
    flat scalars that become a curve when averaged across rows at plot time.
    Hit@k / nDCG@k / Recall@k are computed for every row (Recall raw — its
    multi-gold scoping is a downstream filter on question_type, not stored here).
    hit_at_k_given_retained is NaN unless the gold was resident, mirroring the
    existing scalar — averaging the non-NaN rows gives the conditioned curve.

    gold_rank_best / rank_pct: best (lowest) gold rank and its percentile among
    the N residents — CONDITIONED on retention (NaN when gold was evicted, when
    the store is opaque, or when N is unknown), so the column isolates ranking
    quality from survival exactly like the given-retained hit curve.
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

    best = min(ranks) if ranks else None
    observed = (introspectable and gold_survived and best is not None and n_residents > 0)
    cols["gold_rank_best"] = float(best) if observed else float("nan")
    cols["rank_pct"] = rank_percentile(best, n_residents) if observed else float("nan")
    return cols


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
    """Hit@k conditioned on the gold item still being resident (gold_id_retained).

    This is the star metric: plain Hit@k conflates two failures — the policy
    evicted the gold, and the retriever missed gold that was still there.
    Conditioning on survival isolates *retriever quality given the item is kept*.
    Reported as a curve PAIRED with hit_at(): the gap (hit_at − this) is the
    forgetting cost; the level of this one is the retriever's quality.
    NaN when nothing survived (can't measure retrieval over an empty kept set).
    """
    retained = [r for r in results if r.introspectable and r.gold_id_retained]
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


def evidence_delivery_rate(results: list[EvalResult]) -> float:
    """Fraction of the RETRIEVED items that actually reached the reader.

    1.0 = the prompt carried everything retrieval returned, so retrieval
    metrics and reader metrics are talking about the same evidence. Below 1.0
    the reader was answering from a subset, and hit@k overstates what it saw —
    the exact confound behind "hit@5 = 0.86 with abstain = 0.70".

    Rows with nothing retrieved are skipped (no denominator); NaN if none of
    the rows have one.
    """
    pairs = [(getattr(r, "reader_evidence_delivered", 0) or 0, r.n_retrieved)
             for r in results if (r.n_retrieved or 0) > 0]
    if not pairs:
        return float("nan")
    return sum(min(d, n) / n for d, n in pairs) / len(pairs)


def gold_survival_rate(results: list[EvalResult]) -> float:
    introspectable = _introspectable(results)
    if not introspectable:
        return 0.0
    return sum(1.0 for r in introspectable if r.gold_id_retained) / len(introspectable)


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
    retained = [r for r in results if r.introspectable and r.gold_id_retained]
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
        # ── Reader-CONTEXT audit (NaN unless a reader ran) ──
        # The gate on reading any abstention number above. evidence_delivery_rate
        # is delivered/retrieved: 1.0 means the prompt carried the whole
        # retrieved list, so abstention is a real refusal on visible evidence.
        # Below 1.0, retrieval metrics and reader metrics describe different
        # inputs — hit@k counts items the model was never shown.
        "mean_evidence_delivered": (mean(lambda r: getattr(r, "reader_evidence_delivered", 0) or 0)
                                    if _has_reader(results) else float("nan")),
        "evidence_delivery_rate": (evidence_delivery_rate(results)
                                   if _has_reader(results) else float("nan")),
        "evidence_truncation_rate": (mean(lambda r: 1.0 if getattr(r, "reader_evidence_truncated", False) else 0.0)
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

    # ── size-normalized ranking view (uncensored-depth runs only) ──
    # rank_pct_mean: mean gold rank-percentile among residents, conditioned on
    # retention — the size-invariant headline reported BESIDE gold_survival.
    # hit_at_5_prop_n: k∝N sensitivity — k scaled so it equals 5 at the run's
    # median store size (rows with unknown N fall back to fixed k=5).
    # deployment_view_hit_at_5: the fixed-k number under its honest name — what
    # a deployed top-5 pipeline sees; confounds ranking with store size.
    pcts = [p for p in (_row_rank_pct(r) for r in results) if p == p]
    out["rank_pct_mean"] = (sum(pcts) / len(pcts)) if pcts else float("nan")
    out["hit_at_5_prop_n"] = _hit_at_prop_n(results, k_ref=5)
    out["deployment_view_hit_at_5"] = out["hit_at_5"]
    return out


def _row_rank_pct(r: EvalResult) -> float:
    """Per-row gold rank-percentile, conditioned on retention (NaN otherwise)."""
    ranks = _gold_ranks(r)
    n_res = getattr(r, "n_residents", 0) or 0
    if not (getattr(r, "introspectable", False) and getattr(r, "gold_id_retained", False)
            and ranks and n_res > 0):
        return float("nan")
    return rank_percentile(min(ranks), n_res)


def _hit_at_prop_n(results: list[EvalResult], k_ref: int = 5) -> float:
    """Hit at k ∝ N: per-row k scaled so it equals ``k_ref`` at the run's
    MEDIAN store size (k_row = max(1, round(k_ref · N_row / median N))). Rows
    with unknown N (opaque) use fixed k_ref. NaN when no rows."""
    if not results:
        return float("nan")
    sizes = sorted((getattr(r, "n_residents", 0) or 0)
                   for r in results if (getattr(r, "n_residents", 0) or 0) > 0)
    if not sizes:
        return hit_at(results, k_ref)
    median_n = sizes[len(sizes) // 2]
    total = 0.0
    for r in results:
        n_res = getattr(r, "n_residents", 0) or 0
        k_row = max(1, round(k_ref * n_res / median_n)) if (n_res > 0 and median_n > 0) else k_ref
        total += _hit(_gold_ranks(r), k_row)
    return total / len(results)


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