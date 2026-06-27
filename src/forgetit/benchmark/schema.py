from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from forgetit.core.schema import MemRecord, Query


@dataclass
class WorkloadEvent:
    type: Literal["insert", "retrieve", "query", "reset"]
    timestamp: float
    item: MemRecord | None       # for insert events
    query: Query | None          # for retrieve/query events
    gold_item_ids: list[str]     # which items are needed for this query
    ref_answer: str = ""
    question_type: str = ""

# ── internal intermediates used by the runner ──────────────────────


@dataclass
class QueryEval:
    """Retrieval-layer evaluation for a single scored query."""
    gold_survived: bool
    useful_byte_fraction: float
    non_gold_byte_fraction: float
    evictions_total_at_query_time: int
    compressions: int
    hit_at_k: float
    hit_at_k_given_retained: float
    mrr: float
    used_bytes_end: int
    latency_sec: float
    # Ranking sufficient statistic: 1-based ranks of the gold items within the
    # retrieved list, evaluated to RETRIEVAL_EVAL_DEPTH (≥ k). hit@k / nDCG@k /
    # recall@k / MRR at ANY cutoff are derivable from this offline, so the metric
    # set can change without re-running. n_gold is the relevance-set size (so
    # recall — only meaningful for multi-gold — can be scoped to n_gold > 1).
    n_gold: int = 0
    n_retrieved: int = 0
    gold_ranks: list[int] = field(default_factory=list)
    # Pre-retrieval retrievability: cosine of the query to its NEAREST resident
    # item — "can the kept set answer at all", independent of the ranker. NaN
    # unless items are embedded (dense configs). See metrics.mean_query_coverage.
    query_coverage: float = float("nan")
    retrieved: list[MemRecord] = field(default_factory=list)
    # True if the store exposes enough introspection (iter_ids + get) for the
    # survival/useful-byte metrics to be meaningful.  Opaque stores (Letta,
    # Mem0) set this to False; downstream analysis should filter on it.
    introspectable: bool = False


@dataclass
class QAFields:
    """Reader-layer fields for a single scored query (all optional)."""
    reader_answer: str = ""
    abstained: bool = False
    qa_latency_sec: float = 0.0
    ref_answer: str = ""
    question_text: str = ""
    question_type: str = ""
    # Deterministic headline grade (normalized EM/containment) + token-F1.
    qa_correct: bool = False
    qa_f1: float = 0.0


# ── final per-query output ─────────────────────────────────────────


@dataclass
class EvalResult:
    # configuration
    dataset: str
    query_id: str
    policy: str
    budget_bytes: int
    itemiser: str
    retriever: str
    reader: str
    seed: int
    k: int

    # policy verdict
    gold_survived: bool
    useful_byte_fraction: float
    non_gold_byte_fraction: float          # 1 - useful_byte_fraction (non-gold share of used bytes)
    evictions_total_at_query_time: int     # cumulative over the whole run (odometer, not per-query)

    # retriever verdict
    hit_at_k: float                        # hit@(configured k) — back-compat scalar
    hit_at_k_given_retained: float
    mrr: float                             # reciprocal rank of first gold (over the eval depth)

    # cost
    used_bytes_end: int
    latency_sec: float

    # optional / future fields with defaults
    compressions: int = 0

    # Relative-budget config: budget_frac is the fraction of each example's own
    # footprint used as its byte cap (budget_bytes above is the resolved per-example
    # value). 0.0 means a fixed byte budget was used instead.
    budget_frac: float = 0.0

    # per-query activity (deltas since the previous scored query) — the readable
    # counterparts to the cumulative odometer above.  inserts_since_last_query is
    # "how many items were admitted for this query/example".
    inserts_since_last_query: int = 0
    evictions_since_last_query: int = 0
    compressions_since_last_query: int = 0

    # Ranking sufficient statistic for the retrieval curve (see QueryEval).
    # gold_ranks is a JSON list of the 1-based ranks of gold items in the ranked
    # list (e.g. "[1, 4]"; "[]" if no gold retrieved), evaluated to the eval
    # depth. Hit@k / nDCG@k / Recall@k / MRR at any k are computed from these in
    # metrics.py — so results.csv stays compact and k is not baked in.
    n_gold: int = 0
    n_retrieved: int = 0
    gold_ranks: str = "[]"
    # Pre-retrieval retrievability (max cosine query→nearest resident item); NaN
    # unless the config embeds items (dense). A property of the kept set, not the
    # ranker — the bridge from forgetting back to the content thesis.
    query_coverage: float = float("nan")

    # ── per-query ranking columns (the k-curve, one scalar per (metric, k)) ──
    # A row is one query, so the curve isn't a per-row object — these flat scalars
    # average across rows into Hit@k / nDCG@k / Recall@k / given-retained curves
    # at plot time. Materialised from gold_ranks via metrics.ranking_columns so
    # the columns and the summary aggregates can't diverge. Recall is raw here
    # (== Hit@k for single-gold); its multi-gold scoping is a downstream filter.
    hit_at_1: float = 0.0
    hit_at_3: float = 0.0
    hit_at_5: float = 0.0
    hit_at_10: float = 0.0
    hit_at_1_given_retained: float = float("nan")
    hit_at_3_given_retained: float = float("nan")
    hit_at_5_given_retained: float = float("nan")
    hit_at_10_given_retained: float = float("nan")
    ndcg_at_1: float = 0.0
    ndcg_at_3: float = 0.0
    ndcg_at_5: float = 0.0
    ndcg_at_10: float = 0.0
    recall_at_1: float = 0.0
    recall_at_3: float = 0.0
    recall_at_5: float = 0.0
    recall_at_10: float = 0.0

    # reader verdict
    reader_answer: str = ""
    abstained: bool = False
    qa_latency_sec: float = 0.0
    # Deterministic headline QA grade (normalized EM/containment) + token-F1.
    # The reproducible primary; paraphrase/judge tiers refine it offline.
    qa_correct: bool = False
    qa_f1: float = 0.0

    # reference data
    ref_answer: str = ""
    question_text: str = ""
    question_type: str = ""

    # store identity — lets you filter multi-system result CSVs without
    # reconstructing the label from policy+retriever+budget.  For a retention
    # run this would be e.g. "retention/lru/bm25/budget_262144"; for Letta,
    # "letta/qwen2.5".  Default "" keeps old analyses working.
    store: str = ""

    # True if gold_survived / useful_byte_fraction / non_gold_byte_fraction /
    # evictions_total_at_query_time / compressions reflect real measurements.  False for
    # opaque stores where those fields carry sentinel values only.
    introspectable: bool = False