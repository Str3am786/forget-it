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
    waste_rate: float
    evictions_total_at_query_time: int
    compressions: int
    hit_at_k: float
    hit_at_k_given_retained: float
    mrr: float
    used_bytes_end: int
    latency_sec: float
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
    waste_rate: float
    evictions_total_at_query_time: int

    # retriever verdict
    hit_at_k: float
    hit_at_k_given_retained: float
    mrr: float

    # cost
    used_bytes_end: int
    latency_sec: float

    # optional / future fields with defaults
    compressions: int = 0

    # reader verdict
    reader_answer: str = ""
    abstained: bool = False
    qa_latency_sec: float = 0.0

    # reference data
    ref_answer: str = ""
    question_text: str = ""
    question_type: str = ""

    # store identity — lets you filter multi-system result CSVs without
    # reconstructing the label from policy+retriever+budget.  For a retention
    # run this would be e.g. "retention/lru/bm25/budget_262144"; for Letta,
    # "letta/qwen2.5".  Default "" keeps old analyses working.
    store: str = ""

    # True if gold_survived / useful_byte_fraction / waste_rate /
    # evictions_total_at_query_time / compressions reflect real measurements.  False for
    # opaque stores where those fields carry sentinel values only.
    introspectable: bool = False