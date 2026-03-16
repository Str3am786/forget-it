from dataclasses import dataclass
from typing import Literal

from forgetit.core.schema import MemRecord, Query


@dataclass
class WorkloadEvent:
    type: Literal["insert", "retrieve", "query"]
    timestamp: float
    item: MemRecord | None       # for insert events
    query: Query | None          # for retrieve/query events
    gold_item_ids: list[str]     # which items are needed for this query


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
    evictions: int

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
    oracle_correct: bool = False
    retrieved_correct: bool = False


@dataclass
class GoldSpec:
    id: int

# Consider
# gold survival
# relevant-byte retention
# hit@k
# hit@k | retained
# reader EM/F1 | oracle
# end-to-end EM/F1
# mean compression ratio
# average latency