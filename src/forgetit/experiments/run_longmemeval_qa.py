
from __future__ import annotations

from pathlib import Path
import json
import logging
from typing import Callable, Iterator

from forgetit.benchmark.pipeline import run_pipeline
from forgetit.benchmark.workloads.longmemeval import (
    LongMemEvalWorkload,
    SessionItemiser,
    TurnPairItemiser,
)
from forgetit.memory_system import RetentionManager
from forgetit.memory_system.wrappers.memgpt_sys import LettaWrapper
from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.logging import BenchmarkLogger
from forgetit.retrieval import LexicalOverlapRetriever, BM25Retriever
from forgetit.policies import LRUPolicy, LFUPolicy
from forgetit.llm import get_llm
from forgetit.llm.readers import PlainTextReader
from forgetit.settings import LETTA_BASE_URL, LETTA_API_KEY


REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = REPO_ROOT / "data"


# def load_ref_answers(path: Path) -> dict[str, dict]:
#     data = json.loads(path.read_text(encoding="utf-8"))
#     refs = {}
#     for ex in data:
#         qid = str(ex.get("question_id", ""))
#         refs[qid] = {
#             "answer": ex.get("answer", ""),
#             "question_type": ex.get("question_type", ""),
#         }
#     return refs


# ── Store builders ────────────────────────────────────────────────
# Each builder returns a ``(label, store, extras)`` triple:
#   label   — store identity used in artifact paths + EvalResult.store
#   store   — fresh MemorySystem instance (never reuse — clear() runs at end)
#   extras  — kwargs forwarded to run_pipeline for reporting only
#             (budget_bytes / policy_label / retriever_label columns in the CSV)


def build_retention_store(
    *,
    budget: int,
    policy,
    retriever,
    policy_name: str,
    retriever_name: str,
) -> tuple[str, RetentionManager, dict]:
    backend = InMemoryBackend(retriever=retriever)
    logger = BenchmarkLogger()
    store = RetentionManager(
        budget_bytes=budget,
        backend=backend,
        policy=policy,
        logger=logger,
    )
    label = f"retention/{policy_name}/{retriever_name}/budget_{budget}"
    return label, store, {
        "budget_bytes": budget,
        "policy_label": policy_name,
        "retriever_label": retriever_name,
    }


def build_letta_store(
    *,
    model: str = "ollama/qwen2.5:7b-instruct-q5_K_M",
    embedding: str = "ollama/nomic-embed-text:latest",
    base_url: str = LETTA_BASE_URL,
) -> tuple[str, LettaWrapper, dict]:
    store = LettaWrapper(base_url=base_url, model=model, embedding=embedding, api_key=LETTA_API_KEY)
    # Strip the ollama/ prefix for a cleaner label
    short_model = model.split("/")[-1].split(":")[0]
    label = f"letta/{short_model}"
    return label, store, {}       # no budget / policy / retriever labels


# ── Plans ─────────────────────────────────────────────────────────

def make_stores(
    budgets: list[int],
    include_letta: bool = False,
) -> Iterator[tuple[str, Callable[[], object], dict]]:
    """
    Yield factories (not instances) so we get a *fresh* store per pipeline run.

    Reusing a store across runs is unsafe: ``run_pipeline`` calls ``clear()``
    at the end, and most stores do not support re-use after clear in a
    benchmark-meaningful way.
    """
    policies = [("lru", LRUPolicy), ("lfu", LFUPolicy)]
    retrievers = [("lexical", LexicalOverlapRetriever), ("bm25", BM25Retriever)]

    for pname, PClass in policies:
        for rname, RClass in retrievers:
            for budget in budgets:
                # Close over the current values
                def _retention_factory(b=budget, P=PClass, R=RClass,
                                       pn=pname, rn=rname):
                    return build_retention_store(
                        budget=b, policy=P(), retriever=R(),
                        policy_name=pn, retriever_name=rn,
                    )
                label = f"retention/{pname}/{rname}/budget_{budget}"
                yield label, _retention_factory, {}

    if include_letta:
        yield "letta/default", build_letta_store, {}


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    data_path = DATA_DIR / "longmemeval_s_cleaned.json"
    if not data_path.exists():
        raise FileNotFoundError(f"Dataset not found: {data_path}")

    llm = get_llm()
    reader_impl = PlainTextReader(llm)
    reader_name = getattr(reader_impl, "name", type(reader_impl).__name__)

    itemisers = [SessionItemiser(), TurnPairItemiser()]
    budgets = [262_144, 524_288, 1_048_576, 2_097_152, 5_242_880]

    # Flip this on when your Letta server is running.  The rest of the
    # experiment — policies, budgets, retrievers — stays identical.
    INCLUDE_LETTA = False

    for itemiser in itemisers:
        workload = LongMemEvalWorkload(data_path, itemiser)
        for _label, factory, _ in make_stores(budgets, include_letta=INCLUDE_LETTA):
            label, store, extras = factory()
            _, summary = run_pipeline(
                dataset="longmemeval",
                workload=workload,
                store=store,
                store_label=label,
                k=5,
                seed=42,
                itemiser=itemiser.name,
                reader=reader_name,
                reader_impl=reader_impl,
                answer_max_tokens=128,
                out_dir=REPO_ROOT / "artifacts" / "benchmark",
                **extras,
            )
            print(
                f"itemiser={itemiser.name:<12} "
                f"store={label:<45} "
                f"reader={reader_name:<12} "
                f"gold_survival={summary['gold_survival_rate']:.3f} "
                f"hit@k={summary['hit_at_k']:.3f}"
            )


if __name__ == "__main__":
    main()