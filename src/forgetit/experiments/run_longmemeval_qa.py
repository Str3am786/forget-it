
from __future__ import annotations

from pathlib import Path
import logging
from typing import Callable, Iterator

import pandas as pd

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
from forgetit.policies import (
    LRUPolicy, LFUPolicy, RandomPolicy,
    LLMCompressionPolicy, EbbinghausPolicy, MemoryBankCompressionPolicy,
)
from forgetit.llm import get_llm
from forgetit.llm.readers import PlainTextReader
from forgetit.settings import LETTA_BASE_URL, LETTA_API_KEY


REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = REPO_ROOT / "data"


# ── Store builders ────────────────────────────────────────────────

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
    short_model = model.split("/")[-1].split(":")[0]
    label = f"letta/{short_model}"
    return label, store, {}


# ── Policy / retriever spec helpers ──────────────────────────────

PolicySpec = tuple[str, Callable]      # (name, zero-arg factory)
RetrieverSpec = tuple[str, Callable]   # (name, zero-arg factory)


def default_policy_specs(llm, seed: int = 42) -> list[PolicySpec]:
    """Return zero-arg factories for every policy.  Add or remove entries here."""
    return [
        ("lru",         lambda: LRUPolicy()),
        ("lfu",         lambda: LFUPolicy()),
        ("random",      lambda: RandomPolicy(seed=seed)),
        ("ebbinghaus",  lambda: EbbinghausPolicy()),
        ("compression", lambda: LLMCompressionPolicy(llm=llm, selector=LFUPolicy())),
        ("memorybank",  lambda: MemoryBankCompressionPolicy(llm=llm, selector=EbbinghausPolicy())),
    ]


def default_retriever_specs() -> list[RetrieverSpec]:
    return [
        ("lexical", LexicalOverlapRetriever),
        ("bm25",    BM25Retriever),
    ]


# ── Store iterator ────────────────────────────────────────────────

def make_stores(
    policy_specs: list[PolicySpec],
    retriever_specs: list[RetrieverSpec],
    budgets: list[int],
    include_letta: bool = False,
) -> Iterator[tuple[str, Callable[[], tuple]]]:
    """
    Yield ``(label, factory)`` pairs — one per (policy × retriever × budget)
    combination.  Each ``factory()`` call returns a fresh ``(label, store, extras)``
    triple so the store is never reused across runs.
    """
    for pname, policy_fn in policy_specs:
        for rname, retriever_fn in retriever_specs:
            for budget in budgets:
                def _factory(b=budget, pf=policy_fn, rf=retriever_fn,
                             pn=pname, rn=rname):
                    return build_retention_store(
                        budget=b, policy=pf(), retriever=rf(),
                        policy_name=pn, retriever_name=rn,
                    )
                yield f"retention/{pname}/{rname}/budget_{budget}", _factory

    if include_letta:
        yield "letta/default", build_letta_store


# ── Main ─────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    data_path = DATA_DIR / "longmemeval_s_cleaned.json"
    if not data_path.exists():
        raise FileNotFoundError(f"Dataset not found: {data_path}")

    seed = 42
    llm = get_llm()
    reader_impl = PlainTextReader(llm)
    reader_name = getattr(reader_impl, "name", type(reader_impl).__name__)

    # ── Configure experiment here ──────────────────────────────────
    # Toggle policies by commenting/uncommenting entries in these lists.
    policy_specs = default_policy_specs(llm, seed=seed)
    retriever_specs = default_retriever_specs()
    budgets = [262_144, 524_288, 1_048_576, 2_097_152, 5_242_880]
    INCLUDE_LETTA = False
    # ──────────────────────────────────────────────────────────────

    itemisers = [SessionItemiser(), TurnPairItemiser()]

    out_csv = REPO_ROOT / "artifacts" / "benchmark" / "longmemeval_qa_summary.csv"
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    accumulated: list[dict] = []

    for itemiser in itemisers:
        workload = LongMemEvalWorkload(data_path, itemiser)
        for _label, factory in make_stores(
            policy_specs, retriever_specs, budgets, include_letta=INCLUDE_LETTA
        ):
            label, store, extras = factory()
            _, summary = run_pipeline(
                dataset="longmemeval",
                workload=workload,
                store=store,
                store_label=label,
                k=5,
                seed=seed,
                itemiser=itemiser.name,
                reader=reader_name,
                reader_impl=reader_impl,
                answer_max_tokens=128,
                out_dir=REPO_ROOT / "artifacts" / "benchmark",
                **extras,
            )

            row = {
                "itemiser":      itemiser.name,
                "store":         label,
                "reader":        reader_name,
                "policy":        extras.get("policy_label", ""),
                "retriever":     extras.get("retriever_label", ""),
                "budget_bytes":  extras.get("budget_bytes", ""),
                "gold_survival": summary["gold_survival_rate"],
                "hit_at_5":      summary["hit_at_k"],
                "mrr":           summary["mean_mrr"],
            }
            accumulated.append(row)
            pd.DataFrame(accumulated).to_csv(out_csv, index=False)

            print(
                f"itemiser={itemiser.name:<12} "
                f"store={label:<45} "
                f"reader={reader_name:<12} "
                f"gold_survival={summary['gold_survival_rate']:.3f} "
                f"hit@k={summary['hit_at_k']:.3f}"
            )


if __name__ == "__main__":
    main()
