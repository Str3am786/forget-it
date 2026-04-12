from __future__ import annotations

import csv
from dataclasses import asdict
from pathlib import Path

from forgetit.benchmark.metrics import summarize
from forgetit.benchmark.runner_core import run
from forgetit.benchmark.workloads.base import Workload
from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.logging import BenchmarkLogger
from forgetit.memory_system import RetentionManager
from forgetit.retrieval import Retriever
from forgetit.policies import EvictionPolicy
from forgetit.llm.readers.base import Reader


def save_results_csv(results, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    rows = [asdict(r) for r in results]
    if not rows:
        return

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def save_summary_csv(summary: dict[str, float], path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(summary.keys()))
        writer.writeheader()
        writer.writerow(summary)


def run_pipeline(
    *,
    dataset: str,
    workload: Workload,
    budget_bytes: int,
    retriever: Retriever,
    policy: EvictionPolicy,
    k: int = 5,
    seed: int = 0,
    itemiser: str = "",
    reader: str = "",
    reader_impl: Reader | None = None,
    ref_answers: dict[str, dict] | None = None,
    answer_max_tokens: int = 128,
    out_dir: str = "artifacts/benchmark",
):
    backend = InMemoryBackend(retriever=retriever)
    logger = BenchmarkLogger()
    store = RetentionManager(
        budget_bytes=budget_bytes,
        backend=backend,
        policy=policy,
        logger=logger,
    )

    store.connect()

    policy_label = getattr(policy, "name", type(policy).__name__)
    retriever_label = getattr(retriever, "name", type(retriever).__name__)

    try:
        results = run(
            workload=workload,
            store=store,
            k=k,
            dataset=dataset,
            policy=policy_label,
            retriever_name=retriever_label,
            seed=seed,
            itemiser=itemiser,
            reader_name=reader,
            reader_impl=reader_impl,
            ref_answers=ref_answers,
            answer_max_tokens=answer_max_tokens,
        )
    finally:
        store.close()

    summary = summarize(results)

    base = (
        Path(out_dir)
        / dataset
        / itemiser
        / policy_label
        / retriever_label
        / (reader or "none")
        / f"budget_{budget_bytes}"
    )
    save_results_csv(results, base / "results.csv")
    save_summary_csv(summary, base / "summary.csv")

    return results, summary