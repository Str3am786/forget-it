from __future__ import annotations

import csv
from dataclasses import asdict
from pathlib import Path
from typing import Callable

from forgetit.benchmark.metrics import summarize
from forgetit.benchmark.runner import run
from forgetit.benchmark.workloads.synthetic import SyntheticWorkload
from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.logging import BenchmarkLogger
from forgetit.memory_system.store import RetentionManager
from forgetit.retrieval.lexical import LexicalOverlapRetriever


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


def make_workload(dataset: str):
    if dataset == "synthetic":
        return SyntheticWorkload()

    raise ValueError(f"Unknown dataset: {dataset}")


def run_pipeline(
    *,
    dataset: str,
    budget_bytes: int,
    policy,
    policy_name: str,
    k: int = 5,
    seed: int = 0,
    itemiser: str = "",
    reader: str = "",
    out_dir: str = "artifacts/benchmark",
):
    workload = make_workload(dataset)

    retriever = LexicalOverlapRetriever()
    backend = InMemoryBackend(retriever=retriever)
    logger = BenchmarkLogger()
    store = RetentionManager(
        budget_bytes=budget_bytes,
        backend=backend,
        policy=policy,
        logger=logger,
    )

    store.connect()
    try:
        results = run(
            workload=workload,
            store=store,
            k=k,
            dataset=dataset,
            policy=policy_name,
            retriever=type(retriever).__name__,
            seed=seed,
            itemiser=itemiser,
            reader=reader,
        )
    finally:
        store.close()

    summary = summarize(results)

    base = Path(out_dir) / dataset / policy_name / f"budget_{budget_bytes}"
    save_results_csv(results, base / "results.csv")
    save_summary_csv(summary, base / "summary.csv")

    return results, summary