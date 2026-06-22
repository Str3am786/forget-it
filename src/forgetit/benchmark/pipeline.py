from __future__ import annotations

import csv
from dataclasses import asdict
from pathlib import Path
from typing import Any

from forgetit.benchmark.metrics import summarize
from forgetit.benchmark.runner_core import run, _StoreView
from forgetit.benchmark.workloads.base import Workload
from forgetit.memory_system.base import MemorySystem
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
    store: MemorySystem,
    store_label: str,
    k: int = 5,
    seed: int = 0,
    itemiser: str = "",
    reader: str = "",
    reader_impl: Reader | None = None,
    answer_max_tokens: int = 128,
    reader_workers: int = 1,
    budget_bytes: int = 0,
    budget_frac: float = 0.0,
    example_footprints: list[int] | None = None,
    policy_label: str = "",
    retriever_label: str = "",
    out_dir: str = "artifacts/benchmark",
    clear_after: bool = True,
    write_events: bool = False,
) -> tuple[list, dict[str, float]]:
    """
    Run a workload against a pre-built memory system and persist results.

    The caller is responsible for constructing ``store``.  For a retention
    experiment this means building a ``RetentionManager`` with its backend,
    policy, and logger; for Letta it's a ``LettaWrapper()``.  The pipeline
    does lifecycle (connect/close if the store exposes them) and always
    calls ``store.clear()`` at the end.

    ``store_label`` identifies the configuration in output CSVs and in the
    artifact directory layout.  Use a slash-separated string to get nested
    directories, e.g. ``"retention/lru/bm25/budget_262144"`` →
    ``<out_dir>/<dataset>/<itemiser>/retention/lru/bm25/budget_262144/``.

    ``budget_bytes`` / ``policy_label`` / ``retriever_label`` are pass-through
    labels for reporting only; the actual budget/policy/retriever live inside
    the store and the pipeline doesn't touch them.
    """
    view = _StoreView(store)

    # Artifact layout:
    #   <out_dir>/<dataset>/<itemiser>/<store_label>/<reader>/
    # store_label can contain slashes; they become sub-directories.
    base = (
        Path(out_dir)
        / dataset
        / (itemiser or "default")
        / store_label
        / (reader or "none")
    )

    # Opt-in per-eviction event trace (insert/evict/compress/clear/gold), streamed
    # next to results.csv. Powers the survival / regret / value-decay analysis.
    if write_events:
        logger = getattr(store, "logger", None)
        if logger is not None and hasattr(logger, "set_path"):
            logger.set_path(base / "events.jsonl")

    view.safe_call("connect")

    try:
        results = run(
            workload=workload,
            store=store,
            k=k,
            dataset=dataset,
            store_label=store_label,
            policy=policy_label,
            retriever_name=retriever_label,
            budget_bytes_report=budget_bytes,
            budget_frac=budget_frac,
            example_footprints=example_footprints,
            seed=seed,
            itemiser=itemiser,
            reader_name=reader,
            reader_impl=reader_impl,
            answer_max_tokens=answer_max_tokens,
            reader_workers=reader_workers,
        )
    finally:
        # TODO need to think of a clear_between_examples=True flag
        if clear_after:
            store.clear()

        view.safe_call("close")

        # Flush/close the event-trace file if one was attached.
        logger = getattr(store, "logger", None)
        if logger is not None and hasattr(logger, "close"):
            logger.close()

    summary = summarize(results)

    save_results_csv(results, base / "results.csv")
    save_summary_csv(summary, base / "summary.csv")

    return results, summary