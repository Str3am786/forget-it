from __future__ import annotations

import csv
import json
import logging
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable

from forgetit.benchmark.metrics import summarize, summarize_by_question_type
from forgetit.benchmark.runner_core import run, run_one_example, _run_reader, _StoreView
from forgetit.benchmark.workloads.base import Workload
from forgetit.memory_system.base import MemorySystem
from forgetit.llm.readers.base import Reader
from forgetit.core.timing import (
    PhaseTimer, use_timer, merge_dicts, COARSE_PHASES, SUB_PHASES,
)

log = logging.getLogger(__name__)


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


def save_summary_by_qtype_csv(results, path: str | Path) -> None:
    """One summary row per question_type (long format) so the set-based metrics
    (nDCG/Recall@k) can be read where they're meaningful — the multi-gold types —
    instead of pooled. No-op when there are no results."""
    by_qt = summarize_by_question_type(results)
    if not by_qt:
        return
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [{"question_type": qt, **summ} for qt, summ in by_qt.items()]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _answer_contexts_inline(ctx_rows, *, reader_impl, answer_max_tokens,
                            abstention_accounting=False, answer_max_tokens_list=0,
                            reader_workers: int = 1) -> None:
    """Pass 2 (shared): answer recorded contexts and write the QA fields back
    onto their EvalResults in place. Pure function of (question, context) —
    race-free, store-free. Used by run_pipeline_concurrent and by tests."""
    def _answer(task):
        result, query, retrieved, ref, qtype = task
        qa = _run_reader(reader_impl=reader_impl, query=query, retrieved=retrieved,
                         answer_max_tokens=answer_max_tokens, ref_answer=ref,
                         question_type=qtype,
                         abstention_accounting=abstention_accounting,
                         answer_max_tokens_list=answer_max_tokens_list)
        return result, qa

    if reader_workers > 1 and len(ctx_rows) > 1:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=reader_workers) as pool:
            answered = list(pool.map(_answer, ctx_rows))
    else:
        answered = [_answer(t) for t in ctx_rows]

    for result, qa in answered:
        result.reader_answer = qa.reader_answer
        result.abstained = qa.abstained
        result.qa_latency_sec = qa.qa_latency_sec
        result.qa_correct = qa.qa_correct
        result.qa_f1 = qa.qa_f1
        result.reader_answer_raw = qa.reader_answer_raw
        result.qa_correct_raw = qa.qa_correct_raw
        result.qa_f1_raw = qa.qa_f1_raw


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
    probe_touch_mode: str = "original",
    reader_token_budget: int = 0,
    abstention_accounting: bool = False,
    answer_max_tokens_list: int = 0,
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

    timer = PhaseTimer()
    wall_t0 = time.perf_counter()
    try:
        with use_timer(timer):
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
                probe_touch_mode=probe_touch_mode,
                reader_token_budget=reader_token_budget,
                abstention_accounting=abstention_accounting,
                answer_max_tokens_list=answer_max_tokens_list,
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

    timing = timer.as_dict()
    reader_work = sum(getattr(r, "qa_latency_sec", 0.0) for r in results)
    if reader_work > 0:
        timing["reader"] = {"total_s": reader_work,
                            "n": sum(1 for r in results if getattr(r, "qa_latency_sec", 0.0) > 0)}
    wall_s = time.perf_counter() - wall_t0

    summary = summarize(results)
    summary.update(_flatten_timing(timing, wall_s))

    save_results_csv(results, base / "results.csv")
    save_summary_csv(summary, base / "summary.csv")
    save_summary_by_qtype_csv(results, base / "summary_by_qtype.csv")
    save_timings_json(timing, wall_s, base / "timings.json")

    return results, summary


def _write_events_jsonl(path: Path, event_groups: list[list[dict]]) -> None:
    """Write per-example event rows to one JSONL, in example order.

    A synthetic ``clear`` event separates examples so the offline survival
    analysis (which censors at clears) segments the concurrent log exactly like
    the serial one — where ``store.clear()`` emits a real ``clear`` between
    examples. ``seq`` is re-assigned monotonically in example order; the
    per-example wall-clock ``t`` interleaves under concurrency, so downstream
    code must order by ``seq``, not ``t``.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    seq = 0
    with path.open("w", encoding="utf-8") as f:
        for gi, rows in enumerate(event_groups):
            if gi > 0:
                f.write(json.dumps({"seq": seq, "t": time.time(), "name": "clear"}) + "\n")
                seq += 1
            for row in rows:
                f.write(json.dumps({"seq": seq, **row}) + "\n")
                seq += 1


def _write_gold_snapshot_jsonl(path: Path, rows: list[dict]) -> None:
    """Write the per-(query, gold-id) answer-survival snapshot to one JSONL.

    Each row already carries ``example_id``, so unlike events.jsonl no synthetic
    ``clear`` separators are needed — the offline pass joins by (example_id,
    query_id, gold_id). Gold ids only (a handful per example), so this stays tiny
    next to events.jsonl even though it carries text. See
    runner_core._collect_gold_snapshot for field semantics (O_g / R_g)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def _flatten_timing(timing: dict, wall_s: float | None = None) -> dict[str, float]:
    """Flat ``time_<phase>_s`` + ``n_<phase>`` columns for summary.csv. Coarse
    phases sum to ``time_total_work_s`` (work-seconds, summed over concurrent
    units); ``wall_s`` is end-to-end clock, so wall/work shows the speedup."""
    out: dict[str, float] = {}
    for p in COARSE_PHASES + SUB_PHASES:
        v = timing.get(p, {})
        out[f"time_{p}_s"] = round(float(v.get("total_s", 0.0)), 6)
        out[f"n_{p}"] = int(v.get("n", 0))
    out["time_total_work_s"] = round(
        sum(float(timing.get(p, {}).get("total_s", 0.0)) for p in COARSE_PHASES), 6)
    if wall_s is not None:
        out["wall_s"] = round(float(wall_s), 6)
    return out


def save_timings_json(timing: dict, wall_s: float | None, path: str | Path) -> None:
    """Full phase trace for the visual breakdown (scripts/plot_timings.py)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "coarse_phases": list(COARSE_PHASES),
        "sub_phases": list(SUB_PHASES),
        "wall_s": wall_s,
        "phases": timing,
    }, indent=2))


def run_pipeline_concurrent(
    *,
    dataset: str,
    workload: Workload,
    store_factory: Callable[[], MemorySystem],
    store_label: str,
    k: int = 5,
    seed: int = 0,
    itemiser: str = "",
    reader: str = "",
    reader_impl: Reader | None = None,
    answer_max_tokens: int = 128,
    example_workers: int = 1,
    reader_workers: int = 1,
    budget_bytes: int = 0,
    budget_frac: float = 0.0,
    example_footprints: list[int] | None = None,
    policy_label: str = "",
    retriever_label: str = "",
    probe_touch_mode: str = "original",
    reader_token_budget: int = 0,
    abstention_accounting: bool = False,
    answer_max_tokens_list: int = 0,
    out_dir: str = "artifacts/benchmark",
    write_events: bool = False,
) -> tuple[list, dict[str, float]]:
    """Across-example concurrent driver — same outputs as :func:`run_pipeline`.

    The unit of concurrency is the workload *example* (LongMemEval example /
    BEAM conversation). For one config, every example is replayed against its
    OWN fresh store (``store_factory()``) on a thread pool, so the compression
    LLM-call waits of different examples overlap and one config can saturate the
    vLLM replicas — without any across-example race (isolation is by
    instantiation, the dataset parse is shared read-only, and the OpenAI client
    pool is thread-safe). Configs are processed sequentially by the caller; each
    saturates the GPUs on its own via this example-level parallelism.

    Two-pass model: Pass 1 (this driver, optionally concurrent) does all the
    store-touching work and records each scored query's retrieved context; Pass
    2 fires the reader over those contexts concurrently (the reader is a pure
    function of question+context, so it's race-free and store-free).

    ``store_factory`` MUST return a fresh, independent store on every call.
    Results, the deferred reader, the merged event log, summarize, and the CSV
    layout are all identical to :func:`run_pipeline`.
    """
    base = (
        Path(out_dir)
        / dataset
        / (itemiser or "default")
        / store_label
        / (reader or "none")
    )

    footprints = example_footprints or []
    collect_ctx = reader_impl is not None

    wall_t0 = time.perf_counter()

    def _task(idx: int, example_id: str, evs: list) -> tuple[int, list, list, list, list, dict]:
        store = store_factory()                 # fresh, isolated store per example
        view = _StoreView(store)
        timer = PhaseTimer()                    # per-example, thread-local-scoped
        try:
            fp = (
                footprints[idx] if idx < len(footprints)
                else (footprints[-1] if footprints else None)
            )
            with use_timer(timer):
                results, ctx, event_rows, gold_rows = run_one_example(
                    evs, store,
                    k=k, dataset=dataset, store_label=store_label,
                    policy=policy_label, retriever_name=retriever_label,
                    budget_bytes_report=budget_bytes, budget_frac=budget_frac,
                    footprint=fp, seed=seed, itemiser=itemiser, reader_name=reader,
                    collect_reader_context=collect_ctx, collect_events=write_events,
                    collect_gold_snapshot=write_events, example_id=example_id,
                    answer_max_tokens=answer_max_tokens,
                    probe_touch_mode=probe_touch_mode,
                    reader_token_budget=reader_token_budget,
                    abstention_accounting=abstention_accounting,
                )
        finally:
            # Dispose the per-example store (frees memory; isolation already
            # guaranteed). Event AND gold rows were snapshotted inside
            # run_one_example BEFORE this clear — the gold snapshot reads the
            # FINAL store state, so it must precede disposal (it does).
            clear_fn = getattr(store, "clear", None)
            if callable(clear_fn):
                clear_fn()
            view.safe_call("close")
        return idx, results, ctx, event_rows, gold_rows, timer.as_dict()

    # ── Pass 1: process examples (concurrent when example_workers > 1) ──
    # Keep results in example order so CSVs are deterministic regardless of the
    # completion order of the futures.
    indexed = [(idx, eid, evs)
               for idx, (eid, evs) in enumerate(workload.iter_examples())]
    collected: list[tuple | None] = [None] * len(indexed)

    if example_workers > 1 and len(indexed) > 1:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        log.info("  [%s] Pass 1: %d examples across %d workers",
                 store_label, len(indexed), example_workers)
        with ThreadPoolExecutor(max_workers=example_workers) as pool:
            futs = [pool.submit(_task, idx, eid, evs) for idx, eid, evs in indexed]
            for fut in as_completed(futs):
                idx, results, ctx, event_rows, gold_rows, timing = fut.result()
                collected[idx] = (results, ctx, event_rows, gold_rows, timing)
    else:
        for idx, eid, evs in indexed:
            _, results, ctx, event_rows, gold_rows, timing = _task(idx, eid, evs)
            collected[idx] = (results, ctx, event_rows, gold_rows, timing)

    all_results: list = []
    all_ctx: list = []
    event_groups: list[list[dict]] = []
    gold_rows_all: list[dict] = []
    timings: list[dict] = []
    for item in collected:
        if item is None:
            continue
        results, ctx, event_rows, gold_rows, timing = item
        all_results.extend(results)
        all_ctx.extend(ctx)
        timings.append(timing)
        if write_events:
            event_groups.append(event_rows)
            gold_rows_all.extend(gold_rows)

    # ── Pass 2: concurrent reader over recorded contexts (store-free) ──
    if all_ctx:
        log.info("  [%s] Pass 2: %d reader calls across %d workers",
                 store_label, len(all_ctx), reader_workers)
        _answer_contexts_inline(all_ctx, reader_impl=reader_impl,
                                answer_max_tokens=answer_max_tokens,
                                abstention_accounting=abstention_accounting,
                                answer_max_tokens_list=answer_max_tokens_list,
                                reader_workers=reader_workers)

    if write_events:
        _write_events_jsonl(base / "events.jsonl", event_groups)
        _write_gold_snapshot_jsonl(base / "gold_answer_snapshot.jsonl", gold_rows_all)

    # Merge per-example phase timers; the reader ran in Pass 2 (separate threads),
    # so add it from the per-query qa_latency we already measured.
    timing = merge_dicts(timings)
    reader_work = sum(getattr(r, "qa_latency_sec", 0.0) for r in all_results)
    if reader_work > 0:
        timing["reader"] = {"total_s": reader_work,
                            "n": sum(1 for r in all_results if getattr(r, "qa_latency_sec", 0.0) > 0)}
    wall_s = time.perf_counter() - wall_t0

    summary = summarize(all_results)
    summary.update(_flatten_timing(timing, wall_s))
    save_results_csv(all_results, base / "results.csv")
    save_summary_csv(summary, base / "summary.csv")
    save_summary_by_qtype_csv(all_results, base / "summary_by_qtype.csv")
    save_timings_json(timing, wall_s, base / "timings.json")

    return all_results, summary