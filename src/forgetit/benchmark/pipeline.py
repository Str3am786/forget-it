from __future__ import annotations

import csv
import hashlib
import inspect
import json
import logging
import subprocess
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


# ── provenance (every row self-describing across configs AND runs) ──

_git_commit_cache: str | None = None


def _git_commit() -> str:
    """Short sha of HEAD, '-dirty' suffixed when the tree has changes.
    Cached per process; 'unknown' outside a git checkout."""
    global _git_commit_cache
    if _git_commit_cache is None:
        try:
            cwd = Path(__file__).resolve().parent
            sha = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                                 capture_output=True, text=True, cwd=cwd,
                                 timeout=10).stdout.strip()
            dirty = subprocess.run(["git", "status", "--porcelain"],
                                   capture_output=True, text=True, cwd=cwd,
                                   timeout=10).stdout.strip()
            _git_commit_cache = (sha + ("-dirty" if dirty else "")) if sha else "unknown"
        except Exception:
            _git_commit_cache = "unknown"
    return _git_commit_cache


def _config_hash(**fields) -> str:
    """Stable hash of the config identity. Includes everything that changes
    the science (dataset/itemiser/store_label/reader/k/seed/budgets/example
    count), excludes throughput knobs (workers)."""
    blob = json.dumps(fields, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def _stamp_provenance(results, *, config_hash: str, reader_impl, store_probe) -> None:
    """Fill the provenance columns on every row, in place.

    ``store_probe`` is one representative store for this config — the source
    of the compression prompt template and the encoder tag actually wired.
    """
    llm_model = ""
    prompt_reader_sha = ""
    if reader_impl is not None:
        llm_model = str(getattr(getattr(reader_impl, "llm", None), "model", "") or "")
        try:
            from forgetit.llm.readers import text_reader
            prompt_reader_sha = hashlib.sha256(
                inspect.getsource(text_reader._build_prompt).encode("utf-8")
            ).hexdigest()[:12]
        except Exception:
            pass

    embed_model = str(getattr(getattr(store_probe, "embedder", None), "model", "") or "")

    prompt_compress_sha = ""
    policy = getattr(store_probe, "policy", None)
    tpl = getattr(policy, "prompt_template", None)
    if isinstance(tpl, str):
        prompt_compress_sha = hashlib.sha256(tpl.encode("utf-8")).hexdigest()[:12]
        # Compression uses the chat LLM even reader-off — record its tag too.
        if not llm_model:
            llm_model = str(getattr(getattr(policy, "llm", None), "model", "") or "")

    commit = _git_commit()
    for r in results:
        r.git_commit = commit
        r.config_hash = config_hash
        r.llm_model = llm_model
        r.embed_model = embed_model
        r.prompt_compress_sha = prompt_compress_sha
        r.prompt_reader_sha = prompt_reader_sha


# ── per-config skip-if-done (resumable shards) ──────────────────────
# A completed config leaves a DONE.json next to its CSVs, keyed by the SAME
# config hash the provenance columns carry. Re-running a finished shard finds
# the marker, sees the identical identity, and returns the stored summary as
# a no-op. A marker written by a DIFFERENT config in the same directory (e.g.
# a --limit smoke before the full run — different n_examples, different hash)
# never satisfies the check. FORGETIT_FORCE_RERUN=1 overrides.


def _done_marker_path(base: Path) -> Path:
    return base / "DONE.json"


def _check_done(base: Path, config_hash: str) -> dict | None:
    """Return the stored summary when this exact config already completed."""
    import os
    if os.environ.get("FORGETIT_FORCE_RERUN", "").lower() in ("1", "true", "on"):
        return None
    p = _done_marker_path(base)
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    if data.get("config_hash") != config_hash:
        return None            # same directory, different config → rerun
    return data.get("summary")


def _write_done(base: Path, config_hash: str, summary: dict) -> None:
    p = _done_marker_path(base)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"config_hash": config_hash, "summary": summary}),
                 encoding="utf-8")


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

    # Provenance (serial path): the streaming workload has no example count,
    # so the identity carries n_examples=None — the concurrent driver is the
    # canonical launch path and includes the real count.
    cfg_hash = _config_hash(
        dataset=dataset, itemiser=itemiser, store_label=store_label,
        reader=reader, k=k, seed=seed, budget_bytes=budget_bytes,
        budget_frac=budget_frac, answer_max_tokens=answer_max_tokens,
        n_examples=None, write_events=write_events,
    )
    _stamp_provenance(results, config_hash=cfg_hash,
                      reader_impl=reader_impl, store_probe=store)

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

    # Config identity for provenance. The example count belongs in it: a
    # --limit smoke and the full run are different experiments.
    cfg_hash = _config_hash(
        dataset=dataset, itemiser=itemiser, store_label=store_label,
        reader=reader, k=k, seed=seed, budget_bytes=budget_bytes,
        budget_frac=budget_frac, answer_max_tokens=answer_max_tokens,
        n_examples=len(indexed), write_events=write_events,
    )

    # Skip-if-done: a crashed/re-submitted shard re-runs only its unfinished
    # configs. Returns the completed run's summary with an empty results list
    # (callers use the summary; the rows are already in results.csv).
    done_summary = _check_done(base, cfg_hash)
    if done_summary is not None:
        log.info("  [%s] config %s already DONE — skipping "
                 "(FORGETIT_FORCE_RERUN=1 to override)", store_label, cfg_hash)
        return [], done_summary

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

        def _answer(task):
            result, query, retrieved, ref, qtype = task
            qa = _run_reader(reader_impl=reader_impl, query=query, retrieved=retrieved,
                             answer_max_tokens=answer_max_tokens, ref_answer=ref,
                             question_type=qtype)
            return result, qa

        if reader_workers > 1 and len(all_ctx) > 1:
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=reader_workers) as pool:
                answered = list(pool.map(_answer, all_ctx))
        else:
            answered = [_answer(t) for t in all_ctx]

        for result, qa in answered:
            result.reader_answer = qa.reader_answer
            result.abstained = qa.abstained
            result.qa_latency_sec = qa.qa_latency_sec
            result.qa_correct = qa.qa_correct
            result.qa_f1 = qa.qa_f1

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

    # Stamp provenance on every row before writing. One throwaway store probes
    # the config-side identities (compression prompt, encoder tag).
    probe = store_factory()
    try:
        _stamp_provenance(all_results, config_hash=cfg_hash,
                          reader_impl=reader_impl, store_probe=probe)
    finally:
        clear_fn = getattr(probe, "clear", None)
        if callable(clear_fn):
            clear_fn()

    summary = summarize(all_results)
    summary.update(_flatten_timing(timing, wall_s))
    save_results_csv(all_results, base / "results.csv")
    save_summary_csv(summary, base / "summary.csv")
    save_summary_by_qtype_csv(all_results, base / "summary_by_qtype.csv")
    save_timings_json(timing, wall_s, base / "timings.json")
    _write_done(base, cfg_hash, summary)    # marker LAST: only a full save counts

    return all_results, summary