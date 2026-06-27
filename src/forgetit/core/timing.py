"""Phase timing — a wall-clock trace of where the pipeline spends time.

The decomposition (per example, summed across the run) is:

  embed_ingest  — attaching item vectors before replay        (dense only)
  insert        — admitting an item (upsert + accounting), eviction EXCLUDED
  evict_select  — the policy deciding WHAT to evict/compress   (coarse)
      compress_llm  — └─ the summarisation LLM call            (sub-phase ⊂ evict_select)
  evict_exec    — APPLYING the eviction/compression            (coarse)
      reembed       — └─ re-embedding a compressed item        (sub-phase ⊂ evict_exec)
  retrieve      — ranking the store for a query                (coarse)
      query_embed   — └─ embedding the query (dense)           (sub-phase ⊂ retrieve)
  reader        — the QA reader answering                      (added from qa_latency)

The coarse phases are disjoint critical-path spans (they sum to the per-example
processing time); the sub-phases are drill-downs nested inside a coarse one.

Mechanism: a THREAD-LOCAL timer, so the across-example thread pool never
cross-contaminates — each worker stamps into its own example's timer. Components
(store, retriever, embedder-callers) record via ``with phase(name):`` without any
signature threading; when no timer is active (e.g. the equivalence reference run)
``phase`` is a near-zero no-op. Totals are WORK-seconds (summed over concurrent
units), not wall-clock — that's the meaningful "where does the cost go" view; the
driver records wall-clock separately so wall/work shows the concurrency speedup.
"""
from __future__ import annotations

import threading
import time
from collections import defaultdict
from contextlib import contextmanager

_local = threading.local()

# Phases that partition the critical path (sum ≈ per-example processing time).
COARSE_PHASES: tuple[str, ...] = (
    "embed_ingest", "insert", "evict_select", "evict_exec", "retrieve", "reader",
)
# Drill-downs nested inside a coarse phase (NOT added to the coarse sum).
SUB_PHASES: tuple[str, ...] = ("compress_llm", "reembed", "query_embed")


class PhaseTimer:
    """Accumulates work-seconds + call counts per named phase."""
    __slots__ = ("totals", "counts")

    def __init__(self) -> None:
        self.totals: dict[str, float] = defaultdict(float)
        self.counts: dict[str, int] = defaultdict(int)

    def add(self, phase: str, dt: float) -> None:
        self.totals[phase] += dt
        self.counts[phase] += 1

    def as_dict(self) -> dict[str, dict[str, float]]:
        return {p: {"total_s": self.totals[p], "n": self.counts[p]} for p in self.totals}


def current() -> "PhaseTimer | None":
    return getattr(_local, "timer", None)


@contextmanager
def use_timer(timer: PhaseTimer):
    """Make ``timer`` the active timer for this thread (nestable, restores prev)."""
    prev = getattr(_local, "timer", None)
    _local.timer = timer
    try:
        yield timer
    finally:
        _local.timer = prev


@contextmanager
def phase(name: str):
    """Time the enclosed block into the active thread-local timer (no-op if none)."""
    t = current()
    if t is None:
        yield
        return
    t0 = time.perf_counter()
    try:
        yield
    finally:
        t.add(name, time.perf_counter() - t0)


def record(name: str, dt: float) -> None:
    """Add a pre-measured duration to the active timer (no-op if none). Use when a
    duration was already measured elsewhere (e.g. reader qa_latency)."""
    t = current()
    if t is not None:
        t.add(name, dt)


def merge_dicts(dicts) -> dict[str, dict[str, float]]:
    """Sum a list of ``PhaseTimer.as_dict()`` results into one (totals + counts)."""
    out: dict[str, dict[str, float]] = {}
    for d in dicts:
        if not d:
            continue
        for p, v in d.items():
            o = out.setdefault(p, {"total_s": 0.0, "n": 0})
            o["total_s"] += v.get("total_s", 0.0)
            o["n"] += v.get("n", 0)
    return out
