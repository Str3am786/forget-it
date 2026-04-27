"""
Eviction stress experiment — diagnostic for unbounded vs. budget-managed stores.

What it does
------------
Generates N synthetic MemRecords of a fixed size, inserts them all, then
queries for the *oldest* half.  With a budget small enough to hold only a few
items, LRU must evict aggressively before the queries arrive — so the oldest
items should be gone.

LettaWrapper has no budget and no eviction logic; it accumulates every passage.
Comparing the two stores side-by-side answers:

  RetentionManager:  Are evictions actually being triggered?
                     (evictions_total_at_query_time > 0, gold_survival ≈ 0)
  LettaWrapper:      Is Letta keeping everything?
                     (evictions_total = 0, hit@k should be non-zero if
                      semantic search is working)

Run
---
    python -m forgetit.experiments.run_eviction_stress
"""
from __future__ import annotations

import logging
import time
from typing import Iterator

from forgetit.benchmark.pipeline import run_pipeline
from forgetit.benchmark.schema import WorkloadEvent
from forgetit.benchmark.workloads.base import Workload
from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.logging import BenchmarkLogger
from forgetit.core.schema import MemRecord, Query
from forgetit.memory_system import RetentionManager
from forgetit.memory_system.wrappers.memgpt_sys import LettaWrapper
from forgetit.policies import LRUPolicy, LFUPolicy
from forgetit.retrieval import BM25Retriever
from forgetit.settings import LETTA_BASE_URL, LETTA_API_KEY


# ── Synthetic workload ────────────────────────────────────────────────────────

_ITEM_TEMPLATE = (
    "Conversation record {i:04d}: the user stated their unique preference "
    "is item_{i:04d}. This fact was recorded at timestamp {ts:.0f} and "
    "should be retrievable by asking about preference {i:04d}."
)

# Each query is semantically close to its target item.
_QUERY_TEMPLATE = (
    "What is the user preference for item_{i:04d}? "
    "Recall the unique fact about preference number {i:04d}."
)


class SyntheticEvictionWorkload:
    """
    All inserts first, then queries for the oldest ``n_queries`` items.

    Budget stores will have evicted those items before any query fires;
    unbounded stores (Letta) will still have them.  This maximises the
    contrast between the two store types.
    """

    name = "synthetic_eviction"

    def __init__(
        self,
        n_items: int = 40,
        item_bytes: int = 300,
        n_queries: int | None = None,
    ):
        self.n_items = n_items
        self.item_bytes = item_bytes
        # Query for the oldest half by default.
        self.n_queries = n_queries if n_queries is not None else n_items // 2
        self._records: list[MemRecord] = self._build_records()

    def _build_records(self) -> list[MemRecord]:
        t0 = 1_700_000_000.0  # fixed epoch so runs are reproducible
        records = []
        for i in range(self.n_items):
            core = _ITEM_TEMPLATE.format(i=i, ts=t0 + i)
            # Pad to reach the target byte size for predictable budget pressure.
            pad_len = max(0, self.item_bytes - len(core.encode()))
            text = core + ("." * pad_len)
            records.append(
                MemRecord(
                    id=f"item_{i:04d}",
                    text=text,
                    created_at=t0 + i,
                    bytes=len(text.encode()),
                )
            )
        return records

    def events(self) -> Iterator[WorkloadEvent]:
        # Phase 1: insert everything
        for rec in self._records:
            yield WorkloadEvent(
                type="insert",
                timestamp=rec.created_at,
                item=rec,
                query=None,
                gold_item_ids=[],
            )

        # Phase 2: query for the oldest items
        # Under a tight budget these should have been evicted already.
        for i in range(self.n_queries):
            rec = self._records[i]
            yield WorkloadEvent(
                type="query",
                timestamp=rec.created_at + self.n_items + 1,
                item=None,
                query=Query(
                    id=f"q_{i:04d}",
                    text=_QUERY_TEMPLATE.format(i=i),
                    timestamp=rec.created_at + self.n_items + 1,
                ),
                gold_item_ids=[rec.id],
            )


# ── Store builders ────────────────────────────────────────────────────────────


def _retention_store(budget: int, policy_name: str = "lru"):
    policy = LRUPolicy() if policy_name == "lru" else LFUPolicy()
    backend = InMemoryBackend(retriever=BM25Retriever())
    logger = BenchmarkLogger()
    store = RetentionManager(
        budget_bytes=budget,
        backend=backend,
        policy=policy,
        logger=logger,
    )
    label = f"retention/{policy_name}/bm25/budget_{budget}"
    return label, store, {"budget_bytes": budget, "policy_label": policy_name, "retriever_label": "bm25"}


def _letta_store():
    store = LettaWrapper(
        base_url=LETTA_BASE_URL,
        api_key=LETTA_API_KEY,
        agent_name="eviction_stress_agent",
    )
    return "letta/default", store, {}


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )

    workload = SyntheticEvictionWorkload(n_items=40, item_bytes=300)
    total_bytes = sum(r.bytes for r in workload._records)
    n_queries = workload.n_queries

    # Budget fits only ~3 items → constant eviction pressure.
    tiny_budget = workload.item_bytes * 3
    # Budget fits all items → no eviction (control run).
    full_budget = total_bytes + 1

    logging.info(
        "Synthetic workload: %d items × %d bytes = %d bytes total | "
        "tiny_budget=%d (%d items) | n_queries=%d (oldest items)",
        workload.n_items, workload.item_bytes, total_bytes,
        tiny_budget, tiny_budget // workload.item_bytes, n_queries,
    )

    all_results = []

    # ── RetentionManager: tiny budget (forced evictions) ──────────────────
    logging.info("═" * 60)
    logging.info("Run 1 / 3 — RetentionManager  budget=%d bytes  (FORCED EVICTION)", tiny_budget)
    label, store, extras = _retention_store(tiny_budget, "lru")
    results_tiny, summary_tiny = run_pipeline(
        dataset="synthetic_eviction",
        workload=SyntheticEvictionWorkload(n_items=40, item_bytes=300),
        store=store,
        store_label=label,
        k=5,
        seed=0,
        out_dir="artifacts/eviction_stress",
        **extras,
    )
    all_results.extend(results_tiny)

    # ── RetentionManager: full budget (no evictions — control) ────────────
    logging.info("═" * 60)
    logging.info("Run 2 / 3 — RetentionManager  budget=%d bytes  (CONTROL, no eviction)", full_budget)
    label_ctrl, store_ctrl, extras_ctrl = _retention_store(full_budget, "lru")
    results_ctrl, summary_ctrl = run_pipeline(
        dataset="synthetic_eviction",
        workload=SyntheticEvictionWorkload(n_items=40, item_bytes=300),
        store=store_ctrl,
        store_label=label_ctrl,
        k=5,
        seed=0,
        out_dir="artifacts/eviction_stress",
        **extras_ctrl,
    )
    all_results.extend(results_ctrl)

    # ── LettaWrapper: no budget, no eviction ──────────────────────────────
    logging.info("═" * 60)
    logging.info("Run 3 / 3 — LettaWrapper  (unbounded, no eviction)")
    t0_letta = time.perf_counter()
    label_letta, store_letta, extras_letta = _letta_store()
    results_letta, summary_letta = run_pipeline(
        dataset="synthetic_eviction",
        workload=SyntheticEvictionWorkload(n_items=40, item_bytes=300),
        store=store_letta,
        store_label=label_letta,
        k=5,
        seed=0,
        out_dir="artifacts/eviction_stress",
        **extras_letta,
    )
    all_results.extend(results_letta)
    letta_elapsed = time.perf_counter() - t0_letta

    # ── Diagnostic summary ───────────────────────────────────────────────
    logging.info("═" * 60)
    logging.info("DIAGNOSTIC SUMMARY")
    logging.info("%-50s  %8s  %10s  %10s  %10s", "store", "hit@k", "gold_surv", "evictions", "introspect")
    logging.info("-" * 100)

    for label, results, summary in [
        (label,      results_tiny, summary_tiny),
        (label_ctrl, results_ctrl, summary_ctrl),
        (label_letta, results_letta, summary_letta),
    ]:
        evictions = max((r.evictions_total_at_query_time for r in results), default=0)
        introspectable = all(r.introspectable for r in results) if results else False
        logging.info(
            "%-50s  %8.1f%%  %10.1f%%  %10d  %10s",
            label,
            100 * summary["hit_at_k"],
            100 * summary["gold_survival_rate"],
            evictions,
            "yes" if introspectable else "no (opaque)",
        )

    logging.info("")
    logging.info("What to look for:")
    logging.info("  RetentionManager tiny budget  → evictions > 0, gold_survival≈0%, hit@k≈0%%")
    logging.info("  RetentionManager full budget  → evictions = 0, gold_survival≈100%%, hit@k≈100%%")
    logging.info("  LettaWrapper                  → evictions = 0 (opaque, no budget), hit@k > 0 if semantic search works")
    logging.info("")
    logging.info("If LettaWrapper hit@k ≈ 0: retrieval is broken (check embedding endpoint / agent config).")
    logging.info("If LettaWrapper hit@k ≈ 100%%: Letta keeps everything — it does NOT evict (expected).")
    logging.info("Letta total elapsed: %.1fs", letta_elapsed)


if __name__ == "__main__":
    main()
