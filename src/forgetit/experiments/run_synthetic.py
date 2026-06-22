"""
Synthetic benchmark — validates that the pipeline detects known policy
differences under controlled conditions.

Budgets are per-example relative (fraction of the workload's own footprint).
The synthetic workload is a single "example" (no resets), so the budget is a
fraction of its total bytes. 100% = no-eviction control; lower fractions force
eviction, where LRU/LFU (which see the mid-stream access probes) should keep the
early gold items alive while FIFO/Random shed them.
"""
from __future__ import annotations

from forgetit.benchmark.pipeline import run_pipeline
from forgetit.benchmark.budget import example_footprints, DEFAULT_BUDGET_FRACTIONS
from forgetit.benchmark.workloads.synthetic import SyntheticWorkload
from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.logging import BenchmarkLogger
from forgetit.memory_system import RetentionManager
from forgetit.policies import FIFOPolicy, LRUPolicy, LFUPolicy, RandomPolicy
from forgetit.retrieval import LexicalOverlapRetriever

_BUDGET_SENTINEL = 1 << 62   # runner overrides per example before any insert


def main() -> None:
    workload = SyntheticWorkload()
    footprints = example_footprints(workload)   # one example → [total_bytes]
    print(f"synthetic footprint: {footprints[0]} bytes\n")

    policies = [
        ("fifo",   lambda: FIFOPolicy()),
        ("lru",    lambda: LRUPolicy()),
        ("lfu",    lambda: LFUPolicy()),
        ("random", lambda: RandomPolicy(seed=42)),
    ]

    for pname, pfac in policies:
        print(f"=== Policy: {pname} ===")
        for frac in DEFAULT_BUDGET_FRACTIONS:
            store = RetentionManager(
                budget_bytes=_BUDGET_SENTINEL,
                backend=InMemoryBackend(retriever=LexicalOverlapRetriever()),
                policy=pfac(),
                logger=BenchmarkLogger(),
            )
            pct = f"{int(round(frac * 100)):03d}pct"
            _, summary = run_pipeline(
                dataset="synthetic",
                workload=workload,
                store=store,
                store_label=f"retention/{pname}/lexical/budget_{pct}",
                k=5,
                seed=42,
                itemiser="none",
                reader="none",
                reader_impl=None,
                budget_frac=frac,
                example_footprints=footprints,
                policy_label=pname,
                retriever_label="lexical",
                out_dir="artifacts/benchmark",
            )
            print(
                f"  budget={pct}  "
                f"gold_survival={summary['gold_survival_rate']:.3f}  "
                f"hit@k={summary['hit_at_k']:.3f}  "
                f"non_gold={summary['non_gold_byte_fraction']:.3f}  "
                f"evict/q={summary['mean_evictions_per_query']:.1f}"
            )
        print()


if __name__ == "__main__":
    main()
