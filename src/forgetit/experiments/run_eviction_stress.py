"""
Eviction stress experiment — diagnostic for policy behaviour under budget pressure.

What it does
------------
Generates N synthetic MemRecords of a fixed size and emits them as an insert
stream.  A subset of items is pre-designated as "gold" (the ones we care about
retrieving).  Before and between inserts the workload fires access-only probe
queries for those gold items so that LRU/LFU can rank them as recently used.

Two workload modes are compared:
  unprobed  — all inserts first, then queries (old behaviour; FIFO/LRU/LFU all
              collapse because there is no prior usage signal before eviction)
  probed    — an access-only query is interleaved after each insert (mimics the
              Fix 1 probing added to LongMemEvalWorkload)

Three policies are compared on the probed workload:
  FIFO      — baseline; always evicts oldest-inserted
  LRU       — should protect recently-probed gold items
  LFU       — should protect frequently-probed gold items
  Random    — stochastic baseline

A budget smaller than one full example keeps eviction active throughout.

Run
---
    python -m forgetit.experiments.run_eviction_stress
"""
from __future__ import annotations

import logging
from typing import Iterator

from forgetit.benchmark.pipeline import run_pipeline
from forgetit.benchmark.schema import WorkloadEvent
from forgetit.benchmark.workloads.base import Workload
from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.logging import BenchmarkLogger
from forgetit.core.schema import MemRecord, Query
from forgetit.memory_system import RetentionManager
from forgetit.policies import FIFOPolicy, LRUPolicy, LFUPolicy, RandomPolicy
from forgetit.retrieval import BM25Retriever


# ── Synthetic workload ────────────────────────────────────────────────────────

# Gold items carry a distinctive, high-entropy token (zorpcode_NNNN) so that
# BM25 probes isolate the *specific* gold item rather than a cloud of similar
# filler.  Filler items deliberately share generic vocabulary.
_GOLD_TEMPLATE = (
    "Conversation record {i:04d}: the user's secret access phrase is "
    "zorpcode_{i:04d}. This unique fact zorpcode_{i:04d} was recorded at "
    "timestamp {ts:.0f} and must be retrievable by asking for zorpcode_{i:04d}."
)

_FILLER_TEMPLATE = (
    "Conversation record {i:04d}: generic small talk about the weather, "
    "scheduling, and miscellaneous chit-chat recorded at timestamp {ts:.0f}. "
    "Nothing important was said in this routine exchange number {i:04d}."
)

# Probe/query text targets one specific gold item via its distinctive token.
_QUERY_TEMPLATE = "What is the secret access phrase zorpcode_{i:04d}?"


class SyntheticEvictionWorkload:
    """
    Interleaved insert+probe stream followed by scored queries.

    Gold items receive access-only probe queries after each of their inserts
    (when probed=True), giving LRU/LFU a usage signal before later inserts
    push them into eviction range.  Setting probed=False reproduces the old
    all-inserts-then-queries pattern where every policy collapses to FIFO.

    Parameters
    ----------
    n_items   : total items to insert
    item_bytes: target byte size per item (padded to be predictable)
    n_gold    : number of items (the FIRST n_gold inserted) designated as gold
    n_probes  : how many access-only probes to fire for each gold item after
                its insert (only used when probed=True)
    probed    : whether to interleave access probes during the insert stream
    """

    name = "synthetic_eviction"

    def __init__(
        self,
        n_items: int = 40,
        item_bytes: int = 300,
        n_gold: int | None = None,
        n_probes: int = 3,
        probed: bool = True,
    ):
        self.n_items = n_items
        self.item_bytes = item_bytes
        # Gold = the first n_gold items inserted (they'll be under maximum
        # eviction pressure since later inserts push them oldest-first).
        self.n_gold = n_gold if n_gold is not None else max(1, n_items // 5)
        self.n_probes = n_probes
        self.probed = probed
        self._records: list[MemRecord] = self._build_records()

    def _build_records(self) -> list[MemRecord]:
        t0 = 1_700_000_000.0  # fixed epoch so runs are reproducible
        records = []
        for i in range(self.n_items):
            template = _GOLD_TEMPLATE if i < self.n_gold else _FILLER_TEMPLATE
            core = template.format(i=i, ts=t0 + i)
            pad_len = max(0, self.item_bytes - len(core.encode()))
            text = core + (" filler" * (pad_len // 7))
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
        gold_ids = {self._records[i].id for i in range(self.n_gold)}

        # Phase 1: insert stream with optional probes.
        # After each insert we fire one probe query per gold item.
        # This means every subsequent insert will see gold items accessed more
        # recently than the latest non-gold insert for LRU, and with higher
        # access_count for LFU.  Probing only at initial insert is insufficient
        # for LRU (later inserts would overwrite recency), so we probe on every
        # insert in the stream.
        for insert_idx, rec in enumerate(self._records):
            yield WorkloadEvent(
                type="insert",
                timestamp=rec.created_at,
                item=rec,
                query=None,
                gold_item_ids=[],
            )

            if self.probed:
                # Probe every gold item after every insert so their recency/
                # frequency stays ahead of non-gold items for LRU and LFU.
                for g_i in range(self.n_gold):
                    g_rec = self._records[g_i]
                    yield WorkloadEvent(
                        type="query",
                        timestamp=rec.created_at + 0.1 + g_i * 0.001,
                        item=None,
                        query=Query(
                            id=f"probe_gold{g_i:02d}_after{insert_idx:03d}",
                            text=_QUERY_TEMPLATE.format(i=g_i),
                            timestamp=rec.created_at + 0.1 + g_i * 0.001,
                        ),
                        gold_item_ids=[],  # access-only — not scored
                    )

        # Phase 2: scored queries for ALL gold items
        t_query = self._records[-1].created_at + self.n_items + 1
        for i in range(self.n_gold):
            rec = self._records[i]
            yield WorkloadEvent(
                type="query",
                timestamp=t_query + i,
                item=None,
                query=Query(
                    id=f"q_{i:04d}",
                    text=_QUERY_TEMPLATE.format(i=i),
                    timestamp=t_query + i,
                ),
                gold_item_ids=[rec.id],
            )


# ── Store builders ────────────────────────────────────────────────────────────


def _retention_store(budget: int, policy, policy_name: str):
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


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    logging.basicConfig(
        level=logging.WARNING,       # suppress per-event noise; we print our own table
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)

    n_items = 40
    item_bytes = 300
    n_gold = 5       # first 5 items are gold (inserted earliest → max pressure)
    n_probes = 3     # probe each gold item 3× after insert

    # Budget holds ~12 items: enough that an eviction policy *can* retain all 5
    # gold (so an oracle scores 100%), but small enough that the 40-item stream
    # forces constant eviction.  Gold being inserted first means a recency/
    # frequency-blind policy (FIFO/Random) will shed them; LRU/LFU should keep
    # them alive via the interleaved probe signal.
    tiny_budget = item_bytes * 12

    seed = 42

    print()
    print("=" * 80)
    print("EVICTION STRESS  —  policy comparison")
    print(f"  n_items={n_items}  item_bytes={item_bytes}  n_gold={n_gold}")
    print(f"  budget={tiny_budget} bytes (~{tiny_budget // item_bytes} items)")
    print(f"  n_probes={n_probes} per gold item (probed mode only)")
    print("=" * 80)

    policies = [
        ("fifo",   FIFOPolicy()),
        ("lru",    LRUPolicy()),
        ("lfu",    LFUPolicy()),
        ("random", RandomPolicy(seed=seed)),
    ]

    results_table: list[tuple[str, str, dict]] = []

    for mode, probed in [("unprobed", False), ("probed", True)]:
        print(f"\n--- Mode: {mode} ---")
        workload_cls = lambda p=probed: SyntheticEvictionWorkload(
            n_items=n_items,
            item_bytes=item_bytes,
            n_gold=n_gold,
            n_probes=n_probes,
            probed=p,
        )

        for pname, policy in policies:
            label, store, extras = _retention_store(tiny_budget, policy, pname)
            _, summary = run_pipeline(
                dataset="synthetic_eviction",
                workload=workload_cls(),
                store=store,
                store_label=label,
                k=5,
                seed=seed,
                out_dir="artifacts/eviction_stress",
                **extras,
            )
            results_table.append((mode, pname, summary))

    # ── Print comparison table ────────────────────────────────────────────────
    print()
    print("=" * 80)
    print("RESULTS SUMMARY")
    print(f"  (budget={tiny_budget} bytes = ~{tiny_budget // item_bytes} items; gold=first {n_gold} items)")
    print()
    print(f"  {'mode':<12} {'policy':<8}  {'gold_surv%':>10}  {'hit@k%':>8}")
    print(f"  {'-'*12} {'-'*8}  {'-'*10}  {'-'*8}")

    for mode, pname, summary in results_table:
        print(
            f"  {mode:<12} {pname:<8}  "
            f"{100*summary['gold_survival_rate']:>9.1f}%  "
            f"{100*summary['hit_at_k']:>7.1f}%"
        )

    print()
    print("What to look for:")
    print("  Unprobed mode: all policies should behave similarly (no usage signal).")
    print("  Probed mode:   LRU and LFU should outperform FIFO and match or beat Random,")
    print("                 because gold items received access bumps before eviction fired.")
    print("=" * 80)
    print()


if __name__ == "__main__":
    main()
