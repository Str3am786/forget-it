#!/usr/bin/env python3
"""
Per-cell ORACLE SOUNDNESS mask — which (dataset, itemiser, retriever, budget)
cells the oracle is actually an upper bound in.

WHY. OraclePolicy protects the union of every scored query's gold for an
example. When that union does not FIT in the example's budget, the budget loop
forces it to evict a protected item (``forced_gold_evictions``) and it
degenerates to oldest-protected-first — beatable, and no longer a ceiling. Any
"% of achievable headroom" / normalized-regret statistic computed over such a
cell is meaningless, and a dominance violation there is expected, not a defect.

``forced_gold_evictions`` is incremented on the policy object
(policies/oracle.py:64) and then NEVER emitted — it is absent from results.csv,
summary.csv and the event log — so an analyst cannot tell a sound cell from an
unsound one from the artifacts. Recomputing it per cell from ``events.jsonl``
requires a finished --event-log run.
This script computes the SAME quantity from the WORKLOAD alone: no artifacts, no
GPU, runnable before the grid is submitted.

THE COMPUTATION, per example (mirrors runner_core.py:489-494 and budget.py):
    footprint       = sum(bytes of every inserted item)
    budget_bytes    = max(1, round(budget_frac × footprint))
    gold union      = {gid for e in events if e.type == "query" for gid in e.gold_item_ids}
    gold_union_bytes= sum(bytes of the inserted items in that union)
    INFEASIBLE      iff gold_union_bytes > budget_bytes
A cell is SOUND iff no example in it is infeasible.

Soundness does not depend on the RETRIEVER (or the policy) — it is a property of
(dataset, itemiser, budget_frac) alone. The retriever column is emitted anyway so
the mask joins directly onto the results grid; the repeated rows are one
measurement, not three.

    python scripts/build_soundness_mask.py --out cell_soundness.csv
    python scripts/build_soundness_mask.py --datasets longmemeval --limit 3 --out /tmp/mask.csv
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from forgetit.benchmark.budget import _item_bytes, DEFAULT_BUDGET_FRACTIONS  # noqa: E402

DATA = Path(__file__).resolve().parents[1] / "data"
DEFAULT_RETRIEVERS = ["lexical", "bm25", "dense"]
COLUMNS = ["dataset", "itemiser", "retriever", "budget_frac", "n_examples",
           "n_infeasible", "sound", "worst_gold_over_budget_ratio"]


def _workloads(dataset: str, limit: int | None):
    """(itemiser_name, workload) for one dataset — same itemisers the runners sweep."""
    if dataset == "longmemeval":
        from forgetit.benchmark.workloads.longmemeval import (
            LongMemEvalWorkload, SessionItemiser, TurnPairItemiser)
        path, cls, its = DATA / "longmemeval_s_cleaned.json", LongMemEvalWorkload, \
            [SessionItemiser(), TurnPairItemiser()]
    elif dataset == "beam":
        from forgetit.benchmark.workloads.beam import (
            BeamWorkload, BeamTurnItemiser, BeamTurnPairItemiser, BeamBatchItemiser)
        path, cls, its = DATA / "beam_500K.jsonl", BeamWorkload, \
            [BeamTurnItemiser(), BeamTurnPairItemiser(), BeamBatchItemiser()]
    elif dataset == "locomo":
        from forgetit.benchmark.workloads.locomo import (
            LocomoWorkload, LocomoTurnItemiser, LocomoTurnPairItemiser,
            LocomoSessionItemiser)
        path, cls, its = DATA / "locomo10.json", LocomoWorkload, \
            [LocomoTurnItemiser(), LocomoTurnPairItemiser(), LocomoSessionItemiser()]
    else:
        raise SystemExit(f"unknown dataset '{dataset}'")
    if not path.exists():
        print(f"BLOCKED-MISSING-INPUT: {path} does not exist — skipping {dataset}",
              file=sys.stderr)
        return []
    return [(it.name, cls(path, itemiser=it, probe_every=1,
                          clear_between_examples=True, limit=limit)) for it in its]


def example_gold_pressure(workload) -> list[tuple[int, int, int]]:
    """(footprint, gold_union_bytes, n_gold_missing) per example — ONE replay."""
    out = []
    for _example_id, events in workload.iter_examples():
        sizes = {ev.item.id: _item_bytes(ev.item)
                 for ev in events if ev.type == "insert" and ev.item is not None}
        gold = {gid for ev in events
                if ev.type == "query" and ev.gold_item_ids
                for gid in ev.gold_item_ids}
        # A gold id with no matching insert contributes 0 bytes; count it so a
        # silently-undercounted union can never be mistaken for a feasible one.
        missing = sum(1 for g in gold if g not in sizes)
        out.append((sum(sizes.values()), sum(sizes.get(g, 0) for g in gold), missing))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--datasets", nargs="+", default=["longmemeval", "beam", "locomo"])
    ap.add_argument("--budgets", nargs="+", type=float, default=DEFAULT_BUDGET_FRACTIONS)
    ap.add_argument("--retrievers", nargs="+", default=DEFAULT_RETRIEVERS)
    ap.add_argument("--limit", type=int, default=None,
                    help="examples per dataset (smoke runs; a limited mask is NOT the grid mask)")
    ap.add_argument("--out", type=Path, default=Path("cell_soundness.csv"))
    args = ap.parse_args()

    rows, missing_total = [], 0
    for dataset in args.datasets:
        for itemiser_name, workload in _workloads(dataset, args.limit):
            pressure = example_gold_pressure(workload)
            missing_total += sum(m for _, _, m in pressure)
            print(f"{dataset}/{itemiser_name}: {len(pressure)} examples")
            for frac in args.budgets:
                infeasible, worst = 0, 0.0
                for footprint, gold_bytes, _missing in pressure:
                    budget = max(1, round(frac * footprint))
                    if gold_bytes > budget:
                        infeasible += 1
                    worst = max(worst, gold_bytes / budget)
                for retriever in args.retrievers:
                    rows.append({
                        "dataset": dataset, "itemiser": itemiser_name,
                        "retriever": retriever, "budget_frac": frac,
                        "n_examples": len(pressure), "n_infeasible": infeasible,
                        "sound": infeasible == 0,
                        "worst_gold_over_budget_ratio": round(worst, 4),
                    })
                print(f"    budget_frac={frac:<5} infeasible={infeasible}/{len(pressure)}"
                      f"  worst gold/budget={worst:.3f}  sound={infeasible == 0}")

    if not rows:
        print("no cells computed — every requested dataset was missing", file=sys.stderr)
        return 2
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)

    unsound = [r for r in rows if not r["sound"]]
    print(f"\nwrote {args.out}: {len(rows)} cells, {len(unsound)} UNSOUND "
          f"(oracle is not an upper bound there — exclude from normalized regret)")
    if missing_total:
        print(f"WARNING: {missing_total} gold ids had no matching insert; their bytes "
              f"count as 0, so gold_union_bytes is a LOWER bound in those examples "
              f"and a 'sound' verdict there is optimistic.", file=sys.stderr)
    if args.limit is not None:
        print(f"NOTE: --limit {args.limit} — this mask covers a PREFIX of each dataset, "
              f"not the grid.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
