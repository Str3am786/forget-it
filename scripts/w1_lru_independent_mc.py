"""Q4 adjudication artifact — independent MC of no-re-admission LRU on W1.

Re-implements LRU-with-incoming-exempt-budget-loop in ~15 lines WITHOUT
importing any forgetit store/policy code (only the trace builder), and runs
it over many seeds. If the real pipeline's measured W1 LRU survival sits
inside this reference band, the implementation is vindicated and the failing
Che prediction is an ORACLE mis-specification (Che assumes miss→re-admission;
this pipeline inserts each item once and never re-admits).

    python scripts/w1_lru_independent_mc.py [n_seeds]
"""
from __future__ import annotations

import statistics
import sys

from forgetit.benchmark.workloads.synthetic_controls import build_w1

M = 50   # budget 5000 B / 100 B items


def lru_no_readmit(trace) -> float:
    last: dict[int, int] = {}
    tick = 0
    resident: list[int] = []
    for e in trace.events:
        if e.type == "insert":
            tick += 1
            idx = int(e.item.id[2:])
            while len(resident) >= M:                       # incoming exempt
                victim = min(resident, key=lambda i: last[i])
                resident.remove(victim)
            resident.append(idx)
            last[idx] = tick
        elif e.query is not None and "::probe::" in e.query.id:
            tick += 1
            target = int(e.query.meta["target"][2:])
            if target in resident:
                last[target] = tick
            else:
                # retriever backfill on a zero-score miss touches the NEWEST
                # resident (created_at-reverse tie-break) — mirrored here.
                last[max(resident)] = tick
    return sum(1 for g in trace.gold_indices if g in resident) / len(trace.gold_indices)


def main() -> None:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    survs = [lru_no_readmit(build_w1(seed)) for seed in range(n)]
    print(f"independent-MC LRU (no re-admission), {n} seeds: "
          f"mean={sum(survs)/len(survs):.4f} sd={statistics.pstdev(survs):.4f}")


if __name__ == "__main__":
    main()
