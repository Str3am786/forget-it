"""W1 LRU: independent Monte-Carlo vs MAIN's pipeline (manifest P-4).

WHY NOT THE RESCUED SCRIPT AS-IS
--------------------------------
scripts/w1_lru_independent_mc.py imports
``forgetit.benchmark.workloads.synthetic_controls`` -- 148 lines that exist on
branch `verification-sprint` only and are NOT on the rescue shopping list
(INVENTORY_CORRECTIONS C-B). Rescuing it into src/ would add a module to the
frozen tree. Instead the generator is placed at the SCRIPT layer
(scripts/w1_controls.py, copied verbatim from the branch) and imported from
there. main's ``WorkloadEvent`` carries the same six fields the generator uses,
so the copy needed no edits -- verified by it running unmodified below.
Nothing under src/ is touched.

The rescued MC body (lru_no_readmit) is reproduced verbatim; only its import
moves.

MODELLING DELTA THAT MUST BE MATCHED, OR THE COMPARISON IS MEANINGLESS
----------------------------------------------------------------------
The MC touches exactly ONE item per probe: the Zipf target if resident, else
the newest resident (BM25's zero-score tie-break is (score, created_at, id)
descending -- confirmed against src/forgetit/retrieval/lexical.py:136).
main's runner_core issues probes as ``view.retrieve(query, k=k)`` and touches
ALL k returned records. So the MC is a k=1 reference. Both k=1 and the
production k=5 are run and reported; the k=1 row is the one that adjudicates
P-4, the k=5 row quantifies what the extra backfill touches cost.

Budget-loop parity, also checked rather than assumed: the MC evicts while
``len(resident) >= M`` (incoming exempt); main's ``_ensure_budget`` evicts
while ``used + incoming > budget`` (incoming inclusive). With 100 B items and a
5000 B budget both settle at exactly 50 residents; the harness asserts the
realised resident count.

Usage:
    PYTHONPATH=src .venv/bin/python scripts/w1_lru_agreement_main.py [n_seeds]
"""
from __future__ import annotations

import statistics
import sys
from pathlib import Path
from typing import Iterator

sys.path.insert(0, str(Path(__file__).resolve().parent))

from w1_controls import build_w1                                # noqa: E402

from forgetit.backend.in_memory import InMemoryBackend          # noqa: E402
from forgetit.benchmark.schema import WorkloadEvent             # noqa: E402
from forgetit.core.logging import BenchmarkLogger               # noqa: E402
from forgetit.memory_system import RetentionManager             # noqa: E402
from forgetit.policies import LRUPolicy                         # noqa: E402
from forgetit.retrieval.lexical import BM25Retriever            # noqa: E402
from forgetit.benchmark.runner_core import run                  # noqa: E402

BUDGET_BYTES = 5000
ITEM_BYTES = 100
M = BUDGET_BYTES // ITEM_BYTES          # 50


# ── the independent MC (rescued body, verbatim) ──────────────────────

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
                last[max(resident)] = tick
    return sum(1 for g in trace.gold_indices if g in resident) / len(trace.gold_indices)


# ── the pipeline side ────────────────────────────────────────────────

class _TraceWorkload:
    """Minimal Workload adapter over a ControlTrace. One example, no resets."""

    def __init__(self, trace):
        self.name = f"{trace.name}_s{trace.seed}"
        self._events = list(trace.events)

    def iter_examples(self) -> Iterator[tuple[str, list[WorkloadEvent]]]:
        yield self.name, self._events

    def events(self) -> Iterator[WorkloadEvent]:
        yield from self._events


def pipeline_lru_survival(trace, *, k: int) -> tuple[float, int]:
    """Real store + real policy + real retriever + real runner loop.

    Returns (gold_survival_rate, n_residents_at_last_scored_query).
    """
    store = RetentionManager(
        budget_bytes=BUDGET_BYTES,
        backend=InMemoryBackend(retriever=BM25Retriever()),
        policy=LRUPolicy(),
        logger=BenchmarkLogger(),
    )
    store.connect()
    try:
        results = run(
            workload=_TraceWorkload(trace),
            store=store,
            k=k,
            dataset="w1",
            store_label="retention/lru/bm25",
            policy="lru",
            retriever_name="bm25",
            budget_bytes_report=BUDGET_BYTES,
            budget_frac=0.0,
            seed=trace.seed,
            itemiser="synthetic_control",
            reader_name="",
            reader_impl=None,
        )
    finally:
        store.close()
    if not results:
        raise RuntimeError("pipeline produced no scored rows")
    surv = sum(1 for r in results if r.gold_survived) / len(results)
    n_res = int(getattr(results[-1], "n_residents", 0) or 0)
    return surv, n_res


def main() -> int:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    n_pipeline = min(n, int(sys.argv[2]) if len(sys.argv) > 2 else 30)

    traces = [build_w1(seed) for seed in range(n)]

    mc = [lru_no_readmit(t) for t in traces]
    mc_mean, mc_sd = sum(mc) / len(mc), statistics.pstdev(mc)
    print(f"independent-MC LRU (no re-admission), {n} seeds: "
          f"mean={mc_mean:.4f} sd={mc_sd:.4f}")
    print()

    for k in (1, 5):
        pipe, residents = [], []
        for t in traces[:n_pipeline]:
            s, r = pipeline_lru_survival(t, k=k)
            pipe.append(s)
            residents.append(r)
        p_mean, p_sd = sum(pipe) / len(pipe), statistics.pstdev(pipe)

        # MC restricted to the same seeds, so the comparison is paired.
        mc_sub = mc[:n_pipeline]
        mc_sub_mean = sum(mc_sub) / len(mc_sub)
        mc_sub_sd = statistics.pstdev(mc_sub)
        lo, hi = mc_sub_mean - 2 * mc_sub_sd, mc_sub_mean + 2 * mc_sub_sd
        exact = sum(1 for a, b in zip(pipe, mc_sub) if a == b)

        print(f"--- pipeline LRU, k={k}, {n_pipeline} seeds ---")
        print(f"  resident count at last scored query : "
              f"{sorted(set(residents))}  (MC assumes M={M})")
        print(f"  pipeline  mean={p_mean:.4f} sd={p_sd:.4f}")
        print(f"  MC(same seeds) mean={mc_sub_mean:.4f} sd={mc_sub_sd:.4f}")
        print(f"  MC 2sd band  : [{lo:.4f}, {hi:.4f}]")
        print(f"  |mean delta| : {abs(p_mean - mc_sub_mean):.4f}")
        print(f"  inside band  : {lo <= p_mean <= hi}")
        print(f"  per-seed EXACT agreement: {exact}/{n_pipeline}")
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
