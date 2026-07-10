"""S1 smoke (stamped smoke=true; excluded from all tables/budgets).

Replays 2 BEAM conversations (turn_pair, budget 25%) through run_one_example
under (a) the compression policy with the deterministic stub summariser — the
ITEM-RICHEST store shape, where sprint-1 censoring hit 35-42% — and (b) plain
LRU, then counts censoring against the new unconditional column:

    censored := gold_survived AND (some surviving gold id has no entry in
                gold_ranks_true)

Pass bar (S1): < 1% on the compression cell. Expected: exactly 0 — a resident
item always appears somewhere in the full ranking.

No LLM calls (stub compressor = plumbing). Output: report2/s1_smoke.json.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from forgetit.backend.in_memory import InMemoryBackend
from forgetit.benchmark.runner_core import run_one_example
from forgetit.benchmark.workloads.beam import BeamWorkload, BeamTurnPairItemiser
from forgetit.core.cost import record_cost
from forgetit.memory_system import RetentionManager
from forgetit.policies import LRUPolicy, LFUPolicy, LLMCompressionPolicy
from forgetit.retrieval import BM25Retriever

from tests.corpora.harness import StubSummariser

BEAM = REPO / "data" / "beam_500K.jsonl"


class _L:
    def event(self, *a, **k):
        pass


def run_cell(policy_factory, label):
    wl = BeamWorkload(BEAM, itemiser=BeamTurnPairItemiser(), probe_every=1,
                      clear_between_examples=True, limit=2)
    rows = []
    for conv_id, events in wl.iter_examples():
        footprint = sum(record_cost(e.item) for e in events if e.type == "insert")
        store = RetentionManager(budget_bytes=max(1, round(0.25 * footprint)),
                                 backend=InMemoryBackend(retriever=BM25Retriever()),
                                 policy=policy_factory(), logger=_L())
        store.connect()
        results, _, _, _ = run_one_example(events, store, k=5, dataset="beam",
                                           store_label=label, policy=label)
        rows.extend(results)
    n_gold_rows = censored = 0
    resid = []
    for r in rows:
        if not r.gold_survived:
            continue
        n_gold_rows += 1
        true_ranks = json.loads(r.gold_ranks_true)
        # every SURVIVING gold must appear in the full ranking: the row is
        # censored iff fewer true ranks than surviving gold items … we don't
        # know surviving-count per row here, but ≥1 surviving gold with zero
        # ranks is the unambiguous censoring signal; also check percentile.
        if not true_ranks or r.gold_rank_censored == 1.0:
            censored += 1
        resid.append(r.n_resident_at_query)
    return {"policy": label, "n_rows": len(rows), "n_gold_surviving_rows": n_gold_rows,
            "censored_rows": censored,
            "censoring_rate": round(censored / n_gold_rows, 4) if n_gold_rows else None,
            "mean_n_resident": round(sum(resid) / len(resid), 1) if resid else None}


def main() -> int:
    out = {"smoke": True, "note": "plumbing validation only — excluded from all "
           "tables and budgets (stub compressor; 2 conversations)"}
    out["cells"] = [
        run_cell(lambda: LLMCompressionPolicy(llm=StubSummariser(150),
                                              selector=LFUPolicy()), "compression_stub"),
        run_cell(lambda: LRUPolicy(), "lru"),
    ]
    (REPO / "report2" / "s1_smoke.json").write_text(json.dumps(out, indent=1))
    print(json.dumps(out, indent=1))
    comp = out["cells"][0]
    ok = comp["censoring_rate"] is not None and comp["censoring_rate"] < 0.01
    print("S1 smoke:", "PASS (<1% on compression cell)" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
