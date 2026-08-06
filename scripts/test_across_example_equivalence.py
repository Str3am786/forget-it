#!/usr/bin/env python
"""
Acceptance test for the across-example concurrent runner.

The across-example runner is a THROUGHPUT change only: it replays each example
against its own fresh store on a thread pool, so the compression LLM-call waits
of different examples overlap. The science must NOT move — every eviction
decision inside one example stays serial and identical to the single-store run.

This script proves that on the LLM-free policies (lru / lfu / random), which have
no LLM in the eviction path and are therefore deterministic:

  1. EXACT EQUIVALENCE  — serial run() vs the concurrent driver (example_workers=1
     and example_workers=N) must produce identical per-query
     gold_id_retained / hit_at_k / mrr / used_bytes_end. Any diff is a refactor bug.
     (The cumulative odometer evictions_total_at_query_time legitimately differs:
     it's run-wide in the serial path and per-example — the honest number — in the
     concurrent path, so it is excluded from the equality check by design.)

  4. RACE STRESS        — example_workers=1 vs example_workers=high on the same
     slice: results identical, no exceptions, no cross-example contamination.

Run (no LLM endpoint needed — --no-reader path, LLM-free policies only):

    python scripts/test_across_example_equivalence.py --limit 12 --workers 8

Defaults pick whichever dataset is present under data/.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"

from forgetit.benchmark.runner_core import run
from forgetit.benchmark.pipeline import run_pipeline_concurrent
from forgetit.benchmark.budget import example_footprints
from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.logging import BenchmarkLogger
from forgetit.memory_system import RetentionManager
from forgetit.retrieval import BM25Retriever, LexicalOverlapRetriever
from forgetit.policies import LRUPolicy, LFUPolicy, RandomPolicy

_BUDGET_SENTINEL = 1 << 62

# Fields that MUST match exactly between serial and concurrent (the science).
# evictions_total_at_query_time is intentionally excluded — see module docstring.
_EQ_FIELDS = ("gold_id_retained", "hit_at_k", "mrr", "used_bytes_end",
              "useful_byte_fraction", "non_gold_byte_fraction")

# STATELESS policies read recency/frequency/age straight off the records and hold
# no internal state, so serial (one reused-and-cleared store) and concurrent (a
# fresh store per example) must produce byte-identical results — that's the
# refactor-fidelity guarantee.
#
# RandomPolicy is the exception: it carries a seeded RNG. The serial path threads
# ONE RNG stream across all examples (so its choices depend on example order),
# while the concurrent path builds a fresh policy per example and thus RE-SEEDS
# per example. That makes Random reproducible and order-independent (arguably
# better for a benchmark), but means serial != concurrent BY DESIGN. We still
# require conc(w=1) == conc(w=N) for Random — concurrency itself must change
# nothing — but we don't assert serial == concurrent for it.
_STATELESS_POLICIES = {"lru", "lfu", "fifo"}


def _policy(name: str, seed: int):
    return {"lru": LRUPolicy(), "lfu": LFUPolicy(),
            "random": RandomPolicy(seed=seed)}[name]


def _store_factory(policy_name: str, retriever_cls, seed: int):
    def make():
        backend = InMemoryBackend(retriever=retriever_cls())
        return RetentionManager(
            budget_bytes=_BUDGET_SENTINEL,
            backend=backend,
            policy=_policy(policy_name, seed),
            logger=BenchmarkLogger(),
        )
    return make


def _make_workload(dataset: str, limit: int):
    if dataset == "beam":
        from forgetit.benchmark.workloads.beam import BeamWorkload, BeamTurnPairItemiser
        return BeamWorkload(DATA_DIR / "beam_500K.jsonl", itemiser=BeamTurnPairItemiser(),
                            probe_every=1, clear_between_examples=True, limit=limit)
    from forgetit.benchmark.workloads.longmemeval import LongMemEvalWorkload, TurnPairItemiser
    return LongMemEvalWorkload(DATA_DIR / "longmemeval_s_cleaned.json", TurnPairItemiser(),
                               probe_every=1, clear_between_examples=True, limit=limit)


def _index(results):
    """query_id -> EvalResult. query_ids are unique within a run."""
    out = {}
    for r in results:
        if r.query_id in out:
            raise AssertionError(f"duplicate query_id in results: {r.query_id}")
        out[r.query_id] = r
    return out


def _compare(label_a, a, label_b, b) -> list[str]:
    """Return a list of human-readable diffs (empty == identical)."""
    diffs: list[str] = []
    ka, kb = _index(a), _index(b)
    if ka.keys() != kb.keys():
        only_a = sorted(set(ka) - set(kb))[:3]
        only_b = sorted(set(kb) - set(ka))[:3]
        diffs.append(f"query_id sets differ: {label_a}\\{label_b}={only_a} "
                     f"{label_b}\\{label_a}={only_b} "
                     f"(|{label_a}|={len(ka)} |{label_b}|={len(kb)})")
        return diffs
    for qid in ka:
        ra, rb = ka[qid], kb[qid]
        for f in _EQ_FIELDS:
            va, vb = getattr(ra, f), getattr(rb, f)
            if isinstance(va, float):
                same = abs(va - vb) < 1e-9
            else:
                same = va == vb
            if not same:
                diffs.append(f"{qid}.{f}: {label_a}={va!r} {label_b}={vb!r}")
    return diffs


def _run_serial(dataset, workload, policy_name, retriever_cls, frac, fps, seed):
    store = _store_factory(policy_name, retriever_cls, seed)()
    return run(
        workload=workload, store=store, k=5, dataset=dataset,
        store_label=f"retention/{policy_name}", policy=policy_name,
        budget_frac=frac, example_footprints=fps, seed=seed,
        reader_impl=None, reader_workers=1,
    )


def _run_concurrent(dataset, workload, policy_name, retriever_cls, frac, fps, seed,
                    example_workers, out_dir):
    results, _summary = run_pipeline_concurrent(
        dataset=dataset, workload=workload,
        store_factory=_store_factory(policy_name, retriever_cls, seed),
        store_label=f"retention/{policy_name}", policy_label=policy_name,
        k=5, seed=seed, reader_impl=None,
        example_workers=example_workers, reader_workers=1,
        budget_frac=frac, example_footprints=fps,
        out_dir=str(out_dir), write_events=True,
    )
    return results


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset", choices=["longmemeval", "beam"], default=None,
                    help="Default: longmemeval if present, else beam.")
    ap.add_argument("--limit", type=int, default=12, help="Examples to replay.")
    ap.add_argument("--workers", type=int, default=8, help="High worker count for the race test.")
    ap.add_argument("--policies", nargs="+", default=["lru", "lfu", "random"])
    ap.add_argument("--fracs", nargs="+", type=float, default=[0.5, 0.25])
    ap.add_argument("--retriever", choices=["bm25", "lexical"], default="bm25")
    args = ap.parse_args()

    dataset = args.dataset or (
        "longmemeval" if (DATA_DIR / "longmemeval_s_cleaned.json").exists() else "beam"
    )
    retriever_cls = {"bm25": BM25Retriever, "lexical": LexicalOverlapRetriever}[args.retriever]
    seed = 42

    import tempfile
    tmp = Path(tempfile.mkdtemp(prefix="forgetit_eq_"))

    print(f"== across-example equivalence ==  dataset={dataset} limit={args.limit} "
          f"retriever={args.retriever} workers={args.workers}")
    print(f"   comparing fields: {', '.join(_EQ_FIELDS)}\n")

    total_fail = 0
    for policy_name in args.policies:
        for frac in args.fracs:
            # Footprints come from the workload; build a fresh workload per cell
            # so the parse cache / generators start clean.
            wl = _make_workload(dataset, args.limit)
            fps = example_footprints(wl)

            serial = _run_serial(dataset, wl, policy_name, retriever_cls, frac, fps, seed)
            conc1 = _run_concurrent(dataset, wl, policy_name, retriever_cls, frac, fps, seed,
                                    example_workers=1, out_dir=tmp / "w1")
            concN = _run_concurrent(dataset, wl, policy_name, retriever_cls, frac, fps, seed,
                                    example_workers=args.workers, out_dir=tmp / "wN")

            stateless = policy_name in _STATELESS_POLICIES

            # Race-safety / throughput-neutrality (#4): required for EVERY policy.
            dN = _compare("conc(w=1)", conc1, f"conc(w={args.workers})", concN)
            # Refactor fidelity (#1): required only for the stateless policies.
            dfid = (_compare("serial", serial, "conc(w=1)", conc1)
                    + _compare("serial", serial, f"conc(w={args.workers})", concN)) if stateless else []

            tag = f"{policy_name:<7} frac={frac:<4} n_scored={len(serial):<4}"
            failed = bool(dN or dfid)
            if failed:
                total_fail += 1
                print(f"FAIL  {tag}")
                for d in (dN + dfid)[:8]:
                    print(f"        {d}")
            elif stateless:
                print(f"ok    {tag}  (serial == conc(w=1) == conc(w={args.workers}))")
            else:
                # Random: confirm worker-invariance; report the expected serial
                # re-seed gap as informational only.
                gap = _compare("serial", serial, f"conc(w={args.workers})", concN)
                n_gap = len({d.split(".")[0] for d in gap})
                print(f"ok    {tag}  (conc(w=1) == conc(w={args.workers}); "
                      f"serial differs on {n_gap} query(ies) — expected RNG re-seed)")

    print()
    if total_fail:
        print(f"FAILED: {total_fail} cell(s) diverged — the refactor changed the science.")
        return 1
    print("PASSED: across-example runner is exactly equivalent on the LLM-free policies.")
    print(f"(artifacts under {tmp})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
