"""E3 confirmatory grid — PREPARED COMMAND (the author runs the real thing).

Retention-only (NO reader, NO LLM, NO endpoints): the outcome is gold
survival. Everything configurable via env vars; nothing hardcoded.

    BEAM_DATA    path to beam jsonl        (default: <repo>/data/beam_500K.jsonl)
    E3_OUT_DIR   output root               (default: <repo>/artifacts/e3_grid)
    E3_WORKERS   conversations in parallel (default: 4)

Grid (constants below; seeds pinned):
    35 conversations x budgets {10,25,50,75,100}% x turn_pair x bm25
    original     : lru, lfu, fifo, keep_smallest, random x 5 seeds
    probes_blind : fifo (THE single blind history simulation — blind LRU and
                   blind LFU are the same policy by the class collapse and are
                   ASSERTED identical via independent replay, never re-run as
                   separate confirmations), keep_smallest (the non-history
                   touch-invariance spot-check: its two conditions must be
                   byte-identical)

The five E3 invariants (scripts/e3_invariants.py) run WITH the grid and fail
it loudly (nonzero exit + manifest verdicts). Then run:

    python scripts/analyze_e3_grid.py            # consumes E3_OUT_DIR

Dry-run (plumbing only, 2 conversations, output stamped smoke=true and
written to <E3_OUT_DIR>_smoke):

    python scripts/run_e3_grid.py --smoke
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))

from forgetit.backend.in_memory import InMemoryBackend
from forgetit.benchmark.budget import example_footprints
from forgetit.benchmark.pipeline import run_pipeline_concurrent
from forgetit.benchmark.workloads.beam import BeamWorkload, BeamTurnPairItemiser
from forgetit.core.logging import BenchmarkLogger
from forgetit.memory_system import RetentionManager
from forgetit.policies import (
    FIFOPolicy, KeepSmallestPolicy, LFUPolicy, LRUPolicy, RandomPolicy,
)
from forgetit.retrieval import BM25Retriever

import e3_invariants as inv

BUDGETS = [0.10, 0.25, 0.50, 0.75, 1.00]
RANDOM_SEEDS = [11, 12, 13, 14, 15]
K = 5
ITEMISER = BeamTurnPairItemiser()

POLICY_FACTORIES = {
    "lru": lambda seed: LRUPolicy(),
    "lfu": lambda seed: LFUPolicy(),
    "fifo": lambda seed: FIFOPolicy(),
    "keep_smallest": lambda seed: KeepSmallestPolicy(),
    "random": lambda seed: RandomPolicy(seed=seed),
}


def configs():
    """(policy, seed, touch_mode) triples of the grid."""
    out = []
    for pol in ("lru", "lfu", "fifo", "keep_smallest"):
        out.append((pol, 0, "original"))
    for s in RANDOM_SEEDS:
        out.append(("random", s, "original"))
    for pol in ("fifo", "keep_smallest"):
        out.append((pol, 0, "probes_blind"))
    return out


def label_for(policy: str, seed: int, frac: float, mode: str) -> str:
    pct = f"{int(round(frac * 100)):03d}pct"
    stem = f"retention/{policy}" + (f"_seed{seed}" if policy == "random" else "")
    label = f"{stem}/bm25/budget_{pct}"
    if mode != "original":
        label += f"/touch_{mode}"
    return label


def config_dir(out_dir: Path, label: str) -> Path:
    return out_dir / "beam" / ITEMISER.name / label / "none"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--smoke", action="store_true",
                    help="2-conversation plumbing dry-run; output stamped "
                         "smoke=true under <E3_OUT_DIR>_smoke; excluded from "
                         "all tables and budgets")
    ap.add_argument("--limit", type=int, default=None,
                    help="conversation cap (default: 35, or 2 with --smoke)")
    ap.add_argument("--workers", type=int,
                    default=int(os.environ.get("E3_WORKERS", "4")))
    ap.add_argument("--invariants-only", action="store_true",
                    help="skip the grid replay; re-run the five invariants over "
                         "an EXISTING out dir (uses its grid_manifest.json for "
                         "the config list) and rewrite the manifest verdicts")
    args = ap.parse_args()

    beam_path = Path(os.environ.get("BEAM_DATA", REPO / "data" / "beam_500K.jsonl"))
    out_dir = Path(os.environ.get("E3_OUT_DIR", REPO / "artifacts" / "e3_grid"))
    if args.smoke:
        out_dir = out_dir.parent / (out_dir.name + "_smoke")
    limit = args.limit if args.limit is not None else (2 if args.smoke else 35)
    if not beam_path.exists():
        sys.exit(f"BEAM_DATA not found: {beam_path}")

    if args.invariants_only:
        manifest = json.loads((out_dir / "grid_manifest.json").read_text())
        manifest["invariants"] = {}
        return run_invariants(out_dir, manifest)

    workload = BeamWorkload(beam_path, itemiser=ITEMISER, probe_every=1,
                            clear_between_examples=True, limit=limit)
    footprints = example_footprints(workload)
    print(f"grid: {len(configs())} policy/mode cells x {len(BUDGETS)} budgets, "
          f"{len(footprints)} conversations, out={out_dir}, smoke={args.smoke}")

    manifest = {"smoke": bool(args.smoke), "limit": limit,
                "budgets": BUDGETS, "random_seeds": RANDOM_SEEDS, "k": K,
                "itemiser": ITEMISER.name, "beam_data": str(beam_path),
                "configs": [], "invariants": {}}

    for policy, seed, mode in configs():
        for frac in BUDGETS:
            label = label_for(policy, seed, frac, mode)

            def factory(pol=policy, sd=seed, m=mode):
                return RetentionManager(
                    budget_bytes=1 << 62,
                    backend=InMemoryBackend(retriever=BM25Retriever()),
                    policy=POLICY_FACTORIES[pol](sd),
                    logger=BenchmarkLogger(),
                    probe_touch_mode=m)

            _, summary = run_pipeline_concurrent(
                dataset="beam", workload=workload, store_factory=factory,
                store_label=label, k=K, seed=seed, itemiser=ITEMISER.name,
                reader="none", reader_impl=None,
                example_workers=args.workers,
                budget_frac=frac, example_footprints=footprints,
                policy_label=policy, retriever_label="bm25",
                probe_touch_mode=mode, out_dir=str(out_dir),
                write_events=True)
            manifest["configs"].append({
                "label": label, "policy": policy, "seed": seed, "mode": mode,
                "budget_frac": frac,
                "gold_survival": round(summary["gold_survival_rate"], 4)})
            print(f"  {label:60s} survival={summary['gold_survival_rate']:.3f}")

    return run_invariants(out_dir, manifest)


def run_invariants(out_dir: Path, manifest: dict) -> int:
    # ── the five invariants run WITH the grid ─────────────────────────
    failures = []

    def record(name: str, key: str, result: dict):
        manifest["invariants"].setdefault(name, {})[key] = result
        if not result["ok"]:
            failures.append(f"{name}[{key}]")

    for frac in BUDGETS:
        pct = f"{int(round(frac * 100)):03d}pct"
        fifo_blind = config_dir(out_dir, label_for("fifo", 0, frac, "probes_blind"))
        record("1_fifo_equivalence", pct, inv.check_fifo_equivalence(fifo_blind))
        for pol in ("lru", "lfu"):
            orig = config_dir(out_dir, label_for(pol, 0, frac, "original"))
            record("2_pressure_invariant", f"{pol}@{pct}",
                   inv.check_pressure_invariant(orig, fifo_blind, exact_pair=False))
        for pol in ("fifo", "keep_smallest"):
            orig = config_dir(out_dir, label_for(pol, 0, frac, "original"))
            blind = config_dir(out_dir, label_for(pol, 0, frac, "probes_blind"))
            record("2_pressure_invariant", f"{pol}_pair@{pct}",
                   inv.check_pressure_invariant(orig, blind, exact_pair=True))
        record("4_window_prediction", pct, inv.check_window_prediction(fifo_blind))

    for cfg in manifest["configs"]:
        d = config_dir(out_dir, cfg["label"])
        record("3_independent_recount", cfg["label"], inv.check_independent_recount(d))

    surv = {}
    for cfg in manifest["configs"]:
        key = (cfg["policy"], cfg["seed"], cfg["mode"])
        surv.setdefault(key, {})[cfg["budget_frac"]] = cfg["gold_survival"]
    for (pol, seed, mode), series in surv.items():
        record("5_monotonicity", f"{pol}_seed{seed}_{mode}",
               inv.check_monotonicity(series))

    manifest["invariants_ok"] = not failures
    manifest["invariant_failures"] = failures
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "grid_manifest.json").write_text(json.dumps(manifest, indent=1))
    print(f"\ninvariants: {'ALL PASS' if not failures else 'FAILED: ' + ', '.join(failures)}")
    print(f"manifest → {out_dir}/grid_manifest.json")
    print("next: python scripts/analyze_e3_grid.py"
          + (" --smoke" if manifest.get("smoke") else ""))
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
