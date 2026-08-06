"""Analyze the E3 confirmatory grid — one command over the grid's outputs.

    python scripts/analyze_e3_grid.py [--smoke]

Consumes E3_OUT_DIR (env; default <repo>/artifacts/e3_grid; --smoke appends
_smoke) as produced by run_e3_grid.py and emits, into that directory:

  e3_analysis.md / e3_analysis.json —
    (a) Δ(original − blind) gold-survival per budget for LRU and LFU with
        conversation-level cluster-bootstrap CIs (BCa; percentile fallback
        below 8 clusters, flagged), blind ≡ the fifo probes_blind run by the
        asserted class collapse;
    (b) the random anchor (5 seeds, per-conversation means) beside the blind
        value per budget;
    (c) the invariant attestation table (re-checked here, independently of
        the run-time assertions);
    (d) comparison against docs/prereg_amendment_e3_2026-07-08.md with a
        one-line verdict: CONFIRMED / PARTIALLY CONFIRMED / REFUTED.

Verdict logic (from the amendment's own disconfirmation triggers):
  confirmed  = every budget: blind ≤ random (point estimates, and no budget
               where blind exceeds random's 95% upper bound), window
               prediction exact per query, class collapse + monotonicity hold
  partially  = the direction holds everywhere but ≥1 secondary element fails
               (e.g. a window mismatch or a non-monotone series)
  refuted    = blind ABOVE random's 95% upper bound at any matched budget, or
               blind history traces diverge (collapse broken)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))

import e3_invariants as inv
from run_e3_grid import BUDGETS, RANDOM_SEEDS, config_dir, label_for

MIN_CLUSTERS_FOR_BCA = 8


def conversation_survival(out_dir: Path, label: str) -> pd.Series:
    df = pd.read_csv(config_dir(out_dir, label) / "results.csv")
    conv = df["query_id"].str.split("::").str[0]
    return df.assign(conv=conv).groupby("conv")["gold_id_retained"].mean()


def cluster_ci(values: np.ndarray, seed: int = 0) -> dict:
    """BCa cluster bootstrap of the mean (percentile fallback under 8 clusters)."""
    method = "BCa" if len(values) >= MIN_CLUSTERS_FOR_BCA else "percentile"
    if len(set(values.tolist())) == 1:      # degenerate: bootstrap undefined
        return {"mean": float(values.mean()), "lo": float(values.mean()),
                "hi": float(values.mean()), "method": "degenerate"}
    res = stats.bootstrap((values,), np.mean, n_resamples=10_000,
                          method=method, random_state=seed)
    return {"mean": float(values.mean()),
            "lo": float(res.confidence_interval.low),
            "hi": float(res.confidence_interval.high), "method": method}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--smoke", action="store_true")
    args = ap.parse_args()
    out_dir = Path(os.environ.get("E3_OUT_DIR", REPO / "artifacts" / "e3_grid"))
    if args.smoke:
        out_dir = out_dir.parent / (out_dir.name + "_smoke")
    manifest = json.loads((out_dir / "grid_manifest.json").read_text())
    smoke = bool(manifest.get("smoke"))

    analysis = {"smoke": smoke, "out_dir": str(out_dir), "budgets": {},
                "invariants": {}, "prereg": {}}

    # ── (a)+(b): deltas + anchors per budget ──────────────────────────
    for frac in BUDGETS:
        pct = f"{int(round(frac * 100)):03d}pct"
        blind = conversation_survival(out_dir, label_for("fifo", 0, frac, "probes_blind"))
        rnd = pd.concat([conversation_survival(out_dir, label_for("random", s, frac, "original"))
                         for s in RANDOM_SEEDS], axis=1).mean(axis=1)
        entry = {"blind_fifo": cluster_ci(blind.to_numpy()),
                 "random_anchor": cluster_ci(rnd.to_numpy())}
        for pol in ("lru", "lfu"):
            orig = conversation_survival(out_dir, label_for(pol, 0, frac, "original"))
            joined = pd.concat({"orig": orig, "blind": blind}, axis=1).dropna()
            deltas = (joined["orig"] - joined["blind"]).to_numpy()
            entry[f"delta_{pol}_original_minus_blind"] = cluster_ci(deltas)
        entry["blind_minus_random"] = round(
            entry["blind_fifo"]["mean"] - entry["random_anchor"]["mean"], 4)
        entry["blind_above_random_95"] = (
            entry["blind_fifo"]["mean"] > entry["random_anchor"]["hi"])
        analysis["budgets"][pct] = entry

    # ── (c): invariant attestation (recomputed, independent of run-time) ──
    fails = []
    for frac in BUDGETS:
        pct = f"{int(round(frac * 100)):03d}pct"
        fifo_blind = config_dir(out_dir, label_for("fifo", 0, frac, "probes_blind"))
        analysis["invariants"].setdefault("1_fifo_equivalence", {})[pct] = \
            inv.check_fifo_equivalence(fifo_blind)
        analysis["invariants"].setdefault("4_window_prediction", {})[pct] = \
            inv.check_window_prediction(fifo_blind)
    for name, sub in analysis["invariants"].items():
        fails += [f"{name}[{k}]" for k, v in sub.items() if not v["ok"]]
    runtime_ok = manifest.get("invariants_ok", False)

    # ── (d): prereg comparison → verdict ──────────────────────────────
    above = [pct for pct, e in analysis["budgets"].items()
             if e["blind_above_random_95"]]
    at_or_below = [pct for pct, e in analysis["budgets"].items()
                   if e["blind_minus_random"] <= 0.0]
    window_ok = all(v["ok"] for v in analysis["invariants"]["4_window_prediction"].values())
    collapse_ok = all(v["ok"] for v in analysis["invariants"]["1_fifo_equivalence"].values())
    mono = manifest["invariants"].get("5_monotonicity", {})
    mono_ok = all(v["ok"] for v in mono.values()) if mono else False

    if above or not collapse_ok:
        verdict = "REFUTED"
    elif (len(at_or_below) == len(analysis["budgets"]) and window_ok
          and mono_ok and runtime_ok):
        verdict = "CONFIRMED"
    else:
        verdict = "PARTIALLY CONFIRMED"
    analysis["prereg"] = {
        "prediction": "probes-blind LRU/LFU at-or-below random, approaching the "
                      "trailing-window value (docs/prereg_amendment_e3_2026-07-08.md)",
        "budgets_blind_at_or_below_random": at_or_below,
        "budgets_blind_above_random_95": above,
        "window_prediction_exact": window_ok,
        "class_collapse_ok": collapse_ok,
        "monotonicity_ok": mono_ok,
        "runtime_invariants_ok": runtime_ok,
        "verdict": verdict,
    }

    # ── emit ──────────────────────────────────────────────────────────
    (out_dir / "e3_analysis.json").write_text(json.dumps(analysis, indent=1))
    L = [f"# E3 grid analysis{' (SMOKE — plumbing only, excluded from all tables)' if smoke else ''}",
         "",
         f"**Verdict vs prereg amendment: {verdict}**", ""]
    L.append("| budget | blind(fifo) | random (5 seeds) | Δ lru(orig−blind) [CI] | Δ lfu(orig−blind) [CI] | blind>random95 |")
    L.append("|---|---|---|---|---|---|")
    for pct, e in analysis["budgets"].items():
        dl, df_ = e["delta_lru_original_minus_blind"], e["delta_lfu_original_minus_blind"]
        L.append(f"| {pct} | {e['blind_fifo']['mean']:.3f} | {e['random_anchor']['mean']:.3f} "
                 f"| {dl['mean']:+.3f} [{dl['lo']:+.3f},{dl['hi']:+.3f}] ({dl['method']}) "
                 f"| {df_['mean']:+.3f} [{df_['lo']:+.3f},{df_['hi']:+.3f}] ({df_['method']}) "
                 f"| {'YES' if e['blind_above_random_95'] else 'no'} |")
    L += ["", "## Invariant attestation", ""]
    L.append(f"- run-time invariants (grid manifest): "
             f"{'ALL PASS' if runtime_ok else 'FAILURES: ' + ', '.join(manifest.get('invariant_failures', []))}")
    L.append(f"- re-checked here: fifo-equivalence "
             f"{'PASS' if collapse_ok else 'FAIL'}; window prediction "
             f"{'PASS (exact per query)' if window_ok else 'FAIL'}; "
             f"monotonicity {'PASS' if mono_ok else 'FAIL'}")
    (out_dir / "e3_analysis.md").write_text("\n".join(L) + "\n")
    print("\n".join(L))
    print(f"\nwrote {out_dir}/e3_analysis.md + .json")
    return 0 if (runtime_ok and not fails) else 1


if __name__ == "__main__":
    sys.exit(main())
