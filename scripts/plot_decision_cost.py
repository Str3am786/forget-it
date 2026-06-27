#!/usr/bin/env python3
"""Decision cost vs store occupancy — read a policy's complexity class off the slope.

The run-level timer gives a MEAN ("evict_select ≈ 78µs/decision"). A mean can't
tell you whether victim selection is O(1) (flat as the store fills) or whether
something does an O(n) scan that degrades once the store is full and the
candidate set is large. That distinction is exactly what separates the policy
classes — a recency heuristic is constant-time; a semantic / submodular-coverage
policy does work that grows with the resident set.

So this reads the per-DECISION trace that ``RetentionManager._ensure_budget``
emits into ``events.jsonl`` (``name == "decision"``): each row carries the
decision's own ``dur_select_s`` AND the store ``n_resident`` at that moment. We
bin by occupancy, take the MEDIAN dur per bin (robust to the GIL/scheduling tail
that fattens individual samples), fit a line, and report:

  * median / p95 decision cost (µs),
  * slope (µs per +1 resident item) from a least-squares fit on the binned medians,
  * growth = median(top occupancy decile) / median(bottom decile),
  * a coarse verdict: ~constant vs grows-with-occupancy.

For a clean INTRINSIC reading, generate the trace at ``--workers 1`` — under
concurrency the GIL inflates the CPU-bound select phase (a thread can be
descheduled mid-decision). The median-of-bins is resistant but w=1 is honest.

    python scripts/plot_decision_cost.py artifacts/benchmark --out figs/decision_cost.png
    python scripts/plot_decision_cost.py artifacts/.../events.jsonl   # single config
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

_PHASE = "dur_select_s"   # the decision-cost field to profile (select_action)


def _label_from_path(p: Path) -> str:
    """Compact config label from the artifact path (.../retention/<pol>/<retr>/budget_..)."""
    parts = p.parts
    if "retention" in parts:
        i = parts.index("retention")
        try:
            return "/".join(parts[i + 1:i + 4])   # policy/retriever/budget
        except IndexError:
            pass
    return p.parent.name


def _load_decisions(path: Path) -> dict[str, tuple[np.ndarray, np.ndarray]]:
    """Map config-label → (occupancy[], dur_µs[]) from every events.jsonl found."""
    files = [path] if path.suffix == ".jsonl" else sorted(path.rglob("events.jsonl"))
    out: dict[str, tuple[list, list]] = {}
    for f in files:
        occ, dur = [], []
        try:
            with f.open(encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    row = json.loads(line)
                    if row.get("name") != "decision" or _PHASE not in row:
                        continue
                    occ.append(int(row.get("n_resident", 0)))
                    dur.append(float(row[_PHASE]) * 1e6)   # → microseconds
        except (OSError, ValueError):
            continue
        if dur:
            out[_label_from_path(f)] = (occ, dur)
    return {lbl: (np.asarray(o), np.asarray(d)) for lbl, (o, d) in out.items()}


def _binned_medians(occ: np.ndarray, dur: np.ndarray, nbins: int = 20):
    """Median dur per occupancy bin → (bin_centre, median_dur). Robust to the tail."""
    lo, hi = occ.min(), occ.max()
    if hi <= lo:
        return np.array([lo]), np.array([np.median(dur)])
    edges = np.linspace(lo, hi, nbins + 1)
    idx = np.clip(np.digitize(occ, edges) - 1, 0, nbins - 1)
    centres, meds = [], []
    for b in range(nbins):
        sel = idx == b
        if sel.any():
            centres.append(0.5 * (edges[b] + edges[b + 1]))
            meds.append(float(np.median(dur[sel])))
    return np.asarray(centres), np.asarray(meds)


def _profile(occ: np.ndarray, dur: np.ndarray) -> dict:
    centres, meds = _binned_medians(occ, dur)
    slope = float(np.polyfit(centres, meds, 1)[0]) if len(centres) >= 2 else 0.0
    # growth = top-decile vs bottom-decile median (occupancy-sorted)
    order = np.argsort(occ)
    d = dur[order]
    n = len(d)
    tenth = max(1, n // 10)
    lo_med = float(np.median(d[:tenth])) or 1e-9
    hi_med = float(np.median(d[-tenth:]))
    growth = hi_med / lo_med
    verdict = "~constant" if growth < 1.5 else ("grows" if growth < 4 else "GROWS sharply")
    return {
        "n": n, "median": float(np.median(dur)), "p95": float(np.percentile(dur, 95)),
        "slope": slope, "growth": growth, "verdict": verdict,
        "centres": centres, "meds": meds,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("path", type=Path, help="artifacts dir (recurses) or a single events.jsonl")
    ap.add_argument("--out", type=Path, default=Path("figs/decision_cost.png"))
    ap.add_argument("--top", type=int, default=12, help="plot the N configs with most decisions")
    args = ap.parse_args()

    data = _load_decisions(args.path)
    if not data:
        print(f"no 'decision' events under {args.path} "
              "(run with --write-events; trace is emitted by _ensure_budget)")
        return 1

    profs = {lbl: _profile(o, d) for lbl, (o, d) in data.items()}
    ordered = sorted(profs.items(), key=lambda kv: kv[1]["n"], reverse=True)[:args.top]

    # ── text table ──
    print(f"{'config':40} {'n':>7} {'med_µs':>8} {'p95_µs':>8} "
          f"{'µs/item':>9} {'growth':>7}  verdict")
    for lbl, p in ordered:
        print(f"{lbl:40.40} {p['n']:7d} {p['median']:8.1f} {p['p95']:8.1f} "
              f"{p['slope']:9.4f} {p['growth']:6.2f}x  {p['verdict']}")
    print("\ngrowth = median decision cost in the top occupancy decile ÷ bottom decile.")
    print("µs/item = slope of binned-median cost vs resident-count (the complexity read).")

    # ── plot: binned-median cost vs occupancy, one line per config ──
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception:
        print("\n(matplotlib unavailable — text table only)")
        return 0

    fig, ax = plt.subplots(figsize=(9, 5.5))
    for lbl, p in ordered:
        if len(p["centres"]) >= 2:
            ax.plot(p["centres"], p["meds"], marker="o", ms=3, lw=1.3,
                    label=f"{lbl}  ({p['verdict']})")
    ax.set_xlabel("store occupancy  (n_resident at decision time)")
    ax.set_ylabel("decision cost  (median µs per select_action, per bin)")
    ax.set_title("Eviction decision cost vs store occupancy — complexity by slope")
    ax.grid(alpha=0.3)
    ax.legend(fontsize=7, loc="upper left")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout(); fig.savefig(args.out, dpi=130, bbox_inches="tight")
    print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
