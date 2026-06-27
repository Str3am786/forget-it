#!/usr/bin/env python3
"""
Visual time-breakdown — where the pipeline spends its time, per config.

Reads the ``timings.json`` files the pipeline writes next to each config's
results (one per <dataset>/<itemiser>/<policy>/<retriever>/<budget>), and renders
a STACKED BAR of the coarse phases (embed_ingest → insert → evict_select →
evict_exec → retrieve → reader) so you can see, at a glance, what costs what.
Also prints a text table (seconds + % of work), and reports wall vs work to show
the concurrency speedup. The intent is self-reporting for agentic systems: a
reproducible "time profile" artifact per run.

Coarse phases partition the per-example critical path; the sub-phases
(compress_llm ⊂ evict_select, reembed ⊂ evict_exec, query_embed ⊂ retrieve) are
drill-downs printed under the table but not double-counted in the bar.

    python scripts/plot_timings.py artifacts/benchmark --out figs/time_breakdown.png
    python scripts/plot_timings.py artifacts/benchmark/.../timings.json   # single config
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

COARSE = ("embed_ingest", "insert", "evict_select", "evict_exec", "retrieve", "reader")
SUB = ("compress_llm", "reembed", "query_embed")
_COLORS = {
    "embed_ingest": "#9467bd", "insert": "#1f77b4", "evict_select": "#ff7f0e",
    "evict_exec": "#d62728", "retrieve": "#2ca02c", "reader": "#8c564b",
}


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


def _coarse_seconds(payload: dict) -> dict[str, float]:
    ph = payload.get("phases", {})
    return {p: float(ph.get(p, {}).get("total_s", 0.0)) for p in COARSE}


def _discover(path: Path) -> list[tuple[str, dict]]:
    files = [path] if path.suffix == ".json" else sorted(path.rglob("timings.json"))
    out = []
    for f in files:
        try:
            out.append((_label_from_path(f), json.loads(f.read_text())))
        except (OSError, ValueError):
            continue
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("path", type=Path, help="artifacts dir (recurses) or a single timings.json")
    ap.add_argument("--out", type=Path, default=Path("figs/time_breakdown.png"))
    ap.add_argument("--top", type=int, default=20, help="show the N slowest configs")
    args = ap.parse_args()

    runs = _discover(args.path)
    if not runs:
        print(f"no timings.json under {args.path}")
        return 1

    runs.sort(key=lambda kv: sum(_coarse_seconds(kv[1]).values()), reverse=True)
    runs = runs[:args.top]
    labels = [lbl for lbl, _ in runs]
    coarse = [_coarse_seconds(pl) for _, pl in runs]

    # ── text table ──
    print(f"{'config':42} " + "".join(f"{p[:11]:>12}" for p in COARSE) + f"{'work':>10}{'wall':>9}")
    for lbl, pl, cs in zip(labels, (pl for _, pl in runs), coarse):
        work = sum(cs.values())
        wall = pl.get("wall_s")
        row = "".join(f"{cs[p]:12.3f}" for p in COARSE)
        print(f"{lbl:42.42} {row}{work:10.3f}{(wall if wall else float('nan')):9.2f}")
        ph = pl.get("phases", {})
        subs = {s: ph.get(s, {}).get("total_s", 0.0) for s in SUB if ph.get(s, {}).get("total_s", 0.0) > 0}
        if subs:
            print("    └─ " + ", ".join(f"{s}={v:.3f}s" for s, v in subs.items()))

    # ── stacked bar ──
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception:
        print("\n(matplotlib unavailable — text table only)")
        return 0

    y = np.arange(len(labels))
    fig, ax = plt.subplots(figsize=(11, max(2.5, 0.5 * len(labels) + 1)))
    left = np.zeros(len(labels))
    for p in COARSE:
        vals = np.array([cs[p] for cs in coarse])
        if vals.sum() == 0:
            continue
        ax.barh(y, vals, left=left, color=_COLORS[p], label=p, height=0.6)
        left += vals
    ax.set_yticks(y); ax.set_yticklabels(labels, fontsize=7)
    ax.invert_yaxis(); ax.set_xlabel("work-seconds (summed over concurrent units)")
    ax.set_title("Pipeline time breakdown by phase")
    ax.legend(ncol=len(COARSE), fontsize=7, loc="lower right")
    ax.grid(axis="x", alpha=0.3)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout(); fig.savefig(args.out, dpi=130, bbox_inches="tight")
    print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
