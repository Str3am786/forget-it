#!/usr/bin/env python3
"""
Render Family-A workload characterization into figures + a markdown report.
Reuses the trace-only engine in characterize_workload.py (no policy/reader/LLM).

    python scripts/characterize_report.py [--limit N]
"""
from __future__ import annotations

import argparse
import math
from collections import Counter
from pathlib import Path

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from characterize_workload import builders, analyze_example

FIGS = Path("figs"); FIGS.mkdir(exist_ok=True)
ART = Path("artifacts"); ART.mkdir(exist_ok=True)


def collect(name, examples):
    per_ex = [analyze_example(evs) for _e, evs in examples]
    per_ex = [s for s in per_ex if s is not None]
    refcounts, cn_frac, cn_items, ird_items = [], [], [], []
    items_per_ex, bytes_per_ex = [], []
    for s in per_ex:
        refcounts.extend(s.ref_counts.values())
        items_per_ex.append(s.n_items)
        bytes_per_ex.append(s.footprint_bytes)
        for g in s.gold_accesses:
            if math.isfinite(g.frac):
                if g.repeated:
                    if math.isfinite(g.sd_items):
                        ird_items.append(g.sd_items)
                else:
                    cn_frac.append(g.frac)
                    if math.isfinite(g.sd_items):
                        cn_items.append(g.sd_items)
    return {
        "name": name,
        "n_ex": len(per_ex),
        "refcounts": np.array(refcounts, int),
        "cn_frac": np.array(cn_frac, float),
        "cn_items": np.array(cn_items, float),
        "ird_items": np.array(ird_items, float),
        "items_per_ex": np.array(items_per_ex, float),
        "bytes_per_ex": np.array(bytes_per_ex, float),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    data = [collect(name, wl.iter_examples()) for name, wl in builders(args.limit)]
    grid = np.linspace(0.0, 1.0, 101)
    colors = {"longmemeval/turn_pair": "#1f77b4", "longmemeval/session": "#7fb3e0",
              "beam/turn_pair": "#d62728", "beam/batch": "#f0a0a0"}

    # ── Fig 1: MRC / survival over budget_frac ──
    plt.figure(figsize=(7, 5))
    for d in data:
        f = d["cn_frac"]
        if f.size == 0:
            continue
        surv = [np.mean(f < g) for g in grid]
        plt.plot(grid, surv, label=f"{d['name']} (n={f.size})",
                 color=colors.get(d["name"]), lw=2)
    plt.xlabel("budget_frac  (byte budget / example footprint)")
    plt.ylabel("gold survival  P(creation→need byte-distance < budget)")
    plt.title("MRC — oracle-LRU gold survival vs relative byte budget")
    plt.grid(alpha=0.3); plt.legend(fontsize=8); plt.ylim(0, 1)
    plt.tight_layout(); plt.savefig(FIGS / "A3_mrc_budget_frac.png", dpi=130); plt.close()

    # ── Fig 2: creation→need distance CDF (frac of footprint) ──
    plt.figure(figsize=(7, 5))
    for d in data:
        f = np.sort(d["cn_frac"])
        if f.size == 0:
            continue
        cdf = np.arange(1, f.size + 1) / f.size
        plt.plot(f, cdf, label=d["name"], color=colors.get(d["name"]), lw=2)
    plt.xlabel("creation→need distance  (fraction of example footprint, bytes)")
    plt.ylabel("CDF")
    plt.title("Where gold lives relative to the query (single-reference items)")
    plt.grid(alpha=0.3); plt.legend(fontsize=8)
    plt.tight_layout(); plt.savefig(FIGS / "A1_creation_to_need_cdf.png", dpi=130); plt.close()

    # ── Fig 3: reference-count distribution (the gate) ──
    fig, axes = plt.subplots(1, len(data), figsize=(3.2 * len(data), 3.4), squeeze=False)
    for ax, d in zip(axes[0], data):
        rc = Counter(d["refcounts"].tolist())
        ks = sorted(rc)
        ax.bar([str(k) for k in ks], [rc[k] for k in ks],
               color=colors.get(d["name"]))
        n = sum(rc.values()); single = rc.get(1, 0)
        pct = 100 * single / n if n else 0
        ax.set_title(f"{d['name']}\n{pct:.0f}% single-ref", fontsize=8)
        ax.set_xlabel("#queries referencing item"); ax.set_yscale("log")
    fig.suptitle("A0 — reference-count distribution (gate)", y=1.02)
    plt.tight_layout(); plt.savefig(FIGS / "A0_reference_counts.png", dpi=130,
                                    bbox_inches="tight"); plt.close()

    # ── markdown report ──
    def q(a, p): return float(np.percentile(a, p)) if a.size else float("nan")
    lines = ["# Family A — Workload Characterization (trace-only)\n",
             "_No policy, retriever, reader, or LLM. Deterministic function of "
             "(dataset, itemiser). Probes stripped (`probe_every=0`)._\n",
             "\n## Summary table\n",
             "| workload | examples | items/ex (p50) | footprint KB (p50) | "
             "scored-q total | % single-ref | gold depth frac (p50) | "
             "survival@0.25 | survival@0.50 |",
             "|---|--:|--:|--:|--:|--:|--:|--:|--:|"]
    for d in data:
        rc = Counter(d["refcounts"].tolist()); n = sum(rc.values())
        pct_single = 100 * rc.get(1, 0) / n if n else float("nan")
        f = d["cn_frac"]
        s25 = np.mean(f < 0.25) if f.size else float("nan")
        s50 = np.mean(f < 0.50) if f.size else float("nan")
        lines.append(
            f"| {d['name']} | {d['n_ex']} | {q(d['items_per_ex'],50):.0f} | "
            f"{q(d['bytes_per_ex'],50)/1024:.0f} | {int(d['refcounts'].sum() and 0) or ''}"
            f"{f.size} | {pct_single:.1f}% | {q(f,50):.2f} | {s25:.2f} | {s50:.2f} |")
    (ART / "workload_characterization_report.md").write_text("\n".join(lines) + "\n")
    print("wrote figs/A0_reference_counts.png, A1_creation_to_need_cdf.png, "
          "A3_mrc_budget_frac.png")
    print("wrote artifacts/workload_characterization_report.md")
    for ln in lines[4:]:
        print(ln)


if __name__ == "__main__":
    main()
