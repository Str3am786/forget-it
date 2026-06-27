#!/usr/bin/env python3
"""
Family A — the decision-tree analysis (TRACE ONLY: sizes + gold labels + insert
order; no policy/retriever/reader/LLM/embeddings).

Per scored query, compute the MINIMUM budget (as a fraction of the example
footprint) at which all of its gold survives, under three policies:

  oracle        : keep exactly the gold       -> B* = sum(gold bytes)
  keep_smallest : admit items ascending by size until full (content-agnostic,
                  non-temporal)                -> B* = max_g cumsize(size <= size(g))
  recency_lru   : keep the most-recent bytes   -> B* = max_g (size(g) + bytes
                  inserted after g)            [LRU snapshot after all inserts]

Survival_policy(f) = P_over_queries( B*_policy <= f * footprint ).

The GAP oracle - recency_lru is the headroom a content-based value estimator
could capture. keep_smallest shows how much of that headroom a trivial size
heuristic already gets; the gold/noise size histogram explains why.

    python scripts/oracle_size_analysis.py [--limit N]
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from characterize_workload import builders, byte_size

FIGS = Path("figs"); FIGS.mkdir(exist_ok=True)
ART = Path("artifacts"); ART.mkdir(exist_ok=True)


def analyze_example(events):
    """Return (per_query_crit, gold_sizes, noise_sizes) for one example.
    per_query_crit: list of (oracle_frac, ks_frac, rec_frac).  All inserts in
    these workloads precede all scored queries, so the candidate set at query
    time is the full footprint — the closed forms below are exact."""
    insert_ids, size_of = [], {}
    for ev in events:
        if ev.type == "insert" and ev.item is not None and ev.item.id not in size_of:
            insert_ids.append(ev.item.id)
            size_of[ev.item.id] = byte_size(ev.item.text or "")
    if not insert_ids:
        return [], [], []
    footprint = sum(size_of.values())
    if footprint == 0:
        return [], [], []

    # bytes inserted strictly AFTER each item (insert order) -> LRU recency cost
    bytes_after = {}
    run = 0
    for iid in reversed(insert_ids):
        bytes_after[iid] = run
        run += size_of[iid]

    # cumulative size of all items with size <= s  -> keep-smallest cost
    sizes_sorted = np.sort(np.array([size_of[i] for i in insert_ids], float))
    csum = np.cumsum(sizes_sorted)

    def ks_cost(s):
        # sum of sizes of items with size <= s (ties included)
        idx = np.searchsorted(sizes_sorted, s, side="right")
        return float(csum[idx - 1]) if idx > 0 else 0.0

    # gather each scored query's gold, then the store-wide union
    query_golds = []
    for ev in events:
        if ev.type == "query" and ev.query is not None and ev.gold_item_ids:
            gold = [g for g in ev.gold_item_ids if g in size_of]
            if gold:
                query_golds.append(gold)
    union_gold = set().union(*query_golds) if query_golds else set()
    # union oracle: a query-agnostic (write-time) policy must keep the WHOLE
    # union under one budget -> single per-store critical budget, shared by
    # every query in the store.  On 1-query stores this == the query oracle.
    b_union_frac = sum(size_of[g] for g in union_gold) / footprint

    per_query = []
    for gold in query_golds:
        b_oracle = sum(size_of[g] for g in gold)               # query-conditioned
        b_rec = max(size_of[g] + bytes_after[g] for g in gold)
        b_ks = max(ks_cost(size_of[g]) for g in gold)
        per_query.append((b_oracle / footprint, b_ks / footprint,
                          b_rec / footprint, b_union_frac))

    gold_sizes = [size_of[g] for g in union_gold]
    noise_sizes = [size_of[i] for i in insert_ids if i not in union_gold]
    return per_query, gold_sizes, noise_sizes


def collect(name, examples):
    crit, gold_sz, noise_sz = [], [], []
    for _e, evs in examples:
        c, g, n = analyze_example(evs)
        crit.extend(c); gold_sz.extend(g); noise_sz.extend(n)
    crit = np.array(crit, float)  # (Q, 3): oracle, ks, rec
    return {"name": name, "crit": crit,
            "gold_sz": np.array(gold_sz, float),
            "noise_sz": np.array(noise_sz, float)}


def survival(crit_col, grid):
    return np.array([np.mean(crit_col <= f) for f in grid]) if crit_col.size else np.zeros_like(grid)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    data = [collect(name, wl.iter_examples()) for name, wl in builders(args.limit)]
    grid = np.linspace(0.0, 1.0, 201)

    # ── Fig: oracle vs keep-smallest vs recency, one panel per workload ──
    fig, axes = plt.subplots(1, len(data), figsize=(4.2 * len(data), 4.0), squeeze=False)
    pol = [("oracle_query_cond", 0, "#2ca02c", "-"),
           ("oracle_union_writetime", 3, "#9467bd", "-"),
           ("keep_smallest", 1, "#ff7f0e", "--"),
           ("recency_lru", 2, "#1f77b4", "-.")]
    report = {}
    for ax, d in zip(axes[0], data):
        c = d["crit"]
        ax.plot(grid, grid, color="gray", lw=1, alpha=0.5, label="diagonal (random)")
        rep = {}
        for label, col, color, ls in pol:
            s = survival(c[:, col] if c.size else np.array([]), grid)
            ax.plot(grid, s, color=color, ls=ls, lw=2, label=label)
            rep[label] = {f"{f:.2f}": round(float(np.mean((c[:, col] <= f))), 3)
                          for f in (0.05, 0.10, 0.25, 0.50)} if c.size else {}
        ax.set_title(f"{d['name']}  (Q={c.shape[0] if c.size else 0})", fontsize=9)
        ax.set_xlabel("budget_frac"); ax.set_ylim(0, 1.02); ax.grid(alpha=0.3)
        ax.legend(fontsize=7)
        report[d["name"]] = rep
    axes[0][0].set_ylabel("gold survival (all gold for query resident)")
    fig.suptitle("Oracle vs keep-smallest vs recency — survival headroom", y=1.03)
    plt.tight_layout(); plt.savefig(FIGS / "A3_oracle_keepsmallest_recency.png",
                                    dpi=130, bbox_inches="tight"); plt.close()

    # ── Fig: gold vs noise size CDFs ──
    fig, axes = plt.subplots(1, len(data), figsize=(4.2 * len(data), 3.6), squeeze=False)
    for ax, d in zip(axes[0], data):
        for arr, color, lab in ((d["gold_sz"], "#2ca02c", "gold"),
                                (d["noise_sz"], "#888888", "noise")):
            if arr.size:
                x = np.sort(arr); y = np.arange(1, x.size + 1) / x.size
                ax.plot(x, y, color=color, lw=2, label=f"{lab} (n={arr.size})")
        ax.set_xscale("log"); ax.set_xlabel("item size (bytes)")
        ax.set_title(d["name"], fontsize=9); ax.grid(alpha=0.3); ax.legend(fontsize=7)
    axes[0][0].set_ylabel("CDF")
    fig.suptitle("Gold vs noise item-size distribution (size-signal for keep-smallest)", y=1.03)
    plt.tight_layout(); plt.savefig(FIGS / "A3_gold_noise_size_cdf.png",
                                    dpi=130, bbox_inches="tight"); plt.close()

    # ── numeric report ──
    print(f"{'workload':24} {'budget':>7} | {'orc_qc':>7} {'orc_uni':>7} {'keepsm':>7} "
          f"{'recency':>7} {'headroom':>9}")
    lines = ["# Family A — oracle / keep-smallest / recency survival (trace-only)\n",
             "Survival = P(all gold for a query fits) at budget = frac of footprint.\n",
             "- `oracle_qc` = query-conditioned ceiling (keep this query's gold).\n",
             "- `oracle_union` = query-AGNOSTIC write-time ceiling (keep the whole "
             "store's gold union under one budget). On 1-query stores it equals "
             "`oracle_qc`; the qc−union gap is the price of not knowing the question "
             "at write time.\n",
             "- `headroom` = oracle_union − recency_lru (what a deployable write-time "
             "scorer is actually allowed to chase).\n",
             "- P(gold<noise) = Mann–Whitney U effect size (prob. a random gold item "
             "is smaller than a random noise item).\n",
             "| workload | budget | oracle_qc | oracle_union | keep_smallest | "
             "recency_lru | headroom | gold med B | noise med B | MWU P(gold<noise) |",
             "|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|"]
    for d in data:
        c = d["crit"]
        gmed = float(np.median(d["gold_sz"])) if d["gold_sz"].size else float("nan")
        nmed = float(np.median(d["noise_sz"])) if d["noise_sz"].size else float("nan")
        # P(random gold strictly smaller than random noise) — keep-smallest
        # favourability.  >0.5 => gold tends to be smaller (size helps); ~0.5
        # => no signal; <0.5 => gold is larger (keep-smallest hurts).
        if d["gold_sz"].size and d["noise_sz"].size:
            ns = np.sort(d["noise_sz"])
            ge = np.searchsorted(ns, d["gold_sz"], side="right")  # noise <= gold
            frac_noise_gt = 1.0 - ge / ns.size                    # noise > gold
            auc = float(np.mean(frac_noise_gt))
        else:
            auc = float("nan")
        for f in (0.05, 0.10, 0.25, 0.50):
            o = float(np.mean(c[:, 0] <= f)) if c.size else float("nan")
            u = float(np.mean(c[:, 3] <= f)) if c.size else float("nan")
            k = float(np.mean(c[:, 1] <= f)) if c.size else float("nan")
            r = float(np.mean(c[:, 2] <= f)) if c.size else float("nan")
            print(f"{d['name']:24} {f:7.2f} | {o:7.3f} {u:7.3f} {k:7.3f} {r:7.3f} "
                  f"{u-r:9.3f}")
            lines.append(f"| {d['name']} | {f:.2f} | {o:.3f} | {u:.3f} | {k:.3f} | "
                         f"{r:.3f} | {u-r:.3f} | {gmed:.0f} | {nmed:.0f} | {auc:.2f} |")
        print()
    (ART / "oracle_size_report.md").write_text("\n".join(lines) + "\n")
    (ART / "oracle_size_report.json").write_text(json.dumps(report, indent=2))
    print("wrote figs/A3_oracle_keepsmallest_recency.png, figs/A3_gold_noise_size_cdf.png")
    print("wrote artifacts/oracle_size_report.md, .json")


if __name__ == "__main__":
    main()
