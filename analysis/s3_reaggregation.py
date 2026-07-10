"""S3 — recompute the laundered aggregates. RE-AGGREGATION ONLY:
pandas/matplotlib over RECORDED artifacts. This script never constructs a
store, a retriever, or a model client; if it did, S3 was misread.

Inputs (all read-only, produced by past runs):
  analysis/tables/fair_aucb_ranking.csv          per-cell AUC-B (9 cells x 8 policies)
  artifacts/10-05-results/combined_results.csv   the figure-03 source sweep
  artifacts/benchmark_full/beam/beam_turn_pair/retention/<policy>/bm25/budget_025pct/
                                                 knee cells for the percentile companions

Outputs:
  report2/s3_corrected_aggregates.md             the retired-vs-corrected table
  report2/figs_s3/03_policy_comparison_corrected.png

Historical logs carry depth-10 censoring, so percentile columns are reported
as [pessimistic, optimistic] BOUNDS exactly per the sprint-1 D2 convention.

Rerun: python analysis/s3_reaggregation.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))
sys.path.insert(0, str(REPO / "analysis"))
sys.path.insert(0, str(REPO / "src"))

from forgetit.benchmark.metrics import is_history_policy, policy_unit, NAME_LEVEL_PREFIX

import figures_forg as ff
from k_confound_reanalysis import load_cell, add_metrics, K_FLOOR, _md

OUT = REPO / "report2" / "s3_corrected_aggregates.md"
FIGS = REPO / "report2" / "figs_s3"
KNEE_POLICIES = ["compression", "lru", "lfu", "keep_smallest",
                 "semantic_coverage", "random"]
REACH_PAIRS = [("compression", "lru"), ("compression", "lfu"), ("lru", "lfu"),
               ("compression", "keep_smallest"), ("lru", "keep_smallest"),
               ("keep_smallest", "semantic_coverage"), ("keep_smallest", "random")]


# ── A. robustness mean-rank list (the laundered ranking) ─────────────

def robustness_correction() -> tuple[pd.DataFrame, pd.DataFrame]:
    fa = pd.read_csv(REPO / "analysis" / "tables" / "fair_aucb_ranking.csv")
    fa["rank"] = fa.groupby(["itemiser", "retriever"])["aucb_ans"].rank(ascending=False)
    # OLD (retired): bare-name mean across all 9 itemiser x retriever cells —
    # for lru/lfu this pools three different (policy, retriever) identities.
    old = (fa.groupby("policy")["rank"].mean().sort_values()
             .rename("old_mean_rank_9cells").reset_index())
    # CORRECTED: history policies keyed (policy, retriever) — mean of their 3
    # itemiser cells within one retriever; blind/content policies keep their
    # bare name (their identity does not include the touch source) and are
    # explicitly name-level rows of a labeled view.
    fa["unit"] = [policy_unit(p, r) if is_history_policy(p) else p
                  for p, r in zip(fa["policy"], fa["retriever"])]
    corrected = (fa.groupby("unit")
                   .agg(mean_rank=("rank", "mean"), n_cells=("rank", "size"))
                   .sort_values("mean_rank").reset_index())
    corrected["scope"] = ["(policy, retriever) — 3 itemiser cells"
                          if "@" in u else "name-level (blind/content policy) — 9 cells"
                          for u in corrected["unit"]]
    return old, corrected


# ── B. figure 03 (bare-policy means over the 10-05 sweep) ────────────

def figure03_correction() -> tuple[pd.DataFrame, pd.DataFrame]:
    df = ff.load_results(REPO / "artifacts" / "10-05-results" / "combined_results.csv")
    ev = ff.per_example_evictions(df)
    pressured = df[ev.fillna(0) > 0] if ev.notna().any() else df
    old = (pressured.groupby("policy")[["gold_survived", "hit_at_k"]]
           .mean().round(3).reset_index()
           .rename(columns={c: f"old_{c}" for c in ("gold_survived", "hit_at_k")}))
    P = pressured.copy()
    retr = P["retriever"].fillna("") if "retriever" in P.columns else ""
    P["unit"] = [policy_unit(p, r) for p, r in zip(P["policy"], retr)]
    corrected = (P.groupby("unit")[["gold_survived", "hit_at_k"]]
                 .mean().round(3).reset_index())
    # regenerate the figure via the FIXED plotting code (sprint-1 D3 patch)
    FIGS.mkdir(parents=True, exist_ok=True)
    ax = ff.fig_policy_comparison(df)
    if ax is not None:
        ax.figure.savefig(FIGS / "03_policy_comparison_corrected.png")
    return old, corrected


# ── C. percentile companions for the knee reach numbers ──────────────

def knee_percentiles() -> tuple[pd.DataFrame, pd.DataFrame, float]:
    cells = [load_cell(p, "025") for p in KNEE_POLICIES]
    pool = pd.concat(cells, ignore_index=True)
    median_n = pool["n_resident"].median()
    c = K_FLOOR / median_n
    pool = add_metrics(pool, c)
    per_policy = (pool.groupby("policy_name")
                  .agg(hit_at_5_fixedk=("hit_at_5", "mean"),
                       pct_pessimistic=("pct_pessimistic", "mean"),
                       pct_optimistic=("pct_optimistic", "mean"),
                       censored_frac=("is_censored", "mean"),
                       mean_n_resident=("n_resident", "mean"))
                  .round(4).reset_index())
    idx = per_policy.set_index("policy_name")
    rows = []
    for a, b in REACH_PAIRS:
        rows.append({
            "pair": f"{a} − {b}",
            "d_hit_at_5_fixedk": round(idx.loc[a, "hit_at_5_fixedk"]
                                       - idx.loc[b, "hit_at_5_fixedk"], 4),
            "d_pct_pessimistic": round(idx.loc[a, "pct_pessimistic"]
                                       - idx.loc[b, "pct_pessimistic"], 4),
            "d_pct_optimistic": round(idx.loc[a, "pct_optimistic"]
                                      - idx.loc[b, "pct_optimistic"], 4),
        })
    return per_policy, pd.DataFrame(rows), float(median_n)


def main() -> None:
    old_rank, corr_rank = robustness_correction()
    old_fig, corr_fig = figure03_correction()
    per_policy, pair_deltas, median_n = knee_percentiles()

    L = ["# S3 — retired vs corrected aggregates (re-aggregation only)", ""]
    L.append("Every number below is recomputed with pandas from RECORDED artifacts; "
             "no runner, retriever, or model was invoked. Percentile columns are "
             "[pessimistic, optimistic] interval bounds (historical depth-10 "
             "censoring — sprint-1 D2 convention). Lower percentile = better.")
    L.append("")

    L.append("## Retired vs corrected — the master table")
    L.append("")
    L.append("| old number (as cited) | where cited | why retired / status | corrected number | delta |")
    L.append("|---|---|---|---|---|")
    old_map = dict(zip(old_rank["policy"], old_rank["old_mean_rank_9cells"]))
    for pol in ("lru", "lfu"):
        units = corr_rank[corr_rank["unit"].str.startswith(f"{pol}@")]
        corr_txt = "; ".join(f"{u}: {m:.2f}"
                             for u, m in zip(units["unit"], units["mean_rank"]))
        L.append(f"| robustness mean AUC-B rank **{pol} = {old_map[pol]:.2f}** (9 cells) "
                 f"| phase4_stats.md 'Robustness' list "
                 f"| **RETIRED — laundered**: pools three (policy, retriever) identities under one name "
                 f"| {corr_txt} "
                 f"| name-level mean hid a spread of "
                 f"{units['mean_rank'].max() - units['mean_rank'].min():.2f} rank places |")
    for pol in ("compression", "keep_smallest", "semantic_coverage", "semantic",
                "evict_smallest", "random"):
        L.append(f"| robustness mean rank {pol} = {old_map[pol]:.2f} "
                 f"| phase4_stats.md | kept — blind/content policy, name-level is its real "
                 f"identity; row now carries the {NAME_LEVEL_PREFIX[:-1]} label "
                 f"| unchanged ({old_map[pol]:.2f}, 9 cells) | 0 |")
    L.append("| Δreach(compression−lru) = **−0.048** [−0.077,−0.021] p<0.01 "
             "| phase4_stats.md pairwise; findings.md §2; REPORT.md §3 (as −0.05) "
             "| kept — single cell (beam_turn_pair × bm25), never laundered; but it is a FIXED-K "
             "number and composition-confounded (sprint-1 D2) "
             f"| percentile companion below: Δpct pess {pair_deltas.iloc[0]['d_pct_pessimistic']:+.3f} / "
             f"opt {pair_deltas.iloc[0]['d_pct_optimistic']:+.3f} (negative favors compression) "
             "| fixed-k sign does not survive the invariant metric |")
    L.append("| pairwise Δans table (7 pairs), Friedman χ²=178.1 + single-cell mean ranks, "
             "AUC-B leader 0.782 [0.697,0.862], keep_smallest Δ0.000 p=1.0 vs random "
             "| phase4_stats.md / findings.md §3 "
             "| kept — all computed inside beam_turn_pair × bm25 (verified cell-scoped in the "
             "sprint-1 D3 audit); answer-survival is not a retrieval metric (no percentile form) "
             "| unchanged | 0 |")
    L.append("| keep_smallest AUC-B **0.98** at turn (findings.md §4) "
             "| findings.md | kept — a PER-CELL value read off fair_aucb_ranking.csv "
             "(beam_turn cell), not a pooled number "
             "| unchanged | 0 |")
    L.append("| figure 03_policy_comparison bars (bare-policy means, 10-05 sweep) "
             "| figs/03_policy_comparison.png "
             "| **RETIRED — laundered**: bare-name groupby over a multi-retriever frame "
             "| regenerated per (policy, retriever): report2/figs_s3/03_policy_comparison_corrected.png "
             "| tables below |")
    L.append("")

    L.append("## A. Robustness ranking — corrected view (coupled identity)")
    L.append("")
    L.append(_md(corr_rank))
    L.append("")
    L.append("## B. Figure-03 means — old (retired) vs corrected")
    L.append("")
    L.append("Old (bare-name, RETIRED):")
    L.append("")
    L.append(_md(old_fig))
    L.append("")
    L.append("Corrected (per (policy, retriever) unit for history policies):")
    L.append("")
    L.append(_md(corr_fig))
    L.append("")
    L.append(f"## C. Knee (budget 25%) reach numbers with percentile companions "
             f"(median n_resident of the 6-policy pool = {median_n:.0f})")
    L.append("")
    L.append(_md(per_policy))
    L.append("")
    L.append(_md(pair_deltas, "+.4f"))
    L.append("")
    L.append("Reading: fixed-k Δ(compression−lru) is negative (LRU better) but the "
             "percentile deltas favor compression under BOTH censoring bounds — "
             "the sprint-1 D2 flip, now shown beside every cited pair.")

    OUT.write_text("\n".join(L) + "\n")
    print("\n".join(L[:40]))
    print(f"\nwrote {OUT} and {FIGS}/03_policy_comparison_corrected.png")


if __name__ == "__main__":
    main()
