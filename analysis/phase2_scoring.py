"""Phase 2 — the end-to-end scoring function (insert → retrieve → answer).

Emits the tidy tables Phases 3/4 consume. Given the Phase-0 gate (deterministic
answer accuracy is degenerate on BEAM), the end-to-end HEADLINE is retrieval
reachability (gold reaches the reader's top-k); survival is the mechanism; the
answer stage is reported as graded F1 + C/H/O only, never as a discriminating
headline. All ceilings are recomputed from this run's events (beam_oracle).

Stage indicators per (query, policy, budget):
  S_conj — all gold resident            (conjunctive survival, mechanism)
  reach  — >=1 gold in top-k (hit@k)     (end-to-end through retrieval, HEADLINE)
  reach_conj — all gold in top-k (recall@k==1)
  A / CHO — deterministic answer (floored) + three-way C/H/O

Tables written to analysis/tables/:
  metrics_by_config.csv     — per (itemiser,retriever,policy,budget) metric + cluster-boot CI
  trace_ceilings.csv        — oracle / keep_smallest / recency idealized survival
  aucb_ranking.csv          — normalized AUC-B ranking per (itemiser,retriever), with CI
  regret_by_budget.csv      — avoidable vs unavoidable survival loss
  funnel_by_budget.csv      — lost@survival / lost@retrieval / lost@reader
  cho_by_policy.csv         — C/H/O at the knee budget
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "lib"))
import frame  # noqa: E402
from beam_load import (cluster_bootstrap_mean, cluster_bootstrap_paired,  # noqa: E402
                       POLICIES, ITEMISERS, RETRIEVERS, BUDGET_FRACS)
from beam_oracle import trace_criticals, survival_curve  # noqa: E402

TAB = HERE / "tables"
TAB.mkdir(exist_ok=True)

PRESSURE_BAND = [0.1, 0.25, 0.5, 0.75]     # exclude 1.0 (no-eviction control)
KNEE = 0.25                                 # focus budget (oracle just becomes feasible)
PRIMARY_IT, PRIMARY_RE = "beam_turn_pair", "bm25"
N_BOOT = 10000

# Metric definitions: name -> (column, row-mask function). All are 0/1 per row.
def _metric_cols(m: pd.DataFrame) -> pd.DataFrame:
    m = m.copy()
    m["S_conj_i"] = m["S_conj"].astype(float)
    m["S_any_i"] = m["S_any"].astype(float)
    m["reach"] = m["R_any"].astype(float)               # hit@k (any gold in top-k)
    m["reach_conj"] = m["R_conj"].astype(float)         # all gold in top-k
    m["A_i"] = m["A"].astype(float)
    return m


def aucb(fracs, vals):
    """Trapezoid AUC over log2(budget) on the pressure band, normalized to the band
    width so it reads as a mean-over-band (0..1 for a rate metric)."""
    x = np.log2(np.array(fracs, float))
    y = np.array(vals, float)
    order = np.argsort(x)
    x, y = x[order], y[order]
    return float(np.trapezoid(y, x) / (x[-1] - x[0]))


def main():
    m = _metric_cols(frame.build())

    # ── 1. trace ceilings (idealized survival) ───────────────────────
    cei_rows = []
    crit_by_it = {}
    for it in ITEMISERS:
        crit = trace_criticals(it)
        crit_by_it[it] = crit
        for pol in ("oracle_qcond", "union_wt", "keep_smallest", "recency_lru"):
            sc = survival_curve(crit, pol, BUDGET_FRACS)
            for f, v in sc.items():
                cei_rows.append({"itemiser": it, "ceiling": pol, "budget_frac": f, "survival": v})
    pd.DataFrame(cei_rows).to_csv(TAB / "trace_ceilings.csv", index=False)

    # ── 2. per-config metric table with cluster-bootstrap CIs ────────
    METRICS = ["S_conj_i", "S_any_i", "reach", "reach_conj"]
    rows = []
    for (it, re, pol, bf), g in m.groupby(["itemiser", "retriever", "policy", "budget_frac"]):
        for met in METRICS:
            ci = cluster_bootstrap_mean(g, met, n_boot=N_BOOT, seed=0)
            rows.append({"itemiser": it, "retriever": re, "policy": pol, "budget_frac": bf,
                         "metric": met, **ci})
        # qa_f1 only over gradeable rows
        gg = g[g["gradeable"]]
        ci = cluster_bootstrap_mean(gg, "qa_f1", n_boot=N_BOOT, seed=0) if len(gg) else \
             {"mean": np.nan, "lo": np.nan, "hi": np.nan, "se": np.nan, "n_clusters": 0}
        rows.append({"itemiser": it, "retriever": re, "policy": pol, "budget_frac": bf,
                     "metric": "qa_f1_gradeable", **ci})
    met_df = pd.DataFrame(rows)
    met_df.to_csv(TAB / "metrics_by_config.csv", index=False)

    # ── 3. AUC-B ranking (normalized) per (itemiser, retriever) ──────
    # headline = reach; also compute for S_conj. Normalize reach-AUC by the common
    # no-eviction reach (frac=1.0); S_conj-AUC by oracle(union_wt)-AUC.
    auc_rows = []
    for it in ITEMISERS:
        cei = {pol: {f: survival_curve(crit_by_it[it], pol, [f])[f] for f in PRESSURE_BAND}
               for pol in ("union_wt",)}
        oracle_auc_S = aucb(PRESSURE_BAND, [cei["union_wt"][f] for f in PRESSURE_BAND])
        for re in RETRIEVERS:
            sub = m[(m.itemiser == it) & (m.retriever == re)]
            # common no-eviction reach (all policies identical at frac=1.0)
            reach_full = sub[sub.budget_frac == 1.0]["reach"].mean()
            for pol in POLICIES:
                p = sub[sub.policy == pol]
                reach_band = [p[p.budget_frac == f]["reach"].mean() for f in PRESSURE_BAND]
                S_band = [p[p.budget_frac == f]["S_conj_i"].mean() for f in PRESSURE_BAND]
                auc_reach = aucb(PRESSURE_BAND, reach_band)
                auc_S = aucb(PRESSURE_BAND, S_band)
                # bootstrap CI for the reach-AUC (resample examples, recompute per-budget means)
                ci = _aucb_bootstrap(p, "reach", PRESSURE_BAND, seed=0)
                auc_rows.append({
                    "itemiser": it, "retriever": re, "policy": pol,
                    "aucb_reach": auc_reach,
                    "aucb_reach_norm": auc_reach / reach_full if reach_full else np.nan,
                    "aucb_reach_lo": ci["lo"], "aucb_reach_hi": ci["hi"],
                    "aucb_Sconj": auc_S,
                    "aucb_Sconj_norm_oracle": auc_S / oracle_auc_S if oracle_auc_S else np.nan,
                })
    auc_df = pd.DataFrame(auc_rows)
    auc_df.to_csv(TAB / "aucb_ranking.csv", index=False)

    # ── 4. regret decomposition (survival) at PRIMARY itemiser ───────
    reg_rows = []
    crit = crit_by_it[PRIMARY_IT]
    for re in RETRIEVERS:
        sub = m[(m.itemiser == PRIMARY_IT) & (m.retriever == re)]
        for bf in BUDGET_FRACS:
            ceil = survival_curve(crit, "union_wt", [bf])[bf]      # honest write-time ceiling
            ceil_q = survival_curve(crit, "oracle_qcond", [bf])[bf]
            for pol in POLICIES:
                pv = sub[(sub.policy == pol) & (sub.budget_frac == bf)]["S_conj_i"].mean()
                reg_rows.append({"itemiser": PRIMARY_IT, "retriever": re, "policy": pol,
                                 "budget_frac": bf, "policy_survival": pv,
                                 "oracle_union": ceil, "oracle_qcond": ceil_q,
                                 "avoidable": max(0.0, min(ceil_q, 1.0) - pv),
                                 "unavoidable": max(0.0, 1.0 - ceil_q)})
    pd.DataFrame(reg_rows).to_csv(TAB / "regret_by_budget.csv", index=False)

    # ── 5. funnel: lost@survival / retrieval / reader (primary cfg) ──
    fun_rows = []
    sub = m[(m.itemiser == PRIMARY_IT) & (m.retriever == PRIMARY_RE)]
    for pol in POLICIES:
        for bf in BUDGET_FRACS:
            g = sub[(sub.policy == pol) & (sub.budget_frac == bf)]
            n = len(g)
            # reachability funnel: not-reached decomposed into survival vs retrieval loss;
            # reader loss is among reached+gradeable (answered-wrong or abstain).
            lost_surv = (~g["S_any"]).mean()
            lost_retr = (g["S_any"] & ~g["R_any"]).mean()
            reached = g["R_any"]
            grad = g["gradeable"]
            reader_pool = g[reached & grad]
            reader_wrong = (~reader_pool["A"]).mean() if len(reader_pool) else np.nan
            fun_rows.append({"policy": pol, "budget_frac": bf, "n": n,
                             "lost_survival": lost_surv, "lost_retrieval": lost_retr,
                             "reached": reached.mean(),
                             "reader_wrong_given_reached_gradeable": reader_wrong})
    pd.DataFrame(fun_rows).to_csv(TAB / "funnel_by_budget.csv", index=False)

    # ── 6. C/H/O at the knee (answerable+gradeable subset) ───────────
    cho_rows = []
    for re in RETRIEVERS:
        sub = m[(m.itemiser == PRIMARY_IT) & (m.retriever == re) &
                (m.budget_frac == KNEE) & (m.gradeable)]
        for pol in POLICIES:
            g = sub[sub.policy == pol]
            n = len(g)
            if not n:
                continue
            cho_rows.append({"itemiser": PRIMARY_IT, "retriever": re, "policy": pol,
                             "budget_frac": KNEE, "n": n,
                             "C": (g["CHO"] == "C").mean(), "H": (g["CHO"] == "H").mean(),
                             "O": (g["CHO"] == "O").mean()})
    pd.DataFrame(cho_rows).to_csv(TAB / "cho_by_policy.csv", index=False)

    # ── console summary: primary reach + S_conj ranking at knee ──────
    print(f"== PRIMARY {PRIMARY_IT}/{PRIMARY_RE} — survival & reach vs budget ==")
    prim = met_df[(met_df.itemiser == PRIMARY_IT) & (met_df.retriever == PRIMARY_RE)]
    for met in ("S_conj_i", "reach"):
        print(f"\n  {met}:")
        piv = prim[prim.metric == met].pivot(index="policy", columns="budget_frac", values="mean")
        print(piv.round(3).to_string())
    print("\n== normalized AUC-B (reach) ranking, primary cfg ==")
    r = auc_df[(auc_df.itemiser == PRIMARY_IT) & (auc_df.retriever == PRIMARY_RE)] \
        .sort_values("aucb_reach", ascending=False)
    for _, row in r.iterrows():
        print(f"  {row.policy:18s} AUCB_reach={row.aucb_reach:.3f} "
              f"[{row.aucb_reach_lo:.3f},{row.aucb_reach_hi:.3f}]  "
              f"AUCB_Sconj={row.aucb_Sconj:.3f}")
    print("\nwrote 6 tables to analysis/tables/")


def _aucb_bootstrap(p: pd.DataFrame, col: str, band, n_boot: int = 2000, seed: int = 0) -> dict:
    """Example-cluster bootstrap CI for a policy's normalized-band AUC of ``col``.

    Vectorized: build per-(example × budget) sum/count matrices, resample example
    rows, recompute per-budget means and the band AUC for all draws at once."""
    rng = np.random.default_rng(seed)
    exs = p["example_id"].unique()
    x = np.log2(np.array(band, float))
    w = x[-1] - x[0]
    agg = p.groupby(["example_id", "budget_frac"])[col].agg(["sum", "count"])
    S = np.zeros((len(exs), len(band))); C = np.zeros((len(exs), len(band)))
    for i, e in enumerate(exs):
        for j, f in enumerate(band):
            if (e, f) in agg.index:
                r = agg.loc[(e, f)]
                S[i, j] = r["sum"]; C[i, j] = r["count"]
    idx = rng.integers(0, len(exs), size=(n_boot, len(exs)))
    sums = S[idx].sum(axis=1)                        # (n_boot, n_band)
    cnts = C[idx].sum(axis=1)
    means = np.where(cnts > 0, sums / np.maximum(cnts, 1), np.nan)
    aucs = np.trapezoid(means, x, axis=1) / w
    lo, hi = np.nanpercentile(aucs, [2.5, 97.5])
    return {"lo": float(lo), "hi": float(hi)}


if __name__ == "__main__":
    main()
