"""Phase 2b — the COMPRESSION-FAIR survival comparison.

Ranking compression against pure-eviction policies on id-survival is apples-to-
oranges (compression keeps the item but can compress the ANSWER out). This redoes
the survival metric + AUC-B ranking on ANSWER-survival (conjunctive, over the
localizable gold set — the common denominator where the answer was actually in a
gold item to begin with). For pure-eviction policies answer-survival ≡ id-survival,
so the comparison is fair by construction.

Scope caveat: the localizable subset is SMALL (~42 queries/config on turn_pair),
because BEAM gold is often paraphrased / spread across turns — so CIs are wide and
every claim is qualified. This is the honest price of a compression-fair number.

Writes: fair_survival_metrics.csv, fair_aucb_ranking.csv.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "lib"))
import beam_answer  # noqa: E402
from beam_load import cluster_bootstrap_mean, POLICIES, ITEMISERS, RETRIEVERS, BUDGET_FRACS  # noqa: E402
from phase2_scoring import aucb, PRESSURE_BAND  # noqa: E402

TAB = HERE / "tables"
N_BOOT = 10000


def _aucb_boot(p, col, band, n_boot=5000, seed=0):
    rng = np.random.default_rng(seed)
    exs = p["example_id"].unique()
    x = np.log2(np.array(band, float)); w = x[-1] - x[0]
    agg = p.groupby(["example_id", "budget_frac"])[col].agg(["sum", "count"])
    S = np.zeros((len(exs), len(band))); C = np.zeros((len(exs), len(band)))
    for i, e in enumerate(exs):
        for j, f in enumerate(band):
            if (e, f) in agg.index:
                r = agg.loc[(e, f)]; S[i, j] = r["sum"]; C[i, j] = r["count"]
    idx = rng.integers(0, len(exs), size=(n_boot, len(exs)))
    means = np.where(C[idx].sum(1) > 0, S[idx].sum(1) / np.maximum(C[idx].sum(1), 1), np.nan)
    aucs = np.trapezoid(means, x, axis=1) / w
    return np.nanpercentile(aucs, [2.5, 97.5])


def main():
    ans = beam_answer.build()

    # per-config fair survival with cluster-bootstrap CIs
    rows = []
    for (it, re, pol, bf), g in ans.groupby(["itemiser", "retriever", "policy", "budget_frac"]):
        for met in ("ans_conj", "id_conj", "ans_frac"):
            ci = cluster_bootstrap_mean(g, met, n_boot=N_BOOT, seed=0)
            rows.append({"itemiser": it, "retriever": re, "policy": pol, "budget_frac": bf,
                         "metric": met, "n_query": len(g), **ci})
    met_df = pd.DataFrame(rows)
    met_df.to_csv(TAB / "fair_survival_metrics.csv", index=False)

    # fair AUC-B ranking on ANSWER-survival
    auc_rows = []
    for it in ITEMISERS:
        for re in RETRIEVERS:
            sub = ans[(ans.itemiser == it) & (ans.retriever == re)]
            for pol in POLICIES:
                p = sub[sub.policy == pol]
                band = [p[p.budget_frac == f]["ans_conj"].mean() for f in PRESSURE_BAND]
                a = aucb(PRESSURE_BAND, band)
                lo, hi = _aucb_boot(p, "ans_conj", PRESSURE_BAND) if len(p) else (np.nan, np.nan)
                # id-survival AUC for the same policy (shows compression's inflation)
                band_id = [p[p.budget_frac == f]["id_conj"].mean() for f in PRESSURE_BAND]
                auc_rows.append({"itemiser": it, "retriever": re, "policy": pol,
                                 "aucb_ans": a, "aucb_ans_lo": float(lo), "aucb_ans_hi": float(hi),
                                 "aucb_id": aucb(PRESSURE_BAND, band_id)})
    auc_df = pd.DataFrame(auc_rows)
    auc_df.to_csv(TAB / "fair_aucb_ranking.csv", index=False)

    # console: primary turn_pair/bm25 fair ranking
    print("== FAIR answer-survival AUC-B ranking (turn_pair/bm25) ==")
    r = auc_df[(auc_df.itemiser == "beam_turn_pair") & (auc_df.retriever == "bm25")] \
        .sort_values("aucb_ans", ascending=False)
    for _, x in r.iterrows():
        infl = x.aucb_id - x.aucb_ans
        print(f"  {x.policy:18s} ANS-AUCB={x.aucb_ans:.3f} [{x.aucb_ans_lo:.3f},{x.aucb_ans_hi:.3f}]"
              f"   id-AUCB={x.aucb_id:.3f}  (compression inflation={infl:+.3f})")
    print("\nwrote fair_survival_metrics.csv, fair_aucb_ranking.csv")


if __name__ == "__main__":
    main()
