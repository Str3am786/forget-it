"""Phase 4 — statistical treatment (small-n discipline, n=35 examples).

All uncertainty is example-cluster level. The primary comparison is on the
COMPRESSION-FAIR metric — answer-survival (Phase 2b) — with retrieval reach as a
companion. Produces:
  - pairwise policy comparisons at the knee: paired example-cluster bootstrap of
    the mean difference + McNemar's test on the paired outcome table (fair metric
    AND reach);
  - Friedman over (example × budget) blocks + Nemenyi post-hoc (scikit-posthocs)
    on answer-survival;
  - the explicit AUC-B tie ruling (no "best" if the CI overlaps the runner-up).
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import friedmanchisquare
from statsmodels.stats.contingency_tables import mcnemar
import scikit_posthocs as sp

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "lib"))
import frame  # noqa: E402
import beam_answer  # noqa: E402
from beam_load import cluster_bootstrap_paired  # noqa: E402

TAB = HERE / "tables"
IT, RE, KNEE = "beam_turn_pair", "bm25", 0.25
PB = [0.1, 0.25, 0.5, 0.75]
ORDER = ["compression", "lfu", "lru", "keep_smallest", "semantic_coverage",
         "semantic", "evict_smallest", "random"]
PAIRS = [("compression", "lru"), ("compression", "lfu"), ("lru", "lfu"),
         ("compression", "keep_smallest"), ("lru", "keep_smallest"),
         ("keep_smallest", "semantic_coverage"), ("keep_smallest", "random")]


def _pairwise(df, val, a, b):
    pa = df[df.policy == a][["example_id", "query_id", val]]
    pb = df[df.policy == b][["example_id", "query_id", val]]
    boot = cluster_bootstrap_paired(pa, pb, val, n_boot=10000, seed=0)
    j = pa.merge(pb, on=["example_id", "query_id"], suffixes=("_a", "_b"))
    n10 = int(((j[f"{val}_a"] == 1) & (j[f"{val}_b"] == 0)).sum())
    n01 = int(((j[f"{val}_a"] == 0) & (j[f"{val}_b"] == 1)).sum())
    res = mcnemar([[0, n10], [n01, 0]], exact=(n10 + n01 < 25))
    return {"A": a, "B": b, "delta": boot["delta"], "lo": boot["lo"], "hi": boot["hi"],
            "boot_p": boot["p_two_sided"], "b_only": n10, "a_only": n01,
            "mcnemar_p": float(res.pvalue), "sig95": not (boot["lo"] <= 0 <= boot["hi"])}


def main():
    # fair metric (answer-survival) at knee — localizable subset
    ans = beam_answer.build()
    ansk = ans[(ans.itemiser == IT) & (ans.retriever == RE) & (ans.budget_frac == KNEE)].copy()
    ansk["ans_conj"] = ansk["ans_conj"].astype(int)
    # reach at knee (full query set)
    m = frame.build(); m["reach"] = m["R_any"].astype(int)
    reachk = m[(m.itemiser == IT) & (m.retriever == RE) & (m.budget_frac == KNEE)]

    pw_ans = pd.DataFrame([_pairwise(ansk, "ans_conj", a, b) for a, b in PAIRS])
    pw_reach = pd.DataFrame([_pairwise(reachk, "reach", a, b) for a, b in PAIRS])
    pw_ans.to_csv(TAB / "pairwise_answer_survival_knee.csv", index=False)
    pw_reach.to_csv(TAB / "pairwise_reach_knee.csv", index=False)

    # Friedman + Nemenyi on answer-survival over (example × budget) blocks
    band = ans[(ans.itemiser == IT) & (ans.retriever == RE) & ans.budget_frac.isin(PB)]
    block = band.groupby(["example_id", "budget_frac", "policy"])["ans_conj"].mean().reset_index()
    wide = block.pivot_table(index=["example_id", "budget_frac"], columns="policy",
                             values="ans_conj").dropna(how="any")[ORDER]
    stat, p = friedmanchisquare(*[wide[c].to_numpy() for c in ORDER])
    nem = sp.posthoc_nemenyi_friedman(wide.to_numpy()); nem.index = nem.columns = ORDER
    nem.to_csv(TAB / "nemenyi_answer_survival.csv")
    mean_rank = wide.rank(axis=1, ascending=False).mean().sort_values()

    # AUC-B tie ruling on the FAIR metric
    fa = pd.read_csv(TAB / "fair_aucb_ranking.csv")
    a = fa[(fa.itemiser == IT) & (fa.retriever == RE)].sort_values("aucb_ans", ascending=False)
    top = a.iloc[0]
    tie = a[a.aucb_ans_hi >= top.aucb_ans_lo]
    # NAME-LEVEL VIEW: ranks are computed within each (itemiser, retriever)
    # cell, but averaging them per bare policy name pools lru/lfu ACROSS
    # retrievers — for history policies that is a name-level aggregate, not a
    # policy score (coupled identity, D3). Kept as an explicitly labeled view;
    # lru/lfu rows are stamped in the output.
    from forgetit.benchmark.metrics import is_history_policy, NAME_LEVEL_PREFIX
    rob = (fa.assign(rank=fa.groupby(["itemiser", "retriever"])["aucb_ans"].rank(ascending=False))
           .groupby("policy")["rank"].mean().sort_values())
    rob.index = [NAME_LEVEL_PREFIX + p if is_history_policy(p) else p for p in rob.index]

    L = ["# Phase 4 — statistical treatment (n=35 examples, example-cluster level)\n",
         f"Primary metric = ANSWER-survival (compression-fair, Phase 2b); companion = "
         f"retrieval reach. Config {IT}×{RE}. Localizable subset is small "
         f"(~{ansk.query_id.nunique()} queries at the knee) — CIs are wide by construction.\n"]

    for title, pw, unit in [("ANSWER-survival (FAIR)", pw_ans, "Δans"),
                            ("retrieval reach (companion)", pw_reach, "Δreach")]:
        L.append(f"\n## Pairwise at knee 0.25 — {title}: paired cluster bootstrap + McNemar\n")
        L.append(f"| A | B | {unit}(A−B) | 95% CI | boot p | McNemar p | sig@95% |")
        L.append("|---|---|--:|--:|--:|--:|:--:|")
        for _, r in pw.iterrows():
            L.append(f"| {r.A} | {r.B} | {r.delta:+.3f} | [{r.lo:+.3f},{r.hi:+.3f}] | "
                     f"{r.boot_p:.3f} | {r.mcnemar_p:.2e} | {'YES' if r.sig95 else 'tie'} |")

    L.append(f"\n## Friedman on answer-survival over {len(wide)} (example×budget) blocks\n")
    L.append(f"- χ²={stat:.1f}, p={p:.2e} → policies are not interchangeable overall.")
    L.append("- mean Friedman rank (1=best):")
    for pol, rk in mean_rank.items():
        L.append(f"    - {pol:18s} {rk:.2f}")
    L.append("- Nemenyi post-hoc (answer-survival), key contrasts (full matrix in "
             "tables/nemenyi_answer_survival.csv):")
    for a2, b2 in [("compression", "lru"), ("compression", "keep_smallest"),
                   ("lru", "keep_smallest"), ("lru", "lfu"), ("compression", "semantic")]:
        L.append(f"    - {a2:14s} vs {b2:16s} p={nem.loc[a2, b2]:.3f}")

    L.append("\n## AUC-B tie ruling — FAIR answer-survival (95% cluster-bootstrap CI overlap)\n")
    L.append(f"- Leader: **{top.policy}** (answer-survival AUC-B {top.aucb_ans:.3f} "
             f"[{top.aucb_ans_lo:.3f},{top.aucb_ans_hi:.3f}]).")
    L.append(f"- CI-overlap top cluster (NOT separable at 95%): **{', '.join(tie.policy)}**.")
    L.append("- Robustness — mean answer-survival AUC-B rank across all 9 "
             "itemiser×retriever cells (1=best). NAME_LEVEL_VIEW: lru/lfu rows "
             "pool across retrievers (touch-based policies; their per-retriever "
             "identities are in the per-cell tables):")
    for pol, rk in rob.items():
        L.append(f"    - {pol:18s} {rk:.2f}")
    L.append("\n_Verdict: within the top cluster we report 'tied within uncertainty', not a "
             "winner. Compression's only statistically separable advantage is on id-survival — "
             "the metric answers do not keep (Phase 2b/F11)._")

    (HERE / "phase4_stats.md").write_text("\n".join(L) + "\n")
    print("\n".join(L))
    print("\nwrote phase4_stats.md + pairwise/nemenyi tables")


if __name__ == "__main__":
    main()
