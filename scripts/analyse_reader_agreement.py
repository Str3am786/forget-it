#!/usr/bin/env python3
"""
Reader invariance: does the ordering of forgetting policies survive the reader?

This is the statistic the model axis exists to produce. It is not a trend fit,
because the roster is a crossed design rather than a ladder: two capacity bands,
two families represented in each. So the question is agreement among judges, and
the estimator is Kendall's coefficient of concordance W, with the pairwise tau
matrix underneath it and the decomposition on top.

  W = 1   all readers order the policies identically
  W = 0   the orderings are unrelated

Decomposition, which is the point of crossing rather than laddering:
  family effect   = mean pairwise tau WITHIN a capacity band, ACROSS families
  capacity effect = mean pairwise tau WITHIN a family, ACROSS bands

If both are high, the invariance claim is vendor general. If the family term is
low and the capacity term high, the benchmark is recipe sensitive. If the
reverse, it is capacity sensitive and the threshold can be located. Every
outcome is interpretable, which a bare vendor sample would not be.

Input: long format CSV with one row per (reader, workload, policy) and a score
column. Reader metadata comes from readers.json so family and band are never
retyped.

    python3 analyse_reader_agreement.py \
        --scores artifacts/reader_sweep/summary.csv \
        --manifest configs/readers.json \
        --score-col accuracy_norm \
        --out artifacts/reader_sweep/agreement

Note on the score column: use the ceiling normalised accuracy, each reader's
score divided by its own C1, not raw accuracy. Otherwise the ranking is partly a
restatement of which reader has the higher ceiling.
"""

from __future__ import annotations

import argparse
import itertools
import json
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from scipy.stats import kendalltau, rankdata
except ImportError:  # pragma: no cover
    raise SystemExit("scipy is required:  python3 -m pip install --user scipy")


def kendalls_w(ranks: np.ndarray, n_perm: int = 10000, seed: int = 0) -> dict:
    """Kendall's W over an (m readers x n policies) rank matrix, tie corrected,
    with a permutation p value.

    Tie correction matters here: policies genuinely tie on some workloads, and
    the uncorrected form understates concordance when they do.
    """
    m, n = ranks.shape
    if m < 2 or n < 3:
        return {"W": float("nan"), "note": f"need >=2 readers and >=3 policies, got {m}x{n}"}

    def _w(r: np.ndarray) -> float:
        rank_sums = r.sum(axis=0)
        s = ((rank_sums - rank_sums.mean()) ** 2).sum()
        # tie correction term T, summed over readers
        t = 0.0
        for row in r:
            _, counts = np.unique(row, return_counts=True)
            t += (counts**3 - counts).sum()
        denom = (m**2 * (n**3 - n) - m * t) / 12.0
        return float(s / denom) if denom > 0 else float("nan")

    w_obs = _w(ranks)

    rng = np.random.default_rng(seed)
    null = np.empty(n_perm)
    for i in range(n_perm):
        shuffled = np.array([rng.permutation(row) for row in ranks])
        null[i] = _w(shuffled)
    p = float((null >= w_obs).sum() + 1) / (n_perm + 1)

    return {
        "W": round(w_obs, 4),
        "p_permutation": round(p, 5),
        "n_readers": int(m),
        "n_policies": int(n),
        "chi2_friedman": round(m * (n - 1) * w_obs, 3),
    }


def analyse_workload(df: pd.DataFrame, meta: dict, score_col: str) -> dict:
    """df: rows for a single workload, columns reader/policy/<score_col>."""
    wide = df.pivot_table(index="reader", columns="policy", values=score_col, aggfunc="mean")

    # A reader missing a policy makes the concordance undefined. Drop the policy
    # rather than the reader, and say which, because silently imputing here
    # would fabricate agreement.
    dropped = wide.columns[wide.isna().any()].tolist()
    wide = wide.dropna(axis=1)
    readers = list(wide.index)

    # Higher score = better = rank 1. rankdata ranks ascending, so negate.
    ranks = np.vstack([rankdata(-wide.loc[r].to_numpy()) for r in readers])

    result = {
        "readers": readers,
        "policies": list(wide.columns),
        "dropped_policies_incomplete": dropped,
        "concordance": kendalls_w(ranks),
    }

    # Pairwise tau matrix.
    pairs = []
    for a, b in itertools.combinations(range(len(readers)), 2):
        tau, p = kendalltau(ranks[a], ranks[b])
        ra, rb = readers[a], readers[b]
        pairs.append({
            "reader_a": ra,
            "reader_b": rb,
            "tau": round(float(tau), 4),
            "p": round(float(p), 5),
            "family_a": meta[ra]["family"],
            "family_b": meta[rb]["family"],
            "band_a": meta[ra]["band"],
            "band_b": meta[rb]["band"],
            "contrast": _contrast(meta[ra], meta[rb]),
        })
    result["pairwise"] = pairs

    # The decomposition.
    def _mean(kind: str) -> float | None:
        vals = [p["tau"] for p in pairs if p["contrast"] == kind]
        return round(float(np.mean(vals)), 4) if vals else None

    result["decomposition"] = {
        "family_effect_mean_tau": _mean("cross_family_same_band"),
        "capacity_effect_mean_tau": _mean("same_family_cross_band"),
        "cross_both_mean_tau": _mean("cross_family_cross_band"),
    }

    # Which policies are responsible for whatever disagreement exists. A
    # benchmark whose leading policies are stable while the mid field shuffles
    # is still useful, and it should be you who says which regime you are in.
    rank_df = pd.DataFrame(ranks, index=readers, columns=wide.columns)
    instability = (
        pd.DataFrame({
            "mean_rank": rank_df.mean(),
            "min_rank": rank_df.min(),
            "max_rank": rank_df.max(),
            "rank_range": rank_df.max() - rank_df.min(),
            "rank_sd": rank_df.std(ddof=0),
        })
        .sort_values("mean_rank")
    )
    result["policy_stability"] = instability.round(3).reset_index().rename(
        columns={"index": "policy"}
    ).to_dict("records")

    return result


def _contrast(a: dict, b: dict) -> str:
    same_family = a["family"] == b["family"]
    same_band = a["band"] == b["band"]
    if same_family and same_band:
        return "same_family_same_band"
    if same_band:
        return "cross_family_same_band"
    if same_family:
        return "same_family_cross_band"
    return "cross_family_cross_band"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scores", required=True,
                    help="long CSV: reader, workload, policy, <score-col>")
    ap.add_argument("--manifest", default="configs/readers.json")
    ap.add_argument("--score-col", default="accuracy_norm")
    ap.add_argument("--out", default="artifacts/reader_sweep/agreement")
    ap.add_argument("--exclude-readers", nargs="*", default=[],
                    help="e.g. the floor rung, if preflight failed it")
    ap.add_argument("--cutoff-before", default=None,
                    help="restrict to readers released before this YYYY-MM, "
                         "for the contamination controlled re run")
    args = ap.parse_args()

    manifest = json.loads(Path(args.manifest).read_text())
    meta = {r["id"]: r for r in manifest["readers"]}

    df = pd.read_csv(args.scores)
    need = {"reader", "workload", "policy", args.score_col}
    missing = need - set(df.columns)
    if missing:
        raise SystemExit(f"scores CSV is missing columns: {sorted(missing)}")

    unknown = set(df["reader"]) - set(meta)
    if unknown:
        raise SystemExit(
            f"readers in the scores not present in the manifest: {sorted(unknown)}.\n"
            "This usually means served-model-name was left at 'local' for a run, "
            "so its rows are unattributable."
        )

    if args.exclude_readers:
        df = df[~df["reader"].isin(args.exclude_readers)]
    if args.cutoff_before:
        keep = [r for r, m in meta.items() if m["released"] < args.cutoff_before]
        df = df[df["reader"].isin(keep)]
        print(f"[agreement] contamination controlled subset: {keep}")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    report = {
        "score_col": args.score_col,
        "excluded_readers": args.exclude_readers,
        "cutoff_before": args.cutoff_before,
        "workloads": {},
    }

    rows = []
    for workload, sub in df.groupby("workload"):
        res = analyse_workload(sub, meta, args.score_col)
        report["workloads"][str(workload)] = res
        c = res["concordance"]
        d = res["decomposition"]
        rows.append({
            "workload": workload,
            "n_readers": c.get("n_readers"),
            "n_policies": c.get("n_policies"),
            "kendall_W": c.get("W"),
            "p": c.get("p_permutation"),
            "family_effect_tau": d["family_effect_mean_tau"],
            "capacity_effect_tau": d["capacity_effect_mean_tau"],
        })
        pd.DataFrame(res["pairwise"]).to_csv(out / f"pairwise_{workload}.csv", index=False)
        pd.DataFrame(res["policy_stability"]).to_csv(out / f"stability_{workload}.csv", index=False)

    summary = pd.DataFrame(rows)
    summary.to_csv(out / "concordance.csv", index=False)
    (out / "agreement.json").write_text(json.dumps(report, indent=2, default=str) + "\n")

    print("\n[agreement] reader invariance by workload\n")
    print(summary.to_string(index=False))
    print(f"\n[agreement] written to {out}/")
    print("\nReading the table: W near 1 means the readers order the policies the "
          "same way. Compare family_effect_tau against capacity_effect_tau to see "
          "which axis carries any disagreement.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
