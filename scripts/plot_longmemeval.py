"""DEPRECATED — do not use, do not repair (decision R-4, 2026-07-22).

This script predates the current results schema and the current grid. It is kept
only so the two defects below stay on the record; it is NOT a plotting utility
for any run in this repository.

DEFECT 1 — SCHEMA MISMATCH. It reads columns the results CSV does not have:
  * ``gold_retained``  — the schema field is ``gold_id_retained`` (schema.py:25).
    ``gold_retained`` appears nowhere in ``src/forgetit``.
  * ``question_id``    — the per-row id column is ``query_id`` (schema.py:86);
    ``question_id`` is a raw LongMemEval dataset key, never a results column.
  Both are dereferenced unguarded (``df[col]``, ``("question_id", "count")``),
  so against a current CSV this raises KeyError rather than mis-plotting — the
  one merciful thing about it.
  It also globs a FLAT ``longmemeval_*.csv`` layout; results are now nested as
  ``retention/<policy>/<retriever>/budget_<pct>pct/results.csv``, and the sweep
  axis is a per-example budget FRACTION, not the absolute ``budget_bytes`` this
  file puts on a log x-axis.

DEFECT 2 — POOLING VIOLATION. ``aggregate()`` groups by
``(policy, budget_bytes, k)`` only, pooling over ITEMISER and RETRIEVER. Those
are not nuisance dimensions: the itemiser sets item granularity (so it changes
what a "budget" even holds) and the retriever sets the ranking (so it changes
every hit@k). Averaging across them mixes incomparable populations and can
reverse a policy ordering. Any ranking must be grouped by the COUPLED key
``(itemiser, retriever, policy)`` — the same correction applied to the pooled
cell in ``scripts/experimentation.ipynb``.

Per-retriever result tables belong to the analysis phase, not to this file.
"""
import argparse
import glob
import os
import sys
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

_DEPRECATION_NOTICE = """\
scripts/plot_longmemeval.py is DEPRECATED (R-4) and will not run.
  * it reads 'gold_retained' / 'question_id', which are not results columns
    (the schema has 'gold_id_retained' / 'query_id')
  * it pools over itemiser and retriever, which can reverse policy orderings
Read the module docstring before doing anything with this file. Nothing here
should be repaired — build the analysis-phase tables instead.
"""


METRICS_MAIN = [
    ("gold_retained", "Gold retained rate"),
    ("hit_at_k", "Hit@k rate"),
    ("hit_at_k_given_retained", "Hit@k | retained rate"),
]

METRICS_COST = [
    ("evictions_total_at_query_time", "Avg evictions_total_at_query_time"),
    ("used_bytes_end", "Avg used bytes (end)"),
]


def load_all_csvs(results_dir: Path) -> pd.DataFrame:
    files = sorted(glob.glob(str(results_dir / "longmemeval_*.csv")))
    if not files:
        raise FileNotFoundError(f"No CSVs in {results_dir}")

    dfs = []
    for fp in files:
        df = pd.read_csv(fp)
        df["__source_file"] = os.path.basename(fp)
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)

    df["budget_bytes"] = df["budget_bytes"].astype(int)
    df["k"] = df["k"].astype(int)
    df["policy"] = df["policy"].astype(str)

    for col in ["gold_retained", "hit_at_k", "hit_at_k_given_retained", "used_bytes_end", "evictions_total_at_query_time"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    agg = (
        df.groupby(["policy", "budget_bytes", "k"], as_index=False)
        .agg(
            n=("question_id", "count"),
            gold_retained_mean=("gold_retained", "mean"),
            gold_retained_std=("gold_retained", "std"),
            hit_at_k_mean=("hit_at_k", "mean"),
            hit_at_k_std=("hit_at_k", "std"),
            hit_at_k_given_retained_mean=("hit_at_k_given_retained", "mean"),
            hit_at_k_given_retained_std=("hit_at_k_given_retained", "std"),
            evictions_mean=("evictions_total_at_query_time", "mean"),
            evictions_std=("evictions_total_at_query_time", "std"),
            used_bytes_end_mean=("used_bytes_end", "mean"),
            used_bytes_end_std=("used_bytes_end", "std"),
        )
        .sort_values(["k", "budget_bytes", "policy"])
    )
    # std can be NaN if n=1; replace with 0
    for c in agg.columns:
        if c.endswith("_std"):
            agg[c] = agg[c].fillna(0.0)
    return agg


def _plot_lines_with_errorbars(sub: pd.DataFrame, metric: str, ylabel: str, out_path: Path) -> None:
    plt.figure()
    for policy in sorted(sub["policy"].unique()):
        p = sub[sub["policy"] == policy].sort_values("budget_bytes")
        x = p["budget_bytes"].values
        y = p[f"{metric}_mean"].values
        e = p[f"{metric}_std"].values
        plt.errorbar(x, y, yerr=e, marker="o", capsize=3, label=policy)

    plt.xscale("log")
    plt.xlabel("Budget (bytes, log scale)")
    plt.ylabel(ylabel)
    plt.title(f"{ylabel} vs Budget")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()


def _plot_bars_with_errorbars(sub: pd.DataFrame, metric: str, ylabel: str, out_path: Path) -> None:

    budgets = sorted(sub["budget_bytes"].unique())
    policies = sorted(sub["policy"].unique())

    # map budgets to positions 0..N-1 for clean bars
    pos = list(range(len(budgets)))
    width = 0.35 if len(policies) == 2 else 0.8 / max(1, len(policies))

    plt.figure()
    for i, policy in enumerate(policies):
        p = sub[sub["policy"] == policy].set_index("budget_bytes").reindex(budgets).reset_index()
        y = p[f"{metric}_mean"].values
        e = p[f"{metric}_std"].values
        x = [pp + (i - (len(policies) - 1) / 2) * width for pp in pos]
        plt.bar(x, y, width=width, yerr=e, capsize=3, label=policy)

    plt.xticks(pos, [f"{b//1024}KiB" if b < 1024*1024 else f"{b/(1024*1024):.1f}MiB" for b in budgets], rotation=30)
    plt.xlabel("Budget")
    plt.ylabel(ylabel)
    plt.title(f"{ylabel} vs Budget")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()


def main():
    # Refusing beats emitting a wrong figure: a plot carries no schema, so a
    # pooled/mis-keyed chart is indistinguishable from a correct one once it is
    # in a document. (Deliberately a refusal, not a repair — see R-4.)
    print(_DEPRECATION_NOTICE, file=sys.stderr)
    return 2


def _deprecated_main():
    """The original entry point, kept UNREPAIRED as the record of both defects.
    Not reachable from the CLI; see the module docstring."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--results-dir", default="results/longmemeval_lru_lfu_rerun_limit200")
    ap.add_argument("--out-dir", default="results/longmemeval_lru_lfu_rerun_limit200/plots")
    args = ap.parse_args()

    results_dir = Path(args.results_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_all_csvs(results_dir)
    agg = aggregate(df)
    agg.to_csv(out_dir / "aggregate.csv", index=False)

    for k in sorted(agg["k"].unique()):
        sub = agg[agg["k"] == k].copy()

        # Main metrics: lines + errorbars
        for m, label in METRICS_MAIN:
            _plot_lines_with_errorbars(sub, m, f"{label} (k={k})", out_dir / f"{m}_k{k}.png")

        # Cost metrics: evictions_total_at_query_time is best as bar chart; used_bytes can be line
        _plot_bars_with_errorbars(sub, "evictions_total_at_query_time", f"Avg evictions_total_at_query_time (k={k})", out_dir / f"evictions_k{k}.png")
        _plot_lines_with_errorbars(sub, "used_bytes_end", f"Avg used bytes end (k={k})", out_dir / f"used_bytes_end_k{k}.png")

    print(f"Wrote plots to: {out_dir}")


if __name__ == "__main__":
    raise SystemExit(main())