import argparse
import glob
import os
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


METRICS_MAIN = [
    ("gold_retained", "Gold retained rate"),
    ("hit_at_k", "Hit@k rate"),
    ("hit_at_k_given_retained", "Hit@k | retained rate"),
]

METRICS_COST = [
    ("evictions", "Avg evictions"),
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

    for col in ["gold_retained", "hit_at_k", "hit_at_k_given_retained", "used_bytes_end", "evictions"]:
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
            evictions_mean=("evictions", "mean"),
            evictions_std=("evictions", "std"),
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
    # side-by-side bars per budget so lines don’t hide each other
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
    ap = argparse.ArgumentParser()
    ap.add_argument("--results-dir", default="results/longmemeval")
    ap.add_argument("--out-dir", default="results/longmemeval/plots")
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

        # Cost metrics: evictions is best as bar chart; used_bytes can be line
        _plot_bars_with_errorbars(sub, "evictions", f"Avg evictions (k={k})", out_dir / f"evictions_k{k}.png")
        _plot_lines_with_errorbars(sub, "used_bytes_end", f"Avg used bytes end (k={k})", out_dir / f"used_bytes_end_k{k}.png")

    print(f"Wrote plots to: {out_dir}")


if __name__ == "__main__":
    main()