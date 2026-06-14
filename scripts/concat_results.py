#!/usr/bin/env python3
"""
Concatenate the per-config CSVs produced by a (possibly sharded) benchmark run.

Each ``run_pipeline`` call writes its own
    <root>/<dataset>/<itemiser>/<store_label.../>/<reader>/results.csv
    <root>/<dataset>/<itemiser>/<store_label.../>/<reader>/summary.csv
so after a sharded LUMI run you just point this at the artifacts root and it
merges everything into two flat tables.

    python scripts/concat_results.py artifacts/benchmark
    python scripts/concat_results.py artifacts/benchmark --out-dir artifacts/combined

results.csv rows are already self-describing (policy/budget/itemiser/... columns).
summary.csv rows are bare aggregates, so we tag each with identifiers parsed
from its directory path.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def _tag_from_path(summary_csv: Path, root: Path) -> dict[str, str]:
    """Best-effort identifiers from the directory layout under ``root``."""
    rel_dir = summary_csv.parent.relative_to(root)
    parts = rel_dir.parts  # e.g. (dataset, itemiser, 'retention', policy, retriever, 'budget_N', reader)

    tag: dict[str, str] = {"config_dir": str(rel_dir)}
    if len(parts) >= 1:
        tag["dataset"] = parts[0]
    if len(parts) >= 2:
        tag["itemiser"] = parts[1]
    # reader is always the last path component
    tag["reader"] = parts[-1] if parts else ""
    # budget_<N> segment, wherever it sits
    for p in parts:
        if p.startswith("budget_"):
            tag["budget_bytes"] = p.removeprefix("budget_")
    # policy/retriever for the retention store layout: retention/<policy>/<retriever>/...
    if "retention" in parts:
        i = parts.index("retention")
        if i + 1 < len(parts):
            tag["policy"] = parts[i + 1]
        if i + 2 < len(parts):
            tag["retriever"] = parts[i + 2]
    return tag


def concat(root: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    results = sorted(root.rglob("results.csv"))
    summaries = sorted(root.rglob("summary.csv"))
    print(f"found {len(results)} results.csv and {len(summaries)} summary.csv under {root}")

    if results:
        frames = [pd.read_csv(p) for p in results]
        combined = pd.concat(frames, ignore_index=True)
        dest = out_dir / "combined_results.csv"
        combined.to_csv(dest, index=False)
        print(f"wrote {dest}  ({len(combined)} rows)")
    else:
        print("no results.csv found — nothing to merge")

    if summaries:
        rows = []
        for p in summaries:
            df = pd.read_csv(p)
            for key, val in _tag_from_path(p, root).items():
                df[key] = val
            rows.append(df)
        combined = pd.concat(rows, ignore_index=True)
        dest = out_dir / "combined_summary.csv"
        combined.to_csv(dest, index=False)
        print(f"wrote {dest}  ({len(combined)} rows)")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("root", type=Path, help="Artifacts root to scan recursively.")
    p.add_argument("--out-dir", type=Path, default=None,
                   help="Where to write combined CSVs (default: the root itself).")
    args = p.parse_args()

    root = args.root
    if not root.is_dir():
        p.error(f"not a directory: {root}")
    concat(root, args.out_dir or root)


if __name__ == "__main__":
    main()
