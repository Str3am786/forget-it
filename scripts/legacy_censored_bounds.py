#!/usr/bin/env python3
"""Add censored-bounds columns to a LEGACY results.csv (depth-10 runs).

Rows written before 2026-07-14 observed gold ranks only to depth 10, so a
retained gold absent from gold_ranks is somewhere in [11, N] — with N unknown
(n_residents was not logged then). This script adds clearly-labeled interval
columns so legacy and uncensored runs can be compared without pretending the
legacy data is exact:

    legacy_censored     1.0 = best rank only bounded, 0.0 = observed exactly,
                        NaN = not retained / opaque (no rank to bound)
    gold_rank_best_lo   observed rank, or depth+1 when censored
    gold_rank_best_hi   observed rank, or NaN (upper bound unknowable legacy-side)

Usage:
    python scripts/legacy_censored_bounds.py <results.csv> [-o out.csv] [--depth 10]

Writes <results>.censored_bounds.csv next to the input unless -o is given.
Refuses to run on uncensored CSVs (those with an n_residents column carrying
real counts) — their gold_rank_best is exact; bounding it would only blur it.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from forgetit.benchmark.metrics import censored_bounds_row   # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("results", type=Path)
    ap.add_argument("-o", "--out", type=Path, default=None)
    ap.add_argument("--depth", type=int, default=10,
                    help="Legacy eval depth (RETRIEVAL_EVAL_DEPTH of the old run).")
    args = ap.parse_args()

    df = pd.read_csv(args.results)
    if "n_residents" in df.columns and (df["n_residents"].fillna(0) > 0).any():
        print("refusing: this CSV has real n_residents values — it is an "
              "UNCENSORED run; gold_rank_best is exact and needs no bounds.")
        return 2

    bounds = [
        censored_bounds_row(json.loads(row.get("gold_ranks", "[]") or "[]"), int(row.get("n_gold", 0) or 0),
                            str(row.get("gold_id_retained", "")).lower() == "true",
                            str(row.get("introspectable", "")).lower() == "true", depth=args.depth)
        for _, row in df.iterrows()
    ]
    out_df = pd.concat([df, pd.DataFrame(bounds, index=df.index)], axis=1)

    out = args.out or args.results.with_suffix(".censored_bounds.csv")
    out_df.to_csv(out, index=False)
    n_cens = int((out_df["legacy_censored"] == 1.0).sum())
    print(f"wrote {out}  ({len(out_df)} rows, {n_cens} censored)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
