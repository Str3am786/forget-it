"""D8 — abstain/hallucinate split for empty-store floor runs.

Reads every floor results.csv under a root and reports, per itemiser:
abstained (honest floor), answered-but-wrong (hallucinated), answered-and-
"correct" (parametric leak past the deterministic grader), per question type.

Usage: python scripts/floor_split.py artifacts/verification_sprint/floor_demo
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

root = Path(sys.argv[1] if len(sys.argv) > 1 else "artifacts/verification_sprint/floor_demo")
paths = sorted(root.glob("**/floor/empty_store/**/results.csv"))
if not paths:
    sys.exit(f"no floor results under {root}")
for p in paths:
    df = pd.read_csv(p)
    itemiser = p.relative_to(root).parts[1]
    n = len(df)
    abst = df["abstained"].astype(bool)
    correct = df["qa_correct"].astype(bool)
    print(f"\n== {itemiser}  (policy={df['policy'].iloc[0]}, n={n}, "
          f"grading={df.get('grading_mode', pd.Series(['?'])).iloc[0]})")
    print(f"   abstained (honest floor):        {abst.sum():3d}  ({abst.mean():.2f})")
    print(f"   answered, graded wrong:          {(~abst & ~correct).sum():3d}  "
          f"({(~abst & ~correct).mean():.2f})")
    print(f"   answered, graded correct (leak): {(~abst & correct).sum():3d}  "
          f"({(~abst & correct).mean():.2f})")
    by_qt = df.assign(a=abst).groupby("question_type")["a"].agg(["mean", "count"])
    for qt, row in by_qt.iterrows():
        print(f"     {qt:28s} abstain={row['mean']:.2f} (n={int(row['count'])})")
