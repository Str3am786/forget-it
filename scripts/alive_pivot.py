#!/usr/bin/env python3
import pandas as pd, glob, sys
root = sys.argv[1] if len(sys.argv) > 1 else "/scratch/project_462001123/artifacts/alive"
for f in sorted([f for f in glob.glob(f"{root}/store_*.csv") if not f.endswith(".cache.csv")]):
    d = pd.read_csv(f)
    w = f.split("store_")[1].replace(".csv", "")
    print(f"\n===== {w}   answerable_store   n={d.query_id.nunique()} questions")
    piv = d.pivot_table(index="policy", columns="budget",
                        values="answerable_store", aggfunc="mean").round(3)
    print(piv.to_string())
    err = d.judge_errored.fillna(False).sum()
    if err:
        print(f"  WARNING: {err} unparseable verdicts")
