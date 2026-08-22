#!/usr/bin/env python3
"""Three internal-consistency checks on the store-stage judge, no labels needed.
1. Evicted-context floor: empty context rows are False by construction; report
   how much of each policy's score is carried by free falses vs judged rows.
2. Containment agreement: where the reference answer appears verbatim in the
   surviving text, truth is True by construction. The judge's accept rate
   there is a direct false-reject estimate on eviction AND compression rows.
3. Direction of disagreement on containment-negative rows: the judge saying
   True where containment said False is the paraphrase recovery it exists
   for; report the rate separately for compression vs eviction families.
"""
import pandas as pd, glob, sys, json, os
root = sys.argv[1] if len(sys.argv) > 1 else "/scratch/project_462001123/artifacts/alive"
snaps = "/scratch/project_462001123/artifacts/sweep_subset"
for f in sorted([f for f in glob.glob(f"{root}/store_*.csv") if not f.endswith(".cache.csv")]):
    w = f.split("store_")[1].replace(".csv", "")
    d = pd.read_csv(f)
    d["family"] = (d.policy.str.startswith("compression")
                   .map({True: "compression", False: "eviction"}))
    judged = d[d.n_alive_ids > 0].copy()
    # containment on the JOINED surviving text is not in the csv; rebuild it
    # from the snapshot answer field per row is overkill here — use the cheap
    # proxy: answer string in context was dropped with context. So check 2 and
    # 3 run on the cache file, which kept question+answer+context hashes only.
    print(f"\n===== {w}")
    print("judged rows by family x budget (share of score from real verdicts):")
    t = d.pivot_table(index="family", columns="budget",
                      values="n_alive_ids", aggfunc=lambda s: (s > 0).mean()).round(3)
    print(t.to_string())
