#!/usr/bin/env python3
"""Numbers for the TODOs in ch5_results.tex.

Run on elpingoso after wrongpick.py:
  python3 ch5_fill.py
"""
from pathlib import Path
import glob
import pandas as pd

OUT = Path("~/thesis_checks/wrongpick").expanduser()
ART = Path("~/lumi_backup/scratch/project_462001123/artifacts").expanduser()
PRIMARY = "qwen2.5-7b-instruct"
KEY = ["workload", "budget", "retriever"]
pd.set_option("display.width", 220)
pd.set_option("display.max_columns", 30)

r = pd.read_csv(OUT / "wrongpick_by_setting.csv")
rej = r[(r.reader == PRIMARY) & r.reject_H0]

print("1. Rejecting settings, primary reader")
print(rej[KEY + ["p_ep", "p_acc", "ep_of_p_ep", "ep_of_p_acc", "acc_of_p_ep",
                 "acc_of_p_acc", "W", "W_lo", "W_hi", "p", "mcnemar_b",
                 "mcnemar_c", "n_questions"]].to_string(index=False))

print("\n2. Same settings, every reader")
for _, s in rej.iterrows():
    m = r[(r.workload == s.workload) & (r.budget == s.budget)
          & (r.retriever == s.retriever)]
    print(f"\n{s.workload} {s.budget} {s.retriever}")
    print(m[["reader", "p_ep", "p_acc", "W", "W_lo", "W_hi", "p", "bh_sig",
             "reject_H0"]].to_string(index=False))

print("\n3. Every rejection, any reader")
print(r[r.reject_H0][["reader"] + KEY + ["p_ep", "p_acc", "W"]]
      .to_string(index=False))

print("\n4. Contradicting pairs, primary reader")
c = pd.read_csv(OUT / "contradicts_pairs.csv")
c = c[c.reader == PRIMARY]
print(c.to_string(index=False))
print(c.groupby(KEY).size().to_string())

print("\n5. Retrieval check: LongMemEval 25%, compression vs semantic_keep")
for ret in ["lexical", "bm25", "dense"]:
    for pol in ["compression", "semantic_keep"]:
        fs = glob.glob(str(ART / f"benchmark_full/longmemeval/*/retention/{pol}/"
                           f"{ret}/budget_025pct/none/results.csv"))
        if not fs:
            print(f"  missing {pol} {ret}")
            continue
        d = pd.read_csv(fs[0], low_memory=False)
        if ret == "lexical" and pol == "compression":
            print("  columns:", list(d.columns))
        cols = [x for x in d.columns if any(t in x.lower() for t in
                ("hit", "recall_at", "retained", "answerable", "rank_pct"))]
        means = d[cols].apply(pd.to_numeric, errors="coerce").mean().round(3)
        print(f"  {ret:8s} {pol:14s} n={len(d)}  " +
              "  ".join(f"{k}={v}" for k, v in means.items()))
