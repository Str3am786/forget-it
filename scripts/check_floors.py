#!/usr/bin/env python3
"""Diagnose the reference conditions (empty, parametric, gold) across the
three retriever runs, on the backed-up judged results.

Answers three questions:
  1. Floors move across retriever runs although they use no retriever.
     Is the movement from the reader (different answer text) or from the
     judge (same answer, different verdict)?
  2. Some readers score high on the parametric condition (gemma on LoCoMo,
     qwen2.5-1.5b on LongMemEval). What do the accepted answers look like?
  3. Does the gold condition depend on the retriever? (different number of
     delivered items, truncation or answer text across runs)

The verdict used here is the PRE-GUARD verdict: the judge's verdict where
the judge was called, otherwise the deterministic one. The two guards can
only lower it, and they are applied in rq_tables.py, not here. Counts of
movement are therefore an upper bound on what the guards could change.

Usage:
  python3 check_floors.py [ARTIFACTS_DIR] [OUT_DIR]
Defaults:
  ARTIFACTS_DIR = ~/lumi_backup/scratch/project_462001123/artifacts
  OUT_DIR       = ~/thesis_checks/floors
"""
import sys
from pathlib import Path

import pandas as pd

ART = Path(sys.argv[1] if len(sys.argv) > 1 else
           "~/lumi_backup/scratch/project_462001123/artifacts").expanduser()
OUT = Path(sys.argv[2] if len(sys.argv) > 2 else
           "~/thesis_checks/floors").expanduser()
OUT.mkdir(parents=True, exist_ok=True)

RETRIEVERS = ["lexical", "bm25", "dense"]
ARMS = ["empty", "parametric", "gold"]
KEEP = ["query_id", "arm", "dataset", "question_type", "question",
        "ref_answer", "reader_answer", "tier", "correct", "judge_correct",
        "n_delivered", "n_offered", "truncated", "replay_prompt_sha",
        "render_sha", "model"]


def load(retriever):
    files = sorted((ART / f"judged5_{retriever}").glob("judged_*.csv"))
    if not files:
        sys.exit(f"No judged files in {ART / f'judged5_{retriever}'}")
    frames = []
    for f in files:
        workload = f.stem[len("judged_"):].rsplit("_", 1)[0]
        d = pd.read_csv(f, usecols=lambda c: c in KEEP, low_memory=False)
        d = d[d["arm"].isin(ARMS)].copy()
        d["workload"] = workload
        d["retriever"] = retriever
        frames.append(d)
    return pd.concat(frames, ignore_index=True)


df = pd.concat([load(r) for r in RETRIEVERS], ignore_index=True)

# Gradeable rows only, as in the thesis denominators.
df = df[df["ref_answer"].notna() & (df["ref_answer"].astype(str).str.strip() != "")]

# Pre-guard verdict: judge where called, else deterministic.
df["verdict"] = df["judge_correct"].where(df["judge_correct"].notna(),
                                          df["correct"]).astype(bool)

# Sanity: workload names parsed correctly, and one row per question per run.
dups = df.duplicated(["retriever", "workload", "model", "arm", "query_id"]).sum()
print(f"Rows loaded: {len(df):,}   duplicate keys: {dups}")
print("Workloads:", sorted(df["workload"].unique()))
print("Models:   ", sorted(df["model"].unique()))
print()

# ---------------------------------------------------------------- 1 and 3
wide = df.pivot_table(
    index=["workload", "model", "arm", "query_id"],
    columns="retriever",
    values=["reader_answer", "verdict", "n_delivered", "truncated"],
    aggfunc="first",
)

rows = []
for (workload, model, arm), g in wide.groupby(level=[0, 1, 2]):
    ans = g["reader_answer"].reindex(columns=RETRIEVERS).astype(str)
    ver = g["verdict"].reindex(columns=RETRIEVERS)
    dlv = g["n_delivered"].reindex(columns=RETRIEVERS)
    trc = g["truncated"].reindex(columns=RETRIEVERS)

    complete = ans.notna().all(axis=1) & ver.notna().all(axis=1)
    ans, ver, dlv, trc = ans[complete], ver[complete], dlv[complete], trc[complete]

    same_ans = ans.nunique(axis=1) == 1
    diff_ver = ver.astype(bool).nunique(axis=1) > 1
    diff_dlv = dlv.nunique(axis=1) > 1
    diff_trc = trc.astype(bool).nunique(axis=1) > 1
    acc = ver.astype(bool).mean()

    rows.append({
        "workload": workload, "model": model, "arm": arm,
        "n": int(complete.sum()),
        "acc_lexical": round(acc["lexical"], 4),
        "acc_bm25": round(acc["bm25"], 4),
        "acc_dense": round(acc["dense"], 4),
        "spread": round(acc.max() - acc.min(), 4),
        "answer_text_differs": int((~same_ans).sum()),
        "verdict_differs_same_answer": int((same_ans & diff_ver).sum()),
        "verdict_differs_diff_answer": int((~same_ans & diff_ver).sum()),
        "n_delivered_differs": int(diff_dlv.sum()),
        "truncation_differs": int(diff_trc.sum()),
    })

summary = pd.DataFrame(rows).sort_values(["arm", "workload", "model"])
summary.to_csv(OUT / "reference_conditions_across_retrievers.csv", index=False)
pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)
print("1 and 3. Movement of each reference condition across retriever runs")
print("   verdict_differs_same_answer > 0  -> the judge is not repeatable")
print("   answer_text_differs > 0          -> the reader is not repeatable")
print("   n_delivered_differs > 0 (gold)   -> gold depends on the retriever")
print(summary.to_string(index=False))
print()

# ---------------------------------------------------------------- 2
print("2. Accepted parametric answers (no evidence given)")
par = df[(df["arm"] == "parametric") & (df["retriever"] == "lexical")]
cases = [("locomo_turn_pair", "gemma-3-12b-it"),
         ("longmemeval_turn_pair", "qwen2.5-1.5b-instruct"),
         ("locomo_turn_pair", "qwen2.5-7b-instruct"),
         ("longmemeval_turn_pair", "qwen2.5-7b-instruct")]
for workload, model in cases:
    s = par[(par["workload"] == workload) & (par["model"] == model)]
    acc = s[s["verdict"]]
    print(f"\n{workload} / {model}: {len(acc)} of {len(s)} accepted "
          f"({len(acc) / max(len(s), 1):.3f})")
    print("  by tier:          ", acc["tier"].value_counts().to_dict())
    print("  by question type: ", acc["question_type"].value_counts().to_dict())
    out = acc.sample(min(30, len(acc)), random_state=0)[
        ["query_id", "question_type", "tier", "question", "ref_answer",
         "reader_answer"]]
    name = f"parametric_accepted_{workload}_{model}.csv"
    out.to_csv(OUT / name, index=False)
    print(f"  sample of {len(out)} written to {OUT / name}")

# ---------------------------------------------------------------- resident
print("\nResident (retrieverless) run, for comparison with gold:")
res_dirs = sorted(ART.glob("judged_resident_*"))
if not res_dirs:
    print("  no judged_resident_* folders found")
else:
    f = sorted(res_dirs[0].glob("judged_*.csv"))[0]
    d = pd.read_csv(f, nrows=2000, low_memory=False)
    print(f"  {f}: arms = {sorted(d['arm'].dropna().unique()) if 'arm' in d else 'no arm column'}")

print(f"\nAll outputs in {OUT}")
