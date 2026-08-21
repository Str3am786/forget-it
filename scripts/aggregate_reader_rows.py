#!/usr/bin/env python3
"""Collapse reader_probe.py per-call rows into the long CSV the agreement
analysis expects, normalised by each reader's own C1 (the gold arm).

C1 is the READING stage ceiling: gold items only, pre-compression
original_text, retrieval removed by construction. It is NOT the no-eviction
control, which is a RETENTION stage bound and still runs the retriever.

Decision #10: history policies form state THROUGH the ingest retriever, so
rankings group by policy_identity, never bare policy.

Two normalisations, both emitted:
  accuracy_norm  cell level. retrieved accuracy / that reader's mean gold
                 accuracy over the same questions. The headline.
  accuracy_cond  per question conditional. Retrieved accuracy restricted to
                 questions that reader answered correctly under gold.

    python scripts/aggregate_reader_rows.py artifacts/reader_figures_v2
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))
from forgetit.benchmark.metrics import coupled_identity, grid_raggedness

ADVERSARIAL = re.compile(r"adversarial|unanswerable", re.I)
REQUIRED = {"arm", "query_id", "correct", "errored", "model"}


def parse_config(cfg: str) -> dict:
    """longmemeval/turn_pair/retention/<policy>/<retriever>/<budget>/none"""
    out = {"workload": "", "itemiser": "", "policy": "", "retriever": "", "budget": ""}
    parts = [p for p in str(cfg).split("/") if p]
    if "retention" in parts:
        i = parts.index("retention")
        if i >= 2:
            out["workload"], out["itemiser"] = parts[i - 2], parts[i - 1]
        tail = parts[i + 1:]
        if len(tail) >= 3:
            out["policy"], out["retriever"], out["budget"] = tail[0], tail[1], tail[2]
    return out


def load(root: Path) -> pd.DataFrame:
    rows = []
    for dp, _d, fns in os.walk(root, followlinks=True):
        for fn in fns:
            if fn.endswith(".jsonl") and fn.startswith("reader_"):
                with (Path(dp) / fn).open(encoding="utf-8") as fh:
                    rows += [json.loads(l) for l in fh if l.strip()]
    if not rows:
        raise SystemExit(f"no reader_*.jsonl under {root}")
    missing = REQUIRED - set(rows[0])
    if missing:
        raise SystemExit(f"rows missing required fields: {sorted(missing)}\n"
                         f"fields present: {sorted(rows[0])}")
    df = pd.DataFrame(rows)
    df["reader"] = df["model"]
    if "question_type" not in df.columns:
        print("  NOTE: no question_type on the rows; stratification skipped.")
        df["question_type"] = ""
    meta = df.get("config", pd.Series([""] * len(df))).map(parse_config).apply(pd.Series)
    return pd.concat([df.drop(columns=[c for c in meta.columns if c in df.columns]),
                      meta], axis=1)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("root", type=Path)
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    df = load(args.root)
    print(f"{len(df):,} rows, {df.reader.nunique()} readers, arms={sorted(df.arm.unique())}")

    err = df.errored.mean()
    print(f"error rate {err:.4f}" + ("   <- non zero, check before trusting" if err else ""))
    df = df[~df.errored].copy()

    gold = df[df.arm == "gold"]
    qt = gold.question_type.fillna("").astype(str)
    bad = gold[qt.str.contains(ADVERSARIAL)]
    print(f"\nunanswerable/adversarial in the GOLD arm: {len(bad)} rows "
          f"({bad.query_id.nunique()} questions)")
    if len(bad):
        print("  No gold item to read, so they depress C1 for reasons unrelated to\n"
              "  reading capacity. EXCLUDED from the ceiling and every number below.")
        df = df[~df.query_id.isin(set(bad.query_id))].copy()
        gold = df[df.arm == "gold"]
    else:
        print("  none - the ceiling is clean.")

    ceil = gold.groupby("reader").correct.agg(["mean", "size"]).rename(
        columns={"mean": "C1", "size": "n"})
    print("\nC1 - reading stage ceiling, gold arm")
    print(ceil.round(4).to_string())

    if gold.question_type.nunique() > 1:
        print("\nC1 by question_type (multi hop is the joining ceiling)")
        print(gold.pivot_table(index="question_type", columns="reader",
                               values="correct", aggfunc="mean").round(3).to_string())

    ret = df[(df.arm == "retrieved") & (df.policy != "")].copy()

    ret["policy_name"] = ret["policy"]
    ret["policy"] = [coupled_identity(str(p), str(r))
                     for p, r in zip(ret.policy_name, ret.retriever.fillna(""))]
    n_coupled = int((ret.policy != ret.policy_name).sum())
    print(f"\npolicy_identity: {ret.policy.nunique()} identities from "
          f"{ret.policy_name.nunique()} names ({n_coupled:,} rows coupled)")

    key = ["reader", "workload", "itemiser", "budget", "policy", "policy_name"]
    tab = ret.groupby(key, as_index=False).agg(
        accuracy=("correct", "mean"), n=("query_id", "size"))
    tab = tab.merge(ceil.C1.rename("ceiling"), left_on="reader", right_index=True)
    tab["accuracy_norm"] = tab.accuracy / tab.ceiling.where(tab.ceiling > 0)

    ok = {(r, q) for r, q in
          gold[gold.correct][["reader", "query_id"]].itertuples(index=False)}
    ret["in_reach"] = [(r, q) in ok for r, q in
                       ret[["reader", "query_id"]].itertuples(index=False)]
    cond = (ret[ret.in_reach].groupby(key, as_index=False)
            .agg(accuracy_cond=("correct", "mean"), n_cond=("query_id", "size")))
    tab = tab.merge(cond, on=key, how="left")

    cells = {}
    for _, r in tab.iterrows():
        cells.setdefault(r.policy, set()).add((r.reader, r.budget))
    ragged = list(grid_raggedness(cells))
    if ragged:
        print("\nWARNING: unequal support. A policy missing for one reader is\n"
              "  dropped from that workload's concordance entirely.")
        for line in ragged:
            print(f"  {line}")
    else:
        print("\ngrid complete: every policy_identity covers every (reader, budget).")

    # agreement pivots reader x policy within a workload, so budget must be in
    # the workload key or the three budgets collapse into one ranking.
    tab["workload"] = tab.workload + "_" + tab.itemiser + "_" + tab.budget

    over = tab[tab.accuracy_norm > 1.0]
    if len(over):
        print(f"\nNOTE: {len(over)} cell(s) exceed their ceiling. Not clipped; at "
              "this n it is\n  sampling noise, but check whether it persists.")

    out = args.out or args.root / "summary.csv"
    tab.to_csv(out, index=False)
    print(f"\n{len(tab):,} cells -> {out}")
    print("\nmean accuracy_norm by reader")
    print(tab.groupby("reader").accuracy_norm.agg(["mean", "min", "max"]).round(3).to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
