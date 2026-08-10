#!/usr/bin/env python3
"""
add_question_text.py — retrofit the question text onto the grader corpus.

The annotation sheet asked "does this answer answer the question" without ever
showing the question: the builder carried query_id but not question_text, which
the archive holds at column 61. This joins it back in, in place, preserving any
labels already entered.

Join key: query_id + dataset. The reader's answer to a given question is the
same row across policies/budgets only after dedup, but the QUESTION for a given
query_id is unique regardless — verified before writing, and the script refuses
to write if that assumption fails.

Usage:
    python scripts/add_question_text.py \
        ~/Desktop/results_lumi/benchmark_reader_v2/combined_results.csv \
        corpora/grader
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("results", type=Path)
    ap.add_argument("corpus_dir", type=Path)
    args = ap.parse_args()

    print("reading question_text lookup from the archive …")
    src = pd.read_csv(args.results,
                      usecols=["dataset", "query_id", "question_text"],
                      low_memory=False)
    lut = src.dropna(subset=["question_text"]).drop_duplicates(
        subset=["dataset", "query_id"])

    # One question per (dataset, query_id), or the join is wrong by design.
    dupes = src.dropna(subset=["question_text"]).groupby(
        ["dataset", "query_id"]).question_text.nunique()
    bad = dupes[dupes > 1]
    if len(bad):
        print(f"REFUSING: {len(bad)} query_ids map to multiple question texts:")
        print(bad.head(5).to_string())
        return
    lut = lut.set_index(["dataset", "query_id"]).question_text

    for name in ("annotate.csv", "corpus_full.csv"):
        path = args.corpus_dir / name
        if not path.exists():
            print(f"{name}: not found, skipping")
            continue
        df = pd.read_csv(path)
        if "question_text" in df.columns:
            print(f"{name}: already has question_text, skipping")
            continue

        df["question_text"] = [
            lut.get((d, q), "") for d, q in zip(df.dataset, df.query_id)]
        missing = int((df.question_text == "").sum())

        # Place it where the eye needs it: right before ref_answer.
        cols = list(df.columns)
        cols.remove("question_text")
        cols.insert(cols.index("ref_answer"), "question_text")
        df = df[cols]

        shutil.copy(path, path.with_suffix(".csv.bak"))
        df.to_csv(path, index=False)
        print(f"{name}: +question_text on {len(df)} rows"
              + (f" ({missing} unmatched — inspect these)" if missing else "")
              + f", backup at {path.with_suffix('.csv.bak').name}")


if __name__ == "__main__":
    main()
