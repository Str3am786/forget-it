#!/usr/bin/env python3
"""
analyze_grader_corpus.py — score the grader against the human labels, and say
which fixes would pay.

DISCIPLINE. The labelled rows are split dev/test by a hash of row_key before
anything is computed. Everything this script prints by default comes from the
DEV half only: error rates, the threshold curve, the taxonomy. The TEST half is
scored once, at the end of the work, via --final — after any grader edits are
frozen — so the reported score is not selection on our own tuning. Running
--final early spends the held-out set; the script says so loudly.

OUTPUTS (dev):
  1. Grader-vs-human confusion per tier, with Wilson intervals: which rungs of
     grade() err, in which direction, how often.
  2. Threshold sweep over rows with a similarity score: human-labelled accepts
     and rejects as the embedding threshold moves; the operating point that
     balances them, versus the a-priori 0.83.
  3. The note taxonomy: free-text notes clustered into canonical classes; the
     mapping is PRINTED FOR APPROVAL, not silently applied.
  4. The reweighted bound: per-tier error rates weighted by the archive's true
     tier sizes -> how much of the campaign's qa_correct could be grader error.

Usage:
    python scripts/analyze_grader_corpus.py corpora/grader          # dev only
    python scripts/analyze_grader_corpus.py corpora/grader --final  # test, once
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from pathlib import Path

import pandas as pd

# The FIXED grader, imported live from the package — this script re-grades the
# corpus with whatever grade() currently is, so the report always reflects the
# working tree rather than a snapshot taken when the labels were made.
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from forgetit.benchmark.grading import (  # noqa: E402
    grade, _numeric_veto, _entity_veto, _quoted_span_veto,
)

# Archive tier sizes after the ungradeable exclusion (builder output, this run).
ARCHIVE_TIERS = {"miss": 248_639, "embedding": 43_611, "contains": 34_856,
                 "abstain": 2_170, "exact": 428}

# Free-text note -> canonical class. First match wins; unmatched -> commentary.
NOTE_RULES = [
    ("substitution_full_list", r"full[_ ]list"),
    ("partial_plus_substitution", r"partial[_ ]plus|plus[_ ]substitution"),
    ("substitution", r"substitut|confabulat|wrong[_ ]conversation|wrong[_ ]segment"),
    ("parametric_fallback", r"parametric"),
    ("near_miss_numeric", r"numeric|number.*miss|near.*miss"),
    ("partial", r"partial|half|not full|lacks|missing|incomplete"),
    ("wording_only", r"wording|paraphrase"),
    ("contradiction_flattened", r"contradict|flatten"),
    ("denied_memory", r"denied|denial"),
    ("caption_leak", r"caption"),
    ("language_drift", r"language|chinese|drift"),
    ("right_verdict_wrong_reason", r"wrong[_ ]reason|verdict"),
    ("memory_returned_as_question", r"as[_ ]question|counter.?question|clarif"),
]


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return (float("nan"), float("nan"))
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, c - h), min(1.0, c + h))


def canon_note(note: str) -> str:
    s = str(note).strip().lower()
    if not s or s == "nan":
        return ""
    for name, pat in NOTE_RULES:
        if re.search(pat, s):
            return name
    return "commentary"


def _fill_keys(df: pd.DataFrame) -> pd.DataFrame:
    """The unanswerable stratum was set aside before row_key existed in the
    builder, so those rows carry NaN keys in BOTH files. Recompute from the
    answer texts — same rows, same recipe, restores the join."""
    nan = df.row_key.isna()
    if nan.any():
        df = df.copy()
        # query_id folded in: the unans stratum was never deduped, so several
        # rows share one abstention string against an empty reference.
        df.loc[nan, "row_key"] = (
            df.loc[nan, "reader_answer"].fillna("") + "\x1f"
            + df.loc[nan, "ref_answer"].fillna("") + "\x1f"
            + df.loc[nan, "query_id"].fillna("")
        ).map(lambda t: "u" + hashlib.sha256(t.encode()).hexdigest()[:15])
    return df


def load(corpus_dir: Path) -> pd.DataFrame:
    ann = _fill_keys(pd.read_csv(corpus_dir / "annotate.csv"))
    full = _fill_keys(pd.read_csv(corpus_dir / "corpus_full.csv"))
    right = (full[["row_key", "tier", "similarity", "qa_correct",
                    "regraded_correct", "abstained"]]
             .drop_duplicates(subset="row_key"))
    df = ann.merge(right, on="row_key", how="left", validate="one_to_one")
    # Spreadsheet round-trips rewrite whitespace/quoting in the answer text,
    # so recomputed hashes can diverge between the two files. Second pass for
    # the stragglers: join on query_id, which survives editing untouched.
    unjoined = df.tier.isna()
    if unjoined.any():
        fb = (full.drop_duplicates(subset="query_id")
                  .set_index("query_id")[["tier", "similarity", "qa_correct",
                                          "regraded_correct", "abstained"]])
        for i in df.index[unjoined]:
            q = df.at[i, "query_id"]
            if q in fb.index:
                for c in fb.columns:
                    df.at[i, c] = fb.at[q, c]
        unjoined = df.tier.isna()
    if unjoined.any():
        print(f"WARNING: {int(unjoined.sum())} rows failed to join a verdict "
              f"and are dropped: {df.loc[unjoined, 'corpus_id'].tolist()}")
        df = df[~unjoined]

    lab = df.human_correct.astype(str).str.strip().str.lower()
    unlabelled = lab.isin(("", "nan"))
    if unlabelled.any():
        ids = df.loc[unlabelled, "corpus_id"].tolist()
        print(f"WARNING: {unlabelled.sum()} unlabelled rows dropped: {ids}")
        df = df[~unlabelled]
        lab = lab[~unlabelled]
    df["human"] = lab.eq("true")
    df["grader"] = df.qa_correct.astype(str).str.strip().str.lower().eq("true")

    # Re-grade with the CURRENT grade(). Deterministic tiers run live; the
    # embedding rung is replayed from the stored similarity (no embedder needed,
    # and the archived number is the same one the runtime would compute) but is
    # now subject to the same vetoes as every other non-verbatim path.
    new_ok, new_tier = [], []
    for r in df.itertuples():
        a, rf = str(r.reader_answer or ""), str(r.ref_answer or "")
        v = grade(a, rf, abstained=bool(r.abstained))
        ok, t = v["correct"], v["tier"]
        if (not ok and pd.notna(r.similarity)
                and float(r.similarity) >= 0.83
                and not _numeric_veto(a, rf) and not _entity_veto(a, rf)
                and not _quoted_span_veto(a, rf)):
            ok, t = True, "embedding"
        new_ok.append(ok)
        new_tier.append(t)
    df["fixed"] = new_ok
    df["fixed_tier"] = new_tier
    df["note_class"] = df.human_note.map(canon_note)
    # Stable split: hash, not order, so relabelling or re-sorting never moves
    # a row between halves.
    df["split"] = df.row_key.map(
        lambda k: "dev" if int(hashlib.sha256(str(k).encode()).hexdigest(), 16) % 2
        else "test")
    return df


def old_vs_fixed(d: pd.DataFrame, title: str) -> None:
    """Both graders against the human labels, and every row the fix moved."""
    print(f"\n{'=' * 74}\nARCHIVED vs FIXED grader — {title}\n{'=' * 74}")
    rows = []
    for name, col in (("archived", "grader"), ("fixed", "fixed")):
        fp = int((d[col] & ~d.human).sum())
        fn = int((~d[col] & d.human).sum())
        agree = int((d[col] == d.human).sum())
        lo, hi = wilson(fp + fn, len(d))
        rows.append(dict(grader=name, n=len(d), agree=agree, false_pos=fp,
                         false_neg=fn, err_rate=(fp + fn) / len(d),
                         ci_lo=lo, ci_hi=hi))
    print(pd.DataFrame(rows).set_index("grader").round(3).to_string())

    flips = d[d.grader != d.fixed]
    print(f"\nrows moved by the fix: {len(flips)}")
    if len(flips):
        good = int((flips.fixed == flips.human).sum())
        print(f"  {good} toward the human label, {len(flips) - good} away\n")
        print(flips[["corpus_id", "tier", "fixed_tier", "human", "grader",
                     "fixed"]].rename(columns={"tier": "old_tier",
                                               "grader": "old"})
              .to_string(index=False))

    print("\nfixed-grader tier distribution:")
    print(d.fixed_tier.value_counts().to_string())

    still = d[(~d.fixed) & d.human]
    if len(still):
        print(f"\n{len(still)} rows the fix still rejects but you accepted "
              f"(the deterministic ceiling):")
        for _, r in still.iterrows():
            print(f"  {r.corpus_id}  ref: {str(r.ref_answer)[:70]}")


def confusion_by_tier(d: pd.DataFrame, title: str) -> pd.DataFrame:
    print(f"\n{'=' * 74}\nGRADER vs HUMAN by tier — {title}\n{'=' * 74}")
    rows = []
    for tier, g in d.groupby("tier"):
        n = len(g)
        agree = int((g.grader == g.human).sum())
        fp = int((g.grader & ~g.human).sum())    # grader accepted, human rejected
        fn = int((~g.grader & g.human).sum())    # grader rejected, human accepted
        err = fp + fn
        lo, hi = wilson(err, n)
        rows.append(dict(tier=tier, n=n, agree=agree, false_pos=fp, false_neg=fn,
                         err_rate=err / n, ci_lo=lo, ci_hi=hi))
    out = pd.DataFrame(rows).set_index("tier").sort_values("n", ascending=False)
    print(out.round(3).to_string())
    print("\n  false_pos = grader said correct, you said wrong "
          "(contains-mention, flattened contradictions…)")
    print("  false_neg = grader said wrong, you said correct "
          "(unpaid paraphrase, quote-vs-gloss…)")
    return out


def threshold_curve(d: pd.DataFrame) -> None:
    sim = d[d.similarity.notna() & (d.tier.isin(("embedding", "miss")))]
    if len(sim) < 8:
        print(f"\nthreshold sweep skipped: only {len(sim)} rows carry a "
              f"similarity score")
        return
    print(f"\n{'=' * 74}\nTHRESHOLD SWEEP over {len(sim)} similarity-scored rows"
          f"\n{'=' * 74}")
    print("accept iff similarity >= t; scored against YOUR labels\n")
    rows = []
    for t in [0.70, 0.75, 0.78, 0.80, 0.83, 0.86, 0.89, 0.92]:
        acc = sim.similarity >= t
        tp = int((acc & sim.human).sum())
        fp = int((acc & ~sim.human).sum())
        fn = int((~acc & sim.human).sum())
        tn = int((~acc & ~sim.human).sum())
        rows.append(dict(threshold=t, accept=int(acc.sum()),
                         true_accepts=tp, false_accepts=fp,
                         missed_correct=fn, true_rejects=tn,
                         accuracy=(tp + tn) / len(sim)))
    tab = pd.DataFrame(rows).set_index("threshold")
    print(tab.to_string())
    best = tab.accuracy.idxmax()
    print(f"\n  best accuracy at t={best} "
          f"({tab.loc[best, 'accuracy']:.3f}) vs a-priori 0.83 "
          f"({tab.loc[0.83, 'accuracy']:.3f})")
    print("  Small n: read as direction, not decimal. The compositional "
          "failures\n  (list substitution, near-miss numerics) do not move "
          "with t at all — those\n  rows stay wrong at every threshold, which "
          "is the ceiling on what sweeping buys.")


def taxonomy(d: pd.DataFrame) -> None:
    print(f"\n{'=' * 74}\nNOTE TAXONOMY (mapping below — approve before "
          f"citing counts)\n{'=' * 74}")
    tab = (d[d.note_class != ""]
           .groupby(["note_class", "human"]).size().unstack(fill_value=0))
    tab.columns = ["human_false", "human_true"][: len(tab.columns)]
    print(tab.sort_values(tab.columns.tolist()[0], ascending=False).to_string())
    print("\nraw note -> class (spot-check this mapping):")
    pairs = {(str(r), str(c)) for r, c in zip(d.human_note, d.note_class)}
    for raw, cls in sorted(pairs):
        if cls and cls != "nan" and str(raw).strip().lower() not in ("", "nan"):
            print(f"  {cls:<28} <- {raw[:70]}")


def reweighted_bound(conf: pd.DataFrame) -> None:
    print(f"\n{'=' * 74}\nREWEIGHTED BOUND on grader-attributable error in the "
          f"archive\n{'=' * 74}")
    total = sum(ARCHIVE_TIERS.values())
    mid = up = 0.0
    for tier, w in ARCHIVE_TIERS.items():
        if tier in conf.index:
            r = conf.loc[tier]
            mid += (w / total) * r.err_rate
            up += (w / total) * r.ci_hi
        else:
            up += (w / total) * 1.0
            print(f"  {tier}: no labelled rows — worst-cased at 100% in the "
                  f"upper bound")
    print(f"\n  point estimate: {mid:.1%} of archived verdicts grader-erroneous")
    print(f"  95% upper bound: {up:.1%}")
    print("  Weights are the post-exclusion archive tier sizes; the sample was "
          "stratified,\n  so per-tier rates reweighted by true sizes is the "
          "defensible aggregate.")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("corpus_dir", type=Path)
    ap.add_argument("--final", action="store_true",
                    help="score the held-out TEST half. Do this ONCE, after "
                         "grader edits are frozen.")
    args = ap.parse_args()

    df = load(args.corpus_dir)
    dev, test = df[df.split == "dev"], df[df.split == "test"]
    print(f"{len(df)} labelled rows -> dev {len(dev)} / test {len(test)} "
          f"(hash split, stable)")

    half = test if args.final else dev
    title = "TEST (held out — cite these once)" if args.final else "DEV"
    if args.final:
        print("\n*** FINAL MODE: you are spending the held-out half. If grader "
              "edits are\n*** not frozen yet, ctrl-C now.")

    old_vs_fixed(half, title)
    conf = confusion_by_tier(half, title)
    threshold_curve(half)
    taxonomy(half)
    reweighted_bound(conf)

    overall = int((half.grader != half.human).sum())
    lo, hi = wilson(overall, len(half))
    print(f"\nOVERALL ({title}): grader disagrees with human on "
          f"{overall}/{len(half)} rows ({overall / len(half):.1%}, "
          f"95% CI {lo:.1%}–{hi:.1%})")


if __name__ == "__main__":
    main()