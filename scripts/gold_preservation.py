#!/usr/bin/env python3
"""
gold_preservation.py — identifier retention against answer preservation.

`gold_survived` is set intersection on ids: the gold item is still resident.
It says nothing about whether the retained TEXT still supports the answer, so a
policy that rewrites items in place scores perfectly by construction. This
splits the two:

  gold_id_retained      the gold id is in the resident set  (results.csv)
  gold_answer_preserved the retained text still contains the answer (snapshot)

For policies that never rewrite text the two must agree exactly — that equality
is the validation. The gap for `compression` is the finding.

Grading is deterministic: the benchmark's own `grade()` when importable, else
normalised containment of the reference answer in the retained text. No LLM in
the primary path; the paired corpus validates this measure, it does not replace
it.

Usage:
    python gold_preservation.py artifacts/smoke4/combined
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

try:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
    from forgetit.benchmark.grading import grade as _grade

    def preserved(text: str, answer: str) -> bool:
        if not text or not answer:
            return False
        return bool(_grade(str(text), str(answer), abstained=False)["correct"])
    GRADER = "forgetit.benchmark.grading.grade"
except Exception:
    _NORM = re.compile(r"[^a-z0-9 ]+")

    def _n(s: str) -> str:
        return " ".join(_NORM.sub(" ", str(s).lower()).split())

    def preserved(text: str, answer: str) -> bool:
        if not text or not answer:
            return False
        return _n(answer) in _n(text)
    GRADER = "normalised containment (fallback)"


def _frac(tag) -> float:
    """'025pct' -> 0.25; a raw byte count -> 0.0 (fixed-budget mode)."""
    s = str(tag)
    if s.endswith("pct"):
        try:
            return int(s[:-3]) / 100
        except ValueError:
            return float("nan")
    return 0.0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("combined", type=Path, help="dir holding the combined CSVs")
    ap.add_argument("--out", type=Path, default=None, help="write the joined table here")
    a = ap.parse_args()

    snap = pd.read_csv(a.combined / "combined_gold_snapshot.csv", low_memory=False)
    res = pd.read_csv(a.combined / "combined_results.csv", low_memory=False)
    print(f"grader: {GRADER}")
    print(f"{len(snap):,} snapshot rows, {len(res):,} result rows\n")

    snap["budget_frac"] = snap["budget_bytes"].map(_frac)
    snap["was_evicted"] = snap["was_evicted"].astype(str).str.lower().eq("true")
    snap["was_compressed"] = snap["was_compressed"].astype(str).str.lower().eq("true")

    # ── capture validity ──────────────────────────────────────────────
    # A policy that only evicts cannot alter text. Any retained row where
    # R_g != O_g means the snapshot captured the wrong state, which would make
    # the compression numbers meaningless too.
    ev = snap[~snap.was_evicted & ~snap.was_compressed]
    mismatch = ev[ev.resident_text.fillna("") != ev.original_text.fillna("")]
    print(f"capture check: {len(mismatch)} retained rows changed text without "
          f"compressing (want 0)")
    if len(mismatch):
        print(mismatch[["policy", "example_id", "gold_id"]].head(10).to_string(index=False))
        print("\nSTOP — the snapshot is not reading final store state correctly.")
        return

    # ── grade O_g and R_g ─────────────────────────────────────────────
    # Containment of a short reference answer in a passage measures
    # EXTRACTIVENESS, not preservation: most LoCoMo evidence supports its
    # answer without containing it as a string. Scoring R_g alone therefore
    # says nothing — an untouched copy of the original scores the same as a
    # husk. Grade the ORIGINAL too and use it as the denominator: on the
    # subset where O_g contains the answer, grade(R_g) is a valid test of
    # whether the rewrite destroyed it, and a policy that never edits text
    # scores preserved == retained by construction.
    snap["orig_supports"] = [preserved(o, ans)
                             for o, ans in zip(snap.original_text, snap.answer)]
    snap["res_supports"] = [
        False if ev_ else preserved(t, ans)
        for ev_, t, ans in zip(snap.was_evicted, snap.resident_text, snap.answer)
    ]

    cover = snap.groupby(["dataset", "itemiser"]).orig_supports.agg(["mean", "size"])
    print("extractive share — gold items whose ORIGINAL text contains the answer:")
    print(cover.round(3).to_string())
    print("  Rows outside this subset are out of scope for a string measure;\n"
          "  the paired preservation corpus is what covers them.\n")

    # gold_id_retained is "ANY gold item survived", so collapse to the query the
    # same way before comparing, or the two columns aren't measuring the same
    # event on multi-gold questions. Scope to extractive gold items only.
    key = ["dataset", "itemiser", "policy", "budget_frac", "query_id"]
    ext = snap[snap.orig_supports]
    perq = ext.groupby(key, as_index=False).agg(
        gold_answer_preserved=("res_supports", "max"),
        # Retention scoped to the SAME gold items as preservation. results.csv
        # reports "any gold item survived" over the whole gold set, so on a
        # multi-gold question a surviving non-extractive item makes it True
        # while the extractive one was evicted — a scope mismatch that looks
        # like a preservation failure and is not one.
        gold_id_retained=("was_evicted", lambda s: bool((~s).max())),
        gold_items=("gold_id", "nunique"),
        any_compressed=("was_compressed", "max"),
    )

    # Whole-gold-set retention, kept for contrast: this is the column the
    # results table reports, and the gap between the two scopes is itself worth
    # seeing on multi-gold questions.
    ret_col = "gold_id_retained" if "gold_id_retained" in res.columns else "gold_survived"
    res["gold_id_retained_anygold"] = res[ret_col].astype(str).str.lower().eq("true")
    rkey = res.groupby(key, as_index=False).gold_id_retained_anygold.max()

    j = perq.merge(rkey, on=key, how="inner")
    print(f"joined {len(j):,} (policy, budget, question) rows on the "
          f"extractive subset\n")

    tab = (j.groupby(["itemiser", "budget_frac", "policy"])
             .agg(retained=("gold_id_retained", "mean"),
                  preserved=("gold_answer_preserved", "mean"),
                  n=("query_id", "size")))
    tab["gap"] = tab.retained - tab.preserved
    print(tab.round(3).to_string())

    # ── the two readings ──────────────────────────────────────────────
    print("\nnon-compressing policies: retained and preserved must be EQUAL "
          "(R_g == O_g)")
    nc = j[(j.any_compressed == 0) & (j.policy != "compression")]
    if len(nc):
        d = (nc.groupby("policy")
               .apply(lambda g: (g.gold_id_retained != g.gold_answer_preserved).mean(),
                      include_groups=False)
               .round(4))
        print(d.to_string())
        if float(d.max()) > 0.01:
            print("\n  STOP. These policies never rewrite text, so on the "
                  "extractive subset\n  preservation must equal retention "
                  "exactly. A disagreement here is a\n  bug in the capture or "
                  "the join, not a fact about compression.")
        else:
            print("\n  Equal, as required. The compression gap below is "
                  "measuring the rewrite.")

    print("\ncompression: the gap is the amount of 'survival' that is husk")
    cm = j[j.policy == "compression"]
    if len(cm):
        print(cm.groupby("budget_frac")
                .agg(retained=("gold_id_retained", "mean"),
                     preserved=("gold_answer_preserved", "mean"),
                     n=("query_id", "size")).round(3).to_string())

    if a.out:
        a.out.parent.mkdir(parents=True, exist_ok=True)
        j.to_csv(a.out, index=False)
        print(f"\nwrote {a.out}")


if __name__ == "__main__":
    main()