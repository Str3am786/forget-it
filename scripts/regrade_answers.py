#!/usr/bin/env python3
"""
Offline re-grade of a results CSV — the two grading corrections that must be
applied AFTER the fact, identically to v1 and v2 output, with no re-run and no
change to runtime grading.

Runtime grading is deliberately untouched: every ``qa_correct`` in the CSV keeps
the semantics it was written with, so v1 and v2 rows stay comparable on their
original terms. This script only ADDS columns.

────────────────────────────────────────────────────────────────────────────
RULE 1 (G-6) — exact-mode abstention RE-DETECTION, applied to BOTH v1 and v2.
    ``_is_abstention(mode="loose")`` — v1's setting — flags an answer that
    merely CONTAINS a refusal phrase, so "IDK if there were others, but the
    rent was $2,100" was recorded as a refusal and scored as no answer. v2 runs
    ``exact`` (the whole answer must BE a refusal). Comparing a v1 rate against
    a v2 rate therefore compares two different measurements. Re-detecting in
    ``exact`` mode over the stored ``reader_answer`` puts both on the v2
    definition — the comparability rule — and is the ONLY reason the G-2 rule
    below can be described as "applied identically" to the two vintages.

RULE 2 (G-2) — the LongMemEval abstention-type rule: correct iff abstained.
    ``grade()`` short-circuits to correct=False whenever abstained=True, so a
    CORRECT refusal on an unanswerable ``_abs`` question scores wrong. On
    ``data/longmemeval_s_cleaned.json`` that is exactly 30 of 500 questions
    (all via the ``_abs`` suffix). ``grade_with_abstention`` flips that one
    cell and nothing else.

ORDER MATTERS, and it is the resolution of contradiction X-7 in
verification/reports/04_grading_metrics/REPORT.md. X-7 observes that G-2 and
G-6 disagree on 2 of 4 ``_abs`` answer shapes: applied to v1's LOOSE
``abstained`` column, the G-2 rule credits a HEDGED HALLUCINATION ("IDK, but
you mentioned owning a golden retriever") as a correct refusal. Rule 1 is
therefore applied FIRST and Rule 2 consumes its output, never the stored
column. The v1 number that comes out is a real refusal rate; the raw-column
number ("29/30 on v1") is an upper bound of the same family as the 69.5%
figure, and this script reports both so the gap is visible rather than
asserted.

TWO SCOPE LIMITS, both structural — measured in the harness, restated here:

  1. v1 persisted the CLEANED answer (``_clean_answer(raw, mode="firstline")``),
     so for a MULTI-LINE generation everything after the first newline was
     destroyed BEFORE storage. Re-detection recovers SINGLE-LINE hedges only;
     multi-line hedges are unrecoverable from the CSV at any later date. The
     ``*_multiline_unrecoverable`` note in the report is that population.

  2. Rows whose ``reader_answer`` starts with ``[LLM error:`` were stamped
     abstained=True by the ERROR path without ``_is_abstention`` ever running.
     A naive recount flips them to not-abstained and silently promotes
     infrastructure failures into the answered pool. They are EXCLUDED here and
     counted separately. Any analysis adopting this rule must do the same.

    # augment a results CSV in place (writes a sibling *_regraded.csv):
    python scripts/regrade_answers.py --csv artifacts/benchmark_reader_v2/combined_results.csv
    # method demonstration on constructed rows (no results tree needed):
    python scripts/regrade_answers.py --demo
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from forgetit.benchmark.grading import (  # noqa: E402
    grade, grade_with_abstention, is_answerable,
)
from forgetit.llm.readers.text_reader import (  # noqa: E402
    _clean_answer, _is_abstention,
)

ERROR_PREFIX = "[LLM error:"

# Columns this script ADDS. Originals are never overwritten.
ADDED_COLUMNS = [
    "abstained_exact",            # RULE 1 output: abstention under exact mode
    "abstention_redetect",        # unchanged | flipped | llm_error_excluded
    "answerable",                 # is_answerable(query_id, question_type)
    "qa_correct_regraded",        # RULE 1 + RULE 2 applied
    "qa_f1_regraded",
    "regrade_tier",               # incl. abstain_correct / abstain_missed
    "regrade_delta",              # same | recovered | lost
]


def _truthy(v) -> bool:
    return str(v).strip().lower() in ("true", "1", "yes")


def redetect(row: dict) -> tuple[bool, str]:
    """RULE 1. Returns (abstained_exact, status).

    The stored ``abstained`` is used unchanged for [LLM error:] rows — the
    error path set it without consulting the detector, so re-detection has no
    opinion on them and must not manufacture one.
    """
    answer = row.get("reader_answer") or ""
    was = _truthy(row.get("abstained", ""))
    if answer.startswith(ERROR_PREFIX):
        return was, "llm_error_excluded"
    now = _is_abstention(answer, mode="exact")
    return now, ("flipped" if now != was else "unchanged")


def regrade_row(row: dict) -> dict:
    """RULE 1 then RULE 2 for one row. Returns the ADDED columns only."""
    abstained_exact, status = redetect(row)
    answerable = is_answerable(row.get("query_id", ""), row.get("question_type", ""))
    g = grade_with_abstention(
        row.get("reader_answer") or "",
        row.get("ref_answer") or "",
        abstained=abstained_exact,
        answerable=answerable,
    )
    was_correct = _truthy(row.get("qa_correct", ""))
    if g["correct"] == was_correct:
        delta = "same"
    else:
        delta = "recovered" if g["correct"] else "lost"
    return {
        "abstained_exact": str(abstained_exact),
        "abstention_redetect": status,
        "answerable": str(answerable),
        "qa_correct_regraded": str(g["correct"]),
        "qa_f1_regraded": f"{g['f1']:.6f}",
        "regrade_tier": g["tier"],
        "regrade_delta": delta,
    }


def regrade(rows: list[dict]) -> tuple[list[dict], dict]:
    """Apply both rules to every row; return (added-column list, summary)."""
    added: list[dict] = []
    c: Counter = Counter()
    for row in rows:
        a = regrade_row(row)
        added.append(a)
        c["rows"] += 1
        c[f"redetect_{a['abstention_redetect']}"] += 1
        c[f"delta_{a['regrade_delta']}"] += 1
        if a["answerable"] == "False":
            c["unanswerable_rows"] += 1
            if a["qa_correct_regraded"] == "True":
                c["unanswerable_correct_after"] += 1
            if _truthy(row.get("qa_correct", "")):
                c["unanswerable_correct_before"] += 1
            # what the G-2 rule alone would have said, on the RAW loose column
            if _truthy(row.get("abstained", "")):
                c["unanswerable_correct_g2_on_raw_column"] += 1
    summary = {
        "n_rows": c["rows"],
        "redetect": {
            "unchanged": c["redetect_unchanged"],
            "flipped": c["redetect_flipped"],
            "llm_error_excluded": c["redetect_llm_error_excluded"],
        },
        "verdict_delta": {
            "same": c["delta_same"],
            "recovered": c["delta_recovered"],
            "lost": c["delta_lost"],
        },
        "abstention_class": {
            "n_unanswerable": c["unanswerable_rows"],
            "correct_before": c["unanswerable_correct_before"],
            "correct_after_rule1_then_rule2": c["unanswerable_correct_after"],
            "correct_if_g2_applied_to_raw_loose_column": c["unanswerable_correct_g2_on_raw_column"],
        },
    }
    return added, summary


def report(summary: dict, label: str) -> None:
    s = summary
    print(f"\n=== offline re-grade: {label} ===")
    print(f"rows                                   {s['n_rows']}")
    r = s["redetect"]
    print("RULE 1 — exact-mode abstention re-detection")
    print(f"  unchanged                            {r['unchanged']}")
    print(f"  FLIPPED (loose false positives)      {r['flipped']}")
    print(f"  [LLM error:] excluded                {r['llm_error_excluded']}")
    a = s["abstention_class"]
    print("RULE 2 — LME abstention-type rule (correct iff abstained)")
    print(f"  unanswerable rows                    {a['n_unanswerable']}")
    print(f"  correct BEFORE                       {a['correct_before']}")
    print(f"  correct AFTER (rule 1 then rule 2)   {a['correct_after_rule1_then_rule2']}")
    print(f"  correct if rule 2 used the RAW loose column   "
          f"{a['correct_if_g2_applied_to_raw_loose_column']}   <- UPPER BOUND (X-7)")
    d = s["verdict_delta"]
    print("net verdict change vs the stored qa_correct")
    print(f"  same / recovered / lost              {d['same']} / {d['recovered']} / {d['lost']}")


# ── demonstration on constructed rows ───────────────────────────────

_ABS_IDS = [f"q{i}_abs" for i in range(30)]


def _v1_row(raw: str, query_id: str, ref: str = "") -> dict:
    """Reproduce EXACTLY what a v1 run wrote for one generation:
    firstline cleaning, loose detection, grade() with no abstention rule."""
    if raw.startswith(ERROR_PREFIX):
        stored, abst = raw, True
    else:
        stored = _clean_answer(raw, mode="firstline")
        abst = _is_abstention(stored, mode="loose")
    g = grade(stored, ref, abstained=abst)
    return {"query_id": query_id, "question_type": "single-session-user",
            "reader_answer": stored, "abstained": str(abst),
            "ref_answer": ref, "qa_correct": str(g["correct"]),
            "qa_f1": f"{g['f1']:.6f}"}


def _v2_row(raw: str, query_id: str, ref: str = "") -> dict:
    """The same generation as a v2 run wrote it: full cleaning, exact detection."""
    if raw.startswith(ERROR_PREFIX):
        stored, abst = raw, True
    else:
        stored = _clean_answer(raw, mode="full")
        abst = _is_abstention(stored, mode="exact")
    g = grade(stored, ref, abstained=abst)
    return {"query_id": query_id, "question_type": "single-session-user",
            "reader_answer": stored, "abstained": str(abst),
            "ref_answer": ref, "qa_correct": str(g["correct"]),
            "qa_f1": f"{g['f1']:.6f}"}


def demo() -> int:
    print("OFFLINE RE-GRADE — METHOD DEMONSTRATION on constructed rows.")
    print("SUBSTITUTION NOTICE: no v1/v2 results tree exists locally, so the rules are")
    print("exercised on constructed generations rather than measured on real output.")
    print("The code path below is exactly the one a real CSV takes.")

    # ── 1. the 29/30 class: 30 unanswerable questions, 29 correctly refused ──
    print("\n--- 1. the 29/30 abstention class flips (RULE 2) ---")
    refusals = ["IDK", "I don't know.", "insufficient information",
                "I cannot answer", "none of the memories"]
    rows_v1 = [_v1_row(refusals[i % len(refusals)], _ABS_IDS[i]) for i in range(29)]
    rows_v1.append(_v1_row("You adopted a golden retriever in May.", _ABS_IDS[29]))  # hallucination
    before = sum(_truthy(r["qa_correct"]) for r in rows_v1)
    added, summary = regrade(rows_v1)
    after = sum(a["qa_correct_regraded"] == "True" for a in added)
    print(f"  30 unanswerable (_abs) rows: 29 correct refusals + 1 hallucination")
    print(f"  correct under grade()             {before}/30")
    print(f"  correct after the regrade         {after}/30")
    print(f"  the hallucinated row stays wrong  "
          f"{added[29]['qa_correct_regraded']} (tier={added[29]['regrade_tier']})")
    ok_2930 = (before == 0 and after == 29
               and added[29]["qa_correct_regraded"] == "False")
    print(f"  ACCEPTANCE (29/30 class flips, nothing else): "
          f"{'PASS' if ok_2930 else 'FAIL'}")

    # ── 2. nothing else moves: answerable rows are untouched ────────────────
    print("\n--- 2. the rule touches ONLY the unanswerable cell ---")
    ans_rows = [
        _v1_row("May 14th", "q100", "May 14th"),          # correct answer
        _v1_row("The Hilton", "q101", "the Marriott"),    # wrong answer
        _v1_row("IDK", "q102", "May 14th"),               # refusal on an ANSWERABLE q
    ]
    a2, _ = regrade(ans_rows)
    print(f"  {'row':34s} {'qa_correct':>11s} {'regraded':>10s} {'delta':>10s}")
    for r, a in zip(ans_rows, a2):
        print(f"  {repr(r['reader_answer'])[:34]:34s} {r['qa_correct']:>11s} "
              f"{a['qa_correct_regraded']:>10s} {a['regrade_delta']:>10s}")
    ok_scope = all(a["regrade_delta"] == "same" for a in a2)
    print(f"  ACCEPTANCE (answerable rows unchanged): {'PASS' if ok_scope else 'FAIL'}")

    # ── 3. X-7: why rule 1 must run before rule 2 ───────────────────────────
    print("\n--- 3. X-7 resolved: rule 1 BEFORE rule 2 (the hedged-hallucination trap) ---")
    hedged = "IDK, but you mentioned owning a golden retriever"
    row = _v1_row(hedged, "q200_abs")
    naive_g2 = _truthy(row["abstained"])          # G-2 straight onto the loose column
    a3 = regrade_row(row)
    print(f"  generation: {hedged!r}")
    print(f"  v1 stored abstained (LOOSE)                  {row['abstained']}")
    print(f"  G-2 on that raw column would score it        correct={naive_g2}   <- WRONG")
    print(f"  after RULE 1 (exact), abstained_exact        {a3['abstained_exact']}")
    print(f"  RULE 1 then RULE 2 scores it                 "
          f"correct={a3['qa_correct_regraded']} (tier={a3['regrade_tier']})")
    ok_x7 = naive_g2 is True and a3["qa_correct_regraded"] == "False"
    print(f"  ACCEPTANCE (hedged hallucination NOT credited): {'PASS' if ok_x7 else 'FAIL'}")

    # ── 4. identical treatment of v1 and v2 output ──────────────────────────
    print("\n--- 4. 'applied identically to v1 and v2' — same generation, both vintages ---")
    print(f"  {'generation':44s} {'vintage':>8s} {'stored':>8s} {'exact':>7s} {'regraded':>9s}")
    for raw in ["IDK", hedged, "The rent was $2,100"]:
        for vintage, fn in (("v1", _v1_row), ("v2", _v2_row)):
            r = fn(raw, "q300_abs")
            a = regrade_row(r)
            print(f"  {repr(raw)[:44]:44s} {vintage:>8s} {r['abstained']:>8s} "
                  f"{a['abstained_exact']:>7s} {a['qa_correct_regraded']:>9s}")
    v1v2 = [(regrade_row(_v1_row(raw, "q300_abs"))["qa_correct_regraded"],
             regrade_row(_v2_row(raw, "q300_abs"))["qa_correct_regraded"])
            for raw in ["IDK", hedged, "The rent was $2,100"]]
    ok_ident = all(a == b for a, b in v1v2)
    print(f"  ACCEPTANCE (v1 and v2 regrade to the same verdict): "
          f"{'PASS' if ok_ident else 'FAIL'}")

    # ── 5. the two scope limits, measured ───────────────────────────────────
    print("\n--- 5. SCOPE LIMIT: multi-line hedges are unrecoverable from a v1 CSV ---")
    multi = ["IDK\nBut the lease shows the rent was $2,100 per month.",
             "I don't know.\nThe records list the deposit as $4,200."]
    for raw in multi:
        stored = _clean_answer(raw, mode="firstline")
        a = regrade_row(_v1_row(raw, "q400"))
        print(f"  raw={raw.splitlines()[0]!r}+{len(raw.splitlines()) - 1} more line(s) -> "
              f"stored={stored!r} redetect={a['abstention_redetect']}")
    print("  The content never reached the CSV, so exact mode agrees with loose and NO")
    print("  flip is possible. The answer is gone, not merely mislabelled.")

    print("\n--- 6. HAZARD: [LLM error:] rows are excluded, not flipped ---")
    errs = [_v1_row("[LLM error: connection reset by peer]", "q500"),
            _v1_row("[LLM error: read timeout after 120s]", "q501")]
    a6, s6 = regrade(errs)
    for r, a in zip(errs, a6):
        print(f"  {r['reader_answer'][:44]:44s} stored_abstained={r['abstained']:5s} "
              f"status={a['abstention_redetect']}")
    ok_err = s6["redetect"]["llm_error_excluded"] == 2 and s6["redetect"]["flipped"] == 0
    print(f"  ACCEPTANCE (infrastructure failures not promoted to answers): "
          f"{'PASS' if ok_err else 'FAIL'}")

    report(summary, "constructed 30-row abstention class")

    all_ok = all([ok_2930, ok_scope, ok_x7, ok_ident, ok_err])
    print(f"\nDEMO ACCEPTANCE: {'ALL PASS' if all_ok else 'FAILURES ABOVE'}")
    return 0 if all_ok else 1


# ── CLI ─────────────────────────────────────────────────────────────

REQUIRED = ("reader_answer", "abstained", "qa_correct", "query_id")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", type=Path,
                    help="results.csv / combined_results.csv with reader_answer + abstained")
    ap.add_argument("--out", type=Path,
                    help="output CSV (default: <csv stem>_regraded.csv beside the input)")
    ap.add_argument("--summary-json", type=Path, help="also write the summary as JSON")
    ap.add_argument("--demo", action="store_true",
                    help="run the constructed-row demonstration (no results tree needed)")
    args = ap.parse_args()

    if args.demo and not args.csv:
        return demo()
    if not args.csv:
        ap.error("give --csv or --demo")
    if not args.csv.exists():
        print(f"BLOCKED-MISSING-INPUT: {args.csv} does not exist", file=sys.stderr)
        return 2

    with args.csv.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        print(f"BLOCKED: {args.csv} has no rows", file=sys.stderr)
        return 2
    missing = [c for c in REQUIRED if c not in rows[0]]
    if missing:
        print(f"BLOCKED: {args.csv} lacks columns {missing} "
              f"(a --no-reader run has no answers to re-grade)", file=sys.stderr)
        return 2

    reader_rows = [r for r in rows if (r.get("reader") or "") not in ("", "none")]
    if reader_rows and len(reader_rows) != len(rows):
        print(f"note: {len(rows) - len(reader_rows)} reader-off rows carried through unchanged")

    added, summary = regrade(rows)
    report(summary, str(args.csv))

    out = args.out or args.csv.with_name(args.csv.stem + "_regraded.csv")
    fieldnames = list(rows[0].keys()) + [c for c in ADDED_COLUMNS if c not in rows[0]]
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row, a in zip(rows, added):
            w.writerow({**row, **a})
    print(f"\nwrote {out}  (+{len(ADDED_COLUMNS)} columns; originals untouched)")

    if args.summary_json:
        args.summary_json.parent.mkdir(parents=True, exist_ok=True)
        args.summary_json.write_text(json.dumps(summary, indent=2))
        print(f"wrote {args.summary_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
