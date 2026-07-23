"""G-6: quantify the loose-abstention false-positive rate over v1 results.

WHY. grading.py:235-236 short-circuits `grade()` on `abstained=True` BEFORE any
matching runs. Under v1's `loose` detection, any answer merely CONTAINING a
refusal phrase is flagged, so v1's 69.5% "abstention" is a FLAGGED-UNDER-LOOSE
UPPER BOUND, not a refusal rate. Re-running `_is_abstention` in `exact` mode
over the stored `reader_answer` column recovers the flips offline, zero GPU.

USAGE
    # real mode, when a v1 results.csv exists:
    python verification/g6_loose_abstention_recount.py --csv <path/to/results.csv>
    # method demonstration on constructed rows (no v1 tree required):
    python verification/g6_loose_abstention_recount.py --demo

SCOPE LIMIT (verified by --demo, section 3). v1 persisted the CLEANED answer:
runner_core.py:206 stores `answer_result.answer`, which is
`_clean_answer(raw, mode="firstline")` (text_reader.py:569). `raw_output` lives
only in AnswerResult.meta and never reaches EvalResult/the CSV. So for a
multi-line generation, everything after the first newline was destroyed BEFORE
storage. Re-detection therefore recovers SINGLE-LINE hedges only; multi-line
hedges are unrecoverable from the CSV at any later date.

HAZARD (measured by --demo, section 4). Rows whose reader_answer starts with
"[LLM error:" were stamped abstained=True by the error path (text_reader.py:562)
WITHOUT consulting `_is_abstention` at all. A naive recount flips them to
not-abstained and would silently promote infrastructure failures into the
answer pool. They are excluded here and counted separately.
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from forgetit.llm.readers.text_reader import (  # noqa: E402
    _clean_answer, _is_abstention, _ABSTENTION_PHRASES,
)

ERROR_PREFIX = "[LLM error:"


def recount(rows: list[dict]) -> dict:
    """Re-detect abstention in `exact` mode over the stored reader_answer."""
    n = len(rows)
    stats = Counter()
    flipped: list[dict] = []
    for r in rows:
        ans = (r.get("reader_answer") or "")
        was = str(r.get("abstained", "")).strip().lower() in ("true", "1")
        if ans.startswith(ERROR_PREFIX):
            stats["llm_error_excluded"] += 1
            if was:
                stats["llm_error_was_abstained"] += 1
            continue
        stats["eligible"] += 1
        if was:
            stats["flagged_loose"] += 1
            still = _is_abstention(ans, mode="exact")
            if still:
                stats["genuine_exact"] += 1
            else:
                stats["flipped"] += 1
                flipped.append(r)
    elig = stats["eligible"] or 1
    fl = stats["flagged_loose"] or 1
    return {
        "n_rows": n,
        "llm_error_excluded": stats["llm_error_excluded"],
        "llm_error_was_abstained": stats["llm_error_was_abstained"],
        "eligible": stats["eligible"],
        "flagged_loose": stats["flagged_loose"],
        "genuine_exact": stats["genuine_exact"],
        "flipped": stats["flipped"],
        "loose_rate": stats["flagged_loose"] / elig,
        "exact_rate": stats["genuine_exact"] / elig,
        "flipped_fraction_of_flagged": stats["flipped"] / fl,
        "flipped_fraction_of_eligible": stats["flipped"] / elig,
        "_flipped_rows": flipped,
    }


def report(res: dict, label: str) -> None:
    print(f"\n=== G-6 recount: {label} ===")
    print(f"rows                                   {res['n_rows']}")
    print(f"[LLM error:] rows excluded             {res['llm_error_excluded']}"
          f"  (of which stamped abstained: {res['llm_error_was_abstained']})")
    print(f"eligible rows                          {res['eligible']}")
    print(f"flagged abstained under LOOSE (v1)     {res['flagged_loose']}"
          f"   = {100 * res['loose_rate']:.1f}% of eligible   <- UPPER BOUND")
    print(f"still abstained under EXACT            {res['genuine_exact']}"
          f"   = {100 * res['exact_rate']:.1f}% of eligible   <- refusal rate")
    print(f"FLIPPED (loose false positives)        {res['flipped']}")
    print(f"  flipped / flagged                    {100 * res['flipped_fraction_of_flagged']:.1f}%")
    print(f"  flipped / eligible                   {100 * res['flipped_fraction_of_eligible']:.1f}%")


# ── demonstration ───────────────────────────────────────────────────

_SINGLE_LINE_HEDGES = [
    "IDK if there were others, but the rent was $2,100",
    "I don't know the exact date, but it was in March 2023",
    "Insufficient information on the co-signer; the deposit was $4,200",
    "No relevant information about the second tenant. The first is Dana Wu.",
    "I cannot answer for the whole year, but Q3 revenue was 1.4M",
]
_MULTI_LINE_HEDGES = [
    "IDK\nBut the lease shows the rent was $2,100 per month.",
    "I don't know.\nThe records list the deposit as $4,200.",
    "Not enough information.\nThe tenant of record is Dana Wu.",
]
_GENUINE = ["IDK", "I don't know.", "insufficient information",
            "I cannot answer", "none of the memories"]
_CLEAN = ["The rent was $2,100", "Dana Wu", "March 2023"]
_ERRORS = ["[LLM error: connection reset by peer]",
           "[LLM error: read timeout after 120s]"]


def v1_row(raw: str) -> dict:
    """Reproduce EXACTLY what v1 wrote for one generation."""
    if raw.startswith(ERROR_PREFIX):          # error path: no cleaning, no detection
        return {"reader_answer": raw, "abstained": "True", "_raw": raw}
    cleaned = _clean_answer(raw, mode="firstline")        # v1 clean mode
    return {"reader_answer": cleaned,
            "abstained": str(_is_abstention(cleaned, mode="loose")),  # v1 detect mode
            "_raw": raw}


def demo() -> int:
    print("G-6 METHOD DEMONSTRATION on constructed rows.")
    print("SUBSTITUTION NOTICE: no v1 results tree exists locally, so the rule is")
    print("exercised on constructed generations rather than measured on v1 data.")
    print("The machinery below is the same code path a real CSV would take.")

    raws = (_SINGLE_LINE_HEDGES + _MULTI_LINE_HEDGES + _GENUINE + _CLEAN + _ERRORS)
    rows = [v1_row(r) for r in raws]

    print("\n--- 1. per-row v1 write vs exact re-detection ---")
    print(f"{'raw (repr, truncated)':52s} {'stored reader_answer':38s} {'v1':>6s} {'exact':>6s} {'flip':>5s}")
    print("-" * 112)
    for r in rows:
        was = r["abstained"] == "True"
        if r["reader_answer"].startswith(ERROR_PREFIX):
            now, flip = "n/a", "EXCL"
        else:
            n = _is_abstention(r["reader_answer"], mode="exact")
            now, flip = str(n), ("YES" if (was and not n) else "")
        print(f"{repr(r['_raw'])[:52]:52s} {repr(r['reader_answer'])[:38]:38s} "
              f"{str(was):>6s} {now:>6s} {flip:>5s}")

    res = recount(rows)
    report(res, "constructed corpus")

    print("\n--- 2. grading consequence of one flip (the compounding bias) ---")
    from forgetit.benchmark.grading import grade
    raw = "IDK if there were others, but the rent was $2,100"
    stored = _clean_answer(raw, mode="firstline")
    ref = "$2,100"
    for mode in ("loose", "exact"):
        a = _is_abstention(stored, mode=mode)
        g = grade(stored, ref, abstained=a)
        print(f"  {mode:6s} abstained={str(a):5s} correct={str(g['correct']):5s} "
              f"f1={g['f1']:.3f} tier={g['tier']}")
    print("  -> the same event both understates accuracy and overstates abstention.")

    print("\n--- 3. SCOPE LIMIT, measured: is multi-line loss really irreversible? ---")
    print(f"{'shape':12s} {'raw len':>8s} {'stored len':>11s} {'answer survives storage?':>26s}")
    print("-" * 62)
    lost = kept = 0
    for label, raws_, needle in (("single-line", _SINGLE_LINE_HEDGES, None),
                                 ("multi-line", _MULTI_LINE_HEDGES, None)):
        for raw in raws_:
            stored = _clean_answer(raw, mode="firstline")
            full = _clean_answer(raw, mode="full")
            survives = stored.strip() == full.strip()
            lost += (not survives)
            kept += survives
            print(f"{label:12s} {len(raw):8d} {len(stored):11d} {str(survives):>26s}")
    print(f"  content preserved by firstline: {kept}/{kept + lost}")
    print(f"  content DESTROYED by firstline: {lost}/{kept + lost}")
    print("  For a destroyed row the CSV holds only the bare refusal token, so exact")
    print("  mode agrees with loose and NO flip is possible — the answer is gone, not")
    print("  merely mislabelled. The manifest's 'single-line hedges only' limit is REAL.")

    ml_res = recount([v1_row(r) for r in _MULTI_LINE_HEDGES])
    sl_res = recount([v1_row(r) for r in _SINGLE_LINE_HEDGES])
    print(f"  recoverable flips, single-line set: {sl_res['flipped']}/{len(_SINGLE_LINE_HEDGES)}")
    print(f"  recoverable flips, multi-line  set: {ml_res['flipped']}/{len(_MULTI_LINE_HEDGES)}"
          f"   <- 0 by construction: the content never reached the CSV")

    print("\n--- 4. HAZARD, measured: [LLM error:] rows ---")
    err_rows = [v1_row(r) for r in _ERRORS]
    naive = sum(1 for r in err_rows
                if r["abstained"] == "True"
                and not _is_abstention(r["reader_answer"], mode="exact"))
    print(f"  error rows: {len(err_rows)}; stamped abstained by the error path: "
          f"{sum(1 for r in err_rows if r['abstained'] == 'True')}")
    print(f"  a NAIVE recount would flip {naive}/{len(err_rows)} of them to not-abstained,")
    print("  promoting infrastructure failures into the answered pool. This script")
    print("  excludes them; any analysis script adopting the rule MUST do the same.")

    print("\n--- 5. phrase-level flip sensitivity ---")
    print("  which phrases are substring-reachable inside a longer answer:")
    for p in sorted(_ABSTENTION_PHRASES):
        probe = f"{p}, but the value was 42"
        print(f"    {p:38s} loose={str(_is_abstention(probe, 'loose')):5s} "
              f"exact={_is_abstention(probe, 'exact')}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", type=Path, help="v1 results.csv with reader_answer + abstained")
    ap.add_argument("--demo", action="store_true", help="run the constructed-row demonstration")
    a = ap.parse_args()
    if a.demo and not a.csv:
        return demo()
    if not a.csv:
        ap.error("give --csv or --demo")
    if not a.csv.exists():
        print(f"BLOCKED-MISSING-INPUT: {a.csv} does not exist", file=sys.stderr)
        return 2
    with a.csv.open(newline="") as f:
        rows = list(csv.DictReader(f))
    missing = {"reader_answer", "abstained"} - set(rows[0] if rows else {})
    if missing:
        print(f"BLOCKED: {a.csv} lacks columns {sorted(missing)}", file=sys.stderr)
        return 2
    report(recount(rows), str(a.csv))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

