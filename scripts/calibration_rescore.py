#!/usr/bin/env python3
"""Re-score the annotated corpus under the CURRENT grading chain.

WHY RE-SCORE RATHER THAN RELABEL. A human label is a property of the triple
(question, reference, answer): it answers whether the answer is acceptable, and
that does not change when the pipeline changes. What changed is the instrument.
The tier is computed by grading.py from the reference and the answer, so the
same triple re-graded today lands on today's rungs; the `embedding` tier the
original corpus was stratified around no longer exists, and two rungs the
original corpus never saw -- content_words and reverse -- now carry rows. So the
130 labels stand and are re-joined to freshly computed verdicts.

WHAT IT CANNOT FIX. Rows drawn from strata the campaign now excludes are counted
and reported, never silently dropped: BEAM's reference-less abilities and
LoCoMo's adversarial stratum are ungradeable by scope, and an agreement figure
computed over them would describe a population the thesis does not report. The
corpus was also enriched at the old embedding boundary, so its tier mix is not
the campaign's. Per-tier agreement is what it estimates; a campaign-wide figure
must weight each tier by its real share, read from the draw manifest.

Two phases, so the second is only run if it is needed:

    python3 scripts/calibration_rescore.py --no-judge     # login node, seconds
    python3 scripts/calibration_rescore.py                # needs a served judge
"""
from __future__ import annotations

import argparse
import collections
import csv
import json
import math
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from forgetit.benchmark.grading import grade                    # noqa: E402
from forgetit.llm.readers.text_reader import is_abstention                  # noqa: E402

ROUTED = ("miss", "content_words", "reverse")

# Strata the campaign excludes. A verdict against a missing reference is a false
# accept whichever way it falls, so these rows are counted apart rather than
# folded into an agreement figure that claims to describe reported results.
EXCLUDED_TYPES = {
    "summarization", "instruction_following", "preference_following",  # BEAM
    "adversarial",                                                      # LoCoMo
}


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson interval. At these counts the normal approximation is wrong
    enough to matter: 12 of 12 would report a width of zero."""
    if n == 0:
        return (float("nan"), float("nan"))
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, c - h), min(1.0, c + h))


def load_labels(path: Path) -> list[dict]:
    rows = list(csv.DictReader(path.open(encoding="utf-8")))
    out = []
    for r in rows:
        lab = (r.get("human_correct") or "").strip().lower()
        if lab not in ("true", "false"):
            continue
        r["human"] = lab == "true"
        out.append(r)
    print(f"{len(rows)} rows in the sheet, {len(out)} carry a label")
    return out


def population_share(path: Path) -> dict:
    """The real tier shares, from the draw manifest. Without them a per-tier
    agreement table cannot be turned into a campaign-wide number."""
    if not path.is_file():
        return {}
    return json.load(path.open()).get("population_share", {})


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--labels", type=Path,
                    default=REPO / "corpora" / "grader" / "annotate.csv")
    ap.add_argument("--shares", type=Path,
                    default=REPO / "corpora" / "grader" / "draw3" /
                            "calibration_manifest.json")
    ap.add_argument("--out", type=Path, default=REPO / "report" / "ch5")
    ap.add_argument("--no-judge", action="store_true",
                    help="grade only; report the tier distribution and stop")
    ap.add_argument("--workers", type=int, default=16)
    args = ap.parse_args()

    rows = load_labels(args.labels)
    if not rows:
        print("no labelled rows", file=sys.stderr)
        return 2

    # ── re-grade under the current rules ───────────────────────────────
    for r in rows:
        ans = r.get("reader_answer") or ""
        ref = r.get("ref_answer") or ""
        r["excluded"] = (r.get("question_type") or "").strip() in EXCLUDED_TYPES \
            or not ref.strip()
        g = grade(ans, ref, abstained=is_abstention(ans))
        r["tier"] = g["tier"]
        r["det_correct"] = bool(g["correct"])

    live = [r for r in rows if not r["excluded"]]
    print(f"\n{len(rows) - len(live)} row(s) from strata the campaign excludes "
          f"(BEAM reference-less abilities, LoCoMo adversarial, or no reference)")
    print(f"{len(live)} usable\n")

    dist = collections.Counter(r["tier"] for r in live)
    print(f"{'tier':<16}{'labelled':>10}{'routed':>9}")
    for t, n in dist.most_common():
        print(f"{t:<16}{n:>10}{'yes' if t in ROUTED else '-':>9}")
    thin = [t for t in ROUTED if dist.get(t, 0) < 15]
    if thin:
        print(f"\nTHIN: {thin} carry fewer than 15 labelled rows each. An "
              f"agreement figure on a tier this thin has an interval too wide "
              f"to constrain anything; top up those tiers before quoting one.")

    if args.no_judge:
        print("\n--no-judge: stopping before the model call.")
        return 0

    # ── judge the routed rows ──────────────────────────────────────────
    import concurrent.futures as cf
    import importlib.util as u
    s = u.spec_from_file_location("jra", REPO / "scripts" / "judge_reader_answers.py")
    jra = u.module_from_spec(s)
    s.loader.exec_module(jra)          # same PROMPT, same parser, same instrument
    from forgetit.llm import get_llm

    todo = [r for r in live if r["tier"] in ROUTED]
    llm = get_llm()
    print(f"\njudge prompt sha {jra.prompt_sha(jra.PROMPT)}  "
          f"model {os.environ.get('LLM_MODEL', '?')}")
    print(f"judging {len(todo)} routed rows ...")
    for r in todo:
        r.setdefault("row_key", r.get("corpus_id", ""))
        r["question"] = r.get("question_text", "")
    with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
        for r, res in zip(todo, ex.map(lambda x: jra.judge_one(llm, x), todo)):
            r["judge_correct"] = res["judge_correct"]
            r["judge_why"] = res["judge_why"]
            r["judge_errored"] = res["judge_errored"]

    # ── the table ──────────────────────────────────────────────────────
    shares = population_share(args.shares)
    recs = []
    for tier in sorted(dist):
        sel = [r for r in live if r["tier"] == tier]
        routed = tier in ROUTED
        if routed:
            sel = [r for r in sel if r.get("judge_correct") is not None]
            pred = [bool(r["judge_correct"]) for r in sel]
        else:
            pred = [r["det_correct"] for r in sel]
        truth = [r["human"] for r in sel]
        n = len(sel)
        k = sum(p == t for p, t in zip(pred, truth))
        lo, hi = wilson(k, n)
        recs.append({
            "source": "corpora/grader/annotate.csv re-scored at HEAD",
            "tier": tier,
            "settled_by": "language model" if routed else "string matching",
            "population_share": round(shares.get(tier, float("nan")), 4)
                                if shares else "",
            "n_labelled": n,
            "n_agree": k,
            "agreement": round(k / n, 4) if n else "",
            "ci_lo": round(lo, 4) if n else "",
            "ci_hi": round(hi, 4) if n else "",
            "false_accept": sum(1 for p, t in zip(pred, truth) if p and not t),
            "false_reject": sum(1 for p, t in zip(pred, truth) if t and not p),
        })

    args.out.mkdir(parents=True, exist_ok=True)
    with (args.out / "t51_agreement.csv").open("w", newline="",
                                               encoding="utf-8") as fh:
        w = csv.DictWriter(fh, list(recs[0]))
        w.writeheader()
        w.writerows(recs)

    print(f"\n{'tier':<16}{'settled by':<17}{'share':>8}{'n':>5}{'agree':>8}"
          f"{'95% CI':>18}{'FA':>4}{'FR':>4}")
    for r in recs:
        ci = (f"[{r['ci_lo']:.3f}, {r['ci_hi']:.3f}]"
              if r["n_labelled"] else "")
        sh = f"{r['population_share']:.3f}" if r["population_share"] != "" else "-"
        print(f"{r['tier']:<16}{r['settled_by']:<17}{sh:>8}{r['n_labelled']:>5}"
              f"{r['agreement']:>8}{ci:>18}{r['false_accept']:>4}"
              f"{r['false_reject']:>4}")
    print(f"\nwrote {args.out / 't51_agreement.csv'}")
    print("\nThis estimates PER-TIER agreement. For a campaign-wide figure, "
          "weight each tier by population_share.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
