#!/usr/bin/env python3
"""Draw a blind calibration sample from the frozen judged bundle.

WHY A REDRAW. The 130-row corpus at corpora/grader/ calibrates an instrument
that no longer exists. Its manifest names a pre-campaign pilot as its source,
its stratification was built around the `embedding` tier's decision boundary and
that tier has been removed, 23 of its rows come from strata now excluded, and it
holds no rows at all in `content_words` or `reverse` -- two of the three tiers
the judge now decides. An agreement figure computed on it would describe neither
the current grading chain nor the campaign's population.

WHAT THIS EMITS. Two files, committed together before a single row is read:

  <out>/calibration_sheet.csv   the labeller's file. row_key, the question, the
                                reference, the answer, and two empty columns.
                                Carries NO tier, NO deterministic verdict and NO
                                judge verdict: seeing any of them turns the
                                agreement number into a measure of anchoring.
  <out>/calibration_key.csv     the sealed side. row_key -> tier, deterministic
                                verdict, judge verdict, arm, config, reader.
                                Joined only after labelling is finished.
  <out>/calibration_manifest.json  seed, strata, source files, commit.

STRATIFICATION. Proportional sampling would put almost everything in the tiers
the deterministic rules already settle, where agreement is known to be 1.000 and
nothing is learned. The three routed tiers are weighted heavily because they are
where the judge, not the string match, decides the verdict. The sample is
therefore NOT a population sample: per-tier agreement is what it estimates, and
a campaign-wide rate must be reconstructed by weighting each tier's agreement by
that tier's real share, which is emitted alongside for exactly that purpose.

    python3 scripts/calibration_draw.py
    python3 scripts/calibration_draw.py --n-per-tier miss=40,content_words=40
"""
from __future__ import annotations

import argparse
import collections
import csv
import glob
import hashlib
import json
import os
import random
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PROJ = os.environ.get("PROJ", "project_462001123")

# The three routed tiers are where the judge decides; the other three are
# settled by string matching and are sampled only to keep them checkable.
DEFAULT_STRATA = {"miss": 40, "content_words": 40, "reverse": 40,
                  "contains": 25, "abstain": 25, "exact": 20}

SEED = 20260831

BLIND_COLUMNS = ["row_key", "dataset", "question_type", "question",
                 "ref_answer", "reader_answer", "human_correct", "human_note"]
KEY_COLUMNS = ["row_key", "dataset", "question_type", "query_id", "arm",
               "config", "reader", "tier", "correct", "judge_correct",
               "judge_why", "abstained"]


def git(*args: str) -> str:
    try:
        r = subprocess.run(["git", "-C", str(REPO), *args],
                           capture_output=True, text=True, check=True)
        return r.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return os.environ.get("GIT_COMMIT", "")


def stable_key(r: dict) -> str:
    """Identity of one graded answer. Content-derived, so the same answer drawn
    twice gets the same key and a redraw can be compared with this one."""
    blob = "\x00".join(str(r.get(k, "")) for k in
                       ("model", "config", "query_id", "arm", "reader_answer"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--judged", type=Path,
                    default=Path(f"/scratch/{PROJ}/artifacts/judged5_lexical"),
                    help="the frozen judged bundle to sample from")
    ap.add_argument("--out", type=Path, default=REPO / "corpora" / "grader" / "draw3")
    ap.add_argument("--seed", type=int, default=SEED)
    ap.add_argument("--n-per-tier", default=None,
                    help="comma-separated tier=count overrides")
    ap.add_argument("--dry-run", action="store_true",
                    help="report the population and the draw plan, write nothing")
    args = ap.parse_args()

    strata = dict(DEFAULT_STRATA)
    if args.n_per_tier:
        for part in args.n_per_tier.split(","):
            k, _, v = part.partition("=")
            strata[k.strip()] = int(v)

    files = sorted(glob.glob(str(args.judged / "judged_*.csv")))
    if not files:
        print(f"no judged_*.csv under {args.judged}", file=sys.stderr)
        return 2

    # ── population ─────────────────────────────────────────────────────
    # Rows with no reference are UNGRADEABLE, not incorrect, and are excluded
    # here for the same reason the judge excludes them: a verdict against a
    # missing reference is a false accept whichever way it falls.
    pool: dict[str, list[dict]] = collections.defaultdict(list)
    n_seen = n_noref = 0
    for f in files:
        with open(f, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                n_seen += 1
                if not (r.get("ref_answer") or "").strip():
                    n_noref += 1
                    continue
                tier = (r.get("tier") or "").strip().lower()
                r["row_key"] = stable_key(r)
                r["reader"] = r.get("model", "")
                pool[tier].append(r)

    share = {t: len(v) / sum(len(x) for x in pool.values()) for t, v in pool.items()}
    print(f"population: {n_seen:,} rows, {n_noref:,} without a reference "
          f"(excluded), {sum(len(v) for v in pool.values()):,} gradeable")
    print(f"\n{'tier':<16}{'in population':>15}{'share':>9}{'to draw':>9}")
    for t in sorted(pool, key=lambda t: -len(pool[t])):
        print(f"{t:<16}{len(pool[t]):>15,}{share[t]:>9.3f}"
              f"{min(strata.get(t, 0), len(pool[t])):>9}")
    missing = [t for t in strata if t not in pool]
    if missing:
        print(f"\nWARNING: no rows in the population for {missing}", file=sys.stderr)

    if args.dry_run:
        return 0

    # ── the draw ───────────────────────────────────────────────────────
    # Sorted before shuffling so the sample depends on the seed and the data,
    # never on file order or dict iteration order.
    rng = random.Random(args.seed)
    drawn: list[dict] = []
    for tier in sorted(strata):
        rows = sorted(pool.get(tier, []), key=lambda r: r["row_key"])
        take = min(strata[tier], len(rows))
        drawn.extend(rng.sample(rows, take))
    rng.shuffle(drawn)          # tier order in the sheet would leak the tier

    args.out.mkdir(parents=True, exist_ok=True)
    with (args.out / "calibration_sheet.csv").open("w", newline="",
                                                   encoding="utf-8") as fh:
        w = csv.DictWriter(fh, BLIND_COLUMNS, extrasaction="ignore")
        w.writeheader()
        for r in drawn:
            w.writerow({**{k: r.get(k, "") for k in BLIND_COLUMNS},
                        "human_correct": "", "human_note": ""})
    with (args.out / "calibration_key.csv").open("w", newline="",
                                                 encoding="utf-8") as fh:
        w = csv.DictWriter(fh, KEY_COLUMNS, extrasaction="ignore")
        w.writeheader()
        w.writerows(drawn)

    (args.out / "calibration_manifest.json").write_text(json.dumps({
        "commit": git("rev-parse", "HEAD"),
        "describe": git("describe", "--tags", "--always", "--dirty"),
        "seed": args.seed,
        "source_bundle": str(args.judged),
        "source_files": [Path(f).name for f in files],
        "n_drawn": len(drawn),
        "strata_requested": strata,
        "strata_drawn": dict(collections.Counter(r["tier"] for r in drawn)),
        "population_share": {t: round(s, 5) for t, s in sorted(share.items())},
        "rows_without_reference_excluded": n_noref,
        "note": ("Stratified, NOT proportional. Per-tier agreement is what this "
                 "estimates. A campaign-wide rate must weight each tier's "
                 "agreement by population_share above."),
    }, indent=2) + "\n")

    print(f"\ndrawn {len(drawn)} rows -> {args.out}/")
    print("  calibration_sheet.csv   label human_correct here, nothing else open")
    print("  calibration_key.csv     sealed; join only after labelling")
    print("\nCommit both files BEFORE labelling. The sheet is only blind if the "
          "key was fixed first.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
