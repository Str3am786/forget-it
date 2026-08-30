#!/usr/bin/env python3
"""Emit every table Chapter 5 cites, as labelled CSVs, from the frozen bundles.

WHY A SECOND EMITTER. rq_tables.py answers the prespecified verdicts and writes
one bundle per retriever. Chapter 5 also needs cross-bundle views (the retriever
axis, the loss ladder), question-level breakdowns of the control arms, and the
before-and-after of the date-rendering correction. Computing those by hand in a
terminal is how numbers end up in a chapter with no way to regenerate them.
Everything here is derived; nothing is measured. Delete report/ch5 and re-run.

    python3 scripts/ch5_tables.py                       # defaults below
    python3 scripts/ch5_tables.py --out report/ch5

Each CSV carries a `source` column naming the bundle or artefact it came from,
so a row in the chapter can be traced to a file without reading this script.
"""
from __future__ import annotations

import argparse
import collections
import csv
import glob
import json
import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PROJ = os.environ.get("PROJ", "project_462001123")
SCRATCH = Path(f"/scratch/{PROJ}/artifacts")

RETRIEVERS = ("lexical", "bm25", "dense")
BUDGETS = ("010", "025", "050", "075", "100")
WORKLOADS = ("longmemeval_turn_pair", "beam_beam_turn_pair", "locomo_turn_pair")
PINNED = "qwen2.5-7b-instruct"

# The undated comparison. evidence-v1 is the base the date-rendering defect
# was found in; it is cited only as the before side of the correction.
UNDATED_BUNDLE = REPO / "report" / "rq_canon"
UNDATED_7B_BUNDLE = REPO / "report" / "rq_undated_7b"


def rows(path):
    """Stream a CSV as dicts. Missing file is loud but not fatal: a partial
    emit with a named gap beats a traceback halfway through."""
    p = Path(path)
    if not p.is_file():
        print(f"  MISSING {p}", file=sys.stderr)
        return []
    with p.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write(out: Path, name: str, fieldnames, records, note: str) -> None:
    out.mkdir(parents=True, exist_ok=True)
    with (out / name).open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(records)
    print(f"  {name:<34}{len(records):>6} rows   {note}")


def tb(v) -> bool:
    return str(v).strip().lower() == "true"


def f(v, default=0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


# ── 5.1  floors, ceilings, and the instrument ───────────────────────────────

def t51_controls(out: Path, bundles: dict) -> None:
    """Floors and ceilings per workload, arm and reader.

    The gold arm places the dataset's own marked items directly, so it never
    touches the store or the retriever: the three retriever bundles must agree
    on it to within judge noise. That agreement is itself the determinism read,
    so all three are emitted rather than one.
    """
    recs = []
    for ret, b in bundles.items():
        for r in rows(b / "controls.csv"):
            recs.append({"source": f"rq_dated5_{ret}", "retriever": ret,
                         "workload": r["workload"], "arm": r["arm"],
                         "reader": r["reader"], "acc_det": r["acc_det"],
                         "acc_guarded": r["acc_hyb"], "n": r["n"]})
    write(out, "t51_controls.csv",
          ["source", "retriever", "workload", "arm", "reader", "acc_det",
           "acc_guarded", "n"], recs,
          "floors and ceilings, all readers")


def t51_date_fix(out: Path, bundles: dict) -> None:
    """The date-rendering correction, matched on reader and retriever.

    Both sides are the pinned reader under the overlap retriever, so the only
    difference is whether item text carried its session date at render time.
    The floors are the control: a floor that moved would mean the date leaked
    an answer into the no-evidence arms.
    """
    und = {(r["workload"], r["arm"]): r
           for r in rows(UNDATED_7B_BUNDLE / "controls.csv")
           if r["reader"] == PINNED}
    dat = {(r["workload"], r["arm"]): r
           for r in rows(bundles["lexical"] / "controls.csv")
           if r["reader"] == PINNED}
    recs = []
    for key in sorted(set(und) & set(dat)):
        u, d = und[key], dat[key]
        recs.append({"source": "rq_undated_7b vs rq_dated5_lexical",
                     "workload": key[0], "arm": key[1], "reader": PINNED,
                     "undated_det": u["acc_det"], "dated_det": d["acc_det"],
                     "delta_det": round(f(d["acc_det"]) - f(u["acc_det"]), 4),
                     "undated_guarded": u["acc_hyb"], "dated_guarded": d["acc_hyb"],
                     "delta_guarded": round(f(d["acc_hyb"]) - f(u["acc_hyb"]), 4),
                     "n": d["n"]})
    write(out, "t51_date_fix.csv",
          ["source", "workload", "arm", "reader", "undated_det", "dated_det",
           "delta_det", "undated_guarded", "dated_guarded", "delta_guarded", "n"],
          recs, "before and after the render correction")


def t51_ceiling_by_type(out: Path, judged: dict) -> None:
    """The ceiling decomposed by question type.

    The aggregate ceiling is an average over categories that behave very
    differently, so quoting it alone invites the reading that the pipeline is
    uniformly weak. Emitted per reader as well, because a category that is flat
    across a tenfold size range is bounded by something other than the reader.
    """
    acc = collections.defaultdict(lambda: [0, 0, 0, 0])   # det, guarded, abst, n
    for ret, d in judged.items():
        if ret != "lexical":          # gold arm is retriever-invariant
            continue
        for path in sorted(glob.glob(str(d / "judged_*.csv"))):
            for r in rows(path):
                if r["arm"] != "gold" or not (r.get("ref_answer") or "").strip():
                    continue
                k = (r["dataset"], r["question_type"], r["model"])
                a = acc[k]
                det = tb(r["correct"])
                gd = tb(r["judge_correct"]) if r["tier"] in (
                    "miss", "content_words", "reverse") else det
                a[0] += det; a[1] += gd; a[2] += tb(r["abstained"]); a[3] += 1
    recs = [{"source": "judged5_lexical", "dataset": k[0], "question_type": k[1],
             "reader": k[2], "n": v[3],
             "acc_det": round(v[0] / v[3], 4),
             "acc_judged": round(v[1] / v[3], 4),
             "abstain_rate": round(v[2] / v[3], 4)}
            for k, v in sorted(acc.items()) if v[3]]
    write(out, "t51_ceiling_by_type.csv",
          ["source", "dataset", "question_type", "reader", "n", "acc_det",
           "acc_judged", "abstain_rate"], recs,
          "gold arm by category and reader")


def t51_classifier_determinism(out: Path) -> None:
    """Retest agreement of the sufficiency classifier, measured for free.

    Five policies read no access counter, so their surviving store is identical
    under every retriever. The classifier therefore saw byte-identical contexts
    in three independent runs, and any verdict that differs between them is the
    instrument's own variation rather than a difference in the store.
    """
    NO_ACCESS = {"fifo", "random_s42", "keep_smallest", "evict_smallest", "oracle"}
    per = collections.defaultdict(dict)
    for ret in RETRIEVERS:
        for w in WORKLOADS:
            p = SCRATCH / f"alive_dated_{ret}" / f"store_{w}.csv"
            for r in rows(p):
                if r.get("policy") not in NO_ACCESS:
                    continue
                per[(w, r["policy"], r["budget"], r["query_id"])][ret] = (
                    r.get("answerable_store"), r.get("n_alive_ids"))
    recs, agree, total, store_diff = [], 0, 0, 0
    by_w = collections.defaultdict(lambda: [0, 0, 0])
    for (w, _p, _b, _q), v in per.items():
        if len(v) < len(RETRIEVERS):
            continue
        vals = list(v.values())
        same_store = len({x[1] for x in vals}) == 1
        same_verdict = len({x[0] for x in vals}) == 1
        total += 1; agree += same_verdict
        store_diff += (not same_store)
        s = by_w[w]; s[0] += same_verdict; s[1] += 1; s[2] += (not same_store)
    for w, s in sorted(by_w.items()):
        recs.append({"source": "alive_dated_{lexical,bm25,dense}", "workload": w,
                     "policies": "access-independent (fifo, random_s42, "
                                 "keep_smallest, evict_smallest, oracle)",
                     "n_verdicts": s[1], "n_agree": s[0],
                     "agreement": round(s[0] / s[1], 5) if s[1] else "",
                     "n_store_differs": s[2]})
    write(out, "t51_classifier_determinism.csv",
          ["source", "workload", "policies", "n_verdicts", "n_agree",
           "agreement", "n_store_differs"], recs,
          "retest agreement on identical stores")


# ── 5.2  the store measurement against the answer ───────────────────────────

def t52_pair_states(out: Path, bundles: dict) -> None:
    recs = []
    for ret, b in bundles.items():
        for r in rows(b / "pair_counts.csv"):
            r = dict(r); r["source"] = f"rq_dated5_{ret}"; r["retriever"] = ret
            recs.append(r)
    if not recs:
        return
    head = ["source", "retriever"] + [k for k in recs[0] if k not in ("source", "retriever")]
    write(out, "t52_pair_states.csv", head, recs,
          "five-state landscape per cell, three retrievers")


def t52_contradictions(out: Path, bundles: dict) -> None:
    """Every contradicting pair, with its two policies named.

    The policy pair is the point: if every contradiction sets a rewriting
    policy against an eviction policy, the mechanism is about what the store
    contains rather than how much of it survives.
    """
    recs = []
    for ret, b in bundles.items():
        for r in rows(b / "pairs.csv"):
            if r.get("state") != "CONTRADICTS":
                continue
            r = dict(r); r["source"] = f"rq_dated5_{ret}"; r["retriever"] = ret
            r["pair_kind"] = ("compression_vs_eviction"
                              if ("compression" in (r["A"], r["B"]))
                              else "other")
            recs.append(r)
    if not recs:
        print("  t52_contradictions.csv        0 rows   NONE FOUND — check bundles",
              file=sys.stderr)
        return
    head = ["source", "retriever", "pair_kind"] + [
        k for k in recs[0] if k not in ("source", "retriever", "pair_kind")]
    write(out, "t52_contradictions.csv", head, recs,
          "contradicting pairs, all retrievers")


def t52_retriever_axis(out: Path, bundles: dict) -> None:
    """Contradiction count and decisiveness per retriever.

    A count of zero is ambiguous on its own: it can mean the two sides agree,
    or that neither side could resolve anything. The significant-pair counts
    separate those readings.
    """
    recs = []
    for ret, b in bundles.items():
        pr = rows(b / "pairs.csv")
        by_cell = collections.defaultdict(lambda: collections.Counter())
        for r in pr:
            c = by_cell[(r["workload"], r["budget"])]
            c[r["state"]] += 1
            c["store_sig"] += tb(r.get("store_sig"))
            c["acc_sig"] += tb(r.get("acc_sig"))
            c["n_pairs"] += 1
        for (w, bud), c in sorted(by_cell.items()):
            recs.append({"source": f"rq_dated5_{ret}", "retriever": ret,
                         "workload": w, "budget": bud, "n_pairs": c["n_pairs"],
                         "contradicts": c["CONTRADICTS"], "agrees": c["agrees"],
                         "both_silent": c["both_silent"],
                         "store_significant": c["store_sig"],
                         "answer_significant": c["acc_sig"]})
    write(out, "t52_retriever_axis.csv",
          ["source", "retriever", "workload", "budget", "n_pairs", "contradicts",
           "agrees", "both_silent", "store_significant", "answer_significant"],
          recs, "contradiction and decisiveness per retriever")


def t52_wrongpick(out: Path, bundles: dict) -> None:
    recs = []
    for ret, b in bundles.items():
        for r in rows(b / "wrongpick.csv"):
            r = dict(r); r["source"] = f"rq_dated5_{ret}"; r["retriever"] = ret
            recs.append(r)
    if not recs:
        return
    head = ["source", "retriever"] + [k for k in recs[0] if k not in ("source", "retriever")]
    write(out, "t52_wrongpick.csv", head, recs,
          "cost of selecting on the store measurement")


def t52_monotone(out: Path, bundles: dict) -> None:
    recs = []
    for ret, b in bundles.items():
        for r in rows(b / "monotone.csv"):
            r = dict(r); r["source"] = f"rq_dated5_{ret}"; r["retriever"] = ret
            recs.append(r)
    if not recs:
        return
    head = ["source", "retriever"] + [k for k in recs[0] if k not in ("source", "retriever")]
    write(out, "t52_monotone.csv", head, recs,
          "store and answer steps between adjacent budgets")


# ── 5.3  which policy, which budget, which workload ─────────────────────────

def t53_topsets(out: Path, bundles: dict) -> None:
    recs = []
    for ret, b in bundles.items():
        for r in rows(b / "topsets.csv"):
            r = dict(r); r["source"] = f"rq_dated5_{ret}"; r["retriever"] = ret
            recs.append(r)
    if not recs:
        return
    head = ["source", "retriever"] + [k for k in recs[0] if k not in ("source", "retriever")]
    write(out, "t53_topsets.csv", head, recs,
          "statistically indistinguishable top sets")


def t53_cells(out: Path, bundles: dict) -> None:
    """Every policy in every cell: the raw material for the ranking figures."""
    recs = []
    for ret, b in bundles.items():
        for r in rows(b / "rows_summary.csv"):
            recs.append({"source": f"rq_dated5_{ret}", "retriever": ret,
                         "workload": r["workload"], "budget": r["budget"],
                         "policy": r["policy"], "acc_guarded": r["acc"],
                         "sufficiency": r["store"], "n": r["n"]})
    write(out, "t53_cells.csv",
          ["source", "retriever", "workload", "budget", "policy", "acc_guarded",
           "sufficiency", "n"], recs,
          "per-cell accuracy and sufficiency")


def t53_ladder(out: Path, bundles: dict) -> None:
    """Where accuracy is lost, stage by stage.

    Gold minus the 100 per cent cell is the cost of having to find the evidence
    rather than being handed it, measured with nothing evicted. The 100 per cent
    cell is a single control: with no eviction every policy holds the same store,
    so `policy_at_best` there is arbitrary and is reported as `(no eviction)`.
    The remaining columns are the best of the fourteen deployable policies, which
    is a maximum over fourteen values and biased upward; it is a bound on what
    the stage can do, not the accuracy of any one policy.
    """
    recs = []
    for ret, b in bundles.items():
        gold = {r["workload"]: f(r["acc_hyb"])
                for r in rows(b / "controls.csv")
                if r["arm"] == "gold" and r["reader"] == PINNED}
        best = collections.defaultdict(dict)
        for r in rows(b / "rows_summary.csv"):
            if r["policy"] == "oracle":
                continue
            cur = best[r["workload"]].get(r["budget"], (-1.0, 0.0, ""))
            if f(r["acc"]) > cur[0]:
                best[r["workload"]][r["budget"]] = (f(r["acc"]), f(r["store"]),
                                                    r["policy"])
        for w in sorted(best):
            row = {"source": f"rq_dated5_{ret}", "retriever": ret, "workload": w,
                   "gold_guarded": round(gold.get(w, float("nan")), 4)}
            for bud in BUDGETS:
                a, s, p = best[w].get(bud, (float("nan"),) * 2 + ("",))
                row[f"acc_{bud}"] = round(a, 4)
                row[f"sufficiency_{bud}"] = round(s, 4)
                row[f"policy_{bud}"] = "(no eviction)" if bud == "100" else p
            row["retrieval_cost"] = round(gold.get(w, 0.0) - best[w].get(
                "100", (0.0,))[0], 4)
            row["eviction_cost"] = round(best[w].get("100", (0.0,))[0]
                                         - best[w].get("010", (0.0,))[0], 4)
            recs.append(row)
    head = (["source", "retriever", "workload", "gold_guarded"]
            + [f"{k}_{b}" for b in BUDGETS
               for k in ("acc", "sufficiency", "policy")]
            + ["retrieval_cost", "eviction_cost"])
    write(out, "t53_ladder.csv", head, recs,
          "loss ladder: gold, no eviction, each budget")


# ── 5.4 / 5.5  the axes and the bounds ──────────────────────────────────────

def t54_store_retriever_dependence(out: Path) -> None:
    """Which policies' surviving stores depend on the retriever, and which do not.

    Ingestion probes are issued through the retriever, so a policy that reads an
    access counter is scored on a signal the retriever produced. Policies that
    read no such counter should be byte-identical across retrievers; the split is
    a prediction of the mechanism and is emitted so it can be checked rather than
    asserted.
    """
    recs = []
    for w in WORKLOADS:
        per = collections.defaultdict(dict)
        for ret in RETRIEVERS:
            for r in rows(SCRATCH / f"alive_dated_{ret}" / f"store_{w}.csv"):
                per[(r["policy"], r["budget"], r["query_id"])][ret] = (
                    r.get("answerable_store"), r.get("n_alive_ids"))
        agg = collections.defaultdict(lambda: [0, 0, 0])   # store_diff, verdict_diff, n
        for (p, _b, _q), v in per.items():
            if len(v) < len(RETRIEVERS):
                continue
            a = agg[p]
            a[0] += len({x[1] for x in v.values()}) > 1
            a[1] += len({x[0] for x in v.values()}) > 1
            a[2] += 1
        for p, a in sorted(agg.items()):
            recs.append({"source": "alive_dated_{lexical,bm25,dense}",
                         "workload": w, "policy": p, "n_questions": a[2],
                         "n_store_differs": a[0], "n_verdict_differs": a[1],
                         "reads_access_signal": "yes" if a[0] else "no"})
    write(out, "t54_store_retriever_dependence.csv",
          ["source", "workload", "policy", "n_questions", "n_store_differs",
           "n_verdict_differs", "reads_access_signal"], recs,
          "store dependence on the retriever, per policy")


def t55_guard_audit(out: Path, bundles: dict) -> None:
    recs = []
    for ret, b in bundles.items():
        for r in rows(b / "guard_audit.csv"):
            r = dict(r); r["source"] = f"rq_dated5_{ret}"; r["retriever"] = ret
            recs.append(r)
    if not recs:
        return
    head = ["source", "retriever"] + [k for k in recs[0] if k not in ("source", "retriever")]
    write(out, "t55_guard_audit.csv", head, recs,
          "guard firings sampled for human reading")


# ── provenance ──────────────────────────────────────────────────────────────

def git(*args) -> str:
    try:
        r = subprocess.run(["git", "-C", str(REPO), *args],
                           capture_output=True, text=True, check=True)
        return r.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return os.environ.get("GIT_COMMIT", "")


def manifest(out: Path, bundles: dict, allow_dirty: bool) -> int:
    dirty = bool(git("status", "--porcelain"))
    if dirty and not allow_dirty:
        print("REFUSING to write the manifest from a dirty tree: the commit "
              "would not describe the code that produced these tables. Commit "
              "first, or set ALLOW_DIRTY=1.", file=sys.stderr)
        return 2
    src = {}
    for ret, b in bundles.items():
        mp = b / "manifest.json"
        src[f"rq_dated5_{ret}"] = json.loads(mp.read_text()) if mp.is_file() else None
    (out / "manifest.json").write_text(json.dumps({
        "commit": git("rev-parse", "HEAD"),
        "describe": git("describe", "--tags", "--always", "--dirty"),
        "dirty": dirty,
        "bundles": {k: str(v) for k, v in bundles.items()},
        "undated_comparison": str(UNDATED_7B_BUNDLE),
        "alive_root": str(SCRATCH),
        "source_manifests": src,
    }, indent=2) + "\n")
    return 0


README = """\
Chapter 5 tables, emitted by scripts/ch5_tables.py. Every file is DERIVED from
the frozen bundles named in manifest.json; nothing here is measured. Delete this
directory and re-run the script to regenerate it.

Every table carries a `source` column naming the bundle it came from.

  t51_controls.csv                floors (empty, parametric) and the ceiling
                                  (gold) per workload, arm and reader
  t51_date_fix.csv                the same controls before and after the
                                  date-rendering correction, pinned reader
  t51_ceiling_by_type.csv         the ceiling decomposed by question category
                                  and reader
  t51_classifier_determinism.csv  retest agreement of the sufficiency
                                  classifier on byte-identical stores

  t52_pair_states.csv             the five pair states per cell
  t52_contradictions.csv          every contradicting pair, policies named
  t52_retriever_axis.csv          contradiction count and decisiveness of each
                                  side, per retriever
  t52_wrongpick.csv               cost of selecting a policy on the store
                                  measurement
  t52_monotone.csv                store and answer movement between adjacent
                                  budgets

  t53_topsets.csv                 indistinguishable top sets per cell
  t53_cells.csv                   accuracy and sufficiency for every policy in
                                  every cell
  t53_ladder.csv                  the loss ladder: gold, no eviction, and each
                                  budget, with the retrieval and eviction costs

  t54_store_retriever_dependence.csv
                                  which policies' surviving stores depend on
                                  the retriever
  t55_guard_audit.csv             guard firings sampled for human reading

READING RULES. `acc_guarded` is the guarded verdict; `acc_det` is the
deterministic tier alone and is quoted only where the difference is the point.
`sufficiency` is the share of questions whose surviving store still supports the
answer, NOT the fraction of bytes retained. In t53_ladder.csv the per-budget
accuracy is the best of the fourteen deployable policies, so it is a maximum
over fourteen values and biased upward: a bound on the stage, not the accuracy
of any one policy. At the 100 per cent budget no item is evicted, so every
policy holds the same store and the cell is a single control.
"""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, default=REPO / "report" / "ch5")
    ap.add_argument("--bundle-prefix", default=str(REPO / "report" / "rq_dated5_"),
                    help="bundle directory prefix; the retriever name is appended")
    ap.add_argument("--skip-heavy", action="store_true",
                    help="skip the tables that stream the judged and store row "
                         "files (minutes, hundreds of MB)")
    args = ap.parse_args()

    bundles = {r: Path(f"{args.bundle_prefix}{r}") for r in RETRIEVERS}
    for r, b in bundles.items():
        if not b.is_dir():
            print(f"no bundle for {r}: {b}", file=sys.stderr)
            return 2
    judged = {r: SCRATCH / f"judged5_{r}" for r in RETRIEVERS}

    print(f"emitting to {args.out}")
    t51_controls(args.out, bundles)
    t51_date_fix(args.out, bundles)
    t52_pair_states(args.out, bundles)
    t52_contradictions(args.out, bundles)
    t52_retriever_axis(args.out, bundles)
    t52_wrongpick(args.out, bundles)
    t52_monotone(args.out, bundles)
    t53_topsets(args.out, bundles)
    t53_cells(args.out, bundles)
    t53_ladder(args.out, bundles)
    t55_guard_audit(args.out, bundles)
    if not args.skip_heavy:
        t51_ceiling_by_type(args.out, judged)
        t51_classifier_determinism(args.out)
        t54_store_retriever_dependence(args.out)

    (args.out / "README.txt").write_text(README)
    rc = manifest(args.out, bundles, os.environ.get("ALLOW_DIRTY") == "1")
    print(f"\nwrote {args.out}/  (README.txt describes every file)")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
