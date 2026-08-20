"""Why does preserved disagree with retained on policies that never rewrite?

The validation gate in gold_preservation.py failed at 5 to 15 per cent on every
eviction only policy, and at exactly 0 on `oracle`. Oracle never evicts gold, so
the disagreement lives entirely in evicted gold items. Two causes predict that,
and they need different fixes, so this script separates them.

  H1  MULTI GOLD SCOPING. preserved is collapsed over the EXTRACTIVE gold items
      only; gold_id_retained is collapsed over ALL gold items. A question with
      two gold items, one extractive and one not, scores retained=True because
      the non extractive one survived and preserved=False because the extractive
      one was evicted. The two columns max over different sets.
      PREDICTION: disagreement vanishes on questions where every gold item is
      extractive.
      FIX: recompute retention from the snapshot over the same subset, rather
      than joining a differently scoped column out of results.csv.

  H2  TEMPORAL. The snapshot captures resident text at FINAL store state;
      gold_id_retained is evaluated at QUERY time. An item present when the
      query ran and evicted afterwards scores retained=True, preserved=False.
      PREDICTION: disagreement persists on single, fully extractive questions,
      and is one directional (retained and not preserved), and grows with
      budget tightness.
      FIX: not a fix. It is a scope statement: preservation is a final state
      measure and must be reported as such, or the eviction time has to be
      recovered from the event log and compared against query time.

Usage:
    python scripts/diagnose_preservation_gate.py <subset-dir>
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

try:
    from forgetit.benchmark.grading import answer_in_text
except Exception:
    def answer_in_text(answer: str, text: str) -> bool:
        n = lambda s: re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]+", " ", (s or "").lower())).strip()
        a, t = n(answer), n(text)
        return bool(a) and a in t

GOLD_SNAPSHOT = "gold_answer_snapshot.jsonl"
ADVERSARIAL = re.compile(r"adversarial|unanswerable", re.I)

# Policies that never rewrite text. On the extractive subset their preserved
# must equal retained exactly, so any disagreement here is instrumentation.
NON_REWRITING = {"fifo", "lru", "lfu", "oracle", "evict_smallest", "keep_smallest",
                 "gdsf", "gdsf_frozen", "random_s42", "ebbinghaus",
                 "salience_decay", "semantic", "semantic_coverage", "semantic_keep"}


def parse_config(d: Path) -> dict:
    parts = d.resolve().parts
    out = {"policy": "", "budget": "", "workload": "", "itemiser": ""}
    if "retention" in parts:
        i = parts.index("retention")
        out["workload"], out["itemiser"] = parts[i - 2], parts[i - 1]
        tail = parts[i + 1:]
        if len(tail) >= 3:
            out["policy"], out["budget"] = tail[0], tail[2]
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("root", type=Path)
    args = ap.parse_args()

    dirs = sorted({Path(dp) for dp, _d, fn in os.walk(args.root, followlinks=True)
                   if GOLD_SNAPSHOT in fn})
    print(f"{len(dirs)} config dirs\n")

    rows = []
    for d in dirs:
        meta = parse_config(d)
        if meta["policy"] not in NON_REWRITING:
            continue                        # only policies with the guarantee
        # results.csv: retention as the harness computed it, at query time
        ret = {}
        f = d / "results.csv"
        if f.is_file():
            with f.open(newline="", encoding="utf-8") as fh:
                for r in csv.DictReader(fh):
                    v = r.get("gold_id_retained", r.get("gold_survived", ""))
                    ret[r.get("query_id", "")] = str(v).strip().lower() in ("true", "1")

        per_q = defaultdict(lambda: {"n_gold": 0, "n_ext": 0, "ext_pres": [],
                                     "all_pres": [], "n_evicted": 0})
        with (d / GOLD_SNAPSHOT).open(encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                r = json.loads(line)
                if ADVERSARIAL.search(r.get("question_type", "") or ""):
                    continue
                qid = r.get("query_id", "")
                ans = r.get("answer", "")
                o = r.get("original_text", "") or ""
                ev = bool(r.get("was_evicted"))
                res = "" if ev else (r.get("resident_text", "") or "")
                ext = answer_in_text(ans, o)
                s = per_q[qid]
                s["n_gold"] += 1
                s["n_evicted"] += int(ev)
                # "retained" as the SNAPSHOT sees it: not evicted at final state
                s["all_pres"].append(not ev)
                if ext:
                    s["n_ext"] += 1
                    s["ext_pres"].append(answer_in_text(ans, res))

        for qid, s in per_q.items():
            if s["n_ext"] == 0:
                continue                    # out of scope for a string measure
            rows.append({
                **meta, "query_id": qid,
                "n_gold": s["n_gold"], "n_ext": s["n_ext"],
                "all_extractive": s["n_ext"] == s["n_gold"],
                "single_gold": s["n_gold"] == 1,
                # preserved: any extractive gold item still supports the answer
                "preserved": max(s["ext_pres"]),
                # retained_csv: the harness column, at QUERY time, over ALL gold
                "retained_csv": ret.get(qid, None),
                # retained_snap: not evicted at FINAL state, over ALL gold.
                # If retained_csv and retained_snap disagree, the cause is
                # temporal: same definition, different moment.
                "retained_snap": max(s["all_pres"]),
            })

    df = pd.DataFrame(rows)
    if df.empty:
        print("no rows", file=sys.stderr)
        return 2
    df = df[df.retained_csv.notna()]
    df["dis_csv"] = df.retained_csv != df.preserved
    df["dis_snap"] = df.retained_snap != df.preserved

    print(f"{len(df):,} (config, question) rows on non rewriting policies, "
          "extractive subset\n")

    # ── the decisive table ─────────────────────────────────────────────
    # dis_snap compares two FINAL STATE measures over the same items, so it
    # isolates H1. dis_csv adds the query time column, so the difference
    # between the two columns is H2.
    print("disagreement rate, split by the two hypotheses")
    print(f"  {'slice':<34}{'vs results.csv':>16}{'vs snapshot':>14}{'n':>9}")
    slices = {
        "all extractive questions": df,
        "  single gold item only": df[df.single_gold],
        "  every gold item extractive": df[df.all_extractive],
        "  single AND fully extractive": df[df.single_gold & df.all_extractive],
        "  multi gold, mixed extractive": df[~df.single_gold & ~df.all_extractive],
    }
    for name, sub in slices.items():
        if not len(sub):
            print(f"  {name:<34}{'—':>16}{'—':>14}{0:>9}"); continue
        print(f"  {name:<34}{sub.dis_csv.mean():>16.4f}"
              f"{sub.dis_snap.mean():>14.4f}{len(sub):>9}")

    print("\n  READ IT LIKE THIS")
    print("  'vs snapshot' near zero on the clean slice  -> H1, scoping. The")
    print("     retention column must be recomputed from the snapshot over the")
    print("     same extractive subset, not joined out of results.csv.")
    print("  'vs snapshot' near zero but 'vs results.csv' still high -> H2,")
    print("     temporal. Same definition, different moment: preservation is a")
    print("     FINAL STATE measure and results.csv is a QUERY TIME one.")
    print("  Both non zero on the clean slice -> neither; the capture is wrong")
    print("     and nothing downstream is usable.\n")

    # ── direction ──────────────────────────────────────────────────────
    clean = df[df.single_gold & df.all_extractive]
    if len(clean):
        a = int((clean.retained_csv & ~clean.preserved).sum())
        b = int((~clean.retained_csv & clean.preserved).sum())
        print(f"direction on the clean slice: retained-not-preserved={a}, "
              f"preserved-not-retained={b}")
        print("  Only the first is explicable by eviction after the query. The")
        print("  second cannot happen under either hypothesis and would mean the")
        print("  join is mismatching rows.\n")

    # ── budget gradient: H2 predicts tighter budgets disagree more ─────
    if "budget" in df.columns:
        g = (df.groupby("budget")[["dis_csv", "dis_snap"]].mean()
               .join(df.groupby("budget").size().rename("n")))
        print("by budget (H2 predicts the gap grows as the budget tightens)")
        print(g.round(4).to_string())

    out = REPO / "report" / "gate_diagnosis.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
