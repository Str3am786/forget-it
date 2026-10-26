#!/usr/bin/env python3
"""Did the resident run work? Test it against the sweep rows we already have.

Compares one resident row file with the reader_sweep_dated row file of the SAME
reader, workload and retriever. Uses only fields the reader probe wrote
(answer, correct, n_gold_*), so it needs no judge and no GPU. Read only.

Checks, each with a prediction that follows from how the prompt is built:

  pairing      every (config, query_id) in the resident file has a retrieved
               row in the reference file, and the reverse for the questions
               the resident file covers. Must be exact.
  as_gold      rows where every gold item survived unrewritten: the prompt is
               byte identical to the gold arm's, so the answer should be too.
  as_empty     rows where no gold item survived: the prompt is identical to
               the empty arm's, so the answer should match the empty arm.
  health       errored, truncated and cap_hit rates.

Answer agreement is not required to be 1.0. vLLM at temperature 0 is close to
deterministic but not exactly so under batching, so a few rows can differ in
wording. Below 0.95 means the prompt is not what it should be.

The last table is the first look at the result: deterministic accuracy per
policy for gold, resident and retrieved. It is NOT the guarded verdict; the
thesis numbers come from the judge pass, not from here.

    python3 check_resident_run.py RESIDENT.jsonl REFERENCE.jsonl
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

MIN_AGREE = 0.95


def rows(path: Path):
    with path.open(encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    pass                      # partial trailing write


def policy_budget(config: str) -> tuple[str, str]:
    # config keys look like <wl>/<itemiser>/retention/<policy>/<retriever>/budget_XXXpct/none
    p = config.split("/")
    i = p.index("retention") if "retention" in p else -1
    pol = p[i + 1] if i >= 0 and i + 1 < len(p) else "?"
    bud = next((x for x in p if x.startswith("budget_")), "?")
    return pol, bud


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("resident", type=Path)
    ap.add_argument("reference", type=Path)
    args = ap.parse_args()

    res = [r for r in rows(args.resident) if r.get("arm") == "resident"]
    if not res:
        print("no resident rows", file=sys.stderr)
        return 2
    qids = {r["query_id"] for r in res}

    shared: dict[tuple[str, str], dict] = {}       # (query_id, arm) -> row
    retrieved: dict[tuple[str, str], dict] = {}    # (config, query_id) -> row
    for r in rows(args.reference):
        if r.get("query_id") not in qids:
            continue
        if r.get("arm") in ("gold", "empty"):
            shared[(r["query_id"], r["arm"])] = r
        elif r.get("arm") == "retrieved":
            retrieved[(r["config"], r["query_id"])] = r

    ok = True
    # duplicates: two jobs writing one file both start from an empty resume
    # set and each write the full task list. Every key must appear once.
    keys = [(r["config"], r["query_id"]) for r in res]
    dup = len(keys) - len(set(keys))
    print(f"duplicates: {dup} repeated (config, query_id) rows", end="  ")
    if dup:
        ok = False
        print("FAIL (two jobs wrote this file; delete it and rerun one job)")
        seen_k, uniq = set(), []
        for r in res:                         # keep the first copy for the checks below
            k = (r["config"], r["query_id"])
            if k not in seen_k:
                seen_k.add(k); uniq.append(r)
        res = uniq
    else:
        print("ok")
    # pairing
    rkeys = {(r["config"], r["query_id"]) for r in res}
    configs = {c for c, _ in rkeys}
    tkeys = {k for k in retrieved if k[0] in configs}
    only_res, only_ret = rkeys - tkeys, tkeys - rkeys
    print(f"rows: {len(res)} resident over {len(configs)} configs and {len(qids)} questions")
    print(f"pairing: {len(rkeys & tkeys)} paired, {len(only_res)} resident only, "
          f"{len(only_ret)} retrieved only", end="  ")
    if only_res or only_ret:
        ok = False
        print("FAIL")
        for k in sorted(only_res | only_ret)[:5]:
            print("   ", k)
    else:
        print("ok")

    # predictions
    for name, arm, select in (
        ("as_gold", "gold",
         lambda r: r["n_gold_resident"] == r["n_gold_items"] and not r["n_gold_rewritten"]),
        ("as_empty", "empty", lambda r: r["n_gold_resident"] == 0),
    ):
        sel = [r for r in res if select(r) and not r["errored"]]
        pairs = [(r, shared.get((r["query_id"], arm))) for r in sel]
        pairs = [(a, b) for a, b in pairs if b is not None and not b["errored"]]
        if not pairs:
            print(f"{name}: no rows to test")
            continue
        same_ans = sum(a["answer"].strip() == b["answer"].strip() for a, b in pairs) / len(pairs)
        same_cor = sum(a["correct"] == b["correct"] for a, b in pairs) / len(pairs)
        flag = "ok" if same_cor >= MIN_AGREE else "FAIL"
        ok &= flag == "ok"
        print(f"{name}: {len(pairs)} rows  answer identical {same_ans:.3f}  "
              f"correct agrees {same_cor:.3f}  {flag}")

    # health
    n = len(res)
    err = sum(r["errored"] for r in res) / n
    trunc = sum(bool(r.get("truncated")) for r in res) / n
    cap = sum(bool(r.get("cap_hit")) for r in res) / n
    flag = "ok" if err == 0 else "FAIL"
    ok &= flag == "ok"
    print(f"health: errored {err:.4f}  truncated {trunc:.4f}  cap_hit {cap:.4f}  {flag}")

    # first look: deterministic accuracy, gold / resident / retrieved, per policy and budget
    acc = defaultdict(lambda: defaultdict(list))
    for r in res:
        if r["errored"]:
            continue
        key = policy_budget(r["config"])
        acc[key]["resident"].append(r["correct"])
        t = retrieved.get((r["config"], r["query_id"]))
        if t and not t["errored"]:
            acc[key]["retrieved"].append(t["correct"])
        g = shared.get((r["query_id"], "gold"))
        if g and not g["errored"]:
            acc[key]["gold"].append(g["correct"])

    def m(v):
        return f"{sum(v) / len(v):.3f}" if v else "  -  "
    print(f"\n{'policy':<20}{'budget':<15}{'gold':>7}{'resident':>10}{'retrieved':>11}"
          f"{'forget':>8}{'retr.':>7}")
    for (pol, bud), d in sorted(acc.items()):
        g, s, t = d["gold"], d["resident"], d["retrieved"]
        fg = f"{sum(g)/len(g) - sum(s)/len(s):+.3f}" if g and s else "-"
        rt = f"{sum(s)/len(s) - sum(t)/len(t):+.3f}" if s and t else "-"
        print(f"{pol:<20}{bud:<15}{m(g):>7}{m(s):>10}{m(t):>11}{fg:>8}{rt:>7}")
    print("\nforget = gold - resident, retr. = resident - retrieved. Deterministic "
          "tier only, small n in a smoke: a first look, not a result.")

    print("\nPASS" if ok else "\nFAIL: do not submit the full run until this is explained")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
