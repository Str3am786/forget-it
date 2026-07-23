"""Oracle dominance + budget monotonicity over a summary tree (manifest P-5).

P-5 as written asserts these two properties over the EXISTING v1 summary. That
summary is not in this repository (see the REPORT for the BLOCKED-MISSING-INPUT
finding and the exact required paths). This script is the analysis code for the
row; point it at whichever summary tree is available.

TWO PROPERTIES, MEASURED SEPARATELY
-----------------------------------
DOMINANCE     within a fixed (dataset, itemiser, retriever, budget) cell,
              oracle's gold_survival_rate >= every other policy's.
              OraclePolicy never evicts a protected (gold-bearing) item while
              anything unprotected remains, so it is an upper bound BY
              CONSTRUCTION -- which makes any violation a defect, not noise.
              Violations are reported with the offending policy and the gap.
              ``forced_gold_evictions`` (budget too small to hold the gold set
              at all) is the one sanctioned way the bound can bind; it is a
              property of the (budget, example) pair, not a policy win.

MONOTONICITY  within a fixed (dataset, itemiser, retriever, policy),
              gold_survival_rate is non-decreasing in budget fraction.
              Weak form: plateaus (including at 1.0) are allowed.
              NOTE the scope: this is guaranteed only for policies whose
              eviction order does not depend on the budget path. `random`
              draws from the resident set, whose size is budget-dependent, so
              its sample path changes with the budget; it is reported but
              flagged as not-guaranteed rather than counted as a defect.

Usage:
    PYTHONPATH=src .venv/bin/python scripts/oracle_dominance_monotonicity.py <root>
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

METRIC = "gold_survival_rate"
NOT_GUARANTEED = {"random"}     # budget-dependent sample path


def gold_pressure(config_dir: Path) -> list[dict] | None:
    """Per example: is the UNION of gold bytes larger than the budget?

    This is the precondition for oracle dominance. OraclePolicy protects the
    union of every scored query's gold for that example; when that union does
    not fit, the budget loop forces it to evict protected items
    (``forced_gold_evictions``) and it degenerates to oldest-protected-first,
    i.e. FIFO among gold -- at which point it is beatable and is no longer an
    upper bound.

    Recomputed from events here because ``forced_gold_evictions`` is
    incremented on the policy object (policies/oracle.py:64) and then NEVER
    emitted: it is absent from results.csv, summary.csv, and the event log.
    Without this recompute an analyst cannot tell a sound oracle cell from an
    unsound one.
    """
    ev = config_dir / "events.jsonl"
    if not ev.exists():
        return None
    import json
    events = [json.loads(l) for l in ev.open()]
    examples, cur = [], []
    for e in events:
        if e.get("name") == "clear":
            examples.append(cur)
            cur = []
        else:
            cur.append(e)
    if cur:
        examples.append(cur)

    out = []
    for ex in examples:
        inserts = [e for e in ex if e.get("name") == "insert"]
        if not inserts:
            continue
        gold: set[str] = set()
        for e in ex:
            if e.get("name") == "gold":
                gold.update(e["gold_ids"])
        budget = int(inserts[0]["budget_bytes"])
        gold_bytes = sum(int(e["bytes"]) for e in inserts if e["id"] in gold)
        evicted = [e["id"] for e in ex if e.get("name") == "evict"]
        out.append({
            "n_items": len(inserts),
            "n_gold_items": len(gold),
            "budget_bytes": budget,
            "gold_union_bytes": gold_bytes,
            "gold_union_over_budget": gold_bytes > budget,
            "n_evictions": len(evicted),
            "forced_gold_evictions": sum(1 for i in evicted if i in gold),
        })
    return out


def collect(root: Path) -> pd.DataFrame:
    rows = []
    for p in sorted(root.rglob("summary.csv")):
        parts = p.parent.parts
        try:
            i = parts.index("retention")
        except ValueError:
            continue
        df = pd.read_csv(p)
        if METRIC not in df.columns or df.empty:
            continue
        budget_dir = parts[i + 3]
        rows.append({
            "dataset": parts[i - 2],
            "itemiser": parts[i - 1],
            "policy": parts[i + 1],
            "retriever": parts[i + 2],
            "budget_frac": int(budget_dir.replace("budget_", "").replace("pct", "")) / 100.0,
            METRIC: float(df[METRIC].iloc[0]),
            "n_queries": float(df["n_queries"].iloc[0]) if "n_queries" in df else float("nan"),
            "path": str(p),
        })
    return pd.DataFrame(rows)


def main() -> int:
    root = Path(sys.argv[1])
    df = collect(root)
    if df.empty:
        print(f"no usable summary.csv under {root}")
        return 2

    print(f"root     : {root}")
    print(f"rows     : {len(df)}")
    print(f"policies : {sorted(df.policy.unique())}")
    print(f"budgets  : {sorted(df.budget_frac.unique())}")
    print(f"cells    : {df.groupby(['dataset','itemiser','retriever']).ngroups}")
    print()

    # ── DOMINANCE ────────────────────────────────────────────────────
    print("=" * 78)
    print(f"DOMINANCE: oracle {METRIC} >= every other policy, per cell")
    print("=" * 78)
    dom_checked = dom_fail = 0
    cells_without_oracle = 0
    dom_fail_sound = 0        # violations in cells where the gold union FITS
    for key, g in df.groupby(["dataset", "itemiser", "retriever", "budget_frac"]):
        orc = g[g.policy == "oracle"]
        if orc.empty:
            cells_without_oracle += 1
            continue
        o = float(orc[METRIC].iloc[0])
        gp = gold_pressure(Path(orc["path"].iloc[0]).parent)
        unsound = bool(gp) and any(x["gold_union_over_budget"] for x in gp)
        others = g[g.policy != "oracle"]
        viol = []
        for _, r in others.iterrows():
            dom_checked += 1
            if r[METRIC] > o + 1e-12:
                dom_fail += 1
                if not unsound:
                    dom_fail_sound += 1
                viol.append((r.policy, float(r[METRIC])))
        margins = {r.policy: round(o - r[METRIC], 4) for _, r in others.iterrows()}
        flag = "  [ORACLE UNSOUND: gold union > budget]" if unsound else ""
        print(f"  {key}  oracle={o:.4f}  margins={margins}{flag}")
        for pol, v in viol:
            print(f"      VIOLATION {pol}={v:.4f} > oracle={o:.4f} (gap {v - o:+.4f})")
        if gp:
            for j, x in enumerate(gp):
                print(f"      ex{j}: items={x['n_items']} gold_items={x['n_gold_items']} "
                      f"budget={x['budget_bytes']} gold_union={x['gold_union_bytes']} "
                      f"over_budget={x['gold_union_over_budget']} "
                      f"forced_gold_evictions={x['forced_gold_evictions']}"
                      f"/{x['n_evictions']}")
    print()
    print(f"  comparisons checked            : {dom_checked}")
    print(f"  dominance violations (total)   : {dom_fail}")
    print(f"  ... in cells where gold FITS   : {dom_fail_sound}   <- these would be defects")
    print(f"  ... in cells where gold DOESN'T: {dom_fail - dom_fail_sound}   <- oracle unsound by construction")
    print(f"  cells lacking an oracle        : {cells_without_oracle}")
    print()

    # ── MONOTONICITY ─────────────────────────────────────────────────
    print("=" * 78)
    print(f"MONOTONICITY: {METRIC} non-decreasing in budget_frac, per policy")
    print("=" * 78)
    mono_checked = mono_fail = 0
    mono_flagged = 0
    for key, g in df.groupby(["dataset", "itemiser", "retriever", "policy"]):
        g = g.sort_values("budget_frac")
        if len(g) < 2:
            continue
        series = list(zip(g.budget_frac, g[METRIC]))
        drops = [(b1, v1, b2, v2) for (b1, v1), (b2, v2)
                 in zip(series, series[1:]) if v2 + 1e-12 < v1]
        policy = key[3]
        guaranteed = policy not in NOT_GUARANTEED
        if guaranteed:
            mono_checked += 1
        tag = "ok" if not drops else ("VIOLATION" if guaranteed else "flagged(not-guaranteed)")
        if drops:
            if guaranteed:
                mono_fail += 1
            else:
                mono_flagged += 1
        print(f"  [{tag}] {key}: " +
              " ".join(f"{b:.2f}={v:.4f}" for b, v in series))
        for b1, v1, b2, v2 in drops:
            print(f"        drop {b1:.2f}->{b2:.2f}: {v1:.4f} -> {v2:.4f} "
                  f"({v2 - v1:+.4f})")
    print()
    print(f"  guaranteed series checked : {mono_checked}")
    print(f"  monotonicity violations   : {mono_fail}")
    print(f"  flagged (not guaranteed)  : {mono_flagged}")
    print()

    total = dom_fail_sound + mono_fail
    print("=" * 78)
    print(f"VERDICT: {'PASS' if total == 0 else 'FAIL'} "
          f"(dominance violations in SOUND cells={dom_fail_sound}, "
          f"monotonicity={mono_fail})")
    if dom_fail != dom_fail_sound:
        print(f"NOTE: {dom_fail - dom_fail_sound} further dominance violations occur in cells "
              f"where the gold union exceeds the budget. There the oracle is\n"
              f"      NOT an upper bound -- it is forced to evict gold and degenerates to "
              f"oldest-protected-first. Those cells must be excluded from any\n"
              f"      normalized-regret / '% of achievable headroom' statistic, and "
              f"`forced_gold_evictions` is not emitted anywhere, so they cannot\n"
              f"      currently be identified from the artifacts alone.")
    return 1 if total else 0


if __name__ == "__main__":
    raise SystemExit(main())
