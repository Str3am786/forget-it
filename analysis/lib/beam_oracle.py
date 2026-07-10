"""Trace-level oracle + idealized baselines, recomputed from THIS run's events.

Matches scripts/oracle_size_analysis.py semantics, but reads insert ``bytes`` and
gold from the run's own budget=1.0 event log (artifacts/benchmark_full/...) rather
than re-itemising — so the ceiling is derived from the exact bytes the run used,
not a possibly-stale committed report.

Per scored query we compute the MINIMUM budget fraction at which all of its gold
survives, under (all inserts precede queries here, so these closed forms are exact):
  oracle_qcond  — keep exactly this query's gold      B* = Σ gold bytes
  keep_smallest — admit items ascending by size        B* = max_g cumsize(size ≤ size_g)
  recency_lru   — keep most-recently-inserted bytes     B* = max_g (size_g + bytes_after_g)
  union_wt      — write-time policy must keep the whole example's gold union (shared)

survival_policy(f) = P_over_queries(B*_policy ≤ f). Returned per (example, query)
so aggregates can be example-cluster bootstrapped. This is the honest ceiling
(oracle is a greedy/write-time proxy, NOT the NP-hard optimum — label it so).
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

from beam_events import _events_path


def _per_example(events):
    size_of, order = {}, []
    query_golds = []
    for ev in events:
        n = ev.get("name")
        if n == "insert":
            iid = ev["id"]
            if iid not in size_of:
                size_of[iid] = int(ev.get("bytes", 0))
                order.append(iid)
        elif n == "gold":
            gold = [g for g in ev.get("gold_ids", []) if g in size_of]
            query_golds.append((ev.get("query_id", ""), gold))
    footprint = sum(size_of.values())
    if footprint == 0:
        return []

    bytes_after, run = {}, 0
    for iid in reversed(order):
        bytes_after[iid] = run
        run += size_of[iid]

    sizes_sorted = np.sort(np.fromiter(size_of.values(), float))
    csum = np.cumsum(sizes_sorted)

    def ks_cost(s):
        idx = np.searchsorted(sizes_sorted, s, side="right")
        return float(csum[idx - 1]) if idx > 0 else 0.0

    union = set().union(*[set(g) for _q, g in query_golds]) if query_golds else set()
    union_frac = sum(size_of[g] for g in union) / footprint

    rows = []
    for qid, gold in query_golds:
        if not gold:
            continue
        rows.append({
            "query_id": qid,
            "oracle_qcond": sum(size_of[g] for g in gold) / footprint,
            "keep_smallest": max(ks_cost(size_of[g]) for g in gold) / footprint,
            "recency_lru": max(size_of[g] + bytes_after[g] for g in gold) / footprint,
            "union_wt": union_frac,
        })
    return rows


def trace_criticals(itemiser: str) -> pd.DataFrame:
    """Per-(example, query) critical budget fractions for one itemiser."""
    path = _events_path(itemiser)
    ex_events, cur = [], []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            ev = json.loads(line)
            if ev.get("name") == "clear":
                ex_events.append(cur); cur = []
            else:
                cur.append(ev)
    ex_events.append(cur)

    rows = []
    for eidx, evs in enumerate(ex_events):
        for r in _per_example(evs):
            rows.append({"example_idx": eidx, "itemiser": itemiser, **r})
    return pd.DataFrame(rows)


def survival_curve(crit: pd.DataFrame, policy_col: str, fracs) -> dict:
    """survival(f) = mean_over_queries(B* ≤ f) for one idealized policy column."""
    b = crit[policy_col].to_numpy()
    return {f: float(np.mean(b <= f)) for f in fracs}


if __name__ == "__main__":
    from beam_load import BUDGET_FRACS, ITEMISERS
    for it in ITEMISERS:
        crit = trace_criticals(it)
        print(f"\n=== {it}  (n_query={len(crit)}) ===")
        for pol in ("oracle_qcond", "union_wt", "keep_smallest", "recency_lru"):
            sc = survival_curve(crit, pol, BUDGET_FRACS)
            print(f"  {pol:14s} " + "  ".join(f"{f:g}:{sc[f]:.2f}" for f in BUDGET_FRACS))
