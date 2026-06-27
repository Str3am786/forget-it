#!/usr/bin/env python3
"""
Family A — workload characterization (TRACE ONLY: no policy, no retriever, no
reader, no LLM).  Consumes each workload's per-example demand trace via
``iter_examples()`` with ``probe_every=0`` (synthetic recency probes stripped),
and computes:

  A0. Reference-count distribution        — the GATE (single- vs repeated-ref)
  A1. Reuse-distance distribution         — creation-to-need (write->query) for
                                            single-ref items; classical IRD for
                                            repeated-ref items
  A2. Working-set-size curve              — distinct items per window
  A3. MRC via Mattson stack distance      — in items AND bytes AND budget_frac
  A4. Marginal hit rate                   — the MRC slope (budget band)

Everything is a deterministic function of (dataset, itemiser).  The LLM and the
embedding model never enter here.  Corpus semantic redundancy (the embedding-
based axis) lives in a separate script.

Run:
    python scripts/characterize_workload.py --limit 20      # smoke
    python scripts/characterize_workload.py                 # full
"""
from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

import numpy as np

from forgetit.benchmark.workloads.longmemeval import (
    LongMemEvalWorkload, TurnPairItemiser, SessionItemiser,
)
from forgetit.benchmark.workloads.beam import (
    BeamWorkload, BeamTurnPairItemiser, BeamBatchItemiser,
)

DATA = Path("data")
LME_PATH = DATA / "longmemeval_s_cleaned.json"
BEAM_PATH = DATA / "beam_500K.jsonl"

INF = float("inf")


# ───────────────────────── Fenwick (BIT) ──────────────────────────
class Fenwick:
    """Prefix-sum BIT over float weights at integer positions [0, n)."""
    __slots__ = ("n", "t")

    def __init__(self, n: int):
        self.n = n
        self.t = [0.0] * (n + 1)

    def add(self, i: int, w: float) -> None:
        i += 1
        while i <= self.n:
            self.t[i] += w
            i += i & (-i)

    def _pref(self, i: int) -> float:
        # sum over positions [0, i]  (i is 0-based inclusive)
        i += 1
        s = 0.0
        while i > 0:
            s += self.t[i]
            i -= i & (-i)
        return s

    def range_sum(self, a: int, b: int) -> float:
        # inclusive [a, b]
        if b < a:
            return 0.0
        lo = self._pref(a - 1) if a > 0 else 0.0
        return self._pref(b) - lo


# ───────────────────────── per-example trace ──────────────────────
def byte_size(text: str) -> int:
    return len(text.encode("utf-8"))


@dataclass
class GoldAccess:
    sd_items: float          # Mattson stack distance in DISTINCT items
    sd_bytes: float          # stack distance in bytes (sum of distinct item sizes)
    frac: float              # sd_bytes / example_footprint_bytes
    repeated: bool           # False => creation-to-need (1st ref); True => IRD
    question_type: str


@dataclass
class ExampleStats:
    n_items: int
    footprint_bytes: int
    n_queries: int                       # scored queries (>=1 gold)
    ref_counts: Counter                  # item_id -> #scored queries referencing it
    gold_accesses: list[GoldAccess]
    n_gold_unwritten: int = 0            # gold referenced but never inserted (should be ~0)


def analyze_example(events) -> ExampleStats | None:
    """Build the demand reference string for ONE example and run a Mattson pass."""
    # 1. ordered inserts (installs) + sizes
    insert_ids: list[str] = []
    size_of: dict[str, int] = {}
    for ev in events:
        if ev.type == "insert" and ev.item is not None:
            if ev.item.id not in size_of:           # first install wins
                insert_ids.append(ev.item.id)
                size_of[ev.item.id] = byte_size(ev.item.text or "")
    if not insert_ids:
        return None
    footprint = sum(size_of.values())

    # 2. scored queries (gold_item_ids non-empty), in chronological order
    scored = [ev for ev in events
              if ev.type == "query" and ev.query is not None and ev.gold_item_ids]

    ref_counts: Counter = Counter()
    for ev in scored:
        for gid in set(ev.gold_item_ids):
            ref_counts[gid] += 1

    # 3. Mattson pass over the access sequence: all inserts, then each query's
    #    gold items (a query references its gold set at one logical instant; we
    #    lay them out at consecutive sub-positions in a stable order).
    seq: list[tuple[str, str]] = [(iid, "insert") for iid in insert_ids]
    # tag each query access with whether the item was ALREADY query-referenced
    seen_in_query: set[str] = set()
    query_access: list[tuple[str, bool, str]] = []   # (id, repeated?, qtype)
    for ev in scored:
        qtype = ev.question_type or (ev.query.meta.get("question_type", "") if ev.query else "")
        for gid in ev.gold_item_ids:
            repeated = gid in seen_in_query
            seen_in_query.add(gid)
            query_access.append((gid, repeated, qtype))
    seq += [(iid, "query") for (iid, _r, _q) in query_access]

    n = len(seq)
    fen_cnt = Fenwick(n)
    fen_byt = Fenwick(n)
    last_pos: dict[str, int] = {}

    gold_accesses: list[GoldAccess] = []
    n_unwritten = 0
    qa_idx = 0
    for p, (iid, kind) in enumerate(seq):
        s = float(size_of.get(iid, 0))
        prev = last_pos.get(iid)
        if prev is not None:
            sd_items = fen_cnt.range_sum(prev + 1, p - 1)
            sd_bytes = fen_byt.range_sum(prev + 1, p - 1)
            fen_cnt.add(prev, -1.0)
            fen_byt.add(prev, -s)
        else:
            sd_items = INF
            sd_bytes = INF
        fen_cnt.add(p, 1.0)
        fen_byt.add(p, s)
        last_pos[iid] = p

        if kind == "query":
            _id, repeated, qtype = query_access[qa_idx]
            qa_idx += 1
            if prev is None:                      # gold never written
                n_unwritten += 1
                continue
            frac = (sd_bytes / footprint) if footprint > 0 else INF
            gold_accesses.append(GoldAccess(
                sd_items=sd_items, sd_bytes=sd_bytes, frac=frac,
                repeated=repeated, question_type=qtype,
            ))

    return ExampleStats(
        n_items=len(insert_ids),
        footprint_bytes=footprint,
        n_queries=len(scored),
        ref_counts=ref_counts,
        gold_accesses=gold_accesses,
        n_gold_unwritten=n_unwritten,
    )


# ───────────────────────── aggregation ────────────────────────────
def pctl(a: np.ndarray, ps=(0, 5, 25, 50, 75, 95, 100)) -> dict:
    if a.size == 0:
        return {f"p{p}": None for p in ps}
    return {f"p{p}": float(np.percentile(a, p)) for p in ps}


def survival_curve(fracs: np.ndarray, grid) -> dict:
    """survival(f) = fraction of gold accesses whose byte-stack-distance, as a
    fraction of the example footprint, is < f  (i.e. gold still resident at
    budget_frac=f under an oracle-size LRU)."""
    if fracs.size == 0:
        return {f"{g:.2f}": None for g in grid}
    return {f"{g:.2f}": float(np.mean(fracs < g)) for g in grid}


def characterize(name: str, examples: Iterator[tuple[str, list]]) -> dict:
    per_ex: list[ExampleStats] = []
    for _eid, evs in examples:
        st = analyze_example(evs)
        if st is not None:
            per_ex.append(st)

    n_ex = len(per_ex)
    # ---- footprint / shape ----
    items_per_ex = np.array([s.n_items for s in per_ex], float)
    bytes_per_ex = np.array([s.footprint_bytes for s in per_ex], float)
    q_per_ex = np.array([s.n_queries for s in per_ex], float)

    # ---- A0: reference-count distribution (the gate) ----
    # pooled over all REFERENCED items (count>=1); per example, referenced items
    all_ref_counts: list[int] = []
    gold_per_query: list[int] = []
    for s in per_ex:
        all_ref_counts.extend(s.ref_counts.values())
        # avg gold items per scored query in this example
    rc = Counter(all_ref_counts)
    n_referenced = sum(rc.values())
    n_single = rc.get(1, 0)
    n_repeated = n_referenced - n_single
    rc_hist = {str(k): rc[k] for k in sorted(rc)}

    # ---- A1: reuse-distance (split single vs repeated) ----
    cn_items, cn_bytes, cn_frac = [], [], []     # creation-to-need (1st ref)
    ird_items, ird_bytes = [], []                # classical IRD (re-ref)
    all_frac = []
    for s in per_ex:
        for g in s.gold_accesses:
            all_frac.append(g.frac)
            if g.repeated:
                if math.isfinite(g.sd_items):
                    ird_items.append(g.sd_items)
                    ird_bytes.append(g.sd_bytes)
            else:
                if math.isfinite(g.sd_items):
                    cn_items.append(g.sd_items)
                    cn_bytes.append(g.sd_bytes)
                    cn_frac.append(g.frac)
    cn_items_a = np.array(cn_items, float)
    cn_bytes_a = np.array(cn_bytes, float)
    cn_frac_a = np.array(cn_frac, float)
    ird_items_a = np.array(ird_items, float)
    all_frac_a = np.array([f for f in all_frac if math.isfinite(f)], float)

    # ---- A3/A4: MRC + marginal, over budget_frac ----
    grid = [round(x, 3) for x in np.concatenate([
        np.linspace(0.01, 0.1, 10), np.linspace(0.15, 1.0, 18)])]
    surv = survival_curve(all_frac_a, grid)
    # marginal hit rate ~ d(survival)/d(frac) between grid points
    marg = {}
    gk = list(surv.keys())
    for i in range(1, len(gk)):
        f0, f1 = float(gk[i - 1]), float(gk[i])
        s0, s1 = surv[gk[i - 1]], surv[gk[i]]
        if s0 is not None and s1 is not None and f1 > f0:
            marg[f"{f0:.2f}->{f1:.2f}"] = round((s1 - s0) / (f1 - f0), 3)

    # MRC in item units (absolute slots) — pooled creation-to-need
    item_grid = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]
    mrc_items = {}
    if cn_items_a.size:
        for c in item_grid:
            mrc_items[str(c)] = float(np.mean(cn_items_a < c))

    return {
        "workload": name,
        "n_examples": n_ex,
        "footprint": {
            "items_per_example": pctl(items_per_ex),
            "bytes_per_example": pctl(bytes_per_ex),
            "scored_queries_per_example": pctl(q_per_ex),
            "total_items": int(items_per_ex.sum()),
            "total_scored_queries": int(q_per_ex.sum()),
        },
        "A0_reference_counts": {
            "n_referenced_items": n_referenced,
            "single_reference": n_single,
            "repeated_reference": n_repeated,
            "pct_single": round(100 * n_single / n_referenced, 2) if n_referenced else None,
            "pct_repeated": round(100 * n_repeated / n_referenced, 2) if n_referenced else None,
            "histogram_refcount": rc_hist,
            "gate_verdict": (
                "DEGENERATE: ~all gold single-reference -> Zipf/popularity/hazard = N/A"
                if n_referenced and n_repeated / n_referenced < 0.02
                else "repeated references present -> popularity/hazard metrics meaningful"
            ),
        },
        "A1_reuse_distance": {
            "creation_to_need_single_ref": {
                "n": int(cn_items_a.size),
                "items": pctl(cn_items_a),
                "bytes": pctl(cn_bytes_a),
                "frac_of_footprint": pctl(cn_frac_a),
            },
            "classical_ird_repeated_ref": {
                "n": int(ird_items_a.size),
                "items": pctl(ird_items_a),
            },
        },
        "A3_MRC_budget_frac": surv,
        "A4_marginal_hit_rate": marg,
        "A3_MRC_items": mrc_items,
    }


# ───────────────────────── workload builders ──────────────────────
def builders(limit):
    out = []
    if LME_PATH.exists():
        out.append(("longmemeval/turn_pair",
                    LongMemEvalWorkload(LME_PATH, TurnPairItemiser(),
                                        probe_every=0, limit=limit)))
        out.append(("longmemeval/session",
                    LongMemEvalWorkload(LME_PATH, SessionItemiser(),
                                        probe_every=0, limit=limit)))
    if BEAM_PATH.exists():
        out.append(("beam/turn_pair",
                    BeamWorkload(BEAM_PATH, BeamTurnPairItemiser(),
                                 probe_every=0, limit=limit)))
        out.append(("beam/batch",
                    BeamWorkload(BEAM_PATH, BeamBatchItemiser(),
                                 probe_every=0, limit=limit)))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None,
                    help="max examples per workload (smoke test)")
    ap.add_argument("--out", type=str, default="artifacts/workload_characterization.json")
    args = ap.parse_args()

    results = []
    for name, wl in builders(args.limit):
        print(f"\n=== {name} ===")
        res = characterize(name, wl.iter_examples())
        results.append(res)
        a0 = res["A0_reference_counts"]
        print(f"  examples={res['n_examples']}  total_items={res['footprint']['total_items']}"
              f"  scored_q={res['footprint']['total_scored_queries']}")
        print(f"  items/ex p50={res['footprint']['items_per_example']['p50']}"
              f"  bytes/ex p50={res['footprint']['bytes_per_example']['p50']:.0f}")
        print(f"  [A0 gate] referenced={a0['n_referenced_items']}"
              f"  single={a0['pct_single']}%  repeated={a0['pct_repeated']}%")
        print(f"           {a0['gate_verdict']}")
        cn = res["A1_reuse_distance"]["creation_to_need_single_ref"]
        print(f"  [A1] creation->need frac-of-footprint p50={cn['frac_of_footprint']['p50']}"
              f"  p95={cn['frac_of_footprint']['p95']}")
        print(f"  [A3] survival @frac 0.10={res['A3_MRC_budget_frac'].get('0.10')}"
              f"  0.25={res['A3_MRC_budget_frac'].get('0.25')}"
              f"  0.50={res['A3_MRC_budget_frac'].get('0.50')}")

    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(results, indent=2))
    print(f"\nwrote {outp}")


if __name__ == "__main__":
    main()
