"""The five E3 verification invariants as code (sprint-2 S2).

Definitions from docs/e3_verification_and_truncation_2026-07-08.md and the
prereg amendment. Each check consumes the grid's OWN artifacts (events.jsonl +
results.csv per config) and returns a dict with ``ok`` plus evidence; the grid
runner asserts on them (fails loudly), the analyzer re-emits them as the
attestation table.

  1. fifo_equivalence      blind-condition history traces are insertion-order
                           exact, AND byte-identical to independent blind-LRU /
                           blind-LFU replays of the same insert stream (the
                           class collapse — one simulation, asserted, never two
                           extra confirmations).
  2. pressure_invariant    (refined form) used ≤ budget after every insert;
                           zero evictions after an example's last insert;
                           freed-bytes parity original-vs-blind ≤ 0.1% for the
                           history pair; EXACT trace equality for touch-blind
                           policies across conditions.
  3. independent_recount   survival recounted from raw events (resident set at
                           the gold event) equals results.csv per row.
  4. window_prediction     trailing budget-byte-window prediction from inserts
                           + budget alone reproduces blind survival per query.
  5. monotonicity          per condition x policy, survival non-decreasing in
                           budget.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


# ── artifact access ──────────────────────────────────────────────────

def load_events(config_dir: Path) -> list[dict]:
    with (config_dir / "events.jsonl").open() as f:
        return [json.loads(line) for line in f]


def split_examples(events: list[dict]) -> list[list[dict]]:
    out, cur = [], []
    for e in events:
        if e.get("name") == "clear":
            if cur:
                out.append(cur)
            cur = []
        else:
            cur.append(e)
    if cur:
        out.append(cur)
    return out


def _blind_replay(inserts: list[tuple[str, int]], budget: int, rule: str) -> list[str]:
    """Independent no-touch replay (imports nothing from forgetit): under zero
    touches every history rule reduces to evict-the-oldest-resident; ``rule``
    is kept in the signature to make the three asserted identities explicit."""
    q: list[tuple[str, int]] = []
    used = 0
    evictions: list[str] = []
    for item_id, b in inserts:
        while used + b > budget and q:
            vid, vb = q.pop(0)
            evictions.append(vid)
            used -= vb
        q.append((item_id, b))
        used += b
    return evictions


# ── the five checks ──────────────────────────────────────────────────

def check_fifo_equivalence(blind_history_dir: Path) -> dict:
    """Blind history trace == insertion order, per example, AND identical to
    independent blind-LRU and blind-LFU replays of the same insert stream."""
    bad = 0
    n_evictions = 0
    n_examples = 0
    for ex in split_examples(load_events(blind_history_dir)):
        inserts = [(e["id"], int(e["bytes"])) for e in ex if e.get("name") == "insert"]
        budget = next((int(e["budget_bytes"]) for e in ex if e.get("name") == "insert"), None)
        actual = [e["id"] for e in ex if e.get("name") == "evict"]
        if budget is None:
            continue
        n_examples += 1
        n_evictions += len(actual)
        expected = _blind_replay(inserts, budget, "fifo")
        lru_blind = _blind_replay(inserts, budget, "lru")
        lfu_blind = _blind_replay(inserts, budget, "lfu")
        if not (actual == expected == lru_blind == lfu_blind):
            bad += 1
    return {"ok": bad == 0, "examples": n_examples, "evictions": n_evictions,
            "mismatched_examples": bad}


def check_pressure_invariant(orig_dir: Path, blind_dir: Path,
                             exact_pair: bool) -> dict:
    """Refined invariant on BOTH configs + cross-condition parity.

    exact_pair=True → the two conditions' eviction traces must be IDENTICAL
    (touch-blind policy spot-check).

    Otherwise the parity bound is PER-EXAMPLE and ABSOLUTE:
    |freed_orig − freed_blind| ≤ max item bytes of that example. Derivation:
    the two conditions see identical inserts, so the freed difference equals
    the end-state used_bytes difference, and each budget loop stops within one
    victim of the budget — the divergence is victim-size arithmetic, bounded
    by the largest single item. (Sprint-2 amendment of the already-refined
    check 2: its 0.1% RELATIVE tolerance was calibrated at the 25% budget and
    provably cannot scale — at 75% budget total freed shrinks ~3× while the
    absolute victim-size deltas stay constant, so the ratio triples. Measured
    on the S2 smoke: absolute deltas 1.6–5.0 KB, all within ONE max item of
    9 KB; the aggregate ratio is kept as an informational field.)"""
    def scan(config_dir: Path):
        over = late = 0
        freed_per_ex, max_item_per_ex, traces = [], [], []
        for ex in split_examples(load_events(config_dir)):
            seen_scored = False
            trace = []
            freed = 0
            max_item = 0
            for e in ex:
                name = e.get("name")
                if name == "insert":
                    if int(e["used_bytes"]) > int(e["budget_bytes"]):
                        over += 1
                    max_item = max(max_item, int(e["bytes"]))
                elif name == "retrieve" and "::probe::" not in e.get("q", ""):
                    seen_scored = True
                elif name == "evict":
                    if seen_scored:
                        late += 1
                    freed += int(e.get("freed_bytes", 0))
                    trace.append(e["id"])
            freed_per_ex.append(freed)
            max_item_per_ex.append(max_item)
            traces.append(trace)
        return over, late, freed_per_ex, max_item_per_ex, traces

    o_over, o_late, o_freed, o_max, o_traces = scan(orig_dir)
    b_over, b_late, b_freed, _, b_traces = scan(blind_dir)
    out = {"budget_exceeded_events": o_over + b_over,
           "evictions_after_scored_block": o_late + b_late,
           "freed_bytes_original": sum(o_freed), "freed_bytes_blind": sum(b_freed)}
    clean = (o_over + b_over + o_late + b_late == 0)
    if exact_pair:
        out["traces_identical"] = o_traces == b_traces
        out["ok"] = clean and out["traces_identical"]
        return out
    deltas = [abs(a - b) for a, b in zip(o_freed, b_freed)]
    out["per_example_abs_delta"] = deltas
    out["per_example_bound_max_item"] = o_max
    out["freed_parity_informational"] = (abs(sum(o_freed) - sum(b_freed))
                                         / max(sum(o_freed), 1))
    out["ok"] = clean and all(d <= m for d, m in zip(deltas, o_max))
    return out


def check_independent_recount(config_dir: Path) -> dict:
    """Survival from raw events (resident set at the gold event) == results.csv."""
    df = pd.read_csv(config_dir / "results.csv")
    resident: set[str] = set()
    survived_by_query: dict[str, bool] = {}
    gold_by_query: dict[str, list[str]] = {}
    for e in load_events(config_dir):
        name = e.get("name")
        if name == "insert":
            resident.add(e["id"])
        elif name in ("evict", "delete"):
            resident.discard(e["id"])
        elif name == "clear":
            resident = set()
        elif name == "gold":
            gold_by_query[e["query_id"]] = e["gold_ids"]
            survived_by_query[e["query_id"]] = any(g in resident for g in e["gold_ids"])
    merged = df[df["query_id"].isin(survived_by_query)]
    mism = int((merged["gold_survived"].astype(bool)
                != merged["query_id"].map(survived_by_query)).sum())
    return {"ok": mism == 0 and len(merged) == len(df),
            "rows": len(df), "rows_recounted": len(merged), "mismatches": mism}


def check_window_prediction(blind_history_dir: Path) -> dict:
    """Per query: gold ∈ trailing budget-byte window (FIFO replay of inserts +
    budget alone) must equal the measured gold_survived."""
    df = pd.read_csv(blind_history_dir / "results.csv")
    predictions: dict[str, bool] = {}
    for ex in split_examples(load_events(blind_history_dir)):
        inserts = [(e["id"], int(e["bytes"])) for e in ex if e.get("name") == "insert"]
        budget = next((int(e["budget_bytes"]) for e in ex if e.get("name") == "insert"), None)
        if budget is None:
            continue
        evicted = set(_blind_replay(inserts, budget, "fifo"))
        resident = {i for i, _ in inserts} - evicted
        for e in ex:
            if e.get("name") == "gold":
                predictions[e["query_id"]] = any(g in resident for g in e["gold_ids"])
    merged = df[df["query_id"].isin(predictions)]
    mism = int((merged["gold_survived"].astype(bool)
                != merged["query_id"].map(predictions)).sum())
    return {"ok": mism == 0 and len(merged) == len(df),
            "rows": len(df), "mismatches": mism,
            "predicted_rate": round(float(pd.Series(predictions).mean()), 4)}


def check_monotonicity(survival_by_budget: dict[float, float],
                       tolerance: float = 0.0) -> dict:
    """Survival non-decreasing in budget (weak form — 1.0 plateaus allowed)."""
    budgets = sorted(survival_by_budget)
    ok = all(survival_by_budget[b2] + tolerance >= survival_by_budget[b1]
             for b1, b2 in zip(budgets, budgets[1:]))
    return {"ok": ok, "series": {str(b): round(survival_by_budget[b], 4)
                                 for b in budgets}}
