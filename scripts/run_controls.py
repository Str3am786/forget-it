"""D7 — synthetic controls W1/W2/W3 through the REAL pipeline (retention only).

Runs each control workload (benchmark/workloads/synthetic_controls.py) × 5
seeds × {lru, lfu, fifo, random} through run_one_example with a real
RetentionManager (probe_touch_mode="original" — reuse signal ON) at budget
fraction 0.25, and compares measured gold survival against expectations that
are COMPUTED AND PRINTED BEFORE any measured number:

  W1 zipf(s=1.2)  LFU expected = realized top-B popularity mass (fraction of
                  gold inside the top-m of the trace's own access counts —
                  policy-free, computable from the trace alone).
                  LRU expected = Che's approximation over the theoretical
                  popularity law: occupancy(k) = 1 − exp(−p_k·T), T solving
                  Σ_k occupancy = m. Both must also clear random by ≥ 0.3.
  W2 uniform      every policy ≈ the budget-fraction anchor m/N; band = ±3σ
                  of 50 Bernoulli(m/N) draws (5 seeds × 10 gold ≈ ±0.19),
                  measured against random's own measured mean.
  W3 early-gold   fifo/lru/lfu survival EXACTLY 0.0 by the trailing-window
                  formula (equal sizes → resident set at query time = last
                  m inserts; every gold position < N−m); random ≈ anchor.

A W1 failure is a STOP-AND-ASK event (voids history-policy results everywhere).

Output: report/07_controls_results.json + printed verdict table.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

from forgetit.backend.in_memory import InMemoryBackend
from forgetit.benchmark.runner_core import run_one_example
from forgetit.benchmark.workloads.synthetic_controls import (
    BUILDERS, zipf_probs,
)
from forgetit.memory_system import RetentionManager
from forgetit.policies import FIFOPolicy, LRUPolicy, LFUPolicy, RandomPolicy
from forgetit.retrieval.lexical import LexicalOverlapRetriever

REPO = Path(__file__).resolve().parents[1]
SEEDS = [0, 1, 2, 3, 4]
N_ITEMS, ITEM_BYTES, BUDGET_FRAC = 200, 100, 0.25
BUDGET = int(N_ITEMS * ITEM_BYTES * BUDGET_FRAC)          # 5000 → m = 50 items
M_ITEMS = BUDGET // ITEM_BYTES
ZIPF_S = 1.2
POLICIES = {
    "lru": lambda seed: LRUPolicy(),
    "lfu": lambda seed: LFUPolicy(),
    "fifo": lambda seed: FIFOPolicy(),
    "random": lambda seed: RandomPolicy(seed=seed),
}


class RecordingLogger:
    def __init__(self):
        self.events = []

    def event(self, name, payload=None):
        self.events.append((name, dict(payload or {})))


def che_T(probs: list[float], m: int) -> float:
    """Solve Σ_k (1 − e^{−p_k T}) = m for T (bisection)."""
    lo, hi = 1.0, 1e7
    for _ in range(200):
        mid = (lo + hi) / 2
        val = sum(1 - math.exp(-p * mid) for p in probs)
        if val < m:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def random_first_order(gold_indices: list[int]) -> float:
    """Position-aware first-order random anchor for a SINGLE-INSERT stream:
    an item inserted at 0-based position i faces E_i = N − max(i+1, m)
    evictions, surviving each with (m−1)/m (uniform victim among the m
    residents; incoming exempt). The naive P ≈ budget_fraction anchor is the
    special case of UNIFORM gold positions (it averages to m/N); for early
    gold (W1/W3) it is wildly wrong and this formula is the honest anchor."""
    p = (M_ITEMS - 1) / M_ITEMS
    vals = [p ** (N_ITEMS - max(i + 1, M_ITEMS)) for i in gold_indices]
    return sum(vals) / len(vals)


def expected_w1(traces) -> dict:
    probs = zipf_probs(N_ITEMS, ZIPF_S)
    T = che_T(probs, M_ITEMS)
    lru_exp = sum(1 - math.exp(-probs[g] * T) for g in traces[0].gold_indices) \
        / len(traces[0].gold_indices)
    lfu_exps = []
    for tr in traces:
        ranked = sorted(range(N_ITEMS), key=lambda i: (-tr.access_counts.get(i, 0), i))
        top_m = set(ranked[:M_ITEMS])
        lfu_exps.append(sum(1 for g in tr.gold_indices if g in top_m) / len(tr.gold_indices))
    return {"lru_che": round(lru_exp, 4), "che_T_accesses": round(T, 1),
            "lfu_top_b_mass_per_seed": [round(x, 4) for x in lfu_exps],
            "lfu_top_b_mass_mean": round(sum(lfu_exps) / len(lfu_exps), 4),
            "random_anchor": M_ITEMS / N_ITEMS}


def probe_miss_rate(trace, logger) -> float:
    """Fraction of probes whose intended target was NOT resident (backfill
    touched the newest resident instead) — the quantified W1 artifact."""
    target_by_n = {}
    for e in trace.events:
        if e.type == "query" and e.query is not None and "::probe::" in e.query.id:
            n = int(e.query.id.rsplit("::", 1)[1])
            target_by_n[n] = e.query.meta.get("target")
    if not target_by_n:
        return 0.0
    resident: set[str] = set()
    misses = total = 0
    for name, p in logger.events:
        if name == "insert":
            resident.add(p["id"])
        elif name == "evict":
            resident.discard(p["id"])
        elif name == "retrieve" and "::probe::" in p.get("q", ""):
            n = int(p["q"].rsplit("::", 1)[1])
            total += 1
            if target_by_n.get(n) not in resident:
                misses += 1
    return misses / total if total else 0.0


def run_cell(workload: str, seed: int, policy_name: str) -> dict:
    trace = BUILDERS[workload](seed)
    logger = RecordingLogger()
    store = RetentionManager(
        budget_bytes=BUDGET,
        backend=InMemoryBackend(retriever=LexicalOverlapRetriever()),
        policy=POLICIES[policy_name](seed),
        logger=logger,
        probe_touch_mode="original",
    )
    store.connect()
    results, _, _, _ = run_one_example(
        trace.events, store, k=1, dataset=f"ctrl_{workload}",
        store_label=f"ctrl/{policy_name}", policy=policy_name, seed=seed)
    survival = sum(r.gold_survived for r in results) / len(results)
    return {"survival": survival, "n_scored": len(results),
            "probe_miss_rate": round(probe_miss_rate(trace, logger), 4)}


def main() -> None:
    out = {"config": {"n_items": N_ITEMS, "item_bytes": ITEM_BYTES,
                      "budget_bytes": BUDGET, "m_items": M_ITEMS,
                      "budget_frac": BUDGET_FRAC, "zipf_s": ZIPF_S,
                      "seeds": SEEDS}}

    # ── expectations FIRST (computed from theory + the traces alone) ──
    w1_traces = [BUILDERS["w1"](s) for s in SEEDS]
    w3_traces = [BUILDERS["w3"](s) for s in SEEDS]
    w1_rand_anchor = random_first_order(w1_traces[0].gold_indices)
    w3_rand_anchor = sum(random_first_order(t.gold_indices) for t in w3_traces) / len(w3_traces)
    exp = {
        "w1": {**expected_w1(w1_traces),
               "random_first_order": round(w1_rand_anchor, 4)},
        "w2": {"anchor": M_ITEMS / N_ITEMS,
               "band_3sigma": round(3 * math.sqrt(0.25 * 0.75 / (len(SEEDS) * 10)), 4)},
        "w3": {"history_trailing_window": 0.0,
               "window_formula": f"resident set at query time = last m={M_ITEMS} "
                                 f"inserts; max gold position 19 < {N_ITEMS - M_ITEMS}",
               "random_first_order": round(w3_rand_anchor, 4)},
    }
    out["expected"] = exp
    print("EXPECTED (pre-measurement):")
    print(json.dumps(exp, indent=1))

    # ── measured ──
    measured: dict[str, dict[str, list[float]]] = {}
    miss_rates: dict[str, dict[str, list[float]]] = {}
    for w in ("w1", "w2", "w3"):
        measured[w] = {}
        miss_rates[w] = {}
        for pol in POLICIES:
            cells = [run_cell(w, s, pol) for s in SEEDS]
            measured[w][pol] = [c["survival"] for c in cells]
            miss_rates[w][pol] = [c["probe_miss_rate"] for c in cells]
    out["measured_survival"] = measured
    out["probe_miss_rates"] = miss_rates

    def mean(xs):
        return sum(xs) / len(xs)

    # ── verdicts ──
    verdicts = {}
    m1, e1 = measured["w1"], exp["w1"]
    rnd1 = mean(m1["random"])
    verdicts["w1"] = {
        "lfu_measured": mean(m1["lfu"]), "lfu_expected": e1["lfu_top_b_mass_mean"],
        "lfu_within_0.10": abs(mean(m1["lfu"]) - e1["lfu_top_b_mass_mean"]) <= 0.10,
        "lru_measured": mean(m1["lru"]), "lru_expected_che": e1["lru_che"],
        "lru_within_0.15": abs(mean(m1["lru"]) - e1["lru_che"]) <= 0.15,
        "random_measured": rnd1,
        "lfu_beats_random_by_0.3": mean(m1["lfu"]) >= rnd1 + 0.3,
        "lru_beats_random_by_0.3": mean(m1["lru"]) >= rnd1 + 0.3,
    }
    verdicts["w1"]["PASS"] = all(v for k, v in verdicts["w1"].items()
                                 if isinstance(v, bool))

    m2 = measured["w2"]
    rnd2 = mean(m2["random"])
    band = exp["w2"]["band_3sigma"]
    checks2 = {f"{p}_within_band_of_random": abs(mean(m2[p]) - rnd2) <= band
               for p in ("lru", "lfu", "fifo")}
    verdicts["w2"] = {**{f"{p}_measured": mean(m2[p]) for p in POLICIES},
                      **checks2, "band": band, "PASS": all(checks2.values())}

    m3 = measured["w3"]
    rnd3 = mean(m3["random"])
    checks3 = {f"{p}_exactly_zero": mean(m3[p]) == 0.0
               for p in ("lru", "lfu", "fifo")}
    # random anchor: position-aware first-order value (early gold), 3σ band
    # over 50 Bernoulli(anchor) draws.
    anchor3 = exp["w3"]["random_first_order"]
    band3 = 3 * math.sqrt(anchor3 * (1 - anchor3) / (len(SEEDS) * 10))
    verdicts["w3"] = {**{f"{p}_measured": mean(m3[p]) for p in POLICIES},
                      **checks3,
                      "random_above_history": rnd3 > 0.0,
                      "random_near_first_order_anchor": abs(rnd3 - anchor3) <= band3,
                      "first_order_anchor": anchor3, "anchor_band": round(band3, 4),
                      "PASS": all(checks3.values()) and rnd3 > 0.0
                              and abs(rnd3 - anchor3) <= band3}

    out["verdicts"] = verdicts
    print("\nMEASURED + VERDICTS:")
    print(json.dumps({"measured_survival_means":
                      {w: {p: round(mean(v), 4) for p, v in ps.items()}
                       for w, ps in measured.items()},
                      "w1_probe_miss_mean":
                      {p: round(mean(v), 4) for p, v in miss_rates["w1"].items()},
                      "verdicts": verdicts}, indent=1, default=str))

    (REPO / "report" / "07_controls_results.json").write_text(
        json.dumps(out, indent=1, default=str))
    print("\nwrote report/07_controls_results.json")


if __name__ == "__main__":
    main()
