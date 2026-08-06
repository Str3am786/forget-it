"""Synthetic control workloads W1–W3 (VERIFICATION.md §2) — the truth machine.

Emits the EXACT production event schema (WorkloadEvent insert/query streams,
probe queries stamped ``meta={"probe": True}`` with ``::probe::`` ids, scored
queries carrying gold labels) so the REAL pipeline (run_one_example →
RetentionManager → retriever) replays them unmodified. Retention stage only:
scored queries exist to read ``gold_id_retained`` — no reader.

Design (all workloads): N items of EQUAL size (size-CV = 0, so size policies
are inert and the history axis is isolated), inserted in order; access-only
probes interleave with the inserts (this is where reuse lives); scored queries
fire after the last insert, one per gold item.

  W1 zipf-reuse   — after each insert, R probes drawn iid from a Zipf(s)
                    popularity law over the items inserted so far (law over
                    a-priori item ranks; item 1 = most popular). Gold = the
                    top-G popularity items. Positive control: LFU ≈ realized
                    top-B popularity mass, LRU per Che's approximation, both
                    ≫ random.
  W2 uniform      — one probe per insert, targeting the item JUST inserted
                    (single access each — an ordering no-op). Gold positions
                    uniform. Negative control: every history policy ≡ random
                    within the MC band.
  W3 early-gold   — no probes at all; gold positions confined to the FIRST
                    decile of the insert stream. Trap: FIFO/blind-history
                    survival is 0 by the trailing-window formula; random ≈
                    the budget fraction.

Probe-miss artifact (quantified by the runner, not hidden): a probe whose
Zipf target was already evicted retrieves over zero-score residents and the
retriever's tie-break backfills the NEWEST resident — touching it. Touching
the newest resident is an ordering no-op for LRU and a +1 count for LFU, so
the distortion is small; the runner reports the realized miss rate per cell.
"""
from __future__ import annotations

import random
from dataclasses import dataclass

from forgetit.benchmark.schema import WorkloadEvent
from forgetit.core.schema import MemRecord, Query


def _token(i: int) -> str:
    return f"tok{i:04d}"


@dataclass
class ControlTrace:
    name: str
    seed: int
    events: list[WorkloadEvent]
    gold_indices: list[int]          # item indices (0-based insert order)
    access_counts: dict[int, int]    # realized probe-target draws per item index
    n_items: int
    item_bytes: int


def _item(i: int) -> MemRecord:
    text = f"{_token(i)} synthetic"
    return MemRecord(id=f"it{i:04d}", text=text, created_at=float(i + 1),
                     bytes=100)


def _probe(name: str, n: int, target_idx: int) -> WorkloadEvent:
    return WorkloadEvent(
        type="query", timestamp=float(n), item=None,
        query=Query(id=f"{name}::probe::{n}", text=_token(target_idx),
                    timestamp=float(n), meta={"probe": True,
                                              "target": f"it{target_idx:04d}"}),
        gold_item_ids=[],
    )


def _scored(name: str, j: int, gold_idx: int, n_items: int) -> WorkloadEvent:
    return WorkloadEvent(
        type="query", timestamp=float(n_items + j), item=None,
        query=Query(id=f"{name}::q::{j}", text=_token(gold_idx),
                    timestamp=float(n_items + j), meta={}),
        gold_item_ids=[f"it{gold_idx:04d}"],
        ref_answer="", question_type="synthetic",
    )


def zipf_probs(n: int, s: float) -> list[float]:
    w = [1.0 / (k ** s) for k in range(1, n + 1)]
    tot = sum(w)
    return [x / tot for x in w]


def build_w1(seed: int, *, n_items: int = 200, probes_per_insert: int = 2,
             s: float = 1.2, n_gold: int = 10) -> ControlTrace:
    rng = random.Random(("w1", seed).__repr__())
    probs = zipf_probs(n_items, s)          # popularity of item index k = probs[k]
    events: list[WorkloadEvent] = []
    counts: dict[int, int] = {}
    pn = 0
    for i in range(n_items):
        events.append(WorkloadEvent(type="insert", timestamp=float(i),
                                    item=_item(i), query=None, gold_item_ids=[]))
        # Zipf draw restricted to inserted items (renormalized by rejection).
        for _ in range(probes_per_insert):
            while True:
                r, acc = rng.random(), 0.0
                for k in range(n_items):
                    acc += probs[k]
                    if r <= acc:
                        break
                if k <= i:
                    break
            counts[k] = counts.get(k, 0) + 1
            pn += 1
            events.append(_probe(f"w1s{seed}", pn, k))
    gold = list(range(n_gold))               # the top-G popularity items
    for j, g in enumerate(gold):
        events.append(_scored(f"w1s{seed}", j, g, n_items))
    return ControlTrace("w1", seed, events, gold, counts, n_items, 100)


def build_w2(seed: int, *, n_items: int = 200, n_gold: int = 10) -> ControlTrace:
    rng = random.Random(("w2", seed).__repr__())
    events: list[WorkloadEvent] = []
    counts: dict[int, int] = {}
    for i in range(n_items):
        events.append(WorkloadEvent(type="insert", timestamp=float(i),
                                    item=_item(i), query=None, gold_item_ids=[]))
        events.append(_probe(f"w2s{seed}", i + 1, i))   # single access: itself
        counts[i] = 1
    gold = sorted(rng.sample(range(n_items), n_gold))   # uniform positions
    for j, g in enumerate(gold):
        events.append(_scored(f"w2s{seed}", j, g, n_items))
    return ControlTrace("w2", seed, events, gold, counts, n_items, 100)


def build_w3(seed: int, *, n_items: int = 200, n_gold: int = 10) -> ControlTrace:
    rng = random.Random(("w3", seed).__repr__())
    events: list[WorkloadEvent] = []
    for i in range(n_items):
        events.append(WorkloadEvent(type="insert", timestamp=float(i),
                                    item=_item(i), query=None, gold_item_ids=[]))
    first_decile = max(1, n_items // 10)
    gold = sorted(rng.sample(range(first_decile), min(n_gold, first_decile)))
    for j, g in enumerate(gold):
        events.append(_scored(f"w3s{seed}", j, g, n_items))
    return ControlTrace("w3", seed, events, gold, {}, n_items, 100)


BUILDERS = {"w1": build_w1, "w2": build_w2, "w3": build_w3}
