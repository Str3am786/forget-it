"""Eviction-precision diagnostic from event logs — did a policy shed NOISE or GOLD?

For each (policy, budget) at a fixed itemiser×retriever, parse events.jsonl:
  gold set   = union of gold_ids over the example's queries
  removed set= evicted + deleted item ids
eviction_precision = |removed ∩ noise| / |removed|  (higher = better; sheds noise)
Also byte-weighted, and the gold-shed rate. This is the direct test of whether a
content-aware (semantic) policy's signal helps it AVOID gold vs a blind policy.

Ground truth: artifacts/benchmark_full/ only.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from beam_load import BEAM_ROOT, _config_from_path


def _events_for(itemiser, policy, retriever, budget_pct):
    return (BEAM_ROOT / itemiser / "retention" / policy / retriever /
            f"budget_{budget_pct}" / "PlainTextReader" / "events.jsonl")


def precision_for_config(path: Path) -> dict:
    """Aggregate eviction precision over all examples in one config's event log."""
    n_gold_rm = n_noise_rm = 0
    b_gold_rm = b_noise_rm = 0
    gold: set[str] = set()
    removed: dict[str, int] = {}          # id -> bytes (last seen)
    ins_bytes: dict[str, int] = {}

    def flush():
        nonlocal n_gold_rm, n_noise_rm, b_gold_rm, b_noise_rm, gold, removed, ins_bytes
        for iid, b in removed.items():
            bb = b or ins_bytes.get(iid, 0)
            if iid in gold:
                n_gold_rm += 1; b_gold_rm += bb
            else:
                n_noise_rm += 1; b_noise_rm += bb
        gold, removed, ins_bytes = set(), {}, {}

    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            ev = json.loads(line)
            name = ev.get("name")
            if name == "clear":
                flush()
            elif name == "insert":
                ins_bytes[ev["id"]] = int(ev.get("bytes", 0))
            elif name in ("evict", "delete"):
                removed[ev["id"]] = int(ev.get("freed_bytes", 0))
            elif name == "gold":
                gold.update(ev.get("gold_ids", []))
    flush()

    n_rm = n_gold_rm + n_noise_rm
    b_rm = b_gold_rm + b_noise_rm
    return {"n_removed": n_rm, "n_gold_removed": n_gold_rm,
            "eviction_precision": (n_noise_rm / n_rm) if n_rm else float("nan"),
            "byte_precision": (b_noise_rm / b_rm) if b_rm else float("nan"),
            "gold_shed_rate": (n_gold_rm / n_rm) if n_rm else float("nan")}


def build(itemiser="beam_turn_pair", retriever="bm25") -> pd.DataFrame:
    from beam_load import POLICIES, BUDGET_FRACS
    pct = {0.1: "010pct", 0.25: "025pct", 0.5: "050pct", 0.75: "075pct", 1.0: "100pct"}
    rows = []
    for pol in POLICIES:
        for bf in BUDGET_FRACS:
            p = _events_for(itemiser, pol, retriever, pct[bf])
            if not p.exists():
                continue
            rows.append({"itemiser": itemiser, "retriever": retriever, "policy": pol,
                         "budget_frac": bf, **precision_for_config(p)})
    return pd.DataFrame(rows)


if __name__ == "__main__":
    df = build()
    print("== eviction precision (noise-shed fraction), turn_pair/bm25 — higher=better ==")
    piv = df.pivot(index="policy", columns="budget_frac", values="eviction_precision").round(3)
    print(piv.to_string())
    print("\n== gold-shed rate (fraction of evictions that were GOLD) ==")
    piv2 = df.pivot(index="policy", columns="budget_frac", values="gold_shed_rate").round(3)
    print(piv2.to_string())
