#!/usr/bin/env python3
"""
Per-item redundancy-vs-size characterization (TRACE + embeddings; no policy/
reader/LLM). The diagnostic that INTERPRETS the semantic-policy comparison.

nearest-pair (`semantic`) and marginal-coverage (`semantic_coverage`) diverge
exactly when the corpus has lots of *many-neighbour-moderate-distance* mass
(an item close-ish to many residents but twin of none): coverage sheds it,
nearest-pair keeps it. If redundancy is instead clean twins, the two agree and
the cheap one wins. This script logs, per resident item:

  * nearest_within  — max cosine to any OTHER item in its example (the nearest-
    pair signal). High == has a near-twin.
  * mean_sim        — mean cosine to all other items (the coverage-ish signal).
    High with LOW nearest_within == moderate neighbour to many (the split case).
  * bytes           — its size (the size axis the keep_smallest policies use).
  * is_gold         — whether it's gold for some query in the example.

So you can see *why* the policies split (twin mass vs diffuse mass) and whether
redundancy correlates with size. Needs an embedding endpoint (EMBED_* settings).

    python scripts/characterize_redundancy.py --dataset longmemeval --limit 50
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]


def redundancy_rows(records, gold_ids: set[str]) -> list[dict]:
    """Per-item nearest_within + mean_sim + bytes + is_gold for one example.

    ``records`` must already carry embeddings (attach_embeddings). Items without
    a vector are skipped. Pure function — unit-testable with a stub embedder."""
    embedded = [r for r in records if getattr(r, "embedding", None) is not None]
    if len(embedded) < 2:
        return []
    mat = np.asarray([r.embedding for r in embedded], dtype=np.float32)
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    mat = mat / norms
    sims = mat @ mat.T
    n = len(embedded)
    np.fill_diagonal(sims, np.nan)                       # exclude self
    nearest = np.nanmax(sims, axis=1)
    mean_sim = np.nanmean(sims, axis=1)
    rows = []
    for i, r in enumerate(embedded):
        rows.append({
            "id": r.id,
            "bytes": int(getattr(r, "bytes", 0) or 0),
            "nearest_within": float(nearest[i]),
            "mean_sim": float(mean_sim[i]),
            "is_gold": r.id in gold_ids,
        })
    return rows


def _example_records_and_gold(events):
    """Extract this example's inserted records + the union of its gold ids."""
    records = [e.item for e in events if e.type == "insert" and e.item is not None]
    gold: set[str] = set()
    for e in events:
        if e.type == "query" and e.gold_item_ids:
            gold.update(e.gold_item_ids)
    return records, gold


def _build_workload(dataset: str, itemiser_name: str, limit):
    data = REPO_ROOT / "data"
    if dataset == "beam":
        from forgetit.benchmark.workloads.beam import (
            BeamWorkload, BeamTurnItemiser, BeamTurnPairItemiser, BeamBatchItemiser)
        it = {"beam_turn": BeamTurnItemiser, "beam_turn_pair": BeamTurnPairItemiser,
              "beam_batch": BeamBatchItemiser}.get(itemiser_name, BeamTurnPairItemiser)()
        return BeamWorkload(data / "beam_500K.jsonl", itemiser=it, probe_every=0,
                            clear_between_examples=True, limit=limit)
    from forgetit.benchmark.workloads.longmemeval import (
        LongMemEvalWorkload, TurnPairItemiser, SessionItemiser)
    it = {"turn_pair": TurnPairItemiser, "session": SessionItemiser}.get(
        itemiser_name, TurnPairItemiser)()
    return LongMemEvalWorkload(data / "longmemeval_s_cleaned.json", it, probe_every=0,
                               clear_between_examples=True, limit=limit)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset", choices=["longmemeval", "beam"], default="longmemeval")
    ap.add_argument("--itemiser", default="turn_pair")
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    from forgetit.embeddings import get_embedder, attach_embeddings
    embedder = get_embedder()
    wl = _build_workload(args.dataset, args.itemiser, args.limit)

    all_rows: list[dict] = []
    for ex_id, events in wl.iter_examples():
        records, gold = _example_records_and_gold(events)
        attach_embeddings(records, embedder)
        for row in redundancy_rows(records, gold):
            row["example_id"] = ex_id
            all_rows.append(row)

    if not all_rows:
        print("no rows (need >=2 embedded items per example)")
        return 1

    out = args.out or (REPO_ROOT / "artifacts" /
                       f"redundancy_{args.dataset}_{args.itemiser}.csv")
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["example_id", "id", "bytes",
                                          "nearest_within", "mean_sim", "is_gold"])
        w.writeheader(); w.writerows(all_rows)

    # Summary: gold vs noise redundancy, and the twin-vs-diffuse signal.
    nw = np.array([r["nearest_within"] for r in all_rows])
    ms = np.array([r["mean_sim"] for r in all_rows])
    bs = np.array([r["bytes"] for r in all_rows], float)
    gold = np.array([r["is_gold"] for r in all_rows])
    # diffuse mass: high mean_sim but low nearest_within == coverage/nearest-pair split fuel
    diffuse = float(np.mean((ms > np.median(ms)) & (nw < np.median(nw))))
    summary = {
        "n_items": len(all_rows),
        "mean_nearest_within": float(nw.mean()),
        "mean_mean_sim": float(ms.mean()),
        "gold_mean_nearest_within": float(nw[gold].mean()) if gold.any() else None,
        "noise_mean_nearest_within": float(nw[~gold].mean()) if (~gold).any() else None,
        "corr_redundancy_size": (float(np.corrcoef(nw, bs)[0, 1]) if len(nw) > 1 else None),
        "diffuse_mass_fraction": diffuse,   # high => coverage & nearest-pair will diverge
    }
    print(json.dumps(summary, indent=2))
    print(f"wrote {out}  ({len(all_rows)} items)")
    print("\nRead: high diffuse_mass_fraction => the policies should diverge; "
          "~0 (redundancy is clean twins) => they agree and nearest-pair wins.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
