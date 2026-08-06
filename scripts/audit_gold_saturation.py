"""Gold saturation audit — why the oracle collapses onto FIFO on coarse itemisers.

The oracle receives ``protected_ids`` as the UNION of gold item ids over every
scored query in an example (runner_core, ``wants_gold_ids`` injection). Its
victim rule prefers an unprotected item and, when none exists, falls back to the
oldest protected one — which is exactly the FIFO rule. So on any example where
the protected union covers the whole store, the oracle is FIFO by construction:
same rule, same set, same victim sequence, byte-identical final store.

Whether that happens is a property of the workload and the itemiser alone. It
needs no run, no policy and no budget: it is

    protected_fraction = |union(gold ids) ∩ inserted ids| / |inserted ids|

A fraction of 1.0 means the oracle had no legal victim from its first eviction
onward and its survival numbers carry no information about protection.

Emits one summary row per (dataset, itemiser) and, with --csv, one row per
example. Cross-check: the ``saturated`` column here should reproduce the
oracle-collapses-onto-fifo rate measured from ``used_bytes_end`` in the results.
Two independent measurements of the same quantity.

Usage:
    python -m forgetit.experiments.audit_gold_saturation
    python -m forgetit.experiments.audit_gold_saturation --limit 5
    python -m forgetit.experiments.audit_gold_saturation --csv report/saturation.csv
"""
from __future__ import annotations

import argparse
import csv
from pathlib import Path
from statistics import mean

REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = REPO_ROOT / "data"

# Match the paths the campaign runners use. Adjust if your filenames differ.
LOCOMO_PATH = DATA_DIR / "locomo10.json"
BEAM_PATH = DATA_DIR / "beam_500K.jsonl" 
LONGMEMEVAL_PATH = DATA_DIR / "longmemeval_s_cleaned.json"


def _slices(limit: int | None, skip_image_turns: bool):
    """Yield (dataset, itemiser_name, workload) for every slice whose data exists."""
    from forgetit.benchmark.workloads.locomo import (
        LocomoWorkload, LocomoTurnItemiser, LocomoTurnPairItemiser,
        LocomoSessionItemiser,
    )
    from forgetit.benchmark.workloads.beam import (
        BeamWorkload, BeamTurnItemiser, BeamTurnPairItemiser, BeamBatchItemiser,
    )
    from forgetit.benchmark.workloads.longmemeval import (
        LongMemEvalWorkload, TurnPairItemiser, SessionItemiser,
    )

    specs = [
        ("locomo", LOCOMO_PATH, LocomoWorkload,
         [LocomoTurnItemiser(), LocomoTurnPairItemiser(), LocomoSessionItemiser()],
         {"skip_image_turns": skip_image_turns}),
        ("beam", BEAM_PATH, BeamWorkload,
         [BeamTurnItemiser(), BeamTurnPairItemiser(), BeamBatchItemiser()], {}),
        ("longmemeval", LONGMEMEVAL_PATH, LongMemEvalWorkload,
         [TurnPairItemiser(), SessionItemiser()], {}),
    ]

    for dataset, path, cls, itemisers, extra in specs:
        if not path.exists():
            print(f"  skipping {dataset}: {path} not found")
            continue
        for it in itemisers:
            # probe_every=1 and clear_between_examples=True mirror the campaign.
            # Neither affects this measurement: probes carry no gold ids and each
            # example is scoped anyway. Kept aligned so item counts match the run.
            yield dataset, it.name, cls(
                path, itemiser=it, probe_every=1,
                clear_between_examples=True, limit=limit, **extra,
            )


def audit_example(events) -> dict | None:
    """Protected fraction for one example. None if it never scores anything."""
    items = {e.item.id for e in events if e.type == "insert" and e.item is not None}
    scored = [e for e in events if e.type == "query" and e.gold_item_ids]
    if not items or not scored:
        return None

    union = {gid for e in scored for gid in e.gold_item_ids}
    resolved = union & items

    return {
        "items": len(items),
        "questions": len(scored),
        "gold_per_question": mean(len(set(e.gold_item_ids)) for e in scored),
        "protected": len(resolved),
        # Gold ids that name nothing in the store. Should be 0 everywhere; a
        # non-zero value is a genuine resolution bug rather than saturation.
        "unresolved": len(union - items),
        "protected_frac": len(resolved) / len(items),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int, default=None,
                    help="first N examples per dataset (smoke run)")
    ap.add_argument("--skip-image-turns", action="store_true",
                    help="match the campaign's LoCoMo setting")
    ap.add_argument("--csv", type=Path, default=None,
                    help="write one row per example here")
    args = ap.parse_args()

    per_example: list[dict] = []
    summary: list[dict] = []

    for dataset, itemiser, wl in _slices(args.limit, args.skip_image_turns):
        rows = []
        for example_id, events in wl.iter_examples():
            r = audit_example(events)
            if r is None:
                continue
            r |= {"dataset": dataset, "itemiser": itemiser, "example_id": example_id}
            rows.append(r)
        if not rows:
            continue
        per_example.extend(rows)

        n = len(rows)
        summary.append({
            "dataset": dataset,
            "itemiser": itemiser,
            "examples": n,
            "items": mean(r["items"] for r in rows),
            "questions": mean(r["questions"] for r in rows),
            "gold_per_q": mean(r["gold_per_question"] for r in rows),
            "protected_frac": mean(r["protected_frac"] for r in rows),
            "saturated": sum(r["protected_frac"] >= 0.999 for r in rows) / n,
            "unresolved": sum(r["unresolved"] for r in rows),
        })

    if not summary:
        print("No slices audited. Check the dataset paths at the top of this file.")
        return

    hdr = (f"\n{'dataset':<12} {'itemiser':<14} {'exs':>5} {'items':>8} {'qs':>6} "
           f"{'gold/q':>7} {'prot.frac':>10} {'saturated':>10} {'unres':>7}")
    print(hdr + "\n" + "-" * len(hdr))
    for s in summary:
        print(f"{s['dataset']:<12} {s['itemiser']:<14} {s['examples']:>5} "
              f"{s['items']:>8.1f} {s['questions']:>6.1f} {s['gold_per_q']:>7.2f} "
              f"{s['protected_frac']:>10.3f} {s['saturated']:>10.3f} "
              f"{s['unresolved']:>7}")

    bad = [s for s in summary if s["unresolved"]]
    if bad:
        print("\nGold ids naming no inserted item, which saturation does not explain:")
        for s in bad:
            print(f"  {s['dataset']}/{s['itemiser']}: {s['unresolved']}")

    hot = [s for s in summary if s["saturated"] > 0]
    if hot:
        print("\nSlices where the oracle has no legal victim and degenerates to fifo:")
        for s in hot:
            print(f"  {s['dataset']}/{s['itemiser']}: "
                  f"{s['saturated']:.1%} of examples")
    else:
        print("\nNo slice saturates. The oracle has an unprotected victim throughout.")

    if args.csv:
        args.csv.parent.mkdir(parents=True, exist_ok=True)
        cols = ["dataset", "itemiser", "example_id", "items", "questions",
                "gold_per_question", "protected", "unresolved", "protected_frac"]
        with args.csv.open("w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols)
            w.writeheader()
            w.writerows({c: r[c] for c in cols} for r in per_example)
        print(f"\nWrote {len(per_example):,} example rows to {args.csv}")


if __name__ == "__main__":
    main()