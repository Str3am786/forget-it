"""Q6 per-example eviction bound, ABSOLUTE form (manifest P-3).

No rescued file exists for this row; the demonstration is constructed here from
the event traces a MAIN retention-only smoke already writes.

THE CLAIM, split into the two halves it actually asserts
--------------------------------------------------------
For two configs A and B replaying the IDENTICAL insert stream of one example:

  (i) IDENTITY   freed(A) - freed(B)  ==  used_end(B) - used_end(A)
      Byte conservation. Every inserted byte is either still resident at the
      end of the example or was freed by an eviction, so
      inserted_total = used_end + freed_total for each config separately, and
      inserted_total is shared. Checked here per config (the strong form) AND
      per pair (the form the row states).

  (ii) BOUND     |freed(A) - freed(B)|  <=  max_item_bytes(example)
      "within one victim". Each config's budget loop evicts until it fits, so
      it stops within one victim of the budget; the divergence between two
      configs is victim-size arithmetic, bounded by the largest single item
      of that example.

Half (i) is an identity that must hold EXACTLY (0 tolerance). Half (ii) is an
inequality. Reporting them separately is Rule 5: a conjoined claim is split and
each half measured on its own.

Usage:
    PYTHONPATH=src .venv/bin/python scripts/q6_eviction_bound.py <artifact-root>
"""
from __future__ import annotations

import itertools
import json
import sys
from pathlib import Path


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


def per_example_ledger(config_dir: Path) -> list[dict]:
    """One byte ledger per example."""
    out = []
    for ex in split_examples(load_events(config_dir)):
        inserted = freed = 0
        max_item = 0
        used_end = 0
        n_ins = n_ev = 0
        compressed_freed = 0
        for e in ex:
            n = e.get("name")
            if n == "insert":
                inserted += int(e["bytes"])
                max_item = max(max_item, int(e["bytes"]))
                used_end = int(e["used_bytes"])
                n_ins += 1
            elif n == "evict":
                freed += int(e.get("freed_bytes", 0))
                used_end = int(e["used_bytes"])
                n_ev += 1
            elif n == "compress":
                compressed_freed += int(e.get("freed_bytes", 0))
                used_end = int(e["used_bytes"])
            elif n == "delete":
                freed += int(e.get("freed_bytes", 0))
                used_end = int(e["used_bytes"])
        out.append({"inserted": inserted, "freed": freed, "used_end": used_end,
                    "max_item": max_item, "n_inserts": n_ins, "n_evicts": n_ev,
                    "compressed_freed": compressed_freed})
    return out


def find_configs(root: Path) -> dict[tuple[str, str, str], Path]:
    out = {}
    for ev in sorted(root.rglob("events.jsonl")):
        parts = ev.parent.parts
        try:
            i = parts.index("retention")
        except ValueError:
            continue
        out[(parts[i - 1], parts[i + 1], parts[i + 3])] = ev.parent
    return out


def main() -> int:
    root = Path(sys.argv[1])
    configs = find_configs(root)
    if not configs:
        print(f"no events.jsonl under {root}")
        return 2

    fail_conservation = 0
    fail_identity = 0
    fail_bound = 0
    n_conservation = n_identity = n_bound = 0
    worst_ratio = 0.0
    worst_row = None

    # ── half 0: per-config byte conservation (the identity's foundation) ──
    print("=" * 78)
    print("CONSERVATION (per config, per example): inserted == used_end + freed")
    print("=" * 78)
    ledgers: dict[tuple[str, str, str], list[dict]] = {}
    for key in sorted(configs):
        L = per_example_ledger(configs[key])
        ledgers[key] = L
        for j, r in enumerate(L):
            n_conservation += 1
            lhs = r["inserted"]
            rhs = r["used_end"] + r["freed"] + r["compressed_freed"]
            if lhs != rhs:
                fail_conservation += 1
                print(f"  MISMATCH {key} ex{j}: inserted={lhs} "
                      f"used_end+freed={rhs} delta={lhs - rhs}")
    print(f"  checked {n_conservation} (config, example) ledgers; "
          f"mismatches = {fail_conservation}")
    print()

    # ── halves (i) and (ii): every config PAIR sharing an insert stream ──
    print("=" * 78)
    print("(i) IDENTITY   freed(A)-freed(B) == used_end(B)-used_end(A)   [exact]")
    print("(ii) BOUND     |freed(A)-freed(B)| <= max_item_bytes(example)")
    print("=" * 78)
    by_cell: dict[tuple[str, str], list[tuple[str, str, str]]] = {}
    for (itm, pol, bud) in configs:
        by_cell.setdefault((itm, bud), []).append((itm, pol, bud))

    for (itm, bud), keys in sorted(by_cell.items()):
        if len(keys) < 2:
            continue
        for a, b in itertools.combinations(sorted(keys), 2):
            La, Lb = ledgers[a], ledgers[b]
            if len(La) != len(Lb):
                print(f"  SKIP {a[1]} vs {b[1]} @ {itm}/{bud}: "
                      f"{len(La)} vs {len(Lb)} examples")
                continue
            for j, (ra, rb) in enumerate(zip(La, Lb)):
                if ra["inserted"] != rb["inserted"]:
                    print(f"  SKIP {a[1]} vs {b[1]} ex{j}: insert streams differ")
                    continue
                n_identity += 1
                n_bound += 1
                lhs = ra["freed"] - rb["freed"]
                rhs = rb["used_end"] - ra["used_end"]
                if lhs != rhs:
                    fail_identity += 1
                    print(f"  IDENTITY FAIL {a[1]} vs {b[1]} @ {itm}/{bud} ex{j}: "
                          f"{lhs} != {rhs}")
                delta = abs(lhs)
                bound = max(ra["max_item"], rb["max_item"])
                if delta > bound:
                    fail_bound += 1
                    print(f"  BOUND FAIL {a[1]} vs {b[1]} @ {itm}/{bud} ex{j}: "
                          f"|delta|={delta} > max_item={bound}")
                ratio = delta / bound if bound else 0.0
                if ratio > worst_ratio:
                    worst_ratio = ratio
                    worst_row = (a[1], b[1], itm, bud, j, delta, bound)

    print(f"  pairs x examples checked : {n_identity}")
    print(f"  (i)  identity violations : {fail_identity}")
    print(f"  (ii) bound violations    : {fail_bound}")
    if worst_row:
        pa, pb, itm, bud, j, delta, bound = worst_row
        print(f"  tightest observed case   : {pa} vs {pb} @ {itm}/{bud} ex{j} "
              f"-> |delta|={delta} B, bound={bound} B, "
              f"ratio={worst_ratio:.4f} of one victim")
    print()

    total_fail = fail_conservation + fail_identity + fail_bound
    print("=" * 78)
    print(f"VERDICT: {'PASS' if total_fail == 0 else 'FAIL'} "
          f"(conservation={fail_conservation}, identity={fail_identity}, "
          f"bound={fail_bound})")
    return 1 if total_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
