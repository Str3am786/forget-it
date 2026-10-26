#!/usr/bin/env python3
"""Does the gold snapshot describe the memory store each scored question saw?

The resident arm reads gold_answer_snapshot.jsonl, written once at the end of
replay. The retrieved arm reads retrieved_index.jsonl, written when each scored
question was asked. If an insert (and so an eviction or a recompression) could
fall between a scored question and the end of replay, the two arms would see
different stores and resident - retrieved would not isolate the retriever.

The resident arm only uses gold items, so the test is on gold items only. For
every gold item the retriever returned to a scored question:

  evicted_in_snapshot   snapshot says the item is gone (or its text is empty)
                        although the retriever returned it. Must be 0.
  text_differs          the text the retriever delivered is not the snapshot's
                        resident_text (a later recompression). Must be 0.
  missing_in_snapshot   the gold id is absent from the snapshot. Must be 0.

Plain python3, no repo imports, so it runs on a login node. Read only.

    python3 check_resident_state.py /scratch/$PROJ/artifacts/sweep_subset_bm25/longmemeval_turn_pair
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path

SNAP, INDEX, ITEMS = "gold_answer_snapshot.jsonl", "retrieved_index.jsonl", "evidence_items*.jsonl"


def config_dirs(root: Path) -> list[Path]:
    out = set()
    for d, _sub, files in os.walk(root, followlinks=True):   # subsets are symlinks
        if SNAP in files and INDEX in files:
            out.add(Path(d))
    return sorted(out)


def item_shards(root: Path) -> list[Path]:
    probe = root if root.is_dir() else root.parent
    for _ in range(8):                      # same walk up as reader_probe.py
        found = sorted(probe.glob(ITEMS))
        if found or probe.parent == probe:
            return found
        probe = probe.parent
    return []


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("root", type=Path, help="a workload tree, as passed to reader_probe.py")
    ap.add_argument("--limit-configs", type=int, default=0)
    args = ap.parse_args()

    dirs = config_dirs(args.root)
    if args.limit_configs:
        dirs = dirs[:args.limit_configs]
    if not dirs:
        print(f"no config dirs with {SNAP} and {INDEX} under {args.root}", file=sys.stderr)
        return 2

    # Pass 1: gold hits per config, and the item keys they need.
    hits: dict[Path, list[tuple[str, str, str]]] = {}
    snaps: dict[Path, dict[tuple[str, str], dict]] = {}
    need: set[str] = set()
    for d in dirs:
        snap = {}
        with (d / SNAP).open(encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    r = json.loads(line)
                    snap[(r["query_id"], r["gold_id"])] = r
        snaps[d] = snap
        hs = []
        with (d / INDEX).open(encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                row = json.loads(line)
                gold = set(row.get("gold_item_ids") or [])
                for r in row["retrieved"]:
                    if r["id"] in gold:
                        hs.append((row["query_id"], r["id"], r["key"]))
                        need.add(r["key"])
        hits[d] = hs

    # Pass 2: stream the item shards, keeping only the texts pass 1 needs.
    shards = item_shards(args.root)
    if not shards:
        print(f"no {ITEMS} at or above {args.root}", file=sys.stderr)
        return 2
    text: dict[str, str] = {}
    for sh in shards:
        with sh.open(encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    r = json.loads(line)
                    if r["key"] in need:
                        text[r["key"]] = r.get("text", "")

    total, bad_configs = Counter(), 0
    for d in dirs:
        c = Counter()
        for qid, gid, key in hits[d]:
            c["gold_hits"] += 1
            s = snaps[d].get((qid, gid))
            if s is None:
                c["missing_in_snapshot"] += 1
            elif s.get("was_evicted") or not s.get("resident_text"):
                c["evicted_in_snapshot"] += 1
            elif key not in text:
                c["key_not_in_item_table"] += 1
            elif text[key] != s["resident_text"]:
                c["text_differs"] += 1
        total.update(c)
        problems = {k: v for k, v in c.items() if k != "gold_hits" and v}
        if problems:
            bad_configs += 1
            print(f"PROBLEM {d}: {dict(c)}")

    print(f"\n{len(dirs)} configs, {len(shards)} item shard(s)")
    for k in ("gold_hits", "missing_in_snapshot", "evicted_in_snapshot",
              "text_differs", "key_not_in_item_table"):
        print(f"  {k:<24}{total[k]:>10}")
    ok = bad_configs == 0 and total["gold_hits"] > 0
    print("PASS: snapshot matches what the scored questions saw" if ok else
          f"FAIL: {bad_configs} config(s) disagree. Do not run the resident arm "
          "until this is explained.")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
