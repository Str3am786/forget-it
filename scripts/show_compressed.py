"""Recover the compressed text for any logged compression event.

Each ``compress`` event in events.jsonl carries a ``cache_key``; the disk LLM
cache holds the raw model output under that key; the applied text is
``_sanitise(raw)`` — the exact transformation the policy performed at run time.

Usage:
    python scripts/show_compressed.py <events.jsonl> [--item ID] [--raw]

With --item, only that item's compressions are shown; --raw prints the model
output before sanitisation as well. Exits nonzero if any requested event's
cache entry is missing (cache pruned, or the run predates the cache layer —
those cells fall back to censored-bounds analysis).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from forgetit.llm.disk_cache import LLMDiskCache          # noqa: E402
from forgetit.policies.llm_compression import _sanitise   # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("events", type=Path, help="events.jsonl from a run")
    ap.add_argument("--item", default=None, help="only this item id")
    ap.add_argument("--raw", action="store_true", help="also print pre-sanitise output")
    args = ap.parse_args()

    cache = LLMDiskCache()
    shown = missing = 0

    with args.events.open(encoding="utf-8") as f:
        for line in f:
            row = json.loads(line)
            if row.get("name") != "compress":
                continue
            item_id = row.get("id")
            if args.item is not None and item_id != args.item:
                continue
            key = row.get("cache_key")
            print(f"── compress  item={item_id}  t_insert={row.get('t_insert')}  "
                  f"{row.get('old_bytes')}B→{row.get('new_bytes')}B  key={key}")
            if not key:
                print("   [no cache_key on event — run predates the cache layer]")
                missing += 1
                continue
            raw = cache.get(key)
            if raw is None:
                print("   [cache entry missing]")
                missing += 1
                continue
            if args.raw:
                print("   RAW:", json.dumps(raw))
            print("   TEXT:", _sanitise(raw))
            shown += 1

    print(f"\n{shown} recovered, {missing} missing")
    return 1 if missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
