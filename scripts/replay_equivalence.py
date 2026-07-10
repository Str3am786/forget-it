"""D1: FIFO ≡ blind-LRU ≡ blind-LFU on a REAL event trace (VERIFICATION.md S3).

Replays the insert stream of a recorded ``events.jsonl`` (per example, with
that example's own budget) through the real RetentionManager under each of the
three policies with ``probe_touch_mode="probes_blind"``, and asserts the three
eviction sequences are identical, example by example.

Why replaying ONLY inserts is faithful here: probes never touch under
probes_blind (and FIFO ignores touches entirely), and scored queries — which
DO touch — fire after an example's last insert in both workloads, so no
eviction pressure ever follows a touch. The script VERIFIES that shape on the
trace rather than assuming it (any insert after a scored retrieve within an
example is reported as a shape violation and voids the equivalence claim).

Usage:
    python scripts/replay_equivalence.py <path/to/events.jsonl> [--max-examples N]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.schema import MemRecord
from forgetit.memory_system import RetentionManager
from forgetit.policies import FIFOPolicy, LRUPolicy, LFUPolicy
from forgetit.retrieval.lexical import LexicalOverlapRetriever


class _EvictLogger:
    def __init__(self):
        self.evictions: list[str] = []

    def event(self, name, payload=None):
        if name == "evict":
            self.evictions.append(payload["id"])


def parse_examples(path: Path, max_examples: int | None = None):
    """Split the trace at ``clear`` events. Yields
    (inserts=[(id, bytes)], budget_bytes, shape_ok) per example."""
    inserts: list[tuple[str, int]] = []
    budget: int | None = None
    seen_scored_retrieve = False
    shape_ok = True
    n = 0
    with path.open() as f:
        for line in f:
            e = json.loads(line)
            name = e.get("name")
            if name == "clear":
                if inserts:
                    yield inserts, budget, shape_ok
                    n += 1
                    if max_examples is not None and n >= max_examples:
                        return
                inserts, budget = [], None
                seen_scored_retrieve, shape_ok = False, True
                continue
            if name == "insert":
                if seen_scored_retrieve:
                    shape_ok = False        # pressure after a touching query
                inserts.append((e["id"], int(e["bytes"])))
                b = int(e["budget_bytes"])
                if budget is None:
                    budget = b
                elif budget != b:
                    raise ValueError("budget changed mid-example — replay invalid")
            elif name == "retrieve":
                if "::probe::" not in e.get("q", ""):
                    seen_scored_retrieve = True
    if inserts:
        yield inserts, budget, shape_ok


def replay_policy(policy, inserts, budget: int) -> list[str]:
    logger = _EvictLogger()
    store = RetentionManager(
        budget_bytes=budget,
        backend=InMemoryBackend(retriever=LexicalOverlapRetriever()),
        policy=policy,
        logger=logger,
        probe_touch_mode="probes_blind",
    )
    store.connect()
    for item_id, nbytes in inserts:
        store.insert(MemRecord(id=item_id, text="", bytes=nbytes, created_at=0.0))
    return logger.evictions


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("events", type=Path)
    ap.add_argument("--max-examples", type=int, default=None)
    args = ap.parse_args()

    total_evicts = 0
    mismatches = 0
    shape_violations = 0
    n_examples = 0
    for inserts, budget, shape_ok in parse_examples(args.events, args.max_examples):
        n_examples += 1
        if not shape_ok:
            shape_violations += 1
        seqs = {
            "fifo": replay_policy(FIFOPolicy(), inserts, budget),
            "lru_blind": replay_policy(LRUPolicy(), inserts, budget),
            "lfu_blind": replay_policy(LFUPolicy(), inserts, budget),
        }
        total_evicts += len(seqs["fifo"])
        if not (seqs["fifo"] == seqs["lru_blind"] == seqs["lfu_blind"]):
            mismatches += 1
            firsts = {k: v[:5] for k, v in seqs.items()}
            print(f"MISMATCH example {n_examples}: {firsts}")

    print(f"examples={n_examples} evictions_per_policy={total_evicts} "
          f"mismatched_examples={mismatches} shape_violations={shape_violations}")
    if mismatches == 0 and shape_violations == 0 and total_evicts > 0:
        print("EQUIVALENCE PASS: fifo ≡ blind-lru ≡ blind-lfu, byte-identical "
              "eviction sequences on every example.")
        return 0
    print("EQUIVALENCE FAIL (or vacuous / shape-violated) — see lines above.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
