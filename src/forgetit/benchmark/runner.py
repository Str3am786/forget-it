from __future__ import annotations

import time

from forgetit.benchmark.workloads.base import Workload
from forgetit.benchmark.schema import EvalResult
from forgetit.memory_system.store import RetentionManager


def _safe_bytes(text: str) -> int:
    return len(text.encode("utf-8"))


def run(
    workload: Workload,
    store: RetentionManager,
    *,
    k: int,
    dataset: str,
    policy: str,
    retriever: str,
    seed: int = 0,
    itemiser: str = "",
    reader: str = "",
) -> list[EvalResult]:
    """
    Replay a workload chronologically against the store.

    QUERY handling:
    - gold_item_ids == []  -> access-only query, updates recency/frequency but is not scored
    - gold_item_ids != []  -> scored query, emits one EvalResult
    """
    results: list[EvalResult] = []

    for event in workload.events():
        if event.type == "insert":
            if event.item is None:
                raise ValueError("Insert event missing item.")
            store.insert(event.item)
            continue

        if event.type == "query":
            if event.query is None:
                raise ValueError("Query event missing query.")

            gold_ids = set(event.gold_item_ids or [])

            # Unlabelled query: only creates access signal for LRU/LFU.
            if not gold_ids:
                store.retrieve(event.query, k=k)
                continue

            # -------------------------------
            # Policy layer: did gold survive?
            # -------------------------------
            retained_ids = set(store.iter_ids())
            survived_gold = gold_ids & retained_ids
            gold_survived = len(survived_gold) > 0

            gold_bytes = 0
            for gid in survived_gold:
                rec = store.get(gid)
                if rec is not None:
                    gold_bytes += rec.bytes if rec.bytes > 0 else _safe_bytes(rec.text)

            used_bytes = store.used_bytes
            ubf = gold_bytes / used_bytes if used_bytes > 0 else 0.0
            waste = 1.0 - ubf

            # ----------------------------------------
            # Retriever layer: can retrieval surface it?
            # ----------------------------------------
            t0 = time.perf_counter()
            retrieved = store.retrieve(event.query, k=k)
            latency = time.perf_counter() - t0

            retrieved_ids = [r.id for r in retrieved]
            hit = 1.0 if any(rid in gold_ids for rid in retrieved_ids) else 0.0
            hit_given_retained = hit if gold_survived else 0.0

            mrr = 0.0
            for rank, rid in enumerate(retrieved_ids, start=1):
                if rid in gold_ids:
                    mrr = 1.0 / rank
                    break

            results.append(
                EvalResult(
                    dataset=dataset,
                    query_id=event.query.id,
                    policy=policy,
                    budget_bytes=store.budget_bytes,
                    k=k,
                    itemiser=itemiser,
                    retriever=retriever,
                    reader=reader,
                    seed=seed,
                    gold_survived=gold_survived,
                    useful_byte_fraction=ubf,
                    waste_rate=waste,
                    evictions=store.logger.counts.get("evict", 0),
                    compressions=store.logger.counts.get("compress", 0),
                    hit_at_k=hit,
                    hit_at_k_given_retained=hit_given_retained,
                    mrr=mrr,
                    # TODO reader
                    reader_answer="",
                    oracle_correct=False,
                    retrieved_correct=False,
                    used_bytes_end=used_bytes,
                    latency_sec=latency,
                )
            )
            continue

        raise ValueError(f"Unknown event type: {event.type}")

    return results