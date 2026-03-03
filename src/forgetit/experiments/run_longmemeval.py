from __future__ import annotations

import argparse
import csv
import json
import re
import humanize

import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Tuple

from forgetit.core.schema import MemRecord, Query
from forgetit.memory_system.store import RetentionManager
from forgetit.backend.in_memory import InMemoryBackend
from forgetit.policies.lru import LRUPolicy
from forgetit.policies.lfu import LFUPolicy
from forgetit.retrieval.lexical import LexicalOverlapRetriever


class _Logger:
    def __init__(self) -> None:
        self.counts = {"insert": 0, "evict": 0, "delete": 0, "retrieve": 0}

    def event(self, name: str, *_a, **_kw) -> None:
        if name in self.counts:
            self.counts[name] += 1


_DATE_RE = re.compile(r"^\s*(\d{4})/(\d{2})/(\d{2})\s*\([A-Za-z]{3}\)\s*(\d{2}):(\d{2})\s*$")

def parse_longmemeval_date(s: str) -> float:
    """
    Parse strings like: '2023/05/20 (Sat) 02:21' into unix timestamp seconds.
    Assumes the string is in local time; for your purposes ordering matters more than absolute tz.
    """
    m = _DATE_RE.match(s)
    if not m:
        raise ValueError(f"Unrecognized date format: {s!r}")
    y, mo, d, hh, mm = map(int, m.groups())
    # naive datetime -> timestamp (local). That's fine for relative ordering.
    return dt.datetime(y, mo, d, hh, mm).timestamp()

def flatten_session(session_turns: List[Dict[str, Any]]) -> Tuple[str, bool]:
    """
    Turn a list of turns into a single text blob.
    Returns (text, contains_answer_turn) where contains_answer_turn is based on has_answer flags if present.
    """
    parts: List[str] = []
    contains_answer = False

    for turn in session_turns:
        role = turn.get("role", "unknown")
        content = turn.get("content", "")
        if turn.get("has_answer", False) is True:
            contains_answer = True
        parts.append(f"{role}: {content}")

    return "\n".join(parts), contains_answer


def recall_at_k(retrieved_ids: List[str], gold_ids: List[str]) -> float:
    """
    Session-level Recall@k for LongMemEval: did we retrieve any gold answer session?
    """
    if not gold_ids:
        return 1.0 if len(retrieved_ids) == 0 else 0.0
    gold = set(gold_ids)
    return 1.0 if any(rid in gold for rid in retrieved_ids) else 0.0


def build_policy(name: str):
    name = name.lower()
    if name == "lru":
        return LRUPolicy()
    if name == "lfu":
        return LFUPolicy()
    raise ValueError(f"Unknown policy: {name}")


def build_store(policy_name: str, budget_bytes: int) -> RetentionManager:
    """
    For now we run non-persistent: InMemoryBackend + lexical retriever.
    """
    retriever = LexicalOverlapRetriever()
    backend = InMemoryBackend(retriever=retriever)
    policy = build_policy(policy_name)

    store = RetentionManager(
        budget_bytes=budget_bytes,
        backend=backend,
        policy=policy,
        logger=_Logger(),
    )
    store.connect()
    return store


def ingest_example_sessions(store: RetentionManager, ex: Dict[str, Any]) -> None:
    """
    Ingest all haystack sessions in time order as MemRecords.
    Each session becomes 1 MemRecord (session-level ingestion).
    """
    session_ids = ex["haystack_session_ids"]
    dates = ex["haystack_dates"]
    sessions = ex["haystack_sessions"]

    for idx, (sid, ts, turns) in enumerate(zip(session_ids, dates, sessions)):
        text, contains_answer_turn = flatten_session(turns)

        rec = MemRecord(
            id=str(sid),
            text=text,
            created_at=parse_longmemeval_date(ts),
            meta={
                "dataset": "longmemeval",
                "question_id": ex.get("question_id", ""),
                "question_type": ex.get("question_type", ""),
                "session_index": idx,
                "contains_answer_turn": contains_answer_turn,
            },
        )
        store.insert(rec)


# def run_one(ex: Dict[str, Any], policy_name: str, budget_bytes: int, k: int) -> Dict[str, Any]:
#     store = build_store(policy_name, budget_bytes)

#     ingest_example_sessions(store, ex)

#     q = Query(
#         id=str(ex.get("question_id", "")),
#         text=str(ex["question"]),
#         timestamp=float(ex.get("question_date_ts", 0.0)) if "question_date_ts" in ex else 0.0,
#     )

#     results = store.retrieve(q, k=k)
#     retrieved_ids = [r.id for r in results]

#     gold_ids = ex.get("answer_session_ids", [])
#     r_at_k = recall_at_k(retrieved_ids, gold_ids)

#     return {
#         "question_id": ex.get("question_id", ""),
#         "question_type": ex.get("question_type", ""),
#         "policy": policy_name,
#         "budget_bytes": budget_bytes,
#         "k": k,
#         "recall_at_k": r_at_k,
#         "used_bytes_end": store.used_bytes,
#         "retrieved_session_ids": json.dumps(retrieved_ids),
#         "gold_session_ids": json.dumps(gold_ids),
#     }

def run_one(ex: Dict[str, Any], policy_name: str, budget_bytes: int, k: int) -> Dict[str, Any]:
    store = build_store(policy_name, budget_bytes)
    logger = store.logger

    ingest_example_sessions(store, ex)

    gold_ids = ex.get("answer_session_ids", [])
    store_ids = set(store.iter_ids())
    gold_retained = 1.0 if any(gid in store_ids for gid in gold_ids) else 0.0

    q = Query(
        id=str(ex.get("question_id", "")),
        text=str(ex["question"]),
        timestamp=float(ex.get("question_date_ts", 0.0)) if "question_date_ts" in ex else 0.0,
    )

    results = store.retrieve(q, k=k)
    retrieved_ids = [r.id for r in results]

    hit_at_k = 1.0 if any(rid in set(gold_ids) for rid in retrieved_ids) else 0.0
    hit_given_retained = hit_at_k if gold_retained == 1.0 else 0.0

    return {
        "question_id": ex.get("question_id", ""),
        "question_type": ex.get("question_type", ""),
        "policy": policy_name,
        "budget_bytes": budget_bytes,
        "k": k,


        "gold_retained": gold_retained,
        "hit_at_k": hit_at_k,
        "hit_at_k_given_retained": hit_given_retained,


        "used_bytes_end": store.used_bytes,
        "n_items_retained": len(store_ids),
        "ingested": logger.counts["insert"],
        "evictions": logger.counts["evict"],

        "retrieved_session_ids": json.dumps(retrieved_ids),
        "gold_session_ids": json.dumps(gold_ids),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="Path to longmemeval_*_cleaned.json")

    # new: allow multiple
    ap.add_argument("--policies", nargs="+", default=["lru", "lfu"], choices=["lru", "lfu"])
    ap.add_argument("--budgets", nargs="+", type=int, default=[
        262_144, 524_288, 1_048_576, 2_097_152, 5_242_880, 10_485_760
    ])
    ap.add_argument("--ks", nargs="+", type=int, default=[5, 10])

    ap.add_argument("--limit", type=int, default=0, help="If >0, run only first N examples")
    ap.add_argument("--out-dir", default="results/longmemeval")
    args = ap.parse_args()

    data_path = Path(args.data)
    examples = json.loads(data_path.read_text(encoding="utf-8"))

    if args.limit and args.limit > 0:
        examples = examples[: args.limit]

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    for policy in args.policies:
        for budget in args.budgets:
            for k in args.ks:
                rows: List[Dict[str, Any]] = []
                total = 0.0

                for ex in examples:
                    row = run_one(ex, policy, budget, k)
                    rows.append(row)
                    total += float(row["hit_at_k"])

                avg = total / max(1, len(rows))


                gold_retained_avg = sum(r["gold_retained"] for r in rows) / max(1, len(rows))
                hit_avg = sum(r["hit_at_k"] for r in rows) / max(1, len(rows))

                retained_rows = [r for r in rows if r["gold_retained"] == 1.0]
                hit_given_retained_avg = (
                    sum(r["hit_at_k_given_retained"] for r in retained_rows) / max(1, len(retained_rows))
                    if retained_rows else 0.0
                )

                avg_evictions = sum(r["evictions"] for r in rows) / max(1, len(rows))

                out_path = out_dir / f"longmemeval_{policy}_num_samples-{len(examples)}_{humanize.naturalsize(budget, binary=True)}_k{k}.csv"
                with out_path.open("w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                    writer.writeheader()
                    writer.writerows(rows)

                print(
                f"policy={policy} budget={budget} k={k} examples={len(rows)} "
                f"gold_retained={gold_retained_avg:.3f} hit@{k}={hit_avg:.3f} "
                f"hit@{k}|retained={hit_given_retained_avg:.3f} avg_evictions={avg_evictions:.2f} -> {out_path}"
                )

if __name__ == "__main__":
    main()