from __future__ import annotations

import logging
import time

from forgetit.benchmark.workloads.base import Workload
from forgetit.benchmark.schema import EvalResult, QueryEval, QAFields
from forgetit.memory_system import RetentionManager
from forgetit.core import Question, Query
from forgetit.llm import Reader

log = logging.getLogger(__name__)


def _fmt_bytes(b: int) -> str:
    if b < 1024:
        return f"{b} B"
    if b < 1_048_576:
        return f"{b / 1024:.1f} KB"
    return f"{b / 1_048_576:.1f} MB"


def _safe_bytes(text: str) -> int:
    return len(text.encode("utf-8"))


def _run_reader(
    *,
    reader_impl: Reader | None,
    query: Query,
    retrieved: list,
    answer_max_tokens: int,
    ref_answers: dict[str, dict] | None,
) -> QAFields:
    if reader_impl is None:
        return QAFields()

    question = Question(
        id=query.id,
        text=query.text,
        timestamp=query.timestamp,
        meta=query.meta.copy(),
    )

    answer_result = reader_impl.answer(
        question,
        retrieved,
        max_tokens=answer_max_tokens,
    )

    ref_info = (ref_answers or {}).get(query.id, {})

    return QAFields(
        reader_answer=answer_result.answer,
        abstained=answer_result.abstained,
        qa_latency_sec=answer_result.latency_sec,
        ref_answer=ref_info.get("answer", ""),
        question_text=query.text,
        question_type=ref_info.get("question_type", ""),
    )


def _evaluate_scored_query(
    *,
    store: RetentionManager,
    query: Query,
    gold_ids: set[str],
    k: int,
) -> QueryEval:
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

    t0 = time.perf_counter()
    retrieved = store.retrieve(query, k=k)
    latency = time.perf_counter() - t0

    retrieved_ids = [r.id for r in retrieved]
    hit = 1.0 if any(rid in gold_ids for rid in retrieved_ids) else 0.0
    hit_given_retained = hit if gold_survived else 0.0

    mrr = 0.0
    for rank, rid in enumerate(retrieved_ids, start=1):
        if rid in gold_ids:
            mrr = 1.0 / rank
            break

    return QueryEval(
        gold_survived=gold_survived,
        useful_byte_fraction=ubf,
        waste_rate=waste,
        evictions=store.logger.counts.get("evict", 0),
        compressions=store.logger.counts.get("compress", 0),
        hit_at_k=hit,
        hit_at_k_given_retained=hit_given_retained,
        mrr=mrr,
        used_bytes_end=used_bytes,
        latency_sec=latency,
        retrieved=retrieved,
    )


def run(
    workload: Workload,
    store: RetentionManager,
    *,
    k: int,
    dataset: str,
    policy: str,
    retriever_name: str,
    seed: int = 0,
    itemiser: str = "",
    reader_name: str = "",
    reader_impl: Reader | None = None,
    ref_answers: dict[str, dict] | None = None,
    answer_max_tokens: int = 128,
) -> list[EvalResult]:
    """
    Replay a workload chronologically against the store.

    Handles both retrieval-only and retrieval+QA evaluation in a single
    path.  When *reader_impl* is None the QA fields are left at their
    defaults (empty answer, abstained=False, etc.).

    QUERY handling:
    - gold_item_ids == []  -> access-only query, updates recency/frequency
                              but is not scored
    - gold_item_ids != []  -> scored query, emits one EvalResult
    """
    results: list[EvalResult] = []
    insert_count = 0
    query_count = 0
    skip_count = 0

    mode = "retrieval+QA" if reader_impl else "retrieval-only"
    log.info(
        "═══ Benchmark run ═══  dataset=%s  policy=%s  retriever=%s  "
        "reader=%s  budget=%s  k=%d  mode=%s",
        dataset, policy, retriever_name,
        reader_name or "none", _fmt_bytes(store.budget_bytes), k, mode,
    )

    t_run_start = time.perf_counter()

    for event in workload.events():
        if event.type == "insert":
            if event.item is None:
                raise ValueError("Insert event missing item.")
            store.insert(event.item)
            insert_count += 1
            if insert_count % 50 == 0:
                log.debug(
                    "  [insert %4d]  used %s / %s (%.0f%%)",
                    insert_count,
                    _fmt_bytes(store.used_bytes),
                    _fmt_bytes(store.budget_bytes),
                    100 * store.used_bytes / store.budget_bytes if store.budget_bytes else 0,
                )
            continue

        if event.type != "query":
            raise ValueError(f"Unknown event type: {event.type}")

        if event.query is None:
            raise ValueError("Query event missing query.")

        gold_ids = set(event.gold_item_ids or [])

        # Unlabelled query: only creates access signal for LRU/LFU.
        if not gold_ids:
            store.retrieve(event.query, k=k)
            skip_count += 1
            log.debug("  [query skip]  id=%s  (no gold labels)", event.query.id)
            continue

        query_count += 1

        # ── retrieval evaluation ──
        base = _evaluate_scored_query(
            store=store,
            query=event.query,
            gold_ids=gold_ids,
            k=k,
        )

        # ── reader evaluation (no-op when reader_impl is None) ──
        qa = _run_reader(
            reader_impl=reader_impl,
            query=event.query,
            retrieved=base.retrieved,
            answer_max_tokens=answer_max_tokens,
            ref_answers=ref_answers,
        )

        survived_tag = "SURVIVED" if base.gold_survived else "EVICTED"
        hit_tag = "HIT" if base.hit_at_k > 0 else "MISS"

        qa_detail = ""
        if reader_impl is not None:
            ans_preview = (qa.reader_answer[:60] + "…") if len(qa.reader_answer) > 60 else qa.reader_answer
            qa_detail = (
                f"  answer={'ABSTAINED' if qa.abstained else repr(ans_preview)}"
                f"  qa_latency={qa.qa_latency_sec:.2f}s"
            )

        log.info(
            "  [query %3d]  id=%-20s  gold=%s  retrieval=%s  "
            "hit@%d=%.0f  mrr=%.2f  latency=%.3fs  "
            "store=%s  evictions=%d%s",
            query_count, event.query.id, survived_tag, hit_tag,
            k, base.hit_at_k, base.mrr, base.latency_sec,
            _fmt_bytes(base.used_bytes_end), base.evictions,
            qa_detail,
        )

        results.append(
            EvalResult(
                dataset=dataset,
                query_id=event.query.id,
                policy=policy,
                budget_bytes=store.budget_bytes,
                k=k,
                itemiser=itemiser,
                retriever=retriever_name,
                reader=reader_name,
                seed=seed,
                gold_survived=base.gold_survived,
                useful_byte_fraction=base.useful_byte_fraction,
                waste_rate=base.waste_rate,
                evictions=base.evictions,
                compressions=base.compressions,
                hit_at_k=base.hit_at_k,
                hit_at_k_given_retained=base.hit_at_k_given_retained,
                mrr=base.mrr,
                used_bytes_end=base.used_bytes_end,
                latency_sec=base.latency_sec,
                reader_answer=qa.reader_answer,
                abstained=qa.abstained,
                qa_latency_sec=qa.qa_latency_sec,
                ref_answer=qa.ref_answer,
                question_text=qa.question_text,
                question_type=qa.question_type,
            )
        )

    elapsed = time.perf_counter() - t_run_start

    # ── summary ──
    n = len(results)
    if n > 0:
        avg_hit = sum(r.hit_at_k for r in results) / n
        avg_mrr = sum(r.mrr for r in results) / n
        survival = sum(1 for r in results if r.gold_survived) / n
        log.info(
            "═══ Done ═══  queries=%d (scored=%d, skipped=%d)  inserts=%d  "
            "gold_survival=%.1f%%  hit@%d=%.1f%%  mrr=%.3f  "
            "evictions=%d  compressions=%d  elapsed=%.1fs",
            query_count + skip_count, query_count, skip_count, insert_count,
            100 * survival, k, 100 * avg_hit, avg_mrr,
            store.logger.counts.get("evict", 0),
            store.logger.counts.get("compress", 0),
            elapsed,
        )
    else:
        log.info(
            "═══ Done ═══  no scored queries.  inserts=%d  skipped=%d  elapsed=%.1fs",
            insert_count, skip_count, elapsed,
        )

    return results