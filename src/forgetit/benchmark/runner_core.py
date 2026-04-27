from __future__ import annotations

import logging
import time

from forgetit.benchmark.workloads.base import Workload
from forgetit.benchmark.schema import EvalResult, QueryEval, QAFields
from forgetit.memory_system.base import MemorySystem
from forgetit.core import Question, Query
from forgetit.llm import Reader

log = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════════
# Store view — the single place where system differences are handled.
# ════════════════════════════════════════════════════════════════════


class _StoreView:
    """
    Thin capability-based view over a MemorySystem.

    The runner asks the view for things; the view negotiates with whatever
    the underlying store happens to expose.  No `isinstance` checks, no
    branching over store types.  A store either supports introspection
    (iter_ids + get) or it doesn't — the view reports which via the
    `introspectable` property.

    Why this exists:
        RetentionManager exposes iter_ids, get, used_bytes, budget_bytes,
        logger.counts, connect, close.  Letta / Mem0 expose none of those.
        Keeping the runner driven by capability detection means adding a
        new store wrapper requires zero runner changes.
    """
    __slots__ = ("store", "_can_iter", "_can_get")

    def __init__(self, store: MemorySystem):
        self.store = store
        self._can_iter = callable(getattr(store, "iter_ids", None))
        self._can_get = callable(getattr(store, "get", None))

    # ---- introspection capability ----

    @property
    def introspectable(self) -> bool:
        """
        True iff we can enumerate resident items.  Needed for gold_survived,
        useful_byte_fraction, and waste_rate.  Opaque stores report False
        and the runner falls back to retrieval-only metrics.
        """
        return self._can_iter and self._can_get

    # ---- metrics primitives ----

    def used_bytes(self) -> int:
        # RetentionManager exposes both the attribute and the method;
        # the Protocol only requires the method.  Prefer the method.
        m = getattr(self.store, "get_used_bytes", None)
        if callable(m):
            return int(m())
        attr = getattr(self.store, "used_bytes", None)
        if isinstance(attr, int):
            return attr
        return 0

    def budget_bytes(self) -> int:
        """0 means unbounded / unknown (non-RetentionManager stores)."""
        return int(getattr(self.store, "budget_bytes", 0) or 0)

    def event_count(self, name: str) -> int:
        """Pull a count off store.logger.counts, if the store has one."""
        logger = getattr(self.store, "logger", None)
        if logger is None:
            return 0
        counts = getattr(logger, "counts", None)
        if counts is None:
            return 0
        return int(counts.get(name, 0))

    def retained_ids(self) -> set[str]:
        if not self._can_iter:
            return set()
        return set(self.store.iter_ids())

    def get_record(self, record_id: str):
        if not self._can_get:
            return None
        return self.store.get(record_id)

    # ---- operations ----

    def insert(self, item) -> None:
        self.store.insert(item)

    def retrieve(self, query: Query, k: int):
        """
        Ask the store to retrieve.  Some wrappers require k, some ignore it.
        The updated Protocol takes ``k: int | None = None``; for stores that
        still use the older k-less signature we fall back gracefully.
        """
        try:
            return self.store.retrieve(query, k=k)
        except TypeError:
            return self.store.retrieve(query)

    def safe_call(self, method_name: str) -> None:
        """Call ``store.method_name()`` if it exists.  Used for connect/close."""
        fn = getattr(self.store, method_name, None)
        if callable(fn):
            fn()


# ════════════════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════════════════


def _fmt_bytes(b: int) -> str:
    if b <= 0:
        return "n/a"
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
    ref_answer: str,
    question_type: str,
) -> QAFields:
    if reader_impl is None:
        return QAFields(ref_answer=ref_answer,
                        question_text=query.text,
                        question_type=question_type
                        )

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

    return QAFields(
        reader_answer=answer_result.answer,
        abstained=answer_result.abstained,
        qa_latency_sec=answer_result.latency_sec,
        ref_answer=ref_answer,
        question_text=query.text,
        question_type=question_type,
    )


def _evaluate_scored_query(
    *,
    view: _StoreView,
    query: Query,
    gold_ids: set[str],
    k: int,
) -> QueryEval:
    """
    Compute retrieval metrics for one scored query.

    For introspectable stores we compute the full metric set including
    gold_survived, useful_byte_fraction, waste_rate.  For opaque stores
    those are set to sentinel values and ``introspectable=False`` on the
    output row — don't average them across store types without filtering.
    """
    introspectable = view.introspectable

    if introspectable:
        retained = view.retained_ids()
        survived_gold = gold_ids & retained
        gold_survived = len(survived_gold) > 0

        gold_bytes = 0
        for gid in survived_gold:
            rec = view.get_record(gid)
            if rec is not None:
                gold_bytes += rec.bytes if rec.bytes > 0 else _safe_bytes(rec.text)
    else:
        # Opaque store: we can't tell what survived.  Set sentinels.
        gold_survived = False   # "unknown unless measured"
        gold_bytes = 0

    used_bytes = view.used_bytes()
    if introspectable and used_bytes > 0:
        ubf = gold_bytes / used_bytes
        waste = 1.0 - ubf
    else:
        ubf = 0.0
        waste = 0.0

    t0 = time.perf_counter()
    retrieved = view.retrieve(query, k)
    latency = time.perf_counter() - t0

    retrieved_ids = [r.id for r in retrieved]
    hit = 1.0 if any(rid in gold_ids for rid in retrieved_ids) else 0.0
    # For opaque stores we can't condition on survival, so this collapses to hit.
    hit_given_retained = hit if (gold_survived or not introspectable) else float('nan')

    mrr = 0.0
    for rank, rid in enumerate(retrieved_ids, start=1):
        if rid in gold_ids:
            mrr = 1.0 / rank
            break

    return QueryEval(
        gold_survived=gold_survived,
        useful_byte_fraction=ubf,
        waste_rate=waste,
        evictions_total_at_query_time=view.event_count("evict"),
        compressions=view.event_count("compress"),
        hit_at_k=hit,
        hit_at_k_given_retained=hit_given_retained,
        mrr=mrr,
        used_bytes_end=used_bytes,
        latency_sec=latency,
        retrieved=retrieved,
        introspectable=introspectable,
    )


# ════════════════════════════════════════════════════════════════════
# Main runner
# ════════════════════════════════════════════════════════════════════


def run(
    workload: Workload,
    store: MemorySystem,
    *,
    k: int,
    dataset: str,
    store_label: str,
    policy: str = "",
    retriever_name: str = "",
    budget_bytes_report: int = 0,
    seed: int = 0,
    itemiser: str = "",
    reader_name: str = "",
    reader_impl: Reader | None = None,
    answer_max_tokens: int = 128,
) -> list[EvalResult]:
    """
    Replay a workload chronologically against the store.

    The store can be anything that satisfies MemorySystem — RetentionManager,
    LettaWrapper, Mem0Wrapper, etc.  Retention-specific metrics degrade
    gracefully for opaque stores (see QueryEval.introspectable).

    QUERY handling:
    - gold_item_ids == []  -> access-only query, updates recency/frequency
                              but is not scored
    - gold_item_ids != []  -> scored query, emits one EvalResult
    """
    view = _StoreView(store)

    results: list[EvalResult] = []
    insert_count = 0
    query_count = 0
    skip_count = 0

    budget_display = budget_bytes_report or view.budget_bytes()
    mode = "retrieval+QA" if reader_impl else "retrieval-only"
    log.info(
        "═══ Benchmark run ═══  dataset=%s  store=%s  reader=%s  "
        "budget=%s  k=%d  mode=%s  introspectable=%s",
        dataset, store_label, reader_name or "none",
        _fmt_bytes(budget_display), k, mode, view.introspectable,
    )

    t_run_start = time.perf_counter()

    for event in workload.events():
        if event.type == "insert":
            if event.item is None:
                raise ValueError("Insert event missing item.")
            view.insert(event.item)
            insert_count += 1
            if insert_count % 50 == 0 and view.introspectable:
                ub = view.used_bytes()
                bb = view.budget_bytes()
                log.debug(
                    "  [insert %4d]  used %s / %s (%.0f%%)",
                    insert_count,
                    _fmt_bytes(ub),
                    _fmt_bytes(bb),
                    100 * ub / bb if bb else 0,
                )
            continue

        if event.type != "query":
            raise ValueError(f"Unknown event type: {event.type}")

        if event.query is None:
            raise ValueError("Query event missing query.")

        gold_ids = set(event.gold_item_ids or [])

        # Unlabelled query: only creates access signal for LRU/LFU.
        if not gold_ids:
            view.retrieve(event.query, k=k)
            skip_count += 1
            log.debug("  [query skip]  id=%s  (no gold labels)", event.query.id)
            continue

        query_count += 1

        # ── retrieval evaluation ──
        base = _evaluate_scored_query(
            view=view,
            query=event.query,
            gold_ids=gold_ids,
            k=k,
        )

        # ── reader evaluation (no-op when reader_impl is None) ──
        qa = _run_reader(reader_impl=reader_impl, query=event.query, retrieved=base.retrieved,
                         answer_max_tokens=answer_max_tokens, ref_answer=event.ref_answer,
                         question_type=event.question_type)

        # For opaque stores, "SURVIVED/EVICTED" is just noise.  Show a hyphen.
        if base.introspectable:
            survived_tag = "SURVIVED" if base.gold_survived else "EVICTED"
        else:
            survived_tag = "OPAQUE  "
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
            "store=%s  evictions_total_at_query_time=%d%s",
            query_count, event.query.id, survived_tag, hit_tag,
            k, base.hit_at_k, base.mrr, base.latency_sec,
            _fmt_bytes(base.used_bytes_end), base.evictions_total_at_query_time,
            qa_detail,
        )

        results.append(
            EvalResult(
                dataset=dataset,
                query_id=event.query.id,
                policy=policy,
                budget_bytes=budget_display,
                k=k,
                itemiser=itemiser,
                retriever=retriever_name,
                reader=reader_name,
                seed=seed,
                gold_survived=base.gold_survived,
                useful_byte_fraction=base.useful_byte_fraction,
                waste_rate=base.waste_rate,
                evictions_total_at_query_time=base.evictions_total_at_query_time,
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
                store=store_label,
                introspectable=base.introspectable,
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
            "═══ Done ═══  store=%s  queries=%d (scored=%d, skipped=%d)  "
            "inserts=%d  gold_survival=%.1f%%  hit@%d=%.1f%%  mrr=%.3f  "
            "evictions_total_at_query_time=%d  compressions=%d  elapsed=%.1fs",
            store_label, query_count + skip_count, query_count, skip_count,
            insert_count, 100 * survival, k, 100 * avg_hit, avg_mrr,
            view.event_count("evict"), view.event_count("compress"), elapsed,
        )
    else:
        log.info(
            "═══ Done ═══  store=%s  no scored queries.  inserts=%d  skipped=%d  elapsed=%.1fs",
            store_label, insert_count, skip_count, elapsed,
        )

    return results