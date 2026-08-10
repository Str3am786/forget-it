from __future__ import annotations

import json
import logging
import time

from collections import Counter

from forgetit.benchmark.workloads.base import Workload
from forgetit.benchmark.schema import EvalResult, QueryEval, QAFields
from forgetit.benchmark.metrics import EVAL_K_CURVE, ranking_columns
from forgetit.benchmark.grading import grade
from forgetit.core.timing import PhaseTimer, use_timer, phase
from forgetit.memory_system.base import MemorySystem
from forgetit.core import Question, Query
from forgetit.llm import Reader

log = logging.getLogger(__name__)

# Smoke-run guard: when set, every scored query asserts the read-only
# invariant against the store's counter_state_hash (see custom_manager).
import os
_ASSERT_READONLY_EVAL = os.environ.get("FORGETIT_ASSERT_READONLY_EVAL", "") == "1"

# Eval depth: introspectable stores are ranked at FULL depth (every resident,
# no truncation), so gold_ranks is uncensored — rank 11 and rank 300 are
# distinguishable, and rank-percentile / k∝N / rank-depth distributions become
# exact offline recomputes. Cost ≈ 0: BM25/lexical already score every resident
# and truncate at the end; only gold positions are stored per row. The reader
# still only sees the configured top-k. This constant remains as the FLOOR for
# opaque stores (no resident count available) and as the k-curve max cutoff.
RETRIEVAL_EVAL_DEPTH = max(EVAL_K_CURVE)


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
        True iff we can enumerate resident items.  Needed for gold_id_retained,
        useful_byte_fraction, and non_gold_byte_fraction.  Opaque stores report False
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

    def search(self, query: Query, k: int):
        """
        READ-ONLY retrieval for EVAL queries: same ranking as ``retrieve`` but
        with no access/recency side-effects on the store (RetentionManager
        exposes ``search`` for exactly this). Eval must not feed policy state —
        only ingestion probes may. Opaque stores without a read-only path fall
        back to ``retrieve``; their rows already carry introspectable=False, so
        the read-only guarantee is scoped to introspectable stores.
        """
        fn = getattr(self.store, "search", None)
        if callable(fn):
            return fn(query, k=k)
        return self.retrieve(query, k)

    def safe_call(self, method_name: str) -> None:
        """Call ``store.method_name()`` if it exists.  Used for connect/close."""
        fn = getattr(self.store, method_name, None)
        if callable(fn):
            fn()

    def log_event(self, name: str, payload: dict) -> None:
        """Emit an event through the store's logger if it has one (no-op otherwise)."""
        logger = getattr(self.store, "logger", None)
        if logger is not None and hasattr(logger, "event"):
            logger.event(name, payload)


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
    grader_embedder=None,
    grade_sim_threshold: float = 0.83,
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
    meta = answer_result.meta or {}

    if getattr(answer_result, "errored", False):
        # No answer to grade. Grading it would score an infrastructure failure
        # as a content verdict.
        return QAFields(
            reader_answer="", abstained=False, reader_errored=True,
            qa_latency_sec=answer_result.latency_sec,
            ref_answer=ref_answer, question_text=query.text,
            question_type=question_type,
            reader_evidence_delivered=int(meta.get("evidence_delivered", 0) or 0),
            reader_evidence_truncated=bool(meta.get("evidence_truncated") or False),
            reader_cap_hit=meta.get("cap_hit"),
            reader_item_ids=json.dumps(meta.get("item_ids", [])),
        )

    # Deterministic headline grade (reproducible, no models). The paraphrase
    # (embedding) tier is OPT-IN via grader_embedder — threshold stays a
    # parameter (calibration happens on the budget=1.0 run); the judge tier
    # stays offline. reader_answer + ref_answer are both persisted, so past
    # runs can always be re-graded.
    g = grade(answer_result.answer, ref_answer, abstained=answer_result.abstained,
              embedder=grader_embedder, sim_threshold=grade_sim_threshold)

    meta = answer_result.meta or {}
    return QAFields(
        reader_answer=answer_result.answer,
        abstained=answer_result.abstained,
        qa_latency_sec=answer_result.latency_sec,
        ref_answer=ref_answer,
        question_text=query.text,
        question_type=question_type,
        qa_correct=g["correct"],
        qa_f1=g["f1"],
        # Reader-context audit: how much of `retrieved` the prompt carried.
        # Absent on an LLM-error result, and on readers that don't report it.
        reader_evidence_delivered=int(meta.get("evidence_delivered", 0) or 0),
        reader_evidence_truncated=bool(meta.get("evidence_truncated") or False),
        reader_errored=False,
        reader_cap_hit=meta.get("cap_hit"),
        reader_item_ids=json.dumps(meta.get("item_ids", [])),
    )


def _collect_gold_snapshot(view: _StoreView, events, example_id: str,
                           orig_text: dict[str, str], orig_bytes: dict[str, int]) -> list[dict]:
    """Per-example gold ANSWER-survival snapshot — the data the offline Tier-1
    answer-survival pass needs that events.jsonl (bytes, not text) cannot give.

    MUST be called at the END of an example's replay (store at its final
    post-compression/post-eviction state), BEFORE the store is discarded — so
    ``resident_text`` is R_g, the text the gold item ACTUALLY survived as, not
    its pre-compression form. Reads only GOLD ids (not the whole store).

    For each (query, gold_id):
      original_text (O_g) — the item's text as FIRST inserted, passed in via
        ``orig_text`` (captured at insert time in the replay loop). It CANNOT be
        re-read from the events/store here: compression reassigns ``record.text``
        in place on the very object the insert event holds, so by now that object
        carries the COMPRESSED text. Capturing the original string at insert is
        the only way to keep O_g truly pre-compression.
      resident_text (R_g) — the live store's current text for the gold id, or
        "" if it was hard-evicted (so eviction-loss and compression-loss unify:
        an evicted gold scores 0 on containment automatically).
    """
    first_text, first_bytes = orig_text, orig_bytes

    rows: list[dict] = []
    for e in events:
        if e.type != "query" or e.query is None or not e.gold_item_ids:
            continue
        for gid in e.gold_item_ids:
            rec = view.get_record(gid)                 # FINAL resident state
            resident = rec is not None
            resident_text = rec.text if resident else ""
            rows.append({
                "example_id": example_id,
                "query_id": e.query.id,
                "gold_id": gid,
                "answer": e.ref_answer,
                "question_type": e.question_type,
                "original_text": first_text.get(gid, ""),
                "orig_bytes": int(first_bytes.get(gid, 0)),
                "resident_text": resident_text,
                "resident_bytes": int(
                    rec.bytes if (resident and rec.bytes > 0)
                    else _safe_bytes(resident_text)
                ),
                "was_evicted": not resident,
                # record.meta["compressed"] is set only by a successful compress
                # (new_bytes < old_bytes); evicted items can't report it.
                "was_compressed": bool(rec.meta.get("compressed", False)) if resident else False,
            })
    return rows


def _query_coverage(view: _StoreView, query: Query, introspectable: bool) -> float:
    """Max cosine between the query and any RESIDENT item vector (NaN if items
    aren't embedded). Uses the store's embedder to embed the query (reusing
    query.embedding when present). Scores the kept set's ability to answer at
    all, before — and independent of — whichever retriever ranks."""
    if not introspectable:
        return float("nan")
    embedder = getattr(view.store, "embedder", None)
    if embedder is None:
        return float("nan")

    resident_vecs = []
    for rid in view.retained_ids():
        rec = view.get_record(rid)
        if rec is not None and rec.embedding is not None:
            resident_vecs.append(rec.embedding)
    if not resident_vecs:
        return float("nan")

    import numpy as np
    from forgetit.embeddings.similarity import max_cross_similarity

    qv = query.embedding if query.embedding is not None else embedder.embed_one(query.text)
    q = np.asarray([qv], dtype=np.float32)
    mat = np.asarray(resident_vecs, dtype=np.float32)
    # assume_normalized=False: the helper renormalises, so a pre-set (possibly
    # unnormalised) query.embedding is handled correctly.
    return float(max_cross_similarity(q, mat, assume_normalized=False)[0])


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
    gold_id_retained, useful_byte_fraction, non_gold_byte_fraction.  For opaque stores
    those are set to sentinel values and ``introspectable=False`` on the
    output row — don't average them across store types without filtering.
    """
    introspectable = view.introspectable

    if introspectable:
        retained = view.retained_ids()
        n_residents = len(retained)
        survived_gold = gold_ids & retained
        gold_id_retained = len(survived_gold) > 0

        gold_bytes = 0
        for gid in survived_gold:
            rec = view.get_record(gid)
            if rec is not None:
                gold_bytes += rec.bytes if rec.bytes > 0 else _safe_bytes(rec.text)
    else:
        # Opaque store: we can't tell what survived.  Set sentinels.
        gold_id_retained = False   # "unknown unless measured"
        gold_bytes = 0
        n_residents = 0         # sentinel: resident count unknown

    used_bytes = view.used_bytes()
    if introspectable and used_bytes > 0:
        ubf = gold_bytes / used_bytes
        non_gold = 1.0 - ubf
    else:
        ubf = 0.0
        non_gold = 0.0

    # Pre-retrieval retrievability ("query coverage"): cosine of the query to its
    # NEAREST resident item — whether the kept set can answer at all, as a
    # property of what the policy KEPT, independent of which retriever ranks.
    # Only defined when items are embedded (dense configs carry store.embedder).
    query_coverage = _query_coverage(view, query, introspectable)

    # Rank at FULL depth when the resident count is known (uncensored
    # gold_ranks — see RETRIEVAL_EVAL_DEPTH); opaque stores fall back to the
    # fixed depth. Only the configured top-k reaches the reader, so the QA
    # context is unchanged by the metric depth. The eval retrieval is
    # READ-ONLY (view.search): scored queries must not bump access counts —
    # a full-depth MUTATING retrieval would rehearse every resident on every
    # question, which is why the read-only path landed first.
    eval_k = n_residents if n_residents > 0 else max(k, RETRIEVAL_EVAL_DEPTH)
    eval_k = max(eval_k, k)

    # R8 runtime enforcement (smoke runs): a scored query must leave every
    # policy-visible counter untouched. O(n) per query, so opt-in via env —
    # turn it ON for the pre-launch smoke, OFF for the campaign; the unit
    # test carries the invariant permanently either way.
    _assert_ro = _ASSERT_READONLY_EVAL and callable(
        getattr(view.store, "counter_state_hash", None))
    pre_hash = view.store.counter_state_hash() if _assert_ro else None

    t0 = time.perf_counter()
    with phase("retrieve"):
        retrieved_full = view.search(query, eval_k)

    if _assert_ro and view.store.counter_state_hash() != pre_hash:
        raise RuntimeError(
            f"R8 violation: scored query {query.id!r} mutated policy-visible "
            "state through the eval path. search() must be read-only."
        )
    latency = time.perf_counter() - t0

    retrieved_ids = [r.id for r in retrieved_full]
    # 1-based ranks of gold within the ranked list — the sufficient statistic for
    # hit@k / nDCG@k / recall@k / MRR at any cutoff (computed offline in metrics).
    gold_ranks = [rank for rank, rid in enumerate(retrieved_ids, start=1) if rid in gold_ids]

    # Back-compat scalars: hit at the configured k, MRR over the eval depth.
    hit = 1.0 if any(r <= k for r in gold_ranks) else 0.0
    # For opaque stores we can't condition on survival, so this collapses to hit.
    hit_given_retained = hit if (gold_id_retained or not introspectable) else float('nan')
    mrr = (1.0 / gold_ranks[0]) if gold_ranks else 0.0

    return QueryEval(
        gold_id_retained=gold_id_retained,
        useful_byte_fraction=ubf,
        non_gold_byte_fraction=non_gold,
        evictions_total_at_query_time=view.event_count("evict"),
        compressions=view.event_count("compress"),
        hit_at_k=hit,
        hit_at_k_given_retained=hit_given_retained,
        mrr=mrr,
        n_gold=len(gold_ids),
        n_retrieved=len(retrieved_ids),
        n_residents=n_residents,
        gold_ranks=gold_ranks,
        query_coverage=query_coverage,
        used_bytes_end=used_bytes,
        latency_sec=latency,
        retrieved=retrieved_full[:k],   # reader sees only the configured top-k
        introspectable=introspectable,
    )


# ════════════════════════════════════════════════════════════════════
# Main runner
# ════════════════════════════════════════════════════════════════════


def _apply_example_budget(store, frac: float, footprints, idx: int) -> None:
    """Set the store's byte budget to ``frac × footprint`` for example ``idx``.

    No-op when not in relative-budget mode (frac<=0 / no footprints) or for
    stores without a mutable ``budget_bytes`` (e.g. opaque Letta).
    """
    if frac <= 0 or not footprints or not hasattr(store, "budget_bytes"):
        return
    fp = footprints[idx] if idx < len(footprints) else footprints[-1]
    store.budget_bytes = max(1, round(frac * fp))


def run_one_example(
    events,
    store: MemorySystem,
    *,
    k: int,
    dataset: str,
    store_label: str,
    policy: str = "",
    retriever_name: str = "",
    budget_bytes_report: int = 0,
    budget_frac: float = 0.0,
    footprint: int | None = None,
    seed: int = 0,
    itemiser: str = "",
    reader_name: str = "",
    collect_reader_context: bool = False,
    collect_events: bool = False,
    collect_gold_snapshot: bool = False,
    example_id: str = "",
    answer_max_tokens: int = 128,
) -> tuple[list[EvalResult], list[tuple], list[dict], list[dict]]:
    """Replay ONE example's events against ONE fresh store.

    This is the policy-neutral unit of the across-example concurrent runner
    (see ``run_pipeline_concurrent``). Every eviction/compression decision
    inside an example stays serial and identical to the single-store ``run``;
    only the chains of *different* examples overlap.

    Differences from ``run`` (all throughput-only, none change the science):
      - The store is assumed FRESH and isolated — there is no ``reset``/``clear``
        handling (isolation is by instantiation). The per-example budget is sized
        once, before any insert, from this example's own ``footprint``.
      - The reader is NEVER called here. Scored queries record their retrieved
        context so a single concurrent Pass 2 can answer them all (race-free,
        since the reader is a pure function of question+context). See the driver.
      - Cumulative odometers (``evictions_total_at_query_time``, ``compressions``)
        are naturally per-example here because the store is fresh — which is the
        honest per-example number, not the run-wide odometer the serial path
        reports. The retrieval metrics (gold_id_retained/hit_at_k/mrr/used_bytes_end)
        are identical to the serial path.

    Returns ``(results, context_rows, event_rows)``:
      results      — one EvalResult per scored query (reader fields left default)
      context_rows — ``(eval_result, query, retrieved, ref_answer, qtype)`` tuples
                     for Pass 2; empty unless ``collect_reader_context``
      event_rows   — this example's logger entries as dicts, for the merged
                     events.jsonl; empty unless ``collect_events``
      gold_rows    — per (query, gold-id) answer-survival snapshot rows (O_g/R_g
                     text), captured AT FINAL STORE STATE; empty unless
                     ``collect_gold_snapshot``. See :func:`_collect_gold_snapshot`.
    """
    view = _StoreView(store)
    view.safe_call("connect")

    # Dense retrieval: if the store carries an embedder, attach a vector to each
    # of this example's items in ONE batched call before replaying. The same
    # embedder backs the store's EmbeddingRetriever (query vectors) and re-embed
    # on compress, so item / query / compressed vectors are all from one model.
    if getattr(store, "embedder", None) is not None:
        from forgetit.embeddings.ingest import attach_embeddings
        items = [e.item for e in events if e.type == "insert" and e.item is not None]
        if items:
            with phase("embed_ingest"):
                attach_embeddings(items, store.embedder)

    # Relative budget: size the fresh store to THIS example's own footprint
    # before any insert (mirrors run()'s per-reset re-sizing, minus the reset).
    if budget_frac > 0 and footprint is not None and hasattr(store, "budget_bytes"):
        store.budget_bytes = max(1, round(budget_frac * footprint))

    # Oracle control: a policy that declares wants_gold_ids gets this example's
    # full gold set before replay (privileged by design — it is the upper-bound
    # ceiling, not an implementable policy). Injected here because item metadata
    # does not mark gold on every dataset (BEAM resolves gold per query).
    policy_obj = getattr(store, "policy", None)
    if getattr(policy_obj, "wants_gold_ids", False):
        gold_counts: Counter[str] = Counter(
            gid for e in events
            if e.type == "query" and e.gold_item_ids
            for gid in e.gold_item_ids
        )
        policy_obj.protected_ids = set(gold_counts)
        policy_obj.gold_counts = gold_counts
        policy_obj.forced_gold_evictions = 0
        # policy_obj.protected_ids = {
        #     gid for e in events
        #     if e.type == "query" and e.gold_item_ids
        #     for gid in e.gold_item_ids
        # }

    results: list[EvalResult] = []
    context_rows: list[tuple] = []
    insert_count = 0
    prev_inserts = 0
    prev_evicts = 0
    prev_compresses = 0
    # O_g capture: the original (pre-compression) text per id, snapshotted at
    # insert time. Must be taken here — compression later reassigns record.text
    # on this same object, so reading it after the loop would yield R_g, not O_g.
    orig_text: dict[str, str] = {}
    orig_bytes: dict[str, int] = {}

    for event in events:
        if event.type == "insert":
            if event.item is None:
                raise ValueError("Insert event missing item.")
            if collect_gold_snapshot and event.item.id not in orig_text:
                orig_text[event.item.id] = event.item.text          # first insert wins
                orig_bytes[event.item.id] = (
                    event.item.bytes if event.item.bytes > 0 else _safe_bytes(event.item.text)
                )
            view.insert(event.item)
            insert_count += 1
            continue

        if event.type == "reset":
            # iter_examples() never emits resets; tolerate defensively (no-op,
            # since each example already owns a fresh store).
            continue

        if event.type != "query":
            raise ValueError(f"Unknown event type: {event.type}")
        if event.query is None:
            raise ValueError("Query event missing query.")

        gold_ids = set(event.gold_item_ids or [])

        # Unlabelled query: access signal only (LRU/LFU/Ebbinghaus warmup).
        if not gold_ids:
            with phase("retrieve"):
                view.retrieve(event.query, k=k)
            continue

        # Record gold mapping for offline survival/regret/value-decay analysis.
        view.log_event("gold", {"query_id": event.query.id, "gold_ids": sorted(gold_ids)})

        base = _evaluate_scored_query(view=view, query=event.query, gold_ids=gold_ids, k=k)

        inserts_delta = insert_count - prev_inserts
        evicts_delta = base.evictions_total_at_query_time - prev_evicts
        compresses_delta = base.compressions - prev_compresses
        prev_inserts = insert_count
        prev_evicts = base.evictions_total_at_query_time
        prev_compresses = base.compressions

        eval_budget = view.budget_bytes() if budget_frac > 0 else budget_bytes_report

        # Reader fields default-empty; Pass 2 fills them via the context row.
        qa = QAFields(ref_answer=event.ref_answer, question_text=event.query.text,
                      question_type=event.question_type)

        result = EvalResult(
            dataset=dataset,
            query_id=event.query.id,
            policy=policy,
            budget_bytes=eval_budget,
            budget_frac=budget_frac,
            k=k,
            itemiser=itemiser,
            retriever=retriever_name,
            reader=reader_name,
            seed=seed,
            gold_id_retained=base.gold_id_retained,
            useful_byte_fraction=base.useful_byte_fraction,
            non_gold_byte_fraction=base.non_gold_byte_fraction,
            evictions_total_at_query_time=base.evictions_total_at_query_time,
            compressions=base.compressions,
            inserts_since_last_query=inserts_delta,
            evictions_since_last_query=evicts_delta,
            compressions_since_last_query=compresses_delta,
            hit_at_k=base.hit_at_k,
            hit_at_k_given_retained=base.hit_at_k_given_retained,
            mrr=base.mrr,
            n_gold=base.n_gold,
            n_retrieved=base.n_retrieved,
            n_residents=base.n_residents,
            gold_ranks=json.dumps(base.gold_ranks),
            query_coverage=base.query_coverage,
            used_bytes_end=base.used_bytes_end,
            latency_sec=base.latency_sec,
            reader_answer=qa.reader_answer,
            abstained=qa.abstained,
            qa_latency_sec=qa.qa_latency_sec,
            qa_correct=qa.qa_correct,
            qa_f1=qa.qa_f1,
            reader_evidence_delivered=qa.reader_evidence_delivered,
            reader_evidence_truncated=qa.reader_evidence_truncated,
            reader_errored=qa.reader_errored,
            reader_cap_hit=qa.reader_cap_hit,
            reader_item_ids=qa.reader_item_ids,
            ref_answer=qa.ref_answer,
            question_text=qa.question_text,
            question_type=qa.question_type,
            store=store_label,
            introspectable=base.introspectable,
            **ranking_columns(base.gold_ranks, base.n_gold,
                              base.introspectable, base.gold_id_retained,
                              base.n_residents),
        )
        results.append(result)

        if collect_reader_context:
            context_rows.append((result, event.query, base.retrieved,
                                 event.ref_answer, event.question_type))

    forced = int(getattr(policy_obj, "forced_gold_evictions", 0))
    for r in results:
        r.oracle_forced_gold_evictions = forced

    event_rows: list[dict] = []
    if collect_events:
        logger = getattr(store, "logger", None)
        entries = getattr(logger, "log", None) if logger is not None else None
        if entries:
            for e in entries:
                event_rows.append({"name": e.name, "t": e.timestamp, **e.payload})

    # Answer-survival snapshot: read GOLD text at the store's FINAL state, here —
    # after the replay loop, before the caller disposes the store. Doing this any
    # earlier would capture pre-compression text and make every answer look
    # retained (the exact blindness this metric removes).
    gold_rows: list[dict] = (
        _collect_gold_snapshot(view, events, example_id, orig_text, orig_bytes)
        if collect_gold_snapshot else []
    )

    return results, context_rows, event_rows, gold_rows


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
    budget_frac: float = 0.0,
    example_footprints: list[int] | None = None,
    seed: int = 0,
    itemiser: str = "",
    reader_name: str = "",
    reader_impl: Reader | None = None,
    answer_max_tokens: int = 128,
    reader_workers: int = 1,
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

    # Running totals at the previous scored query, so we can report per-query
    # deltas (activity since the last query) instead of only the cumulative
    # odometers, which are easy to misread as per-example counts.
    prev_inserts = 0
    prev_evicts = 0
    prev_compresses = 0

    # Relative budgets: size the store to example 0 before any insert, then
    # re-size at each reset boundary.
    example_idx = 0
    _apply_example_budget(store, budget_frac, example_footprints, example_idx)

    # Two-pass reader: when reader_workers > 1, the store-touching Pass 1 (this
    # loop) records each query's retrieved context instead of calling the reader
    # inline; Pass 2 fires all reader calls concurrently. The reader is a pure
    # function of (question, retrieved context) — no store, no shared mutable
    # state — so concurrent calls are race-free, and there's no CPU between them
    # so a thread pool overlaps the LLM waits near-perfectly.
    defer_reader = reader_impl is not None and reader_workers > 1
    deferred: list[tuple] = []   # (result_index, query, retrieved, ref_answer, question_type)

    # The serial path streams workload.events() and cannot pre-scan gold ids;
    # the oracle control needs the per-example runner (run_pipeline_concurrent).
    # Failing loudly beats silently running an UNPROTECTED oracle.
    if getattr(getattr(store, "policy", None), "wants_gold_ids", False):
        raise ValueError(
            "Policy requests gold-id injection (wants_gold_ids=True), which the "
            "streaming run() path cannot provide — use the per-example concurrent "
            "runner (run_pipeline_concurrent / the QA runners)."
        )

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

        if event.type == "reset":
            # Example-boundary signal: wipe the store so each example is
            # evaluated in isolation.  Only fires when the workload emits
            # reset events (controlled by clear_between_examples=True).
            clear_fn = getattr(store, "clear", None)
            if callable(clear_fn):
                clear_fn()
            # Re-size the budget to the next example's own footprint.
            example_idx += 1
            _apply_example_budget(store, budget_frac, example_footprints, example_idx)
            log.debug("  [reset]  store cleared; budget=%s",
                      _fmt_bytes(view.budget_bytes()))
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

        # Record which items are gold for this query — needed offline to compute
        # survival / regret / value-decay curves from the event log.
        view.log_event("gold", {"query_id": event.query.id, "gold_ids": sorted(gold_ids)})

        # ── retrieval evaluation ──
        base = _evaluate_scored_query(
            view=view,
            query=event.query,
            gold_ids=gold_ids,
            k=k,
        )

        # Per-query activity = totals now minus totals at the previous scored query.
        inserts_delta = insert_count - prev_inserts
        evicts_delta = base.evictions_total_at_query_time - prev_evicts
        compresses_delta = base.compressions - prev_compresses
        prev_inserts = insert_count
        prev_evicts = base.evictions_total_at_query_time
        prev_compresses = base.compressions

        # ── reader evaluation ──
        # Inline unless deferred to the concurrent Pass 2 (reader_workers > 1).
        if defer_reader:
            qa = QAFields(ref_answer=event.ref_answer, question_text=event.query.text,
                          question_type=event.question_type)   # answer filled in Pass 2
            deferred.append((len(results), event.query, base.retrieved,
                             event.ref_answer, event.question_type))
        else:
            qa = _run_reader(reader_impl=reader_impl, query=event.query, retrieved=base.retrieved,
                             answer_max_tokens=answer_max_tokens, ref_answer=event.ref_answer,
                             question_type=event.question_type)

        # For opaque stores, "SURVIVED/EVICTED" is just noise.  Show a hyphen.
        if base.introspectable:
            survived_tag = "SURVIVED" if base.gold_id_retained else "EVICTED"
        else:
            survived_tag = "OPAQUE  "
        hit_tag = "HIT" if base.hit_at_k > 0 else "MISS"

        qa_detail = ""
        if reader_impl is not None and not defer_reader:
            ans_preview = (qa.reader_answer[:60] + "…") if len(qa.reader_answer) > 60 else qa.reader_answer
            qa_detail = (
                f"  answer={'ABSTAINED' if qa.abstained else repr(ans_preview)}"
                f"  qa_latency={qa.qa_latency_sec:.2f}s"
            )

        log.info(
            "  [query %3d]  id=%-20s  gold=%s  retrieval=%s  "
            "hit@%d=%.0f  mrr=%.2f  latency=%.3fs  store=%s  "
            "inserts=%d  evicted=%d  compressed=%d  (evict_total=%d)%s",
            query_count, event.query.id, survived_tag, hit_tag,
            k, base.hit_at_k, base.mrr, base.latency_sec,
            _fmt_bytes(base.used_bytes_end),
            inserts_delta, evicts_delta, compresses_delta,
            base.evictions_total_at_query_time, qa_detail,
        )

        # In relative mode budget_bytes varies per example — report the current one.
        eval_budget = view.budget_bytes() if budget_frac > 0 else budget_display

        results.append(
            EvalResult(
                dataset=dataset,
                query_id=event.query.id,
                policy=policy,
                budget_bytes=eval_budget,
                budget_frac=budget_frac,
                k=k,
                itemiser=itemiser,
                retriever=retriever_name,
                reader=reader_name,
                seed=seed,
                gold_id_retained=base.gold_id_retained,
                useful_byte_fraction=base.useful_byte_fraction,
                non_gold_byte_fraction=base.non_gold_byte_fraction,
                evictions_total_at_query_time=base.evictions_total_at_query_time,
                compressions=base.compressions,
                inserts_since_last_query=inserts_delta,
                evictions_since_last_query=evicts_delta,
                compressions_since_last_query=compresses_delta,
                hit_at_k=base.hit_at_k,
                hit_at_k_given_retained=base.hit_at_k_given_retained,
                mrr=base.mrr,
                n_gold=base.n_gold,
                n_retrieved=base.n_retrieved,
                n_residents=base.n_residents,
                gold_ranks=json.dumps(base.gold_ranks),
                query_coverage=base.query_coverage,
                used_bytes_end=base.used_bytes_end,
                latency_sec=base.latency_sec,
                reader_answer=qa.reader_answer,
                abstained=qa.abstained,
                qa_latency_sec=qa.qa_latency_sec,
                qa_correct=qa.qa_correct,
                qa_f1=qa.qa_f1,
                reader_evidence_delivered=qa.reader_evidence_delivered,
                reader_evidence_truncated=qa.reader_evidence_truncated,
                reader_errored=qa.reader_errored,
                reader_cap_hit=qa.reader_cap_hit,
                reader_item_ids=qa.reader_item_ids,
                ref_answer=qa.ref_answer,
                question_text=qa.question_text,
                question_type=qa.question_type,
                store=store_label,
                introspectable=base.introspectable,
                **ranking_columns(base.gold_ranks, base.n_gold,
                                  base.introspectable, base.gold_id_retained,
                                  base.n_residents),
            )
        )

    # ── Pass 2: concurrent reader over recorded contexts (store-free, race-free) ──
    if deferred:
        from concurrent.futures import ThreadPoolExecutor
        log.info("  reader Pass 2: %d calls across %d workers", len(deferred), reader_workers)

        def _answer(task):
            idx, query, retrieved, ref, qtype = task
            return idx, _run_reader(reader_impl=reader_impl, query=query, retrieved=retrieved,
                                    answer_max_tokens=answer_max_tokens, ref_answer=ref,
                                    question_type=qtype)

        with ThreadPoolExecutor(max_workers=reader_workers) as pool:
            for idx, qa2 in pool.map(_answer, deferred):
                r = results[idx]                      # EvalResult is a mutable dataclass
                r.reader_answer = qa2.reader_answer
                r.abstained = qa2.abstained
                r.qa_latency_sec = qa2.qa_latency_sec
                r.qa_correct = qa2.qa_correct
                r.qa_f1 = qa2.qa_f1
                r.reader_evidence_delivered = qa2.reader_evidence_delivered
                r.reader_evidence_truncated = qa2.reader_evidence_truncated
                r.reader_errored = qa2.reader_errored
                r.reader_cap_hit = qa2.reader_cap_hit
                r.reader_item_ids = qa2.reader_item_ids

    elapsed = time.perf_counter() - t_run_start

    # ── summary ──
    n = len(results)
    if n > 0:
        avg_hit = sum(r.hit_at_k for r in results) / n
        avg_mrr = sum(r.mrr for r in results) / n
        survival = sum(1 for r in results if r.gold_id_retained) / n
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