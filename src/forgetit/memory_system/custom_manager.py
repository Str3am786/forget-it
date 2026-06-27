from __future__ import annotations

import logging
import time
from typing import Optional, Iterable, TYPE_CHECKING

from forgetit.core import MemRecord, Query, MemFeatures
from forgetit.core.cost import record_cost, text_bytes, vector_bytes
from forgetit.core.timing import phase
from forgetit.policies import Action, Evict, Compress, Prune
from forgetit.memory_system.base import MemorySystem

if TYPE_CHECKING:
    from forgetit.policies import EvictionPolicy
    from forgetit.backend import Backend
    from forgetit.embeddings.base import Embedder

log = logging.getLogger(__name__)


def estimate_bytes(item: MemRecord) -> int:
    """Fallback storage cost for a record whose ``bytes`` was never set.

    Delegates to the single byte-cost source of truth (``core.cost.record_cost``),
    so it honours the global COUNT_VECTOR_BYTES switch exactly like every other
    byte consumer. Records that went through ``attach_embeddings`` already carry a
    pinned ``bytes`` and never reach this fallback.
    """
    return record_cost(item)


# TODO maybe look at calling it "ForgettingController"
class RetentionManager(MemorySystem):

    def __init__(self, budget_bytes: int, backend: Backend, policy: EvictionPolicy, logger,
                 embedder: "Embedder | None" = None):
        self.budget_bytes = budget_bytes
        self.backend = backend
        self.policy = policy
        self.logger = logger
        # Optional: when records carry embeddings (dense/vector retrieval), the
        # store re-embeds a record's vector after compression so it describes the
        # new (compressed) text rather than the stale original. Required for
        # correct vector retrieval under compression; harmless (unused) otherwise.
        self.embedder = embedder

        self.used_bytes = 0
        self._bytes_by_id: dict[str, int] = {}
        self._tick: int = 0
        # Global monotonic insert ordinal — the "clock" for lifetime/survival
        # analysis.  NOT reset by clear(); the offline analysis censors at clears.
        self._n_inserts: int = 0

    def connect(self) -> None:
        self.backend.connect()
        # TODO remove once cheap
        if self.backend.is_persistent:
            self.rebuild_accounting()

    def close(self) -> None:
        self.backend.close()

    def insert(self, item: MemRecord) -> None:
        # Ensure bytes exists deterministically
        b = item.bytes if item.bytes > 0 else estimate_bytes(item)
        item.bytes = b  # normalize

        if b > self.budget_bytes:
            # Under per-example relative budgets a single item can legitimately
            # exceed the cap (e.g. a large session at a low budget fraction).
            # Keep it rather than crash — it'll be the sole resident and evicted
            # as soon as anything else needs room.
            self.logger.event("oversized_insert", {
                "id": item.id, "bytes": b, "budget_bytes": self.budget_bytes,
            })

        existing = self.backend.get(item.id)
        if existing is not None:
            old_b = self._bytes_by_id.get(item.id, existing.bytes)
            self.used_bytes -= old_b
            self._bytes_by_id.pop(item.id, None)
            # Carry forward accumulated retrieval signal across re-inserts so
            # LRU/LFU don't treat a re-seen session as a cold-start.
            item.access_count = max(item.access_count, existing.access_count)

        # Eviction/compression is timed inside _ensure_budget (evict_select /
        # evict_exec); the admit work below is the disjoint "insert" phase.
        self._ensure_budget(incoming_bytes=b)

        with phase("insert"):
            # Use a monotonic insertion counter for last_access so that LRU/LFU
            # operate on workload-stream order rather than historical session dates
            # (which are non-monotonic across LongMemEval examples).
            self._tick += 1
            self._n_inserts += 1
            item.last_access = float(self._tick)
            # Leave access_count at 0 for fresh items so LFU only counts true
            # retrievals.  The carry-forward above already preserved any prior
            # retrieval signal for re-inserted items.
            self.backend.upsert(item)
            self._bytes_by_id[item.id] = b
            self.used_bytes += b

            self.logger.event(
                "insert",
                {"id": item.id, "bytes": b, "t_insert": self._n_inserts,
                 "used_bytes": self.used_bytes, "budget_bytes": self.budget_bytes},
            )

    def retrieve(self, query: Query, k: int | None = None) -> list[MemRecord]:
        # RetentionManager always honours k; the Protocol's optional-k signature
        # is about callers that don't want to supply one (e.g. Letta benchmarks).
        if k is None:
            raise ValueError("RetentionManager.retrieve requires k.")
        results = self.backend.search(query, k)

        self._tick += 1
        # TODO change to a multiple upsert
        for it in results:
            it.access_count += 1
            it.last_access = float(self._tick)
            self.backend.upsert(it)

        self.logger.event("retrieve", {"q": query.id, "hits": [r.id for r in results]})
        return results

    def get(self, item_id: str) -> Optional[MemRecord]:
        return self.backend.get(item_id)

    def get_used_bytes(self) -> int:
        # Protocol conformance — mirrors the attribute.  Keeping both because
        # internal code (and callers who already pattern-match RetentionManager)
        # uses the attribute; the method is what MemorySystem promises.
        return self.used_bytes

    def delete(self, item_id: str) -> None:
        # Maintain accounting
        freed = self._remove_record(item_id)
        self.logger.event("delete", {
            "id": item_id,
            "freed_bytes": freed,
            "used_bytes": self.used_bytes,
        })

    def clear(self) -> None:
        """Wipes the backend and resets all byte accounting"""
        self.backend.clear()
        self.used_bytes = 0
        self._bytes_by_id.clear()

        if hasattr(self.logger, "event"):
            self.logger.event("clear", {"t_insert": self._n_inserts, "budget_bytes": self.budget_bytes})

    def _execute_action(self, action: Action) -> None:
        """Dispatch a policy Action and update byte accounting"""
        if isinstance(action, Evict):
            self._do_evict(action.id)
        elif isinstance(action, Compress):
            self._do_compress(action.id, action.new_text)
        elif isinstance(action, Prune):
            raise NotImplementedError("Not Implemented yet and requires a graph backend")
        else:
            raise TypeError(f" Unknown action type: {type(action)}")

    def _do_evict(self, record_id: str) -> None:
        """Policy-driven eviction."""
        freed = self._remove_record(record_id)
        self.logger.event("evict", {
            "id": record_id,
            "freed_bytes": freed,
            "t_insert": self._n_inserts,
            "used_bytes": self.used_bytes,
        })

    def _do_compress(self, record_id: str, new_text: str) -> None:
        record = self.backend.get(record_id)

        if record is None:
            # TODO re-evaluate, this only happens if the record disappears mid way.
            return

        # A record's cost = text term (shrinkable) + a fixed term (the vector,
        # when COUNT_VECTOR_BYTES is on). Compression only rewrites the text, so
        # it credits savings against the text term and PRESERVES the fixed term.
        # Recover the fixed term from the authoritative total minus the *current*
        # text bytes; in the default text-only model it is 0 and this reduces to
        # the old text-vs-text accounting exactly.
        old_total = self._bytes_by_id.get(record_id, 0)
        fixed_bytes = max(0, old_total - text_bytes(record))
        new_total = len(new_text.encode("utf-8")) + fixed_bytes

        # Compression didn't help (text didn't actually shrink the total).
        if new_total >= old_total:
            self.logger.event("compress_skipped", {
                "id": record_id,
                "old_bytes": old_total,
                "new_bytes": new_total,
            })
            return

        # Compression worked
        freed = old_total - new_total
        record.text = new_text
        record.bytes = new_total  # keep record.bytes in sync so the policy
                                  # (which reads record.bytes as the cost) doesn't
                                  # see a stale, pre-compression size.
        record.meta["compressed"] = True
        record.meta["compression_saved_bytes"] = freed

        # Re-embed: the vector must describe the NEW (compressed) text, or the
        # embedding retriever would match this item by its stale pre-compression
        # vector. Same model → same dimensionality, so the fixed (vector) byte
        # term computed above is unchanged. This is about retrieval correctness,
        # so it runs whether or not the vector counts toward the budget.
        if record.embedding is not None:
            if self.embedder is not None:
                try:
                    with phase("reembed"):
                        record.embedding = self.embedder.embed_one(new_text)
                except Exception as e:
                    # Keep the compression (text already saved) rather than abort
                    # the budget loop; surface the stale vector loudly.
                    self.logger.event("compress_reembed_failed", {"id": record_id})
                    log.warning("[compress] %s re-embed failed — keeping stale vector: %s",
                                record_id, e)
            else:
                self.logger.event("compress_stale_embedding", {"id": record_id})
                log.warning("[compress] %s compressed but no embedder configured — its "
                            "vector now describes the pre-compression text. Pass "
                            "embedder= to RetentionManager for vector-retrieval runs.",
                            record_id)

        self.backend.upsert(record)

        self._bytes_by_id[record_id] = new_total
        self.used_bytes += (new_total - old_total)

        self.logger.event("compress", {
            "id": record_id,
            "old_bytes": old_total,
            "new_bytes": new_total,
            "freed_bytes": freed,
            "t_insert": self._n_inserts,
            "used_bytes": self.used_bytes,
        })

    def _ensure_budget(self, incoming_bytes: int) -> None:
        # Each iteration must make progress (free bytes or drop a record).
        # If a policy/compression action does neither, the loop would spin
        # forever; we surface that as a clear error instead of hanging.
        while self.used_bytes + incoming_bytes > self.budget_bytes:
            if self.backend.count() == 0:
                # Nothing left to evict — the incoming item alone exceeds the
                # budget. Accept the over-budget state for this single item.
                return
            before_bytes = self.used_bytes
            before_count = self.backend.count()

            # evict_select = deciding what to evict/compress (for compression this
            # includes the summarise LLM call, recorded as the compress_llm
            # sub-phase). evict_exec = applying it (compression's re-embed is the
            # reembed sub-phase).
            #
            # Per-DECISION latency is also captured here (not just the run-level
            # aggregate the PhaseTimer accumulates) and stamped with the store
            # OCCUPANCY at decision time (n_resident / used_bytes). That pairing is
            # what turns a mean ("78µs/decision") into a complexity-class claim:
            # plot dur_select_s vs n_resident and read the slope — O(1) LRU stays
            # flat, an O(n) semantic/coverage scan slopes up as the store fills.
            t0 = time.perf_counter()
            with phase("evict_select"):
                action = self.policy.select_action(self)
            t1 = time.perf_counter()
            with phase("evict_exec"):
                self._execute_action(action=action)
            t2 = time.perf_counter()

            self.logger.event("decision", {
                "action": type(action).__name__,
                "id": getattr(action, "id", None),
                "n_resident": before_count,        # store occupancy at decision time
                "used_bytes": before_bytes,
                "budget_bytes": self.budget_bytes,
                "t_insert": self._n_inserts,
                "dur_select_s": t1 - t0,           # decision cost (policy.select_action)
                "dur_exec_s": t2 - t1,             # apply cost (evict/compress + re-embed)
            })

            made_progress = (
                self.used_bytes < before_bytes
                or self.backend.count() < before_count
            )
            if not made_progress:
                raise RuntimeError(
                    f"Eviction made no progress: used_bytes={self.used_bytes}, "
                    f"budget={self.budget_bytes}, incoming={incoming_bytes}, "
                    f"last_action={type(action).__name__}(id={getattr(action, 'id', '?')}). "
                    "Policy returned an action that neither freed bytes nor removed a record."
                )

    def _remove_record(self, record_id: str) -> int:
        """Remove a record from backend and accounting. Returns bytes freed.

        No logging — callers decide what event name and payload to use.
        """
        freed = self._bytes_by_id.get(record_id, 0)
        self.backend.delete(record_id)              # if this throws, nothing changed
        self._bytes_by_id.pop(record_id, None)
        self.used_bytes = max(0, self.used_bytes - freed)
        return freed

    def rebuild_accounting(self) -> None:
        self.used_bytes = 0
        self._bytes_by_id.clear()

        for item_id, b in self.backend.scan_accounting():
            self._bytes_by_id[item_id] = b
            self.used_bytes += b

    def iter_ids(self):
        return self.backend.iter_ids()

    def iter_features(self) -> Iterable[MemFeatures]:
        return self.backend.scan_features()

    def iter_items(self):
        # TODO add 'feature cache' or policy-specific indexes.
        for item_id in self.backend.iter_ids():
            it = self.backend.get(item_id)
            if it is not None:
                yield it