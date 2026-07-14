"""Value-per-byte ("density") eviction family — the web-cache lineage.

Two policies, one parametric family with the size baselines:

  * SalienceDecayPolicy ("salience_decay") — FiFA-adapted priority-decay
    density: frequency × recency-decay ÷ bytes. The missing age-decay cell
    between pure LFU (no aging) and pure LRU (no frequency).

  * GDSFPolicy ("gdsf") — GreedyDual-Size with Frequency: frequency ÷ bytes
    plus a global inflation clock, so recency emerges from eviction pressure
    itself, without timestamps. ``freeze_L=True`` pins the clock at 0, which
    is exactly frequency-density — the no-aging ablation as a config flag.

Family view (thesis framing): size-blind (LRU/LFU) → size-only
(keep_smallest = largest-first) → value-per-byte + explicit decay
(salience_decay) → value-per-byte + inflation aging (GDSF). GDSF with L
frozen at 0 is frequency-density exactly; with the count ignored it is
largest-first exactly.

Both are metadata-only (MemFeatures), no embeddings, no LLM.
"""
from __future__ import annotations

import math
from typing import TYPE_CHECKING

from forgetit.policies.base import SimpleEvictionPolicy

if TYPE_CHECKING:
    from forgetit.memory_system import RetentionManager


class SalienceDecayPolicy(SimpleEvictionPolicy):
    """FiFA-adapted priority-decay density eviction.

    score(item) = (1 + access_count) * 2^(-age / half_life) / bytes
    evict argmin. Ranked in LOG space (the ebbinghaus lesson — exp scores
    underflow and silently erase the frequency signal for old items):

        log2 score = log2(1 + access_count) - age/half_life - log2(bytes)

    Clock: ``last_access`` — the store's monotonic logical tick (bumped on
    insert and on every retrieval touch), NOT wall-clock. Wall-clock replay
    ages everything by dataset dates regardless of workload order; the tick
    clock decays by how much has HAPPENED since the item was last useful,
    which is the cache-relevant signal. "now" = the largest tick over ALL
    residents, so candidates-restricted calls (compression selectors) rank
    against real elapsed pressure. half_life is therefore in TICKS
    (inserts + retrieval touches), the one swept knob.

    Delta from FiFA's Priority-Decay (state in the thesis): their salience
    also carries a memory-type prior and a subtracted sensitivity/privacy
    term; MemFeatures has neither, so this is the recency-decay × frequency
    ÷ weight core only. The (1 + count) smoothing keeps cold items from all
    tying at zero, exactly when the size/decay signal should drive the choice.
    """

    name = "salience_decay"

    def __init__(self, half_life_ticks: float = 512.0) -> None:
        if half_life_ticks <= 0:
            raise ValueError(f"half_life_ticks must be > 0, got {half_life_ticks}")
        self.half_life_ticks = half_life_ticks

    def _log_score(self, now: float, feat) -> float:
        age = max(0.0, now - feat.last_access)
        return (
            math.log2(1 + feat.access_count)
            - age / self.half_life_ticks
            - math.log2(max(feat.bytes, 1))
        )

    def select_victim(self, store: "RetentionManager", candidates: set[str] | None = None) -> str:
        now = 0.0
        for feat in store.iter_features():
            if feat.last_access > now:
                now = feat.last_access

        victim_id: str | None = None
        best: tuple[float, float, str] | None = None
        n = 0
        for feat in store.iter_features():
            if candidates is not None and feat.id not in candidates:
                continue
            n += 1
            score = (self._log_score(now, feat), feat.last_access, feat.id)
            if best is None or score < best:
                best = score
                victim_id = feat.id

        if victim_id is None:
            raise RuntimeError("salience_decay: store empty but eviction requested.")
        self._note_decision(best[0], n)
        return victim_id


class GDSFPolicy(SimpleEvictionPolicy):
    """GreedyDual-Size with Frequency.

    Lineage: GreedyDual-Size (Cao & Irani, USENIX ITS 1997); frequency
    extension GDSF (Cherkasova, HP Labs 1998). The canonical size-aware
    web-cache policy, imported here for agent memory.

    Mechanics — global inflation clock L (starts 0, monotone nondecreasing):
        on insert/access of i:  H_i = L + (access_count_i + 1) / bytes_i
        on eviction:            victim = argmin H_i ; then L <- H_victim
    Untouched items keep credits computed under an older, smaller L, so they
    sink relative to fresh arrivals — recency from inflation, no timestamps.
    This is the fix for plain frequency-density's cache pollution: an item
    hammered early otherwise keeps a high count/bytes score forever.

    Integration: stateful. RetentionManager calls ``on_insert`` at admission,
    ``on_access`` on every retrieval touch, and ``on_evict`` on removal
    (capability hooks — stores without them leave the policy in insert-only
    mode, which degrades to insert-time density with arrival aging; label
    such runs "gdsf (insert-only)"). Items seen at eviction with no stored
    credit (hot-swapped policy) are backfilled at the CURRENT L, which
    slightly favours them; only occurs on backfill.

    Caveats (documented, accepted):
      * ``select_victim`` advances L — it assumes selection == eviction, the
        contract of the store's budget loop. Using GDSF as a COMPRESSION
        selector would inflate L per compressed (not evicted) item; prefer it
        as a pure eviction policy.
      * Compression shrinks record.bytes without re-crediting; the stale
        (pre-compression) H persists until the item's next access.
    """

    name = "gdsf"

    def __init__(self, *, freeze_L: bool = False) -> None:
        """``freeze_L=True`` pins the inflation clock at 0, which reduces GDSF
        to pure frequency-density ((count+1)/bytes) — the no-aging ablation
        cell, as a config flag rather than a separate class."""
        self.freeze_L = freeze_L
        if freeze_L:
            self.name = "gdsf_frozen"
        self.L: float = 0.0
        self._H: dict[str, float] = {}

    def _fresh_H(self, access_count: int, nbytes: int) -> float:
        return self.L + (access_count + 1) / max(nbytes, 1)

    # ---- store hooks (RetentionManager calls these when present) ----

    def on_insert(self, record) -> None:
        self._H[record.id] = self._fresh_H(record.access_count, record.bytes)

    def on_access(self, record) -> None:
        self._H[record.id] = self._fresh_H(record.access_count, record.bytes)

    def on_evict(self, record_id: str) -> None:
        self._H.pop(record_id, None)

    # ---- selection ----

    def select_victim(self, store: "RetentionManager", candidates: set[str] | None = None) -> str:
        victim_id: str | None = None
        best: tuple[float, str] | None = None
        n = 0
        for feat in store.iter_features():
            if candidates is not None and feat.id not in candidates:
                continue
            n += 1
            h = self._H.get(feat.id)
            if h is None:                       # backfill (see docstring)
                h = self._fresh_H(feat.access_count, feat.bytes)
                self._H[feat.id] = h
            key = (h, feat.id)
            if best is None or key < best:
                best = key
                victim_id = feat.id

        if victim_id is None:
            raise RuntimeError("gdsf: store empty but eviction requested.")
        if not self.freeze_L:
            self.L = max(self.L, best[0])       # inflation step (monotone)
        self._H.pop(victim_id, None)
        self._note_decision(best[0], n)
        return victim_id
