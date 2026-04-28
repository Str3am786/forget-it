from __future__ import annotations

import math
from typing import TYPE_CHECKING

from forgetit.policies.base import SimpleEvictionPolicy

if TYPE_CHECKING:
    from forgetit.memory_system import RetentionManager


class EbbinghausPolicy(SimpleEvictionPolicy):
    """
    MemoryBank-style forgetting-curve eviction.

    Each item gets a retention score
        R(item) = exp( -(now - last_access) / S(item) )
    where S grows with rehearsal count (each retrieval boosts strength):
        S(item) = base_strength + strength_growth * access_count

    The victim is the lowest-R item — the one our forgetting curve
    predicts is most likely to have been forgotten.

    "now" is the latest timestamp seen across the store, so the policy
    works under benchmark replay (where timestamps live in the past).

    Reference:
        Zhong, Guo, Gao, Wang. "MemoryBank: Enhancing Large Language
        Models with Long-Term Memory." AAAI 2024.
        https://arxiv.org/abs/2305.10250
    """

    name = "ebbinghaus"

    def __init__(
        self,
        base_strength: float = 1.0,
        strength_growth: float = 1.0,
        time_unit_seconds: float = 86_400.0,  # 1 day, matches paper
    ) -> None:
        self.base_strength = base_strength
        self.strength_growth = strength_growth
        self.time_unit_seconds = time_unit_seconds

    def _strength(self, access_count: int) -> float:
        return self.base_strength + self.strength_growth * access_count

    def select_victim(self, store: "RetentionManager") -> str:
        # Pass 1: snapshot the workload's notion of "now" from the store
        # itself. Using time.time() would be wrong for replay benchmarks.
        now = 0.0
        for feat in store.iter_features():
            last = feat.last_access if feat.last_access > 0 else (feat.created_at or 0.0)
            if last > now:
                now = last

        # Pass 2: rank by retention. Tiebreak deterministically.
        victim_id: str | None = None
        best: tuple[float, float, str] | None = None
        for feat in store.iter_features():
            last = feat.last_access if feat.last_access > 0 else (feat.created_at or 0.0)
            dt = max(0.0, (now - last) / self.time_unit_seconds)
            s = self._strength(feat.access_count)
            retention = math.exp(-dt / s)

            score = (retention, feat.created_at or 0.0, feat.id)
            if best is None or score < best:
                best = score
                victim_id = feat.id

        if victim_id is None:
            raise RuntimeError("Ebbinghaus: store empty but eviction requested.")
        return victim_id