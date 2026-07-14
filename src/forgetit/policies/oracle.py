"""Gold-oracle eviction — the privileged upper-bound control, not a policy.

Never evicts an answer-bearing item while anything else remains. Converts
survival numbers into "% of achievable headroom" (the normalized-regret
ceiling): no implementable policy can beat it, so policy − random gains are
read against oracle − random.

Gold-ness is INJECTED, not inferred: item metadata does not mark gold on
every dataset (BEAM resolves gold per query, not per item), so the runner —
which sees the example's full event stream — populates ``protected_ids``
before replay. The ``wants_gold_ids = True`` class flag is the capability
signal; the runner fills the attribute on any policy that sets it.

When ONLY protected items remain and the budget still overflows, it must
evict one (the budget loop requires progress); it takes the oldest protected
item and counts the violation in ``forced_gold_evictions`` — nonzero means
the budget is too small to hold even the gold set, which is a property of
the (budget, example) pair, not a policy failure.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from forgetit.policies.base import SimpleEvictionPolicy

if TYPE_CHECKING:
    from forgetit.memory_system import RetentionManager


class OraclePolicy(SimpleEvictionPolicy):

    name = "oracle"
    wants_gold_ids = True   # runner injects protected_ids from the event stream

    def __init__(self) -> None:
        self.protected_ids: set[str] = set()
        self.forced_gold_evictions: int = 0

    def select_victim(self, store: "RetentionManager", candidates: set[str] | None = None) -> str:
        # Oldest-first (the FIFO rule) within each class — arbitrary but
        # deterministic; the oracle's claim is only about WHAT it protects,
        # not how it orders the sacrificial rest.
        best_victim: tuple[float, str] | None = None
        best_protected: tuple[float, str] | None = None

        n = 0
        for feat in store.iter_features():
            if candidates is not None and feat.id not in candidates:
                continue
            n += 1
            created = feat.created_at if feat.created_at is not None else 0.0
            key = (created, feat.id)
            if feat.id in self.protected_ids:
                if best_protected is None or key < best_protected:
                    best_protected = key
            else:
                if best_victim is None or key < best_victim:
                    best_victim = key

        if best_victim is not None:
            self._note_decision(best_victim[0], n)
            return best_victim[1]
        if best_protected is not None:
            self.forced_gold_evictions += 1
            self._note_decision(best_protected[0], n)
            return best_protected[1]
        raise RuntimeError("oracle: store empty but eviction requested.")
