"""Fiat policies for fixtures and counterfactual experiments.

``ScriptedEvictionPolicy`` (VERIFICATION.md §7 Corpus E; corpus_e e008; D6
gold-purge) evicts exactly a scripted id list — no selection logic, no budget
interaction. It manufactures predetermined store states so downstream stages
(retrieval, reader) are evaluated decoupled from upstream stochasticity.

``PinnedPolicy`` (D6 gold-pinned) wraps any SimpleEvictionPolicy and makes a
fixed id set unevictable — the inner policy ranks only the un-pinned residents.
Gold survival is then 1.0 *by construction*, so every downstream failure is
provably not the eviction policy's.

Both are experiment fixtures, not benchmark policies: neither is registered in
the runners' default policy specs.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Mapping

from forgetit.policies.base import EvictionPolicy, SimpleEvictionPolicy
from forgetit.policies.actions import Action

if TYPE_CHECKING:
    from forgetit.memory_system import RetentionManager


class ScriptedEvictionPolicy(EvictionPolicy):
    """Evicts exactly the scripted ids, at the scripted steps, by fiat.

    ``evict_at_step`` maps a 1-based STEP NUMBER to the ids force-evicted once
    that step is reached. A "step" is whatever the driving harness says it is —
    the corpus_e fixture runner advances one step per listed event (so
    ``{4: [B]}`` with events [insert, insert, insert, step] fires on the
    ``step`` op). Drivers call :meth:`on_step` after each unit of work.

    The budget must NEVER trigger while this policy is installed (that is the
    fixture's invariant — scripts are the only evictor), so being consulted
    for a victim is a hard error, not a fallback.
    """

    name = "scripted_eviction"

    def __init__(self, evict_at_step: Mapping[int, Iterable[str]] | None = None):
        self._evict_at_step: dict[int, list[str]] = {
            int(step): list(ids) for step, ids in (evict_at_step or {}).items()
        }
        self.step_no = 0

    def on_step(self, store: "RetentionManager") -> list[str]:
        """Advance the step counter; force-evict any ids scheduled at it."""
        self.step_no += 1
        ids = self._evict_at_step.get(self.step_no, [])
        for record_id in ids:
            store.force_evict(record_id)
        return list(ids)

    def select_action(self, store: "RetentionManager") -> Action:
        raise RuntimeError(
            "scripted_eviction was consulted under budget pressure — the script "
            "is the only permitted evictor; size the budget so it never triggers."
        )


class PinnedPolicy(SimpleEvictionPolicy):
    """Wrap an inner policy; ``pinned`` ids can never be selected as victims.

    The inner policy sees ``candidates = residents − pinned``. If only pinned
    items remain under pressure this raises (the pin set no longer fits the
    budget — a mis-sized experiment, surfaced loudly rather than absorbed).
    """

    def __init__(self, inner: SimpleEvictionPolicy, pinned: Iterable[str]):
        self.inner = inner
        self.pinned = frozenset(pinned)
        self.name = f"pinned_{inner.name}"

    def select_victim(self, store: "RetentionManager",
                      candidates: set[str] | None = None) -> str:
        cands = set(store.iter_ids()) if candidates is None else set(candidates)
        cands -= self.pinned
        if not cands:
            raise RuntimeError(
                "PinnedPolicy: eviction requested but only pinned items remain — "
                "the pin set exceeds the budget's headroom."
            )
        return self.inner.select_victim(store, candidates=cands)
