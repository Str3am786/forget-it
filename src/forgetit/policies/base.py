from __future__ import annotations

from typing import TYPE_CHECKING
from abc import ABC, abstractmethod

from forgetit.policies.actions import Action, Evict

if TYPE_CHECKING:
    from forgetit.memory_system import RetentionManager


class EvictionPolicy(ABC):
    """Base for all forgetting policies"""
    name: str

    # Decision provenance: every select_victim/select_action records the score
    # it used to pick the victim here, and the store logs it into the decision
    # and evict events AT DECISION TIME. Never reconstruct scores offline —
    # stateful policies make replay a bug farm.
    last_decision: dict | None = None

    @abstractmethod
    def select_action(self, store: RetentionManager) -> Action:
        ...

    def _note_decision(self, score: float, candidate_count: int) -> None:
        """Record the chosen victim's policy score + how many items competed."""
        self.last_decision = {
            "policy_score": float(score),
            "candidate_count": int(candidate_count),
        }

class SimpleEvictionPolicy(EvictionPolicy):
    """Base for delete-only policies"""
    @abstractmethod
    def select_victim(
        self, store: RetentionManager, candidates: set[str] | None = None
    ) -> str:
        """Pick a victim id.

        If ``candidates`` is given, only those ids are considered. This lets a
        two-phase policy (e.g. compress-to-pack) ask "rank only the un-compressed
        items" without changing how the underlying ranking works.

        Implementations must call ``_note_decision(score, candidate_count)``
        with the victim's scalar score before returning (see EvictionPolicy).
        """
        ...

    def select_action(self, store: RetentionManager) -> Action:
        return Evict(id=self.select_victim(store))