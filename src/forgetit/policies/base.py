from __future__ import annotations

from typing import TYPE_CHECKING
from abc import ABC, abstractmethod

from forgetit.policies.actions import Action, Evict

if TYPE_CHECKING:
    from forgetit.memory_system import RetentionManager


class EvictionPolicy(ABC):
    """Base for all forgetting policies"""
    name: str
    
    @abstractmethod
    def select_action(self, store: RetentionManager) -> Action:
        ...

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
        """
        ...

    def select_action(self, store: RetentionManager) -> Action:
        return Evict(id=self.select_victim(store))