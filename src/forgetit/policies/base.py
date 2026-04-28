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
    def select_victim(self, store: RetentionManager) -> str:
        ...
    
    def select_action(self, store: RetentionManager) -> Action:
        return Evict(id=self.select_victim(store))