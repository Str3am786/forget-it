from __future__ import annotations

from typing import Protocol, TYPE_CHECKING
from forgetit.memory_system import RetentionManager

from forgetit.policies.actions import Action, Evict

if TYPE_CHECKING:
    from forgetit.memory_system.store import RetentionManager  # or your FeatureView type


class EvictionPolicy(Protocol):
    name: str
    

    def select_action(self, store: RetentionManager) -> Action:
        ...

class SimpleEvictionPolicy:

    def select_victim(self, store: RetentionManager) -> str:
        raise NotImplementedError
    
    def select_action(self, store: RetentionManager):
        return Evict(id=self.select_victim(store))