from __future__ import annotations

from typing import Protocol

class EvictionPolicy(Protocol):
    name: str
    

    def select_victim(self, store) -> str:
        """Returns the id of the item you want to evict"""
        ...