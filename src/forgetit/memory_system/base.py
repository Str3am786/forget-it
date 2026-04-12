from typing import Protocol, List
from forgetit.core import Query, MemRecord

class MemorySystem(Protocol):
    """
    Unified interface for agentic memory systems.
    Any memory backend (custom, Mem0, MemGPT) must implement these methods.
    """

    def insert(self, item: MemRecord) -> None:
        """Inserts a new memory record into the system."""
        ...

    def retrieve(self, query: Query) -> List[MemRecord]: # or list[MemRecord] in Python 3.9+
        """Retrieves memories relevant to the given query."""
        ...

    def get_used_bytes(self) -> int:
        """Returns the current storage footprint in bytes."""
        ...

    def clear(self) -> None:
        """Wipes the memory system (useful between benchmark runs)."""
        ...