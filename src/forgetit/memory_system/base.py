from __future__ import annotations

from typing import Protocol, runtime_checkable

from forgetit.core import Query, MemRecord


@runtime_checkable
class MemorySystem(Protocol):
    """
    Unified interface for agentic memory systems.

    Any memory backend (custom, Mem0, MemGPT) must implement these methods.
    """

    def insert(self, item: MemRecord) -> None:
        """Insert a new memory record into the system."""
        ...

    def retrieve(self, query: Query, k: int | None = None) -> list[MemRecord]:
        """
        Retrieve memories relevant to the given query.

        ``k`` is a hint: systems that honour a caller-specified top-k (e.g.
        RetentionManager) use it; systems that manage their own retrieval
        budget (Letta, Mem0) may ignore it.  Passing ``None`` means "use the
        system's default."
        """
        ...

    def get_used_bytes(self) -> int:
        """Return the current storage footprint in bytes."""
        ...

    def clear(self) -> None:
        """Wipe the memory system (used between benchmark runs)."""
        ...