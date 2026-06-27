from typing import Protocol, Iterator
from forgetit.benchmark.schema import WorkloadEvent
from forgetit.core import MemRecord

class Workload(Protocol):
    name: str

    def events(self) -> Iterator[WorkloadEvent]:
        """
        Yield events in chronological order.
        The runner processes them one by one — inserts go to the store,
        queries trigger retrieval and evaluation.
        """
        ...

    def iter_examples(self) -> Iterator[tuple[str, list[WorkloadEvent]]]:
        """
        Yield ``(example_id, events)`` one example at a time, WITHOUT the
        inter-example ``reset`` events that :meth:`events` interleaves.

        This is the unit of concurrency for the across-example runner: each
        example is replayed against its own fresh store, so isolation comes from
        instantiation, not from a ``reset``/``clear``.  ``events()`` is defined
        in terms of this generator, so the two replays are byte-identical
        per-example (which keeps the concurrent runner policy-neutral).
        """
        ...

class Itemiser(Protocol):
    name: str

    def itemise(
        self,
        session_id: str,
        timestamp: float,
        turns: list[dict],
    ) -> list[MemRecord]:
        """Convert one session's turns into one or more MemRecords."""
        ...

    def resolve_gold(
        self,
        gold_session_ids: list[str],
        turns_by_session: dict[str, list[dict]],
    ) -> list[str]:
        """Map dataset-level gold session IDs to MemRecord IDs."""
        ...