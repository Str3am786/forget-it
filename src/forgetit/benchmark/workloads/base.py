from typing import Protocol, Iterator
from forgetit.benchmark.schema import WorkloadEvent

class Workload(Protocol):
    name: str

    def events(self) -> Iterator[WorkloadEvent]:
        """
        Yield events in chronological order.
        The runner processes them one by one — inserts go to the store,
        queries trigger retrieval and evaluation.
        """
        ...

