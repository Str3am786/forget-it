from __future__ import annotations

from typing import Iterator

from forgetit.benchmark.schema import WorkloadEvent
from forgetit.core.schema import MemRecord, Query


class SyntheticWorkload:
    """
    Controlled synthetic workload for validating the benchmark pipeline.

    Purpose:
    - Gold items inserted early -> stresses FIFO.
    - Mid-stream unlabeled queries -> create access signal for LRU/LFU.
    - Later filler -> creates budget pressure.
    - Final labeled queries -> evaluate policy survival + retrieval quality.

    R8 CONSTRAINT (see benchmark/probe_split.py): the mid-stream access queries
    must NOT reuse the scored query strings. They target the same items by
    content overlap, phrased differently, so LRU/LFU receive a usage signal
    without receiving a copy of the answer key. Before this was fixed, access_1
    and access_2 were byte-identical to q1 and q2, which meant part of this
    fixture's ability to separate policies came from the leak rather than from
    the policies. Keep any new access query lexically distinct from every
    scored query below.

    This is not meant to prove the thesis by itself.
    It is meant to prove that the benchmark pipeline can detect known
    policy differences under controlled conditions.
    """

    name = "synthetic"

    def __init__(self) -> None:
        self._base_ts = 1_000_000.0
        self._items: list[MemRecord] = []

    def _make_item(self, item_id: str, text: str, created_at: float) -> MemRecord:
        """
        Keep bytes=0 and last_access=0.0 so RetentionManager remains the single
        source of truth for byte normalization and initial last_access handling.
        """
        item = MemRecord(
            id=item_id,
            text=text,
            created_at=created_at,
            last_access=0.0,
            access_count=0,
            bytes=0,
            meta={},
        )
        self._items.append(item)
        return item

    def _make_query(self, query_id: str, text: str, timestamp: float) -> Query:
        return Query(
            id=query_id,
            text=text,
            timestamp=timestamp,
            embedding=None,
            meta={},
        )

    def total_text_bytes_hint(self) -> int:
        """
        Rough planning helper based on raw text bytes only.
        Useful for choosing budgets before running experiments.
        """
        return sum(len(item.text.encode("utf-8")) for item in self._items)
    
    
    def max_item_text_bytes_hint(self) -> int:
        """
        Largest single item size based on raw text bytes only.
        Any valid budget must be at least this large.
        """
        if not self._items:
            return 0
        return max(len(item.text.encode("utf-8")) for item in self._items)


    def suggested_budgets(self) -> list[int]:
        """
        Heuristic budget suggestions after one materialization pass.

        Guarantees every suggested budget can fit at least the largest single item.
        Call list(workload.events()) once before using this.
        """
        total = self.total_text_bytes_hint()
        max_item = self.max_item_text_bytes_hint()

        if total == 0 or max_item == 0:
            return []

        raw = [
            int(total * 0.20),
            int(total * 0.35),
            int(total * 0.50),
            int(total * 0.70),
            int(total * 0.90),
        ]

        budgets = sorted({max(b, max_item) for b in raw})
        return budgets

    def events(self) -> Iterator[WorkloadEvent]:
        self._items.clear()
        ts = self._base_ts

        def tick(step: float = 1.0) -> float:
            nonlocal ts
            ts += step
            return ts

        # ------------------------------------------------------------------
        # Gold items inserted early on purpose.
        # ------------------------------------------------------------------
        g1 = self._make_item(
            "gold_1",
            "Alice's passport number is ZX12345 and expires in June 2030.",
            tick(),
        )
        yield WorkloadEvent(
            type="insert",
            timestamp=g1.created_at,
            item=g1,
            query=None,
            gold_item_ids=[],
        )

        g2 = self._make_item(
            "gold_2",
            "The Wi-Fi recovery code for the cabin router is pine-forest-8821.",
            tick(),
        )
        yield WorkloadEvent(
            type="insert",
            timestamp=g2.created_at,
            item=g2,
            query=None,
            gold_item_ids=[],
        )

        g3 = self._make_item(
            "gold_3",
            "Bob is allergic to peanuts and carries an epinephrine injector.",
            tick(),
        )
        yield WorkloadEvent(
            type="insert",
            timestamp=g3.created_at,
            item=g3,
            query=None,
            gold_item_ids=[],
        )

        g4 = self._make_item(
            "gold_4",
            "Project Atlas deadline was moved to 15 September 2026.",
            tick(),
        )
        yield WorkloadEvent(
            type="insert",
            timestamp=g4.created_at,
            item=g4,
            query=None,
            gold_item_ids=[],
        )

        g5 = self._make_item(
            "gold_5",
            "Nora's preferred hotel room is a quiet high-floor corner room.",
            tick(),
        )
        yield WorkloadEvent(
            type="insert",
            timestamp=g5.created_at,
            item=g5,
            query=None,
            gold_item_ids=[],
        )

        # ------------------------------------------------------------------
        # Early filler.
        # ------------------------------------------------------------------
        filler_texts_early = [
            "Shopping list: tea, soap, batteries, envelopes, printer paper.",
            "Meeting notes about team lunch options and preferred dessert places.",
            "Brainstorming ideas for a logo refresh and color palette variants.",
            "Reminder to compare train ticket prices for three travel dates next month.",
            "Draft notes on local gym opening hours and membership options.",
            "A long filler note " * 25,
            "Another long filler paragraph about unrelated errands and weekend planning. " * 20,
        ]

        for i, text in enumerate(filler_texts_early, start=1):
            f = self._make_item(f"filler_early_{i}", text, tick())
            yield WorkloadEvent(
                type="insert",
                timestamp=f.created_at,
                item=f,
                query=None,
                gold_item_ids=[],
            )

        # ------------------------------------------------------------------
        # Mid-stream access-only queries.
        # These create access signal for LRU/LFU but are not scored. Phrased to
        # overlap the target items lexically WITHOUT reusing any scored query
        # string (R8 — see the class docstring).
        #
        # Intended effect:
        # - gold_3 gets hit twice (access_1, access_3)
        # - gold_2 gets hit once  (access_2)
        # ------------------------------------------------------------------
        t = tick()
        q_access_1 = self._make_query(
            "access_1",
            "notes about the epinephrine injector and peanut sensitivity",
            t,
        )
        yield WorkloadEvent(
            type="query",
            timestamp=t,
            item=None,
            query=q_access_1,
            gold_item_ids=[],
        )

        t = tick()
        q_access_2 = self._make_query(
            "access_2",
            "pine forest code taped to the cabin Wi-Fi router",
            t,
        )
        yield WorkloadEvent(
            type="query",
            timestamp=t,
            item=None,
            query=q_access_2,
            gold_item_ids=[],
        )

        t = tick()
        q_access_3 = self._make_query(
            "access_3",
            "which injector does Bob carry for his peanut reaction",
            t,
        )
        yield WorkloadEvent(
            type="query",
            timestamp=t,
            item=None,
            query=q_access_3,
            gold_item_ids=[],
        )

        # ------------------------------------------------------------------
        # Later filler to intensify eviction pressure.
        # ------------------------------------------------------------------
        filler_texts_late = [
            "Conference packing checklist with shirts, chargers, adapters, and badge.",
            "Unrelated notes on recipe substitutions for soup and bread.",
            "Phone numbers for local repair shops and bike servicing times.",
            "Notes from a film discussion group about lighting and soundtrack choices.",
            "Verbose filler block " * 30,
            "Administrative to-do items for tax receipts, subscriptions, and invoices. " * 18,
            "Travel planning filler with multiple hotel options and train changes. " * 22,
        ]

        for i, text in enumerate(filler_texts_late, start=1):
            f = self._make_item(f"filler_late_{i}", text, tick())
            yield WorkloadEvent(
                type="insert",
                timestamp=f.created_at,
                item=f,
                query=None,
                gold_item_ids=[],
            )

        # ------------------------------------------------------------------
        # Final scored queries.
        # Every gold item gets one scored query.
        # ------------------------------------------------------------------
        t = tick()
        q1 = self._make_query("q1", "What is Bob allergic to?", t)
        yield WorkloadEvent(
            type="query",
            timestamp=t,
            item=None,
            query=q1,
            gold_item_ids=["gold_3"],
        )

        t = tick()
        q2 = self._make_query("q2", "What is the cabin router recovery code?", t)
        yield WorkloadEvent(
            type="query",
            timestamp=t,
            item=None,
            query=q2,
            gold_item_ids=["gold_2"],
        )

        t = tick()
        q3 = self._make_query("q3", "When is the Project Atlas deadline?", t)
        yield WorkloadEvent(
            type="query",
            timestamp=t,
            item=None,
            query=q3,
            gold_item_ids=["gold_4"],
        )

        t = tick()
        q4 = self._make_query("q4", "What kind of hotel room does Nora prefer?", t)
        yield WorkloadEvent(
            type="query",
            timestamp=t,
            item=None,
            query=q4,
            gold_item_ids=["gold_5"],
        )

        t = tick()
        q5 = self._make_query("q5", "What is Alice's passport number?", t)
        yield WorkloadEvent(
            type="query",
            timestamp=t,
            item=None,
            query=q5,
            gold_item_ids=["gold_1"],
        )