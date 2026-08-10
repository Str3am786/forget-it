from __future__ import annotations

import json
import re
import datetime as dt
from pathlib import Path
from typing import Any, Iterator

from forgetit.benchmark.probe_split import (
    self_probe_index, SELF_PROBE_MAX_CHARS,
    PROBE_SCHEME_SELF, PROBE_SCHEME_LEAKY,
)
from forgetit.benchmark.schema import WorkloadEvent
from forgetit.benchmark.workloads.base import Itemiser
from forgetit.core.schema import MemRecord, Query


_DATE_RE = re.compile(
    r"^\s*(\d{4})/(\d{2})/(\d{2})\s*\([A-Za-z]{3}\)\s*(\d{2}):(\d{2})\s*$"
)


def parse_longmemeval_date(s: str) -> float:
    """
    Parse strings like:
        '2023/05/20 (Sat) 02:21'
    into a unix timestamp.

    Absolute timezone is not very important here; stable ordering is.
    """
    m = _DATE_RE.match(s)
    if not m:
        raise ValueError(f"Unrecognized LongMemEval date format: {s!r}")
    y, mo, d, hh, mm = map(int, m.groups())
    return dt.datetime(y, mo, d, hh, mm).timestamp()


def _turn_text(turn: dict[str, Any]) -> str:
    role = str(turn.get("role", "unknown"))
    content = str(turn.get("content", ""))
    return f"{role}: {content}"


class TurnPairItemiser:
    """
    Chunk one session into adjacent turn-pairs:
        turns[0:2], turns[2:4], turns[4:6], ...

    Gold heuristic:
    - any emitted chunk containing at least one turn with has_answer=True
      is considered gold for policy/retrieval evaluation.
    """

    name = "turn_pair"

    def itemise(
        self,
        session_id: str,
        date: str,
        turns: list[dict[str, Any]],
    ) -> list[MemRecord]:
        created_at = parse_longmemeval_date(date)
        items: list[MemRecord] = []

        for i in range(0, len(turns), 2):
            chunk = turns[i:i + 2]
            if not chunk:
                continue

            text = "\n".join(_turn_text(t) for t in chunk)
            contains_answer_turn = any(bool(t.get("has_answer", False)) for t in chunk)

            rec = MemRecord(
                id=f"{session_id}::pair::{i//2}",
                text=text,
                created_at=created_at,
                last_access=0.0,
                access_count=0,
                bytes=0,
                meta={
                    "dataset": "longmemeval",
                    "session_id": str(session_id),
                    "itemiser": self.name,
                    "chunk_index": i // 2,
                    "turn_start": i,
                    "turn_end": min(i + 1, len(turns) - 1),
                    "contains_answer_turn": contains_answer_turn,
                },
            )
            items.append(rec)

        return items

class SessionItemiser:
    """
    One MemRecord per session — the coarsest granularity.
    All turns are flattened into a single text blob.

    Gold resolution: if any turn in the session has has_answer=True,
    the whole session record is gold.
    """

    name = "session"

    def itemise(
        self,
        session_id: str,
        date: str,
        turns: list[dict[str, Any]],
    ) -> list[MemRecord]:
        created_at = parse_longmemeval_date(date)
        text = "\n".join(_turn_text(t) for t in turns)
        contains_answer_turn = any(bool(t.get("has_answer", False)) for t in turns)

        rec = MemRecord(
            id=str(session_id),
            text=text,
            created_at=created_at,
            last_access=0.0,
            access_count=0,
            bytes=0,
            meta={
                "dataset": "longmemeval",
                "session_id": str(session_id),
                "itemiser": self.name,
                "contains_answer_turn": contains_answer_turn,
            },
        )
        return [rec]

class LongMemEvalWorkload:
    name = "longmemeval"

    def __init__(
        self,
        path: str | Path,
        itemiser: Itemiser,
        *,
        probe_every: int = 1,
        probe_scheme: str = PROBE_SCHEME_SELF,
        clear_between_examples: bool = True,
        limit: int | None = None,
    ):
        """
        Parameters
        ----------
        limit
            If set, only the first ``limit`` examples are replayed.  Handy for
            quick smoke runs on a cluster before launching the full sweep.
        probe_every
            Emit an access-only query probe after every N session inserts
            within an example, giving the access-consuming policies a usage
            signal before later inserts trigger eviction. Set 0 to disable
            (collapses LRU/LFU toward FIFO — ablation only).
        probe_scheme
            ``resident_self_query_v1`` (default, R8-compliant): the probe
            recites the text (first SELF_PROBE_MAX_CHARS chars) of an
            already-inserted item, chosen deterministically per
            (example, insert ordinal) — generic rehearsal, blind to the
            question. ``leaky_v0`` (ARCHIVED, leak-ablation arm only):
            the probe recites the example's scored question, handing every
            access-consuming policy a soft answer label — LongMemEval has ONE
            question per example, so probe and scored question coincide
            exactly; this is the worst case sec:bm:leak measures.
        clear_between_examples
            When True, emit a ``reset`` event between examples so the store is
            wiped at each example boundary.  This isolates evaluation: gold
            items from example N cannot accidentally survive into example N+1,
            and the budget pressure that each example sees is exactly the
            pressure from its own haystack.  Set False to accumulate across
            all examples (original behaviour — useful for studying long-horizon
            memory but conflates examples during eval).
        """
        if probe_scheme not in (PROBE_SCHEME_SELF, PROBE_SCHEME_LEAKY):
            raise ValueError(
                f"LongMemEval probe_scheme must be {PROBE_SCHEME_SELF!r} or "
                f"{PROBE_SCHEME_LEAKY!r} (archived ablation arm), got {probe_scheme!r}"
            )
        if probe_scheme == PROBE_SCHEME_LEAKY:
            import logging as _logging
            _logging.getLogger(__name__).warning(
                "LongMemEval running the ARCHIVED leaky_v0 probe scheme: the "
                "probe IS the scored question (R8 violation) — ablation arm ONLY."
            )
        self.path = Path(path)
        self.itemiser = itemiser
        self.probe_every = probe_every
        self.probe_scheme = probe_scheme
        self.clear_between_examples = clear_between_examples
        self.limit = limit
        self._cache: list[dict[str, Any]] | None = None   # parsed once, shared read-only

    def _load(self) -> list[dict[str, Any]]:
        # Cache the parse so concurrent workers (thread pool) share ONE copy of
        # the dataset instead of each re-parsing ~250 MB. Iteration is read-only.
        if self._cache is None:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(data, list):
                raise ValueError("Expected LongMemEval JSON to be a list of examples.")
            self._cache = data
        data = self._cache
        if self.limit is not None:
            data = data[: self.limit]
        return data

    def _resolve_gold(
        self,
        example: dict[str, Any],
        emitted_items_by_session: dict[str, list[MemRecord]],
    ) -> list[str]:
        """
        Gold mapping for chunked LongMemEval:

        1. Look up the gold answer_session_ids.
        2. Within those sessions, keep only emitted items whose metadata says
           contains_answer_turn=True.
        3. If none match, fall back to all emitted items from the gold sessions.

        This is the key Level-B heuristic: item-level gold derived from answer-bearing turns.
        """
        gold_ids: list[str] = []

        for sid in example.get("answer_session_ids", []):
            sid_str = str(sid)
            items = emitted_items_by_session.get(sid_str, [])
            answer_items = [it.id for it in items if bool(it.meta.get("contains_answer_turn", False))]

            if answer_items:
                gold_ids.extend(answer_items)
            else:
                gold_ids.extend(it.id for it in items)

        return gold_ids

    def iter_examples(self) -> Iterator[tuple[str, list[WorkloadEvent]]]:
        """Yield ``(example_id, events)`` per LongMemEval example, WITHOUT
        inter-example ``reset`` events.

        This is the per-example unit the across-example concurrent runner
        consumes: each example runs against its own fresh store, so isolation
        comes from instantiation rather than from a ``reset``/``clear``.  The
        serial :meth:`events` wraps these exact sequences with ``reset`` events,
        so both runners replay byte-identical per-example event streams — that's
        what makes the concurrent runner policy-neutral.
        """
        for example in self._load():
            evs: list[WorkloadEvent] = []
            emitted_items_by_session: dict[str, list[MemRecord]] = {}

            session_ids = example["haystack_session_ids"]
            dates = example["haystack_dates"]
            sessions = example["haystack_sessions"]

            if "question_date_ts" in example:
                q_ts = float(example["question_date_ts"])
            elif "question_date" in example:
                q_ts = parse_longmemeval_date(example["question_date"])
            else:
                raise ValueError("No question date or question date provided.")

            qid = str(example.get("question_id", ""))
            ref_answer = str(example.get("answer", ""))
            question_type = str(example.get("question_type", ""))
            question_text = str(example["question"])

            sessions_inserted = 0
            inserted_texts: list[str] = []   # self-query probe pool (in insert order)
            for session_id, date, turns in zip(session_ids, dates, sessions):
                sid = str(session_id)
                items = self.itemiser.itemise(sid, date, turns)
                emitted_items_by_session[sid] = items

                for item in items:
                    evs.append(WorkloadEvent(
                        type="insert",
                        timestamp=item.created_at,
                        item=item,
                        query=None,
                        gold_item_ids=[],
                    ))
                    inserted_texts.append(item.text)

                sessions_inserted += 1

                # Access-only probe (gold_item_ids=[] -> runner skips eval).
                # R8 fix (resident_self_query_v1): the probe recites an
                # already-inserted item's text, chosen deterministically —
                # generic rehearsal with NO sight of the question. The archived
                # leaky_v0 scheme recites the scored question itself (LongMemEval
                # has one question per example, so probe == scored question):
                # the soft answer label the leak subsection measures.
                if self.probe_every > 0 and sessions_inserted % self.probe_every == 0:
                    if self.probe_scheme == PROBE_SCHEME_LEAKY:
                        probe_text = question_text
                        probe_meta = {
                            "dataset": "longmemeval",
                            "question_type": question_type,
                            "probe": True,
                            "probe_scheme": self.probe_scheme,
                        }
                    else:
                        src_idx = self_probe_index(
                            "longmemeval", qid,
                            sessions_inserted, len(inserted_texts),
                        )
                        probe_text = inserted_texts[src_idx][:SELF_PROBE_MAX_CHARS]
                        probe_meta = {
                            "dataset": "longmemeval",
                            "probe": True,
                            "probe_scheme": self.probe_scheme,
                            "probe_source_index": src_idx,
                        }
                    if probe_text.strip():
                        probe_query = Query(
                            id=f"{qid}::probe::{sessions_inserted}",
                            text=probe_text,
                            timestamp=q_ts,
                            embedding=None,
                            meta=probe_meta,
                        )
                        evs.append(WorkloadEvent(
                            type="query",
                            timestamp=q_ts,
                            item=None,
                            query=probe_query,
                            gold_item_ids=[],   # access-only — not scored
                        ))

            query = Query(
                id=qid,
                text=question_text,
                timestamp=q_ts,
                embedding=None,
                meta={
                    "dataset": "longmemeval",
                    "question_type": question_type,
                },
            )

            evs.append(WorkloadEvent(
                type="query",
                timestamp=q_ts,
                item=None,
                query=query,
                gold_item_ids=self._resolve_gold(example, emitted_items_by_session),
                ref_answer=ref_answer,
                question_type=question_type
            ))

            yield qid, evs

    def events(self) -> Iterator[WorkloadEvent]:
        # Serial replay: flatten the per-example streams, inserting a reset
        # between examples so the single reused store is wiped at each boundary.
        # The very first example never needs a preceding reset.
        first_example = True
        for _example_id, evs in self.iter_examples():
            if self.clear_between_examples and not first_example:
                yield WorkloadEvent(
                    type="reset",
                    timestamp=0.0,
                    item=None,
                    query=None,
                    gold_item_ids=[],
                )
            first_example = False
            yield from evs