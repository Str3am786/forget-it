from __future__ import annotations

import json
import re
import datetime as dt
from pathlib import Path
from typing import Any, Iterator

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

    def __init__(self, path: str | Path, itemiser: Itemiser):
        self.path = Path(path)
        self.itemiser = itemiser

    def _load(self) -> list[dict[str, Any]]:
        data = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError("Expected LongMemEval JSON to be a list of examples.")
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

    def events(self) -> Iterator[WorkloadEvent]:
        for example in self._load():
            emitted_items_by_session: dict[str, list[MemRecord]] = {}

            session_ids = example["haystack_session_ids"]
            dates = example["haystack_dates"]
            sessions = example["haystack_sessions"]

            for session_id, date, turns in zip(session_ids, dates, sessions):
                sid = str(session_id)
                items = self.itemiser.itemise(sid, date, turns)
                emitted_items_by_session[sid] = items

                for item in items:
                    yield WorkloadEvent(
                        type="insert",
                        timestamp=item.created_at,
                        item=item,
                        query=None,
                        gold_item_ids=[],
                    )

            if "question_date_ts" in example:
                q_ts = float(example["question_date_ts"])
            elif "question_date" in example:
                q_ts = parse_longmemeval_date(example["question_date"])
            else:
                q_ts = 0.0

            query = Query(
                id=str(example.get("question_id", "")),
                text=str(example["question"]),
                timestamp=q_ts,
                embedding=None,
                meta={
                    "dataset": "longmemeval",
                    "question_type": example.get("question_type", ""),
                },
            )

            yield WorkloadEvent(
                type="query",
                timestamp=q_ts,
                item=None,
                query=query,
                gold_item_ids=self._resolve_gold(example, emitted_items_by_session),
            )