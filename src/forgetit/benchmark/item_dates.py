"""Item-id -> raw session-date lookup, read from the raw data files only.

The itemisers deliberately keep dates out of ``MemRecord.text``: bytes, budgets
and every eviction decision stay identical whether or not anything downstream
renders a date. The reader's evidence renderer therefore looks dates up HERE,
at render time, keyed by the item ids the itemisers mint. Nothing in this module
reads a store, and nothing it returns is reformatted -- LoCoMo's
``'8:56 pm on 20 July, 2023'``, LongMemEval's ``'2023/05/20 (Sat) 02:21'`` and
BEAM's ``'January-05-2024'`` reach the prompt verbatim.

Id grammars are mirrored from the itemiser code, which is authoritative:

    LongMemEval  ``<sid>::pair::<n>`` (TurnPairItemiser) and bare ``<sid>``
                 (SessionItemiser). The date is a session property, so only the
                 leading sid matters.
    LoCoMo       ``<conv>::{turn|pair|session}::<session_num>[::<i>]``, keyed
                 ``(sample_id, session_num)``.
    BEAM         ``<conv>::pair::<batch>::<i//2>``, ``<conv>::turn::<b>::<m>``,
                 ``<conv>::batch::<b>``. Anchors are sparse per-message
                 ``time_anchor`` fields the parser drops, so this map is keyed
                 by the FULL item id, built by replaying the parse. Pair and
                 batch ids index the FILTERED batch list (their itemisers
                 re-enumerate it); turn ids keep the raw per-message indices.
                 Both are mirrored below -- they diverge whenever a batch parses
                 empty.
"""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

# Imported, not reimplemented: these define the exact parse the itemisers see,
# so the date map cannot drift from the id grammar.
from forgetit.benchmark.workloads.beam import _parse_chat_field
from forgetit.benchmark.workloads.locomo import _SESSION_DT_RE

log = logging.getLogger(__name__)

WORKLOADS = ("locomo", "longmemeval", "beam")

_DATA_FILES = {
    "locomo": "locomo10.json",
    "longmemeval": "longmemeval_s_cleaned.json",
    "beam": "beam_500K.jsonl",
}

_SESSION_RE = re.compile(r"^session_(\d+)$")


# -- per-dataset extractors -------------------------------------------------

def longmemeval_dates(path: str | Path) -> dict[str, str]:
    """``session_id -> haystack_dates entry``, GLOBALLY -- strict.

    Raises ``ValueError`` when one sid carries two different dates. That is not
    hypothetical: on ``longmemeval_s_cleaned.json`` **20.5% of sids (3,942 of
    19,195, 8 of them gold) are ambiguous**, because haystacks reuse session ids
    across examples with different dates. So this function is a CHECKER, not the
    lookup path -- :func:`longmemeval_dates_by_example` is what ``DateMap`` uses.
    Keeping the strict version means the ambiguity can never quietly reappear.
    """
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    out: dict[str, str] = {}
    for ex in data:
        if not isinstance(ex, dict):
            continue
        for sid, date in zip(ex.get("haystack_session_ids", []),
                             ex.get("haystack_dates", [])):
            sid, date = str(sid), str(date)
            if not date.strip():
                continue
            prev = out.get(sid)
            if prev is not None and prev != date:
                raise ValueError(
                    f"LongMemEval session {sid!r} appears with two different "
                    f"dates: {prev!r} and {date!r}")
            out[sid] = date
    return out


def longmemeval_dates_by_example(
    path: str | Path,
) -> tuple[dict[tuple[str, str], str], dict[str, str]]:
    """``((question_id, session_id) -> date, unambiguous session_id -> date)``.

    Each example replays against its own store, so ``(question_id, sid)`` is the
    scope in which a session's date is actually well defined. The second map
    holds only sids whose date is identical everywhere they appear -- the safe
    fallback when a caller has no example in hand. A sid that repeats WITHIN one
    example with conflicting dates (13 examples repeat a sid) is dropped from the
    scoped map too: a wrong date rendered as fact is worse than no date.
    """
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    scoped: dict[tuple[str, str], str] = {}
    conflicted: set[tuple[str, str]] = set()
    seen: dict[str, set[str]] = {}
    for ex in data:
        if not isinstance(ex, dict):
            continue
        qid = str(ex.get("question_id", ""))
        for sid, date in zip(ex.get("haystack_session_ids", []),
                             ex.get("haystack_dates", [])):
            sid, date = str(sid), str(date)
            if not date.strip():
                continue
            seen.setdefault(sid, set()).add(date)
            key = (qid, sid)
            prev = scoped.get(key)
            if prev is not None and prev != date:
                conflicted.add(key)
            else:
                scoped[key] = date
    for key in conflicted:
        scoped.pop(key, None)
    if conflicted:
        log.warning("LongMemEval: %d (question, session) pairs carry conflicting "
                    "dates within one example; those items render undated",
                    len(conflicted))
    unambiguous = {sid: next(iter(ds)) for sid, ds in seen.items() if len(ds) == 1}
    return scoped, unambiguous


def longmemeval_question_dates(path: str | Path) -> dict[str, str]:
    """``question_id -> question_date``: the question side of the render fix."""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return {str(ex["question_id"]): str(ex["question_date"])
            for ex in data
            if isinstance(ex, dict) and ex.get("question_id")
            and str(ex.get("question_date", "")).strip()}


def locomo_dates(path: str | Path) -> dict[tuple[str, int], str]:
    """``(sample_id, session_num) -> session_N_date_time`` raw string.

    Empty strings are skipped, so a missing date is an absent key rather than
    an empty one, and a dangling date_time (no matching turn list) is harmless.
    """
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = data if isinstance(data, list) else [data]
    out: dict[tuple[str, int], str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        sample_id = str(row.get("sample_id", ""))
        conv = row.get("conversation")
        if not sample_id or not isinstance(conv, dict):
            continue
        for k, v in conv.items():
            m = _SESSION_DT_RE.match(k)
            if m and str(v).strip():
                out[(sample_id, int(m.group(1)))] = str(v)
    return out


def beam_dates(path: str | Path) -> dict[str, str]:
    """``item_id -> time_anchor``, every BEAM granularity, in one walk.

    The last non-empty ``time_anchor`` is carried forward within a conversation;
    messages before the first anchor get no entry. A pair (or batch) spanning
    two anchors takes its FIRST message's date.
    """
    out: dict[str, str] = {}
    with Path(path).open(encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as e:
                log.warning("beam_dates: skipping malformed line %d: %s", lineno, e)
                continue
            conv_id = str(row.get("conversation_id", row.get("id", "")))
            raw_chat = row.get("chat")
            batches = _parse_chat_field(raw_chat)
            if not conv_id or not batches:
                continue

            # Anchors by RAW position. _parse_chat_field wraps a flat message
            # list into a single batch; mirror that so the indices line up with
            # the (batch_idx, msg_idx) each parsed message carries.
            raw_batches: list[Any] = raw_chat
            if isinstance(raw_chat[0], dict):
                raw_batches = [raw_chat]
            anchors: dict[tuple[int, int], str] = {}
            for bi, batch in enumerate(raw_batches):
                if not isinstance(batch, list):
                    continue
                for mi, m in enumerate(batch):
                    if isinstance(m, dict):
                        ta = str(m.get("time_anchor") or "").strip()
                        if ta:
                            anchors[(bi, mi)] = ta

            # Carry forward over the PARSED messages in conversation order. A
            # message's date is the carry AFTER its own anchor applies.
            current: str | None = None
            date_by_key: dict[tuple[int, int], str] = {}
            for batch in batches:
                for m in batch:
                    ta = anchors.get(m.key)
                    if ta:
                        current = ta
                    if current is not None:
                        date_by_key[m.key] = current

            # Key every granularity exactly as its itemiser does: pair/batch ids
            # enumerate the filtered list, turn ids use the raw indices.
            for batch_idx, batch in enumerate(batches):
                d = date_by_key.get(batch[0].key)
                if d:
                    out[f"{conv_id}::batch::{batch_idx}"] = d
                for m in batch:
                    dm = date_by_key.get(m.key)
                    if dm:
                        out[f"{conv_id}::turn::{m.batch_idx}::{m.msg_idx}"] = dm
                for i in range(0, len(batch), 2):
                    dp = date_by_key.get(batch[i].key)
                    if dp:
                        out[f"{conv_id}::pair::{batch_idx}::{i // 2}"] = dp
    return out


# -- the lookup object ------------------------------------------------------

class DateMap:
    """One workload's item-id -> raw-date lookup.

    ``lookup`` never raises on a malformed id: an id the grammar does not cover
    simply has no date, and the renderer falls back to the undated prefix.

    LongMemEval needs an ``example_id`` (the question id) to be exact, because
    its session ids are only unique within an example -- see
    :func:`longmemeval_dates_by_example`. Without one, only sids that carry the
    same date everywhere resolve; an ambiguous sid returns None rather than a
    guess. LoCoMo and BEAM ids are globally scoped by their conversation id, so
    ``example_id`` is ignored there.
    """

    def __init__(self, workload: str, mapping: dict,
                 scoped: dict | None = None) -> None:
        self.workload = workload
        self._map = mapping
        self._scoped = scoped or {}

    @classmethod
    def for_workload(cls, name: str, data_dir: str | Path) -> "DateMap":
        """``name`` is the dataset name as the runners record it: ``locomo`` /
        ``longmemeval`` / ``beam``. Substrings match, so a ``dataset`` column
        carrying e.g. ``longmemeval_turn_pair`` resolves too."""
        key = cls.resolve_name(name)
        if key is None:
            raise ValueError(
                f"unknown workload {name!r}; expected one of {list(WORKLOADS)}")
        path = Path(data_dir) / _DATA_FILES[key]
        if key == "locomo":
            return cls(key, locomo_dates(path))
        if key == "longmemeval":
            scoped, unambiguous = longmemeval_dates_by_example(path)
            return cls(key, unambiguous, scoped)
        return cls(key, beam_dates(path))

    @staticmethod
    def resolve_name(name: str) -> str | None:
        """Dataset label -> canonical workload name, or None if it is neither."""
        s = str(name or "").strip().lower()
        for w in WORKLOADS:
            if w in s:
                return w
        return None

    def lookup(self, item_id: str, example_id: str | None = None) -> str | None:
        if not item_id:
            return None
        if self.workload == "beam":
            return self._map.get(item_id)
        if self.workload == "longmemeval":
            sid = item_id.split("::", 1)[0]
            if example_id:
                hit = self._scoped.get((str(example_id), sid))
                if hit is not None:
                    return hit
            # No example in hand (or the pair conflicted): only a sid that means
            # the same date everywhere may be rendered.
            return self._map.get(sid)
        parts = item_id.split("::")          # locomo
        if len(parts) < 3:
            return None
        try:
            return self._map.get((parts[0], int(parts[2])))
        except (TypeError, ValueError):
            return None

    def __len__(self) -> int:
        return len(self._map)


# -- coverage report --------------------------------------------------------

def _main() -> int:
    """Per workload: how many ids carry a date, and how many raw items do not."""
    repo = Path(__file__).resolve().parents[3]
    data_dir = repo / "data"

    lme_path = data_dir / _DATA_FILES["longmemeval"]
    scoped, unambiguous = longmemeval_dates_by_example(lme_path)
    lme_map = DateMap("longmemeval", unambiguous, scoped)
    # Count what the RENDERER will actually resolve: one pair item per session,
    # looked up example-scoped exactly as reader_probe does.
    n_items = n_dated = n_unscoped_ok = 0
    all_sids: set[str] = set()
    undated: list[str] = []
    for ex in json.loads(lme_path.read_text(encoding="utf-8")):
        qid = str(ex.get("question_id", ""))
        for sid in ex.get("haystack_session_ids", []):
            sid = str(sid)
            all_sids.add(sid)
            n_items += 1
            item_id = f"{sid}::pair::0"
            if lme_map.lookup(item_id, qid) is not None:
                n_dated += 1
            elif len(undated) < 3:
                undated.append(item_id)
            if lme_map.lookup(item_id) is not None:
                n_unscoped_ok += 1
    ambiguous = len(all_sids) - len(unambiguous)
    print(f"longmemeval  {n_dated:>8,} of {n_items:,} session items dated "
          f"(example-scoped);  {n_items - n_dated} undated"
          + (f"  e.g. {', '.join(undated)}" if undated else ""))
    print(f"             {ambiguous:>8,} of {len(all_sids):,} distinct sids are "
          f"AMBIGUOUS (same sid, different dates across examples) -- "
          f"{n_unscoped_ok:,}/{n_items:,} would resolve without an example id")
    print(f"             {len(longmemeval_question_dates(lme_path)):>8,} "
          f"question dates")

    loco_path = data_dir / _DATA_FILES["locomo"]
    loco_map = locomo_dates(loco_path)
    loco_rows = json.loads(loco_path.read_text(encoding="utf-8"))
    sessions: set[tuple[str, int]] = set()
    loco_undated: set[tuple[str, int]] = set()
    for row in (loco_rows if isinstance(loco_rows, list) else [loco_rows]):
        sid = str(row.get("sample_id", ""))
        for k in (row.get("conversation") or {}):
            m = _SESSION_RE.match(k)
            if m:
                key = (sid, int(m.group(1)))
                sessions.add(key)
                if key not in loco_map:
                    loco_undated.add(key)
    print(f"locomo       {len(loco_map):>8,} session dates;  "
          f"{len(loco_undated)} of {len(sessions)} turn-bearing sessions without one"
          + (f"  e.g. {sorted(loco_undated)[:3]}" if loco_undated else ""))

    beam_path = data_dir / _DATA_FILES["beam"]
    beam_map = beam_dates(beam_path)
    totals = {"pair": 0, "turn": 0, "batch": 0}
    missing: dict[str, list[str]] = {"pair": [], "turn": [], "batch": []}
    with beam_path.open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            conv_id = str(row.get("conversation_id", row.get("id", "")))
            batches = _parse_chat_field(row.get("chat"))
            if not conv_id or not batches:
                continue
            for batch_idx, batch in enumerate(batches):
                totals["batch"] += 1
                bid = f"{conv_id}::batch::{batch_idx}"
                if bid not in beam_map:
                    missing["batch"].append(bid)
                for m in batch:
                    totals["turn"] += 1
                    tid = f"{conv_id}::turn::{m.batch_idx}::{m.msg_idx}"
                    if tid not in beam_map:
                        missing["turn"].append(tid)
                for i in range(0, len(batch), 2):
                    totals["pair"] += 1
                    pid = f"{conv_id}::pair::{batch_idx}::{i // 2}"
                    if pid not in beam_map:
                        missing["pair"].append(pid)
    for gran in ("turn", "pair", "batch"):
        miss = missing[gran]
        print(f"beam {gran:<6}  {totals[gran] - len(miss):>8,} of {totals[gran]:,} "
              f"ids dated;  {len(miss)} before the first anchor"
              + (f"  e.g. {', '.join(miss[:3])}" if miss else ""))
    print(f"\ntotal beam keys in map: {len(beam_map):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
