from __future__ import annotations

import ast
import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Sequence

from forgetit.benchmark.schema import WorkloadEvent
from forgetit.benchmark.workloads.base import Itemiser
from forgetit.core.schema import MemRecord, Query

log = logging.getLogger(__name__)



MessageKey = tuple[int, int]  # (batch_idx, msg_idx_within_batch)


@dataclass
class _BeamMessage:
    batch_idx: int
    msg_idx: int
    role: str
    content: str
    msg_id: int | None = None   # explicit global message id from the data ("id" field)

    @property
    def key(self) -> MessageKey:
        return (self.batch_idx, self.msg_idx)

    @property
    def key_str(self) -> str:
        return f"b{self.batch_idx}m{self.msg_idx}"


@dataclass
class _BeamProbingQuestion:
    ability: str           # e.g. "abstention", "multi_hop_reasoning"
    question: str
    answer: str
    source_keys: list[MessageKey]     # message keys this question's answer derives from
    raw_source_ids: Any               # kept for debugging
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class _BeamConversation:
    conversation_id: str
    batches: list[list[_BeamMessage]]    # batches[i] is the list of messages in batch i
    probing_questions: list[_BeamProbingQuestion]
    seed: dict[str, Any] = field(default_factory=dict)
    chat_size: str = ""                  # "128K", "500K", "1M", "10M" if known

    def all_messages(self) -> Iterator[_BeamMessage]:
        for batch in self.batches:
            for m in batch:
                yield m




_TUPLE_LIKE = re.compile(r"^\s*\(?\s*(\d+)\s*[,_]\s*(\d+)\s*\)?\s*$")
_B_M_LIKE = re.compile(r"^\s*b(\d+)\s*[_\-]?\s*m(\d+)\s*$", re.IGNORECASE)


def _normalize_one_source_id(raw: Any, flat_index: dict[int, MessageKey]) -> MessageKey | None:
    """
    Try to turn one raw source-id value into a (batch_idx, msg_idx) key.

    Accepted shapes:
      - int                       -> looked up in ``flat_index`` (global message number)
      - (int, int) tuple/list     -> taken as-is
      - "b<i>m<j>" / "B<i>_M<j>"  -> parsed
      - "<i>,<j>" / "<i>_<j>"     -> parsed
      - "(<i>, <j>)"              -> parsed
      - dict with "batch" + "msg" (or "message_index") keys -> extracted
    Anything else returns ``None`` and the caller logs.
    """
    if raw is None:
        return None

    if isinstance(raw, (tuple, list)) and len(raw) == 2 and all(isinstance(x, int) for x in raw):
        return (int(raw[0]), int(raw[1]))

    if isinstance(raw, int):
        return flat_index.get(raw)

    if isinstance(raw, dict):
        b = raw.get("batch", raw.get("batch_idx", raw.get("batch_index")))
        m = raw.get("msg", raw.get("message", raw.get("msg_idx", raw.get("message_index"))))
        if isinstance(b, int) and isinstance(m, int):
            return (b, m)
        flat = raw.get("id", raw.get("index"))
        if isinstance(flat, int):
            return flat_index.get(flat)
        return None

    if isinstance(raw, str):
        s = raw.strip()

        if s.isdigit():
            return flat_index.get(int(s))

        m = _B_M_LIKE.match(s)
        if m:
            return (int(m.group(1)), int(m.group(2)))

        m = _TUPLE_LIKE.match(s)
        if m:
            return (int(m.group(1)), int(m.group(2)))

        try:
            v = ast.literal_eval(s)
        except (ValueError, SyntaxError):
            return None
        return _normalize_one_source_id(v, flat_index)

    return None


def _iter_source_leaves(raw: Any) -> Iterator[Any]:
    """Yield scalar source-id leaves from BEAM's varied encodings:

      - a list of ints                       -> each int
      - a dict {label: [ints], ...}          -> every int across values
      - a string repr of either              -> parsed, then flattened
      - a bare int / "30"                    -> itself
    """
    if raw is None:
        return
    if isinstance(raw, str):
        s = raw.strip()
        try:
            raw = ast.literal_eval(s)        # "[30, 31]" / "{'a':[1]}" -> real object
        except (ValueError, SyntaxError):
            yield s                          # opaque token (e.g. "b0m1"); handle downstream
            return
    if isinstance(raw, dict):
        for v in raw.values():
            yield from _iter_source_leaves(v)
    elif isinstance(raw, (list, tuple)):
        for v in raw:
            yield from _iter_source_leaves(v)
    else:
        yield raw


def _normalize_source_ids(raw: Any, flat_index: dict[int, MessageKey]) -> list[MessageKey]:
    """Normalise a probing question's source-id field into message keys.

    BEAM source ids are flat global message ids, so an int is a message id
    (resolved via flat_index), NOT a (batch, msg) pair. Non-numeric tokens fall
    back to _normalize_one_source_id (handles tuple-like / "bXmY" forms).
    """
    out: list[MessageKey] = []
    unresolved: list[Any] = []
    seen: set[MessageKey] = set()
    for leaf in _iter_source_leaves(raw):
        if isinstance(leaf, bool):
            continue
        if isinstance(leaf, int):
            key = flat_index.get(leaf)
        elif isinstance(leaf, str) and leaf.strip().isdigit():
            key = flat_index.get(int(leaf.strip()))
        else:
            key = _normalize_one_source_id(leaf, flat_index)
        if key is None:
            unresolved.append(leaf)
        elif key not in seen:
            seen.add(key)
            out.append(key)
    if unresolved:
        log.debug("BEAM: could not normalise %d source_ids: %r", len(unresolved), unresolved[:3])
    return out



_SOURCE_ID_FIELDS = (
    "source_chat_ids",   # the field BEAM-500K actually uses (values = message ids)
    "source_ids", "source_message_ids", "message_ids",
    "source_ids_list", "source_messages", "provenance",
    "turn_ids", "source_indices", "sources",
)
_ANSWER_FIELDS = ("answer", "ideal_answer", "candidate_answer", "gold_answer")


def _extract_probing_questions(
    raw_probing: Any,
    flat_index: dict[int, MessageKey],
) -> list[_BeamProbingQuestion]:
    """Parse the ``probing_questions`` field of a BEAM row into ``_BeamProbingQuestion``s."""
    if isinstance(raw_probing, str):
        try:
            probing = ast.literal_eval(raw_probing)
        except (ValueError, SyntaxError) as e:
            log.error("BEAM: could not literal_eval probing_questions: %s", e)
            return []
    else:
        probing = raw_probing

    if not isinstance(probing, dict):
        log.error("BEAM: probing_questions must be a dict of ability -> list, got %s",
                  type(probing).__name__)
        return []

    out: list[_BeamProbingQuestion] = []
    for ability, items in probing.items():
        if not isinstance(items, list):
            continue
        for q in items:
            if not isinstance(q, dict):
                continue

            question_text = str(q.get("question", "")).strip()
            if not question_text:
                continue

            answer = ""
            for f in _ANSWER_FIELDS:
                if f in q and q[f]:
                    answer = str(q[f])
                    break

            raw_src: Any = None
            for f in _SOURCE_ID_FIELDS:
                if f in q:
                    raw_src = q[f]
                    break
            source_keys = _normalize_source_ids(raw_src, flat_index)

            out.append(_BeamProbingQuestion(
                ability=str(ability),
                question=question_text,
                answer=answer,
                source_keys=source_keys,
                raw_source_ids=raw_src,
                extra={k: v for k, v in q.items()
                       if k not in ("question", *_ANSWER_FIELDS, *_SOURCE_ID_FIELDS)},
            ))
    return out


def _parse_chat_field(raw_chat: Any) -> list[list[_BeamMessage]]:
    """
    The HF viewer shows ``chat`` as a list-of-lists of message dicts.  We also
    tolerate a single flat list of messages (in case a local JSON export is
    already flattened).
    """
    if not isinstance(raw_chat, list) or not raw_chat:
        return []

    if isinstance(raw_chat[0], dict):          # flat list
        raw_chat = [raw_chat]

    batches: list[list[_BeamMessage]] = []
    for batch_idx, batch in enumerate(raw_chat):
        if not isinstance(batch, list):
            continue
        msgs: list[_BeamMessage] = []
        for msg_idx, m in enumerate(batch):
            if not isinstance(m, dict):
                continue
            role = str(m.get("role", "")).strip() or "unknown"
            content = str(m.get("content", "")).strip()
            if not content:
                continue
            try:
                mid = int(m.get("id"))
            except (TypeError, ValueError):
                mid = None
            msgs.append(_BeamMessage(batch_idx=batch_idx, msg_idx=msg_idx,
                                     role=role, content=content, msg_id=mid))
        if msgs:
            batches.append(msgs)
    return batches


def _parse_row(row: dict[str, Any]) -> _BeamConversation | None:
    conv_id = str(row.get("conversation_id", row.get("id", "")))
    if not conv_id:
        return None

    batches = _parse_chat_field(row.get("chat"))
    if not batches:
        log.warning("BEAM: conversation %s has no parseable chat — skipping", conv_id)
        return None

    # Map message-id -> (batch, msg). source_chat_ids reference the explicit
    # message "id"; fall back to positional index when an id is missing.
    flat_index: dict[int, MessageKey] = {}
    for i, m in enumerate(msg for batch in batches for msg in batch):
        flat_index[m.msg_id if m.msg_id is not None else i] = m.key

    probing = _extract_probing_questions(row.get("probing_questions"), flat_index)

    seed = row.get("conversation_seed") or {}
    if isinstance(seed, str):
        try:
            seed = ast.literal_eval(seed)
        except (ValueError, SyntaxError):
            seed = {}

    return _BeamConversation(
        conversation_id=conv_id,
        batches=batches,
        probing_questions=probing,
        seed=seed if isinstance(seed, dict) else {},
        chat_size=str(row.get("chat_size", row.get("split", ""))),
    )


# ════════════════════════════════════════════════════════════════════
# Itemisers
# ════════════════════════════════════════════════════════════════════


def _msg_text(m: _BeamMessage) -> str:
    return f"{m.role}: {m.content}"


class BeamTurnItemiser:
    """One MemRecord per message (finest-grained)."""
    name = "beam_turn"

    def itemise(
        self,
        conversation_id: str,
        base_ts: float,
        batches: list[list[_BeamMessage]],
    ) -> tuple[list[MemRecord], dict[MessageKey, str]]:
        items: list[MemRecord] = []
        key_to_item: dict[MessageKey, str] = {}
        ts = base_ts
        ts_step = 1.0

        for batch in batches:
            for m in batch:
                item_id = f"{conversation_id}::turn::{m.batch_idx}::{m.msg_idx}"
                rec = MemRecord(
                    id=item_id,
                    text=_msg_text(m),
                    created_at=ts,
                    last_access=0.0,
                    access_count=0,
                    bytes=0,
                    meta={
                        "dataset": "beam",
                        "conversation_id": conversation_id,
                        "itemiser": self.name,
                        "batch_idx": m.batch_idx,
                        "msg_idx": m.msg_idx,
                        "role": m.role,
                    },
                )
                items.append(rec)
                key_to_item[m.key] = item_id
                ts += ts_step
        return items, key_to_item

    def resolve_gold(
        self,
        source_keys: Sequence[MessageKey],
        key_to_item: dict[MessageKey, str],
    ) -> list[str]:
        ids: list[str] = []
        seen: set[str] = set()
        for k in source_keys:
            iid = key_to_item.get(k)
            if iid and iid not in seen:
                seen.add(iid)
                ids.append(iid)
        return ids


class BeamTurnPairItemiser:
    """
    Adjacent user/assistant pairs inside a batch — mirrors the LongMemEval
    ``TurnPairItemiser`` so the budget/retrieval story is comparable.

    Pairs are formed inside each batch only (we don't bridge batches).
    """
    name = "beam_turn_pair"

    def itemise(
        self,
        conversation_id: str,
        base_ts: float,
        batches: list[list[_BeamMessage]],
    ) -> tuple[list[MemRecord], dict[MessageKey, str]]:
        items: list[MemRecord] = []
        key_to_item: dict[MessageKey, str] = {}
        ts = base_ts
        ts_step = 1.0

        for batch_idx, batch in enumerate(batches):
            for i in range(0, len(batch), 2):
                pair = batch[i:i + 2]
                if not pair:
                    continue
                item_id = f"{conversation_id}::pair::{batch_idx}::{i // 2}"
                text = "\n".join(_msg_text(m) for m in pair)
                rec = MemRecord(
                    id=item_id,
                    text=text,
                    created_at=ts,
                    last_access=0.0,
                    access_count=0,
                    bytes=0,
                    meta={
                        "dataset": "beam",
                        "conversation_id": conversation_id,
                        "itemiser": self.name,
                        "batch_idx": batch_idx,
                        "pair_idx": i // 2,
                        "turn_start": pair[0].msg_idx,
                        "turn_end": pair[-1].msg_idx,
                    },
                )
                items.append(rec)
                for m in pair:
                    key_to_item[m.key] = item_id
                ts += ts_step
        return items, key_to_item

    def resolve_gold(
        self,
        source_keys: Sequence[MessageKey],
        key_to_item: dict[MessageKey, str],
    ) -> list[str]:
        ids: list[str] = []
        seen: set[str] = set()
        for k in source_keys:
            iid = key_to_item.get(k)
            if iid and iid not in seen:
                seen.add(iid)
                ids.append(iid)
        return ids


class BeamBatchItemiser:
    """
    One MemRecord per batch (one sub-plan).  This is the "session" granularity
    for BEAM — a MemRecord contains every message in a batch, flattened.
    """
    name = "beam_batch"

    def itemise(
        self,
        conversation_id: str,
        base_ts: float,
        batches: list[list[_BeamMessage]],
    ) -> tuple[list[MemRecord], dict[MessageKey, str]]:
        items: list[MemRecord] = []
        key_to_item: dict[MessageKey, str] = {}
        ts = base_ts
        ts_step = 1.0

        for batch_idx, batch in enumerate(batches):
            if not batch:
                continue
            item_id = f"{conversation_id}::batch::{batch_idx}"
            text = "\n".join(_msg_text(m) for m in batch)
            rec = MemRecord(
                id=item_id,
                text=text,
                created_at=ts,
                last_access=0.0,
                access_count=0,
                bytes=0,
                meta={
                    "dataset": "beam",
                    "conversation_id": conversation_id,
                    "itemiser": self.name,
                    "batch_idx": batch_idx,
                    "n_messages": len(batch),
                },
            )
            items.append(rec)
            for m in batch:
                key_to_item[m.key] = item_id
            ts += ts_step
        return items, key_to_item

    def resolve_gold(
        self,
        source_keys: Sequence[MessageKey],
        key_to_item: dict[MessageKey, str],
    ) -> list[str]:
        ids: list[str] = []
        seen: set[str] = set()
        for k in source_keys:
            iid = key_to_item.get(k)
            if iid and iid not in seen:
                seen.add(iid)
                ids.append(iid)
        return ids


# ════════════════════════════════════════════════════════════════════
# Loaders
# ════════════════════════════════════════════════════════════════════


def _load_rows(path: Path) -> list[dict[str, Any]]:
    """
    Load BEAM rows from JSON, JSONL, or parquet.

    Parquet requires pandas/pyarrow and is only imported lazily so the
    benchmark still runs in a minimal environment.
    """
    suffix = path.suffix.lower()

    if suffix == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return [data]     # single conversation
        if isinstance(data, list):
            return data
        raise ValueError(f"Unexpected JSON shape at {path}: {type(data).__name__}")

    if suffix == ".jsonl":
        rows: list[dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                rows.append(json.loads(line))
        return rows

    if suffix == ".parquet":
        try:
            import pandas as pd  # noqa: PLC0415
        except ImportError as e:
            raise ImportError(
                "Reading BEAM from parquet requires pandas + pyarrow. "
                "Install with: pip install pandas pyarrow"
            ) from e
        df = pd.read_parquet(path)
        return df.to_dict(orient="records")

    raise ValueError(f"Unsupported BEAM file extension: {suffix}")


# ════════════════════════════════════════════════════════════════════
# Workload
# ════════════════════════════════════════════════════════════════════


class BeamWorkload:
    """
    Replay BEAM conversations as a forget-it workload.
    Parameters
    ----------
    path
        Path to a BEAM file (``.json`` / ``.jsonl`` / ``.parquet``) or a
        directory containing multiple such files (concatenated in sorted
        filename order).
    itemiser
        One of :class:`BeamTurnItemiser`, :class:`BeamTurnPairItemiser`,
        :class:`BeamBatchItemiser`.  Controls MemRecord granularity.
    conversation_ids
        Optional allow-list.  When given, only conversations whose
        ``conversation_id`` is in this iterable are replayed.  Handy for
        per-conversation benchmarking — see :func:`iter_single_conversation`.
    base_timestamp
        Starting timestamp for synthetic message ordering.  Each message gets
        ``base_timestamp + i`` so recency/LRU policies have a stable signal.
    """

    name = "beam"

    def __init__(
        self,
        path: str | Path,
        itemiser: Itemiser,
        *,
        conversation_ids: Sequence[str] | None = None,
        base_timestamp: float = 1_700_000_000.0,
        probe_every: int = 1,
        clear_between_examples: bool = True,
        limit: int | None = None,
    ):
        """
        Parameters
        ----------
        probe_every
            Emit an access-only query probe after every N item inserts within a
            conversation, cycling through that conversation's probing questions.
            The probe retrieves (bumping recency/frequency) but is not scored, so
            LRU/LFU/Ebbinghaus get a usage signal *before* later inserts trigger
            eviction (otherwise they collapse into FIFO).  Set 0 to disable.
        clear_between_examples
            When True, emit a ``reset`` event between conversations so the store is
            wiped at each boundary — each conversation is evaluated in isolation.
        limit
            If set, only the first ``limit`` conversations are replayed.
        """
        self.path = Path(path)
        self.itemiser = itemiser
        self._allow: set[str] | None = (
            {str(c) for c in conversation_ids} if conversation_ids is not None else None
        )
        self._base_ts = base_timestamp
        self.probe_every = probe_every
        self.clear_between_examples = clear_between_examples
        self.limit = limit


    # ---- public introspection ----

    def conversations(self) -> list[_BeamConversation]:
        """Eagerly load and parse all BEAM conversations (for inspection / sizing)."""
        return list(self._iter_conversations())



    # ---- internals ----

    def _iter_files(self) -> Iterator[Path]:
        if self.path.is_dir():
            for p in sorted(self.path.iterdir()):
                if p.suffix.lower() in (".json", ".jsonl", ".parquet"):
                    yield p
        else:
            yield self.path

    def _iter_conversations(self) -> Iterator[_BeamConversation]:
        for fp in self._iter_files():
            for row in _load_rows(fp):
                if not isinstance(row, dict):
                    continue
                conv = _parse_row(row)
                if conv is None:
                    continue
                if self._allow is not None and conv.conversation_id not in self._allow:
                    continue
                yield conv

    def events(self) -> Iterator[WorkloadEvent]:
        itemiser = self.itemiser
        n_conv = 0
        n_queries_emitted = 0
        n_queries_with_gold = 0
        ts = self._base_ts
        first_conv = True

        for conv in self._iter_conversations():
            if self.limit is not None and n_conv >= self.limit:
                break
            n_conv += 1

            # Reset between conversations so each is evaluated in isolation.
            if self.clear_between_examples and not first_conv:
                yield WorkloadEvent(
                    type="reset", timestamp=0.0, item=None, query=None, gold_item_ids=[],
                )
            first_conv = False

            items, key_to_item = itemiser.itemise(conv.conversation_id, ts, conv.batches)
            if items:
                ts = items[-1].created_at + 1.0

            # Access-only probes cycle through this conversation's questions so
            # the items they retrieve gain recency/frequency before eviction.
            probe_texts = [pq.question for pq in conv.probing_questions if pq.question]
            probe_i = 0

            for n, rec in enumerate(items, start=1):
                yield WorkloadEvent(
                    type="insert",
                    timestamp=rec.created_at,
                    item=rec,
                    query=None,
                    gold_item_ids=[],
                )
                if self.probe_every > 0 and probe_texts and n % self.probe_every == 0:
                    probe_text = probe_texts[probe_i % len(probe_texts)]
                    probe_i += 1
                    yield WorkloadEvent(
                        type="query",
                        timestamp=ts,
                        item=None,
                        query=Query(
                            id=f"{conv.conversation_id}::probe::{n}",
                            text=probe_text,
                            timestamp=ts,
                            embedding=None,
                            meta={"dataset": "beam",
                                  "conversation_id": conv.conversation_id,
                                  "probe": True},
                        ),
                        gold_item_ids=[],   # access-only — not scored
                    )

            for q_idx, pq in enumerate(conv.probing_questions):
                q_id = f"{conv.conversation_id}::q::{q_idx}::{pq.ability}"
                q_ts = ts
                ts += 1.0

                gold_item_ids = itemiser.resolve_gold(pq.source_keys, key_to_item)
                if not gold_item_ids and pq.source_keys:
                    log.warning(
                        "BEAM: conv %s q%d (%s): %d source_keys but 0 resolved under itemiser=%s",
                        conv.conversation_id, q_idx, pq.ability,
                        len(pq.source_keys), itemiser.name,
                    )
                if not gold_item_ids and not pq.source_keys and pq.raw_source_ids is not None:
                    log.debug(
                        "BEAM: conv %s q%d (%s): raw source_ids present but unparseable; sample=%r",
                        conv.conversation_id, q_idx, pq.ability,
                        pq.raw_source_ids if not isinstance(pq.raw_source_ids, list)
                        else pq.raw_source_ids[:2],
                    )

                query = Query(
                    id=q_id,
                    text=pq.question,
                    timestamp=q_ts,
                    embedding=None,
                    meta={
                        "dataset": "beam",
                        "conversation_id": conv.conversation_id,
                        "chat_size": conv.chat_size,
                        "memory_ability": pq.ability,
                        "question_type": pq.ability,
                    },
                )

                yield WorkloadEvent(
                    type="query",
                    timestamp=q_ts,
                    item=None,
                    query=query,
                    gold_item_ids=gold_item_ids,
                    ref_answer=pq.answer,
                    question_type=pq.ability,
                )
                n_queries_emitted += 1
                if gold_item_ids:
                    n_queries_with_gold += 1

        log.info(
            "BEAM workload done: conversations=%d queries=%d with_gold=%d unresolved=%d (itemiser=%s)",
            n_conv, n_queries_emitted, n_queries_with_gold,
            n_queries_emitted - n_queries_with_gold, itemiser.name,
        )

def iter_single_conversation(
    path: str | Path,
    itemiser: Itemiser,
    *,
    base_timestamp: float = 1_700_000_000.0,
) -> Iterator[BeamWorkload]:
    """
    Yield one single-conversation ``BeamWorkload`` at a time.
    """
    probe = BeamWorkload(path, itemiser=itemiser, base_timestamp=base_timestamp)
    conv_ids = [c.conversation_id for c in probe._iter_conversations()]
    for cid in conv_ids:
        yield BeamWorkload(
            path,
            itemiser=itemiser,
            conversation_ids=[cid],
            base_timestamp=base_timestamp,
        )