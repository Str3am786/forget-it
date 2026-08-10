"""
LoCoMo workload adapter (Maharana et al. 2024, snap-research/locomo).

Very long-term dyadic conversations (~27 sessions, ~588 turns, ~19K tokens per
conversation; 10 conversations in ``locomo10.json``) with QA over five reasoning
categories. Structurally BEAM's closest sibling — many QA per example, explicit
evidence→turn provenance — so this module mirrors ``beam.py``: three itemiser
granularities returning a ``(items, key_to_item)`` pair, gold resolved from
evidence ids via that map, per-example probes, and scored queries fired after
the conversation's final insert.

GRADING PROVENANCE (binding author instruction): **we never use LoCoMo's own
grading.** LoCoMo's LLM-judge prompts and evaluation scripts are NOT ported.
A community audit (grey literature, unverified) reports ~6.4% of answer keys are
wrong and that LoCoMo's judge is unreliable; therefore we grade with this repo's
own deterministic stack (:mod:`forgetit.benchmark.grading`) and export a
label-audit CSV for the author to hand-check the key-error rate before any
LoCoMo number is trusted. See :func:`grade_locomo` and ``report_locomo/``.

Category integers are mapped to the harness ability taxonomy empirically (by
evidence multiplicity + answer presence — see ``report_locomo/l0_schema.md``):

    1 → multi_hop     2 → temporal      3 → commonsense (open-domain)
    4 → single_hop    5 → adversarial   (unanswerable → abstention-graded)

The adversarial category is unanswerable: it routes through the symmetric
abstention path (``grading.is_answerable`` returns False for it; correct-when-
abstained), never scored for answer containment. See :func:`grade_locomo`.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Sequence

from forgetit.benchmark.grading import grade, is_answerable
from forgetit.benchmark.probe_split import (
    ProbePartition, write_manifest,
    PROBE_SCHEME_SPLIT, PROBE_SCHEME_LEAKY,
)
from forgetit.benchmark.schema import WorkloadEvent
from forgetit.benchmark.workloads.base import Itemiser
from forgetit.core.schema import MemRecord, Query

log = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════════
# Category taxonomy
# ════════════════════════════════════════════════════════════════════

# LoCoMo integer category → harness ability name. Confirmed empirically in L0
# (evidence multiplicity + answer presence), not assumed from the paper.
CATEGORY_NAMES: dict[int, str] = {
    1: "multi_hop",
    2: "temporal",
    3: "commonsense",     # open-domain / world knowledge
    4: "single_hop",
    5: "adversarial",     # unanswerable — abstention-graded
}
ADVERSARIAL_CATEGORY = 5


def category_name(category: Any) -> str:
    """Map a raw LoCoMo ``category`` value to its ability name.

    Unknown categories fall back to ``locomo_cat_<n>`` (logged) rather than
    crashing, so a dataset revision that adds a 6th category still runs.
    """
    try:
        c = int(category)
    except (TypeError, ValueError):
        return "locomo_cat_unknown"
    name = CATEGORY_NAMES.get(c)
    if name is None:
        log.warning("LoCoMo: unknown category %r — tagging as locomo_cat_%s", category, c)
        return f"locomo_cat_{c}"
    return name


# ════════════════════════════════════════════════════════════════════
# Parsing
# ════════════════════════════════════════════════════════════════════

_SESSION_RE = re.compile(r"^session_(\d+)$")
_SESSION_DT_RE = re.compile(r"^session_(\d+)_date_time$")
# LoCoMo timestamps look like "8:56 pm on 20 July, 2023".
_DT_FORMAT = "%I:%M %p on %d %B, %Y"
# A well-formed dia_id: D<session>:<turn>. The tolerant evidence parser below
# recovers malformed variants (joined / zero-padded / extra-colon ids).
_DIA_ID_RE = re.compile(r"^D(\d+):(\d+)$")


def parse_locomo_datetime(s: str) -> float | None:
    """Parse a LoCoMo session timestamp to a unix timestamp, or None if absent.

    Absolute timezone is irrelevant — this value is METADATA only (the FIFO key
    is the monotonic insert ordinal per the sprint-1 fix), so we just need a
    stable, comparable number. Returns None (not a crash) for an unparseable or
    empty string so a malformed date_time never sinks an example.
    """
    if not s:
        return None
    try:
        return dt.datetime.strptime(s.strip(), _DT_FORMAT).timestamp()
    except (ValueError, TypeError):
        log.warning("LoCoMo: unparseable session date_time %r", s)
        return None


def normalize_evidence_token(raw: Any) -> list[str]:
    """Normalise ONE raw evidence entry into zero or more ``D<s>:<t>`` ids.

    LoCoMo evidence is usually a clean ``"D2:3"`` string, but ~9/2815 refs are
    malformed in the source: joined ids (``"D8:6; D9:17"``, ``"D9:1 D4:4"``),
    extra-colon (``"D:11:26"``), zero-padded (``"D30:05"``). We split on
    ``;`` / whitespace, then read the first two digit-runs of each piece as
    ``(session, turn)`` and re-emit a canonical ``D<s>:<t>`` (leading zeros
    stripped). Pieces with fewer than two digit-runs (e.g. a bare ``"D"``) yield
    nothing. This mirrors BEAM's tolerant ``_normalize_source_ids`` — recover
    what is recoverable, drop the rest, and let the caller log the residue.
    Whether a recovered id actually EXISTS as a turn is checked downstream.
    """
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        out: list[str] = []
        for r in raw:
            out.extend(normalize_evidence_token(r))
        return out
    s = str(raw).strip()
    if not s:
        return []
    ids: list[str] = []
    for piece in re.split(r"[;\s]+", s):
        if not piece:
            continue
        digits = re.findall(r"\d+", piece)
        if len(digits) >= 2:
            ids.append(f"D{int(digits[0])}:{int(digits[1])}")
    return ids


@dataclass
class _LocomoTurn:
    session_num: int
    turn_idx: int            # 0-based position within the session
    dia_id: str              # "D<session>:<turn>" as given (the key evidence uses)
    speaker: str
    text: str
    caption: str | None = None      # blip_caption on image turns, else None

    @property
    def is_image(self) -> bool:
        return self.caption is not None


@dataclass
class _LocomoSession:
    session_num: int
    timestamp: float | None          # parsed session date_time (metadata)
    timestamp_str: str               # raw session date_time string
    turns: list[_LocomoTurn]


@dataclass
class _LocomoQA:
    question: str
    answer: str                      # "" for adversarial (unanswerable)
    evidence_keys: list[str]         # normalised dia_ids the answer derives from
    category: int
    category_name: str
    adversarial_answer: str = ""     # distractor (audit metadata; NEVER graded)
    raw_evidence: Any = None         # kept verbatim for the label audit

    @property
    def is_adversarial(self) -> bool:
        return self.category == ADVERSARIAL_CATEGORY


@dataclass
class _LocomoConversation:
    conversation_id: str
    speaker_a: str
    speaker_b: str
    sessions: list[_LocomoSession]
    qa: list[_LocomoQA]

    def all_turns(self) -> Iterator[_LocomoTurn]:
        for sess in self.sessions:
            yield from sess.turns


def _parse_conversation(row: dict[str, Any], *, skip_image_turns: bool) -> _LocomoConversation | None:
    conv_id = str(row.get("sample_id", ""))
    if not conv_id:
        return None

    convo = row.get("conversation")
    if not isinstance(convo, dict):
        log.warning("LoCoMo: %s has no 'conversation' dict — skipping", conv_id)
        return None

    speaker_a = str(convo.get("speaker_a", ""))
    speaker_b = str(convo.get("speaker_b", ""))

    # Collect session date_times first (some are dangling — a date_time with no
    # matching session turn list; those are simply never used).
    dt_by_num: dict[int, str] = {}
    for k, v in convo.items():
        m = _SESSION_DT_RE.match(k)
        if m:
            dt_by_num[int(m.group(1))] = str(v)

    sessions: list[_LocomoSession] = []
    for k, v in convo.items():
        m = _SESSION_RE.match(k)
        if not m or not isinstance(v, list):
            continue
        snum = int(m.group(1))
        raw_dt = dt_by_num.get(snum, "")
        turns: list[_LocomoTurn] = []
        for ti, t in enumerate(v):
            if not isinstance(t, dict):
                continue
            caption = t.get("blip_caption")
            is_image = caption is not None
            if is_image and skip_image_turns:
                continue                      # exclusion variant drops image turns
            turns.append(_LocomoTurn(
                session_num=snum,
                turn_idx=ti,
                dia_id=str(t.get("dia_id", f"D{snum}:{ti + 1}")),
                speaker=str(t.get("speaker", "")),
                text=str(t.get("text", "")),
                caption=(str(caption) if is_image else None),
            ))
        sessions.append(_LocomoSession(
            session_num=snum,
            timestamp=parse_locomo_datetime(raw_dt),
            timestamp_str=raw_dt,
            turns=turns,
        ))
    # Chronological by session number — the natural conversation order.
    sessions.sort(key=lambda s: s.session_num)

    qa_items: list[_LocomoQA] = []
    for qa in row.get("qa", []):
        if not isinstance(qa, dict):
            continue
        question = str(qa.get("question", "")).strip()
        if not question:
            continue
        cat = qa.get("category")
        cname = category_name(cat)
        try:
            cat_int = int(cat)
        except (TypeError, ValueError):
            cat_int = -1
        raw_ev = qa.get("evidence")
        ev_keys = normalize_evidence_token(raw_ev)
        adv = qa.get("adversarial_answer")
        ans = qa.get("answer")
        # Adversarial (unanswerable): never grade against an answer — ref is "".
        # (2/446 cat-5 rows carry a real "No"; the binding ruling keeps them
        #  uniform under the abstention path. See report_locomo/QUESTIONS.md Q2.)
        if cat_int == ADVERSARIAL_CATEGORY:
            ref = ""
        else:
            ref = "" if ans is None else str(ans)
        qa_items.append(_LocomoQA(
            question=question,
            answer=ref,
            evidence_keys=ev_keys,
            category=cat_int,
            category_name=cname,
            adversarial_answer="" if adv is None else str(adv),
            raw_evidence=raw_ev,
        ))

    return _LocomoConversation(
        conversation_id=conv_id,
        speaker_a=speaker_a,
        speaker_b=speaker_b,
        sessions=sessions,
        qa=qa_items,
    )


# ════════════════════════════════════════════════════════════════════
# Item text
# ════════════════════════════════════════════════════════════════════

def _turn_text(turn: _LocomoTurn) -> str:
    """Render one turn as ``speaker: text``, mirroring BEAM/LongMemEval.

    Image turns are caption-INCLUSION: the speaker's verbal message plus the
    caption appended as ``[shared image: <caption>]`` (both carry answerable
    signal; 38% of gold evidence lands on image turns). ``--skip-image-turns``
    excludes image turns entirely instead. See report_locomo/QUESTIONS.md Q5.
    """
    base = f"{turn.speaker}: {turn.text}"
    if turn.caption:
        return f"{base} [shared image: {turn.caption}]"
    return base


# ════════════════════════════════════════════════════════════════════
# Itemisers  (parity with beam.py: turn / turn_pair / session)
# ════════════════════════════════════════════════════════════════════

def _resolve_gold_via_map(
    evidence_keys: Sequence[str],
    key_to_item: dict[str, str],
) -> list[str]:
    """Map evidence dia_ids → item ids via the itemiser's key map (order-stable,
    de-duplicated). Mirrors ``BeamTurnItemiser.resolve_gold``."""
    ids: list[str] = []
    seen: set[str] = set()
    for k in evidence_keys:
        iid = key_to_item.get(k)
        if iid and iid not in seen:
            seen.add(iid)
            ids.append(iid)
    return ids


class LocomoTurnItemiser:
    """One MemRecord per turn (finest-grained)."""
    name = "turn"

    def itemise(
        self,
        conversation_id: str,
        base_ts: float,
        sessions: list[_LocomoSession],
    ) -> tuple[list[MemRecord], dict[str, str]]:
        items: list[MemRecord] = []
        key_to_item: dict[str, str] = {}
        ts = base_ts
        for sess in sessions:
            for turn in sess.turns:
                item_id = f"{conversation_id}::turn::{sess.session_num}::{turn.turn_idx}"
                rec = MemRecord(
                    id=item_id,
                    text=_turn_text(turn),
                    created_at=ts,
                    last_access=0.0,
                    access_count=0,
                    bytes=0,
                    meta={
                        "dataset": "locomo",
                        "conversation_id": conversation_id,
                        "itemiser": self.name,
                        "session_num": sess.session_num,
                        "turn_idx": turn.turn_idx,
                        "dia_id": turn.dia_id,
                        "speaker": turn.speaker,
                        "session_datetime": sess.timestamp,
                        "session_datetime_str": sess.timestamp_str,
                        **({"multimodal": "caption"} if turn.is_image else {}),
                    },
                )
                items.append(rec)
                key_to_item[turn.dia_id] = item_id
                ts += 1.0
        return items, key_to_item

    def resolve_gold(self, evidence_keys, key_to_item):
        return _resolve_gold_via_map(evidence_keys, key_to_item)


class LocomoTurnPairItemiser:
    """Adjacent turn-pairs within a session — mirrors the BEAM/LongMemEval pair
    itemisers so the budget/retrieval story is comparable. Pairs form inside a
    session only (we don't bridge sessions)."""
    name = "turn_pair"

    def itemise(
        self,
        conversation_id: str,
        base_ts: float,
        sessions: list[_LocomoSession],
    ) -> tuple[list[MemRecord], dict[str, str]]:
        items: list[MemRecord] = []
        key_to_item: dict[str, str] = {}
        ts = base_ts
        for sess in sessions:
            turns = sess.turns
            for i in range(0, len(turns), 2):
                pair = turns[i:i + 2]
                if not pair:
                    continue
                item_id = f"{conversation_id}::pair::{sess.session_num}::{i // 2}"
                text = "\n".join(_turn_text(t) for t in pair)
                any_image = any(t.is_image for t in pair)
                rec = MemRecord(
                    id=item_id,
                    text=text,
                    created_at=ts,
                    last_access=0.0,
                    access_count=0,
                    bytes=0,
                    meta={
                        "dataset": "locomo",
                        "conversation_id": conversation_id,
                        "itemiser": self.name,
                        "session_num": sess.session_num,
                        "pair_idx": i // 2,
                        "turn_start": pair[0].turn_idx,
                        "turn_end": pair[-1].turn_idx,
                        "session_datetime": sess.timestamp,
                        "session_datetime_str": sess.timestamp_str,
                        **({"multimodal": "caption"} if any_image else {}),
                    },
                )
                items.append(rec)
                for t in pair:
                    key_to_item[t.dia_id] = item_id
                ts += 1.0
        return items, key_to_item

    def resolve_gold(self, evidence_keys, key_to_item):
        return _resolve_gold_via_map(evidence_keys, key_to_item)


class LocomoSessionItemiser:
    """One MemRecord per session (coarsest) — LoCoMo's 'session' granularity,
    the analogue of LongMemEval's SessionItemiser and BEAM's batch itemiser."""
    name = "session"

    def itemise(
        self,
        conversation_id: str,
        base_ts: float,
        sessions: list[_LocomoSession],
    ) -> tuple[list[MemRecord], dict[str, str]]:
        items: list[MemRecord] = []
        key_to_item: dict[str, str] = {}
        ts = base_ts
        for sess in sessions:
            if not sess.turns:
                continue
            item_id = f"{conversation_id}::session::{sess.session_num}"
            text = "\n".join(_turn_text(t) for t in sess.turns)
            any_image = any(t.is_image for t in sess.turns)
            rec = MemRecord(
                id=item_id,
                text=text,
                created_at=ts,
                last_access=0.0,
                access_count=0,
                bytes=0,
                meta={
                    "dataset": "locomo",
                    "conversation_id": conversation_id,
                    "itemiser": self.name,
                    "session_num": sess.session_num,
                    "n_turns": len(sess.turns),
                    "session_datetime": sess.timestamp,
                    "session_datetime_str": sess.timestamp_str,
                    **({"multimodal": "caption"} if any_image else {}),
                },
            )
            items.append(rec)
            for t in sess.turns:
                key_to_item[t.dia_id] = item_id
            ts += 1.0
        return items, key_to_item

    def resolve_gold(self, evidence_keys, key_to_item):
        return _resolve_gold_via_map(evidence_keys, key_to_item)


# ════════════════════════════════════════════════════════════════════
# Grading (ours, never LoCoMo's) — category-aware typing
# ════════════════════════════════════════════════════════════════════

def grade_locomo(
    answer: str,
    ref: str,
    *,
    category: int,
    abstained: bool = False,
    aliases: tuple[str, ...] = (),
    embedder=None,
    sim_threshold: float = 0.83,
) -> dict:
    """Category-aware LoCoMo grade over the repo's deterministic stack.

    Answerable categories (single_hop / multi_hop / temporal / commonsense)
    delegate straight to :func:`forgetit.benchmark.grading.grade` (normalized
    EM + containment + token-F1 + alias/embedding tiers).

    Adversarial (category 5) is unanswerable and routes through the **symmetric
    abstention path**: correct iff the reader abstained (or returned nothing).
    A concrete answer to an adversarial question is a hallucination → incorrect,
    regardless of what it says (in particular, matching the ``adversarial_answer``
    distractor is NOT credit). This is the sprint-1 correct-when-abstained ruling.

    Returns the same ``{correct, f1, tier, similarity}`` shape as ``grade`` so it
    is a drop-in for the offline grading passes. LoCoMo's own judge is never
    invoked (see module docstring).
    """
    cname = category_name(category)
    if not is_answerable("", cname):        # adversarial → symmetric abstention
        correct = bool(abstained) or not (answer or "").strip()
        return {
            "correct": correct,
            "f1": 1.0 if correct else 0.0,
            "tier": "abstain_correct" if correct else "hallucinated",
            "similarity": float("nan"),
        }
    return grade(answer, ref, abstained=abstained, aliases=aliases,
                 embedder=embedder, sim_threshold=sim_threshold)


# ════════════════════════════════════════════════════════════════════
# Loader + workload
# ════════════════════════════════════════════════════════════════════

def _load_rows(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return data
    raise ValueError(f"Unexpected LoCoMo JSON shape at {path}: {type(data).__name__}")


class LocomoWorkload:
    """Replay LoCoMo conversations as a forget-it workload.

    Parameters
    ----------
    path
        Path to ``locomo10.json`` (a JSON list of conversations).
    itemiser
        One of :class:`LocomoTurnItemiser`, :class:`LocomoTurnPairItemiser`,
        :class:`LocomoSessionItemiser`.
    probe_every
        Emit an access-only probe after every N SESSIONS' inserts, cycling
        through this conversation's QA question texts (LongMemEval per-session
        probe convention × BEAM question-cycling). Probes bump recency/frequency
        without being scored, so LRU/LFU/Ebbinghaus get a usage signal before
        later inserts trigger eviction. Set 0 to disable.
    clear_between_examples
        When True, emit a ``reset`` between conversations so the store is wiped
        at each boundary (each conversation evaluated in isolation).
    skip_image_turns
        Exclude image turns entirely (exclusion variant). Default False =
        caption-inclusion (image turns kept, caption appended to the turn text).
    limit
        Only replay the first N conversations (smoke).
    base_timestamp
        Starting value for the monotonic ``created_at`` clock. Per the sprint-1
        fix the insert ordinal is the FIFO key; parsed session timestamps are
        metadata (``meta['session_datetime']``), not the ordering key.
    """

    name = "locomo"

    def __init__(
        self,
        path: str | Path,
        itemiser: Itemiser,
        *,
        probe_every: int = 1,
        probe_scheme: str = PROBE_SCHEME_SPLIT,
        clear_between_examples: bool = True,
        skip_image_turns: bool = False,
        limit: int | None = None,
        base_timestamp: float = 1_700_000_000.0,
    ):
        if probe_scheme not in (PROBE_SCHEME_SPLIT, PROBE_SCHEME_LEAKY):
            raise ValueError(
                f"LoCoMo probe_scheme must be {PROBE_SCHEME_SPLIT!r} or "
                f"{PROBE_SCHEME_LEAKY!r} (archived ablation arm), got {probe_scheme!r}"
            )
        if probe_scheme == PROBE_SCHEME_LEAKY:
            log.warning(
                "LoCoMo running the ARCHIVED leaky_v0 probe scheme: probes recite "
                "the scored questions (R8 violation) — leak-ablation arm ONLY."
            )
        self.path = Path(path)
        self.itemiser = itemiser
        self.probe_every = probe_every
        self.probe_scheme = probe_scheme
        self.clear_between_examples = clear_between_examples
        self.skip_image_turns = skip_image_turns
        self.limit = limit
        self._base_ts = base_timestamp
        self._conv_cache: list[_LocomoConversation] | None = None   # parsed once, shared read-only
        # conversation_id -> ProbePartition, built lazily (iter_examples) or
        # eagerly (write_partition_manifest); deterministic either way, so both
        # construction orders yield byte-identical partitions.
        self._partitions: dict[str, ProbePartition] = {}

    # ---- probe partition (R8) ----

    def _partition_for(self, conv: _LocomoConversation) -> ProbePartition:
        part = self._partitions.get(conv.conversation_id)
        if part is None:
            q_ids = [f"{conv.conversation_id}::q::{i}::{qa.category_name}"
                     for i, qa in enumerate(conv.qa)]
            part = ProbePartition.build(
                dataset="locomo",
                example_id=conv.conversation_id,
                questions=[(qid, qa.category_name)
                           for qid, qa in zip(q_ids, conv.qa)],
            )
            self._partitions[conv.conversation_id] = part
        return part

    def write_partition_manifest(self, path: str | Path) -> str:
        """Eagerly build every conversation's partition and write the manifest
        that ships with the artefact (part of the experimental definition).
        Returns the content hash — the value that MUST enter the config hash."""
        if self.probe_scheme == PROBE_SCHEME_LEAKY:
            raise ValueError("leaky_v0 has no partition manifest (ablation arm).")
        for conv in self._iter_conversations():
            self._partition_for(conv)
        return write_manifest(sorted(self._partitions.values(),
                                     key=lambda p: p.example_id),
                              path, probe_scheme=self.probe_scheme)

    # ---- public introspection ----

    def conversations(self) -> list[_LocomoConversation]:
        """Eagerly parse all conversations (inspection / sizing / tests)."""
        return list(self._iter_conversations())

    # ---- internals ----

    def _iter_conversations(self) -> Iterator[_LocomoConversation]:
        # Parse once and cache so concurrent workers share one copy (read-only).
        if self._conv_cache is None:
            convs: list[_LocomoConversation] = []
            for row in _load_rows(self.path):
                if not isinstance(row, dict):
                    continue
                conv = _parse_conversation(row, skip_image_turns=self.skip_image_turns)
                if conv is not None:
                    convs.append(conv)
            self._conv_cache = convs
        data = self._conv_cache
        if self.limit is not None:
            data = data[: self.limit]
        yield from data

    def iter_examples(self) -> Iterator[tuple[str, list[WorkloadEvent]]]:
        """Yield ``(conversation_id, events)`` per conversation, WITHOUT
        inter-conversation ``reset`` events — the per-example unit the
        across-example concurrent runner consumes.

        The monotonic ``ts`` clock carries forward across conversations exactly
        as BEAM's does, so per-example ``created_at`` values (and therefore every
        eviction decision) are byte-identical whether replayed serially or
        concurrently — the property that keeps the concurrent runner
        policy-neutral. Scored QA queries fire AFTER the conversation's final
        insert (the scored-at-end convention E3's probes-blind semantics rely on;
        LoCoMo's structure preserves it because QA are conversation-level, not
        tied to a mid-conversation point).
        """
        itemiser = self.itemiser
        ts = self._base_ts
        n_conv = n_q = n_q_gold = 0

        for conv in self._iter_conversations():
            n_conv += 1
            evs: list[WorkloadEvent] = []

            items, key_to_item = itemiser.itemise(conv.conversation_id, ts, conv.sessions)
            if items:
                ts = items[-1].created_at + 1.0

            # R8 fix (disjoint_split_v1): probes recite ONLY the probe half of
            # a deterministic, stratified partition of this conversation's
            # questions; the scored loop below emits ONLY the scored half — the
            # two sets never intersect, so the access counters carry usage
            # signal without a soft answer label. Under the archived leaky_v0
            # scheme (leak-ablation arm) probes cycle ALL questions, scored
            # ones included — the exact defect sec:bm:leak measures.
            text_by_qid = {
                f"{conv.conversation_id}::q::{i}::{qa.category_name}": qa.question
                for i, qa in enumerate(conv.qa)
            }
            if self.probe_scheme == PROBE_SCHEME_LEAKY:
                partition = None
                probe_texts = [qa.question for qa in conv.qa if qa.question]
            else:
                partition = self._partition_for(conv)
                probe_texts = [text_by_qid[qid] for qid in partition.probe_order
                               if text_by_qid[qid]]
            probe_i = 0

            # Emit inserts session-by-session; fire a probe after each session's
            # inserts (per-session cadence, LongMemEval-style). We walk the items
            # in emission order and cut on session boundaries via meta.
            sessions_seen = 0
            last_session: Any = object()
            for rec in items:
                evs.append(WorkloadEvent(
                    type="insert",
                    timestamp=rec.created_at,
                    item=rec,
                    query=None,
                    gold_item_ids=[],
                ))
                sess_key = rec.meta.get("session_num")
                if sess_key != last_session:
                    last_session = sess_key
                    sessions_seen += 1
                    if (self.probe_every > 0 and probe_texts
                            and sessions_seen % self.probe_every == 0):
                        probe_text = probe_texts[probe_i % len(probe_texts)]
                        probe_i += 1
                        evs.append(WorkloadEvent(
                            type="query",
                            timestamp=ts,
                            item=None,
                            query=Query(
                                id=f"{conv.conversation_id}::probe::{sessions_seen}",
                                text=probe_text,
                                timestamp=ts,
                                embedding=None,
                                meta={"dataset": "locomo",
                                      "conversation_id": conv.conversation_id,
                                      "probe": True,
                                      "probe_scheme": self.probe_scheme},
                            ),
                            gold_item_ids=[],     # access-only — not scored
                        ))

            # Scored QA queries — after the final insert. Probe-half questions
            # are SKIPPED (they exist only as access signal); the analysed
            # counts shrink accordingly, as documented in the methods chapter.
            for q_idx, qa in enumerate(conv.qa):
                q_id = f"{conv.conversation_id}::q::{q_idx}::{qa.category_name}"
                if partition is not None and q_id not in partition.scored_ids:
                    continue
                q_ts = ts
                ts += 1.0

                gold_item_ids = itemiser.resolve_gold(qa.evidence_keys, key_to_item)
                if not gold_item_ids and qa.evidence_keys:
                    log.warning(
                        "LoCoMo: %s q%d (%s): %d evidence keys but 0 resolved under "
                        "itemiser=%s (evidence=%r)",
                        conv.conversation_id, q_idx, qa.category_name,
                        len(qa.evidence_keys), itemiser.name, qa.evidence_keys,
                    )

                query = Query(
                    id=q_id,
                    text=qa.question,
                    timestamp=q_ts,
                    embedding=None,
                    meta={
                        "dataset": "locomo",
                        "conversation_id": conv.conversation_id,
                        "category": qa.category,
                        "category_name": qa.category_name,
                        "question_type": qa.category_name,
                        "is_adversarial": qa.is_adversarial,
                        # Distractor kept for the label audit ONLY — never graded.
                        "adversarial_answer": qa.adversarial_answer,
                        "evidence": qa.evidence_keys,
                    },
                )
                evs.append(WorkloadEvent(
                    type="query",
                    timestamp=q_ts,
                    item=None,
                    query=query,
                    gold_item_ids=gold_item_ids,
                    ref_answer=qa.answer,
                    question_type=qa.category_name,
                ))
                n_q += 1
                if gold_item_ids:
                    n_q_gold += 1

            yield conv.conversation_id, evs

        log.info(
            "LoCoMo workload done: conversations=%d queries=%d with_gold=%d "
            "unresolved=%d (itemiser=%s, skip_image_turns=%s)",
            n_conv, n_q, n_q_gold, n_q - n_q_gold, itemiser.name, self.skip_image_turns,
        )

    def events(self) -> Iterator[WorkloadEvent]:
        # Serial replay: flatten per-conversation streams, inserting a reset
        # between conversations. Shares iter_examples() so both runners see
        # identical per-example streams (policy-neutral concurrency).
        first = True
        for _conv_id, evs in self.iter_examples():
            if self.clear_between_examples and not first:
                yield WorkloadEvent(
                    type="reset", timestamp=0.0, item=None, query=None, gold_item_ids=[],
                )
            first = False
            yield from evs