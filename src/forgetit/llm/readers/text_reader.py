from __future__ import annotations

import re
import time
from typing import TYPE_CHECKING

from forgetit.core.schema import AnswerResult, MemRecord, Question

if TYPE_CHECKING:
    from forgetit.llm.base import LLM


# ── prompt builder ──────────────────────────────────────────────────

# Explicit truncation marker for the budgeted path — the model (and anyone
# reading the prompt offline) can tell a cut item from a complete one.
TRUNCATION_MARKER = " …[truncated]"

# chars-per-token approximation used when no tokenizer for the served model is
# available (per E6c spec: len(text) // 4 — do not add a tokenizer dependency).
_CHARS_PER_TOKEN = 4

# Budgeted-mode default: ~matches the legacy prompt's worst-case evidence size
# (5 items × 300 chars ≈ 1500 chars ≈ 375 tokens), so switching modes changes
# HOW the window is filled, not how big it is.
DEFAULT_READER_TOKEN_BUDGET = 375


def _wrap_prompt(question: Question, evidence: str) -> str:
    """v1 (legacy, DEFAULT — pinned byte-for-byte by tests): forbids derivation
    ("ONLY", "not in the memories") and forbids list answers ("one sentence
    maximum"). Both are measured reader losses (probe findings v/vi); kept as
    the default so historical runs reproduce exactly."""
    return (
        f"Answer the question using ONLY the memories below.\n"
        f"If the answer is not in the memories, say exactly: IDK\n"
        f"Be brief — one sentence maximum.\n"
        f"\n"
        f"Memories:\n"
        f"{evidence}\n"
        f"\n"
        f"Question: {question.text}\n"
        f"Answer:"
    )


def _wrap_prompt_v2(question: Question, evidence: str) -> str:
    """v2 (D4 fix iv, flagged): permits combining/computing from the memories
    (the date-subtraction false-refusal class) and drops the one-sentence cap
    (the enumeration mutilation class), while KEEPING the exact-form IDK
    instruction so abstention detection stays sharp. Adopted only on probe-
    bench evidence (false refusals down on visible rows, no hallucination rise
    on absent rows) — see report/04_reader_fixes.md."""
    return (
        f"Answer the question using ONLY the memories below.\n"
        f"You may combine facts from several memories and compute derived "
        f"answers from them (date differences, counts, ordered lists).\n"
        f"If the information needed is absent from the memories, say exactly: IDK\n"
        f"Be concise.\n"
        f"\n"
        f"Memories:\n"
        f"{evidence}\n"
        f"\n"
        f"Question: {question.text}\n"
        f"Answer:"
    )


PROMPT_BUILDERS = {"v1": _wrap_prompt, "v2": _wrap_prompt_v2}

# BEAM abilities whose reference answers are multi-item lists — the types the
# 128-token cap demonstrably cuts mid-list (probe finding: 10/32 answered
# generations cut mid-sentence, worst on these). The per-type generation-cap
# override (D4 fix iii, --answer-max-tokens-list) applies to exactly this set.
LIST_ANSWER_TYPES = frozenset({"event_ordering", "summarization"})


def _build_prompt(question: Question, records: list[MemRecord], *, wrap=_wrap_prompt) -> str:
    """Legacy prompt: hard-truncate every memory to ~300 chars.

    Kept selectable (token_budget=None) so historical runs stay reproducible.
    Known artifact: mutilates long items (e.g. compression summaries) — the
    budgeted builder below is the documented choice for new runs (E6c).
    ``wrap`` selects the instruction wrapper (v1 default; see PROMPT_BUILDERS).
    """
    # Number the memories — models handle numbered lists well
    evidence_lines: list[str] = []
    for i, r in enumerate(records, 1):
        # Truncate each memory to ~300 chars to keep prompt short
        text = r.text[:300].rstrip()
        if len(r.text) > 300:
            text += "…"
        evidence_lines.append(f"[{i}] {text}")

    return wrap(question, "\n".join(evidence_lines))


def _build_evidence_budgeted(records: list[MemRecord], token_budget: int) -> tuple[str, bool]:
    """Fill evidence in retrieval-rank order under ONE total token budget.

    Whole items preferred — no fixed per-item cap, so a long item (compression
    summary) is not silently mutilated. When the next item doesn't fit, its
    head is included with an explicit truncation marker, then filling stops.
    The same total budget applies to every policy/config: parity is the point —
    compression's "more, shorter items fit" advantage becomes expressible.

    Returns (evidence_block, truncated_any).
    """
    remaining = token_budget * _CHARS_PER_TOKEN
    lines: list[str] = []
    truncated = False
    for i, r in enumerate(records, 1):
        prefix = f"[{i}] "
        text = r.text.rstrip()
        sep = 1 if lines else 0                    # joining newline
        cost = sep + len(prefix) + len(text)
        if cost <= remaining:
            lines.append(prefix + text)
            remaining -= cost
            continue
        # Doesn't fit whole: emit the head that fits + marker, then stop.
        head = remaining - sep - len(prefix) - len(TRUNCATION_MARKER)
        if head > 0:
            lines.append(prefix + text[:head].rstrip() + TRUNCATION_MARKER)
            truncated = True
        break
    return "\n".join(lines), truncated


def _build_prompt_budgeted(question: Question, records: list[MemRecord],
                           token_budget: int, *, wrap=_wrap_prompt) -> tuple[str, bool]:
    evidence, truncated = _build_evidence_budgeted(records, token_budget)
    return wrap(question, evidence), truncated


# ── post-processing ────────────────────────────────────────────────

# Strip common preamble noise from small models
_PREAMBLE_RE = re.compile(
    r"^(based on the (?:provided )?memories?[,:\s]*"
    r"|according to the (?:provided )?memories?[,:\s]*"
    r"|the answer is[:\s]*"
    r"|sure[,!:\s]*"
    r"|okay[,!:\s]*"
    r"|here(?:'s| is) the answer[:\s]*)",
    re.IGNORECASE,
)

# Sentence-ending punctuation (for truncation)
_SENTENCE_END = re.compile(r"[.!?]")


def _clean_answer(raw: str, mode: str = "firstline") -> str:
    """Trim small-model output.

    ``firstline`` (legacy, DEFAULT): noise-strip then keep only the first
    line — known to mutilate correct multi-line enumerations (probe finding
    vi; mean raw F1 beats cleaned in every measured condition). Pinned as the
    default so historical runs reproduce.
    ``full`` (D4 fix ii, flagged): same noise stripping (fences, asterisks,
    quotes, preamble), whole text kept — list answers survive post-processing.
    """
    text = raw.strip()

    # Remove markdown fences, asterisks, quotes
    text = re.sub(r"```[\s\S]*?```", "", text)
    text = text.replace("**", "").replace("*", "")
    text = text.strip('"\'')

    # Strip preamble
    text = _PREAMBLE_RE.sub("", text).strip()

    if mode == "full":
        return text

    # Ramble prevention, take first line
    first_line = text.split("\n")[0].strip()

    if len(first_line) > 200:
        m = _SENTENCE_END.search(first_line, 20)
        if m:
            first_line = first_line[: m.end()]

    return first_line


_ABSTENTION_PHRASES = frozenset({
    "idk",
    "i do not know",
    "i don't know",
    "i cannot answer",
    "i can't answer",
    "not enough information",
    "insufficient information",
    "no relevant information",
    "the answer is not in the memories",
    "none of the memories",
})


def _is_abstention(text: str, mode: str = "loose") -> bool:
    """Abstention detection.

    ``loose`` (legacy, DEFAULT): substring match — flags any answer that
    CONTAINS a refusal phrase, including informative hedged answers
    ("…IDK if there are other options"), a measured false-positive class
    (probe finding vii). Pinned as the default.
    ``exact`` (D4 fix v, flagged): the whole answer (normalized) must BE an
    abstention phrase — hedged answers never flag.
    """
    t = text.strip().lower().rstrip(".")

    if t in _ABSTENTION_PHRASES:
        return True
    if mode == "exact":
        return False
    return any(p in t for p in _ABSTENTION_PHRASES)


# ── reader class ────────────────────────────────────────────────────

class PlainTextReader:
    """Reader for small local LLMs.

    Parameters
    ----------
    llm : LLM
        The language model (OpenAI-compatible).
    max_tokens : int
        Hard generation cap.  64 is aggressive but prevents rambling.
        Increase to 96 if answers are getting truncated mid-word.
    max_evidence : int
        Maximum number of retrieved memories to include in the prompt
        (legacy mode only). Fewer = shorter prompt = more reliable output
        from small models.
    token_budget : int | None
        None (default) = legacy context: ``max_evidence`` items, each
        hard-truncated to ~300 chars — byte-for-byte what historical runs used.
        An int = E6c budgeted context: ONE total evidence budget of ~N tokens
        (len//4 approximation), filled whole-item in retrieval-rank order with
        an explicit marker on a final truncated item. The documented choice
        for new runs; use DEFAULT_READER_TOKEN_BUDGET (~legacy prompt size)
        unless the experiment says otherwise. The reader ``name`` carries the
        budget (e.g. ``PlainTextReader_rtok375``) so artifact paths and the
        ``reader`` results column can never confuse the two modes.
    """

    def __init__(
        self,
        llm: LLM,
        *,
        max_tokens: int = 64,
        max_evidence: int = 5,
        token_budget: int | None = None,
        prompt_version: str = "v1",
        abstention_match: str = "loose",
        clean_mode: str = "firstline",
    ) -> None:
        if prompt_version not in PROMPT_BUILDERS:
            raise ValueError(f"prompt_version must be one of {sorted(PROMPT_BUILDERS)}")
        if abstention_match not in ("loose", "exact"):
            raise ValueError("abstention_match must be 'loose' or 'exact'")
        if clean_mode not in ("firstline", "full"):
            raise ValueError("clean_mode must be 'firstline' or 'full'")
        self.llm = llm
        self.max_tokens = max_tokens
        self.max_evidence = max_evidence
        self.token_budget = token_budget
        self.prompt_version = prompt_version
        self.abstention_match = abstention_match
        self.clean_mode = clean_mode
        # Every non-default mode is encoded in the reader name, so artifact
        # paths and the `reader` results column can never confuse configs
        # (same convention as _rtokN).
        name = ("PlainTextReader" if token_budget is None
                else f"PlainTextReader_rtok{token_budget}")
        if prompt_version != "v1":
            name += f"_p{prompt_version}"
        if abstention_match != "loose":
            name += "_absexact"
        if clean_mode != "firstline":
            name += "_cleanfull"
        self.name = name

    def answer(
        self,
        question: Question,
        records: list[MemRecord],
        *,
        max_tokens: int | None = None,
    ) -> AnswerResult:
        wrap = PROMPT_BUILDERS[self.prompt_version]
        if self.token_budget is None:
            # Legacy: count-capped, per-item 300-char truncation.
            evidence = records[: self.max_evidence]
            prompt = _build_prompt(question, evidence, wrap=wrap)
            truncated = None            # not tracked on the legacy path
        else:
            # Budgeted: rank-order fill under one total token budget.
            evidence = records
            prompt, truncated = _build_prompt_budgeted(
                question, evidence, self.token_budget, wrap=wrap)
        tok_limit = max_tokens or self.max_tokens

        t0 = time.perf_counter()
        try:
            raw = self.llm.generate(
                prompt,
                max_tokens=tok_limit,
                temperature=0.0,
            ).strip()
        except Exception as exc:
            return AnswerResult(
                query_id=question.id,
                answer=f"[LLM error: {exc}]",
                abstained=True,
                latency_sec=time.perf_counter() - t0,
                meta={"error": str(exc)},
            )
        latency = time.perf_counter() - t0

        cleaned = _clean_answer(raw, mode=self.clean_mode)
        abstained = _is_abstention(cleaned, mode=self.abstention_match)

        return AnswerResult(
            query_id=question.id,
            answer=cleaned,
            abstained=abstained,
            latency_sec=latency,
            meta={
                "raw_output": raw,
                "cleaned_output": cleaned,
                "prompt_char_len": len(prompt),
                "evidence_count": len(evidence),
                "token_budget": self.token_budget,
                "evidence_truncated": truncated,
            },
        )