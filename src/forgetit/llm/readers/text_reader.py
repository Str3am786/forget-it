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


def _build_prompt(question: Question, records: list[MemRecord]) -> str:
    """Legacy prompt: hard-truncate every memory to ~300 chars.

    Kept selectable (token_budget=None) so historical runs stay reproducible.
    Known artifact: mutilates long items (e.g. compression summaries) — the
    budgeted builder below is the documented choice for new runs (E6c).
    """
    # Number the memories — models handle numbered lists well
    evidence_lines: list[str] = []
    for i, r in enumerate(records, 1):
        # Truncate each memory to ~300 chars to keep prompt short
        text = r.text[:300].rstrip()
        if len(r.text) > 300:
            text += "…"
        evidence_lines.append(f"[{i}] {text}")

    return _wrap_prompt(question, "\n".join(evidence_lines))


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
                           token_budget: int) -> tuple[str, bool]:
    evidence, truncated = _build_evidence_budgeted(records, token_budget)
    return _wrap_prompt(question, evidence), truncated


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


def _clean_answer(raw: str) -> str:
    """Aggressively trim small-model output to the first useful sentence."""
    text = raw.strip()

    # Remove markdown fences, asterisks, quotes
    text = re.sub(r"```[\s\S]*?```", "", text)
    text = text.replace("**", "").replace("*", "")
    text = text.strip('"\'')

    # Strip preamble
    text = _PREAMBLE_RE.sub("", text).strip()

    # Ramble prevention, take first line
    first_line = text.split("\n")[0].strip()

    if len(first_line) > 200:
        m = _SENTENCE_END.search(first_line, 20) 
        if m:
            first_line = first_line[: m.end()]

    return first_line


def _is_abstention(text: str) -> bool:
    """Broad abstention detection — catches many refusal phrasings."""
    t = text.strip().lower().rstrip(".")
    abstention_phrases = {
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
    }

    if t in abstention_phrases:
        return True

    return any(p in t for p in abstention_phrases)


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
    ) -> None:
        self.llm = llm
        self.max_tokens = max_tokens
        self.max_evidence = max_evidence
        self.token_budget = token_budget
        self.name = ("PlainTextReader" if token_budget is None
                     else f"PlainTextReader_rtok{token_budget}")

    def answer(
        self,
        question: Question,
        records: list[MemRecord],
        *,
        max_tokens: int | None = None,
    ) -> AnswerResult:
        if self.token_budget is None:
            # Legacy: count-capped, per-item 300-char truncation.
            evidence = records[: self.max_evidence]
            prompt = _build_prompt(question, evidence)
            truncated = None            # not tracked on the legacy path
        else:
            # Budgeted: rank-order fill under one total token budget.
            evidence = records
            prompt, truncated = _build_prompt_budgeted(
                question, evidence, self.token_budget)
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

        cleaned = _clean_answer(raw)
        abstained = _is_abstention(cleaned)

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