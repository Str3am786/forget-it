from __future__ import annotations

import re
import time
from typing import TYPE_CHECKING

from forgetit.core.schema import AnswerResult, MemRecord, Question

if TYPE_CHECKING:
    from forgetit.llm.base import LLM


# ── prompt builder ──────────────────────────────────────────────────

def _build_prompt(question: Question, records: list[MemRecord]) -> str:
    """Build a minimal, hard-to-derail prompt."""
    # Number the memories — models handle numbered lists well
    evidence_lines: list[str] = []
    for i, r in enumerate(records, 1):
        # Truncate each memory to ~300 chars to keep prompt short
        text = r.text[:300].rstrip()
        if len(r.text) > 300:
            text += "…"
        evidence_lines.append(f"[{i}] {text}")

    evidence = "\n".join(evidence_lines)

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
        Maximum number of retrieved memories to include in the prompt.
        Fewer = shorter prompt = more reliable output from small models.
    """

    def __init__(
        self,
        llm: LLM,
        *,
        max_tokens: int = 64,
        max_evidence: int = 5,
    ) -> None:
        self.llm = llm
        self.max_tokens = max_tokens
        self.max_evidence = max_evidence

    def answer(
        self,
        question: Question,
        records: list[MemRecord],
        *,
        max_tokens: int | None = None,
    ) -> AnswerResult:
        # Limit evidence count
        evidence = records[: self.max_evidence]
        prompt = _build_prompt(question, evidence)
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
            },
        )