"""The reading stage: retrieved memories in, a graded answer out.

"""
from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass, field, asdict
from typing import TYPE_CHECKING

from forgetit.core.schema import AnswerResult, MemRecord, Question

if TYPE_CHECKING:
    from forgetit.llm.base import LLM

_CHARS_PER_TOKEN = 4
TRUNCATION_MARKER = " […truncated]"

# The campaign's measured value, not a round number. At 1500 the k-list
# questions measured evidence delivery at 0.4567 — session itemisers produce
# items large enough that the budget cuts gold before the reader sees it. 32k
# leaves no headroom for wrapper + generation against the 32768 chat serving
# window the LUMI jobs pin, so 16000 is the ceiling that actually delivers.
DEFAULT_TOKEN_BUDGET = 16000
DEFAULT_ANSWER_MAX_TOKENS = 256


# ── the prompt ──────────────────────────────────────────────────────

DEFAULT_PROMPT = (
    "Answer the question using ONLY the memories below.\n"
    "You may combine facts from several memories and compute derived "
    "answers from them (date differences, counts, ordered lists).\n"
    "Each memory begins with the date of the conversation it comes from, in "
    "parentheses, when that date is known.\n"
    "If the information needed is absent from the memories, say exactly: IDK\n"
    "Be concise.\n"
    "\n"
    "Memories:\n"
    "{evidence}\n"
    "\n"
    "Question: {question}\n"
    "Answer:"
)
"""The default instruction wrapper.

Permits derivation ("you may combine facts and compute derived answers"),
which removes the false refusals the previous wording caused on questions
whose answer is a difference or a count, and drops the one-sentence cap that
mutilated enumerations. The exact IDK string is what abstention detection keys
on, so it stays literal in any replacement.

A TEMPLATE, not a version enum. The prompt is an experimental axis — comparing
wordings across reader models is a real study — but a two-item enum with a
deprecated member is not how you run one. Pass your own via
``PlainTextReader(prompt=...)``; its sha lands on every row, so two runs with
different wordings are never silently pooled. Must contain {evidence} and
{question}.
"""

# Constructor templates only — PARAMETRIC_PROMPT deliberately has no {evidence}
# and is therefore not a valid PlainTextReader(prompt=...) argument.
REQUIRED_PROMPT_FIELDS = ("evidence", "question")

PARAMETRIC_PROMPT = (
    "Answer the question from your own knowledge.\n"
    "If you do not know, say exactly: IDK\n"
    "Be concise.\n"
    "\n"
    "Question: {question}\n"
    "Answer:"
)
"""The no-evidence wrapper for measuring PARAMETRIC memory.

Feeding the standard prompt an empty memories block measures instruction
compliance ("say IDK when memories are absent"), not what the model knows.
Measuring what the campaign's accuracy owes to the model rather than to the
memory system requires asking the model directly — this template does that,
and the delta between it and a gold-evidence context is the memory system's
actual contribution.
"""


def prompt_sha(template: str) -> str:
    """Content hash of a prompt template — the provenance handle.

    Same convention as the compression policy's prompt_compress_sha: the
    wording is part of the experiment's identity, so a run that changes it is
    a different configuration and must be distinguishable by columns alone.
    """
    return hashlib.sha256((template or "").encode("utf-8")).hexdigest()[:12]


def render_prompt(template: str, question: Question, evidence: str) -> str:
    return template.format(evidence=evidence, question=question.text)


# ── context construction (pure — no model) ──────────────────────────

@dataclass
class ReaderContext:
    """Everything the model will see, and the audit of how it was assembled.

    Serialisable on purpose: persist these during a run and every later reader
    model is an offline pass over stored text. `item_ids` is what makes a
    delivered-evidence claim checkable against the retrieval log rather than
    inferred from a count.
    """
    query_id: str
    question_text: str
    prompt: str
    item_ids: list[str] = field(default_factory=list)
    n_offered: int = 0            # what retrieval handed over
    n_delivered: int = 0          # what the prompt actually carried
    truncated: bool = False       # a delivered item was cut mid-text
    evidence_chars: int = 0
    token_budget: int = DEFAULT_TOKEN_BUDGET
    # The wording that produced this prompt, and its handle. Persisted with the
    # context so an offline reader pass is reproducible even if the default
    # template later changes.
    prompt_sha: str = ""
    # Empty default: a context persisted before the renderer carried an identity
    # must deserialise as what it was, not be relabelled.
    render_sha: str = ""

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json(cls, s: str) -> "ReaderContext":
        return cls(**json.loads(s))


def build_evidence(records: list[MemRecord],
                   token_budget: int) -> tuple[str, bool, list[str]]:
    """Fill evidence in rank order under ONE total budget, whole items first.

    Returns (block, truncated, delivered_ids). No per-item cap: a long item is
    either delivered whole, delivered as a marked head, or not at all — never
    silently mutilated, which is what the old 300-char cap did to every
    compression summary in the campaign.
    """
    remaining = token_budget * _CHARS_PER_TOKEN
    lines: list[str] = []
    delivered: list[str] = []
    truncated = False

    for i, r in enumerate(records, 1):
        # The date is looked up at RENDER time (forgetit.benchmark.item_dates)
        # and rides in on meta, never in r.text -- item bytes, budgets and every
        # eviction decision stay identical to the undated campaign.
        date = (getattr(r, "meta", None) or {}).get("date_str")
        prefix = f"[{i}] ({date}) " if date else f"[{i}] "
        text = (r.text or "").rstrip()
        sep = 1 if lines else 0
        cost = sep + len(prefix) + len(text)
        if cost <= remaining:
            lines.append(prefix + text)
            delivered.append(r.id)
            remaining -= cost
            continue
        head = remaining - sep - len(prefix) - len(TRUNCATION_MARKER)
        if head > 0:
            lines.append(prefix + text[:head].rstrip() + TRUNCATION_MARKER)
            delivered.append(r.id)
            truncated = True
        break

    return "\n".join(lines), truncated, delivered


def render_sha(template: str = DEFAULT_PROMPT) -> str:
    """Identity of the render contract: template wording plus the exact output
    shape of build_evidence, so a change to either is detectable from a row
    alone.

    Derived, never typed: a hand-maintained label goes stale the moment someone
    edits the renderer and forgets to bump it, and the stale label is worse than
    no label because it certifies the wrong thing. Same convention as
    :func:`prompt_sha`.
    """
    fixture = [MemRecord(id="a", text="alpha", meta={"date_str": "D"}),
               MemRecord(id="b", text="beta")]
    block, _, _ = build_evidence(fixture, 1000)
    return hashlib.sha256((template + "\x00" + block).encode()).hexdigest()[:12]


def build_context(question: Question, records: list[MemRecord], *,
                  token_budget: int = DEFAULT_TOKEN_BUDGET,
                  prompt: str = DEFAULT_PROMPT) -> ReaderContext:
    """Deterministic: same records, budget and template give the same context."""
    evidence, truncated, delivered = build_evidence(records, token_budget)
    return ReaderContext(
        query_id=question.id,
        question_text=question.text,
        prompt=render_prompt(prompt, question, evidence),
        prompt_sha=prompt_sha(prompt),
        item_ids=delivered,
        n_offered=len(records),
        n_delivered=len(delivered),
        truncated=truncated,
        evidence_chars=len(evidence),
        token_budget=token_budget,
        render_sha=render_sha(prompt),
    )


def build_parametric_context(question: Question) -> ReaderContext:
    """A context with NO evidence, under the parametric wrapper."""
    return ReaderContext(
        query_id=question.id,
        question_text=question.text,
        prompt=PARAMETRIC_PROMPT.format(question=question.text),
        item_ids=[], n_offered=0, n_delivered=0, truncated=False,
        evidence_chars=0, token_budget=0,
        prompt_sha=prompt_sha(PARAMETRIC_PROMPT),
        render_sha=render_sha(PARAMETRIC_PROMPT),
    )


# ── post-processing ─────────────────────────────────────────────────

_PREAMBLE_RE = re.compile(
    r"^(based on the (?:provided )?memories?[,:\s]*"
    r"|according to the (?:provided )?memories?[,:\s]*"
    r"|the answer is[:\s]*"
    r"|sure[,!:\s]*"
    r"|okay[,!:\s]*"
    r"|here(?:'s| is) the answer[:\s]*)",
    re.IGNORECASE,
)


def clean_answer(raw: str) -> str:
    """Strip small-model noise. Never truncate.

    The old first-line rule cut correct multi-line answers — enumerations are
    the reference format for several BEAM and LongMemEval types, so truncating
    at the newline destroyed exactly the answers those questions want.
    """
    text = (raw or "").strip()
    text = re.sub(r"```[\s\S]*?```", "", text)
    text = text.replace("**", "").replace("*", "")
    text = text.strip("\"'")
    return _PREAMBLE_RE.sub("", text).strip()


_ABSTENTION_PHRASES = frozenset({
    "idk", "i do not know", "i don't know", "i cannot answer", "i can't answer",
    "not enough information", "insufficient information",
    "no relevant information", "the answer is not in the memories",
    "none of the memories",
})


def is_abstention(text: str) -> bool:
    """The whole answer must BE a refusal.

    Substring matching flagged "…, though IDK whether there were others" as an
    abstention and scored an informative answer as a refusal. An answer that
    merely CONTAINS a refusal phrase is an answer.
    """
    return (text or "").strip().lower().rstrip(".") in _ABSTENTION_PHRASES


# ── reader ──────────────────────────────────────────────────────────

class PlainTextReader:
    """Reads an answer out of retrieved memories.

    ``name`` carries the model tag and the evidence budget, and nothing else —
    there are no other behavioural knobs to encode. Two readers with different
    models are two arms; two readers with the same model and budget are the
    same reader.
    """

    def __init__(
        self,
        llm: "LLM",
        *,
        token_budget: int = DEFAULT_TOKEN_BUDGET,
        max_tokens: int = DEFAULT_ANSWER_MAX_TOKENS,
        prompt: str = DEFAULT_PROMPT,
    ) -> None:
        if token_budget <= 0:
            raise ValueError("token_budget must be > 0")
        if max_tokens <= 0:
            raise ValueError("max_tokens must be > 0")
        missing = [f for f in REQUIRED_PROMPT_FIELDS if "{" + f + "}" not in prompt]
        if missing:
            raise ValueError(
                f"prompt template is missing {missing}; it must contain "
                f"{{evidence}} and {{question}}")
        self.llm = llm
        self.token_budget = token_budget
        self.max_tokens = max_tokens
        self.prompt = prompt
        self.prompt_sha = prompt_sha(prompt)
        # Colons and slashes become path separators / drive markers in the
        # artifact layout, and the reader name IS a directory component.
        model = re.sub(r"[^A-Za-z0-9._-]", "-",
                       str(getattr(llm, "model", "unknown")))
        self.name = f"reader_{model}_rtok{token_budget}_gen{max_tokens}"
        if self.prompt_sha != prompt_sha(DEFAULT_PROMPT):
            # A non-default wording is a different arm; the artifact path and
            # the `reader` column must say so.
            self.name += f"_p{self.prompt_sha[:6]}"

    # -- generation from a prebuilt context -------------------------------
    def answer_from_context(self, ctx: ReaderContext, *,
                            max_tokens: int | None = None) -> AnswerResult:
        """Generate from a context built earlier — possibly by another run.

        This is the seam that makes a reader sweep affordable: contexts are
        persisted once during the policy run, then every model afterwards is an
        offline pass with no store, no eviction and no retrieval.
        """
        cap = max_tokens or self.max_tokens
        t0 = time.perf_counter()

        raw, finish_reason, error = "", None, None
        try:
            # Prefer a metadata-returning call where the client offers one, so
            # a generation cut at the cap is recorded rather than inferred.
            gen_meta = getattr(self.llm, "generate_with_meta", None)
            if callable(gen_meta):
                raw, finish_reason = gen_meta(ctx.prompt, max_tokens=cap,
                                              temperature=0.0)
            else:
                raw = self.llm.generate(ctx.prompt, max_tokens=cap,
                                        temperature=0.0)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"

        latency = time.perf_counter() - t0
        meta = {
            "prompt": ctx.prompt,
            "prompt_char_len": len(ctx.prompt),
            "item_ids": list(ctx.item_ids),
            "evidence_count": ctx.n_offered,
            "evidence_delivered": ctx.n_delivered,
            "evidence_truncated": ctx.truncated,
            "token_budget": ctx.token_budget,
            "prompt_sha": ctx.prompt_sha,
            "render_sha": ctx.render_sha,
            "gen_max_tokens": cap,
            "finish_reason": finish_reason,
            # A cut generation is not a wrong answer, it is an unfinished one —
            # the distinction the campaign could not make.
            "cap_hit": (finish_reason == "length") if finish_reason else None,
            "raw_output": raw,
        }

        if error is not None:
            # NOT an abstention. Scoring a timeout as a correct refusal on an
            # unanswerable question was a live bug; errored rows belong in
            # neither the correct nor the abstained column.
            meta["error"] = error
            return AnswerResult(
                query_id=ctx.query_id, answer="", abstained=False,
                errored=True, latency_sec=latency, meta=meta,
            )

        cleaned = clean_answer(raw)
        meta["cleaned_output"] = cleaned
        return AnswerResult(
            query_id=ctx.query_id, answer=cleaned,
            abstained=is_abstention(cleaned), errored=False,
            latency_sec=latency, meta=meta,
        )

    # -- the runner-facing path -------------------------------------------
    def answer(self, question: Question, records: list[MemRecord], *,
               max_tokens: int | None = None) -> AnswerResult:
        ctx = build_context(question, records, token_budget=self.token_budget,
                            prompt=self.prompt)
        return self.answer_from_context(ctx, max_tokens=max_tokens)