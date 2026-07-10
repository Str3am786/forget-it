"""Versioned prompt text for the offline judge (E1 recoverability bracket +
E2 answer re-grade).

BLINDNESS IS STRUCTURAL: no builder in this module accepts a policy, budget,
retriever, or store label — a prompt physically cannot contain them. Tests
assert this on real rows; keep it that way.

Bump PROMPT_VERSION on ANY change to the strings below. The pair
(judge_model, prompt_version) is stamped into every output row, and the cache
key includes it, so old cached verdicts are never silently reused across
prompt revisions.
"""
from __future__ import annotations

PROMPT_VERSION = "judge-v1"

# ── E1 — recoverability bracket ─────────────────────────────────────
# "Is the gold answer recoverable from this (possibly compressed) resident
# text?" Binary, verdict-first so the judgment is forced before any prose.

_E1_TEMPLATE = """\
You are grading whether a stored memory still supports answering a question.

Question:
{question}

Correct answer:
{gold_answer}

Stored memory text:
---
{resident_text}
---

Could a careful reader recover the correct answer to the question from the \
stored memory text alone? Paraphrase or summary counts as recoverable if the \
answer's content is present; it is NOT recoverable if essential specifics \
are missing.

Reply with exactly "VERDICT: YES" or "VERDICT: NO" on the first line, then \
one short sentence of justification on the next line."""


def build_e1_prompt(question: str, gold_answer: str, resident_text: str) -> str:
    return _E1_TEMPLATE.format(
        question=question.strip(),
        gold_answer=gold_answer.strip(),
        resident_text=resident_text.strip(),
    )


# ── E2 — answer re-grade ────────────────────────────────────────────
# Pointwise, reference-based, per-question-type rubric hooks. The deterministic
# EM/containment/F1 grade stays the reproducible headline; this instrument
# exists because that grader floors on format mismatch.

DEFAULT_RUBRIC = (
    "The answer is CORRECT if it conveys the same information as the "
    "reference answer — wording, order, and formatting differences do not "
    "matter. It is INCORRECT if it contradicts the reference, omits the "
    "referenced fact, or answers a different question."
)

# question_type → rubric text. Types not listed fall back to DEFAULT_RUBRIC.
# Keys cover both datasets' vocabularies (LongMemEval types, BEAM abilities).
RUBRICS: dict[str, str] = {
    "abstention": (
        "The correct behavior for this question is to decline: the needed "
        "information does not exist in the conversation. The answer is "
        "CORRECT if it abstains or states the information is unavailable "
        "(e.g. 'IDK'); it is INCORRECT if it asserts a substantive answer."
    ),
    "temporal-reasoning": (
        "The answer is CORRECT if the date/time/duration/order it gives "
        "matches the reference (equivalent formats count: '2 weeks' = '14 "
        "days'). It is INCORRECT if the temporal value or ordering differs."
    ),
    "event_ordering": (
        "The answer is CORRECT if it reproduces the reference ordering of "
        "events (paraphrased event names are fine). It is INCORRECT if any "
        "events are out of order or missing from the ordering."
    ),
    "knowledge-update": (
        "The reference reflects the MOST RECENT state of a fact that changed "
        "over time. The answer is CORRECT only if it gives that updated "
        "value; an earlier, superseded value is INCORRECT even if it appeared "
        "in the conversation."
    ),
    "contradiction_resolution": (
        "The correct behavior is to notice that the conversation contains "
        "contradictory statements about the topic. The answer is CORRECT if "
        "it flags the contradiction (naming the conflicting claims counts); "
        "it is INCORRECT if it silently picks one side or answers as though "
        "no conflict exists."
    ),
}


_E2_TEMPLATE = """\
You are grading an answer to a question against a reference.

Question:
{question}

Reference answer:
{reference}

Answer to grade:
{reader_answer}

Grading rule: {rubric}

Reply with exactly "VERDICT: CORRECT" or "VERDICT: INCORRECT" on the first \
line, then one short sentence of justification on the next line."""


_E2_GOLDTEXT_TEMPLATE = """\
You are grading an answer to a question. There is no short reference answer; \
instead you are given the conversation excerpts the answer must be based on.

Question:
{question}

Conversation excerpts (ground truth):
---
{gold_texts}
---

Answer to grade:
{reader_answer}

Grading rule: {rubric} Judge the answer against the excerpts: it is CORRECT \
if the excerpts support it, INCORRECT if they contradict it or do not \
contain the needed information while the answer asserts it anyway.

Reply with exactly "VERDICT: CORRECT" or "VERDICT: INCORRECT" on the first \
line, then one short sentence of justification on the next line."""


def rubric_for(question_type: str) -> str:
    return RUBRICS.get((question_type or "").strip(), DEFAULT_RUBRIC)


def build_e2_prompt(question: str, question_type: str, reference: str,
                    reader_answer: str) -> str:
    return _E2_TEMPLATE.format(
        question=question.strip(),
        reference=reference.strip(),
        reader_answer=(reader_answer or "").strip() or "(empty answer)",
        rubric=rubric_for(question_type),
    )


def build_e2_goldtext_prompt(question: str, question_type: str,
                             gold_texts: list[str], reader_answer: str,
                             max_chars_per_text: int = 2000) -> str:
    """Empty-reference types: grade against the gold items' content instead.

    Gold texts are truncated defensively (they can be whole sessions); the
    cut is marked so the judge knows the excerpt is partial.
    """
    blocks = []
    for t in gold_texts:
        t = t.strip()
        if len(t) > max_chars_per_text:
            t = t[:max_chars_per_text].rstrip() + " …[truncated]"
        blocks.append(t)
    return _E2_GOLDTEXT_TEMPLATE.format(
        question=question.strip(),
        gold_texts="\n\n".join(blocks),
        reader_answer=(reader_answer or "").strip() or "(empty answer)",
        rubric=rubric_for(question_type),
    )
