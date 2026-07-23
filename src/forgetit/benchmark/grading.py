"""Reader-answer grading — the QA-correctness metric the harness was missing.

A tiered stack, mirroring the eviction-regret design (reproducible core first,
expensive refinement layered on top, never as the headline):

  1. DETERMINISTIC (headline, free, the default): SQuAD-style normalized
     exact-match + containment, plus token-F1. Reproducible, no models. This is
     the primary number and a conservative LOWER BOUND on accuracy.
  2. PARAPHRASE RECOVERY (deterministic, opt-in): per-question alias sets and/or
     an embedding-similarity gate (cosine(answer, ref) >= threshold). Recovers
     "a heart doctor" == "cardiologist" without a judge — same embedder model =>
     same number => reproducible, at the cost of one calibrated threshold.
  3. JUDGE (validation only, never headline): an LLM judge on a SAMPLE, compared
     to the deterministic verdicts to report agreement — i.e. how much the cheap
     metric under-counts. Pin the model + prompt so even that is reproducible.
     Lives in the offline path; this module exposes the deterministic + embedding
     tiers, which are what a run can compute inline.

The honest gap deterministic matching leaves is paraphrase / semantic equivalence
(a correct answer the matcher can't see because it's phrased differently). Tiers
2 and 3 exist to *measure and shrink* that gap, not to replace the core.
"""
from __future__ import annotations

import re
import string
from collections import Counter

_ARTICLES = re.compile(r"\b(a|an|the)\b")
_PUNCT_TABLE = str.maketrans("", "", string.punctuation)


def normalize_answer(s: str) -> str:
    """SQuAD normalization: lowercase, drop punctuation, articles, extra space."""
    s = (s or "").lower().translate(_PUNCT_TABLE)
    s = _ARTICLES.sub(" ", s)
    return " ".join(s.split())


def normalized_em(answer: str, ref: str) -> bool:
    return normalize_answer(answer) == normalize_answer(ref) and bool(normalize_answer(ref))


def contains_match(answer: str, ref: str) -> bool:
    """Normalized gold string appears within the normalized answer (the common
    short-gold case: 'cardiologist' inside 'you should see a cardiologist')."""
    na, nr = normalize_answer(answer), normalize_answer(ref)
    return bool(nr) and nr in na


def token_f1(answer: str, ref: str) -> float:
    """SQuAD token-overlap F1 — graceful partial credit between EM and miss."""
    ta, tr = normalize_answer(answer).split(), normalize_answer(ref).split()
    if not ta and not tr:
        return 1.0
    if not ta or not tr:
        return 0.0
    common = Counter(ta) & Counter(tr)
    same = sum(common.values())
    if same == 0:
        return 0.0
    p, r = same / len(ta), same / len(tr)
    return 2 * p * r / (p + r)


# ════════════════════════════════════════════════════════════════════
# Answer-survival primitives (Tier-1 "is the gold ANSWER still recoverable
# from the resident item text"). Scored offline by scripts/eviction_regret.py
# over a (possibly long) item text T, NOT over a reader answer. These reuse the
# SQuAD ``normalize_answer`` above so the headline survival number and the QA
# grade share one normalization. See the module docstring there.
# ════════════════════════════════════════════════════════════════════

# LongMemEval marks abstention questions with a ``_abs`` question_id suffix (the
# base question_type is unchanged); BEAM marks them with ability=="abstention";
# LoCoMo marks its unanswerable category with question_type=="adversarial".
# A question is "answerable" iff it is none of these — answer-survival is only
# defined where there is an answer in the haystack to preserve. This frozenset is
# the single cross-workload registry of abstention/unanswerable type names.
_ABSTENTION_QUESTION_TYPES = frozenset({"abstention", "adversarial"})


def is_answerable(query_id: str, question_type: str) -> bool:
    """Whether a question expects an answer present in memory (vs. an abstention
    'no-info' question). Answer-survival rollups are scoped to these. This is the
    single source of truth for the answerable/abstention split across workloads."""
    if (query_id or "").endswith("_abs"):          # LongMemEval abstention convention
        return False
    if (question_type or "").strip().lower() in _ABSTENTION_QUESTION_TYPES:   # BEAM
        return False
    return True


def tokens(s: str) -> list[str]:
    """``normalize_answer`` then whitespace-split — the answer-token primitive."""
    return normalize_answer(s).split()


# ── numeric / date normalization (so "two million"==2000000, "May"==05) ─────
_NUM_WORDS = {
    "zero": 0, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6,
    "seven": 7, "eight": 8, "nine": 9, "ten": 10, "eleven": 11, "twelve": 12,
    "thirteen": 13, "fourteen": 14, "fifteen": 15, "sixteen": 16, "seventeen": 17,
    "eighteen": 18, "nineteen": 19, "twenty": 20, "thirty": 30, "forty": 40,
    "fifty": 50, "sixty": 60, "seventy": 70, "eighty": 80, "ninety": 90,
}
_SCALE_WORDS = {"hundred": 100, "thousand": 1_000, "million": 1_000_000,
                "billion": 1_000_000_000}
_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}
_NUMBER_RUN_WORDS = set(_NUM_WORDS) | set(_SCALE_WORDS) | {"and"}


def _eval_number_run(run: list[str]) -> int | None:
    """Fold a maximal run of cardinal-number words into one integer.
    ``["two", "million"] -> 2000000``; ``["forty", "five"] -> 45``;
    ``["two", "hundred", "thirty", "four"] -> 234``. None if the run held no
    actual number word (e.g. a lone "and")."""
    run = [w for w in run if w != "and"]
    result = current = 0
    seen = False
    for w in run:
        if w in _NUM_WORDS:
            current += _NUM_WORDS[w]
            seen = True
        elif w in _SCALE_WORDS:
            sc = _SCALE_WORDS[w]
            if sc == 100:
                current = (current or 1) * 100
            else:
                result += (current or 1) * sc
                current = 0
            seen = True
    return (result + current) if seen else None


def digit_normalize(s: str) -> str:
    """``normalize_answer`` + map month names and spelled cardinals to digits, so
    a numeric/date answer matches whichever surface form the item text used. Only
    ADDS digit forms; it never removes a token, so it can only turn a near-miss
    into a hit (used as the max with the plain containment test)."""
    toks = tokens(s)
    toks = [str(_MONTHS[t]) if t in _MONTHS else t for t in toks]
    out: list[str] = []
    i, n = 0, len(toks)
    while i < n:
        if toks[i] in _NUM_WORDS or toks[i] in _SCALE_WORDS:
            j = i
            run: list[str] = []
            while j < n and toks[j] in _NUMBER_RUN_WORDS:
                run.append(toks[j])
                j += 1
            while run and run[-1] == "and":     # don't swallow a trailing "and"
                run.pop()
                j -= 1
            val = _eval_number_run(run)
            if val is not None:
                out.append(str(val))
            else:
                out.extend(run)
            i = j
        else:
            out.append(toks[i])
            i += 1
    return " ".join(out)


def answer_in_text(answer: str, text: str) -> bool:
    """Containment primitive: the normalized gold answer is a substring of the
    normalized item text. Tries a digit-normalized variant too (for numeric/date
    answers) and takes the max. ``T`` is a haystack, so this is the headline
    "the exact answer phrase is still present" signal."""
    na, nt = normalize_answer(answer), normalize_answer(text)
    if na and na in nt:
        return True
    da, dt = digit_normalize(answer), digit_normalize(text)
    return bool(da) and da in dt


def answer_recall(answer: str, text: str) -> float:
    """ROUGE-1 RECALL of the answer span against the item text: the fraction of
    the answer's tokens (with multiplicity) present in the text. Precision is
    deliberately omitted — ``text`` is a haystack so precision≈0 and is
    meaningless; recall alone captures "are the answer tokens still present"."""
    ta = tokens(answer)
    if not ta:
        return 0.0
    ca, ct = Counter(ta), Counter(tokens(text))
    overlap = sum(min(c, ct[w]) for w, c in ca.items())
    return overlap / len(ta)


def answer_contained_any(answers, text: str) -> int:
    """Binary containment maxed over a set of acceptable gold answers (mirrors
    grade()'s multi-ref handling). 1 if ANY acceptable answer is contained."""
    return 1 if any(answer_in_text(a, text) for a in answers if a) else 0


def answer_recall_any(answers, text: str) -> float:
    """Graded recall maxed over a set of acceptable gold answers."""
    return max((answer_recall(a, text) for a in answers if a), default=0.0)


def embedding_similarity(answer: str, ref: str, embedder) -> float:
    """Cosine between answer and ref under the project embedder (paraphrase tier).
    Reuses the shared max_cross_similarity primitive; same model => same number."""
    import numpy as np
    from forgetit.embeddings.similarity import max_cross_similarity
    va = np.asarray([embedder.embed_one(answer)], dtype=np.float32)
    vr = np.asarray([embedder.embed_one(ref)], dtype=np.float32)
    return float(max_cross_similarity(va, vr, assume_normalized=False)[0])


def grade(
    answer: str,
    ref: str,
    *,
    abstained: bool = False,
    aliases: tuple[str, ...] = (),
    embedder=None,
    sim_threshold: float = 0.83,
) -> dict:
    """Tiered verdict for one (answer, ref). Returns
    ``{correct, f1, tier, similarity}``.

    Deterministic core uses only ``ref`` (+ optional ``aliases``); pass an
    ``embedder`` to enable the paraphrase tier. An abstention or empty answer is
    incorrect by definition (it declined to answer). ``tier`` records which rung
    accepted it (exact / contains / alias / embedding / miss / abstain) so the
    paraphrase-recovery contribution is itself measurable.
    """
    if abstained or not (answer or "").strip():
        return {"correct": False, "f1": 0.0, "tier": "abstain", "similarity": float("nan")}

    refs = [ref, *aliases]
    f1 = max((token_f1(answer, r) for r in refs), default=0.0)

    if any(normalized_em(answer, r) for r in refs):
        tier = "exact" if normalized_em(answer, ref) else "alias"
        return {"correct": True, "f1": f1, "tier": tier, "similarity": float("nan")}
    if any(contains_match(answer, r) for r in refs):
        return {"correct": True, "f1": f1, "tier": "contains", "similarity": float("nan")}

    sim = float("nan")
    if embedder is not None:
        sim = max((embedding_similarity(answer, r, embedder) for r in refs), default=float("nan"))
        if sim == sim and sim >= sim_threshold:
            return {"correct": True, "f1": f1, "tier": "embedding", "similarity": sim}

    return {"correct": False, "f1": f1, "tier": "miss", "similarity": sim}


def grade_with_abstention(
    answer: str,
    ref: str,
    *,
    abstained: bool,
    answerable: bool,
    **grade_kwargs,
) -> dict:
    """Abstention-aware tier over :func:`grade` (D4 fix i).

    For ANSWERABLE questions this defers to ``grade()`` unchanged (abstention
    stays a failure there — that is correct behavior). For UNANSWERABLE
    questions (LongMemEval ``_abs`` ids, BEAM ability=="abstention" — see
    :func:`is_answerable`), abstaining IS the reference behavior: correct iff
    abstained. f1 mirrors correctness (1.0/0.0) so mean-F1 rollups don't
    silently punish the rows the binary grade rewards. Tiers
    ``abstain_correct`` / ``abstain_missed`` keep these rows countable apart
    from content grades.

    SCOPE (SYMS, 2026-07-22): ported from the verification-sprint branch for
    OFFLINE re-grading only — ``scripts/regrade_answers.py`` is its one caller.
    The runtime path (``runner_core``) deliberately still calls ``grade()``, so
    every stored ``qa_correct`` keeps its historical semantics and v1/v2 rows
    stay comparable. Do not wire this into the runners without re-running the
    grid: it changes 30 of 500 LongMemEval verdicts.
    """
    if answerable:
        return grade(answer, ref, abstained=abstained, **grade_kwargs)
    if abstained:
        return {"correct": True, "f1": 1.0, "tier": "abstain_correct",
                "similarity": float("nan")}
    return {"correct": False, "f1": 0.0, "tier": "abstain_missed",
            "similarity": float("nan")}
