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
