"""
BM25-based lexical retriever.

Drop-in replacement for LexicalOverlapRetriever.  Deterministic,
no external dependencies, much better ranking for the benchmark.

Why BM25 instead of raw overlap:
    - Term frequency saturation (k1): a word appearing 10× in a doc
      doesn't score 10× a word appearing once.
    - Document-length normalisation (b): long documents aren't
      unfairly boosted.
    - IDF weighting: rare words matter more than common ones.

The retriever is stateless between calls — corpus stats are computed
on-the-fly from the ``items`` dict passed to ``topk()``.  This is fine
because the item set changes between calls (evictions_total_at_query_time happen), and
the total number of items is small (bounded by your memory budget).
"""
from __future__ import annotations

import math
import re
from typing import Mapping, List

from forgetit.core.schema import MemRecord, Query
from forgetit.retrieval.base import Retriever

# ── tokenisation ────────────────────────────────────────────────────

_WORD = re.compile(r"[A-Za-z0-9_]+")

# Minimal English stop-words — enough to stop "the" and "is" from
# dominating, small enough to not accidentally filter meaningful tokens.
_STOP: frozenset[str] = frozenset(
    "a an the is are was were be been being have has had do does did "
    "will would shall should can could may might must to of in for on "
    "at by with from and or but not no nor so yet if then than that "
    "this it its i me my we our you your he him his she her they them "
    "their what which who whom how when where why".split()
)


def _tokenize(text: str, *, remove_stops: bool = True) -> list[str]:
    """Return lowered tokens, optionally with stop-words removed."""
    tokens = [t.lower() for t in _WORD.findall(text)]
    if remove_stops:
        tokens = [t for t in tokens if t not in _STOP]
    return tokens


# ── BM25 retriever ──────────────────────────────────────────────────

class BM25Retriever(Retriever):
    """Deterministic BM25 retriever.

    Parameters
    ----------
    k1 : float
        Term-frequency saturation. 1.2–2.0 is standard.
    b : float
        Length-normalisation factor. 0 = no normalisation, 1 = full.
    """

    def __init__(self, *, k1: float = 1.5, b: float = 0.75) -> None:
        self.k1 = k1
        self.b = b

    def topk(
        self,
        items: Mapping[str, MemRecord],
        query: Query,
        k: int,
    ) -> List[MemRecord]:
        if k <= 0 or not items:
            return []

        q_tokens = _tokenize(query.text)
        if not q_tokens:
            # Fallback: newest items
            return sorted(
                items.values(),
                key=lambda it: (it.created_at, it.id),
                reverse=True,
            )[:k]

        q_set = set(q_tokens)

        doc_tokens: dict[str, list[str]] = {}
        df: dict[str, int] = {}

        for rid, rec in items.items():
            toks = _tokenize(rec.text)
            doc_tokens[rid] = toks
            seen: set[str] = set()
            for t in toks:
                if t not in seen:
                    df[t] = df.get(t, 0) + 1
                    seen.add(t)

        N = len(items)
        avgdl = sum(len(v) for v in doc_tokens.values()) / max(N, 1)


        idf: dict[str, float] = {}
        for term in q_set:
            n = df.get(term, 0)
            # Standard BM25 IDF, floored at 0
            val = math.log((N - n + 0.5) / (n + 0.5) + 1.0)
            idf[term] = max(val, 0.0)

        # ── score each document ─────────────────────────────────
        scored: list[tuple[float, str, str, MemRecord]] = []

        for rid, rec in items.items():
            toks = doc_tokens[rid]
            dl = len(toks)

            # Build term-frequency map for this doc
            tf: dict[str, int] = {}
            for t in toks:
                if t in q_set:
                    tf[t] = tf.get(t, 0) + 1

            score = 0.0
            for term in q_set:
                f = tf.get(term, 0)
                if f == 0:
                    continue
                num = f * (self.k1 + 1)
                denom = f + self.k1 * (1 - self.b + self.b * dl / max(avgdl, 1))
                score += idf.get(term, 0.0) * (num / denom)

            # Tie-breakers: higher score → newer → lexicographic id
            scored.append((score, rec.created_at, rec.id, rec))

        scored.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
        return [x[3] for x in scored[:k]]




class LexicalOverlapRetriever(Retriever):
    """Original token-overlap retriever (preserved for comparison)."""

    def topk(
        self,
        items: Mapping[str, MemRecord],
        query: Query,
        k: int,
    ) -> List[MemRecord]:
        if k <= 0:
            return []
        q = {t.lower() for t in _WORD.findall(query.text)}
        if not q:
            return sorted(
                items.values(),
                key=lambda it: (it.created_at, it.id),
                reverse=True,
            )[:k]
        scored = []
        for it in items.values():
            itoks = {t.lower() for t in _WORD.findall(it.text)}
            overlap = len(q.intersection(itoks))
            scored.append((overlap, it.created_at, it.id, it))
        scored.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
        return [x[3] for x in scored[:k]]