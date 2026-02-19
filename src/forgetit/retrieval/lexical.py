from __future__ import annotations

import re
from typing import Mapping, List

from forgetit.core.schema import MemRecord, Query
from forgetit.retrieval.base import Retriever


_WORD = re.compile(r"[A-Za-z0-9_]+")


def _tokenize(text: str) -> set[str]:
    return {t.lower() for t in _WORD.findall(text)}


class LexicalOverlapRetriever(Retriever):
    """
    Simple deterministic baseline retriever.
    Score = |tokens(query) ∩ tokens(item.text)|.
    Tie-breakers: higher overlap, then newer created_at, then id.
    """

    def topk(self, items: Mapping[str, MemRecord], query: Query, k: int) -> List[MemRecord]:
        if k <= 0:
            return []

        q = _tokenize(query.text)
        if not q:
            # If query has no tokens, return newest items (deterministic)
            return sorted(items.values(), key=lambda it: (it.created_at, it.id), reverse=True)[:k]

        scored = []
        for it in items.values():
            itoks = _tokenize(it.text)
            overlap = len(q.intersection(itoks))
            scored.append((overlap, it.created_at, it.id, it))

        scored.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
        return [x[3] for x in scored[:k]]
