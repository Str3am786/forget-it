"""Cosine-similarity primitives — equally for in-store pairs (redundancy) and
store-vs-outside pairs (e.g. gold vs noise separability).

All functions take/return ``numpy`` arrays of shape (N, D).  They do NOT assume
the vectors are normalised; pass ``assume_normalized=True`` to skip the
re-normalisation when you embedded with L2 already (the default in
:class:`OpenAiEmbedder`).  Large sets are processed in row-blocks so an
N×N matrix is never materialised.
"""
from __future__ import annotations

import numpy as np

_BLOCK = 2048


def l2_normalize(mat: np.ndarray) -> np.ndarray:
    n = np.linalg.norm(mat, axis=1, keepdims=True)
    n[n == 0] = 1.0
    return mat / n


def _prep(a: np.ndarray, assume_normalized: bool) -> np.ndarray:
    a = np.asarray(a, dtype=np.float32)
    return a if assume_normalized else l2_normalize(a)


def max_cross_similarity(
    a: np.ndarray, b: np.ndarray, *, assume_normalized: bool = True,
) -> np.ndarray:
    """For each row of ``a``, the max cosine to any row of ``b``.

    Use for "is this item similar to anything in the store / the other class".
    Returns shape (len(a),).  ``a`` and ``b`` need NOT be disjoint, but if they
    overlap a row may match itself — use :func:`nearest_within` for in-set work.
    """
    a = _prep(a, assume_normalized)
    b = _prep(b, assume_normalized)
    out = np.empty(a.shape[0], dtype=np.float32)
    for i in range(0, a.shape[0], _BLOCK):
        sims = a[i:i + _BLOCK] @ b.T
        out[i:i + _BLOCK] = sims.max(axis=1) if b.shape[0] else 0.0
    return out


def nearest_within(
    mat: np.ndarray, *, assume_normalized: bool = True,
) -> np.ndarray:
    """For each row, the max cosine to any OTHER row of the same set (self
    excluded).  The redundancy signal for one store.  Returns shape (N,)."""
    mat = _prep(mat, assume_normalized)
    n = mat.shape[0]
    out = np.full(n, -1.0, dtype=np.float32)
    if n < 2:
        return out
    for i in range(0, n, _BLOCK):
        block = mat[i:i + _BLOCK]
        sims = block @ mat.T                      # (block, N)
        # mask the diagonal (self) for this block
        for r in range(block.shape[0]):
            sims[r, i + r] = -1.0
        out[i:i + block.shape[0]] = sims.max(axis=1)
    return out


def near_duplicate_mass(
    mat: np.ndarray, threshold: float, *, assume_normalized: bool = True,
) -> float:
    """Fraction of items that have at least one near-duplicate (cosine >=
    threshold) elsewhere in the set.  The corpus-redundancy scalar."""
    if mat.shape[0] < 2:
        return 0.0
    nn = nearest_within(mat, assume_normalized=assume_normalized)
    return float(np.mean(nn >= threshold))


def auc_separation(pos: np.ndarray, neg: np.ndarray) -> float:
    """Mann–Whitney U effect size: P(a random ``pos`` score > a random ``neg``
    score), with ties counted as 0.5.  1.0 = perfectly separable, 0.5 = none.
    Feed it any scalar per-item feature (e.g. distance-to-centroid)."""
    pos = np.asarray(pos, float); neg = np.asarray(neg, float)
    if pos.size == 0 or neg.size == 0:
        return float("nan")
    ns = np.sort(neg)
    lt = np.searchsorted(ns, pos, side="left")
    eq = np.searchsorted(ns, pos, side="right") - lt
    return float(np.mean((lt + 0.5 * eq) / ns.size))
