"""Semantic (content-aware) eviction — the embedding counterpart to the size
and recency baselines. TWO policies, kept side by side because the comparison
between them is itself a result (cheap proxy vs principled coverage):

  * SemanticRedundancyPolicy ("semantic") — NEAREST-PAIR redundancy. Evicts the
    item most a near-twin of *some single other resident* (max nearest-neighbour
    cosine). Cheap, O(N^2), the honest baseline.

  * SemanticCoveragePolicy ("semantic_coverage") — MARGINAL set-coverage. Evicts
    the item whose removal costs the kept set the least *total* semantic coverage,
    where coverage = the summed nearest-neighbour similarity of the residents
    (the unsupervised, self-contained stand-in for query-coverage). This is
    submodular max-coverage / facility-location machinery; greedy-by-marginal is
    the textbook 1-1/e algorithm — so it's principled and citable, but NOT novel
    as an algorithm. What's novel is evaluating it as a forgetting policy on
    buried-gold benchmarks against the oracle — the harness's job, not the
    policy's. Keeping the cheap nearest-pair beside it gives the ablation: does
    principled coverage actually buy survival/answer-quality over the proxy here?

Why both, and why greedy-recompute is mandatory (not an optimization to trade
away): for submodular coverage, greedy-recompute and score-once-and-trim give
DIFFERENT answers. The marginal value of dropping the second twin changes once
the first is gone; the eviction loop (one victim per select_victim call,
recomputed on the shrunk set) is exactly that recompute, and it's what preserves
the guarantee. Score-once double-counts redundancy and over-evicts clusters. The
O(N^2 * E) cost is the price of correctness; if it ever bites, the fix is
lazy-greedy, not score-once.

Both need item vectors (dense configs). With < 2 embedded residents they fall
back to the content-agnostic size rule (evict largest), keeping progress on
non-dense runs. Unsupervised (nearest-neighbour) coverage is built first because
it's self-contained; the query-coverage-weighted form waits on the query proxy.
"""
from __future__ import annotations

from forgetit.policies.base import SimpleEvictionPolicy


def _evict_largest(store, candidates: set[str] | None, policy=None):
    """Content-agnostic fallback shared by both semantic policies: evict the
    largest item (the keep_smallest rule). Ties: largest, oldest, id."""
    victim_id = None
    best = None
    n = 0
    for it in store.iter_features():
        if candidates is not None and it.id not in candidates:
            continue
        n += 1
        created = it.created_at if it.created_at is not None else 0.0
        key = (-it.bytes, created, it.id)
        if best is None or key < best:
            best = key
            victim_id = it.id
    if victim_id is None:
        raise RuntimeError("semantic: store empty but eviction requested.")
    if policy is not None:
        policy._note_decision(best[0], n)
    return victim_id


def _embedded_residents(store):
    """(ids, vectors, sizes) for residents carrying an embedding, in store order."""
    ids: list[str] = []
    vecs: list[list[float]] = []
    sizes: list[int] = []
    for it in store.iter_items():
        if it.embedding is None:
            continue
        ids.append(it.id)
        vecs.append(it.embedding)
        sizes.append(it.bytes)
    return ids, vecs, sizes


def _cosine_matrix(vecs):
    """N×N cosine matrix (vectors L2-normalised defensively)."""
    import numpy as np
    mat = np.asarray(vecs, dtype=np.float32)
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    mat = mat / norms
    return mat @ mat.T


class SemanticRedundancyPolicy(SimpleEvictionPolicy):
    """Nearest-pair redundancy: evict max nearest-neighbour cosine (the item most
    duplicated by some single other resident). ``size_weighted`` multiplies the
    redundancy by bytes so it sheds redundant-AND-large items first."""
    name = "semantic"
    needs_embeddings = True   # reads item vectors → store must carry the embedder,
                              # even when retrieval is lexical/BM25 (not just --dense)

    def __init__(self, *, size_weighted: bool = False) -> None:
        self.size_weighted = size_weighted
        if size_weighted:
            self.name = "semantic_size"

    def select_victim(self, store, candidates: set[str] | None = None):
        ids, vecs, sizes = _embedded_residents(store)
        if len(ids) < 2:
            return _evict_largest(store, candidates, policy=self)

        from forgetit.embeddings.similarity import nearest_within
        import numpy as np
        redundancy = nearest_within(np.asarray(vecs, dtype=np.float32))  # max cosine to another

        victim_id = None
        best = None
        n = 0
        for i, rid in enumerate(ids):
            if candidates is not None and rid not in candidates:
                continue
            n += 1
            red = float(redundancy[i])
            score = red * sizes[i] if self.size_weighted else red
            key = (score, sizes[i], rid)        # maximise: most redundant, then larger, then id
            if best is None or key > best:
                best = key
                victim_id = rid
        if victim_id is not None:
            self._note_decision(best[0], n)
        return victim_id if victim_id is not None else _evict_largest(store, candidates, policy=self)


class SemanticCoveragePolicy(SimpleEvictionPolicy):
    """Marginal coverage: evict the item whose removal least reduces the kept
    set's summed nearest-neighbour coverage.

    coverage(S) = sum_j nn_S(j), nn_S(j) = max cosine of j to any OTHER resident.
    marginal_i = coverage(S) - coverage(S \\ i)
               = nn(i) + sum_{j: i is j's nearest} (nn(j) - secondnn(j)),
    i.e. i's own covered value PLUS the exclusive coverage i provides to the items
    that rely on it as their nearest neighbour. Evicting argmin marginal removes
    the resident that contributes the least total coverage — the broadly-redundant
    item, not merely a near-twin of one other. Greedy-recompute happens naturally:
    the budget loop re-scores the shrunk set on the next call.

    Differs from nearest-pair exactly where the user flagged: an item moderately
    close to MANY residents (twin of none) has low marginal coverage here (its
    mass is well-covered by the set) and is evicted, where nearest-pair would keep
    it; and an item with one close neighbour that others rely on keeps a high
    marginal and survives.
    """
    name = "semantic_coverage"
    needs_embeddings = True   # reads item vectors → store must carry the embedder,
                              # even when retrieval is lexical/BM25 (not just --dense)

    def select_victim(self, store, candidates: set[str] | None = None):
        ids, vecs, _sizes = _embedded_residents(store)
        if len(ids) < 2:
            return _evict_largest(store, candidates, policy=self)

        import numpy as np
        n = len(ids)
        mat = _cosine_matrix(vecs)
        np.fill_diagonal(mat, -np.inf)              # coverage is by OTHER items

        argnn = np.argmax(mat, axis=1)              # each item's nearest other
        nn = mat[np.arange(n), argnn]
        masked = mat.copy()
        masked[np.arange(n), argnn] = -np.inf
        secondnn = np.max(masked, axis=1)
        # clamp "no neighbour" / negative coverage to 0 (uncovered == 0 coverage)
        nn = np.clip(np.where(np.isfinite(nn), nn, 0.0), 0.0, None)
        secondnn = np.clip(np.where(np.isfinite(secondnn), secondnn, 0.0), 0.0, None)

        # marginal_i = nn(i) + sum over items that rely on i of their exclusive gain
        marginal = nn.astype(float).copy()
        gain = nn - secondnn                        # exclusive coverage each item gets from its nearest
        np.add.at(marginal, argnn, gain)

        victim_id = None
        best = None
        n = 0
        for i, rid in enumerate(ids):
            if candidates is not None and rid not in candidates:
                continue
            n += 1
            key = (float(marginal[i]), rid)         # minimise marginal, tie by id
            if best is None or key < best:
                best = key
                victim_id = rid
        if victim_id is not None:
            self._note_decision(best[0], n)
        return victim_id if victim_id is not None else _evict_largest(store, candidates, policy=self)
