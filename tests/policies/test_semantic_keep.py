"""
Tests for SemanticKeepSimilarPolicy — leave-one-out centroid correctness at
n=2 and n=3 (the §4 rerun-prep assert), the mirror-of-redundancy behavior,
candidate filtering, and the content-agnostic fallback without vectors.
"""
from __future__ import annotations

from forgetit.core.schema import MemFeatures, MemRecord
from forgetit.policies.semantic import SemanticKeepSimilarPolicy, SemanticRedundancyPolicy


class FakeStore:
    """Provides both iter_items (embeddings) and iter_features (fallback)."""

    def __init__(self, *records: MemRecord):
        self._records = list(records)

    def iter_items(self):
        return iter(self._records)

    def iter_features(self):
        for r in self._records:
            yield MemFeatures(id=r.id, bytes=r.bytes, last_access=r.last_access,
                              access_count=r.access_count, created_at=r.created_at)


def _rec(id_: str, embedding, nbytes: int = 100, created_at: float = 0.0) -> MemRecord:
    return MemRecord(id=id_, text="t", embedding=embedding, bytes=nbytes,
                     created_at=created_at)


class TestLeaveOneOutCentroid:

    def test_n3_outlier_evicted(self):
        # Two aligned vectors and one orthogonal: LOO similarity is
        # cos(e_i, c_{-i}) = 1/√2 for the aligned pair, 0 for the outlier.
        store = FakeStore(
            _rec("a", [1.0, 0.0]),
            _rec("b", [1.0, 0.0]),
            _rec("outlier", [0.0, 1.0]),
        )
        assert SemanticKeepSimilarPolicy().select_victim(store) == "outlier"

    def test_n2_symmetric_tie_evicts_larger(self):
        # With two orthogonal vectors both LOO similarities are 0; the
        # tiebreak sheds the LARGER item (cheaper to keep small outliers).
        store = FakeStore(
            _rec("small", [1.0, 0.0], nbytes=100),
            _rec("large", [0.0, 1.0], nbytes=300),
        )
        assert SemanticKeepSimilarPolicy().select_victim(store) == "large"

    def test_mirror_of_redundancy(self):
        # The same store, opposite victims: a near-twin pair pulls the
        # centroid toward itself, so keep_similar sheds the off-axis item
        # while redundancy sheds one of the twins.
        store = FakeStore(
            _rec("twin1", [1.0, 0.0]),
            _rec("twin2", [0.999, 0.045]),
            _rec("off_axis", [0.5, 0.866]),
        )
        assert SemanticKeepSimilarPolicy().select_victim(store) == "off_axis"
        assert SemanticRedundancyPolicy().select_victim(store) in {"twin1", "twin2"}

    def test_candidates_filter(self):
        store = FakeStore(
            _rec("a", [1.0, 0.0]),
            _rec("b", [1.0, 0.0]),
            _rec("outlier", [0.0, 1.0]),
        )
        victim = SemanticKeepSimilarPolicy().select_victim(store, candidates={"a", "b"})
        assert victim in {"a", "b"}

    def test_records_decision_provenance(self):
        policy = SemanticKeepSimilarPolicy()
        store = FakeStore(
            _rec("a", [1.0, 0.0]), _rec("b", [1.0, 0.0]), _rec("outlier", [0.0, 1.0]),
        )
        policy.select_victim(store)
        assert policy.last_decision is not None
        assert policy.last_decision["candidate_count"] == 3
        assert policy.last_decision["policy_score"] < 0.1   # outlier's ~0 cosine


class TestFallback:

    def test_no_embeddings_falls_back_to_evict_largest(self):
        store = FakeStore(
            _rec("small", None, nbytes=50),
            _rec("large", None, nbytes=500),
        )
        assert SemanticKeepSimilarPolicy().select_victim(store) == "large"

    def test_single_embedded_resident_falls_back(self):
        store = FakeStore(
            _rec("embedded", [1.0, 0.0], nbytes=50),
            _rec("large_plain", None, nbytes=500),
        )
        assert SemanticKeepSimilarPolicy().select_victim(store) == "large_plain"

    def test_declares_needs_embeddings(self):
        assert SemanticKeepSimilarPolicy.needs_embeddings is True
