"""A5: seeded replicates must collapse to one aggregation identity.

The stochastic control runs ten seeds. Left as ten distinct policy names they
occupy ten of the roster's twenty-three entries, so every pooled per-policy
average, mean rank and rank matrix gives the blind baseline roughly 43% of the
weight and pulls the ranking toward it.

The counter-requirement, and the reason this is not a blanket rename: each seed
is a separate deterministic run, so a bound violation by ANY seed is a real
counterexample. The raggedness gate needs the same resolution, because a merge
where one seed covers forty cells and another ninety-six is exactly the unequal
support it exists to catch, and the collapsed view hides it behind the union.
"""
from __future__ import annotations

import pytest

from forgetit.benchmark.metrics import (
    canonical_policy, coupled_identity, grid_raggedness,
)

ROSTER = ["fifo", "lru", "lfu", "gdsf", "gdsf_frozen", "evict_smallest",
          "keep_smallest", "semantic", "semantic_coverage", "semantic_keep",
          "salience_decay", "ebbinghaus", "compression", "oracle", "floor",
          "memorybank", "density"]
SEEDS = [f"random_s{n}" for n in range(42, 52)]


class TestCanonicalPolicy:

    @pytest.mark.parametrize("seeded", SEEDS)
    def test_seed_suffix_is_stripped(self, seeded):
        assert canonical_policy(seeded) == "random"

    @pytest.mark.parametrize("name", ROSTER)
    def test_no_roster_name_is_damaged(self, name):
        """The rule is a suffix match on _s<digits>. If a future policy ends
        that way this test fails before the ranking silently changes."""
        assert canonical_policy(name) == name

    def test_unseeded_random_is_unchanged(self):
        assert canonical_policy("random") == "random"

    def test_only_a_trailing_seed_counts(self):
        assert canonical_policy("random_s47_variant") == "random_s47_variant"
        assert canonical_policy("semantic_s2coverage") == "semantic_s2coverage"

    def test_handles_empty_and_whitespace(self):
        assert canonical_policy("") == ""
        assert canonical_policy("  random_s7  ") == "random"


class TestCoupledIdentity:

    def test_seeds_collapse_by_default(self):
        assert {coupled_identity(s, "bm25") for s in SEEDS} == {"random"}

    def test_seeds_are_preserved_when_asked(self):
        assert len({coupled_identity(s, "bm25", collapse_seeds=False)
                    for s in SEEDS}) == len(SEEDS)

    def test_history_policies_still_couple_to_the_retriever(self):
        assert coupled_identity("lru", "bm25") == "lru×bm25"
        assert coupled_identity("lru", "dense") != coupled_identity("lru", "bm25")

    def test_random_stays_retriever_independent(self):
        """random forms no state through the ingest retriever, so collapsing
        seeds must not accidentally couple it."""
        assert coupled_identity("random_s42", "bm25") == \
            coupled_identity("random_s42", "dense")

    def test_roster_weight_after_collapsing(self):
        """The point of the exercise: ten of twenty-seven identities were one
        policy."""
        all_names = SEEDS + [p for p in ROSTER if p != "floor"]
        raw = {coupled_identity(p, "bm25", collapse_seeds=False) for p in all_names}
        collapsed = {coupled_identity(p, "bm25") for p in all_names}
        assert len(raw) - len(collapsed) == len(SEEDS) - 1


class TestRaggednessKeepsSeedResolution:
    """Why the gate opts out of collapsing."""

    def test_uneven_seed_coverage_is_visible_uncollapsed(self):
        cells = {("locomo", "turn", b) for b in ("025", "050", "075")}
        by_ident = {
            coupled_identity("random_s42", "bm25", collapse_seeds=False): set(cells),
            coupled_identity("random_s43", "bm25", collapse_seeds=False): {
                ("locomo", "turn", "025")},
            coupled_identity("fifo", "bm25", collapse_seeds=False): set(cells),
        }
        warnings = grid_raggedness(by_ident)
        assert any("random_s43" in w for w in warnings), (
            "a seed covering part of the grid must still be flagged")

    def test_uneven_seed_coverage_is_hidden_when_collapsed(self):
        """Documents the trade-off rather than asserting a preference: the
        collapsed identity's union covers everything, so the gap vanishes.
        This is why concat_results passes collapse_seeds=False."""
        cells = {("locomo", "turn", b) for b in ("025", "050", "075")}
        by_ident = {
            coupled_identity("random_s42", "bm25"): set(cells),
            coupled_identity("fifo", "bm25"): set(cells),
        }
        assert grid_raggedness(by_ident) == []