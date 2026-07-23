"""D2 composition-invariant metric primitives (metrics.py)."""
import math

import pytest

from forgetit.benchmark.metrics import (
    gold_rank_percentile, kprop, composition_columns,
)


def test_percentile_basic():
    assert gold_rank_percentile([3], 100) == 0.03
    assert gold_rank_percentile([7, 2], 50) == 2 / 50     # best gold
    assert gold_rank_percentile([1], 1) == 1.0            # sole resident


def test_percentile_undefined_cases():
    assert math.isnan(gold_rank_percentile([], 100))      # censored/evicted
    assert math.isnan(gold_rank_percentile([3], 0))       # unknown denominator


def test_percentile_is_composition_invariant():
    """Same relative position, different store sizes → same percentile.
    This is the property fixed-k lacks: rank 5 of 50 == rank 50 of 500."""
    assert gold_rank_percentile([5], 50) == gold_rank_percentile([50], 500)


def test_kprop_floor_and_scaling():
    c = 5 / 463                          # the D2 calibration
    assert kprop(463, c) == 5            # median store → floor
    assert kprop(51, c) == 5             # small store → floor holds
    assert kprop(926, c) == 10           # 2× median → 2× k
    assert kprop(0, c) == 5              # unknown → floor


def test_composition_columns_censoring_semantics():
    obs = composition_columns([2], 100, introspectable=True, gold_survived=True)
    assert obs["gold_rank_censored"] == 0.0
    assert obs["gold_rank_percentile"] == 0.02
    assert obs["n_resident_at_query"] == 100

    cen = composition_columns([], 100, introspectable=True, gold_survived=True)
    assert cen["gold_rank_censored"] == 1.0
    assert math.isnan(cen["gold_rank_percentile"])

    evicted = composition_columns([], 100, introspectable=True, gold_survived=False)
    assert math.isnan(evicted["gold_rank_censored"])

    opaque = composition_columns([1], 0, introspectable=False, gold_survived=False)
    assert math.isnan(opaque["gold_rank_censored"])
    assert math.isnan(opaque["gold_rank_percentile"])
