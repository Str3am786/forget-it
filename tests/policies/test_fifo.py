"""
Tests for FIFOPolicy — evict the oldest created_at.

Same FakeStore pattern as test_lru.py. The tie test pins the documented
semantics: strict < keeps the FIRST-encountered item among equal timestamps,
which is insertion order on the in-memory backend (true FIFO tiebreak).
"""
from __future__ import annotations

import pytest

from forgetit.core.schema import MemFeatures
from forgetit.policies import FIFOPolicy
from forgetit.policies.actions import Evict


def _feat(id_: str, created_at: float | None) -> MemFeatures:
    return MemFeatures(id=id_, bytes=100, last_access=0.0, access_count=0,
                       created_at=created_at)


class FakeStore:
    def __init__(self, *features: MemFeatures):
        self._features = list(features)

    def iter_features(self):
        return iter(self._features)


class TestFIFOSelection:

    def test_evicts_oldest_created_at(self):
        store = FakeStore(_feat("b", 20.0), _feat("a", 10.0), _feat("c", 30.0))
        assert FIFOPolicy().select_victim(store) == "a"

    def test_ignores_access_signal(self):
        # FIFO ranks purely by age — a heavily-accessed old item still goes.
        old_hot = MemFeatures(id="old_hot", bytes=100, last_access=99.0,
                              access_count=50, created_at=10.0)
        store = FakeStore(old_hot, _feat("new_cold", 20.0))
        assert FIFOPolicy().select_victim(store) == "old_hot"

    def test_tie_keeps_first_encountered(self):
        # Equal timestamps → strict < never replaces → first in iteration
        # order (insertion order on the in-memory backend) is the victim.
        store = FakeStore(_feat("inserted_first", 10.0), _feat("inserted_second", 10.0))
        assert FIFOPolicy().select_victim(store) == "inserted_first"

    def test_none_created_at_treated_as_epoch(self):
        # Regression: None used to crash on `None < float`. It now ranks as
        # 0.0 (infinitely old), matching LRU/LFU's fallback.
        store = FakeStore(_feat("dated", 10.0), _feat("undated", None))
        assert FIFOPolicy().select_victim(store) == "undated"

    def test_candidates_filter_restricts_victim_set(self):
        store = FakeStore(_feat("oldest", 10.0), _feat("newer", 20.0))
        assert FIFOPolicy().select_victim(store, candidates={"newer"}) == "newer"

    def test_empty_store_raises(self):
        with pytest.raises(RuntimeError):
            FIFOPolicy().select_victim(FakeStore())

    def test_select_action_wraps_victim_in_evict(self):
        store = FakeStore(_feat("old", 10.0), _feat("new", 20.0))
        assert FIFOPolicy().select_action(store) == Evict(id="old")
