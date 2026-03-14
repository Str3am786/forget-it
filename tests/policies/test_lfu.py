from dataclasses import dataclass
from unittest import TestCase

from forgetit.policies import LRUPolicy, LFUPolicy


@dataclass
class FakeFeature:
    id: str
    access_count: int = 0
    last_access: float = 0.0
    created_at: float = 0.0


class FakeStore:
    def __init__(self, *features):
        self._features = list(features)

    def iter_features(self):
        return iter(self._features)


class TestPolicySelection(TestCase):

    def test_lfu_selects_lowest_access_count(self):
        store = FakeStore(
            FakeFeature(id="A", access_count=5, last_access=50.0, created_at=1.0),
            FakeFeature(id="B", access_count=1, last_access=100.0, created_at=2.0),
            FakeFeature(id="C", access_count=2, last_access=91.0, created_at=3.0),
        )

        victim = LFUPolicy().select_victim(store)

        self.assertEqual(victim, "B")

    def test_lfu_tie_breaks_by_last_access_then_created_at_then_id(self):
        store = FakeStore(
            FakeFeature(id="A", access_count=1, last_access=20.0, created_at=1.0),
            FakeFeature(id="B", access_count=1, last_access=10.0, created_at=2.0),
        )

        victim = LFUPolicy().select_victim(store)

        self.assertEqual(victim, "B")