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
    def test_lru_selects_oldest_last_access(self):
        store = FakeStore(
            FakeFeature(id="A", access_count=5, last_access=50.0, created_at=1.0),
            FakeFeature(id="B", access_count=1, last_access=100.0, created_at=2.0),
            FakeFeature(id="C", access_count=2, last_access=91.0, created_at=3.0),
        )

        victim = LRUPolicy().select_victim(store)

        self.assertEqual(victim, "A")


    def test_lru_tie_breaks_by_created_at_then_id(self):
        store = FakeStore(
            FakeFeature(id="B", access_count=1, last_access=10.0, created_at=2.0),
            FakeFeature(id="A", access_count=1, last_access=10.0, created_at=1.0),
        )

        victim = LRUPolicy().select_victim(store)

        self.assertEqual(victim, "A")
