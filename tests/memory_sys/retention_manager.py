from unittest import TestCase

from forgetit.policies import LRUPolicy, LFUPolicy
from forgetit.memory_system.store import RetentionManager
from forgetit.retrieval.lexical import LexicalOverlapRetriever
from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.schema import MemRecord, Query


class DummyLogger:
    def event(self, *args, **kwargs):
        pass


class RetentionManagerTestMixin:
    POLICY = None

    def make_store(self, budget_bytes=30):
        backend = InMemoryBackend(retriever=LexicalOverlapRetriever())
        store = RetentionManager(
            budget_bytes=budget_bytes,
            backend=backend,
            policy=self.POLICY(),
            logger=DummyLogger(),
        )
        store.connect()
        return store

    def make_record(self, item_id, text, created_at, bytes_=10):
        return MemRecord(
            id=item_id,
            text=text,
            created_at=created_at,
            bytes=bytes_,
        )

    def touch(self, store, qid, text, timestamp):
        return store.retrieve(
            Query(
                id=qid,
                text=text,
                timestamp=timestamp,
            ),
            k=1,
        )


class TestLRUIntegration(RetentionManagerTestMixin, TestCase):
    POLICY = LRUPolicy

    def test_insert_over_budget_triggers_eviction(self):
        store = self.make_store(budget_bytes=20)

        store.insert(self.make_record("A", "alpha", created_at=1.0, bytes_=10))
        store.insert(self.make_record("B", "beta", created_at=2.0, bytes_=10))
        store.insert(self.make_record("C", "gamma", created_at=3.0, bytes_=10))

        self.assertEqual(set(store.iter_ids()), {"B", "C"})
        self.assertEqual(store.used_bytes, 20)

    def test_lru_eviction_prefers_oldest_last_access(self):
        store = self.make_store(budget_bytes=30)

        store.insert(self.make_record("A", "alpha", created_at=1.0))
        store.insert(self.make_record("B", "beta", created_at=2.0))
        store.insert(self.make_record("C", "gamma", created_at=3.0))

        # A: 5 accesses, but not recent
        self.touch(store, "q1", "alpha", 10.0)
        self.touch(store, "q2", "alpha", 20.0)
        self.touch(store, "q3", "alpha", 30.0)
        self.touch(store, "q4", "alpha", 40.0)
        self.touch(store, "q5", "alpha", 50.0)

        # C: 2 accesses, fairly recent
        self.touch(store, "q6", "gamma", 90.0)
        self.touch(store, "q7", "gamma", 91.0)

        # B: 1 access, most recent
        self.touch(store, "q8", "beta", 100.0)

        # Force eviction by inserting a 4th 10-byte item into a 30-byte store
        store.insert(self.make_record("D", "delta", created_at=101.0))

        # LRU should evict A because its last_access = 50 is the oldest
        self.assertEqual(set(store.iter_ids()), {"B", "C", "D"})
        self.assertIsNone(store.get("A"))
        self.assertEqual(store.used_bytes, 30)


class TestLFUIntegration(RetentionManagerTestMixin, TestCase):
    POLICY = LFUPolicy

    def test_lfu_eviction_prefers_lowest_access_count(self):
        store = self.make_store(budget_bytes=30)

        store.insert(self.make_record("A", "alpha", created_at=1.0))
        store.insert(self.make_record("B", "beta", created_at=2.0))
        store.insert(self.make_record("C", "gamma", created_at=3.0))

        # A: 5 accesses
        self.touch(store, "q1", "alpha", 10.0)
        self.touch(store, "q2", "alpha", 20.0)
        self.touch(store, "q3", "alpha", 30.0)
        self.touch(store, "q4", "alpha", 40.0)
        self.touch(store, "q5", "alpha", 50.0)

        # C: 2 accesses
        self.touch(store, "q6", "gamma", 90.0)
        self.touch(store, "q7", "gamma", 91.0)

        # B: 1 access
        self.touch(store, "q8", "beta", 100.0)

        store.insert(self.make_record("D", "delta", created_at=101.0))

        # LFU should evict B because it has the lowest access_count
        self.assertEqual(set(store.iter_ids()), {"A", "C", "D"})
        self.assertIsNone(store.get("B"))
        self.assertEqual(store.used_bytes, 30)