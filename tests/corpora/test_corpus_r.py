"""corpus_r — retrieval fixtures (deterministic subset: bm25/lexical classes).

Dense-retriever fixtures (paraphrase_needle_dense) need the embedding endpoint
and run via scripts/run_corpus_r_dense.py; they are skipped here by class.
Needle classes are a 100% bar (VERIFICATION.md §7).
"""
from __future__ import annotations

import pytest

from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.schema import MemRecord, Query
from forgetit.memory_system import RetentionManager
from forgetit.policies import ScriptedEvictionPolicy
from forgetit.retrieval import BM25Retriever

from tests.corpora.harness import load_fixtures

_FIXTURES = {fx["id"]: fx for fx in load_fixtures("corpus_r")}
_DENSE_CLASSES = {"paraphrase_needle_dense"}


def _records(docs: dict[str, str]) -> dict[str, MemRecord]:
    return {k: MemRecord(id=k, text=t, created_at=float(i), bytes=len(t))
            for i, (k, t) in enumerate(docs.items(), 1)}


def _rank_of(results: list[MemRecord], doc_id: str) -> int | None:
    ids = [r.id for r in results]
    return ids.index(doc_id) + 1 if doc_id in ids else None


@pytest.mark.parametrize("fixture_id", sorted(_FIXTURES))
def test_corpus_r_fixture(fixture_id):
    fx = _FIXTURES[fixture_id]
    if fx["class"] in _DENSE_CLASSES:
        pytest.skip("dense fixture — needs a real embedder; not runnable in the offline unit suite")
    if isinstance(fx["input"].get("docs"), str):
        # r004 exemplar ships docs as a scaffold INSTRUCTION ("agent fills…"),
        # not data. The frozen exemplar file is not edited (rule 5); the class
        # is materialized concretely by bulk fixtures r033–r041.
        pytest.skip("scaffold placeholder docs — class covered by bulk fixtures")
    inp = fx["input"]
    expected = fx["expected"]
    k = int(inp["k"])
    retriever = BM25Retriever()

    if fx["class"] == "post_eviction_ghost":
        class _L:
            def event(self, *a, **k): pass
        store = RetentionManager(budget_bytes=1 << 62,
                                 backend=InMemoryBackend(retriever=retriever),
                                 policy=ScriptedEvictionPolicy(), logger=_L())
        store.connect()
        for op in inp["setup"]:
            if op["op"] == "insert":
                store.insert(MemRecord(id=op["item"], text=op["text"],
                                       created_at=1.0, bytes=len(op["text"])))
            elif op["op"] == "evict_scripted":
                store.force_evict(op["item"])
            else:
                raise ValueError(op["op"])
        results = store.retrieve(Query(id="q", text=inp["query"], timestamp=2.0,
                                       meta={}), k=k)
        for doc_id, rule in expected["results_containing"].items():
            assert rule == "never"
            assert doc_id not in [r.id for r in results], (
                f"{fixture_id}: GHOST HIT — evicted {doc_id} returned by retrieval"
            )
        return

    items = _records(inp["docs"])
    results = retriever.topk(items, Query(id="q", text=inp["query"], timestamp=1.0,
                                          meta={}), k)

    if "rank_of" in expected:
        for doc_id, want in expected["rank_of"].items():
            got = _rank_of(results, doc_id)
            if isinstance(want, dict):
                assert got is not None and got <= int(want["lte"]), (
                    f"{fixture_id}: {doc_id} rank {got} > lte {want['lte']}")
            else:
                assert got == int(want), f"{fixture_id}: {doc_id} rank {got} != {want}"
    if "order" in expected:
        assert [r.id for r in results] == list(expected["order"]), (
            f"{fixture_id}: order {[r.id for r in results]} != {expected['order']}")
    if "max_gold_in_topk" in expected:
        gold = set(inp["gold_ids"])
        in_top = sum(1 for r in results if r.id in gold)
        assert in_top == int(expected["max_gold_in_topk"]), (
            f"{fixture_id}: {in_top} gold in top-{k}, expected {expected['max_gold_in_topk']}")
        composition_bound = len(gold) > k
        assert composition_bound == bool(expected["composition_bound_flag"]), (
            f"{fixture_id}: composition_bound accounting wrong")
