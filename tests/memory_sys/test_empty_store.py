"""E2b empty-store control: the floor store retains nothing, and the runner
produces honestly-zero, unmistakably-labeled rows from it."""
from forgetit.benchmark.runner_core import run_one_example
from forgetit.benchmark.schema import WorkloadEvent
from forgetit.core.schema import MemRecord, Query
from forgetit.memory_system import EmptyStore


def test_empty_store_contract():
    store = EmptyStore()
    store.insert(MemRecord(id="a", text="hello world", created_at=1.0, bytes=11))
    assert store.get_used_bytes() == 0
    assert store.get("a") is None
    assert list(store.iter_ids()) == []
    q = Query(id="q", text="hello", timestamp=2.0, meta={})
    assert store.retrieve(q, k=5) == []
    store.clear()  # no-op, must not raise


def test_run_one_example_floor_rows():
    events = [
        WorkloadEvent(type="insert", timestamp=1.0,
                      item=MemRecord(id="g1", text="the answer is 42", created_at=1.0, bytes=16),
                      query=None, gold_item_ids=[]),
        WorkloadEvent(type="query", timestamp=2.0, item=None,
                      query=Query(id="q1", text="what is the answer?", timestamp=2.0, meta={}),
                      gold_item_ids=["g1"], ref_answer="42", question_type="single-session-user"),
    ]
    results, ctx, _, _ = run_one_example(
        events, EmptyStore(),
        k=5, dataset="test", store_label="floor/empty_store",
        policy="empty_store_floor", retriever_name="none",
        collect_reader_context=True, example_id="ex0",
    )
    (row,) = results
    assert row.policy == "empty_store_floor"
    assert row.store == "floor/empty_store"
    assert row.introspectable is True          # zeros are real, not sentinels
    assert row.gold_id_retained is False
    assert row.hit_at_k == 0.0
    assert row.n_retrieved == 0
    assert row.used_bytes_end == 0
    # Reader context recorded with ZERO evidence — Pass 2 answers evidence-free.
    (ctx_row,) = ctx
    _, _, retrieved, ref, qtype = ctx_row
    assert retrieved == [] and ref == "42"
