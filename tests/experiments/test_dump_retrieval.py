"""`--dump-retrieval`: one policy run, readable afterwards by any number of models.

`build_context()` is LLM-free, so what a reader WOULD see can be persisted at
zero inference cost — including under `--no-reader`. These tests pin that the
artefact is faithful enough to replay from: every key resolves, rank survives
the round trip, and rebuilding from the artefact reproduces exactly what the
live reader was handed.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from forgetit.backend.in_memory import InMemoryBackend
from forgetit.benchmark.pipeline import (
    RetrievalDump, _append_evidence_items, _config_hash, _item_key,
    run_pipeline_concurrent,
)
from forgetit.benchmark.schema import WorkloadEvent
from forgetit.core.logging import BenchmarkLogger
from forgetit.core.schema import MemRecord, Query
from forgetit.llm.readers import (
    DEFAULT_PROMPT, DEFAULT_TOKEN_BUDGET, PlainTextReader, build_context,
    prompt_sha,
)
from forgetit.memory_system.custom_manager import RetentionManager
from forgetit.policies import LFUPolicy, LLMCompressionPolicy
from forgetit.policies.baselines import FIFOPolicy
from forgetit.retrieval.lexical import LexicalOverlapRetriever

_BODY = ("alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu "
         "nu xi omicron pi rho sigma tau upsilon phi chi psi omega ")


class StubLLM:
    """Never a real endpoint. Any construction of a real client fails the test."""
    model = "stub-model"

    def generate(self, prompt, **kwargs):
        # Item-specific, so two compressed items do not collapse to one key and
        # mask the property under test.
        marker = next((w for w in prompt.split() if w.startswith("item")), "x")
        return f"summary of {marker}."


class TinyWorkload:
    """2 examples x 4 inserts + 1 scored query each."""

    def iter_examples(self):
        for ex in range(2):
            evs = []
            for i in range(4):
                text = f"ex{ex} item{i} " + _BODY
                evs.append(WorkloadEvent(
                    type="insert", timestamp=float(i + 1),
                    item=MemRecord(id=f"e{ex}i{i}", text=text,
                                   bytes=len(text.encode()), created_at=float(i + 1)),
                    query=None, gold_item_ids=[]))
            evs.append(WorkloadEvent(
                type="query", timestamp=10.0, item=None,
                query=Query(text=f"ex{ex} item1 alpha beta", id=f"e{ex}q0",
                            timestamp=10.0),
                gold_item_ids=[f"e{ex}i1"]))
            yield f"ex{ex}", evs


class RewriteWorkload:
    """One example that queries BEFORE and AFTER the compressor rewrites text.

    The first query captures an item's original text; the budget is then blown
    so compression rewrites it in place under the SAME id; the second query
    captures the rewritten text. That is the (id, text) collision the item table
    has to keep distinct.
    """

    def iter_examples(self):
        def _insert(i):
            text = f"item{i} " + _BODY
            return WorkloadEvent(
                type="insert", timestamp=float(i + 1),
                item=MemRecord(id=f"r{i}", text=text, bytes=len(text.encode()),
                               created_at=float(i + 1)),
                query=None, gold_item_ids=[])

        # One item only: comfortably under budget, so nothing is rewritten yet
        # and this query captures r0's ORIGINAL text.
        evs = [_insert(0)]
        evs.append(WorkloadEvent(
            type="query", timestamp=5.0, item=None,
            query=Query(text=_BODY, id="q_before", timestamp=5.0),
            gold_item_ids=["r0"]))
        # Now blow the budget. A FIFO selector picks the oldest resident — r0 —
        # so the item this workload queries twice is the one that gets rewritten.
        evs += [_insert(i) for i in range(1, 5)]
        evs.append(WorkloadEvent(
            type="query", timestamp=20.0, item=None,
            query=Query(text=_BODY, id="q_after", timestamp=20.0),
            gold_item_ids=["r0"]))
        yield "rewrite", evs


def _plain_store():
    return RetentionManager(
        budget_bytes=1 << 40,                     # no eviction: keep it simple
        backend=InMemoryBackend(retriever=LexicalOverlapRetriever()),
        policy=FIFOPolicy(), logger=BenchmarkLogger())


def _compress_store():
    return RetentionManager(
        budget_bytes=400,
        backend=InMemoryBackend(retriever=LexicalOverlapRetriever()),
        policy=LLMCompressionPolicy(StubLLM(), selector=FIFOPolicy(),
                                    min_compress_bytes=50, min_saving_ratio=0.3),
        logger=BenchmarkLogger())


def _run(tmp_path, *, store_label, reader_impl=None, workload=None,
         store_factory=None, shard=0, policy="fifo"):
    return run_pipeline_concurrent(
        dataset="dumpsmoke", workload=workload or TinyWorkload(),
        store_factory=store_factory or _plain_store,
        store_label=store_label, k=5, seed=0, itemiser="fixed",
        reader="none" if reader_impl is None else reader_impl.name,
        reader_impl=reader_impl, budget_bytes=300, policy_label=policy,
        retriever_label="lexical", out_dir=str(tmp_path),
        retrieval_dump=RetrievalDump(shard=shard,
                                     token_budget=DEFAULT_TOKEN_BUDGET,
                                     prompt_sha=prompt_sha(DEFAULT_PROMPT)),
    )


def _read_jsonl(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as f:
        return [json.loads(l) for l in f if l.strip()]


def _indexes(tmp_path) -> list[Path]:
    return sorted(tmp_path.rglob("retrieved_index.jsonl"))


def _item_table(tmp_path) -> dict[str, dict]:
    items = {}
    for f in sorted(tmp_path.glob("evidence_items*.jsonl")):
        for r in _read_jsonl(f):
            items[r["key"]] = r
    return items


# ── under --no-reader: no LLM client is ever constructed ─────────────


def test_writes_one_index_per_config_and_one_item_table(tmp_path):
    _run(tmp_path, store_label="retention/fifo/lexical/b1")
    _run(tmp_path, store_label="retention/lru/lexical/b1", policy="lru")

    assert len(_indexes(tmp_path)) == 2, "one retrieved_index.jsonl per config"
    tables = sorted(tmp_path.glob("evidence_items*.jsonl"))
    assert [p.name for p in tables] == ["evidence_items.shard0.jsonl"], \
        "one item table per shard, at the out-dir root, shared across configs"


def test_dump_happens_with_no_reader_at_all(tmp_path):
    """Regression guard for the collection gate.

    `collect_ctx` used to be `reader_impl is not None`, so under --no-reader
    nothing was ever collected and the dump wrote an empty file in silence.
    """
    _run(tmp_path, store_label="retention/fifo/lexical/b1")
    rows = _read_jsonl(_indexes(tmp_path)[0])
    assert len(rows) == 2, "one row per scored query (2 examples x 1 query)"
    assert {r["query_id"] for r in rows} == {"e0q0", "e1q0"}
    assert all(r["retrieved"] for r in rows), "each query recorded its evidence"


def test_every_key_resolves_in_the_item_table(tmp_path):
    _run(tmp_path, store_label="retention/fifo/lexical/b1")
    items = _item_table(tmp_path)
    dangling = [r["key"] for row in _read_jsonl(_indexes(tmp_path)[0])
                for r in row["retrieved"] if r["key"] not in items]
    assert dangling == []


def test_ranks_are_contiguous_from_one_and_carry_the_order(tmp_path):
    """Rank is stored, never implied: a JSON round trip must not be able to
    reorder the evidence."""
    _run(tmp_path, store_label="retention/fifo/lexical/b1")
    for row in _read_jsonl(_indexes(tmp_path)[0]):
        ranks = [r["rank"] for r in row["retrieved"]]
        assert ranks == list(range(1, len(ranks) + 1))
        # Shuffling the stored list must not change what a replay rebuilds.
        shuffled = list(reversed(row["retrieved"]))
        rebuilt = [r["id"] for r in sorted(shuffled, key=lambda r: r["rank"])]
        assert rebuilt == [r["id"] for r in row["retrieved"]]


def test_index_carries_gold_ids_and_run_provenance(tmp_path):
    """gold_item_ids comes from the workload events, so it is COMPLETE — gold
    that was never retrieved has no rank and would be missing if derived from
    gold_ranks instead."""
    _run(tmp_path, store_label="retention/fifo/lexical/b1")
    rows = {r["query_id"]: r for r in _read_jsonl(_indexes(tmp_path)[0])}
    assert rows["e0q0"]["gold_item_ids"] == ["e0i1"]
    assert rows["e0q0"]["example_id"] == "ex0"
    # Written even under --no-reader: they record what a reader WOULD have used.
    assert rows["e0q0"]["run_token_budget"] == DEFAULT_TOKEN_BUDGET
    assert rows["e0q0"]["run_prompt_sha"] == prompt_sha(DEFAULT_PROMPT)


def test_one_id_under_two_keys_survives_the_item_table(tmp_path):
    """Keyed on TEXT, not id — the invariant, tested where it lives.

    A compressor rewrites an item's text under the SAME id, so (id, text) pairs
    are what must stay distinct. Keying on id would silently drop whichever
    version arrived second and a replay would rebuild the wrong evidence.

    Asserted on the table helpers rather than by driving a compression run: which
    item gets rewritten depends on the policy's victim schedule and whether it is
    still resident and top-k at the next query, so an end-to-end version would
    fail on unrelated changes to eviction order.
    """
    path = tmp_path / "evidence_items.shard0.jsonl"
    original = "item0 " + _BODY
    rewritten = "summary of item0."
    assert _item_key(original) != _item_key(rewritten)

    _append_evidence_items(path, {_item_key(original):
                                  {"key": _item_key(original), "id": "r0",
                                   "text": original}})
    _append_evidence_items(path, {_item_key(rewritten):
                                  {"key": _item_key(rewritten), "id": "r0",
                                   "text": rewritten}})

    rows = _read_jsonl(path)
    assert len(rows) == 2, "the rewrite did not displace the original"
    assert {r["id"] for r in rows} == {"r0"}
    assert {r["text"] for r in rows} == {original, rewritten}


def test_item_table_dedups_and_survives_a_resume(tmp_path):
    """Re-appending the same key is a no-op, and the registry is seeded from
    disk — so a resumed shard appends only genuinely new keys rather than
    duplicating everything it wrote before the crash."""
    import forgetit.benchmark.pipeline as pl
    path = tmp_path / "evidence_items.shard0.jsonl"
    a = {"key": _item_key("aaa"), "id": "i0", "text": "aaa"}
    b = {"key": _item_key("bbb"), "id": "i1", "text": "bbb"}

    assert _append_evidence_items(path, {a["key"]: a}) == 1
    assert _append_evidence_items(path, {a["key"]: a}) == 0        # in-process
    pl._evidence_keys_written.clear()                              # simulate a restart
    assert _append_evidence_items(path, {a["key"]: a, b["key"]: b}) == 1
    assert len(_read_jsonl(path)) == 2


def test_compression_run_captures_rewritten_and_original_text(tmp_path):
    """End-to-end wiring under a compressing policy: both rewritten and
    untouched items reach the table, and every key still resolves."""
    _run(tmp_path, store_label="retention/compression/lexical/b400",
         workload=RewriteWorkload(), store_factory=_compress_store,
         policy="compression")
    texts = [r["text"] for r in _item_table(tmp_path).values()]
    assert any(t.startswith("summary of ") for t in texts), "no rewritten text captured"
    assert any(_BODY[:20] in t for t in texts), "no original text captured"

    items = _item_table(tmp_path)
    for row in _read_jsonl(_indexes(tmp_path)[0]):
        assert all(r["key"] in items for r in row["retrieved"])


def test_flag_does_not_touch_the_reader_identity(tmp_path):
    """--dump-retrieval is an ARTEFACT switch: it changes what is written, never
    the reader's name or the prompt sha those artefacts are compared by."""
    reader = PlainTextReader(StubLLM())
    _run(tmp_path, store_label="retention/fifo/lexical/b1", reader_impl=reader)
    assert "dump" not in reader.name
    assert reader.name == "reader_stub-model_rtok16000_gen256"

    import csv
    csv_path = next(tmp_path.rglob("results.csv"))
    with csv_path.open() as f:
        rows = list(csv.DictReader(f))
    assert all(r["prompt_reader_sha"] == prompt_sha(DEFAULT_PROMPT) for r in rows)


def test_flag_changes_the_config_hash_so_a_done_config_re_runs(tmp_path):
    """Not experiment identity — skip-if-done. Without it, re-running a finished
    config with --dump-retrieval matches its DONE.json, skips, and silently
    produces no artefact."""
    base = dict(dataset="d", itemiser="i", store_label="s", reader="r", k=5,
                seed=0, budget_bytes=1, budget_frac=0.0, n_examples=2,
                write_events=False, write_gold_snapshot=False)
    assert _config_hash(**base, dump_retrieval=False) != \
           _config_hash(**base, dump_retrieval=True)


# ── under a stub reader: the acceptance test ─────────────────────────


def test_replay_reproduces_exactly_what_the_reader_saw(tmp_path):
    """THE acceptance test.

    Rebuild the records from the artefact alone, hand them to build_context with
    the run's own budget, and the delivered item ids must equal what the live
    reader recorded on its results row. If this holds, an offline pass over the
    dump sees precisely what the run saw — which is the entire premise.
    """
    import csv
    reader = PlainTextReader(StubLLM())
    _run(tmp_path, store_label="retention/fifo/lexical/b1", reader_impl=reader)

    with next(tmp_path.rglob("results.csv")).open() as f:
        live = {r["query_id"]: json.loads(r["reader_item_ids"])
                for r in csv.DictReader(f)}
    assert live and any(v for v in live.values()), "reader recorded no item ids"

    items = _item_table(tmp_path)
    for row in _read_jsonl(_indexes(tmp_path)[0]):
        records = [MemRecord(id=r["id"], text=items[r["key"]]["text"])
                   for r in sorted(row["retrieved"], key=lambda r: r["rank"])]
        ctx = build_context(Query(text=row["question_text"], id=row["query_id"]),
                            records, token_budget=row["run_token_budget"],
                            prompt=DEFAULT_PROMPT)
        assert ctx.item_ids == live[row["query_id"]], \
            f"replay diverged from the run for {row['query_id']}"


def test_replay_may_use_a_different_budget_than_the_run(tmp_path):
    """The reason retrieval output is stored instead of a rendered prompt: a
    larger model must be able to take more evidence than the run's budget."""
    _run(tmp_path, store_label="retention/fifo/lexical/b1")
    items = _item_table(tmp_path)
    row = _read_jsonl(_indexes(tmp_path)[0])[0]
    records = [MemRecord(id=r["id"], text=items[r["key"]]["text"])
               for r in sorted(row["retrieved"], key=lambda r: r["rank"])]

    tight = build_context(Query(text=row["question_text"], id=row["query_id"]),
                          records, token_budget=20)
    wide = build_context(Query(text=row["question_text"], id=row["query_id"]),
                         records, token_budget=row["run_token_budget"])
    assert len(tight.item_ids) < len(wide.item_ids)
    assert tight.truncated and not wide.truncated
