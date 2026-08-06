"""V-3 (stage 05): provenance columns are stamped on EVERY row of a real
pipeline run, not just by ``_stamp_provenance`` in isolation.

``test_provenance.py`` pins the stamping function against fake objects. This
file drives the actual pipeline — tiny in-memory workload, stub chat LLM, stub
embedder, compression policy, reader ON — writes ``results.csv``, reads it back
from disk, and asserts each of the six provenance columns is non-empty AND
equal to the value independently recomputed from the wired objects.

Both entry points are covered because they build the config identity
differently: ``run_pipeline`` (serial) carries ``n_examples=None``,
``run_pipeline_concurrent`` (the launch path) carries the real count.

OBSERVE ONLY — nothing in ``src/`` is imported for mutation.
"""
from __future__ import annotations

import csv
import hashlib
import inspect
import subprocess

import pytest

from forgetit.backend.in_memory import InMemoryBackend
from forgetit.benchmark.pipeline import (
    _config_hash, run_pipeline, run_pipeline_concurrent,
)
from forgetit.benchmark.schema import WorkloadEvent
from forgetit.core.logging import BenchmarkLogger
from forgetit.core.schema import MemRecord, Query
from forgetit.llm.readers import PlainTextReader
from forgetit.llm.readers import text_reader
from forgetit.memory_system.custom_manager import RetentionManager
from forgetit.policies import LLMCompressionPolicy
from forgetit.policies.baselines import LFUPolicy
from forgetit.retrieval.lexical import LexicalOverlapRetriever

CHAT_MODEL = "stub-chat:0.5b"
EMBED_MODEL = "stub-encoder:0.6b"
COMPRESS_TEMPLATE = "Summarise in one line:\n{text}"

PROVENANCE_COLS = ("git_commit", "config_hash", "llm_model", "embed_model",
                   "prompt_compress_sha", "prompt_reader_sha")


class StubLLM:
    model = CHAT_MODEL

    def generate(self, prompt, **kwargs):
        return "stub answer"


class StubEmbedder:
    model = EMBED_MODEL

    def embed(self, texts):
        return [[float(len(t) % 7), 1.0, 0.0] for t in texts]

    def embed_one(self, text):
        return [float(len(text) % 7), 1.0, 0.0]


_BODY = ("alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu "
         "nu xi omicron pi rho sigma tau upsilon phi chi psi omega ")


class TinyWorkload:
    """2 examples x 4 inserts (~250B each) + 1 scored query. The 300B budget
    forces the compress phase, so a compression prompt template is genuinely
    in play when provenance is stamped."""

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

    def events(self):
        # Serial replay: same per-example streams with a ``reset`` between them,
        # exactly as the real workloads define it (longmemeval.py:304).
        first = True
        for _eid, evs in self.iter_examples():
            if not first:
                yield WorkloadEvent(type="reset", timestamp=0.0, item=None,
                                    query=None, gold_item_ids=[])
            first = False
            yield from evs


def _store():
    return RetentionManager(
        budget_bytes=300,
        backend=InMemoryBackend(retriever=LexicalOverlapRetriever()),
        policy=LLMCompressionPolicy(StubLLM(), selector=LFUPolicy(),
                                    min_compress_bytes=50, min_saving_ratio=0.3,
                                    prompt_template=COMPRESS_TEMPLATE),
        logger=BenchmarkLogger(),
        embedder=StubEmbedder(),
    )


def _reader():
    return PlainTextReader(StubLLM())          # legacy evidence, prompt v1


CFG = dict(dataset="provsmoke", store_label="retention/llm_compression/lexical/budget_300",
           k=5, seed=0, itemiser="fixed", budget_bytes=300)


def _expected_git_commit() -> str:
    sha = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                         capture_output=True, text=True).stdout.strip()
    dirty = subprocess.run(["git", "status", "--porcelain"],
                           capture_output=True, text=True).stdout.strip()
    return sha + ("-dirty" if dirty else "")


def _expected_reader_sha() -> str:
    # legacy evidence (token_budget None) + prompt_version v1 → the bare
    # evidence-builder source, exactly as historical rows recorded it.
    return hashlib.sha256(
        inspect.getsource(text_reader._build_prompt).encode("utf-8")
    ).hexdigest()[:12]


def _rows_from_disk(tmp_path, reader_name):
    base = (tmp_path / CFG["dataset"] / CFG["itemiser"] / CFG["store_label"]
            / reader_name)
    csv_path = base / "results.csv"
    assert csv_path.exists(), f"no results.csv at {csv_path}"
    with csv_path.open() as f:
        return list(csv.DictReader(f)), base


def _assert_provenance(rows, expected_config_hash):
    assert rows, "vacuous: no rows to check"
    expected = {
        "git_commit": _expected_git_commit(),
        "config_hash": expected_config_hash,
        "llm_model": CHAT_MODEL,
        "embed_model": EMBED_MODEL,
        "prompt_compress_sha": hashlib.sha256(
            COMPRESS_TEMPLATE.encode("utf-8")).hexdigest()[:12],
        "prompt_reader_sha": _expected_reader_sha(),
    }
    for i, r in enumerate(rows):
        for col in PROVENANCE_COLS:
            assert col in r, f"row {i}: column {col} missing from results.csv"
            assert r[col] != "", f"row {i}: {col} is empty"
            assert r[col] == expected[col], (
                f"row {i}: {col}={r[col]!r} != expected {expected[col]!r}")


def test_concurrent_pipeline_stamps_every_row(tmp_path):
    reader = _reader()
    results, _ = run_pipeline_concurrent(
        workload=TinyWorkload(), store_factory=_store, reader=reader.name,
        reader_impl=reader, out_dir=str(tmp_path), write_events=False, **CFG)
    assert len(results) == 2, "expected one scored query per example"

    rows, _ = _rows_from_disk(tmp_path, reader.name)
    assert len(rows) == len(results)
    expected_hash = _config_hash(
        dataset=CFG["dataset"], itemiser=CFG["itemiser"],
        store_label=CFG["store_label"], reader=reader.name, k=CFG["k"],
        seed=CFG["seed"], budget_bytes=CFG["budget_bytes"], budget_frac=0.0,
        answer_max_tokens=128, n_examples=2, write_events=False,
        write_gold_snapshot=False)
    _assert_provenance(rows, expected_hash)


def test_serial_pipeline_stamps_every_row(tmp_path):
    reader = _reader()
    results, _ = run_pipeline(
        workload=TinyWorkload(), store=_store(), reader=reader.name,
        reader_impl=reader, out_dir=str(tmp_path), **CFG)
    assert len(results) == 2

    rows, _ = _rows_from_disk(tmp_path, reader.name)
    # The serial path's identity carries n_examples=None (documented in
    # pipeline.py) — a different hash from the concurrent path for the same
    # science. Pinned here so the asymmetry cannot change unnoticed.
    expected_hash = _config_hash(
        dataset=CFG["dataset"], itemiser=CFG["itemiser"],
        store_label=CFG["store_label"], reader=reader.name, k=CFG["k"],
        seed=CFG["seed"], budget_bytes=CFG["budget_bytes"], budget_frac=0.0,
        answer_max_tokens=128, n_examples=None, write_events=False)
    _assert_provenance(rows, expected_hash)


def test_serial_and_concurrent_disagree_on_config_hash(tmp_path):
    """The two entry points stamp DIFFERENT config_hash values for the same
    science. Documented behaviour, not a bug — but it means a serial row and a
    concurrent row of one config are not comparable by config_hash alone."""
    reader = _reader()
    run_pipeline_concurrent(workload=TinyWorkload(), store_factory=_store,
                            reader=reader.name, reader_impl=reader,
                            out_dir=str(tmp_path / "conc"), **CFG)
    run_pipeline(workload=TinyWorkload(), store=_store(), reader=reader.name,
                 reader_impl=reader, out_dir=str(tmp_path / "ser"), **CFG)
    conc, _ = _rows_from_disk(tmp_path / "conc", reader.name)
    ser, _ = _rows_from_disk(tmp_path / "ser", reader.name)
    assert conc[0]["config_hash"] != ser[0]["config_hash"]
    # everything else about provenance agrees
    for col in ("git_commit", "llm_model", "embed_model",
                "prompt_compress_sha", "prompt_reader_sha"):
        assert conc[0][col] == ser[0][col]


def test_reader_off_run_still_stamps_llm_and_leaves_reader_sha_blank(tmp_path):
    """--no-reader compression runs: the chat tag must come from the policy,
    and prompt_reader_sha must be blank rather than stale."""
    results, _ = run_pipeline_concurrent(
        workload=TinyWorkload(), store_factory=_store, reader="",
        reader_impl=None, out_dir=str(tmp_path), **CFG)
    assert results
    rows, _ = _rows_from_disk(tmp_path, "none")
    for r in rows:
        assert r["llm_model"] == CHAT_MODEL          # from the policy
        assert r["embed_model"] == EMBED_MODEL
        assert r["prompt_compress_sha"] == hashlib.sha256(
            COMPRESS_TEMPLATE.encode("utf-8")).hexdigest()[:12]
        assert r["prompt_reader_sha"] == ""          # no reader ran
        assert r["git_commit"] and r["config_hash"]


def test_no_provenance_column_is_left_at_its_default(tmp_path):
    """Guards the failure mode the columns exist for: a row that silently
    carries the dataclass default ("") is indistinguishable from another run."""
    reader = _reader()
    run_pipeline_concurrent(workload=TinyWorkload(), store_factory=_store,
                            reader=reader.name, reader_impl=reader,
                            out_dir=str(tmp_path), **CFG)
    rows, _ = _rows_from_disk(tmp_path, reader.name)
    blank = {c: sum(1 for r in rows if r[c] == "") for c in PROVENANCE_COLS}
    assert all(v == 0 for v in blank.values()), f"blank provenance cells: {blank}"


@pytest.mark.parametrize("col", PROVENANCE_COLS)
def test_column_is_constant_within_a_config(tmp_path, col):
    """Every row of one config must agree — provenance identifies the CONFIG,
    so a per-row difference would mean the stamp leaked run-local state."""
    reader = _reader()
    run_pipeline_concurrent(workload=TinyWorkload(), store_factory=_store,
                            reader=reader.name, reader_impl=reader,
                            out_dir=str(tmp_path), **CFG)
    rows, _ = _rows_from_disk(tmp_path, reader.name)
    assert len({r[col] for r in rows}) == 1
