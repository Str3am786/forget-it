"""
Per-config skip-if-done: rerunning a completed config is a no-op.

The DONE.json marker is keyed by the SAME config hash the provenance columns
carry, so a --limit smoke (different n_examples → different hash) can never
satisfy the full run, and FORGETIT_FORCE_RERUN=1 forces a replay.
"""
from __future__ import annotations

import json

from forgetit.backend.in_memory import InMemoryBackend
from forgetit.benchmark.pipeline import (
    _check_done, _write_done, run_pipeline_concurrent,
)
from forgetit.benchmark.schema import WorkloadEvent
from forgetit.core.logging import BenchmarkLogger
from forgetit.core.schema import MemRecord, Query
from forgetit.memory_system.custom_manager import RetentionManager
from forgetit.policies import FIFOPolicy
from forgetit.retrieval.lexical import LexicalOverlapRetriever


class TestMarkerSemantics:

    def test_roundtrip(self, tmp_path):
        base = tmp_path / "cfg"
        assert _check_done(base, "abc") is None            # nothing there yet
        _write_done(base, "abc", {"gold_survival_rate": 0.5})
        assert _check_done(base, "abc") == {"gold_survival_rate": 0.5}
        assert _check_done(base, "OTHER") is None          # different config → rerun

    def test_force_rerun_env(self, tmp_path, monkeypatch):
        base = tmp_path / "cfg"
        _write_done(base, "abc", {"x": 1.0})
        monkeypatch.setenv("FORGETIT_FORCE_RERUN", "1")
        assert _check_done(base, "abc") is None

    def test_corrupt_marker_is_a_rerun_not_a_crash(self, tmp_path):
        base = tmp_path / "cfg"
        base.mkdir()
        (base / "DONE.json").write_text("garbage{{{", encoding="utf-8")
        assert _check_done(base, "abc") is None

    def test_nan_summaries_survive_the_roundtrip(self, tmp_path):
        # summarize() emits NaN for undefined metrics; the marker must carry them.
        base = tmp_path / "cfg"
        _write_done(base, "abc", {"qa_accuracy": float("nan"), "hit_at_5": 1.0})
        got = _check_done(base, "abc")
        assert got["hit_at_5"] == 1.0
        assert got["qa_accuracy"] != got["qa_accuracy"]    # NaN


class _TinyWorkload:
    """One example, three inserts, one scored query."""

    def iter_examples(self):
        def rec(id_: str, ts: float) -> MemRecord:
            return MemRecord(id=id_, text=f"alpha beta {id_}", bytes=100, created_at=ts)

        events = [
            WorkloadEvent(type="insert", timestamp=1.0, item=rec("gold", 1.0),
                          query=None, gold_item_ids=[]),
            WorkloadEvent(type="insert", timestamp=2.0, item=rec("d1", 2.0),
                          query=None, gold_item_ids=[]),
            WorkloadEvent(type="insert", timestamp=3.0, item=rec("d2", 3.0),
                          query=None, gold_item_ids=[]),
            WorkloadEvent(type="query", timestamp=4.0, item=None,
                          query=Query(text="alpha beta gold"), gold_item_ids=["gold"]),
        ]
        yield "ex0", events


class TestEndToEnd:

    def _factory_counter(self):
        calls = {"n": 0}

        def factory():
            calls["n"] += 1
            return RetentionManager(
                budget_bytes=1 << 30,
                backend=InMemoryBackend(retriever=LexicalOverlapRetriever()),
                policy=FIFOPolicy(), logger=BenchmarkLogger(),
            )

        return factory, calls

    def test_second_run_is_a_noop(self, tmp_path):
        factory, calls = self._factory_counter()
        kwargs = dict(dataset="unit", workload=_TinyWorkload(),
                      store_factory=factory, store_label="skiptest",
                      k=5, out_dir=str(tmp_path))

        results1, summary1 = run_pipeline_concurrent(**kwargs)
        assert len(results1) == 1
        first_calls = calls["n"]                 # 1 example + 1 provenance probe
        assert first_calls >= 1

        results2, summary2 = run_pipeline_concurrent(**kwargs)
        assert results2 == []                    # skipped: rows already on disk
        assert calls["n"] == first_calls, "skip must build no stores at all"
        assert summary2["gold_survival_rate"] == summary1["gold_survival_rate"]
        assert summary2["n_queries"] == summary1["n_queries"]

    def test_force_rerun_replays(self, tmp_path, monkeypatch):
        factory, calls = self._factory_counter()
        kwargs = dict(dataset="unit", workload=_TinyWorkload(),
                      store_factory=factory, store_label="skiptest",
                      k=5, out_dir=str(tmp_path))
        run_pipeline_concurrent(**kwargs)
        first_calls = calls["n"]
        monkeypatch.setenv("FORGETIT_FORCE_RERUN", "1")
        results, _ = run_pipeline_concurrent(**kwargs)
        assert len(results) == 1                 # replayed for real
        assert calls["n"] > first_calls

    def test_marker_written_next_to_csvs(self, tmp_path):
        factory, _ = self._factory_counter()
        run_pipeline_concurrent(dataset="unit", workload=_TinyWorkload(),
                                store_factory=factory, store_label="skiptest",
                                k=5, out_dir=str(tmp_path))
        base = tmp_path / "unit" / "default" / "skiptest" / "none"
        assert (base / "results.csv").exists()
        marker = json.loads((base / "DONE.json").read_text())
        assert marker["config_hash"]
        assert "gold_survival_rate" in marker["summary"]
