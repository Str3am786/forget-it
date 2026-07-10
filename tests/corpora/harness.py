"""Shared fixture harness for the golden corpora (golden_corpora/*).

Loads YAML fixtures and executes corpus_e eviction sequences against the REAL
RetentionManager + InMemoryBackend — no mocks in the decision path. Semantics
mirrors the production runner:

  - ``insert``  → ``RetentionManager.insert`` (budget loop runs inside, before
                  the incoming item is admitted — incoming is NOT evictable).
  - ``touch``   → one scored retrieval, k=1, whose query text is the item's
                  unique token, so exactly that item is touched (the store's
                  probe_touch_mode is "original", every retrieval touches).
  - ``step``    → no store operation; exists to advance the scripted policy.

STEP SEMANTICS (scripted_eviction): the step counter advances once per LISTED
EVENT, in order — so ``evict_at_step: {4: [B]}`` with events
[insert, insert, insert, step] fires when the ``step`` op is reached.
"""
from __future__ import annotations

from pathlib import Path

import yaml

from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.schema import MemRecord, Query
from forgetit.memory_system import RetentionManager
from forgetit.policies import (
    FIFOPolicy, LRUPolicy, LFUPolicy, KeepSmallestPolicy, EvictSmallestPolicy,
    RandomPolicy, ScriptedEvictionPolicy, LLMCompressionPolicy,
)
from forgetit.retrieval.lexical import LexicalOverlapRetriever

CORPORA_ROOT = Path(__file__).resolve().parents[2] / "golden_corpora"

# Exemplar file + optional agent-bulk sibling (exemplars are frozen — bulk
# fixtures live in *_bulk.yaml so the verified files are never rewritten).
_CORPUS_FILES = {
    "corpus_e": [CORPORA_ROOT / "corpus_e" / "e_fixtures.yaml",
                 CORPORA_ROOT / "corpus_e" / "e_fixtures_bulk.yaml",
                 CORPORA_ROOT / "corpus_e" / "e_fixtures_bulk2.yaml"],
    "corpus_r": [CORPORA_ROOT / "corpus_r" / "r_fixtures.yaml",
                 CORPORA_ROOT / "corpus_r" / "r_fixtures_bulk.yaml",
                 CORPORA_ROOT / "corpus_r" / "r_fixtures_bulk2.yaml"],
    "corpus_a_reader": [CORPORA_ROOT / "corpus_a_reader" / "reader_fixtures.yaml",
                        CORPORA_ROOT / "corpus_a_reader" / "reader_fixtures_bulk.yaml",
                        CORPORA_ROOT / "corpus_a_reader" / "reader_fixtures_bulk2.yaml"],
    "verdict_triplets": [CORPORA_ROOT / "corpus_a_judge" / "verdict_triplets.yaml",
                         CORPORA_ROOT / "corpus_a_judge" / "verdict_triplets_bulk.yaml",
                         CORPORA_ROOT / "corpus_a_judge" / "verdict_triplets_bulk2.yaml"],
}


def load_fixtures(corpus: str, include_bulk: bool = True) -> list[dict]:
    paths = _CORPUS_FILES[corpus]
    if not include_bulk:
        paths = paths[:1]
    fixtures: list[dict] = []
    for path in paths:
        if not path.exists():
            continue
        with path.open() as f:
            part = yaml.safe_load(f)
        if not isinstance(part, list):
            raise ValueError(f"{path}: expected a top-level list of fixtures")
        fixtures.extend(part)
    ids = [f["id"] for f in fixtures]
    if len(ids) != len(set(ids)):
        raise ValueError(f"{corpus}: duplicate fixture ids across exemplar/bulk files")
    return fixtures


class StubSummariser:
    """Deterministic 'LLM' for compression_trigger fixtures: always returns a
    summary of exactly ``n_chars`` chars (plumbing/accounting corpus — the
    quality of compression is corpus_a_judge's business, not corpus_e's)."""

    def __init__(self, n_chars: int):
        self.n_chars = int(n_chars)

    def generate(self, prompt: str, **kwargs) -> str:
        return "s" * self.n_chars


class RecordingLogger:
    def __init__(self):
        self.events = []

    def event(self, name, payload=None):
        self.events.append((name, dict(payload or {})))

    def evictions(self) -> list[str]:
        return [p["id"] for n, p in self.events if n == "evict"]

    def compressions(self) -> list[str]:
        return [p["id"] for n, p in self.events if n == "compress"]


def build_policy(name: str, fixture_input: dict):
    if name == "lru":
        return LRUPolicy()
    if name == "lfu":
        return LFUPolicy()
    if name == "fifo":
        return FIFOPolicy()
    if name == "keep_smallest":
        return KeepSmallestPolicy()
    if name == "evict_smallest":
        return EvictSmallestPolicy()
    if name == "random":
        return RandomPolicy(seed=fixture_input.get("seed", 0))
    if name == "scripted_eviction":
        script = (fixture_input.get("script") or {}).get("evict_at_step", {})
        return ScriptedEvictionPolicy(evict_at_step=script)
    if name == "compression":
        stub = StubSummariser(fixture_input["stub_summary_chars"])
        return LLMCompressionPolicy(llm=stub, selector=LFUPolicy())
    raise ValueError(f"corpus harness: unknown policy {name!r}")


def run_e_fixture(fx: dict) -> dict:
    """Execute one corpus_e fixture. Returns {evictions, resident}."""
    inp = fx["input"]
    policy = build_policy(inp["policy"], inp)
    logger = RecordingLogger()
    store = RetentionManager(
        budget_bytes=int(inp["budget"]),
        backend=InMemoryBackend(retriever=LexicalOverlapRetriever()),
        policy=policy,
        logger=logger,
        probe_touch_mode="original",
    )
    store.connect()

    scripted = isinstance(policy, ScriptedEvictionPolicy)
    compression = isinstance(policy, LLMCompressionPolicy)
    for i, ev in enumerate(inp["events"], start=1):
        op = ev["op"]
        if op == "insert":
            # created_at mirrors listed order (data-clock agrees with the
            # stream here; fixtures that need disagreement set it explicitly).
            # Compression fixtures need text whose UTF-8 length IS the size
            # (compression math runs on text bytes); other classes keep the
            # frozen small-unique-token form with explicit bytes.
            if compression:
                head = f"{ev['item']} "
                text = head + "x" * (int(ev["size"]) - len(head.encode("utf-8")))
                store.insert(MemRecord(id=ev["item"], text=text, bytes=0,
                                       created_at=float(ev.get("created_at", i))))
                continue
            store.insert(MemRecord(
                id=ev["item"],
                text=f"{ev['item']} filler",
                bytes=int(ev["size"]),
                created_at=float(ev.get("created_at", i)),
            ))
        elif op == "touch":
            hits = store.retrieve(
                Query(id=f"touch::{i}", text=ev["item"], timestamp=float(i), meta={}),
                k=1,
            )
            assert [h.id for h in hits] == [ev["item"]], (
                f"touch aimed at {ev['item']!r} hit {[h.id for h in hits]} — "
                "harness bug, fixture result would be meaningless"
            )
        elif op == "step":
            pass  # no store operation; the step advance below is the point
        else:
            raise ValueError(f"corpus_e: unknown op {op!r}")
        if scripted:
            policy.on_step(store)

    resident = sorted(store.iter_ids())
    return {
        "evictions": logger.evictions(),
        "compressions": logger.compressions(),
        "resident": resident,
        "resident_bytes": {rid: store.get(rid).bytes for rid in resident},
    }
