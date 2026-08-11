"""
Disk LLM cache: resumability + compressed-text retention.

Acceptance shape (rerun-prep A2): a killed shard restarts with zero repeated
LLM calls (new wrapper instance over the same directory = the restart), and
any compression event's text is recoverable via its logged cache_key.
"""
from __future__ import annotations

import pytest

from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.logging import BenchmarkLogger
from forgetit.core.schema import MemRecord
from forgetit.llm.disk_cache import CachedLLM, LLMDiskCache, cache_key
from forgetit.memory_system.custom_manager import RetentionManager
from forgetit.policies import LFUPolicy, LLMCompressionPolicy
from forgetit.retrieval.lexical import LexicalOverlapRetriever


class StubLLM:
    model = "stub-model-v1"

    def __init__(self, response: str = "Tiny summary."):
        self.calls = 0
        self.response = response

    def generate(self, prompt, *, max_tokens=64, temperature=0.0, **kw):
        self.calls += 1
        return self.response

    def stream(self, prompt, *, max_tokens=64, temperature=0.0):
        yield self.generate(prompt)


class MetaStubLLM(StubLLM):
    """A client that can report finish_reason."""

    def __init__(self, response="Tiny summary.", finish_reason="stop"):
        super().__init__(response)
        self.finish_reason = finish_reason
        self.meta_calls = 0

    def generate_with_meta(self, prompt, *, max_tokens=64, temperature=0.0, **kw):
        self.meta_calls += 1
        return self.response, self.finish_reason


class TestGenerateWithMeta:
    """finish_reason distinguishes an UNFINISHED answer from a wrong one."""

    def test_variant_keyspace_cannot_collide_with_plain_generate(self):
        plain = cache_key("m", "p", 64, 0.0)
        meta = cache_key("m", "p", 64, 0.0, variant="meta")
        assert plain != meta

    def test_default_variant_reproduces_the_historical_key(self):
        """Every cache_key already logged into a compress event depends on this
        payload being byte-identical — changing it would dangle the whole
        campaign's offline text recovery."""
        import hashlib
        expected = hashlib.sha256(
            f"m\x00{64}\x00{0.0:.6g}\x00p".encode("utf-8")).hexdigest()
        assert cache_key("m", "p", 64, 0.0) == expected
        assert cache_key("m", "p", 64, 0.0, variant="") == expected

    def test_meta_call_is_cached_and_returns_the_pair(self, tmp_path):
        inner = MetaStubLLM(finish_reason="length")
        llm = CachedLLM(inner, LLMDiskCache(tmp_path))
        assert llm.generate_with_meta("prompt") == ("Tiny summary.", "length")
        assert llm.generate_with_meta("prompt") == ("Tiny summary.", "length")
        assert inner.meta_calls == 1                  # second call served from disk

    def test_meta_entry_is_not_served_to_plain_generate(self, tmp_path):
        inner = MetaStubLLM()
        llm = CachedLLM(inner, LLMDiskCache(tmp_path))
        llm.generate_with_meta("prompt")
        llm.generate("prompt")
        assert inner.calls == 1                       # generate really went out
        assert inner.meta_calls == 1

    def test_degrades_when_the_inner_client_has_no_meta_call(self, tmp_path):
        """Unknown is not the same as not-cut: a client that cannot report
        finish_reason returns None rather than raising."""
        inner = StubLLM()                             # generate() only
        llm = CachedLLM(inner, LLMDiskCache(tmp_path))
        assert llm.generate_with_meta("prompt") == ("Tiny summary.", None)
        assert inner.calls == 1


class TestCachedLLM:

    def test_second_call_is_a_hit(self, tmp_path):
        inner = StubLLM()
        llm = CachedLLM(inner, LLMDiskCache(tmp_path))
        a = llm.generate("hello world", max_tokens=32, temperature=0.0)
        b = llm.generate("hello world", max_tokens=32, temperature=0.0)
        assert a == b
        assert inner.calls == 1

    def test_restart_reuses_disk(self, tmp_path):
        # "kill a shard mid-run, restart, zero repeated LLM calls"
        first = StubLLM()
        CachedLLM(first, LLMDiskCache(tmp_path)).generate("prompt-1", max_tokens=32)
        assert first.calls == 1
        fresh = StubLLM()                      # new process, same cache dir
        out = CachedLLM(fresh, LLMDiskCache(tmp_path)).generate("prompt-1", max_tokens=32)
        assert out == "Tiny summary."
        assert fresh.calls == 0

    def test_key_distinguishes_model_params_prompt(self):
        base = cache_key("m", "p", 64, 0.0)
        assert cache_key("m2", "p", 64, 0.0) != base
        assert cache_key("m", "p2", 64, 0.0) != base
        assert cache_key("m", "p", 65, 0.0) != base
        assert cache_key("m", "p", 64, 0.5) != base

    def test_model_attribute_passes_through(self, tmp_path):
        assert CachedLLM(StubLLM(), LLMDiskCache(tmp_path)).model == "stub-model-v1"

    def test_corrupt_entry_is_a_miss_not_a_crash(self, tmp_path):
        cache = LLMDiskCache(tmp_path)
        key = cache_key("m", "p", 64, 0.0)
        path = cache._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("not json{{{", encoding="utf-8")
        assert cache.get(key) is None


class TestCompressEventRetention:

    def test_compress_event_carries_recoverable_cache_key(self, tmp_path):
        llm = CachedLLM(StubLLM(), LLMDiskCache(tmp_path))
        policy = LLMCompressionPolicy(llm=llm, selector=LFUPolicy(),
                                      min_compress_bytes=100)
        store = RetentionManager(
            budget_bytes=900,
            backend=InMemoryBackend(retriever=LexicalOverlapRetriever()),
            policy=policy, logger=BenchmarkLogger(),
        )
        store.connect()
        store.insert(MemRecord(id="a", text="lorem ipsum " * 60, bytes=0))    # ~720B
        store.insert(MemRecord(id="b", text="dolor sit amet " * 40, bytes=0)) # overflow → compress

        compresses = [e for e in store.logger.log if e.name == "compress"]
        assert compresses, "no compression happened"
        for e in compresses:
            key = e.payload.get("cache_key")
            assert key, "compress event missing cache_key"
            raw = LLMDiskCache(tmp_path).get(key)
            assert raw == "Tiny summary.", "cache_key must recover the raw LLM output"

    def test_cache_key_is_pure_function_of_the_call(self, tmp_path):
        # The policy computes the key independently of the client (race-free
        # across example threads); both must agree.
        llm = CachedLLM(StubLLM(), LLMDiskCache(tmp_path))
        policy = LLMCompressionPolicy(llm=llm, selector=LFUPolicy(),
                                      min_compress_bytes=100)
        record_text = "lorem ipsum " * 60
        prompt = policy.prompt_template.format(text=record_text)
        expected = cache_key(llm.model, prompt, policy.max_tokens, 0.0)

        store = RetentionManager(
            budget_bytes=700,
            backend=InMemoryBackend(retriever=LexicalOverlapRetriever()),
            policy=policy, logger=BenchmarkLogger(),
        )
        store.connect()
        store.insert(MemRecord(id="a", text=record_text, bytes=0))
        store.insert(MemRecord(id="b", text="x" * 200, bytes=0))   # overflow → compress "a"

        compresses = [e for e in store.logger.log if e.name == "compress"]
        assert compresses and compresses[0].payload["cache_key"] == expected


def test_get_llm_wraps_with_cache(monkeypatch, tmp_path):
    monkeypatch.setenv("LLM_CACHE_DIR", str(tmp_path))
    monkeypatch.setenv("LLM_CACHE", "on")
    import forgetit.llm as llm_pkg
    monkeypatch.setattr(llm_pkg, "LLM_MODEL", "test-model")
    built = llm_pkg.get_llm()
    assert isinstance(built, llm_pkg.CachedLLM)
    assert built.cache.root == tmp_path

    monkeypatch.setenv("LLM_CACHE", "off")
    assert not isinstance(llm_pkg.get_llm(), llm_pkg.CachedLLM)
