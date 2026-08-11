"""Disk cache for LLM calls — the resumability + retention layer.

Every ``generate`` call is keyed by (model tag, prompt, max_tokens,
temperature) and stored as one JSON file under the cache directory. Because
all shipped LLM paths run temp-0 against pinned models, a cache hit is
byte-identical to a live call — so a killed shard restarts with zero repeated
LLM work, and any past compression's text is recoverable offline from its
logged cache key (see scripts/show_compressed.py).

Key properties:
  * ``cache_key`` is a pure function — callers (e.g. the compression policy,
    which logs the key into its compress events) can compute it independently
    of the client, with no shared mutable state across worker threads.
  * Writes are atomic (tempfile + rename), so concurrent shards sharing one
    cache directory never see torn files; a lost race just rewrites identical
    content.
  * The wrapper is transparent: ``CachedLLM`` exposes ``model`` and delegates
    ``stream`` (uncached — streaming is interactive-only, never a benchmark
    path).

Config: ``LLM_CACHE_DIR`` env overrides the location (default
``<repo>/artifacts/llm_cache``); ``LLM_CACHE=off`` disables the layer (live
calls only, e.g. when deliberately probing server nondeterminism).
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Iterable

from forgetit.llm.base import LLM

log = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parents[3]


def cache_enabled() -> bool:
    return os.environ.get("LLM_CACHE", "on").lower() not in ("off", "0", "false")


def cache_dir() -> Path:
    return Path(os.environ.get("LLM_CACHE_DIR", _REPO_ROOT / "artifacts" / "llm_cache"))


def cache_key(model: str, prompt: str, max_tokens: int, temperature: float,
              *, variant: str = "") -> str:
    """Content hash identifying one LLM call. Pure function — safe to compute
    anywhere (policy event logging, offline recovery) without touching the
    client.

    ``variant`` opens a disjoint keyspace for calls that return more than text
    (see ``generate_with_meta``), so a metadata entry can never be served to a
    plain ``generate``. The default "" leaves the payload byte-identical, which
    every cache_key already logged into a compress event depends on.
    """
    payload = f"{model}\x00{max_tokens}\x00{temperature:.6g}\x00{prompt}"
    if variant:
        payload = f"{variant}\x00{payload}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class LLMDiskCache:
    """One JSON file per call under ``root`` (sharded by key prefix)."""

    def __init__(self, root: Path | str | None = None):
        self.root = Path(root) if root is not None else cache_dir()

    def _path(self, key: str) -> Path:
        return self.root / key[:2] / f"{key}.json"

    def get(self, key: str) -> str | None:
        p = self._path(key)
        try:
            return json.loads(p.read_text(encoding="utf-8"))["response"]
        except FileNotFoundError:
            return None
        except Exception as e:                    # torn/corrupt file → miss, warn
            log.warning("[llm-cache] unreadable entry %s (%s) — treating as miss", p, e)
            return None

    def get_meta(self, key: str) -> tuple[str, str | None] | None:
        """Read a ``generate_with_meta`` entry as (text, finish_reason).

        ``None`` means miss. A hit whose stored ``finish_reason`` is null is
        still a hit — the server simply did not report one.
        """
        p = self._path(key)
        try:
            rec = json.loads(p.read_text(encoding="utf-8"))
            return rec["response"], rec.get("finish_reason")
        except FileNotFoundError:
            return None
        except Exception as e:                    # torn/corrupt file → miss, warn
            log.warning("[llm-cache] unreadable entry %s (%s) — treating as miss", p, e)
            return None

    def put(self, key: str, *, model: str, prompt: str, max_tokens: int,
            temperature: float, response: str,
            finish_reason: str | None = None) -> None:
        p = self._path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "key": key,
            "model": model,
            "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
            "prompt": prompt,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "response": response,
            "finish_reason": finish_reason,
        }
        fd, tmp = tempfile.mkstemp(dir=p.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(record, f)
            os.replace(tmp, p)                    # atomic on POSIX
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise


class CachedLLM(LLM):
    """Transparent read-through/write-through cache around any LLM client."""

    def __init__(self, inner: LLM, cache: LLMDiskCache | None = None):
        self.inner = inner
        self.cache = cache or LLMDiskCache()

    @property
    def model(self) -> str:
        return getattr(self.inner, "model", "?")

    def generate(self, prompt: str, *, max_tokens: int = 512,
                 temperature: float = 0.0, **kwargs) -> str:
        key = cache_key(self.model, prompt, max_tokens, temperature)
        hit = self.cache.get(key)
        if hit is not None:
            return hit
        out = self.inner.generate(prompt, max_tokens=max_tokens,
                                  temperature=temperature, **kwargs)
        self.cache.put(key, model=self.model, prompt=prompt,
                       max_tokens=max_tokens, temperature=temperature,
                       response=out)
        return out

    def generate_with_meta(self, prompt: str, *, max_tokens: int = 512,
                           temperature: float = 0.0,
                           **kwargs) -> tuple[str, str | None]:
        """Cached (text, finish_reason).

        Cached under variant="meta" so these entries share no keys with plain
        ``generate``: a text-only hit must never be served as though its
        finish_reason were known-absent. An inner client that predates this
        method still works — it just reports no finish_reason.
        """
        inner_meta = getattr(self.inner, "generate_with_meta", None)
        if not callable(inner_meta):
            return self.generate(prompt, max_tokens=max_tokens,
                                 temperature=temperature, **kwargs), None

        key = cache_key(self.model, prompt, max_tokens, temperature, variant="meta")
        hit = self.cache.get_meta(key)
        if hit is not None:
            return hit
        text, finish_reason = inner_meta(prompt, max_tokens=max_tokens,
                                         temperature=temperature, **kwargs)
        self.cache.put(key, model=self.model, prompt=prompt,
                       max_tokens=max_tokens, temperature=temperature,
                       response=text, finish_reason=finish_reason)
        return text, finish_reason

    def stream(self, prompt: str, *, max_tokens: int = 512,
               temperature: float = 0.0) -> Iterable[str]:
        # Streaming is interactive-only (never a benchmark path) — no caching.
        return self.inner.stream(prompt, max_tokens=max_tokens, temperature=temperature)
