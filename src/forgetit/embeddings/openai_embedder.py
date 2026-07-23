"""OpenAI-compatible Embedder.

Hits the ``/v1/embeddings`` endpoint, which Ollama (``nomic-embed-text``), vLLM
(when serving an embedding model), and OpenAI all expose identically.  Mirrors
:class:`forgetit.llm.openai_llm.OpenAiLLM`: optional httpx pool sizing for the
concurrent runner, exponential backoff with jitter on rate limits.
"""
from __future__ import annotations

import logging
import math
import random
import time

from openai import OpenAI, BadRequestError, RateLimitError

from .base import Embedder

log = logging.getLogger(__name__)


def _is_context_length_error(e: BadRequestError) -> bool:
    """Does this 400 mean the input was longer than the model's context window?"""
    msg = str(getattr(e, "message", "") or e).lower()
    return ("context length" in msg or "context window" in msg
            or "maximum context" in msg or "too long" in msg
            or ("input" in msg and "length" in msg))


class OpenAiEmbedder(Embedder):
    def __init__(
        self,
        base_url: str,
        api_key: str,
        model: str,
        *,
        batch_size: int = 128,
        normalize: bool = True,
        max_connections: int | None = None,
        max_retries: int = 6,
        base_delay: float = 0.5,
        max_input_chars: int | None = None,
    ):
        http_client = None
        if max_connections and max_connections > 0:
            import httpx
            limits = httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_connections,
            )
            http_client = httpx.Client(limits=limits)
        self.client = OpenAI(base_url=base_url, api_key=api_key, http_client=http_client)
        self.model = model
        self.batch_size = batch_size
        self.normalize = normalize
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_input_chars = max_input_chars
        self._truncated = 0   # count of inputs we had to shorten (observability)
        self._warned_trunc = False
        # ── stale-vector accounting (CACHE decision (a)) ──────────────────
        # The cache-key namespace (``name``) carries the CONFIGURED cap. When a
        # context-length 400 forces the cap DOWN mid-run, the resulting vectors
        # describe a shorter head than their key claims, and they are written
        # under the unshrunken key. The key format is deliberately NOT changed
        # (that would invalidate the warm emb_cache), so the mismatch is made
        # COUNTABLE and LOUD instead of silent.
        self._cap_shrinks = 0             # how many times the cap was halved
        self._vectors_at_reduced_cap = 0  # vectors keyed at a cap they don't match
        self._effective_min_cap: int | None = None   # smallest cap actually used

    @property
    def name(self) -> str:
        # Identity used as the cache-key namespace (cache.py:_key). It MUST capture
        # every input that changes the produced vector — including the input-length
        # cap, because truncation happens in _embed_batch AFTER keying. Omitting it
        # let a cache built under one EMBED_MAX_INPUT_CHARS silently serve
        # differently-truncated vectors under another. ``@<cap>`` (or ``@full`` when
        # uncapped) closes that. NOTE: this changes the namespace, so any cache built
        # by a previous version is a clean miss and will re-embed — intended.
        cap = self.max_input_chars if self.max_input_chars else "full"
        return f"{self.model}{'+l2' if self.normalize else ''}@{cap}"

    def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        out: list[list[float]] = []
        # Progress is emitted at DEBUG only, so it is SILENT at the default INFO
        # level — the benchmark's many small per-example embed() calls across
        # 16 shards × N workers won't spam. Set this module's logger to DEBUG to
        # watch throughput on a bulk embed (offline corpus build / analysis). No
        # interface change and no per-item cost when DEBUG is off (isEnabledFor
        # short-circuits before any formatting). The batch boundary lives here, not
        # in CachingEmbedder, so this is the only layer that can report progress.
        debug = log.isEnabledFor(logging.DEBUG)
        n = len(texts)
        t0 = time.perf_counter() if debug else 0.0
        for i in range(0, n, self.batch_size):
            out.extend(self._embed_batch(texts[i:i + self.batch_size]))
            if debug:
                done = min(i + self.batch_size, n)
                rate = done / max(time.perf_counter() - t0, 1e-9)
                log.debug("[embed] %s: %d/%d (%.0f items/s)", self.model, done, n, rate)
        if self.dim is None and out:
            self.dim = len(out[0])
        return out

    def _truncate(self, texts: list[str], cap: int | None) -> list[str]:
        """Clip each input to ``cap`` chars (no-op if cap is None). Counts clips."""
        if not cap or cap <= 0:
            return [t if t else " " for t in texts]
        out = []
        for t in texts:
            t = t or " "
            if len(t) > cap:
                if not self._warned_trunc:
                    log.warning("[embed] input %d chars > cap %d — truncating long items "
                                "to fit the embedding context (vector describes the head "
                                "only). Tune EMBED_MAX_INPUT_CHARS.", len(t), cap)
                    self._warned_trunc = True
                self._truncated += 1
                t = t[:cap]
            out.append(t)
        return out

    def _embed_batch(self, batch: list[str]) -> list[list[float]]:
        # Memory items (whole-session chunks) can exceed the embedding model's
        # context window. Pre-truncate to the configured char cap; if the server
        # STILL reports a context-length error (cap too high for this model, or
        # unset), halve the cap and retry rather than crash the whole run.
        cap = self.max_input_chars
        cleaned = self._truncate(batch, cap)
        last_err: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                r = self.client.embeddings.create(model=self.model, input=cleaned)
                rows = sorted(r.data, key=lambda d: d.index)
                vecs = [list(d.embedding) for d in rows]
                return [self._maybe_norm(v) for v in vecs]
            except RateLimitError as e:
                last_err = e
                sleep_s = self.base_delay * (2 ** attempt) * (1.0 + random.random() * 0.2)
                time.sleep(sleep_s)
            except BadRequestError as e:
                if not _is_context_length_error(e):
                    raise
                last_err = e
                longest = max((len(t) for t in cleaned), default=0)
                cap = max(256, min(cap or longest, longest) // 2)
                self._cap_shrinks += 1
                self._vectors_at_reduced_cap += len(cleaned)
                self._effective_min_cap = (cap if self._effective_min_cap is None
                                           else min(self._effective_min_cap, cap))
                # LOUD, not incidental: this is a CACHE-INTEGRITY event, not a
                # retry. These vectors are about to be written under the
                # namespace "<name>" — which advertises the CONFIGURED cap —
                # while describing only the first `cap` chars. Any later run
                # with the same configured cap will read them as if they were
                # full-cap vectors. See CACHE decision (a): the key format is
                # kept so the warm cache survives, so this counter is the only
                # record that the mismatch happened.
                log.warning(
                    "[embed] CACHE INTEGRITY: context-length 400 forced the input cap "
                    "DOWN to %d chars (configured %s, longest input %d). %d vector(s) "
                    "in this batch will be stored under namespace '%s', which claims "
                    "the configured cap — they describe a shorter head. "
                    "Run totals so far: %d shrink event(s), %d affected vector(s). "
                    "Lower EMBED_MAX_INPUT_CHARS to the effective value and re-embed "
                    "if these vectors matter.",
                    cap, self.max_input_chars or "uncapped", longest, len(cleaned),
                    self.name, self._cap_shrinks, self._vectors_at_reduced_cap)
                cleaned = self._truncate(cleaned, cap)
        assert last_err is not None
        raise last_err

    def cache_integrity_stats(self) -> dict:
        """Stale-vector accounting for this embedder (CACHE decision (a)).

        ``vectors_at_reduced_cap > 0`` means this process wrote vectors into the
        ``namespace`` below that describe a SHORTER head than the namespace
        claims. The key format is intentionally unchanged (a warm ``emb_cache``
        is worth more than a perfectly self-describing key here), so this is the
        only signal that it happened — surface it in run attestation rather than
        letting it live and die in the log.
        """
        return {
            "namespace": self.name,
            "configured_cap": self.max_input_chars,
            "effective_min_cap": self._effective_min_cap,
            "cap_shrink_events": self._cap_shrinks,
            "vectors_at_reduced_cap": self._vectors_at_reduced_cap,
            "inputs_truncated": self._truncated,
        }

    def _maybe_norm(self, v: list[float]) -> list[float]:
        if not self.normalize:
            return v
        n = math.sqrt(sum(x * x for x in v))
        if n == 0.0:
            return v
        return [x / n for x in v]
