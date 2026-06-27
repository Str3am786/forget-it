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

    @property
    def name(self) -> str:
        return f"{self.model}{'+l2' if self.normalize else ''}"

    def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        out: list[list[float]] = []
        for i in range(0, len(texts), self.batch_size):
            out.extend(self._embed_batch(texts[i:i + self.batch_size]))
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
                log.warning("[embed] context-length 400 — shrinking input cap to "
                            "%d chars and retrying (longest was %d)", cap, longest)
                cleaned = self._truncate(cleaned, cap)
        assert last_err is not None
        raise last_err

    def _maybe_norm(self, v: list[float]) -> list[float]:
        if not self.normalize:
            return v
        n = math.sqrt(sum(x * x for x in v))
        if n == 0.0:
            return v
        return [x / n for x in v]
