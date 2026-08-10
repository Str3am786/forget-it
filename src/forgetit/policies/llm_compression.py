from __future__ import annotations

import logging
import re
import textwrap
from typing import TYPE_CHECKING

from forgetit.core.cost import text_bytes
from forgetit.core.timing import phase
from forgetit.policies.actions import Evict, Compress
from forgetit.policies.base import EvictionPolicy

if TYPE_CHECKING:
    from forgetit.policies.base import SimpleEvictionPolicy
    from forgetit.llm.base import LLM
    from forgetit.memory_system import RetentionManager


_COMPRESS_PROMPT = textwrap.dedent("""\
    Compress the following text into a SHORT summary.
    Keep ONLY the important facts, names, dates, and numbers.
    Remove filler, greetings, and repetition.
    Output ONLY the summary — no preamble, no explanation.

    TEXT:
    {text}

    SUMMARY:""")

_PREAMBLE_RE = re.compile(
    r"^\s*(?:here\s+is\s+(?:a|the)\s+(?:\w+\s+)*\w+[:.\s]*|sure[,!:\s]*|okay[,!:\s]*)",
    re.IGNORECASE,
)
_FENCE_RE = re.compile(r"```[\s\S]*?```")
_REPEATED_LINE_RE = re.compile(r"^(.+?)(\n\1)+", re.MULTILINE)

log = logging.getLogger(__name__)


def _sanitise(text: str) -> str:
    """Clean up common small-model artefacts."""
    text = _FENCE_RE.sub("", text).strip()

    # Strip stacked preambles like:
    # "Sure, here is the summary:"
    # "Okay: here is a short summary:"
    while True:
        new_text = _PREAMBLE_RE.sub("", text).lstrip()
        if new_text == text:
            break
        text = new_text

    text = _REPEATED_LINE_RE.sub(r"\1", text).strip()

    lines = text.splitlines()

    # Drop a trailing incomplete line only when it looks like a fragment
    # attached after otherwise complete content.
    if len(lines) > 1 and lines[-1].rstrip() and not lines[-1].rstrip().endswith(
        (".", "!", "?", '"', "'")
    ):
        lines = lines[:-1]

    return "\n".join(line.rstrip() for line in lines).strip()


class LLMCompressionPolicy(EvictionPolicy):
    """
    Two-phase "compress-to-pack" policy driven by a small local LLM.

    The idea: a fixed byte budget holds *more* memories if we shrink them
    instead of deleting them. So on every overflow:

      1. COMPRESS phase — if any resident item is not yet compressed, pick the
         least-useful *un-compressed* item (per ``compress_selector``) and
         summarise it in place. It stays resident, now smaller.
      2. EVICT phase — only when *every* resident item is already compressed and
         we are *still* over budget do we actually delete: the least-useful item
         overall (per ``evict_selector``) is evicted.

    Each item is therefore compressed at most once (compressed items are excluded
    from the compress phase), so there is no re-compression churn and no need for
    retry/``max_attempts`` bookkeeping. The ``compressed`` flag — set by the
    RetentionManager on a successful compress and surfaced via MemFeatures — is
    what separates the two candidate sets.

    Both selectors are pluggable forgetting policies (FIFO / LRU / LFU / ...),
    defaulting to LFU. Pass ``selector=`` to set both at once, or
    ``compress_selector=`` / ``evict_selector=`` to mix them.
    """

    name = "llm_compression"

    def __init__(
        self,
        llm: LLM,
        selector: SimpleEvictionPolicy | None = None,
        *,
        compress_selector: SimpleEvictionPolicy | None = None,
        evict_selector: SimpleEvictionPolicy | None = None,
        max_tokens: int = 128,
        min_saving_ratio: float = 0.5,
        min_compress_bytes: int = 200,
        max_compress_bytes: int | None = None,
        prompt_template: str = _COMPRESS_PROMPT,
    ) -> None:
        # Lazy import keeps the policy package free of import-order surprises.
        from forgetit.policies.baselines import LFUPolicy

        base = selector if selector is not None else LFUPolicy()
        self.llm = llm
        self.compress_selector = compress_selector or base
        self.evict_selector = evict_selector or base
        self.max_tokens = max_tokens
        self.min_saving_ratio = min_saving_ratio
        self.min_compress_bytes = min_compress_bytes
        # Upper bound mirroring the lower one. An item whose prompt exceeds the
        # model's context window cannot be compressed: the call raises and the
        # handler below evicts, so the policy degrades to its evict selector on
        # that whole input class. 90% of BEAM's batch-level items are 50k-98k
        # tokens against a 32k window, so `compression` there was LFU eviction
        # for an entire campaign, and the only trace was a log line. Set this
        # from the wired model's window; None keeps the previous behaviour.
        self.max_compress_bytes = max_compress_bytes
        self.prompt_template = prompt_template

    def select_action(self, store: RetentionManager) -> Evict | Compress:
        uncompressed = {f.id for f in store.iter_features() if not f.compressed}

        if uncompressed:
            return self._compress_phase(store, uncompressed)

        # Everything resident is already compressed and we're still over budget:
        # last resort — delete the least-useful item.
        victim_id = self.evict_selector.select_victim(store)
        # COPY the selector's provenance rather than aliasing it: we annotate
        # below, and mutating the selector's own dict would corrupt its state.
        sel = getattr(self.evict_selector, "last_decision", None)
        self.last_decision = dict(sel) if sel else {}
        self._annotate_decision(action="evict", outcome="all_compressed")
        log.info("[compress] all resident items compressed → EVICT least-useful %s", victim_id)
        return Evict(id=victim_id)

    def _compress_phase(
        self, store: RetentionManager, uncompressed: set[str]
    ) -> Evict | Compress:
        victim_id = self.compress_selector.select_victim(store, candidates=uncompressed)
        # COPY, not alias — see _annotate_decision and the evict phase above.
        sel = getattr(self.compress_selector, "last_decision", None)
        self.last_decision = dict(sel) if sel else {}

        record = store.backend.get(victim_id)
        if record is None:
            log.info("[compress] %s vanished before compression → EVICT", victim_id)
            self._annotate_decision(action="compress", outcome="vanished")
            return Evict(id=victim_id)

        # Compression only rewrites the text, so every decision here is measured
        # against the TEXT bytes — not record.bytes, which may also carry a fixed
        # vector term (COUNT_VECTOR_BYTES). Crediting savings against the text it
        # actually shrinks keeps min_saving_ratio honest in both cost models.
        original_text_bytes = text_bytes(record)
        if original_text_bytes < self.min_compress_bytes:
            # Too small for an LLM call to pay for itself. Evict it (cheap) so the
            # budget loop still makes progress.
            log.info("[compress] %s too small to compress (%dB text < %dB floor) → EVICT",
                     victim_id, original_text_bytes, self.min_compress_bytes)
            self._annotate_decision(action="compress", outcome="too_small",
                                    orig_bytes=original_text_bytes)
            return Evict(id=victim_id)

        if self.max_compress_bytes is not None and original_text_bytes > self.max_compress_bytes:
            # Declared skip, not a thrown call: same eviction, but countable,
            # and it stops spending an unservable request per decision.
            log.info("[compress] %s too large to compress (%dB text > %dB ceiling) → EVICT",
                     victim_id, original_text_bytes, self.max_compress_bytes)
            self._annotate_decision(action="compress", outcome="too_large",
                                    orig_bytes=original_text_bytes)
            return Evict(id=victim_id)

        prompt = self.prompt_template.format(text=record.text)
        # Cache-key provenance: computed here as a pure function — NOT read off
        # the shared client (which would race across example threads). The store
        # logs it into the compress event, making the compressed text
        # recoverable from the disk cache for any logged compression.
        from forgetit.llm.disk_cache import cache_key
        self.last_llm_cache_key = cache_key(
            getattr(self.llm, "model", "?"), prompt, self.max_tokens, 0.0)
        try:
            with phase("compress_llm"):
                raw = self.llm.generate(prompt, max_tokens=self.max_tokens, temperature=0.0)
        except Exception as e:
            log.warning("[compress] %s LLM error (%s) → EVICT: %s",
                        victim_id, type(e).__name__, e)
            self._annotate_decision(action="compress", outcome="llm_error",
                                    orig_bytes=original_text_bytes,
                                    error=type(e).__name__)
            return Evict(id=victim_id)

        compressed = _sanitise(raw)
        compressed_bytes = len(compressed.encode("utf-8"))
        saving = 1 - (compressed_bytes / max(original_text_bytes, 1))

        # Full original/response text only at DEBUG so it doesn't flood INFO runs.
        log.debug("[compress] %s ORIGINAL (%dB text): %r", victim_id, original_text_bytes, record.text[:300])
        log.debug("[compress] %s RESPONSE (%dB): %r", victim_id, compressed_bytes, compressed[:300])

        if not compressed or saving < self.min_saving_ratio:
            # Can't shrink it enough and can't keep it (still over budget) → evict.
            # No retry: in the two-phase model each item is compressed at most once.
            log.info("[compress] %s REJECT %dB→%dB text saving=%.0f%% (need ≥%.0f%%) → EVICT",
                     victim_id, original_text_bytes, compressed_bytes,
                     100 * saving, 100 * self.min_saving_ratio)
            self._annotate_decision(
                action="compress",
                outcome="empty_output" if not compressed else "low_saving",
                orig_bytes=original_text_bytes, out_bytes=compressed_bytes,
                raw_bytes=len(raw.encode("utf-8")),
                cache_key=self.last_llm_cache_key)
            return Evict(id=victim_id)

        log.info("[compress] %s APPLY %dB→%dB text saving=%.0f%% (kept, marked compressed)",
                 victim_id, original_text_bytes, compressed_bytes, 100 * saving)
        # raw_bytes vs out_bytes separates what the model produced from what
        # survived _sanitise: with max_tokens=128 a truncated generation ends
        # mid-clause and the trailing-fragment rule deletes it, silently.
        self._annotate_decision(action="compress", outcome="applied",
                                orig_bytes=original_text_bytes,
                                out_bytes=compressed_bytes,
                                raw_bytes=len(raw.encode("utf-8")),
                                cache_key=self.last_llm_cache_key)
        return Compress(id=victim_id, new_text=compressed)