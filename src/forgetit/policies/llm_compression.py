from __future__ import annotations

import re
import textwrap
from typing import TYPE_CHECKING

from forgetit.policies.actions import Evict, Compress
from forgetit.policies.base import EvictionPolicy

if TYPE_CHECKING:
    from forgetit.policies.base import SimpleEvictionPolicy
    from forgetit.llm.base import LLM
    from forgetit.memory_system.store import RetentionManager


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
    """Compress-then-evict policy driven by a small local LLM."""

    name = "llm_compression"

    def __init__(
        self,
        llm: LLM,
        selector: SimpleEvictionPolicy,
        max_tokens: int = 128,
        min_saving_ratio: float = 0.5,
        max_attempts: int = 3,
        min_compress_bytes: int = 200,
    ) -> None:
        self.llm = llm
        self.selector = selector
        self.max_tokens = max_tokens
        self.min_saving_ratio = min_saving_ratio
        self.max_attempts = max_attempts
        self.min_compress_bytes = min_compress_bytes
        self._failures: dict[str, int] = {}

    def select_action(self, store: RetentionManager) -> Evict | Compress:
        victim_id = self.selector.select_victim(store)

        if self._failures.get(victim_id, 0) >= self.max_attempts:
            self._failures.pop(victim_id, None)
            return Evict(id=victim_id)

        record = store.backend.get(victim_id)
        if record is None:
            return Evict(id=victim_id)

        original_bytes = record.bytes

        if original_bytes < self.min_compress_bytes:
            return Evict(id=victim_id)

        prompt = _COMPRESS_PROMPT.format(text=record.text)

        try:
            raw = self.llm.generate(
                prompt,
                max_tokens=self.max_tokens,
                temperature=0.0,
            )
        except Exception:
            return Evict(id=victim_id)

        compressed = _sanitise(raw)
        compressed_bytes = len(compressed.encode("utf-8"))

        saving = 1 - (compressed_bytes / max(original_bytes, 1))

        if saving < self.min_saving_ratio or not compressed:
            self._failures[victim_id] = self._failures.get(victim_id, 0) + 1
            return Evict(id=victim_id)

        self._failures.pop(victim_id, None)
        return Compress(id=victim_id, new_text=compressed)