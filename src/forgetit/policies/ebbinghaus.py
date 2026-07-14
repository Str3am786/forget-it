from __future__ import annotations

import textwrap
from typing import TYPE_CHECKING

from forgetit.policies.base import SimpleEvictionPolicy
from forgetit.policies.llm_compression import LLMCompressionPolicy

if TYPE_CHECKING:
    from forgetit.memory_system import RetentionManager



class EbbinghausPolicy(SimpleEvictionPolicy):
    """
    MemoryBank-style forgetting-curve eviction.

    Each item gets a retention score
        R(item) = exp( -(now - created_at) / S(item) )
    where S grows with rehearsal count (each retrieval boosts strength):
        S(item) = base_strength + strength_growth * access_count

    The victim is the lowest-R item — the one our forgetting curve
    predicts is most likely to have been forgotten.

    Non-exponent to avoid underflow. 
    "now" is the latest timestamp seen across the store, so the policy
    works under benchmark replay (where timestamps live in the past).

    Reference:
        Zhong, Guo, Gao, Wang. "MemoryBank: Enhancing Large Language
        Models with Long-Term Memory." AAAI 2024.
        https://arxiv.org/abs/2305.10250
    """

    name = "ebbinghaus"

    def __init__(
        self,
        base_strength: float = 1.0,
        strength_growth: float = 1.0,
        time_unit_seconds: float = 86_400.0,  # 1 day, matches paper
    ) -> None:
        if base_strength <= 0:
            raise ValueError(f"base_strength must be > 0, got {base_strength}")
        if strength_growth < 0:
            raise ValueError(f"strength_growth must be >= 0, got {strength_growth}")
        if time_unit_seconds <= 0:
            raise ValueError(f"time_unit_seconds must be > 0, got {time_unit_seconds}")
        self.base_strength = base_strength
        self.strength_growth = strength_growth
        self.time_unit_seconds = time_unit_seconds

    def _strength(self, access_count: int) -> float:
        return self.base_strength + self.strength_growth * access_count

    def _log_retention(self, now: float, created_at: float, access_count: int) -> float:
        """log R(item) = -dt/S. Order-equivalent to retention, underflow-free."""
        dt = max(0.0, (now - created_at) / self.time_unit_seconds)
        return -dt / self._strength(access_count)

    def select_victim(self, store: "RetentionManager", candidates: set[str] | None = None) -> str:
        # last_access is a logical tick counter (not wall-clock), so we use
        # created_at for the forgetting-curve time delta. created_at preserves
        # the historical session timestamps from the replay dataset, which is
        # the correct "elapsed time" signal for the Ebbinghaus decay formula.
        # A missing created_at falls back to 0.0 (epoch = infinitely stale),
        # matching how LRU/LFU treat it; every shipped workload sets it.
        # Pass 1: "now" = most recent session creation timestamp in the store.
        # "now" is taken over ALL items (real elapsed time), not just candidates.
        now = 0.0
        for feat in store.iter_features():
            ts = feat.created_at or 0.0
            if ts > now:
                now = ts

        # Pass 2: rank by log-retention. Tiebreak deterministically.
        victim_id: str | None = None
        best: tuple[float, float, str] | None = None
        n = 0
        for feat in store.iter_features():
            if candidates is not None and feat.id not in candidates:
                continue
            n += 1
            ts = feat.created_at or 0.0
            score = (self._log_retention(now, ts, feat.access_count), ts, feat.id)
            if best is None or score < best:
                best = score
                victim_id = feat.id

        if victim_id is None:
            raise RuntimeError("Ebbinghaus: store empty but eviction requested.")
        self._note_decision(best[0], n)
        return victim_id


# Adapted from MemoryBank (Zhong et al., AAAI 2024), `summarize_content_prompt`
# in memory_bank/summarize_memory.py. We strip the role-prefixing loop because
# our MemRecord text is already formatted as "role: content\n..." by the
# LongMemEval itemisers (see _turn_text in workloads/longmemeval.py). The
# instruction wording is preserved verbatim from the English branch of the
# original prompt; the trailing "no preamble" line is added because small
# local models otherwise prepend "Sure, here is the summary:" etc.
_MEMORYBANK_PROMPT = textwrap.dedent("""\
    Please summarize the following dialogue as concisely as possible,
    extracting the main themes and key information. If there are multiple
    key events, you may summarize them separately.
    Output ONLY the summary — no preamble, no explanation.

    Dialogue content:
    {text}

    Summarization:""")

# TODO Not fully implemented, lacking "hierarchy"
#  MemoryBankHierarchicalCompressionPolicy
#  that prefers compressing already-compressed records (consolidation) before reaching for fresh ones.
class MemoryBankCompressionPolicy(LLMCompressionPolicy):
    """
    MemoryBank-style summarization-as-compression policy.

    Reference:
        Zhong, Guo, Gao, Wang. "MemoryBank: Enhancing Large Language
        Models with Long-Term Memory." AAAI 2024.
        https://arxiv.org/abs/2305.10250
    """

    name = "memorybank_compression"

    def __init__(self, *args, **kwargs) -> None:
        # Sampling parameters. Original MemoryBank calls GPT-3.5 with
        # temperature=0.7, top_p=1.0, frequency_penalty=0.4, presence_penalty=0.2, max_tokens=400.

        kwargs.setdefault("prompt_template", _MEMORYBANK_PROMPT)
        super().__init__(*args, **kwargs)