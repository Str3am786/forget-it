"""
Per-example relative budgeting.

Instead of a fixed byte cap, budgets are expressed as a *fraction of each
example's own footprint*: ``budget = frac × (bytes of that example's items)``.
Because the store is cleared between examples (clear_between_examples=True), the
runner re-sizes the budget at every example boundary, so pressure is uniform —
every example evicts ~(1 − frac) of its own content — and "50%" means the same
thing on LongMemEval and BEAM regardless of their very different absolute sizes.

frac = 1.0 is a built-in no-eviction control (budget == footprint).
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from forgetit.benchmark.workloads.base import Workload

# Trace the degradation curve rather than guess a threshold:
# 100% (control / ceiling) → 75 → 50 → 25 → 10.
DEFAULT_BUDGET_FRACTIONS: list[float] = [1.0, 0.75, 0.50, 0.25, 0.10]


def _item_bytes(item) -> int:
    """Byte size of one MemRecord, matching RetentionManager.estimate_bytes."""
    b = getattr(item, "bytes", 0)
    if b and b > 0:
        return b
    emb = getattr(item, "embedding", None)
    return len(item.text.encode("utf-8")) + (0 if emb is None else 4 * len(emb))


def example_footprints(workload: "Workload") -> list[int]:
    """Total inserted bytes per example, in event order.

    One full replay of the event stream (no store, no LLM). Examples are the
    segments delimited by ``reset`` events; workloads emit resets *between*
    examples (not before the first), so the returned list has one entry per
    example aligned with the runner's example index. A workload with no resets
    yields a single-element list (the whole stream is one example).
    """
    footprints: list[int] = []
    cur = 0
    for ev in workload.events():
        if ev.type == "reset":
            footprints.append(cur)
            cur = 0
        elif ev.type == "insert" and ev.item is not None:
            cur += _item_bytes(ev.item)
    footprints.append(cur)
    return footprints
