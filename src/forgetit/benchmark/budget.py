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

from forgetit.core.cost import record_cost

if TYPE_CHECKING:
    from forgetit.benchmark.workloads.base import Workload

# Trace the degradation curve rather than guess a threshold:
# 100% (control / ceiling) → 75 → 50 → 25 → 10.
DEFAULT_BUDGET_FRACTIONS: list[float] = [1.0, 0.75, 0.50, 0.25, 0.10]


def _item_bytes(item) -> int:
    """Byte size of one MemRecord for footprint accounting.

    Prefers the authoritative pinned ``bytes`` (set at ingestion / insert), and
    otherwise falls back to the single byte-cost source of truth
    (``core.cost.record_cost``), so the footprint denominator uses the SAME
    COUNT_VECTOR_BYTES model as the store's used_bytes numerator.

    Note on ordering: when COUNT_VECTOR_BYTES is on, embeddings must be attached
    (pinning ``bytes``) BEFORE footprints are computed — otherwise records here
    still carry no vector and the footprint would undercount relative to the
    eventual used_bytes. With the switch off (default) there is nothing to order.
    """
    b = getattr(item, "bytes", 0)
    if b and b > 0:
        return b
    return record_cost(item)


def example_footprints(workload: "Workload") -> list[int]:
    """Total inserted bytes per example, indexed by example.

    One full replay of the workload (no store, no LLM). When the workload
    exposes ``iter_examples()`` we sum per example directly — this aligns the
    footprint list with the across-example concurrent runner's example index
    regardless of ``clear_between_examples``. Otherwise we fall back to the
    ``reset``-delimited replay of ``events()`` (workloads emit resets *between*
    examples, not before the first; a stream with no resets is one example).
    """
    iter_examples = getattr(workload, "iter_examples", None)
    if callable(iter_examples):
        out: list[int] = []
        for _example_id, evs in iter_examples():
            out.append(sum(
                _item_bytes(ev.item)
                for ev in evs
                if ev.type == "insert" and ev.item is not None
            ))
        return out

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
