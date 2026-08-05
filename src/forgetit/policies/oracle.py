"""Gold-oracle eviction — the privileged upper-bound control, not a policy.

Protects every answer-bearing item and evicts one only when nothing else
remains. Converts survival numbers into "% of achievable headroom" (the
normalized-regret ceiling): policy − random gains are read against
oracle − random.

Gold-ness is INJECTED, not inferred: item metadata does not mark gold on
every dataset (BEAM resolves gold per query, not per item), so the runner —
which sees the example's full event stream — populates ``protected_ids`` and
``gold_counts`` before replay. The ``wants_gold_ids = True`` class flag is the
capability signal; the runner fills the attributes on any policy that sets it.

SCOPE OF THE CEILING. One replay serves every scored query in an example, so
what is protected is the UNION of gold over that example's questions, not the
gold of one question at a time. Where that union covers the whole store the
oracle has no legal victim and its guarantee lapses: policies can and do beat
it on individual questions. This is not hypothetical — LoCoMo scores ~198
questions against ~27 session-level items, so the union saturates on 100% of
examples, and BEAM's batch itemiser saturates on 77%. See
``experiments/audit_gold_saturation.py`` for the per-slice measurement, and
report ``forced_gold_evictions`` alongside any oracle-normalized figure.

SACRIFICE ORDER. When only protected items remain, the choice is which answers
to lose, so the victim is the protected item supporting the FEWEST scored
questions; age breaks ties. Ordering that branch by age alone makes it the FIFO
rule applied to the same candidate set, which produced an oracle byte-identical
to ``fifo`` on every saturated example.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from forgetit.policies.base import SimpleEvictionPolicy

if TYPE_CHECKING:
    from forgetit.memory_system import RetentionManager


class OraclePolicy(SimpleEvictionPolicy):

    name = "oracle"
    wants_gold_ids = True   # runner injects protected_ids + gold_counts

    def __init__(self) -> None:
        self.protected_ids: set[str] = set()
        # item id -> number of scored queries whose gold set names it. Empty
        # when injection has not run; ``.get(id, 1)`` keeps the fallback safe.
        self.gold_counts: dict[str, int] = {}
        self.forced_gold_evictions: int = 0

    def select_victim(self, store: "RetentionManager", candidates: set[str] | None = None) -> str:
        # Unprotected items are ranked oldest-first (the FIFO rule): arbitrary
        # but deterministic, since the oracle's claim is about WHAT it protects,
        # not how it orders the sacrificial rest. Protected items are ranked by
        # questions at stake first, so a forced sacrifice costs the fewest
        # answers; see SACRIFICE ORDER in the module docstring.
        best_victim: tuple[float, str] | None = None
        best_protected: tuple[int, float, str] | None = None

        n = 0
        for feat in store.iter_features():
            if candidates is not None and feat.id not in candidates:
                continue
            n += 1
            created = feat.created_at if feat.created_at is not None else 0.0
            if feat.id in self.protected_ids:
                stake = self.gold_counts.get(feat.id, 1)
                key = (stake, created, feat.id)
                if best_protected is None or key < best_protected:
                    best_protected = key
            else:
                key = (created, feat.id)
                if best_victim is None or key < best_victim:
                    best_victim = key

        if best_victim is not None:
            self._note_decision(best_victim[0], n)
            return best_victim[1]

        if best_protected is not None:
            self.forced_gold_evictions += 1
            # Log the victim's age so ``policy_score`` keeps one unit across
            # both branches, then mark the branch so a forced sacrifice is
            # identifiable per decision rather than only in the run total.
            self._note_decision(best_protected[1], n)
            if self.last_decision is not None:
                self.last_decision["forced_gold"] = True
                self.last_decision["questions_at_stake"] = int(best_protected[0])
            return best_protected[2]

        raise RuntimeError("oracle: store empty but eviction requested.")