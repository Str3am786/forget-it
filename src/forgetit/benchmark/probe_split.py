"""Deterministic probe/scored partition of evaluation questions (R8 fix, B1/B2).

THE DEFECT THIS REMOVES: the ingestion access probe recited the evaluation
questions through the mutating retrieval path, inflating the access counters
of answer-bearing items before eviction began — a soft answer label for every
access-consuming policy. The fix: probe texts and scored texts must be
DISJOINT sets, fixed once per (dataset, example) under a dedicated seed,
shipped with the artefact, and hashed into the config identity.

Design decisions (agreed 2026-08; do not change silently — the partition is
part of the experimental definition):

  * Scored share 0.7 / probe share 0.3, stratified by question category, so
    the scored half keeps every LoCoMo category and BEAM ability in
    proportion (the abstention analysis needs the adversarial stratum).
  * Singleton strata are SCORED. A category with one question never
    sacrifices it to probing.
  * The partition seed is a MODULE CONSTANT, deliberately independent of the
    random-policy seed sweep: random_s42..random_s51 must all replay the
    identical partition or cross-policy comparisons break.
  * Determinism is order-free: the draw happens on SORTED question ids under
    a key derived from (dataset, example_id, seed), so ingestion order,
    dict order, or shard order cannot move a question across the boundary.
  * LongMemEval admits no split (one question per example): its probes are
    RESIDENT SELF-QUERIES — the text of an already-inserted item, chosen
    deterministically per (example, insert ordinal). See self_probe_index.

Integration (one call per workload):
    part = ProbePartition.build(dataset="locomo", example_id=conv_id,
                                questions=[(qid, qtype), ...])
    part.probe_order   -> cycle these ids' texts as the access probes
    part.scored_ids    -> emit query events ONLY for these
    ...and write_manifest(...) once per run; hash goes into _config_hash.
"""
from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass, field
from pathlib import Path

# ── Experimental constants (hash-relevant; changing any invalidates DONE.json) ──
PARTITION_SEED: int = 13          # dedicated; NOT the policy seed sweep
SCORED_SHARE: float = 0.7         # probe share = 1 - SCORED_SHARE
PROBE_SCHEME_SPLIT = "disjoint_split_v1"       # BEAM / LoCoMo
PROBE_SCHEME_SELF = "resident_self_query_v1"   # LongMemEval
PROBE_SCHEME_LEAKY = "leaky_v0"                # ARCHIVED: the pre-fix behaviour,
                                               # kept runnable ONLY for the
                                               # leak-ablation arm (sec:bm:leak).
SELF_PROBE_MAX_CHARS = 512                     # self-query probes recite at most
                                               # this many chars of the item


def _derive_rng(dataset: str, example_id: str, seed: int, salt: str = "") -> random.Random:
    """Order-free, platform-stable RNG for one example's draw."""
    key = f"{dataset}|{example_id}|{seed}|{salt}".encode()
    digest = hashlib.blake2b(key, digest_size=8).digest()
    return random.Random(int.from_bytes(digest, "big"))


@dataclass(frozen=True)
class ProbePartition:
    dataset: str
    example_id: str
    seed: int
    scored_share: float
    probe_ids: frozenset[str]
    scored_ids: frozenset[str]
    probe_order: tuple[str, ...] = field(default=())   # fixed cycle order

    @classmethod
    def build(
        cls,
        *,
        dataset: str,
        example_id: str,
        questions: list[tuple[str, str]],   # (question_id, stratum_label)
        seed: int = PARTITION_SEED,
        scored_share: float = SCORED_SHARE,
    ) -> "ProbePartition":
        """Stratified draw. Deterministic; independent of input ordering."""
        if not questions:
            raise ValueError(f"{dataset}/{example_id}: no questions to partition")
        if not (0.0 < scored_share < 1.0):
            raise ValueError(f"scored_share must be in (0,1), got {scored_share}")

        ids_seen: set[str] = set()
        strata: dict[str, list[str]] = {}
        for qid, label in sorted(questions):            # sort → order-free
            if qid in ids_seen:
                raise ValueError(f"{dataset}/{example_id}: duplicate question id {qid!r}")
            ids_seen.add(qid)
            strata.setdefault(label or "unlabelled", []).append(qid)

        probe: set[str] = set()
        scored: set[str] = set()
        for label in sorted(strata):
            qids = strata[label]                         # already sorted
            if len(qids) == 1:
                scored.update(qids)                      # singleton → scored
                continue
            rng = _derive_rng(dataset, example_id, seed, salt=label)
            shuffled = qids[:]
            rng.shuffle(shuffled)
            # At least one scored AND at least one probe per multi-member stratum.
            n_scored = min(len(qids) - 1, max(1, round(scored_share * len(qids))))
            scored.update(shuffled[:n_scored])
            probe.update(shuffled[n_scored:])

        assert probe.isdisjoint(scored), "internal error: partition overlap"
        assert probe | scored == ids_seen, "internal error: question lost"

        order = sorted(probe)
        _derive_rng(dataset, example_id, seed, salt="cycle").shuffle(order)
        return cls(dataset=dataset, example_id=example_id, seed=seed,
                   scored_share=scored_share,
                   probe_ids=frozenset(probe), scored_ids=frozenset(scored),
                   probe_order=tuple(order))

    # ── manifest / hashing ──

    def row(self) -> dict:
        return {
            "dataset": self.dataset,
            "example_id": self.example_id,
            "seed": self.seed,
            "scored_share": self.scored_share,
            "probe_ids": sorted(self.probe_ids),
            "scored_ids": sorted(self.scored_ids),
            "probe_order": list(self.probe_order),
        }


def partition_hash(parts: list[ProbePartition]) -> str:
    """Stable content hash over a run's partitions — put THIS in _config_hash."""
    h = hashlib.blake2b(digest_size=16)
    for p in sorted(parts, key=lambda p: (p.dataset, p.example_id)):
        h.update(json.dumps(p.row(), sort_keys=True).encode())
    return h.hexdigest()


def write_manifest(parts: list[ProbePartition], path: str | Path,
                   probe_scheme: str = PROBE_SCHEME_SPLIT) -> str:
    """Write the partition file that ships with the artefact. Returns the hash."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    content_hash = partition_hash(parts)
    manifest = {
        "probe_scheme": probe_scheme,
        "partition_seed": parts[0].seed if parts else PARTITION_SEED,
        "scored_share": parts[0].scored_share if parts else SCORED_SHARE,
        "partition_hash": content_hash,
        "examples": [p.row() for p in
                     sorted(parts, key=lambda p: (p.dataset, p.example_id))],
    }
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    return content_hash


# ── LongMemEval: resident self-query probes (no split possible) ──

def self_probe_index(dataset: str, example_id: str, insert_ordinal: int,
                     n_inserted: int, seed: int = PARTITION_SEED) -> int:
    """Index of the already-inserted item whose TEXT the probe recites.

    Called at probe time (after insert number ``insert_ordinal``, with
    ``n_inserted`` items inserted so far). Deterministic per
    (dataset, example, ordinal, seed); uniform over inserted-so-far, so the
    probe stream mimics generic rehearsal with NO knowledge of the question.
    """
    if n_inserted <= 0:
        raise ValueError("self_probe_index called before any insert")
    rng = _derive_rng(dataset, example_id, seed, salt=f"selfprobe:{insert_ordinal}")
    return rng.randrange(n_inserted)