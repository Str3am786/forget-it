#!/usr/bin/env python3
"""
Layer B — eviction-regret analysis (OFFLINE PASS over events.jsonl).

The unbuilt core of the eviction-measurement story, and the part that makes the
benchmark *better than WritePolicyBench rather than equal to it*. WPB computes a
single-stage regret: destroyed synthetic utility vs a within-stream knapsack
oracle, with no retriever and no reader downstream. Three things here are
structurally impossible for WPB and are the reason to build it this way:

  1. STAGED regret (annotation -> retrieval-counterfactual -> answer-counterfactual).
     WPB stops at the label, so its regret *is* annotation regret with no way to
     refine it. Here an evicted gold item only costs a *retrieval* if it would
     have been retrieved had it survived, and only an *answer* if keeping it flips
     abstain/wrong -> correct. The attrition down the funnel (how much "lost a
     gold label" overstates "lost an answer") is the honest cost of forgetting.
     The cheapest stage (annotation) needs zero model calls and runs today.

  2. NORMALIZED by a RANDOM floor and an ORACLE ceiling. WPB reports raw regret
     with no floor and a within-stream ceiling. Here every policy's regret is the
     fraction of the way from RANDOM (worst defensible policy, measured in the
     same sweep) to the query-agnostic ORACLE (Belady-proxy ceiling already in
     oracle_size_analysis.py). That fraction IS the avoidable/unavoidable split as
     one number, comparable across datasets, answering "policy's fault or budget's
     fault?" at every point on the budget-quality frontier.

  3. TIME-TO-REGRET vs REAL reuse distance. WPB's stream timing is synthetic.
     Here each regret event is tagged with the evicted gold's creation-to-need
     depth (n_inserts - insert_pos, computed straight from the log — the same
     quantity Family A characterizes), so we can ask whether a policy's mistakes
     are on the deep/hard gold (expected) or on shallow gold (failing the easy
     cases). No synthetic benchmark can make this claim.

This is the SECOND analysis path: it does NOT touch per-query summarize()/
EvalResult. It consumes the event log (+ the gold map it already carries) and
produces a per-(dataset, policy, budget) report.

CURRENCY NOTE. forgetit gold is BINARY relevance, not graded utility, so regret
here is in lost-gold / lost-answers, not WPB's synthetic-utility units. That is an
honest difference and arguably a cleaner one — state it in the thesis.

Annotation floor + bracket + time-to-regret + recency-calibration (no models):
    python scripts/eviction_regret.py --events-dir artifacts/benchmark \
        --oracle artifacts/oracle_size_report.json
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Iterator

import numpy as np

# Tier-1 answer-survival primitives live in grading.py (single source of truth
# for normalization + the answerable/abstention split). Reused, never reinvented.
from forgetit.benchmark.grading import (
    is_answerable, answer_contained_any, answer_recall_any,
)

# Reuse the project's Mann-Whitney AUC primitive for value calibration.
try:
    from forgetit.embeddings.similarity import auc_separation
except Exception:  # keep importable standalone / in a notebook
    def auc_separation(pos, neg) -> float:
        pos = np.asarray(pos, float); neg = np.asarray(neg, float)
        if pos.size == 0 or neg.size == 0:
            return float("nan")
        ns = np.sort(neg)
        lt = np.searchsorted(ns, pos, side="left")
        eq = np.searchsorted(ns, pos, side="right") - lt
        return float(np.mean((lt + 0.5 * eq) / ns.size))

_EPS = 1e-9


# ───────────────────────── data model ─────────────────────────────


@dataclass
class Item:
    id: str
    insert_pos: int                 # 0-based arrival index within its example
    bytes: int
    evicted_at: int | None = None   # arrival index (within example) when evicted
    deleted: bool = False
    compressed: bool = False
    compress_old: int = 0           # total bytes before compression (text+fixed)
    compress_freed: int = 0         # bytes reclaimed by compression (text-only)

    @property
    def survived(self) -> bool:
        return self.evicted_at is None and not self.deleted

    @property
    def removed(self) -> bool:
        """Thrown away by the policy (evicted or deleted) — the discards Layer C
        scores, as opposed to the survivors retention scores."""
        return self.evicted_at is not None or self.deleted

    @property
    def survival_span(self) -> int | None:
        """Arrivals between this item landing and being evicted (small => shed
        fast; a recency-policy signature)."""
        return None if self.evicted_at is None else max(0, self.evicted_at - self.insert_pos)


@dataclass
class ScoredQuery:
    query_id: str
    gold_ids: set[str]
    actual_hits: list[str] = field(default_factory=list)   # from the retrieve event
    abstained: bool | None = None
    answered_correctly: bool | None = None                 # absent in results.csv today
    question_type: str = ""


@dataclass
class Example:
    items: dict[str, Item]
    queries: list[ScoredQuery]
    n_inserts: int = 0

    @property
    def survivors(self) -> set[str]:
        return {i for i, it in self.items.items() if it.survived}

    def creation_to_need(self, item_id: str) -> int | None:
        """Depth the item sat before the (end-of-stream) query: n_inserts -
        insert_pos. High == inserted long before it was needed (Family-A's
        'gold sits deep'); the reuse-distance of THIS workload, from the log."""
        it = self.items.get(item_id)
        return None if it is None else (self.n_inserts - it.insert_pos)


@dataclass
class Run:
    dataset: str
    policy: str
    budget_frac: float
    examples: list[Example]
    meta: dict = field(default_factory=dict)


@dataclass
class RegretEvent:
    query_id: str
    item_id: str
    bytes: int
    question_type: str
    survival_span: int | None
    creation_to_need: int | None
    stage: str = "annotation"       # annotation | retrieval | answer


# ───────────────────────── loader ─────────────────────────────────


def _read_jsonl(path: Path) -> Iterable[dict]:
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def load_run(events_path: Path, *, dataset: str, policy: str, budget_frac: float,
             results_csv: Path | None = None) -> Run:
    """Parse one events.jsonl into a Run, segmenting examples on `clear`.

    Matches forgetit's logger schema ({"name": ..., **payload}):
      insert  : {id, bytes, t_insert}
      evict   : {id, freed_bytes, t_insert}   # t_insert = global insert counter AT eviction
      delete  : {id, freed_bytes}
      compress: {id, old_bytes, new_bytes, freed_bytes, t_insert}
      gold    : {query_id, gold_ids}
      retrieve: {q, hits}
      clear   : {}                              # example boundary (concurrent runner inserts it)
    `t_insert` is a per-store counter; we subtract each example's first insert so
    positions are 0-based within the example regardless of serial/concurrent log.
    """
    examples: list[Example] = []
    items: dict[str, Item] = {}
    queries: list[ScoredQuery] = []
    by_qid: dict[str, ScoredQuery] = {}
    base: int | None = None
    n_ins = 0

    def flush():
        nonlocal items, queries, by_qid, base, n_ins
        if items or queries:
            examples.append(Example(items=items, queries=queries, n_inserts=n_ins))
        items, queries, by_qid, base, n_ins = {}, [], {}, None, 0

    for ev in _read_jsonl(events_path):
        name = ev.get("name") or ev.get("event") or ev.get("type")
        if name == "clear":
            flush(); continue
        if name == "insert":
            pos = int(ev["t_insert"])
            if base is None:
                base = pos
            items[ev["id"]] = Item(id=ev["id"], insert_pos=pos - base, bytes=int(ev.get("bytes", 0)))
            n_ins += 1
        elif name == "evict":
            at = int(ev["t_insert"]) - (base or 0)
            it = items.get(ev["id"])
            if it is not None:
                it.evicted_at = at
            else:                                  # evict before insert seen (defensive)
                items[ev["id"]] = Item(id=ev["id"], insert_pos=at, bytes=0, evicted_at=at)
        elif name == "delete":
            it = items.get(ev["id"])
            if it is not None:
                it.deleted = True
        elif name == "compress":
            it = items.get(ev["id"])
            if it is not None:
                it.compressed = True
                it.compress_old = int(ev.get("old_bytes", 0))
                it.compress_freed = int(ev.get("freed_bytes", 0))
        elif name == "gold":
            q = ScoredQuery(query_id=ev["query_id"], gold_ids=set(ev["gold_ids"]))
            by_qid[q.query_id] = q
            queries.append(q)
        elif name == "retrieve":
            q = by_qid.get(ev.get("q"))
            if q is not None:
                q.actual_hits = list(ev.get("hits", []))
        # oversized_insert / compress_skipped / compress_*_embedding: ignored
    flush()

    if results_csv and results_csv.exists():
        _attach_results(examples, results_csv)
    return Run(dataset=dataset, policy=policy, budget_frac=budget_frac, examples=examples)


def _attach_results(examples: list[Example], results_csv: Path) -> None:
    """Pull abstained / qa_correct / question_type per query from results.csv for
    the answer-counterfactual stage. `qa_correct` is the deterministic headline
    grade (benchmark.grading); a query that was answered *wrong* OR abstained is a
    candidate for answer-regret."""
    import csv
    by_qid = {q.query_id: q for ex in examples for q in ex.queries}
    with results_csv.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            q = by_qid.get(row.get("query_id", ""))
            if q is None:
                continue
            q.question_type = row.get("question_type", q.question_type)
            ab = row.get("abstained", "")
            q.abstained = (str(ab).lower() == "true") if ab != "" else None
            qc = row.get("qa_correct", "")
            q.answered_correctly = (str(qc).lower() == "true") if qc != "" else None


# ───────────────── stage 1: annotation regret (RUNS TODAY) ─────────


def annotation_regret_events(run: Run) -> list[RegretEvent]:
    """Every (query, gold-item) where the gold did NOT survive to query time.
    Pure function of the event log — no retriever, no reader. The floor: the
    counterfactual stages only ever REMOVE events from this set."""
    out: list[RegretEvent] = []
    for ex in run.examples:
        survivors = ex.survivors
        for q in ex.queries:
            for g in q.gold_ids:
                if g in survivors:
                    continue
                it = ex.items.get(g)
                out.append(RegretEvent(
                    query_id=q.query_id, item_id=g,
                    bytes=(it.bytes if it else 0),
                    question_type=q.question_type,
                    survival_span=(it.survival_span if it else None),
                    creation_to_need=ex.creation_to_need(g),
                ))
    return out


@dataclass
class Magnitude:
    """Regret in the three honest currencies for binary-gold workloads."""
    query_rate: float       # fraction of queries with >=1 evicted gold (NOT-all-survived)
    item_count: int         # (query, evicted-gold) events
    byte_mass: int          # bytes of evicted gold
    n_queries: int

    def as_dict(self) -> dict:
        return {"query_rate": self.query_rate, "item_count": self.item_count,
                "byte_mass": self.byte_mass, "n_queries": self.n_queries}


def magnitude(run: Run, events: list[RegretEvent]) -> Magnitude:
    n_q = sum(len(ex.queries) for ex in run.examples)
    affected = {e.query_id for e in events}
    return Magnitude(query_rate=(len(affected) / n_q) if n_q else 0.0,
                     item_count=len(events),
                     byte_mass=sum(e.bytes for e in events), n_queries=n_q)


# ───────────────── normalization: random floor / oracle ceiling ────


@dataclass
class Bracket:
    policy: float
    oracle: float           # ceiling (lowest achievable regret) -> unavoidable
    random: float           # floor (worst defensible policy)

    @property
    def avoidable(self) -> float:
        return max(0.0, self.policy - self.oracle)

    @property
    def normalized(self) -> float:
        denom = self.random - self.oracle
        if denom <= _EPS:
            return 0.0          # no room in this regime; policy can't be blamed
        return float(min(1.0, max(0.0, self.avoidable / denom)))

    def as_dict(self) -> dict:
        return {"policy": self.policy, "oracle": self.oracle, "random": self.random,
                "avoidable": self.avoidable, "unavoidable": self.oracle,
                "normalized_avoidable": self.normalized}


def load_oracle_ceiling(oracle_json: Path, dataset: str, budget_frac: float,
                        *, kind: str = "oracle_union_writetime") -> float | None:
    """Regret ceiling = 1 - oracle_survival(B), from oracle_size_analysis.py's
    report: {workload: {policy: {"0.50": survival, ...}}}. The query-AGNOSTIC
    union oracle is the honest write-time ceiling. Returns None when the dataset
    or budget isn't in the report (the report only covers fracs 0.05/0.10/0.25/0.50)."""
    try:
        data = json.loads(Path(oracle_json).read_text())
    except (OSError, ValueError):
        return None
    wl = data.get(dataset) or data.get(dataset.replace("/", "_"))
    if not wl:
        return None
    surv = (wl.get(kind) or {}).get(f"{budget_frac:.2f}")
    return None if surv is None else float(1.0 - float(surv))


# ───────────────── time-to-regret (real reuse distance) ────────────


@dataclass
class TimeToRegret:
    regret_median_c2n: float | None     # depth of EVICTED gold (the mistakes)
    workload_median_c2n: float | None   # depth of ALL gold (the background)
    n: int

    @property
    def verdict(self) -> str:
        # c2n = creation-to-need depth: SMALL == gold needed SOON after it arrived,
        # LARGE == needed far later. A policy whose mistakes cluster on soon-needed
        # gold is myopic (shed something about to be used); mistakes on far-needed
        # gold are defensible bets that happened to lose.
        if self.regret_median_c2n is None or self.workload_median_c2n is None:
            return "unknown"
        if self.regret_median_c2n < self.workload_median_c2n - _EPS:
            return "myopic"        # mistakes on soon-needed gold — the real blunder
        if self.regret_median_c2n > self.workload_median_c2n + _EPS:
            return "far-shed"      # mistakes on far-needed gold — defensible bets that lost
        return "neutral"

    def as_dict(self) -> dict:
        return {"regret_median_c2n": self.regret_median_c2n,
                "workload_median_c2n": self.workload_median_c2n,
                "n_events": self.n, "verdict": self.verdict}


def time_to_regret(run: Run, events: list[RegretEvent]) -> TimeToRegret:
    regret_c2n = [e.creation_to_need for e in events if e.creation_to_need is not None]
    all_gold_c2n = [c for ex in run.examples for q in ex.queries for g in q.gold_ids
                    if (c := ex.creation_to_need(g)) is not None]
    return TimeToRegret(
        regret_median_c2n=(float(np.median(regret_c2n)) if regret_c2n else None),
        workload_median_c2n=(float(np.median(all_gold_c2n)) if all_gold_c2n else None),
        n=len(regret_c2n))


# ───────────────── Layer C: eviction-as-classification ────────────
# Scores the DISCARDS (not the survivors): of everything the policy threw away,
# how much was noise (correct sheds) vs gold (mistakes). gold = the union of all
# gold ids for the example's queries. New relative to retention's gold_survival.


@dataclass
class EvictionClassification:
    n_evicted: int
    n_evicted_gold: int             # mistakes (evicted something needed)
    n_evicted_noise: int            # correct sheds
    bytes_evicted_gold: int
    bytes_evicted_noise: int

    @property
    def eviction_precision(self) -> float:
        """Fraction of evictions that were correct (noise). High == discarding the
        right things; low == shedding gold."""
        return (self.n_evicted_noise / self.n_evicted) if self.n_evicted else float("nan")

    @property
    def byte_weighted_precision(self) -> float:
        """Same over BYTES — the knapsack view: are you shedding cheap noise mass?"""
        tot = self.bytes_evicted_gold + self.bytes_evicted_noise
        return (self.bytes_evicted_noise / tot) if tot else float("nan")

    def as_dict(self) -> dict:
        return {"n_evicted": self.n_evicted, "n_evicted_gold": self.n_evicted_gold,
                "n_evicted_noise": self.n_evicted_noise,
                "bytes_evicted_gold": self.bytes_evicted_gold,
                "bytes_evicted_noise": self.bytes_evicted_noise,
                "eviction_precision": self.eviction_precision,
                "byte_weighted_precision": self.byte_weighted_precision}


def eviction_classification(run: Run) -> EvictionClassification:
    n_ev = ng = nn = bg = bn = 0
    for ex in run.examples:
        gold = {g for q in ex.queries for g in q.gold_ids}
        for it in ex.items.values():
            if not it.removed:
                continue
            n_ev += 1
            if it.id in gold:
                ng += 1; bg += it.bytes
            else:
                nn += 1; bn += it.bytes
    return EvictionClassification(n_ev, ng, nn, bg, bn)


# ───────────────── Layer D: compression outcomes (ratio) ──────────
# Compression is the alternative response to the same pressure, so measuring
# eviction means measuring the choice. Ratio is text-only (the vector term is
# preserved per the cost-model decision) because freed_bytes already nets it out.


def compression_stats(run: Run) -> dict:
    olds, freeds = [], []
    for ex in run.examples:
        for it in ex.items.values():
            if it.compressed and it.compress_old > 0:
                olds.append(it.compress_old); freeds.append(it.compress_freed)
    n = len(olds)
    return {
        "n_compressions": n,
        "bytes_reclaimed": int(sum(freeds)),
        # fraction of compressed bytes reclaimed (aggregate, byte-weighted)
        "compression_ratio": (sum(freeds) / sum(olds)) if olds else float("nan"),
        "mean_compression_ratio": (float(np.mean([f / o for f, o in zip(freeds, olds)]))
                                   if n else float("nan")),
    }


# ───────────────── value-function calibration (recency, today) ─────


def recency_value(it: Item) -> float:
    """What LRU protects: higher arrival index == more recent == higher value."""
    return float(it.insert_pos)


def value_calibration(run: Run, predicted_value: Callable[[Item], float] = recency_value) -> dict:
    """Does the value signal separate gold from noise? AUC>0.5 protects gold;
    ~0.5 is blind. The recency signal is expected AT or BELOW 0.5 on buried-gold
    workloads — the quantified version of 'recency is uninformative here'."""
    gold = {g for ex in run.examples for q in ex.queries for g in q.gold_ids}
    pos, neg = [], []
    for ex in run.examples:
        for it in ex.items.values():
            (pos if it.id in gold else neg).append(predicted_value(it))
    auc = auc_separation(np.array(pos, float), np.array(neg, float))
    return {"value_auc": auc, "n_gold": len(pos), "n_noise": len(neg),
            "separability": (abs(auc - 0.5) * 2 if auc == auc else float("nan"))}


# ───────────────── stages 2 & 3: counterfactual contracts ──────────
# These tighten the annotation floor but need data the event log doesn't hold
# (item CONTENT for re-ranking; the reader for re-answering). They're correct,
# injectable interfaces — the CLI runs the annotation floor; wire a workload
# resolver / reader to run these. Kept here so the staging is the backbone.

def retrieval_counterfactual_events(
    annotation_events: list[RegretEvent], *, retriever, k: int,
    item_content: Callable[[str], object],            # id -> MemRecord (re-itemise the workload)
    residents_at: Callable[[str], dict],              # query_id -> {id: MemRecord} resident at Q
    query_obj: Callable[[str], object],               # query_id -> Query
) -> list[RegretEvent]:
    """Keep a regret event only if re-admitting JUST the evicted gold (rest of the
    resident set fixed) would land it in the top-k. Single-item counterfactual is
    the well-defined version — it answers exactly what regret asks."""
    out = []
    for e in annotation_events:
        rec = item_content(e.item_id)
        if rec is None:
            continue
        resident = dict(residents_at(e.query_id)); resident[e.item_id] = rec
        ranked = retriever.topk(resident, query_obj(e.query_id), k)
        top = [getattr(r, "id", r) for r in ranked][:k]
        if e.item_id in top:
            out.append(RegretEvent(**{**e.__dict__, "stage": "retrieval"}))
    return out


def answer_counterfactual_events(
    retrieval_events: list[RegretEvent], run: Run, *, reader, grader,
    build_context: Callable[[str, str], list],        # (query_id, extra_item_id) -> reader context
    query_obj: Callable[[str], object], ref_answer: Callable[[str], str],
    sample_size: int | None = 200, rng_seed: int = 0,
) -> list[RegretEvent]:
    """Tightest stage; re-runs the reader (LLM) so SAMPLE only. An evicted gold
    cost an answer iff the query actually failed (abstained, the only failure
    signal in results.csv today) AND re-admitting it flips the reader to correct."""
    qmeta = {q.query_id: q for ex in run.examples for q in ex.queries}
    def _failed(qid):
        q = qmeta.get(qid)
        return q is not None and (q.abstained is True or q.answered_correctly is False)
    cand = [e for e in retrieval_events if _failed(e.query_id)]
    if sample_size and len(cand) > sample_size:
        idx = np.random.default_rng(rng_seed).choice(len(cand), size=sample_size, replace=False)
        cand = [cand[i] for i in idx]
    out = []
    for e in cand:
        ctx = build_context(e.query_id, e.item_id)
        if grader(reader.answer(ctx, query_obj(e.query_id)), ref_answer(e.query_id)):
            out.append(RegretEvent(**{**e.__dict__, "stage": "answer"}))
    return out


@dataclass
class Funnel:
    n_annotation: int
    n_retrieval: int | None = None
    n_answer: int | None = None

    def as_dict(self) -> dict:
        ra = (self.n_retrieval / self.n_annotation) if (self.n_retrieval is not None and self.n_annotation) else None
        aa = (self.n_answer / self.n_retrieval) if (self.n_answer is not None and self.n_retrieval) else None
        return {"n_annotation": self.n_annotation, "n_retrieval": self.n_retrieval,
                "n_answer": self.n_answer, "retrieval_attrition": ra, "answer_attrition": aa}


# ───────────────── Tier-1: gold ANSWER survival (the !A side) ──────
# id-survival (gold_survival_rate / annotation regret above) asks "did the gold
# ITEM survive". This asks the harder question compression forces: did the gold
# ANSWER STRING survive INSIDE the item's resident text. Embedder-independent —
# reads the stored TEXT (O_g original vs R_g resident), never vectors. Needs the
# gold_answer_snapshot.jsonl the runner emits under --event-log; without it this
# whole block is skipped (the id-survival analysis above still runs).

GOLD_SNAPSHOT_NAME = "gold_answer_snapshot.jsonl"


def load_gold_snapshot(events_path: Path) -> list[dict] | None:
    """Read the per-(query, gold-id) answer-survival snapshot sibling of
    events.jsonl. Returns None when absent (run predates the snapshot, or
    --event-log was off) so callers can degrade gracefully."""
    p = events_path.parent / GOLD_SNAPSHOT_NAME
    if not p.exists():
        return None
    return list(_read_jsonl(p))


@dataclass
class _AnsVerdict:
    """Per (query, gold-id) answer-survival verdict. base_* score the ORIGINAL
    inserted text O_g (the unavoidable/gold-mapping baseline); res_* score the
    RESIDENT text R_g (post-compression, "" if evicted)."""
    query_id: str
    gold_id: str
    answerable: bool
    resident: bool
    was_compressed: bool
    base_contain: int
    res_contain: int
    base_recall: float
    res_recall: float

    @property
    def localizable(self) -> bool:
        # The answer was actually present in the item to begin with — only then
        # is a later loss a COMPRESSION failure rather than a gold-mapping artifact.
        return self.base_contain == 1


def _verdicts_from_snapshot(rows: list[dict]) -> list[_AnsVerdict]:
    out: list[_AnsVerdict] = []
    for r in rows:
        answers = [r.get("answer", "")]            # list = future multi-gold; max-over
        O = r.get("original_text", "")
        R = r.get("resident_text", "")
        out.append(_AnsVerdict(
            query_id=r.get("query_id", ""),
            gold_id=r.get("gold_id", ""),
            answerable=is_answerable(r.get("query_id", ""), r.get("question_type", "")),
            resident=not bool(r.get("was_evicted", False)),
            was_compressed=bool(r.get("was_compressed", False)),
            base_contain=answer_contained_any(answers, O),
            res_contain=answer_contained_any(answers, R),
            base_recall=answer_recall_any(answers, O),
            res_recall=answer_recall_any(answers, R),
        ))
    return out


def _mean(xs) -> float:
    xs = list(xs)
    return (sum(xs) / len(xs)) if xs else float("nan")


def answer_survival_report(rows: list[dict], *, n_compress: int, theta: float = 0.5) -> dict:
    """Per-config answer-survival rollups over LOCALIZABLE, ANSWERABLE gold items.

    Headline ``gold_answer_survival`` replaces the flat id-survival line for the
    compression policy: a hard-evicted gold has R_g="" so it scores 0
    automatically — unifying eviction-loss and compression-loss in one number."""
    verdicts = _verdicts_from_snapshot(rows)
    answerable = [v for v in verdicts if v.answerable]
    n_abstention = len(verdicts) - len(answerable)
    loc = [v for v in answerable if v.localizable]
    nonloc = [v for v in answerable if not v.localizable]
    n_loc = len(loc)

    touched = [v for v in loc if v.was_compressed]      # compression actually rewrote it
    resident_loc = [v for v in loc if v.resident]

    retained = sum(1 for v in loc if v.res_contain == 1)
    degraded = sum(1 for v in loc if v.res_contain == 0 and v.res_recall >= theta)
    lost     = sum(1 for v in loc if v.res_contain == 0 and v.res_recall < theta)

    return {
        # headline + graded
        "gold_answer_survival": _mean(v.res_contain for v in loc),
        "gold_answer_recall":   _mean(v.res_recall for v in loc),
        # the avoidable-loss term (Layer B): loss only on items compression touched
        "compression_attributable_loss": (_mean(v.base_contain - v.res_contain for v in touched)
                                           if touched else 0.0),
        # same-scope id residency — the airtight upper bound for contract #1
        "gold_id_survival": _mean(1.0 if v.resident else 0.0 for v in loc),
        # resident-scoped survival — the airtight contract-#2 bug-catcher
        "survival_resident_localizable": _mean(v.res_contain for v in resident_loc),
        # counts
        "n_localizable": n_loc,
        "n_non_localizable": len(nonloc),
        "n_abstention_items": n_abstention,
        "n_gold_resident": sum(1 for v in loc if v.resident),
        "n_gold_evicted": sum(1 for v in loc if not v.resident),
        "n_compressed_localizable": len(touched),
        # optional tiering (theta)
        "theta": theta,
        "frac_retained": (retained / n_loc) if n_loc else float("nan"),
        "frac_degraded": (degraded / n_loc) if n_loc else float("nan"),
        "frac_lost":     (lost / n_loc) if n_loc else float("nan"),
        "n_compress": int(n_compress),
    }


def answer_survival_contracts(rep: dict, *, gold_survival_rate: float | None = None,
                              tol: float = 1e-9) -> list[dict]:
    """The bug-catchers, mirroring the existing 'query_rate == 1 - gold_survival'
    check. Each is {name, ok, hard, detail}; a HARD failure must abort the pass."""
    checks: list[dict] = []
    n_loc = rep["n_localizable"]
    surv = rep["gold_answer_survival"]
    id_surv = rep["gold_id_survival"]

    # Contract 1 — you cannot preserve an answer inside a gold item that did not
    # itself survive. Asserted at the SAME (localizable-item) scope as survival,
    # which makes it an exact invariant: res_contain<=resident per item.
    if n_loc == 0:
        checks.append({"name": "C1 answer<=id survival", "ok": True, "hard": True,
                       "detail": "no localizable items"})
    else:
        checks.append({"name": "C1 answer<=id survival",
                       "ok": surv <= id_surv + tol, "hard": True,
                       "detail": f"{surv:.4f} <= {id_surv:.4f}"})
    # Informational only: the query-level summary.gold_survival_rate is over a
    # DIFFERENT denominator (all queries incl. abstention/non-localizable), so
    # answer-survival sitting above it is a scope difference, not a bug.
    if gold_survival_rate is not None and n_loc:
        above = surv > gold_survival_rate + tol
        checks.append({"name": "  vs summary.gold_survival_rate", "ok": True, "hard": False,
                       "detail": f"{surv:.4f} vs {gold_survival_rate:.4f}"
                                 + ("  (above — scope differs)" if above else "")})

    # Contract 2 — no compression => resident gold keeps R_g==O_g => res_contain==1
    # for every RESIDENT localizable item, and zero compression-attributable loss.
    # This is the check that catches a wrong O_g/R_g source (it would show spurious
    # loss on an untouched item). HARD — never downgrade to a warning.
    if rep["n_compress"] == 0:
        sr = rep["survival_resident_localizable"]
        cal = rep["compression_attributable_loss"]
        ok_sr = (sr != sr) or abs(sr - 1.0) <= tol          # NaN (no resident loc) -> vacuous
        checks.append({"name": "C2 resident res_contain==1 (n_compress==0)", "ok": ok_sr,
                       "hard": True, "detail": f"survival_resident_localizable={sr:.4f}"
                       if sr == sr else "no resident localizable items"})
        checks.append({"name": "C2 compression_attributable_loss==0", "ok": abs(cal) <= tol,
                       "hard": True, "detail": f"cal={cal:.6f}"})
        if rep["n_gold_evicted"] == 0 and n_loc:
            checks.append({"name": "C2 headline==1.0 (no evictions)", "ok": abs(surv - 1.0) <= tol,
                           "hard": True, "detail": f"gold_answer_survival={surv:.4f}"})

    # Contract 3 — the tiers partition the localizable items.
    if n_loc:
        tot = rep["frac_retained"] + rep["frac_degraded"] + rep["frac_lost"]
        checks.append({"name": "C3 tiers sum to 1", "ok": abs(tot - 1.0) <= 1e-9,
                       "hard": True, "detail": f"sum={tot:.6f}"})
    return checks


def _read_gold_survival_rate(events_path: Path) -> float | None:
    """Pull the query-level gold_survival_rate from the run's summary.csv (sibling
    of events.jsonl) — the flat id-residency line answer-survival is replacing."""
    import csv
    p = events_path.parent / "summary.csv"
    if not p.exists():
        return None
    try:
        with p.open(encoding="utf-8") as f:
            row = next(csv.DictReader(f), None)
        return float(row["gold_survival_rate"]) if row and row.get("gold_survival_rate") else None
    except (OSError, ValueError, KeyError):
        return None


# ───────────────────────── orchestration ──────────────────────────


def analyze(run: Run, *, oracle_json: Path | None = None,
            random_query_rate: float | None = None,
            gold_snapshot_rows: list[dict] | None = None,
            gold_survival_rate: float | None = None) -> dict:
    """Annotation floor + magnitude + bracket + time-to-regret + recency calibration.
    The counterfactual stages are opt-in (need a retriever/reader); the funnel
    reports annotation-only here."""
    ann = annotation_regret_events(run)
    mag = magnitude(run, ann)

    bracket = None
    if oracle_json is not None and random_query_rate is not None:
        ceil = load_oracle_ceiling(oracle_json, run.dataset, run.budget_frac)
        if ceil is not None:
            bracket = Bracket(policy=mag.query_rate, oracle=ceil, random=random_query_rate)

    comp = compression_stats(run)                                            # Layer D

    out = {
        "dataset": run.dataset, "policy": run.policy, "budget_frac": run.budget_frac,
        "itemiser": run.meta.get("itemiser", ""), "retriever": run.meta.get("retriever", ""),
        "magnitude": mag.as_dict(),
        "funnel": Funnel(n_annotation=len(ann)).as_dict(),
        "bracket": (bracket.as_dict() if bracket else None),
        "time_to_regret": time_to_regret(run, ann).as_dict(),
        "eviction_classification": eviction_classification(run).as_dict(),   # Layer C
        "compression": comp,
        "value_calibration_recency": value_calibration(run),
    }

    # Tier-1 answer survival (the !A side) — only when the snapshot is present.
    if gold_snapshot_rows is not None:
        rep = answer_survival_report(gold_snapshot_rows,
                                     n_compress=int(comp["n_compressions"]))
        out["answer_survival"] = rep
        out["answer_survival_contracts"] = answer_survival_contracts(
            rep, gold_survival_rate=gold_survival_rate)
    return out


# ───────────────────────── CLI ────────────────────────────────────


def _parse_identity(events_path: Path) -> dict | None:
    """Recover (dataset, itemiser, policy, retriever, budget_frac, reader) from the
    artifact layout <out_dir>/<dataset>/<itemiser>/retention/<policy>/<retriever>/
    budget_NNNpct/<reader>/events.jsonl — robust to where out_dir sits."""
    parts = events_path.parts
    if "retention" not in parts:
        return None
    i = parts.index("retention")
    try:
        policy, retriever, budget = parts[i + 1], parts[i + 2], parts[i + 3]
        reader = parts[i + 4]
        dataset, itemiser = parts[i - 2], parts[i - 1]
    except IndexError:
        return None
    frac = 0.0
    if budget.startswith("budget_") and budget.endswith("pct"):
        try:
            frac = int(budget[len("budget_"):-len("pct")]) / 100.0
        except ValueError:
            pass
    return {"dataset": dataset, "itemiser": itemiser, "policy": policy,
            "retriever": retriever, "budget_frac": frac, "reader": reader}


def discover_runs(events_dir: Path) -> Iterator[tuple[Path, dict]]:
    for p in sorted(events_dir.rglob("events.jsonl")):
        ident = _parse_identity(p)
        if ident is not None:
            yield p, ident


def main() -> int:
    ap = argparse.ArgumentParser(description="Layer-B eviction-regret offline pass.")
    ap.add_argument("--events-dir", type=Path, required=True,
                    help="artifacts root containing <dataset>/.../events.jsonl (run with --event-log).")
    ap.add_argument("--oracle", type=Path, default=None,
                    help="oracle_size_report.json — enables the avoidable/unavoidable bracket.")
    ap.add_argument("--with-results", action="store_true",
                    help="also read each run's results.csv (abstained/question_type) for later stages.")
    ap.add_argument("--out", type=Path, default=Path("artifacts/eviction_regret_report.json"))
    args = ap.parse_args()

    runs: list[Run] = []
    for path, ident in discover_runs(args.events_dir):
        rcsv = (path.parent / "results.csv") if args.with_results else None
        run = load_run(path, dataset=ident["dataset"], policy=ident["policy"],
                       budget_frac=ident["budget_frac"], results_csv=rcsv)
        run.meta.update(ident)
        run.meta["events_path"] = str(path)            # to locate the gold snapshot
        runs.append(run)

    if not runs:
        print(f"no events.jsonl found under {args.events_dir} "
              f"(did you run with --event-log?)")
        return 1

    # Random floor per (dataset, itemiser, retriever, budget_frac): the random
    # policy's own annotation query_rate, measured in this same sweep.
    floor: dict[tuple, float] = {}
    for r in runs:
        if r.policy == "random":
            key = (r.dataset, r.meta.get("itemiser"), r.meta.get("retriever"), r.budget_frac)
            floor[key] = magnitude(r, annotation_regret_events(r)).query_rate

    reports = []
    print(f"{'dataset':14}{'policy':12}{'retr':9}{'frac':>5} | "
          f"{'q_regret':>9}{'items':>7}{'norm_avoid':>11}  ttr")
    for r in runs:
        key = (r.dataset, r.meta.get("itemiser"), r.meta.get("retriever"), r.budget_frac)
        epath = Path(r.meta["events_path"])
        snap = load_gold_snapshot(epath)
        rep = analyze(r, oracle_json=args.oracle, random_query_rate=floor.get(key),
                      gold_snapshot_rows=snap,
                      gold_survival_rate=_read_gold_survival_rate(epath))
        rep["itemiser"] = r.meta.get("itemiser", "")
        rep["retriever"] = r.meta.get("retriever", "")
        reports.append(rep)
        b = rep["bracket"]
        norm = f"{b['normalized_avoidable']:.3f}" if b else "    -"
        print(f"{r.dataset:14}{r.policy:12}{r.meta.get('retriever',''):9}{r.budget_frac:5.2f} | "
              f"{rep['magnitude']['query_rate']:9.3f}{rep['magnitude']['item_count']:7d}"
              f"{norm:>11}  {rep['time_to_regret']['verdict']}")

    # ── Tier-1 answer-survival table + contract validation (the !A side) ──
    n_violations = _print_answer_survival(reports)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(reports, indent=2))
    print(f"\nwrote {args.out}  ({len(reports)} runs)")
    if n_violations:
        print(f"\n✗ {n_violations} HARD answer-survival contract violation(s) — see table above.")
        return 2
    return 0


def _fmt(x, w=8, p=3) -> str:
    """Right-aligned float that renders NaN as a dash."""
    try:
        if x != x:                       # NaN
            return f"{'-':>{w}}"
        return f"{x:{w}.{p}f}"
    except (TypeError, ValueError):
        return f"{str(x):>{w}}"


def _print_answer_survival(reports: list[dict]) -> int:
    """Print the per-config answer-survival rollups and the contract validation
    table; return the number of HARD contract violations. Configs without a gold
    snapshot are listed so a missing-snapshot run is never silently skipped."""
    have = [r for r in reports if "answer_survival" in r]
    missing = [r for r in reports if "answer_survival" not in r]

    print("\n══ Tier-1 gold ANSWER survival (resident-text containment; localizable, "
          "answerable items) ══")
    if not have:
        print("  no gold_answer_snapshot.jsonl found for any config — re-run the "
              "sweep with --event-log on the updated runner to emit it.")
        return 0

    print(f"{'dataset':12}{'itemiser':10}{'policy':12}{'retr':7}{'frac':>5} | "
          f"{'ans_surv':>9}{'ans_rec':>9}{'id_surv':>9}{'cmp_loss':>9} | "
          f"{'nLoc':>5}{'nNonL':>6}{'nEvct':>6}{'nCmp':>5} | ret/deg/lost")
    for r in have:
        a = r["answer_survival"]
        tier = f"{_fmt(a['frac_retained'],4,2)}/{_fmt(a['frac_degraded'],4,2)}/{_fmt(a['frac_lost'],4,2)}"
        print(f"{r['dataset']:12}{r.get('itemiser',''):10}{r['policy']:12}"
              f"{r.get('retriever',''):7}{r['budget_frac']:5.2f} | "
              f"{_fmt(a['gold_answer_survival'],9)}{_fmt(a['gold_answer_recall'],9)}"
              f"{_fmt(a['gold_id_survival'],9)}{_fmt(a['compression_attributable_loss'],9)} | "
              f"{a['n_localizable']:5d}{a['n_non_localizable']:6d}{a['n_gold_evicted']:6d}"
              f"{a['n_compressed_localizable']:5d} | {tier}")

    if missing:
        print(f"\n  ({len(missing)} config(s) had no snapshot and were skipped for "
              f"answer-survival: " + ", ".join(sorted(
                  f"{m['policy']}/{m.get('retriever','')}/{m['budget_frac']:.2f}" for m in missing)) + ")")

    # Contract validation table — like the existing 0/60-mismatch check.
    print("\n── contract validation ──")
    n_violations = 0
    for r in have:
        label = f"{r.get('itemiser',''):>8}/{r['policy']}/{r.get('retriever','')}/{r['budget_frac']:.2f}"
        for c in r["answer_survival_contracts"]:
            hard = c.get("hard", True)
            ok = c["ok"]
            mark = "✓" if ok else "✗"
            tag = "" if hard else " (info)"
            if hard and not ok:
                n_violations += 1
            print(f"  {mark} {label:34} {c['name']:38}{tag}  {c['detail']}")
    return n_violations


if __name__ == "__main__":
    raise SystemExit(main())
