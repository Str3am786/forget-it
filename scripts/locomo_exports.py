#!/usr/bin/env python3
"""
LoCoMo author-review exports (deterministic, seeded, no model judgments).

Two blinded CSVs the brief asks for, both for the AUTHOR's manual pass — nothing
here is a model correctness opinion:

  L2  mapping-fixture   — 10 QA stratified over (category × itemiser), showing
                          question, the raw evidence turn text, and the item text
                          the evidence mapped to. ``verified=false`` for the
                          author to tick. Guards the known gold-mapping-drift
                          conjured-error class.

  L4  label-audit       — 50 QA stratified over all five categories, showing
                          question, the official answer key, and the mapped
                          evidence text, with rows and evidence order randomized
                          (fixed seed → reproducible). Purpose: the author
                          hand-checks the claimed ~6.4% answer-key error rate on
                          OUR sample before any LoCoMo number is trusted.

Both are pure functions of ``locomo10.json`` + the LoCoMo adapter — no LLM, no
embedder, no network. Seed is fixed so the exports are byte-reproducible (a
deterministic check, permitted under the execution fence).

Run:
    python scripts/locomo_exports.py mapping-fixture
    python scripts/locomo_exports.py label-audit
    python scripts/locomo_exports.py all
"""
from __future__ import annotations

import argparse
import csv
import random
from pathlib import Path
from typing import Any

from forgetit.benchmark.workloads.locomo import (
    LocomoWorkload,
    LocomoTurnItemiser,
    LocomoTurnPairItemiser,
    LocomoSessionItemiser,
    _turn_text,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA = REPO_ROOT / "data" / "locomo10.json"
OUT = REPO_ROOT / "report_locomo"

CATEGORIES = ["single_hop", "multi_hop", "temporal", "commonsense", "adversarial"]
ITEMISERS = {
    "turn": LocomoTurnItemiser,
    "turn_pair": LocomoTurnPairItemiser,
    "session": LocomoSessionItemiser,
}
SEED = 20240614   # fixed — reproducible shuffles (Date/random are otherwise nondet)


def _dia_text_map(conv) -> dict[str, str]:
    """dia_id → rendered turn text, for showing the raw evidence turn."""
    out: dict[str, str] = {}
    for t in conv.all_turns():
        out[t.dia_id] = _turn_text(t)
    return out


def _item_text_map(items) -> dict[str, str]:
    return {it.id: it.text for it in items}


def _gather(conv_list):
    """Pre-compute, per conversation, the per-itemiser (key_to_item, item_text)
    and the dia→text map — so both exporters share one pass."""
    ctx = {}
    for conv in conv_list:
        per_it = {}
        for iname, icls in ITEMISERS.items():
            items, ktoi = icls().itemise(conv.conversation_id, 0.0, conv.sessions)
            per_it[iname] = (icls(), ktoi, _item_text_map(items))
        ctx[conv.conversation_id] = {"dia": _dia_text_map(conv), "per_it": per_it}
    return ctx


def export_mapping_fixture(path: Path = OUT / "l2_mapping_fixture.csv") -> int:
    """10 QA stratified over category × itemiser; blinded (verified=false)."""
    wl = LocomoWorkload(str(DATA), LocomoTurnItemiser(), probe_every=0)
    convs = wl.conversations()
    ctx = _gather(convs)
    rng = random.Random(SEED)

    # Flat pool of (conv, qa) with resolvable gold, grouped by category.
    by_cat: dict[str, list[tuple[Any, Any]]] = {c: [] for c in CATEGORIES}
    for conv in convs:
        for qa in conv.qa:
            if qa.category_name in by_cat and qa.evidence_keys:
                by_cat[qa.category_name].append((conv, qa))

    # 10 rows: cycle categories × itemisers so the sample is stratified over both.
    plan = [(c, i) for c in CATEGORIES for i in ITEMISERS]      # 15 combos
    rng.shuffle(plan)
    plan = plan[:10]

    rows = []
    for fid, (cat, iname) in enumerate(plan):
        pool = by_cat.get(cat) or []
        if not pool:
            continue
        conv, qa = rng.choice(pool)
        it, ktoi, itext = ctx[conv.conversation_id]["per_it"][iname]
        dia = ctx[conv.conversation_id]["dia"]
        ev_text = " ||| ".join(dia.get(k, f"<UNRESOLVED {k}>") for k in qa.evidence_keys)
        gold_ids = it.resolve_gold(qa.evidence_keys, ktoi)
        mapped = " ||| ".join(itext.get(g, f"<MISSING {g}>") for g in gold_ids)
        rows.append({
            "fixture_id": fid,
            "conversation_id": conv.conversation_id,
            "itemiser": iname,
            "category_name": cat,
            "question": qa.question,
            "evidence_dia_ids": ",".join(qa.evidence_keys),
            "evidence_turn_text": ev_text,
            "mapped_item_ids": ",".join(gold_ids),
            "mapped_item_text": mapped,
            "verified": "false",
        })

    _write(path, rows)
    return len(rows)


def export_label_audit(path: Path = OUT / "l4_label_audit.csv", n: int = 50) -> int:
    """50 QA stratified over all five categories; answer key + evidence order
    randomized (seeded). No model correctness opinions."""
    wl = LocomoWorkload(str(DATA), LocomoTurnItemiser(), probe_every=0)
    convs = wl.conversations()
    ctx = _gather(convs)
    rng = random.Random(SEED + 1)

    by_cat: dict[str, list[tuple[Any, Any]]] = {c: [] for c in CATEGORIES}
    for conv in convs:
        for qa in conv.qa:
            if qa.category_name in by_cat:
                by_cat[qa.category_name].append((conv, qa))

    per_cat = n // len(CATEGORIES)             # 10 each → 50
    picked: list[tuple[Any, Any]] = []
    for cat in CATEGORIES:
        pool = by_cat[cat][:]
        rng.shuffle(pool)
        picked.extend(pool[:per_cat])
    rng.shuffle(picked)                        # randomize row order across the file

    rows = []
    for aid, (conv, qa) in enumerate(picked):
        dia = ctx[conv.conversation_id]["dia"]
        ev = list(qa.evidence_keys)
        rng.shuffle(ev)                        # randomize evidence order within the row
        ev_text = " ||| ".join(dia.get(k, f"<UNRESOLVED {k}>") for k in ev)
        rows.append({
            "audit_id": aid,
            "conversation_id": conv.conversation_id,
            "category_name": qa.category_name,
            "question": qa.question,
            # For adversarial the official key is "no answer in conversation";
            # the distractor is shown separately so the author can judge the key.
            "answer_key": qa.answer if qa.category_name != "adversarial" else "(adversarial: expected no answer)",
            "adversarial_answer": qa.adversarial_answer,
            "evidence_text": ev_text,
        })

    _write(path, rows)
    return len(rows)


def _write(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("what", choices=["mapping-fixture", "label-audit", "all"])
    args = ap.parse_args()
    if args.what in ("mapping-fixture", "all"):
        n = export_mapping_fixture()
        print(f"wrote {OUT / 'l2_mapping_fixture.csv'}  ({n} rows, verified=false)")
    if args.what in ("label-audit", "all"):
        n = export_label_audit()
        print(f"wrote {OUT / 'l4_label_audit.csv'}  ({n} rows, seed={SEED + 1})")


if __name__ == "__main__":
    main()
