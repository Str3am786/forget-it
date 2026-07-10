"""Generate compression_pairs.yaml (corpus_a_judge, D5) per
golden_corpora/corpus_a_judge/compression_pairs_INSTRUCTIONS.md.

- 25 W7-style synthetic fact items (dates, numbers, names, ordered lists —
  facts enumerable by construction), round-1.
- 25 REAL BEAM turn_pair gold items (sampled seeded from the budget_100pct
  gold snapshot, answers verbatim-locatable in the original), stratified:
  15 round-1, 10 round-2 (the round-1 output compressed AGAIN — the
  re-compression decay measurement needs round-2 coverage).
- Compressor = the PRODUCTION path verbatim: LLMCompressionPolicy's prompt +
  _sanitise on the pinned chat model, temperature 0, max_tokens 128.
- Output rows follow the INSTRUCTIONS spec exactly: {pair_id, class, round,
  question(s), original_text, compressed_text, compressor_tag, size_before,
  size_after, author_label: null, verified: false}. NO answer fields, NO
  model-suggested labels — the blind protocol is the author's.
"""
from __future__ import annotations

import json
import random
from pathlib import Path

import yaml

from forgetit.benchmark.grading import answer_in_text
from forgetit.llm.openai_llm import OpenAiLLM
from forgetit.policies.llm_compression import _COMPRESS_PROMPT, _sanitise
from forgetit.settings import LLM_API_KEY, LLM_BASE_URL, LLM_MODEL

REPO = Path(__file__).resolve().parents[1]
SNAPSHOT = (REPO / "artifacts" / "benchmark_full" / "beam" / "beam_turn_pair"
            / "retention" / "lru" / "bm25" / "budget_100pct" / "PlainTextReader"
            / "gold_answer_snapshot.jsonl")
RESULTS = SNAPSHOT.parent / "results.csv"
OUT = REPO / "golden_corpora" / "corpus_a_judge" / "compression_pairs.yaml"
SEED = 20260708

_NAMES = ["Ruth Salazar", "Omar Haddad", "Ines Aalto", "Viktor Brandt",
          "Leila Osei", "Marco Petrov", "Sanne Vogel", "Tomasz Lindqvist"]
_PLACES = ["the Corner Forge hall", "Riverside Annex", "the Old Granary",
           "Harbourview Loft", "the Cedar Pavilion"]
_LISTS = [["soundcheck", "dinner", "speeches", "dancing"],
          ["registration", "keynote", "workshops", "closing panel"],
          ["loading", "inspection", "sealing", "dispatch"],
          ["priming", "base coat", "detail work", "varnish"]]


def synthetic_item(rng: random.Random, i: int) -> tuple[str, list[str]]:
    name = rng.choice(_NAMES)
    place = rng.choice(_PLACES)
    amount = rng.randrange(120, 980, 10)
    day = rng.randrange(2, 28)
    month = rng.choice(["March", "April", "May", "June", "September"])
    ext = rng.randrange(2000, 9900)
    hour = rng.choice(["6pm", "7pm", "5:30pm", "8pm"])
    steps = rng.choice(_LISTS)
    text = (
        f"Planning notes, item {i + 1}. The booking at {place} is confirmed for "
        f"{month} {day}. The deposit of {amount} euros must be paid ten days "
        f"before the event. The contact person is {name}, reachable on extension "
        f"{ext}. Setup begins at {hour}. The agreed running order is: "
        f"{steps[0]}, then {steps[1]}, then {steps[2]}, and finally {steps[3]}. "
        f"Two volunteers handle the cloakroom, and parking passes are collected "
        f"at the side entrance on arrival."
    )
    questions = [
        f"How much is the deposit for the booking at {place}?",
        f"What is the agreed running order for the event on {month} {day}?",
    ]
    return text, questions


def load_beam_candidates() -> list[dict]:
    q_text = {}
    import csv
    with RESULTS.open() as f:
        for row in csv.DictReader(f):
            q_text[row["query_id"]] = row["question_text"]
    seen: dict[str, dict] = {}
    with SNAPSHOT.open() as f:
        for line in f:
            r = json.loads(line)
            gid = r["gold_id"]
            if gid in seen or r["orig_bytes"] < 400:
                continue
            if not r["answer"] or not answer_in_text(r["answer"], r["original_text"]):
                continue                     # answer must be verbatim-locatable
            q = q_text.get(r["query_id"])
            if not q:
                continue
            seen[gid] = {"gold_id": gid, "question": q,
                         "original_text": r["original_text"]}
    return list(seen.values())


def compress(llm, text: str) -> str:
    raw = llm.generate(_COMPRESS_PROMPT.format(text=text),
                       max_tokens=128, temperature=0.0)
    return _sanitise(raw)


def main() -> None:
    rng = random.Random(SEED)
    llm = OpenAiLLM(base_url=LLM_BASE_URL, api_key=LLM_API_KEY, model=LLM_MODEL)
    rows = []

    for i in range(25):
        text, questions = synthetic_item(rng, i)
        comp = compress(llm, text)
        rows.append({
            "pair_id": f"cp{len(rows)+1:03d}", "class": "w7_synthetic", "round": 1,
            "questions": questions, "original_text": text, "compressed_text": comp,
            "compressor_tag": LLM_MODEL, "size_before": len(text.encode()),
            "size_after": len(comp.encode()),
            "author_label": None, "verified": False,
        })
        print(f"cp{len(rows):03d} w7 r1 {len(text)}B→{len(comp)}B")

    candidates = load_beam_candidates()
    rng.shuffle(candidates)
    beam = candidates[:25]
    if len(beam) < 25:
        print(f"WARNING: only {len(beam)} eligible BEAM candidates")
    for j, cand in enumerate(beam):
        target_round = 1 if j < 15 else 2
        comp1 = compress(llm, cand["original_text"])
        if target_round == 1:
            original, comp, rnd = cand["original_text"], comp1, 1
        else:
            comp2 = compress(llm, comp1)
            # round-2 pair: original stays the TRUE original; compressed is the
            # twice-compressed text (recoverability across rounds is the point).
            original, comp, rnd = cand["original_text"], comp2, 2
        rows.append({
            "pair_id": f"cp{len(rows)+1:03d}", "class": "beam_real", "round": rnd,
            "beam_gold_id": cand["gold_id"],
            "questions": [cand["question"]],
            "original_text": original, "compressed_text": comp,
            "compressor_tag": LLM_MODEL,
            "size_before": len(original.encode()), "size_after": len(comp.encode()),
            "author_label": None, "verified": False,
        })
        print(f"cp{len(rows):03d} beam r{rnd} {len(original)}B→{len(comp)}B")

    header = ("# compression_pairs — generated by scripts/gen_compression_pairs.py "
              f"(seed {SEED}) per compression_pairs_INSTRUCTIONS.md.\n"
              "# author_label is null EVERYWHERE by design: the author labels blind "
              "(compressed+question first). No model suggestions anywhere.\n")
    OUT.write_text(header + yaml.safe_dump(rows, sort_keys=False, width=100))
    print(f"wrote {OUT} ({len(rows)} pairs)")


if __name__ == "__main__":
    main()
