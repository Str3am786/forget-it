"""Reader probe bench — THE A/B instrument for every reader/prompt change (D4).

Provenance: versioned from the 2026-07-08 session-scratchpad probe that
produced docs/reader_investigation_2026-07-08.md (same design: one BEAM
conversation, budget 1.0 so eviction plays no role, turn_pair × bm25, k=5,
17 scored queries × 4 context conditions = 68 real reader calls). This copy is
parametrized over the D4 fix flags so an A/B is two invocations:

    python scripts/reader_probe.py --label B0_baseline --model qwen2.5:7b-instruct-q5km-16k
    python scripts/reader_probe.py --label A4_pv2 --prompt-version v2 --model qwen2.5:7b-instruct-q5km-16k

Outputs per run: report/probe_runs/<label>.jsonl (row-level: evidence stats,
raw/cleaned answers, grades) + <label>.summary.json + a printed summary.

Bench metrics (per condition):
  idk               — abstention count / 17
  correct           — deterministic qa_correct count (legacy grade; the July-08
                      baseline is 0/68 across all conditions)
  f1_cleaned/f1_raw — mean token-F1 of cleaned vs raw output
  false_refusal     — IDK on VISIBLE rows (answer_recall(ref, shown) ≥ 0.7):
                      the reader refusing answers it was shown
  answered_absent   — non-IDK on ABSENT rows (recall < 0.3): hallucination proxy
  sent_boundary     — answered rows whose RAW output ends at a sentence
                      boundary (fix-iii acceptance: ≥ 0.95)

NOTE: the July-08 numbers were measured on the 4096-ctx tag (before the
front-truncation discovery); this bench defaults to the 16k tag, so absolute
baselines may differ from that report — A/Bs are valid only within one tag.
"""
from __future__ import annotations

import argparse
import json
import re
import time
from pathlib import Path

from forgetit.benchmark.grading import grade, answer_recall
from forgetit.benchmark.workloads.beam import BeamWorkload, BeamTurnPairItemiser
from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.logging import BenchmarkLogger
from forgetit.core.schema import Question
from forgetit.llm.openai_llm import OpenAiLLM
from forgetit.llm.readers import PlainTextReader
from forgetit.memory_system import RetentionManager
from forgetit.policies import LRUPolicy
from forgetit.retrieval import BM25Retriever
from forgetit.settings import LLM_API_KEY, LLM_BASE_URL

REPO = Path(__file__).resolve().parents[1]
BEAM = REPO / "data" / "beam_500K.jsonl"
OUT_DIR = REPO / "report" / "probe_runs"
K = 5
CONDITIONS = {"legacy": None, "rtok375": 375, "rtok1500": 1500, "rtok3000": 3000}

VISIBLE = 0.7      # answer_recall(ref, shown evidence) at/above → answer was shown
ABSENT = 0.3       # below → answer was (essentially) not shown
_SENT_END = re.compile(r'[.!?]["\')\]]?\s*$')


def evidence_of(reader: PlainTextReader, records) -> str:
    """The evidence block exactly as this reader's prompt builder renders it."""
    from forgetit.llm.readers.text_reader import (
        _build_prompt, _build_prompt_budgeted, PROMPT_BUILDERS)
    q = Question(id="x", text="Q", timestamp=0.0, meta={})
    wrap = PROMPT_BUILDERS[reader.prompt_version]
    if reader.token_budget is None:
        prompt = _build_prompt(q, records[:5], wrap=wrap)
    else:
        prompt, _ = _build_prompt_budgeted(q, records, reader.token_budget, wrap=wrap)
    return prompt.split("Memories:\n", 1)[1].rsplit("\n\nQuestion:", 1)[0]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True, help="run label → output file names")
    ap.add_argument("--model", default="qwen2.5:7b-instruct-q5km-16k")
    ap.add_argument("--conditions", default=",".join(CONDITIONS),
                    help=f"comma list from {list(CONDITIONS)}")
    ap.add_argument("--prompt-version", choices=["v1", "v2"], default="v1")
    ap.add_argument("--abstention-match", choices=["loose", "exact"], default="loose")
    ap.add_argument("--clean-mode", choices=["firstline", "full"], default="firstline")
    ap.add_argument("--max-tokens", type=int, default=128)
    ap.add_argument("--max-tokens-list", type=int, default=0,
                    help="raised cap for event_ordering/summarization rows (0=off)")
    args = ap.parse_args()

    conds = {c: CONDITIONS[c] for c in args.conditions.split(",")}
    llm = OpenAiLLM(base_url=LLM_BASE_URL, api_key=LLM_API_KEY, model=args.model)
    readers = {name: PlainTextReader(llm, token_budget=tb,
                                     prompt_version=args.prompt_version,
                                     abstention_match=args.abstention_match,
                                     clean_mode=args.clean_mode)
               for name, tb in conds.items()}

    wl = BeamWorkload(BEAM, itemiser=BeamTurnPairItemiser(), probe_every=0,
                      clear_between_examples=True, limit=1)
    (example_id, events), = list(wl.iter_examples())

    class _L:
        def event(self, *a, **k): pass
    store = RetentionManager(budget_bytes=1 << 62,
                             backend=InMemoryBackend(retriever=BM25Retriever()),
                             policy=LRUPolicy(), logger=_L())
    store.connect()
    for e in events:
        if e.type == "insert":
            store.insert(e.item)

    from forgetit.llm.readers.text_reader import LIST_ANSWER_TYPES
    scored = [e for e in events if e.type == "query" and e.gold_item_ids]
    print(f"example={example_id} scored_queries={len(scored)} "
          f"items={store.backend.count()} model={args.model} label={args.label}")

    rows = []
    t0 = time.time()
    for e in scored:
        retrieved = store.retrieve(e.query, k=K)
        q = Question(id=e.query.id, text=e.query.text, timestamp=0.0, meta={})
        max_tok = args.max_tokens
        if args.max_tokens_list > 0 and e.question_type in LIST_ANSWER_TYPES:
            max_tok = args.max_tokens_list
        row = dict(query_id=e.query.id, qtype=e.question_type, ref=e.ref_answer,
                   n_gold=len(e.gold_item_ids),
                   gold_in_top5=sum(1 for g in e.gold_item_ids
                                    if g in [r.id for r in retrieved]),
                   max_tokens=max_tok, conds={})
        for name, reader in readers.items():
            ev = evidence_of(reader, retrieved)
            res = reader.answer(q, retrieved, max_tokens=max_tok)
            raw = str(res.meta.get("raw_output", ""))
            g = grade(res.answer, e.ref_answer, abstained=res.abstained)
            g_raw = grade(raw, e.ref_answer, abstained=res.abstained)
            row["conds"][name] = dict(
                shown_chars=len(ev),
                recall_ref_in_shown=round(answer_recall(e.ref_answer, ev), 4),
                answer=res.answer, raw=raw[:600], abstained=res.abstained,
                f1=round(g["f1"], 4), correct=g["correct"],
                f1_raw=round(g_raw["f1"], 4), correct_raw=g_raw["correct"],
                ends_sentence=bool(_SENT_END.search(raw.strip())) if raw.strip() else False,
            )
        rows.append(row)
        print(f"  {e.query.id[:44]:44s} done", flush=True)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / f"{args.label}.jsonl").write_text(
        "\n".join(json.dumps(r) for r in rows))

    summary = {"label": args.label, "model": args.model,
               "prompt_version": args.prompt_version,
               "abstention_match": args.abstention_match,
               "clean_mode": args.clean_mode, "max_tokens": args.max_tokens,
               "max_tokens_list": args.max_tokens_list,
               "elapsed_s": round(time.time() - t0, 1), "conditions": {}}
    for name in readers:
        cs = [r["conds"][name] for r in rows]
        vis = [c for c in cs if c["recall_ref_in_shown"] >= VISIBLE]
        absent = [c for c in cs if c["recall_ref_in_shown"] < ABSENT]
        answered = [c for c in cs if not c["abstained"]]
        summary["conditions"][name] = {
            "n": len(cs),
            "idk": sum(c["abstained"] for c in cs),
            "correct": sum(c["correct"] for c in cs),
            "f1_cleaned": round(sum(c["f1"] for c in cs) / len(cs), 4),
            "f1_raw": round(sum(c["f1_raw"] for c in cs) / len(cs), 4),
            "n_visible": len(vis),
            "false_refusal": (round(sum(c["abstained"] for c in vis) / len(vis), 4)
                              if vis else None),
            "n_absent": len(absent),
            "answered_absent": (round(sum(not c["abstained"] for c in absent) / len(absent), 4)
                                if absent else None),
            "sent_boundary": (round(sum(c["ends_sentence"] for c in answered) / len(answered), 4)
                              if answered else None),
        }
    (OUT_DIR / f"{args.label}.summary.json").write_text(json.dumps(summary, indent=1))
    print(json.dumps(summary["conditions"], indent=1))
    print(f"wrote {OUT_DIR}/{args.label}.jsonl + .summary.json")


if __name__ == "__main__":
    main()
