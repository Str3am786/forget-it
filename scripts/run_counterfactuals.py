"""D6 — forced counterfactuals on annotated BEAM (the integration ground truth).

Five BEAM conversations, turn_pair × bm25, budget 25 %, reader at the
D4-fixed configuration on the 16k tag. Two runs per conversation:

  PIN   — eviction can never select a gold id (PinnedPolicy(LRU, pin set =
          union of gold over the conversation's scored queries); everything
          else evicts per LRU). Gold survival is 1.0 BY CONSTRUCTION and is
          asserted; every downstream failure is then provably not the
          policy's. Reported per ability: gold-in-top-k, answer_recall on the
          EXACT evidence block the reader saw, IDK rate — and the headline:
          IDK on rows with answer_recall ≥ 0.7 = the pure reader
          false-refusal rate on real data.
  PURGE — the conversation replays under plain LRU, then every surviving
          gold id is force-evicted BEFORE the scored block (survival 0.0,
          asserted). Correct behavior is abstention; every confident correct
          answer is a parametric-knowledge/contamination leak. Reported as a
          per-question contamination map + leak rates (strict = deterministic
          qa_correct; soft = token-F1 ≥ 0.3 while not abstaining).

Conversation selection: the first five (in workload order) whose pin set fits
within 80 % of the 25 % budget — a conversation whose gold alone overflows
the budget cannot host the pin construction; skipped ones are listed in the
output. Abstention-ability questions carry no gold ids and are excluded from
both runs' scored sets (they have no pin/purge semantics).

Usage:  python scripts/run_counterfactuals.py
Output: report/06_counterfactual_runs.json (+ printed tables)
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

from forgetit.backend.in_memory import InMemoryBackend
from forgetit.benchmark.grading import answer_recall
from forgetit.benchmark.runner_core import _run_reader
from forgetit.benchmark.workloads.beam import BeamWorkload, BeamTurnPairItemiser
from forgetit.core.cost import record_cost
from forgetit.llm.openai_llm import OpenAiLLM
from forgetit.llm.readers import PlainTextReader
from forgetit.llm.readers.text_reader import _build_evidence_budgeted
from forgetit.memory_system import RetentionManager
from forgetit.policies import LRUPolicy, PinnedPolicy
from forgetit.retrieval import BM25Retriever
from forgetit.settings import LLM_API_KEY, LLM_BASE_URL

REPO = Path(__file__).resolve().parents[1]
BEAM = REPO / "data" / "beam_500K.jsonl"
MODEL_16K = "qwen2.5:7b-instruct-q5km-16k"
BUDGET_FRAC = 0.25
K = 5
READER_TOKEN_BUDGET = 6000       # ≈ five median full turn-pair items (E6c calibration)
ANSWER_MAX_TOKENS = 128
ANSWER_MAX_TOKENS_LIST = 384
VISIBLE = 0.7
SOFT_F1 = 0.3
N_CONVERSATIONS = 5
PIN_FIT_FRACTION = 0.8


class _L:
    def event(self, *a, **k):
        pass


def split_events(events):
    inserts, probes, scored = [], [], []
    for e in events:
        if e.type == "insert":
            inserts.append(e)
        elif e.type == "query" and not e.gold_item_ids:
            if "::probe::" in e.query.id:
                probes.append(e)
            # gold-less non-probe (abstention ability) — excluded, no pin/purge semantics
        else:
            scored.append(e)
    return inserts, probes, scored


def build_store(policy, budget):
    store = RetentionManager(budget_bytes=budget,
                             backend=InMemoryBackend(retriever=BM25Retriever()),
                             policy=policy, logger=_L())
    store.connect()
    return store


def replay_pressure_phase(store, events):
    """Inserts + interleaved probes, in original event order (probes touch)."""
    for e in events:
        if e.type == "insert":
            store.insert(e.item)
        elif e.type == "query":
            store.retrieve(e.query, k=K)


def answer_scored(store, scored, reader):
    rows = []
    for e in scored:
        retrieved = store.retrieve(e.query, k=K)
        gold = set(e.gold_item_ids)
        resident = set(store.iter_ids())
        survived = sorted(gold & resident)
        shown, _ = _build_evidence_budgeted(retrieved, READER_TOKEN_BUDGET)
        qa = _run_reader(reader_impl=reader, query=e.query, retrieved=retrieved,
                         answer_max_tokens=ANSWER_MAX_TOKENS,
                         ref_answer=e.ref_answer, question_type=e.question_type,
                         abstention_accounting=True,
                         answer_max_tokens_list=ANSWER_MAX_TOKENS_LIST)
        rows.append({
            "query_id": e.query.id, "ability": e.question_type,
            "n_gold": len(gold), "gold_survived_n": len(survived),
            "gold_in_topk": sum(1 for r in retrieved if r.id in gold),
            "answer_recall_shown": round(answer_recall(e.ref_answer, shown), 4),
            "abstained": qa.abstained, "qa_correct": qa.qa_correct,
            "qa_f1": round(qa.qa_f1, 4), "answer": qa.reader_answer[:200],
            "ref": e.ref_answer[:200],
        })
    return rows


def main() -> None:
    wl = BeamWorkload(BEAM, itemiser=BeamTurnPairItemiser(), probe_every=1,
                      clear_between_examples=True, limit=25)
    llm = OpenAiLLM(base_url=LLM_BASE_URL, api_key=LLM_API_KEY, model=MODEL_16K)
    reader = PlainTextReader(llm, token_budget=READER_TOKEN_BUDGET,
                             prompt_version="v2", abstention_match="exact",
                             clean_mode="full")

    picked, skipped = [], []
    for conv_id, events in wl.iter_examples():
        inserts, probes, scored = split_events(events)
        footprint = sum(record_cost(e.item) for e in inserts)
        budget = max(1, round(BUDGET_FRAC * footprint))
        gold_union = sorted({g for e in scored for g in e.gold_item_ids})
        bytes_by_id = {e.item.id: record_cost(e.item) for e in inserts}
        gold_bytes = sum(bytes_by_id[g] for g in gold_union if g in bytes_by_id)
        if gold_bytes <= PIN_FIT_FRACTION * budget:
            picked.append((conv_id, events, budget, gold_union))
            if len(picked) == N_CONVERSATIONS:
                break
        else:
            skipped.append({"conversation": conv_id, "gold_bytes": gold_bytes,
                            "budget": budget,
                            "reason": "pin set exceeds 80% of budget"})

    out = {"config": {"model": MODEL_16K, "reader": reader.name,
                      "budget_frac": BUDGET_FRAC, "k": K,
                      "reader_token_budget": READER_TOKEN_BUDGET,
                      "pin_fit_fraction": PIN_FIT_FRACTION},
           "skipped_conversations": skipped, "conversations": []}

    for conv_id, events, budget, gold_union in picked:
        pressure = [e for e in events
                    if e.type == "insert"
                    or (e.type == "query" and not e.gold_item_ids)]
        _, _, scored = split_events(events)
        print(f"conversation {conv_id}: budget={budget}B pin_set={len(gold_union)} "
              f"scored={len(scored)}")

        # ── PIN ──
        store = build_store(PinnedPolicy(LRUPolicy(), gold_union), budget)
        replay_pressure_phase(store, pressure)
        resident = set(store.iter_ids())
        missing = [g for g in gold_union if g not in resident]
        assert not missing, f"PIN INVARIANT BROKEN: gold evicted {missing}"
        pin_rows = answer_scored(store, scored, reader)
        for r in pin_rows:
            assert r["gold_survived_n"] == r["n_gold"], \
                f"PIN INVARIANT BROKEN on {r['query_id']}"

        # ── PURGE ──
        store = build_store(LRUPolicy(), budget)
        replay_pressure_phase(store, pressure)
        resident = set(store.iter_ids())
        for g in gold_union:
            if g in resident:
                store.force_evict(g)
        left = set(store.iter_ids()) & set(gold_union)
        assert not left, f"PURGE INVARIANT BROKEN: gold resident {sorted(left)}"
        purge_rows = answer_scored(store, scored, reader)
        for r in purge_rows:
            assert r["gold_survived_n"] == 0, \
                f"PURGE INVARIANT BROKEN on {r['query_id']}"

        out["conversations"].append({
            "conversation": conv_id, "budget_bytes": budget,
            "pin_set_size": len(gold_union),
            "pin": pin_rows, "purge": purge_rows,
        })

    # ── aggregates ──
    pin_all = [r for c in out["conversations"] for r in c["pin"]]
    purge_all = [r for c in out["conversations"] for r in c["purge"]]

    def by_ability(rows):
        buckets = defaultdict(list)
        for r in rows:
            buckets[r["ability"]].append(r)
        table = {}
        for ab, rs in sorted(buckets.items()):
            table[ab] = {
                "n": len(rs),
                "gold_in_topk_rate": round(
                    sum(1 for r in rs if r["gold_in_topk"] > 0) / len(rs), 3),
                "mean_answer_recall_shown": round(
                    sum(r["answer_recall_shown"] for r in rs) / len(rs), 3),
                "idk_rate": round(sum(r["abstained"] for r in rs) / len(rs), 3),
            }
        return table

    visible = [r for r in pin_all if r["answer_recall_shown"] >= VISIBLE]
    false_refusal = (sum(r["abstained"] for r in visible) / len(visible)
                     if visible else None)
    out["pin_summary"] = {
        "n_rows": len(pin_all), "by_ability": by_ability(pin_all),
        "n_visible_rows": len(visible),
        "pure_reader_false_refusal_on_visible": round(false_refusal, 4)
        if false_refusal is not None else None,
    }

    leaks_strict = [r for r in purge_all if not r["abstained"] and r["qa_correct"]]
    leaks_soft = [r for r in purge_all
                  if not r["abstained"] and r["qa_f1"] >= SOFT_F1]
    out["purge_summary"] = {
        "n_rows": len(purge_all),
        "abstention_rate": round(sum(r["abstained"] for r in purge_all)
                                 / len(purge_all), 4),
        "leak_rate_strict_qa_correct": round(len(leaks_strict) / len(purge_all), 4),
        "leak_rate_soft_f1>=0.3": round(len(leaks_soft) / len(purge_all), 4),
        "contamination_map": [
            {"query_id": r["query_id"], "ability": r["ability"],
             "f1": r["qa_f1"], "answer": r["answer"], "ref": r["ref"]}
            for r in sorted(purge_all, key=lambda x: -x["qa_f1"])
            if not r["abstained"] and r["qa_f1"] >= SOFT_F1
        ],
    }

    (REPO / "report" / "06_counterfactual_runs.json").write_text(
        json.dumps(out, indent=1))
    print(json.dumps({"pin_summary": out["pin_summary"],
                      "purge_summary": {k: v for k, v in out["purge_summary"].items()
                                        if k != "contamination_map"},
                      "n_contamination_rows":
                      len(out["purge_summary"]["contamination_map"]),
                      "skipped": skipped}, indent=1))
    print("wrote report/06_counterfactual_runs.json")


if __name__ == "__main__":
    main()
