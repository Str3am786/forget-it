"""S6 — instrument error budget v2.

Deterministic bounds recomputed from the GROWN corpora (n=150 each at zero
failures → rule-of-three 95 % bound 0.02); reader terms are PENDING slots
that fill themselves from returned candidate scorecards
(report2/candidate_runs/*.json, smoke rows excluded), with the answer-stage
floor formula pre-wired per candidate. The judge term stays not-measured —
never estimated — until the author's κ labels exist.

Rerun (idempotent; safe after every returned scorecard):
    python scripts/build_error_budget_v2.py
"""
from __future__ import annotations

import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
CAND_DIR = REPO / "report2" / "candidate_runs"
OUT = REPO / "report2" / "instrument_error_budget_v2.json"

# n = 150 at zero failures, verified by tests/corpora (451 green):
DET_N = 150
DET_BOUND = round(3 / DET_N, 4)          # 0.02


def candidate_slots() -> dict:
    slots: dict[str, dict] = {}
    if CAND_DIR.exists():
        for path in sorted(CAND_DIR.glob("candidate_*.json")):
            rec = json.loads(path.read_text())
            if rec.get("smoke"):
                continue                  # fence: smoke never enters a budget
            card = rec["scorecard"]
            fr = card.get("false_refusal_on_answerable")
            halluc = card.get("hallucination_on_absent")
            slot = {
                "source": path.name,
                "false_refusal_on_answerable": fr,
                "hallucination_on_absent": halluc,
                "position_tail_pass": card.get("position_tail"),
                "hard_negatives_pass": card.get("hard_negatives"),
                "answer_stage_floor": (
                    round(3 * DET_BOUND + fr, 4) if fr is not None else None),
                "formula": "eviction + retrieval + grader (3 x 0.02) + "
                           "candidate false-refusal",
            }
            slots[rec["model"]] = slot
    if not slots:
        slots["<candidate model tag>"] = {
            "status": "PENDING — run scripts/evaluate_reader_candidate.py on an "
                      "attested stack; this slot fills itself on rerun",
            "answer_stage_floor": None,
            "formula": "eviction + retrieval + grader (3 x 0.02) + "
                       "candidate false-refusal",
        }
    return slots


def main() -> None:
    budget = {
        "version": 2,
        "convention": "zero-failure rates enter as rule-of-three 95% upper "
                      "bounds; smoke-stamped runs never enter; the budget is "
                      "PER SERVING STACK (attestation fingerprint binds it)",
        "stages": {
            "eviction_accounting": {
                "measured_error_rate": 0.0, "n": DET_N,
                "budget_rate_95": DET_BOUND,
                "source": "tests/corpora corpus_e 150/150 vs real store"},
            "retrieval_deterministic": {
                "measured_error_rate": 0.0, "n": DET_N,
                "budget_rate_95": DET_BOUND,
                "source": "tests/corpora corpus_r 150/150 deterministic "
                          "(needles/order/composition/ghost) + dense needles "
                          "10/10 (sprint-1)"},
            "grader_deterministic": {
                "measured_error_rate": 0.0, "n": DET_N,
                "budget_rate_95": DET_BOUND,
                "source": "tests/corpora verdict deterministic tiers 150/150"},
            "judge_llm": {
                "measured_error_rate": None,
                "note": "NOT MEASURED — awaiting the author's blind κ labels on "
                        "compression_pairs.yaml; never estimated"},
            "reader_per_candidate": candidate_slots(),
        },
        "minimum_attributable_policy_delta": {
            "survival_stage": DET_BOUND,
            "retrieval_stage": round(2 * DET_BOUND, 4),
            "answer_stage": "per candidate — see reader_per_candidate slots "
                            "(3 x 0.02 + that candidate's false-refusal)",
            "note": "deterministic floors shrank 3x vs v1 (0.061→0.02 per "
                    "stage) from the corpus growth alone; the reader term is "
                    "now the only large unknown and is candidate-specific",
        },
        "v1_reference": "report/instrument_error_budget.json (7B, sprint-1: "
                        "reader false-refusal 4.8% real / 11.1% corpus-overall)",
    }
    OUT.write_text(json.dumps(budget, indent=1))
    print(json.dumps(budget["minimum_attributable_policy_delta"], indent=1))
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
