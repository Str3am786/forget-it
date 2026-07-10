"""D9 — build report/instrument_error_budget.json v1.

Per pipeline stage: the measured instrument error rate (with its source
artifact) and the implied MINIMUM ATTRIBUTABLE POLICY DELTA at that stage —
a stage-attribution claim ("policy A beats B at stage S by Δ") is only
meaningful if Δ exceeds the summed measured instrument error of the stages
beneath it (VERIFICATION.md §7). Where a rate was measured at zero failures,
the rule-of-three 95 % upper bound is the number that enters the budget
(never the flattering point estimate).

Inputs (all produced by this sprint):
  report/corpus_a_runs/full100_v2.json      reader corpus, adopted config
  report/corpus_r_dense.json                dense needles
  tests/corpora (via pytest state)          deterministic rates asserted green
  report/06_counterfactual_runs.json        pin-run reader false-refusal on
                                            real data; purge-run leak rate
Rerun:  python scripts/build_error_budget.py
"""
from __future__ import annotations

import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
REPORT = REPO / "report"


def rule_of_three(n: int) -> float:
    return round(3 / n, 4)


def main() -> None:
    budget: dict = {"version": 1, "date": "2026-07-08",
                    "convention": "zero-failure rates enter as the rule-of-three "
                                  "95% upper bound, never as 0",
                    "stages": {}}

    # ── S3 eviction / S1 byte accounting (deterministic, corpus_e) ──
    budget["stages"]["eviction_accounting"] = {
        "measured_error_rate": 0.0, "n": 49,
        "budget_rate_95": rule_of_three(49),
        "source": "tests/corpora/test_corpus_e.py — 49/49 vs real store "
                  "(+ e005 OPEN, Q1); includes compression accounting",
    }

    # ── S5 retrieval (needles + ghost invariant) ──
    dense = json.loads((REPORT / "corpus_r_dense.json").read_text())
    budget["stages"]["retrieval_needles"] = {
        "measured_error_rate": 0.0, "n": 20 + 19,
        "budget_rate_95": rule_of_three(39),
        "detail": {"bm25_unique_needles": "10/10 rank 1",
                   "dense_paraphrase_needles": f"{dense['passed']}/{dense['n']} (all rank 1)",
                   "graded_order": "10/10", "post_eviction_ghost": "10/10 (0 ghosts)"},
        "source": "tests/corpora/test_corpus_r.py + report/corpus_r_dense.json",
    }

    # ── S8 grader (deterministic tiers) ──
    budget["stages"]["grader_deterministic"] = {
        "measured_error_rate": 0.0, "n": 30,
        "budget_rate_95": rule_of_three(30),
        "source": "tests/corpora/test_verdict_deterministic.py — exact/abstain±/"
                  "hallucination tiers, 30/30",
    }
    budget["stages"]["judge_llm"] = {
        "measured_error_rate": None,
        "note": "NOT MEASURED this sprint: judge-vs-author κ requires the author's "
                "blind labels on compression_pairs.yaml (all author_label null by "
                "design). Judge numbers stay quarantined until κ ≥ 0.85 is shown.",
    }

    # ── S7 reader (corpus + real-data counterfactual) ──
    corpus = json.loads((REPORT / "corpus_a_runs" / "full100_v2.json").read_text())
    rows = corpus["rows"]
    answerable = [r for r in rows if r["class"] != "absent_abstain"]
    fr_corpus = sum(r["abstained"] for r in answerable) / len(answerable)
    pos = {p: [r for r in rows if r["class"] == f"position_{p}"]
           for p in ("head", "mid", "tail")}
    pos_rate = {p: sum(r["pass"] for r in rs) / len(rs) for p, rs in pos.items()}

    reader_stage = {
        "corpus_false_refusal_on_answerable": round(fr_corpus, 4),
        "corpus_pass_rate": corpus["pass_rate"],
        "position_bias": {**{f"{p}_pass": v for p, v in pos_rate.items()},
                          "spread": round(pos_rate["head"] - pos_rate["tail"], 4)},
        "hallucination_on_absent": 0.0,
        "hallucination_note": "absent_abstain 10/10 under adopted config; "
                              "rule-of-three bound 0.30 at n=10",
        "source": "report/corpus_a_runs/full100_v2.json (adopted D4 config, 16k tag)",
    }
    cf_path = REPORT / "06_counterfactual_runs.json"
    if cf_path.exists():
        cf = json.loads(cf_path.read_text())
        reader_stage["real_data_false_refusal_on_visible"] = \
            cf["pin_summary"]["pure_reader_false_refusal_on_visible"]
        reader_stage["real_data_n_visible"] = cf["pin_summary"]["n_visible_rows"]
        budget["stages"]["contamination_floor"] = {
            "purge_leak_rate_strict": cf["purge_summary"]["leak_rate_strict_qa_correct"],
            "purge_leak_rate_soft_f1_0.3": cf["purge_summary"]["leak_rate_soft_f1>=0.3"],
            "purge_abstention_rate": cf["purge_summary"]["abstention_rate"],
            "source": "report/06_counterfactual_runs.json (gold-purged run)",
            "meaning": "any end-to-end correctness below the soft leak rate is "
                       "indistinguishable from parametric knowledge",
        }
    else:
        reader_stage["real_data_false_refusal_on_visible"] = None
        reader_stage["note"] = "D6 pin run pending"
    budget["stages"]["reader"] = reader_stage

    # ── implied minimum attributable policy deltas ──
    # A delta claimed AT a stage must exceed the summed error of that stage and
    # everything beneath it (worst-case, one-sided).
    ev = budget["stages"]["eviction_accounting"]["budget_rate_95"]
    ret = budget["stages"]["retrieval_needles"]["budget_rate_95"]
    grd = budget["stages"]["grader_deterministic"]["budget_rate_95"]
    rdr_fr = reader_stage.get("real_data_false_refusal_on_visible")
    rdr = max(fr_corpus, rdr_fr if rdr_fr is not None else 0.0)
    budget["minimum_attributable_policy_delta"] = {
        "survival_stage (eviction only)": round(ev, 4),
        "retrieval_stage (eviction+retrieval)": round(ev + ret, 4),
        "answer_stage_deterministic (…+reader+grader)": round(ev + ret + rdr + grd, 4),
        "inputs": {"eviction": ev, "retrieval": ret,
                   "reader_false_refusal(max of corpus/real)": round(rdr, 4),
                   "grader_deterministic": grd},
        "caveat": "answer-stage deltas below this are indistinguishable-from-"
                  "instrument and must never be reported as policy effects; the "
                  "reader term dominates — see 05_corpora.md reader-swap trigger",
    }

    out = REPORT / "instrument_error_budget.json"
    out.write_text(json.dumps(budget, indent=1))
    print(json.dumps(budget["minimum_attributable_policy_delta"], indent=1))
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
