#!/usr/bin/env python3
"""
Offline reader re-grade — the paraphrase + judge tiers, run over the persisted
``reader_answer`` + ``ref_answer`` columns so ANY past run is re-gradable without
re-running. The deterministic headline (normalized EM + token-F1) already lives
inline in results.csv; this adds the two refinement tiers, in priority order.

TIER 2 — embedding re-grade (deterministic, the high-value half).
  For answers the headline marked WRONG (qa_correct=False, not abstained),
  cosine(answer, ref) under the project embedder. Counts how many would be
  RECOVERED at each threshold τ — the recovered-false-negative count that lets
  you state "EM/F1 is a conservative lower bound, under-counting by R(τ)."
  Reported as a CURVE over τ (0.70→0.95), NOT a single point, because τ is a
  false-positive risk: a loose τ credits "a lung doctor" for "cardiologist". The
  per-band sample pairs are the calibration view — eyeball them to find where
  recovery stops being correct and pick the headline τ. Same discipline as the
  redundancy thresholds: report the curve, justify the cutoff.

TIER 3 — sampled judge agreement (validation only, defensive, build last/small).
  A pinned judge (model + exact prompt + temperature 0) on a FROZEN sample,
  reporting only its AGREEMENT with the deterministic+embedding grade. Never
  folded into the headline — the least-reproducible artifact, one row. Requires
  an LLM endpoint; enable with --judge.

Aliases stay deferred (the workload carries a single ref_answer, no accepted-
answer sets yet; the embedding tier covers most of what aliases would).

    # high-value tier (needs an embedding endpoint):
    python scripts/grade_answers.py --results artifacts/benchmark/combined_results.csv
    # + defensive judge on a frozen 100-row sample (needs a chat endpoint):
    python scripts/grade_answers.py --results ... --judge --judge-sample 100
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import numpy as np

TAUS = (0.70, 0.75, 0.80, 0.85, 0.90, 0.95)


def _truthy(v) -> bool:
    return str(v).strip().lower() == "true"


def _answered_wrong(row: dict) -> bool:
    """Deterministic headline marked it wrong AND the reader actually answered
    (so there's text to re-grade) — the false-negative candidates."""
    return (not _truthy(row.get("abstained", ""))
            and not _truthy(row.get("qa_correct", ""))
            and bool((row.get("reader_answer") or "").strip())
            and bool((row.get("ref_answer") or "").strip()))


def _norm_rows(mat: np.ndarray) -> np.ndarray:
    n = np.linalg.norm(mat, axis=1, keepdims=True)
    n[n == 0] = 1.0
    return mat / n


def recovered_fn_curve(rows: list[dict], embedder, *, taus=TAUS,
                       examples_per_band: int = 4) -> dict:
    """Recovered-false-negative CURVE over τ for the answered-wrong rows.

    Embeds answers and refs in two batched calls, computes per-row cosine, then
    for each τ reports how many cross it and a sample of the pairs NEWLY recovered
    in the band [τ, next τ) — the borderline pairs you inspect to calibrate."""
    wrong = [r for r in rows if _answered_wrong(r)]
    n_wrong = len(wrong)
    if n_wrong == 0:
        return {"n_answered_wrong": 0, "curve": [], "note": "no answered-wrong rows to recover"}

    av = _norm_rows(np.asarray(embedder.embed([r["reader_answer"] for r in wrong]), dtype=np.float32))
    rv = _norm_rows(np.asarray(embedder.embed([r["ref_answer"] for r in wrong]), dtype=np.float32))
    sims = (av * rv).sum(axis=1)   # per-row cosine(answer, its ref)

    order = list(np.argsort(-sims))
    curve = []
    taus_sorted = sorted(taus)
    for i, tau in enumerate(taus_sorted):
        nxt = taus_sorted[i + 1] if i + 1 < len(taus_sorted) else 1.0001
        recovered = int(np.sum(sims >= tau))
        band_idx = [j for j in order if tau <= sims[j] < nxt][:examples_per_band]
        curve.append({
            "tau": tau,
            "recovered": recovered,
            "recovered_frac_of_answered_wrong": recovered / n_wrong,
            "newly_recovered_in_band": [
                {"sim": round(float(sims[j]), 3),
                 "answer": wrong[j]["reader_answer"][:90],
                 "ref": wrong[j]["ref_answer"][:90]}
                for j in band_idx
            ],
        })
    return {"n_answered_wrong": n_wrong, "curve": curve}


# ── Tier 3: pinned judge agreement (defensive, validation only) ────

_JUDGE_PROMPT = (
    "You are grading a question-answering system. Given the QUESTION, the GOLD "
    "answer, and the SYSTEM answer, reply with exactly one word: CORRECT if the "
    "system answer conveys the same factual answer as the gold (paraphrase is "
    "fine), or WRONG otherwise.\n\n"
    "QUESTION: {q}\nGOLD: {gold}\nSYSTEM: {ans}\n\nVerdict:"
)


def judge_agreement(rows: list[dict], llm, *, deterministic_or_embed_correct,
                    sample_size: int, seed: int = 0, model_name: str = "?") -> dict:
    """Agreement of a PINNED judge with our reproducible grade on a FROZEN sample.

    Reports only an agreement rate — never folded into accuracy. ``deterministic_
    or_embed_correct`` maps a row -> our bool verdict (EM/F1 OR embedding≥τ)."""
    gradeable = [r for r in rows if not _truthy(r.get("abstained", ""))
                 and (r.get("reader_answer") or "").strip()]
    if not gradeable:
        return {"n_sample": 0, "agreement": float("nan"), "model": model_name}
    rng = np.random.default_rng(seed)
    idx = rng.choice(len(gradeable), size=min(sample_size, len(gradeable)), replace=False)
    sample = [gradeable[i] for i in sorted(idx)]   # sorted => frozen given seed

    agree = 0
    for r in sample:
        prompt = _JUDGE_PROMPT.format(q=r.get("question_text", ""),
                                      gold=r.get("ref_answer", ""), ans=r.get("reader_answer", ""))
        verdict = llm.generate(prompt, max_tokens=4, temperature=0.0).strip().upper()
        judge_correct = verdict.startswith("CORRECT")
        if judge_correct == deterministic_or_embed_correct(r):
            agree += 1
    return {"n_sample": len(sample), "agreement": agree / len(sample),
            "model": model_name, "temperature": 0.0, "seed": seed,
            "prompt_sha": _short_sha(_JUDGE_PROMPT)}


def _short_sha(s: str) -> str:
    import hashlib
    return hashlib.sha256(s.encode()).hexdigest()[:12]


# ── CLI ────────────────────────────────────────────────────────────


def _load_rows(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", type=Path, required=True,
                    help="combined_results.csv (or a single results.csv) with reader_answer/ref_answer.")
    ap.add_argument("--tau", type=float, default=0.85,
                    help="Headline threshold for the recovered-FN statement (calibrate from the curve).")
    ap.add_argument("--judge", action="store_true",
                    help="Also run the pinned judge-agreement tier (needs a chat endpoint).")
    ap.add_argument("--judge-sample", type=int, default=100)
    ap.add_argument("--out", type=Path, default=Path("artifacts/reader_regrade.json"))
    args = ap.parse_args()

    rows = _load_rows(args.results)
    rows = [r for r in rows if (r.get("reader") or "") not in ("", "none")]
    if not rows:
        print("no reader rows in this CSV (was it a --no-reader run?)")
        return 1

    from forgetit.embeddings import get_embedder
    embedder = get_embedder()
    report = recovered_fn_curve(rows, embedder)

    print(f"answered-wrong (false-negative candidates): {report['n_answered_wrong']}")
    print(f"{'tau':>5} {'recovered':>10} {'frac_of_wrong':>14}   newly-recovered-in-band (calibration)")
    for c in report["curve"]:
        ex = "; ".join(f"[{e['sim']}] {e['answer']!r}≈{e['ref']!r}" for e in c["newly_recovered_in_band"][:2])
        print(f"{c['tau']:5.2f} {c['recovered']:10d} {c['recovered_frac_of_answered_wrong']:14.3f}   {ex[:90]}")
    rec_at = next((c["recovered"] for c in report["curve"] if abs(c["tau"] - args.tau) < 1e-9), None)
    if rec_at is not None:
        print(f"\nHEADLINE: at τ={args.tau}, EM under-counts by {rec_at} recovered paraphrase(s) "
              f"=> reported EM/F1 is a conservative lower bound.")
    report["headline_tau"] = args.tau

    if args.judge:
        from forgetit.llm import get_llm
        from forgetit.settings import LLM_MODEL
        from forgetit.benchmark.grading import embedding_similarity
        wrong_sim = {}  # row id -> recovered@tau (cache not needed; recompute small)
        def our_verdict(r):
            if _truthy(r.get("qa_correct", "")):
                return True
            if not (r.get("reader_answer") or "").strip():
                return False
            return embedding_similarity(r["reader_answer"], r.get("ref_answer", ""), embedder) >= args.tau
        report["judge"] = judge_agreement(rows, get_llm(),
                                          deterministic_or_embed_correct=our_verdict,
                                          sample_size=args.judge_sample, model_name=LLM_MODEL)
        j = report["judge"]
        print(f"\nJUDGE (defensive, not folded in): agreement={j['agreement']:.3f} on n={j['n_sample']} "
              f"[model={j['model']}, temp=0, prompt_sha={j.get('prompt_sha')}]")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2))
    print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
