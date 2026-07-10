"""S5 — reader-swap candidate evaluation: PREPARED HARNESS (author runs it).

One command per (model, endpoint). Everything from env vars:

    READER_MODEL   candidate tag (the model under evaluation)
    LLM_BASE_URL / LLM_API_KEY   the serving endpoint (OpenAI-compatible)

PREREQUISITE (refused otherwise): a FRESH attestation of THIS exact stack —
run `python scripts/attest_environment.py` first; the scorecard checks that
`report2/attestation/latest.json` has ok=true, a fingerprint matching the
live env, and age < 24 h. The error budget is per-stack; un-attested numbers
don't count.

What it runs (via the existing instruments, as subprocesses):
  1. the FULL corpus_a_reader (115 fixtures: 100 sprint-1 classes incl. the
     30 position triplets + 15 related_but_insufficient hard negatives) at the
     D4-adopted reader configuration;
  2. the probe bench (17 BEAM queries × 4 context conditions, 68 calls).

Scorecard vs the acceptance bar (VERIFICATION.md §7 + sprint-2 brief):
  pass_rate ≥ 0.95 · position_tail ≥ 0.9 · false-refusal ≤ 5 % ·
  hallucination-on-absent = 0 · hard negatives materially above the 7B
  baseline (> baseline + 0.10; pass --baseline <7B scorecard.json>) ·
  contradictions improved (> baseline) · list completeness green (100 %) ·
  latency/call reported. ADOPTION REQUIRES AUTHOR COUNTERSIGN regardless of
  a green scorecard.

Smoke (plumbing validation, ≤50 LLM calls, output stamped smoke=true and
excluded from all tables/budgets):  --smoke  → 2 fixtures per corpus class +
one probe condition.

Usage:
    python scripts/attest_environment.py
    python scripts/evaluate_reader_candidate.py [--smoke] [--baseline report2/candidate_runs/<7b>.json]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))

from attest_environment import reader_model, stack_fingerprint

OUT_DIR = REPO / "report2" / "candidate_runs"
ATTEST = REPO / "report2" / "attestation" / "latest.json"
MAX_ATTEST_AGE_S = 24 * 3600
D4_FLAGS = ["--prompt-version", "v2", "--abstention-match", "exact",
            "--clean-mode", "full"]
ABSENT_CLASSES = {"absent_abstain", "related_but_insufficient_abstain"}
BAR = {"pass_rate": 0.95, "position_tail": 0.90, "false_refusal": 0.05,
       "hallucination_on_absent": 0.0, "materially_above": 0.10}


def require_attestation(model: str) -> dict:
    if not ATTEST.exists():
        sys.exit("REFUSED: no attestation found — run scripts/attest_environment.py "
                 "on this stack first (the error budget is per-stack).")
    rec = json.loads(ATTEST.read_text())
    problems = []
    if not rec.get("ok"):
        problems.append("attestation record is not ok")
    if rec.get("fingerprint") != stack_fingerprint(model):
        problems.append(f"attestation fingerprint {rec.get('fingerprint')} does not "
                        f"match the live env ({stack_fingerprint(model)}) — "
                        "endpoint or model changed since attestation")
    age = time.time() - float(rec.get("attested_epoch", 0))
    if age > MAX_ATTEST_AGE_S:
        problems.append(f"attestation is {age/3600:.1f}h old (> 24h)")
    if problems:
        sys.exit("REFUSED: " + "; ".join(problems)
                 + " — re-run scripts/attest_environment.py")
    return rec


def _sanitize(tag: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", tag)


def smoke_fixture_ids() -> list[str]:
    from run_corpus_a import FIXTURE_FILES
    by_class: dict[str, list[str]] = defaultdict(list)
    for path in FIXTURE_FILES:
        if path.exists():
            for fx in yaml.safe_load(path.read_text()):
                by_class[fx["class"]].append(fx["id"])
    return [fid for ids in by_class.values() for fid in ids[:2]]


def run_corpus(model: str, label: str, smoke: bool) -> dict:
    cmd = [sys.executable, str(REPO / "scripts" / "run_corpus_a.py"),
           "--label", label, "--model", model, *D4_FLAGS]
    if smoke:
        cmd += ["--only", ",".join(smoke_fixture_ids())]
    subprocess.run(cmd, check=True, cwd=REPO)
    return json.loads((REPO / "report" / "corpus_a_runs" / f"{label}.json").read_text())


def run_probe(model: str, label: str, smoke: bool) -> dict:
    cmd = [sys.executable, str(REPO / "scripts" / "reader_probe.py"),
           "--label", label, "--model", model, *D4_FLAGS,
           "--max-tokens-list", "384"]
    if smoke:
        cmd += ["--conditions", "rtok1500"]
    subprocess.run(cmd, check=True, cwd=REPO)
    return json.loads((REPO / "report" / "probe_runs" / f"{label}.summary.json").read_text())


def scorecard(corpus: dict, probe: dict, baseline: dict | None,
              smoke: bool) -> dict:
    rows = corpus["rows"]
    per_class: dict[str, list] = defaultdict(list)
    for r in rows:
        per_class[r["class"]].append(r)
    class_rates = {c: round(sum(x["pass"] for x in xs) / len(xs), 4)
                   for c, xs in sorted(per_class.items())}
    answerable = [r for r in rows if r["class"] not in ABSENT_CLASSES]
    false_refusal = (sum(r["abstained"] for r in answerable) / len(answerable)
                     if answerable else None)
    halluc_absent = 1.0 - class_rates.get("absent_abstain", 1.0)

    card = {
        "pass_rate": corpus.get("pass_rate"),
        "per_class": class_rates,
        "position_tail": class_rates.get("position_tail"),
        "false_refusal_on_answerable": round(false_refusal, 4)
        if false_refusal is not None else None,
        "hallucination_on_absent": round(halluc_absent, 4),
        "hard_negatives": class_rates.get("related_but_insufficient_abstain"),
        "contradictions": class_rates.get("conflicting_evidence_flag"),
        "list_completeness": class_rates.get("enumeration_full_list"),
        "latency_s_per_call": round(corpus["elapsed_s"] / max(corpus["n"], 1), 3),
        "probe": {name: {k: m[k] for k in
                         ("idk", "f1_cleaned", "f1_raw", "false_refusal",
                          "answered_absent", "sent_boundary_prose") if k in m}
                  for name, m in probe["conditions"].items()},
    }

    gates = {
        "pass_rate>=0.95": (card["pass_rate"] or 0) >= BAR["pass_rate"],
        "position_tail>=0.9": (card["position_tail"] or 0) >= BAR["position_tail"],
        "false_refusal<=0.05": (card["false_refusal_on_answerable"] or 1) <= BAR["false_refusal"],
        "hallucination_on_absent==0": card["hallucination_on_absent"] == 0.0,
        "list_completeness==1.0": card["list_completeness"] == 1.0,
    }
    if baseline is not None:
        b = baseline["scorecard"]
        gates["hard_negatives_materially_above_baseline"] = (
            (card["hard_negatives"] or 0)
            >= (b.get("hard_negatives") or 0) + BAR["materially_above"])
        gates["contradictions_improved"] = (
            (card["contradictions"] or 0) > (b.get("contradictions") or 0))
    else:
        gates["hard_negatives_materially_above_baseline"] = "PENDING (no --baseline)"
        gates["contradictions_improved"] = "PENDING (no --baseline)"
    card["gates"] = gates
    hard = [v for v in gates.values() if isinstance(v, bool)]
    card["verdict"] = (
        "SMOKE ONLY — plumbing validated, numbers excluded from all tables" if smoke
        else ("BAR MET — adoption still requires author countersign" if all(hard)
              and not any(isinstance(v, str) for v in gates.values())
              else "BAR NOT MET" if not all(hard)
              else "BAR MET ON MEASURED GATES — baseline gates pending"))
    return card


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--smoke", action="store_true",
                    help="≤50-call plumbing pass; output stamped smoke=true")
    ap.add_argument("--baseline", type=Path, default=None,
                    help="a previous scorecard JSON (the 7B baseline) for the "
                         "relative gates")
    args = ap.parse_args()

    model = reader_model()
    attestation = require_attestation(model)
    label = f"candidate_{_sanitize(model)}" + ("_smoke" if args.smoke else "")
    baseline = json.loads(args.baseline.read_text()) if args.baseline else None

    corpus = run_corpus(model, label, args.smoke)
    probe = run_probe(model, label, args.smoke)
    card = scorecard(corpus, probe, baseline, args.smoke)

    out = {"smoke": bool(args.smoke), "model": model,
           "reader_config": "v2/absexact/cleanfull/list384 (D4-adopted)",
           "attestation_fingerprint": attestation["fingerprint"],
           "attested_at": attestation["attested_at"],
           "n_corpus_fixtures": corpus["n"],
           "scorecard": card}
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / f"{label}.json"
    path.write_text(json.dumps(out, indent=1))
    print(json.dumps(card, indent=1))
    print(f"\n{card['verdict']}\nwrote {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
