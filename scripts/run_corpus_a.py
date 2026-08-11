"""Run corpus_a_reader fixtures against the real reader (D4/D5).

Executes golden_corpora/corpus_a_reader/reader_fixtures.yaml on a pinned model
tag and reader configuration, grading BEHAVIOR per each fixture's ``expected``
block (must_answer / must_abstain / answer_must_contain_any / _all /
must_not_be_flagged_abstention). Position fixtures (class position_*) build
their evidence deterministically: ``filler_tokens`` of neutral diary filler
(seeded per fixture id; vocabulary disjoint from every needle) split into
~250-token records, with the needle record placed at head / mid / tail of the
evidence order.

``min_token_f1`` is only checkable against an explicit ``reference`` field;
fixtures that specify it without one get a recorded SKIPPED-CHECK note (the
containment checks remain binding).

Usage:
    python scripts/run_corpus_a.py --label a_baseline \
        [--model TAG] [--prompt-version v2] [--abstention-match exact]
        [--clean-mode full] [--token-budget 4000] [--only a001,a002]

Output: report/corpus_a_runs/<label>.json (+ printed per-fixture verdicts).
"""
from __future__ import annotations

import argparse
import json
import random
import time
from pathlib import Path

import yaml

from forgetit.core.schema import MemRecord, Question
from forgetit.llm.openai_llm import OpenAiLLM
from forgetit.llm.readers import PlainTextReader
from forgetit.settings import LLM_API_KEY, LLM_BASE_URL

REPO = Path(__file__).resolve().parents[1]
FIXTURE_FILES = [
    REPO / "golden_corpora" / "corpus_a_reader" / "reader_fixtures.yaml",
    REPO / "golden_corpora" / "corpus_a_reader" / "reader_fixtures_bulk.yaml",
    REPO / "golden_corpora" / "corpus_a_reader" / "reader_fixtures_bulk2.yaml",
]
OUT_DIR = REPO / "report" / "corpus_a_runs"

# Neutral filler vocabulary — no numbers, no vault/code/vendor/date words that
# could collide with any needle or question.
_FILLER_SENTENCES = [
    "Watered the tomato seedlings before breakfast and thinned the basil.",
    "The evening walk looped past the old mill and back along the towpath.",
    "Spent an hour untangling yarn for the striped scarf project.",
    "The sourdough starter looked sluggish so it moved nearer the stove.",
    "Raked the maple leaves into three heaps and composted two of them.",
    "The library copy of the sailing memoir finally came off the hold shelf.",
    "Tried a new stretch routine after the long ride and felt the difference.",
    "The neighbours repainted their fence a soft sage green this weekend.",
    "Roasted squash with rosemary for dinner and saved the seeds for later.",
    "A heron stood in the shallows for ages while the kettle boiled.",
    "Mended the elbow of the grey cardigan with a patch of felted wool.",
    "The choir rehearsal ran late but the descant finally came together.",
]
_CHARS_PER_TOKEN = 4


def _filler_records(fixture_id: str, total_tokens: int, per_record_tokens: int = 250):
    rng = random.Random(f"corpus_a::{fixture_id}")
    records, buf, buf_chars = [], [], 0
    target_chars = total_tokens * _CHARS_PER_TOKEN
    made_chars = 0
    while made_chars < target_chars:
        s = rng.choice(_FILLER_SENTENCES)
        buf.append(s)
        buf_chars += len(s) + 1
        made_chars += len(s) + 1
        if buf_chars >= per_record_tokens * _CHARS_PER_TOKEN:
            records.append(" ".join(buf))
            buf, buf_chars = [], 0
    if buf:
        records.append(" ".join(buf))
    return records


def build_evidence(fx: dict) -> list[MemRecord]:
    inp = fx["input"]
    if "evidence" in inp:
        return [MemRecord(id=f"{fx['id']}::e{i}", text=t, created_at=float(i))
                for i, t in enumerate(inp["evidence"], 1)]
    # position fixture
    filler = _filler_records(fx["id"], int(inp["filler_tokens"]))
    needle = inp["needle"]
    pos = inp["needle_position"]
    idx = {"head": 0, "mid": len(filler) // 2, "tail": len(filler)}[pos]
    texts = filler[:idx] + [needle] + filler[idx:]
    return [MemRecord(id=f"{fx['id']}::e{i}", text=t, created_at=float(i))
            for i, t in enumerate(texts, 1)]


def check_expected(fx: dict, answer: str, abstained: bool) -> tuple[bool, list[str]]:
    exp = fx["expected"]
    failures: list[str] = []
    notes: list[str] = []
    low = answer.lower()
    if exp.get("must_answer") and abstained:
        failures.append("abstained but must_answer")
    if exp.get("must_abstain") and not abstained:
        failures.append(f"answered ({answer[:60]!r}) but must_abstain")
    if exp.get("must_not_be_flagged_abstention") and abstained:
        failures.append("flagged as abstention but must_not_be_flagged_abstention")
    for needle in exp.get("answer_must_contain_any", []) or []:
        if needle.lower() in low:
            break
    else:
        if exp.get("answer_must_contain_any"):
            failures.append(f"none of {exp['answer_must_contain_any']} in answer")
    for needle in exp.get("answer_must_contain_all", []) or []:
        if needle.lower() not in low:
            failures.append(f"missing required {needle!r}")
    if "min_token_f1" in exp and "reference" not in exp:
        notes.append("min_token_f1 specified without a reference — check skipped")
    return (not failures), failures + notes


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--model", default="qwen2.5:7b-instruct-q5km-16k")
    ap.add_argument("--max-tokens", type=int, default=256)
    ap.add_argument("--token-budget", type=int, default=4000,
                    help="evidence token budget (must cover the 3000-token "
                         "position fixtures whole)")
    ap.add_argument("--only", default=None, help="comma list of fixture ids")
    args = ap.parse_args()

    fixtures = []
    for path in FIXTURE_FILES:
        if path.exists():
            fixtures.extend(yaml.safe_load(path.read_text()))
    if args.only:
        want = set(args.only.split(","))
        fixtures = [f for f in fixtures if f["id"] in want]

    llm = OpenAiLLM(base_url=LLM_BASE_URL, api_key=LLM_API_KEY, model=args.model)
    reader = PlainTextReader(llm, token_budget=args.token_budget,
                             max_tokens=args.max_tokens)

    rows, n_pass = [], 0
    t0 = time.time()
    for fx in fixtures:
        records = build_evidence(fx)
        q = Question(id=fx["id"], text=fx["input"]["question"], timestamp=0.0, meta={})
        res = reader.answer(q, records)
        ok, details = check_expected(fx, res.answer, res.abstained)
        n_pass += ok
        rows.append({"id": fx["id"], "class": fx["class"], "pass": ok,
                     "details": details, "answer": res.answer,
                     "abstained": res.abstained,
                     "raw": str(res.meta.get("raw_output", ""))[:400],
                     "n_evidence": len(records)})
        print(f"  {fx['id']} [{fx['class']:<28}] {'PASS' if ok else 'FAIL ' + '; '.join(details)}")

    n = len(rows)
    out = {"label": args.label, "model": args.model,
           "reader": reader.name, "max_tokens": args.max_tokens,
           "n": n, "passed": n_pass,
           "pass_rate": round(n_pass / n, 4) if n else None,
           "rule_of_three_upper_bound": round(3 / n, 4) if n and n_pass == n else None,
           "elapsed_s": round(time.time() - t0, 1),
           "rows": rows}
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / f"{args.label}.json").write_text(json.dumps(out, indent=1))
    print(f"\n{n_pass}/{n} passed → {OUT_DIR}/{args.label}.json")


if __name__ == "__main__":
    main()
