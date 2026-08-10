#!/usr/bin/env python3
"""
judge_compression_corpus.py — score the compression corpus, then hand you the
rows worth labelling by hand.

THREE QUESTIONS PER ROW, and the first one is not decoration:

  answer_in_original    Can the reference answer be derived from O_g?
  answer_in_compressed  Can it be derived from R_g?          ← the metric
  unsupported_claim     Does R_g assert anything O_g does not?

Preservation is REPORTED CONDITIONAL ON THE FIRST. Most LoCoMo gold items do
not contain their answer: the date lives in the session timestamp, or the
answer collects facts from several sessions. Scoring R_g without that filter
measures LoCoMo's question design and reports it as compression damage. That
mistake has already been made once in this project with a containment metric;
this is where it gets prevented.

The third question is a control, not a headline. Hand inspection of the dev
sample found no confabulation, so a non-zero rate here is a finding and a zero
is the expected result — either way it is measured rather than assumed.

BLINDING. The judge never sees which arm produced the text, which budget it ran
at, or what the achieved ratio was. `compression` and `memorybank` differ in
prompt AND selector on the grid, so the corpus is the only place their prompts
can be compared with everything else held out; leaking the arm into the judge
would throw that away.

CROSS-CHECK. Where O_g states the answer verbatim (grading.answer_in_text, the
`original_extractive` column), question 2 is decidable without a model. The
judge is scored against that deterministic verdict on those rows, which is a
free calibration point before any human labelling.

REPRODUCIBILITY. The prompt lives in a file, its sha and the model tag are
recorded on every row, temperature is 0, and results append to a JSONL as they
arrive so an interrupted run resumes instead of re-spending calls.

Usage:
    python scripts/judge_compression_corpus.py corpora/compression/corpus.csv
    python scripts/judge_compression_corpus.py corpora/compression/corpus.csv --split dev
    python scripts/judge_compression_corpus.py ... --annotate 50
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from forgetit.benchmark.grading import answer_in_text  # noqa: E402

DEFAULT_PROMPT = """\
You are grading whether a summary preserves the information needed to answer a \
question. Judge only what the texts say. Do not use outside knowledge.

QUESTION:
{question}

REFERENCE ANSWER:
{answer}

TEXT A (the original):
{original}

TEXT B (a summary of TEXT A):
{compressed}

Answer three questions with true or false.

1. answer_in_original: Reading TEXT A alone, could someone give the reference \
answer? True only if TEXT A states or directly implies it. If the reference \
answer depends on a date, a fact, or a detail that TEXT A never mentions, \
answer false.

2. answer_in_compressed: Reading TEXT B alone, could someone give the reference \
answer? Apply the same standard.

3. unsupported_claim: Does TEXT B state anything that TEXT A does not support? \
Rewording and omission are fine; only assertions absent from TEXT A count.

Reply with JSON only, no other text:
{{"answer_in_original": true, "answer_in_compressed": false, \
"unsupported_claim": false, "note": "<12 words max>"}}
"""

_JSON_RE = re.compile(r"\{.*\}", re.DOTALL)


def load_prompt(path: Path) -> tuple[str, str]:
    """Read the prompt from disk, creating the default on first run.

    On disk so it is version controlled and diffable: a prompt that lives in
    source is a prompt nobody reviews, and its wording is the instrument.
    """
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(DEFAULT_PROMPT, encoding="utf-8")
        print(f"wrote default judge prompt to {path}")
    text = path.read_text(encoding="utf-8")
    return text, hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


def parse_verdict(raw: str) -> dict | None:
    m = _JSON_RE.search(raw or "")
    if not m:
        return None
    try:
        d = json.loads(m.group(0))
    except json.JSONDecodeError:
        return None
    out = {}
    for k in ("answer_in_original", "answer_in_compressed", "unsupported_claim"):
        v = d.get(k)
        if isinstance(v, str):
            v = v.strip().lower() in ("true", "yes", "1")
        if not isinstance(v, bool):
            return None
        out[k] = v
    out["note"] = str(d.get("note", ""))[:120]
    return out


def judge_rows(rows, llm, template, prompt_sha, out_jsonl: Path, max_tokens: int):
    done = {}
    if out_jsonl.exists():
        for line in out_jsonl.open():
            r = json.loads(line)
            done[r["corpus_id"]] = r
        print(f"resuming: {len(done)} rows already judged")

    with out_jsonl.open("a", encoding="utf-8") as fh:
        for i, row in enumerate(rows, 1):
            if row["corpus_id"] in done:
                continue
            prompt = template.format(
                question=row.get("question_text") or row["query_id"],
                answer=row["reference_answer"],
                original=row["original_text"],
                compressed=row["compressed_text"],
            )
            verdict, raw = None, ""
            for attempt in range(2):        # one retry: small models drift on format
                try:
                    raw = llm.generate(prompt, max_tokens=max_tokens, temperature=0.0)
                except Exception as e:
                    raw = f"__error__ {type(e).__name__}: {e}"
                    break
                verdict = parse_verdict(raw)
                if verdict:
                    break
            rec = {
                "corpus_id": row["corpus_id"],
                "judge_model": getattr(llm, "model", "?"),
                "judge_prompt_sha": prompt_sha,
                "parsed": bool(verdict),
                "raw": "" if verdict else raw[:400],
                **(verdict or {}),
            }
            fh.write(json.dumps(rec) + "\n")
            fh.flush()                       # a killed run keeps what it paid for
            done[row["corpus_id"]] = rec
            if i % 10 == 0:
                print(f"  judged {i}/{len(rows)}")
    return done


def report(rows, verdicts):
    ok = [r for r in rows if verdicts.get(r["corpus_id"], {}).get("parsed")]
    bad = len(rows) - len(ok)
    print(f"\n{len(ok)}/{len(rows)} judged cleanly" + (f", {bad} unparsed" if bad else ""))
    if not ok:
        return

    derivable = [r for r in ok if verdicts[r["corpus_id"]]["answer_in_original"]]
    print(f"\nanswer derivable from the ORIGINAL: {len(derivable)}/{len(ok)} "
          f"({len(derivable)/len(ok):.1%})")
    print("  The rest are questions this gold item cannot answer at all — "
          "out of scope\n  for preservation, and nothing to do with the compressor.")

    if derivable:
        kept = sum(verdicts[r["corpus_id"]]["answer_in_compressed"] for r in derivable)
        print(f"\nPRESERVATION (conditional): {kept}/{len(derivable)} "
              f"({kept/len(derivable):.1%})\n")
        by = Counter()
        tot = Counter()
        for r in derivable:
            key = (r["itemiser"], r["policy"], r["ratio_band"])
            tot[key] += 1
            by[key] += verdicts[r["corpus_id"]]["answer_in_compressed"]
        print(f"  {'itemiser':<12} {'policy':<14} {'band':<8} {'preserved':>10} {'n':>5}")
        for key in sorted(tot, key=str):
            print(f"  {key[0]:<12} {key[1]:<14} {key[2]:<8} "
                  f"{by[key]/tot[key]:>10.1%} {tot[key]:>5}")

    unsup = sum(verdicts[r["corpus_id"]]["unsupported_claim"] for r in ok)
    print(f"\nunsupported claims: {unsup}/{len(ok)} ({unsup/len(ok):.1%}) "
          f"— expected near zero; a control, not a headline")

    # Free calibration: where O_g contains the answer verbatim, question 2 is
    # decidable without a model.
    ext = [r for r in ok if r["original_extractive"] == "True"]
    if ext:
        agree = sum(
            verdicts[r["corpus_id"]]["answer_in_compressed"]
            == bool(answer_in_text(r["reference_answer"], r["compressed_text"]))
            for r in ext)
        print(f"\njudge vs deterministic containment on the {len(ext)} extractive "
              f"rows: {agree}/{len(ext)} agree ({agree/len(ext):.1%})")
        print("  Disagreement is not automatically judge error — containment "
              "misses paraphrase —\n  but a low number means the judge needs "
              "looking at before its numbers are used.")


def write_annotation_sheet(rows, verdicts, path: Path, n: int, seed: int):
    """Stratify on the judge's verdicts, then STRIP them.

    Labelling 50 rows at random spends most of the effort where everything
    already agrees. The rows that discriminate are the preservation failures,
    so those are oversampled — but the sheet carries no verdict columns, and
    the order is shuffled, so the labelling itself stays blind.
    """
    rng = random.Random(seed)
    buckets = {"failure": [], "kept": [], "not_derivable": [], "unsupported": []}
    for r in rows:
        v = verdicts.get(r["corpus_id"])
        if not v or not v.get("parsed"):
            continue
        if v["unsupported_claim"]:
            buckets["unsupported"].append(r)
        elif not v["answer_in_original"]:
            buckets["not_derivable"].append(r)
        elif v["answer_in_compressed"]:
            buckets["kept"].append(r)
        else:
            buckets["failure"].append(r)

    quota = {"failure": int(n * 0.40), "kept": int(n * 0.30),
             "not_derivable": int(n * 0.20), "unsupported": int(n * 0.10)}
    picked = []
    for name, group in buckets.items():
        rng.shuffle(group)
        picked.extend(group[: quota[name]])
    pool = [r for r in rows if r not in picked]
    rng.shuffle(pool)
    picked.extend(pool[: max(0, n - len(picked))])
    rng.shuffle(picked)

    cols = ["corpus_id", "question", "reference_answer", "original_text",
            "compressed_text", "answer_in_original", "answer_in_compressed",
            "unsupported_claim", "notes"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in picked:
            w.writerow({"corpus_id": r["corpus_id"],
                        "question": r.get("question_text") or r["query_id"],
                        "reference_answer": r["reference_answer"],
                        "original_text": r["original_text"],
                        "compressed_text": r["compressed_text"],
                        "answer_in_original": "", "answer_in_compressed": "",
                        "unsupported_claim": "", "notes": ""})
    print(f"\nannotation sheet: {len(picked)} rows -> {path}")
    print("  Fill the three blank columns with true/false. Do NOT open the "
          "judge output first —\n  the agreement number is only worth "
          "anything if your labels were made blind.")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("corpus", type=Path)
    ap.add_argument("--split", choices=["dev", "test", "all"], default="dev")
    ap.add_argument("--prompt", type=Path,
                    default=Path("prompts/compression_judge_v1.txt"))
    ap.add_argument("--out", type=Path, default=None,
                    help="judged JSONL (default: beside the corpus)")
    ap.add_argument("--annotate", type=int, default=50,
                    help="rows for the human sheet; 0 to skip")
    ap.add_argument("--max-tokens", type=int, default=120)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rows = [r for r in csv.DictReader(args.corpus.open(encoding="utf-8"))
            if args.split == "all" or r["split"] == args.split]
    if not rows:
        print(f"no rows in split {args.split!r}")
        return
    print(f"{len(rows)} rows in split {args.split!r}")

    template, prompt_sha = load_prompt(args.prompt)
    out_jsonl = args.out or args.corpus.parent / f"judged_{args.split}.jsonl"

    from forgetit.llm import get_llm
    llm = get_llm()
    print(f"judge model {getattr(llm, 'model', '?')}, prompt {prompt_sha}")

    verdicts = judge_rows(rows, llm, template, prompt_sha, out_jsonl, args.max_tokens)
    report(rows, verdicts)

    if args.annotate:
        write_annotation_sheet(rows, verdicts, args.corpus.parent / "annotate.csv",
                               args.annotate, args.seed)


if __name__ == "__main__":
    main()
