#!/usr/bin/env python3
"""LLM-as-judge for reader answers: a SECOND column beside the deterministic
grade, calibrated against it, never a replacement.

WHERE IT SITS. Measured on the 130-row annotated corpus, the deterministic
tiers perform very unevenly:

    exact       n=12   agreement 1.000
    abstain     n=17   agreement 1.000
    contains    n=16   agreement 0.812   3 false accepts (abstention leakage)
    embedding   n=12   agreement 0.583   5 false accepts (near-numeric misses)
    miss        n=63   agreement 0.698   19 false rejects (paraphrase)

So exact/contains/abstain stay deterministic: they are fast, auditable, and
exact/abstain are the rungs the instrument-validation arguments rest on. Only
`embedding` and `miss` are routed here. The judge REPLACES the embedding tier
(it is the worst rung) and REVIEWS the miss bucket (where recall is lost).

TWO SIDED. It is an arbiter, not a recall booster: it must be able to reject a
row the embedding tier accepted. A judge that can only add correctness would
make the false-accept problem worse.

SCOPE. Rows with no reference answer are UNGRADEABLE, not incorrect. They are
excluded before judging and reported separately (locomo's adversarial stratum,
and beam's instruction_following / preference_following / summarization).

    # calibrate against the human labels
    python scripts/judge_reader_answers.py corpus_full_csv.bak \
        --out report/judge_corpus.csv --calibrate annotate.csv

    # production, over reader sweep rows
    python scripts/judge_reader_answers.py <rows.jsonl> --out report/judged.csv
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import json
import os
import re
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))
from forgetit.llm import get_llm

JUDGE_SHA_LEN = 12
ROUTED_TIERS = {"embedding", "miss"}

# Mirrors RULE.txt, the rule the human labels were made under. The clauses on
# numbers and partial answers target the two observed false-accept modes:
# "15 patterns / 4 optimizations" accepted against "14 / 3", and
# partial_plus_substitution.
PROMPT = """You grade whether a candidate answer correctly answers a question.

QUESTION:
{question}

REFERENCE ANSWER:
{ref}

CANDIDATE ANSWER:
{answer}

Rule: the candidate is CORRECT if and only if a reasonable reader would accept \
it as answering the question, regardless of wording. Paraphrase is fine.
- Numbers, dates, quantities and names must match the reference. A close but \
different number or date is INCORRECT.
- A partial answer to a multi-part question is INCORRECT.
- An abstention ("I don't know", "IDK", "not provided") is CORRECT only if the \
question is genuinely unanswerable. If the reference gives an answer, an \
abstention is INCORRECT.
- Extra unsupported detail around a correct answer does not make it incorrect, \
but confident wrongness does.
- Judge the candidate against the question and reference only.

Reply with exactly two lines and nothing else:
VERDICT: CORRECT
WHY: <at most 12 words>"""

VERDICT_RE = re.compile(r"VERDICT:\s*(CORRECT|INCORRECT)", re.I)
WHY_RE = re.compile(r"WHY:\s*(.+)", re.I)


def prompt_sha(p: str) -> str:
    return hashlib.sha256(p.encode()).hexdigest()[:JUDGE_SHA_LEN]


def row_key(r: dict) -> str:
    """Stable key for resume. Uses the corpus key when present."""
    if r.get("row_key"):
        return str(r["row_key"])
    blob = "\x00".join(str(r.get(k, "")) for k in
                       ("model", "config", "query_id", "arm"))
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


def load(path: Path) -> pd.DataFrame:
    if path.suffix == ".jsonl":
        rows = [json.loads(l) for l in path.open(encoding="utf-8") if l.strip()]
        df = pd.DataFrame(rows)
        df = df.rename(columns={"answer": "reader_answer"})
    else:
        df = pd.read_csv(path)
    for c in ("question", "ref_answer", "reader_answer", "tier"):
        if c not in df.columns:
            df[c] = ""
    df["tier"] = df.tier.astype(str).str.strip().str.lower()
    df["row_key"] = [row_key(r) for r in df.to_dict("records")]
    return df


def judge_one(llm, r: dict) -> dict:
    p = PROMPT.format(question=str(r.get("question", "") or "")[:2000],
                      ref=str(r.get("ref_answer", "") or "")[:1000],
                      answer=str(r.get("reader_answer", "") or "")[:3000])
    try:
        out = llm.generate(p, max_tokens=64, temperature=0.0)
    except Exception as exc:
        return {"row_key": r["row_key"], "judge_correct": None,
                "judge_why": f"{type(exc).__name__}", "judge_errored": True}
    m = VERDICT_RE.search(out or "")
    w = WHY_RE.search(out or "")
    return {
        "row_key": r["row_key"],
        # An unparseable verdict is NOT a rejection: it is a missing datum.
        "judge_correct": (m.group(1).upper() == "CORRECT") if m else None,
        "judge_why": (w.group(1).strip()[:80] if w else (out or "")[:80]),
        "judge_errored": m is None,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("rows", type=Path)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--calibrate", type=Path, default=None,
                    help="CSV with row_key,human_correct")
    ap.add_argument("--tiers", nargs="*", default=sorted(ROUTED_TIERS))
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    df = load(args.rows)
    print(f"{len(df):,} rows loaded from {args.rows.name}")
    print("\ntier distribution as loaded:")
    print(df.tier.value_counts(dropna=False).to_string())

    # ── scope: no reference means ungradeable, not incorrect ───────────
    no_ref = df.ref_answer.isna() | (df.ref_answer.astype(str).str.strip() == "")
    if no_ref.any():
        print(f"\n{int(no_ref.sum())} row(s) have NO reference answer -> UNGRADEABLE")
        print(df[no_ref].groupby(["dataset", "question_type"]).size().to_string()
              if {"dataset", "question_type"}.issubset(df.columns) else "")
        print("  Excluded from judging. A verdict against a missing reference is\n"
              "  a false accept by construction, whichever way it falls.")
    work = df[~no_ref & df.tier.isin(args.tiers)].copy()
    if args.limit:
        work = work.head(args.limit)
    print(f"\nrouted to the judge: {len(work):,} rows, tiers={args.tiers}")
    print(f"passed through deterministic: "
          f"{int((~no_ref).sum()) - len(work):,} rows")
    if not len(work):
        print("\nSTOP - nothing routed. The tier values above do not intersect "
              f"{args.tiers}.\n  Pass --tiers with the values actually present, or "
              "check the input file.", file=sys.stderr)
        return 2

    # ── resume ─────────────────────────────────────────────────────────
    done: dict[str, dict] = {}
    if args.out.is_file():
        prev = pd.read_csv(args.out)
        done = {r["row_key"]: r for r in prev.to_dict("records")
                if not r.get("judge_errored")}
        print(f"resuming: {len(done):,} already judged")
    todo = [r for r in work.to_dict("records") if r["row_key"] not in done]

    llm = get_llm()
    print(f"judge prompt sha {prompt_sha(PROMPT)}   model {os.environ.get('LLM_MODEL','?')}")
    print(f"judging {len(todo):,} rows with {args.workers} workers ...")

    results = list(done.values())
    if todo:
        with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
            for i, res in enumerate(ex.map(lambda r: judge_one(llm, r), todo), 1):
                results.append(res)
                if i % 200 == 0:
                    print(f"  {i:,}/{len(todo):,}", flush=True)

    jd = pd.DataFrame(results) if results else pd.DataFrame(
        columns=["row_key", "judge_correct", "judge_why", "judge_errored"])
    args.out.parent.mkdir(parents=True, exist_ok=True)
    merged = df.merge(jd, on="row_key", how="left")
    merged.to_csv(args.out, index=False)
    bad = int(jd.judge_errored.sum()) if len(jd) else 0
    print(f"\nwrote {args.out}  ({len(merged):,} rows, {bad} unparseable verdicts)")

    # ── the judged column beside the deterministic one ─────────────────
    if "regraded_correct" in merged.columns:
        j = merged.dropna(subset=["judge_correct"])
        print("\ndeterministic vs judge, on the routed tiers")
        print(pd.crosstab(j.regraded_correct.astype(bool),
                          j.judge_correct.astype(bool),
                          rownames=["deterministic"], colnames=["judge"],
                          margins=True).to_string())

    # ── calibration against the human labels ───────────────────────────
    if args.calibrate:
        hum = pd.read_csv(args.calibrate)[["row_key", "human_correct"]]
        hum = hum.drop_duplicates("row_key")
        c = merged.merge(hum, on="row_key", how="inner").drop_duplicates("row_key")
        c = c.dropna(subset=["human_correct"])
        print(f"\n=== calibration on {len(c)} labelled rows ===")

        for name, col in (("deterministic", "regraded_correct"), ("judge", "judge_correct")):
            if col not in c.columns:
                continue
            s = c.dropna(subset=[col])
            if not len(s):
                continue
            pred = s[col].astype(bool)
            truth = s.human_correct.astype(bool)
            fp = int((pred & ~truth).sum())
            fn = int((~pred & truth).sum())
            print(f"  {name:<14} n={len(s):<4} agreement={(pred==truth).mean():.3f}  "
                  f"FP={fp}  FN={fn}")

        if {"regraded_correct", "judge_correct"}.issubset(c.columns):
            b = c.dropna(subset=["regraded_correct", "judge_correct"])
            det = b.regraded_correct.astype(bool)
            jug = b.judge_correct.astype(bool)
            tru = b.human_correct.astype(bool)
            print(f"\n  on the {len(b)} rows both graded:")
            print(f"    judge fixed a deterministic error : "
                  f"{int(((det != tru) & (jug == tru)).sum())}")
            print(f"    judge broke a deterministic hit   : "
                  f"{int(((det == tru) & (jug != tru)).sum())}")
            print("\n  The second number is the one that decides adoption. A judge\n"
                  "  that fixes ten and breaks eight has bought you noise.")
        print("\n  NOTE: this corpus is drawn at sim_threshold 0.83 +/- 0.06, so it is\n"
              "  enriched at the embedding tier's decision boundary. These rates are\n"
              "  boundary-case rates, not population rates.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
