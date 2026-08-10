#!/usr/bin/env python3
"""
build_grader_corpus.py — the labelling corpus for the answer grader.

WHY. `qa_correct` is the verdict behind every accuracy number in the thesis,
and the instrument that produces it has two documented-but-unquantified failure
classes (firstline cleaning mutilates list answers; loose abstention swallows
hedged answers) and one promise the analysis plan has not kept: the 0.83
paraphrase threshold was chosen a priori and never calibrated. This corpus is
the calibration set. Human labels against grader verdicts give (a) the grader's
error rate, (b) the calibrated threshold with its trade-off curve, (c) the
disagreement rate by question type. It also doubles as the validation set the
compression work deferred.

WHAT A ROW IS. One (question, reference answer, reader answer) triple from an
archived run, with the grader's verdict RECOMPUTED offline via the project's own
grade() — same code path, same normalisation — so the tier and similarity that
the row schema does not carry are recovered exactly rather than approximated.
The probe leak in the archived run is irrelevant here: however retrieval chose
the evidence, the reader produced a real answer and the grader graded it, which
is precisely the population the grader faces after any rerun.

STRATIFICATION — on where the grader is guessing, not uniformly:

  boundary   similarity within ±0.06 of the threshold: the verdict flips with
             the threshold, so these rows ARE the calibration signal.
  contains   accepted by substring: where "mentions the string, asserts the
             wrong thing" and negation false-positives live.
  abstain    flagged as abstention: where the loose matcher's measured
             false-positive class (hedged-but-informative answers) lives.
  miss/other proportional fill across tiers and question types, temporal and
             multi-answer oversampled via the qtype quota.

LABELLING RULE (printed onto the sheet): correct iff a reasonable reader would
accept the answer as answering the question, wording free; an abstention is
correct only if the question is unanswerable; a partial answer to a multi-part
question is incorrect, note it. Label BLIND: the sheet carries no grader columns.

Usage:
    python scripts/build_grader_corpus.py \
        ~/Desktop/results_lumi/benchmark_reader_v2/combined_results.csv --n 120
    # with the paraphrase tier (recommended — the boundary stratum needs it):
    python scripts/build_grader_corpus.py ... --embed
"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from forgetit.benchmark.grading import grade, is_answerable  # noqa: E402

BOUNDARY = 0.06          # half-width of the band around the threshold
RULE = ("LABEL RULE: human_correct=true iff a reasonable reader would accept "
        "the answer as answering the question, regardless of wording. An "
        "abstention ('I don't know' etc.) is true only if the question is "
        "genuinely unanswerable. A partial answer to a multi-part question is "
        "false — note 'partial'. Judge the ANSWER against the QUESTION and "
        "REFERENCE only; do not reward confident wrongness.")


def regrade(df: pd.DataFrame, embedder, sim_threshold: float) -> pd.DataFrame:
    """Recompute the verdict with the project's own grade().

    Two passes: the deterministic tiers for everyone, then the embedder only
    for deterministic misses — the sole rows where similarity can change the
    verdict, and the archived run's own order of evaluation.
    """
    out = []
    for r in df.itertuples(index=False):
        v = grade(str(r.reader_answer or ""), str(r.ref_answer or ""),
                  abstained=bool(r.abstained))
        out.append(v)
    df = df.copy()
    df["tier"] = [v["tier"] for v in out]
    df["similarity"] = [v["similarity"] for v in out]
    df["regraded_correct"] = [v["correct"] for v in out]

    if embedder is not None:
        misses = df.index[df.tier == "miss"]
        print(f"embedding pass over {len(misses)} deterministic misses …")
        for i in misses:
            v = grade(str(df.at[i, "reader_answer"] or ""),
                      str(df.at[i, "ref_answer"] or ""),
                      abstained=False, embedder=embedder,
                      sim_threshold=sim_threshold)
            df.at[i, "tier"] = v["tier"]
            df.at[i, "similarity"] = v["similarity"]
            df.at[i, "regraded_correct"] = v["correct"]
    return df


def stratify(df: pd.DataFrame, n: int, seed: int, sim_threshold: float) -> pd.DataFrame:
    rng = random.Random(seed)
    near = (df.similarity - sim_threshold).abs() <= BOUNDARY

    strata = {
        "boundary": df[near],
        "contains": df[df.tier == "contains"],
        "abstain": df[df.tier.isin(("abstain", "abstain_missed"))],
        "accept_exact": df[df.tier.isin(("exact", "alias"))],
        "miss": df[(df.tier == "miss") & ~near],
    }
    quota = {"boundary": 0.30, "contains": 0.20, "abstain": 0.15,
             "accept_exact": 0.10, "miss": 0.25}

    picked: list[pd.DataFrame] = []
    taken: set = set()
    for name, share in quota.items():
        pool = strata[name]
        pool = pool[~pool.index.isin(taken)]
        want = round(n * share)
        if len(pool) > want:
            # spread across question types inside the stratum
            groups = list(pool.groupby("question_type", dropna=False))
            rng.shuffle(groups)
            rows, per = [], max(1, want // max(1, len(groups)))
            for _, g in groups:
                rows.append(g.sample(min(per, len(g)), random_state=rng.randint(0, 9999)))
            pool = pd.concat(rows).head(want) if rows else pool.head(0)
        picked.append(pool)
        taken.update(pool.index)
        print(f"  {name:<12} wanted {want:>4}  got {len(pool):>4}")

    out = pd.concat(picked).drop_duplicates(subset="row_key")
    if len(out) < n:
        rest = df[~df.index.isin(taken)]
        out = pd.concat([out, rest.sample(min(n - len(out), len(rest)),
                                          random_state=seed)])
    return out.sample(frac=1, random_state=seed).head(n)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results", type=Path, help="combined_results.csv with reader on")
    ap.add_argument("--n", type=int, default=120)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--sim-threshold", type=float, default=0.83)
    ap.add_argument("--embed", action="store_true",
                    help="run the paraphrase tier on deterministic misses "
                         "(needs the project embedder; the boundary stratum "
                         "is empty without it)")
    ap.add_argument("--out", type=Path, default=Path("corpora/grader"))
    args = ap.parse_args()

    df = pd.read_csv(args.results, low_memory=False)
    need = {"reader_answer", "ref_answer", "abstained", "qa_correct",
            "question_type", "query_id", "dataset", "policy"}
    if "question_text" not in df.columns:
        # The sheet asks "does this answer answer the question" — without the
        # question it is unanswerable by the human too. Retrofit via
        # add_question_text.py if labelling an old corpus.
        print("WARNING: no question_text column — the annotation sheet will "
              "lack the question itself.")
        df["question_text"] = ""
    missing = need - set(df.columns)
    if missing:
        print(f"missing columns: {sorted(missing)}")
        return

    df = df[df.reader_answer.notna() | df.abstained.astype(str).str.lower().eq("true")]
    df["abstained"] = df.abstained.astype(str).str.lower().eq("true")
    df["qa_correct"] = df.qa_correct.astype(str).str.lower().eq("true")

    # Unanswerable questions follow a different rule (grade_with_abstention);
    # keep a small, SEPARATE stratum rather than mixing the two rule systems.
    # Empty reference + answerable question = ungradeable by ANY string
    # measure: grade() compares against "", so qa_correct is False by
    # construction however good the answer. BEAM's open-ended abilities
    # (summarization, preference/instruction following) live here. They leave
    # the calibration frame — you cannot label correctness against no
    # reference — and are counted as their own category instead.
    no_ref = df.ref_answer.isna() | (df.ref_answer.astype(str).str.strip() == "")
    answerable = df.apply(lambda r: is_answerable(str(r.query_id),
                                                  str(r.question_type)), axis=1)
    ungradeable = df[no_ref & answerable]
    if len(ungradeable):
        print(f"\nungradeable (empty reference, answerable question): "
              f"{len(ungradeable):,} unique triples — EXCLUDED from labelling:")
        print(ungradeable.groupby(["dataset", "question_type"]).size().to_string())
    unans = df[~answerable].sample(min(10, int((~answerable).sum())),
                                   random_state=args.seed)
    df = df[answerable & ~no_ref]

    # The grader is deterministic given (answer, ref, abstained), so identical
    # triples across policies and budgets are ONE calibration point, not many.
    triple = (df.reader_answer.fillna("") + "\x1f" + df.ref_answer.fillna("")
              + "\x1f" + df.abstained.astype(str))
    # Hash: the key is a join handle between annotate.csv and corpus_full.csv,
    # not a datum. Embedding the raw texts put multi-KB answers with newlines
    # into a key column and made the sheet unopenable.
    df["row_key"] = triple.map(lambda t: hashlib.sha256(t.encode()).hexdigest()[:16])
    before = len(df)
    df["question_text"] = df.groupby("row_key").question_text.transform(
        lambda g: g[g.astype(str).str.len() > 0].iloc[0]
        if (g.astype(str).str.len() > 0).any() else "")
    df = df.drop_duplicates(subset="row_key").reset_index(drop=True)
    print(f"{before:,} reader rows -> {len(df):,} unique (answer, ref, abstained) triples")

    embedder = None
    if args.embed:
        from forgetit.embeddings import get_embedder      # adjust if named differently
        embedder = get_embedder()

    df = regrade(df, embedder, args.sim_threshold)

    # Consistency check: our offline regrade must reproduce the archived
    # verdict wherever the archived run used the same tiers. Disagreement here
    # means the archive was graded with different code and the corpus should
    # calibrate against THAT, not this.
    det = df[df.tier != "embedding"]
    agree = (det.regraded_correct == det.qa_correct).mean()
    print(f"offline regrade agrees with archived qa_correct on "
          f"{agree:.1%} of deterministic-tier rows")
    if agree < 0.97:
        print("  WARNING: the archive was graded by a different code path — "
              "inspect before labelling.")

    print("\ntier distribution over unique triples:")
    print(df.tier.value_counts().to_string())

    sample = stratify(df, args.n, args.seed, args.sim_threshold)
    sample = pd.concat([sample, unans])

    args.out.mkdir(parents=True, exist_ok=True)

    keep = ["row_key", "dataset", "question_type", "query_id", "question_text",
            "ref_answer", "reader_answer", "abstained",
            "tier", "similarity", "regraded_correct", "qa_correct"]
    sample[keep].to_csv(args.out / "corpus_full.csv", index=False)

    sheet = sample[["row_key", "dataset", "question_type", "query_id",
                    "question_text", "ref_answer", "reader_answer"]].copy()
    sheet.insert(0, "corpus_id", [f"g{i:04d}" for i in range(len(sheet))])
    sheet["human_correct"] = ""
    sheet["human_note"] = ""
    sheet.to_csv(args.out / "annotate.csv", index=False)
    (args.out / "RULE.txt").write_text(RULE + "\n")
    (args.out / "manifest.json").write_text(json.dumps({
        "source": str(args.results), "seed": args.seed, "n": len(sample),
        "sim_threshold": args.sim_threshold, "boundary": BOUNDARY,
        "embed": bool(embedder), "unanswerable_rows": len(unans),
        "ungradeable_no_reference": int(len(ungradeable)),
        "ungradeable_by_qtype": {f"{d}/{q}": int(c) for (d, q), c in
                                 ungradeable.groupby(["dataset", "question_type"]).size().items()},
    }, indent=2))

    print(f"\n{len(sample)} rows -> {args.out}/annotate.csv  (+ corpus_full.csv, RULE.txt)")
    print("Label human_correct true/false per the rule; the sheet carries no "
          "grader columns, keep it that way until you are done.")


if __name__ == "__main__":
    main()