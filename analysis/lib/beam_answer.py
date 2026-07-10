"""Answer-survival loader — the COMPRESSION-FAIR survival currency.

Comparing compression's id-survival against pure-eviction policies is apples-to-
oranges: compression can keep the item (id survives) while compressing the ANSWER
STRING out of it. The fair question is whether the gold ANSWER is still recoverable
from the resident text.

Per (config, query) over the LOCALIZABLE gold set (gold whose ORIGINAL inserted
text actually contained the answer — a policy-independent denominator), from
gold_answer_snapshot.jsonl:

  ans_conj — all localizable gold still have their answer in resident text
             (compression-fair; for pure-eviction policies == id_conj)
  id_conj  — all localizable gold still resident (the compression-inflated version)
  ans_frac / id_frac — graded (fraction of localizable gold), for multi-gold
  n_loc    — localizable gold count (0 → query excluded: no localizable answer)

Cached to parquet. Uses the project's own answer_in_text (SQuAD normalization +
digit/date variants) so this matches the runner's Tier-1 answer-survival metric.
"""
from __future__ import annotations

import json
import sys
from functools import lru_cache
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE.parent / "src"))
from forgetit.benchmark.grading import (  # noqa: E402
    normalize_answer, digit_normalize, is_answerable)

from beam_load import BEAM_ROOT, _config_from_path  # noqa: E402

# Memoized normalizers. The 360 BEAM configs share the same 35 conversations, so
# answers and original item texts repeat thousands of times — caching the (slow)
# SQuAD + digit-normalization collapses the per-config cost. Same semantics as
# grading.answer_in_text, just cached.
_norm = lru_cache(maxsize=200_000)(normalize_answer)
_dnorm = lru_cache(maxsize=200_000)(digit_normalize)


def answer_in_text(answer: str, text: str) -> bool:
    na, nt = _norm(answer), _norm(text)
    if na and na in nt:
        return True
    da, dt = _dnorm(answer), _dnorm(text)
    return bool(da) and da in dt

CACHE = HERE / "tables" / "_answer_survival.parquet"


def _query_rows(snap: Path):
    by_q: dict[str, list[dict]] = {}
    with snap.open() as f:
        for line in f:
            line = line.strip()
            if line:
                r = json.loads(line)
                by_q.setdefault(r.get("query_id", ""), []).append(r)
    return by_q


def build(force: bool = False) -> pd.DataFrame:
    if CACHE.exists() and not force:
        return pd.read_parquet(CACHE)

    recs = []
    for snap in sorted(BEAM_ROOT.rglob("gold_answer_snapshot.jsonl")):
        cfg = _config_from_path(snap)
        if cfg is None or cfg["budget_frac"] is None:
            continue
        for qid, golds in _query_rows(snap).items():
            if not is_answerable(qid, golds[0].get("question_type", "")):
                continue
            # localizable gold: answer present in the ORIGINAL inserted text
            loc = [g for g in golds if g.get("original_text")
                   and answer_in_text(g.get("answer", ""), g["original_text"])]
            n_loc = len(loc)
            if n_loc == 0:
                continue
            ans_ok = [(not g.get("was_evicted", False)) and
                      answer_in_text(g.get("answer", ""), g.get("resident_text", "")) for g in loc]
            id_ok = [not g.get("was_evicted", False) for g in loc]
            recs.append({**cfg, "example_id": qid.split("::")[0], "query_id": qid,
                         "question_type": golds[0].get("question_type", ""),
                         "n_loc": n_loc,
                         "ans_conj": int(all(ans_ok)), "id_conj": int(all(id_ok)),
                         "ans_frac": sum(ans_ok) / n_loc, "id_frac": sum(id_ok) / n_loc})
    df = pd.DataFrame(recs)
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(CACHE, index=False)
    return df


if __name__ == "__main__":
    df = build(force=True)
    print("answer-survival rows:", len(df),
          "| localizable queries/config (turn_pair):",
          df[(df.itemiser == "beam_turn_pair")].query_id.nunique())
    piv = (df[(df.itemiser == "beam_turn_pair") & (df.retriever == "bm25")]
           .groupby(["policy", "budget_frac"])["ans_conj"].mean().unstack().round(3))
    print("\nANSWER-survival (conjunctive, localizable) turn_pair/bm25:")
    print(piv.to_string())
    piv2 = (df[(df.itemiser == "beam_turn_pair") & (df.retriever == "bm25")]
            .groupby(["policy", "budget_frac"])["id_conj"].mean().unstack().round(3))
    print("\nID-survival on the SAME localizable denominator (compare compression row):")
    print(piv2.to_string())
