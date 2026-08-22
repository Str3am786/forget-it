#!/usr/bin/env python3
"""alive_labels.py — two outputs from one context rebuild.

1. TEST-RETEST: every context <=12,000 chars was judged twice on an identical
   prompt, once under the 12k cap (archived cache) and once under the 100k cap
   (current cache), in two independent serving sessions at temperature 0.
   Joining the two caches through recomputed keys gives exact verdict
   agreement. That is the judge's reliability, measured, no labels needed.

2. LABEL SAMPLE: a blind sheet drawn from the four contain x judge corners,
   per family, for the human calibration. Two files: label_me.csv with NO
   judge verdict in it, and label_key.csv holding the verdicts for the join
   AFTER labelling. Label blind or the labels are worthless.

   Scope: only contexts <= LABEL_MAX_CHARS are sampled, so the labeller reads
   exactly what the judge read, in full. State this as a scope sentence in the
   methods: agreement is reported on the stratum a human can actually read.

Run on a login node, CPU only:
  singularity exec --bind /scratch/$PROJ "$CLIENT_SIF" \
      python3 scripts/alive_labels.py
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import random
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import gold_alive  # noqa: E402  (lazy get_llm, safe to import)

ALIVE = Path("/scratch/project_462001123/artifacts/alive")
SUBSET = Path("/scratch/project_462001123/artifacts/sweep_subset")
OLD_CACHE = ALIVE / "cache_cap12k"
WORKLOADS = ("longmemeval_turn_pair", "beam_beam_turn_pair", "locomo_turn_pair")
LABEL_MAX_CHARS = 6_000
PER_CORNER = 8          # per corner, per family, per workload; ~120 total
SEED = 42


def old_key(q: str, a: str, ctx: str) -> str:
    return hashlib.sha256("\x00".join((q, a, ctx)).encode()).hexdigest()[:24]


def rebuild(workload: str) -> pd.DataFrame:
    """Recompute every judged context exactly as gold_alive built it."""
    args = argparse.Namespace(field_query_id=None, field_question=None,
                              field_answer=None, field_resident=None)
    rows = []
    root = SUBSET / workload
    for cfg in sorted(gold_alive.iter_config_dirs(root)):
        facets = gold_alive.config_facets(cfg, root)
        for r in gold_alive.load_store_contexts(cfg, args):
            if not r["context"].strip():
                continue
            r.update(facets)
            rows.append(r)
    d = pd.DataFrame(rows)
    d["new_key"] = [gold_alive.cache_key(r) for r in d.to_dict("records")]
    d["old_key"] = [old_key(r["question"], r["answer"], r["context"])
                    for r in d.to_dict("records")]
    d["context_chars"] = d.context.str.len()
    a = d.answer.astype(str).str.strip().str.lower()
    hits = pd.Series([x in c.lower() for x, c in zip(a, d.context)],
                     index=d.index)
    d["answer_in_context"] = (a != "") & hits
    return d


def load_cache(path: Path) -> dict[str, bool]:
    if not path.is_file():
        return {}
    c = pd.read_csv(path)
    c = c[~c.judge_errored.fillna(False)].dropna(subset=["answerable"])
    return dict(zip(c.cache_key, c.answerable.astype(bool)))


def main() -> int:
    random.seed(SEED)
    label_rows, key_rows = [], []
    print(f"{'workload':26s} {'n_pairs':>8} {'agree':>8} {'flips':>6}")
    for w in WORKLOADS:
        d = rebuild(w)
        new = load_cache(ALIVE / f"store_{w}.cache.csv")
        old = load_cache(OLD_CACHE / f"store_{w}.cache.csv")
        d["v_new"] = d.new_key.map(new)
        d["v_old"] = d.old_key.map(old)

        # ---- retest: unique contexts, <=12k so both prompts were identical
        u = (d[d.context_chars <= 12_000]
             .drop_duplicates("new_key")
             .dropna(subset=["v_new", "v_old"]))
        n = len(u)
        agree = (u.v_new == u.v_old).mean() if n else float("nan")
        flips = int((u.v_new != u.v_old).sum())
        print(f"{w:26s} {n:8,d} {agree:8.4f} {flips:6d}")

        # ---- label sample from the four corners of the CURRENT verdicts
        s = d.dropna(subset=["v_new"])
        s = s[s.context_chars <= LABEL_MAX_CHARS].copy()
        s["family"] = s.policy.astype(str).str.startswith("compression").map(
            {True: "compression", False: "eviction"})
        for (fam, cont, judge), g in s.groupby(
                ["family", "answer_in_context", "v_new"]):
            take = g.drop_duplicates("new_key").sample(
                min(PER_CORNER, g.new_key.nunique()), random_state=SEED)
            for _, r in take.iterrows():
                sid = f"s{len(label_rows):04d}"
                label_rows.append({
                    "sample_id": sid, "workload": w, "family": fam,
                    "question": r.question, "answer": r.answer,
                    "context": r.context, "context_chars": r.context_chars,
                    "human_sufficient": "", "cause": "", "note": ""})
                key_rows.append({
                    "sample_id": sid, "workload": w, "family": fam,
                    "policy": r.policy, "budget": r.get("budget", ""),
                    "query_id": r.query_id, "new_key": r.new_key,
                    "answer_in_context": bool(cont), "judge": bool(judge)})

    random.shuffle(label_rows)
    out = Path("corpora/sufficiency"); out.mkdir(parents=True, exist_ok=True)
    for name, rows in (("label_me.csv", label_rows), ("label_key.csv", key_rows)):
        with open(out / name, "w", newline="", encoding="utf-8") as f:
            wcsv = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            wcsv.writeheader(); wcsv.writerows(rows)
    print(f"\nwrote {len(label_rows)} rows to {out}/label_me.csv (BLIND: no verdicts)")
    print(f"wrote the verdict key to {out}/label_key.csv — do not open until labelled")
    print("\ncorner coverage (family, contain, judge):")
    kdf = pd.DataFrame(key_rows)
    print(kdf.groupby(["family", "answer_in_context", "judge"]).size().to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
