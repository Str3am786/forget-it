"""Build (and cache) the merged per-query analysis frame.

results.csv ⋈ conjunctive-survival snapshot, on
(policy, itemiser, retriever, budget_frac, query_id). Adds S_conj (all gold
resident) alongside the results-native S_any. Cached to parquet so Phases 2/3/4
reuse one build. Ground truth: artifacts/benchmark_full/ only.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from beam_load import load_results, load_conjunctive_survival

CACHE = Path(__file__).resolve().parents[1] / "tables" / "_merged_frame.parquet"


def build(force: bool = False) -> pd.DataFrame:
    if CACHE.exists() and not force:
        return pd.read_parquet(CACHE)

    df = load_results()
    cs = load_conjunctive_survival()
    keys = ["policy", "itemiser", "retriever", "budget_frac", "query_id"]
    cs_small = cs[keys + ["S_conj", "n_gold_resident", "n_gold_snap"]]
    m = df.merge(cs_small, on=keys, how="left", validate="one_to_one")

    # answerable & gradeable subset flags for the answer stage (Phase 0 F-A)
    from beam_load import UNGRADEABLE_TYPES
    m["gradeable"] = m["answerable"] & ~m["question_type"].isin(UNGRADEABLE_TYPES)

    CACHE.parent.mkdir(parents=True, exist_ok=True)
    m.to_parquet(CACHE, index=False)
    return m


if __name__ == "__main__":
    m = build(force=True)
    print("merged rows:", len(m))
    miss = m["S_conj"].isna().sum()
    print("S_conj missing after merge:", miss)
    print("gradeable rows:", int(m["gradeable"].sum()),
          f"({m['gradeable'].mean()*100:.1f}%)")
    # sanity: S_conj implies S_any
    bad = ((m["S_conj"] == True) & (m["S_any"] == False)).sum()
    print("S_conj & !S_any (should be 0):", int(bad))
