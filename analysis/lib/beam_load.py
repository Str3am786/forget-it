"""Shared loaders for the BEAM eviction-policy analysis.

Single source of truth for turning the committed benchmark artifacts into tidy,
derived tables the phase drivers consume. Nothing here fabricates: every derived
column is a deterministic function of columns that are actually present in
``combined_results.csv`` or the per-config ``gold_answer_snapshot.jsonl`` /
``events.jsonl`` files.

Key derivations (see the BEAM-analysis protocol):
  example_id  — the conversation id (query_id before the first '::').
  gold_ranks  — parsed list[int]; 1-based ranks of gold items in the ranked list.
  R_conj      — retrieval-conjunctive: ALL of a query's gold in the top-k
                (== recall_at_k == 1.0). R_any == hit_at_k.
  answerable  — ref_answer is non-empty (the deterministic grader can score it).
  CHO         — three-way outcome: O(mitted)=abstained, C(orrect)=qa_correct,
                H(allucinated)=answered-but-wrong. Mutually exclusive, sum to 1.
  S_conj      — survival-conjunctive: NONE of a query's gold items were evicted,
                from the gold snapshot (results.csv only carries ANY-survival).
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
BENCH_FULL = REPO / "artifacts" / "benchmark_full"
RESULTS_CSV = BENCH_FULL / "combined_results.csv"
BEAM_ROOT = BENCH_FULL / "beam"

# The run's configured reader top-k (used for R_conj / R_any). k=5 in every runner.
K = 5

# BEAM question types whose reference answer is empty for EVERY example — the
# deterministic grader cannot score them at all (confirmed in Phase 0).
UNGRADEABLE_TYPES = ("instruction_following", "preference_following", "summarization")

POLICIES = ["lru", "lfu", "random", "keep_smallest", "evict_smallest",
            "semantic", "semantic_coverage", "compression"]
ITEMISERS = ["beam_turn", "beam_turn_pair", "beam_batch"]
RETRIEVERS = ["lexical", "bm25", "dense"]
BUDGET_FRACS = [0.1, 0.25, 0.5, 0.75, 1.0]


def _to_bool(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower().eq("true")


def _parse_ranks(v) -> list[int]:
    if not v or v == "[]":
        return []
    try:
        return [int(x) for x in json.loads(v)]
    except (ValueError, TypeError):
        return []


def load_results(dataset: str = "beam") -> pd.DataFrame:
    """Load the combined per-query results for ``dataset`` with derived columns.

    One row == one scored query under one (policy, itemiser, retriever, budget)
    config. Derived columns documented in the module docstring.
    """
    df = pd.read_csv(RESULTS_CSV, low_memory=False)
    df = df[df["dataset"] == dataset].copy()

    df["budget_frac"] = df["budget_frac"].astype(float)
    df["n_gold"] = df["n_gold"].astype(int)
    df["example_id"] = df["query_id"].str.split("::").str[0]

    for c in ("gold_survived", "abstained", "qa_correct", "introspectable"):
        df[c] = _to_bool(df[c])

    for c in ("hit_at_k", "mrr", "qa_f1", "recall_at_5", "hit_at_5",
              "used_bytes_end", "budget_bytes", "useful_byte_fraction"):
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["gold_ranks_list"] = df["gold_ranks"].map(_parse_ranks)
    df["ref_answer"] = df["ref_answer"].fillna("").astype(str)

    # ── stage indicators ─────────────────────────────────────────────
    df["S_any"] = df["gold_survived"]                    # ANY gold resident
    # retrieval-conjunctive: all gold in top-k  (recall_at_k == 1)
    df["R_conj"] = df["recall_at_5"].fillna(0.0).ge(1.0)
    df["R_any"] = df["hit_at_5"].fillna(0.0).gt(0.0)
    df["A"] = df["qa_correct"]

    # answerable = grader has a non-empty gold string to match against
    df["answerable"] = df["ref_answer"].str.strip().ne("")

    # three-way outcome C/H/O (only meaningful on answerable rows)
    cho = np.where(df["abstained"], "O",
                   np.where(df["A"], "C", "H"))
    df["CHO"] = cho

    # pressure flag: the store actually evicted/compacted (budget < footprint).
    # budget_frac == 1.0 is the no-eviction control (no pressure by construction).
    df["under_pressure"] = df["budget_frac"] < 1.0
    return df


def _config_from_path(p: Path) -> dict | None:
    """Recover (itemiser, policy, retriever, budget_frac) from the artifact path
    <root>/beam/<itemiser>/retention/<policy>/<retriever>/budget_NNNpct/<reader>/..."""
    parts = p.parts
    if "retention" not in parts:
        return None
    i = parts.index("retention")
    try:
        policy, retriever, budget = parts[i + 1], parts[i + 2], parts[i + 3]
        itemiser = parts[i - 1]
    except IndexError:
        return None
    frac = None
    if budget.startswith("budget_") and budget.endswith("pct"):
        try:
            frac = int(budget[len("budget_"):-len("pct")]) / 100.0
        except ValueError:
            pass
    return {"itemiser": itemiser, "policy": policy, "retriever": retriever,
            "budget_frac": frac}


def load_conjunctive_survival() -> pd.DataFrame:
    """Per (config, query) conjunctive survival from the gold snapshots.

    S_conj(q) = 1 iff NONE of query q's gold items were evicted. Returns tidy rows
    keyed by (policy, itemiser, retriever, budget_frac, example_id, query_id) with
    columns S_conj (all gold resident), S_any (>=1 gold resident), n_gold,
    n_gold_resident. This is the ONLY source of conjunctive survival — results.csv
    carries ANY-survival only.
    """
    recs: list[dict] = []
    for snap in BEAM_ROOT.rglob("gold_answer_snapshot.jsonl"):
        cfg = _config_from_path(snap)
        if cfg is None or cfg["budget_frac"] is None:
            continue
        # group snapshot rows by query
        by_q: dict[str, list[dict]] = {}
        with snap.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                by_q.setdefault(row.get("query_id", ""), []).append(row)
        for qid, golds in by_q.items():
            n = len(golds)
            n_res = sum(0 if g.get("was_evicted", False) else 1 for g in golds)
            recs.append({**cfg, "example_id": qid.split("::")[0], "query_id": qid,
                         "n_gold_snap": n, "n_gold_resident": n_res,
                         "S_conj": (n_res == n and n > 0),
                         "S_any_snap": (n_res > 0)})
    return pd.DataFrame.from_records(recs)


# ── example-level cluster bootstrap (BEAM ~35 examples, queries correlated) ──

def cluster_bootstrap_mean(df: pd.DataFrame, value_col: str,
                           cluster_col: str = "example_id",
                           n_boot: int = 10000, seed: int = 0) -> dict:
    """Cluster bootstrap CI for the mean of ``value_col``, resampling whole
    clusters (examples) with replacement. Returns {mean, lo, hi, se, n_clusters}.

    This is the protocol's mandated uncertainty: resample examples, not queries,
    because within-example queries are correlated. The point estimate is the
    plain (query-weighted) mean; the CI reflects between-example variance.
    """
    rng = np.random.default_rng(seed)
    g = df[[cluster_col, value_col]].dropna()
    clusters = g[cluster_col].unique()
    # precompute per-cluster (sum, count) for a fast weighted resample
    grp = g.groupby(cluster_col)[value_col]
    sums = grp.sum().reindex(clusters).to_numpy(dtype=float)
    cnts = grp.count().reindex(clusters).to_numpy(dtype=float)
    m = len(clusters)
    if m == 0:
        return {"mean": np.nan, "lo": np.nan, "hi": np.nan, "se": np.nan, "n_clusters": 0}
    point = sums.sum() / cnts.sum()
    idx = rng.integers(0, m, size=(n_boot, m))
    boot = sums[idx].sum(axis=1) / cnts[idx].sum(axis=1)
    lo, hi = np.percentile(boot, [2.5, 97.5])
    return {"mean": float(point), "lo": float(lo), "hi": float(hi),
            "se": float(boot.std(ddof=1)), "n_clusters": int(m)}


def cluster_bootstrap_paired(df_a: pd.DataFrame, df_b: pd.DataFrame,
                             value_col: str, key=("example_id", "query_id"),
                             cluster_col: str = "example_id",
                             n_boot: int = 10000, seed: int = 0) -> dict:
    """Paired example-cluster bootstrap for mean(A) - mean(B) on the same queries.

    Aligns A and B on ``key`` (so every query is compared to itself), then
    resamples example clusters. Returns {delta, lo, hi, se, n_pairs, p_two_sided}.
    """
    rng = np.random.default_rng(seed)
    a = df_a[list(key) + [value_col]].rename(columns={value_col: "a"})
    b = df_b[list(key) + [value_col]].rename(columns={value_col: "b"})
    m = a.merge(b, on=list(key), how="inner").dropna(subset=["a", "b"])
    m["d"] = m["a"] - m["b"]
    clusters = m[cluster_col].unique()
    grp = m.groupby(cluster_col)["d"]
    sums = grp.sum().reindex(clusters).to_numpy(dtype=float)
    cnts = grp.count().reindex(clusters).to_numpy(dtype=float)
    n = len(clusters)
    if n == 0:
        return {"delta": np.nan, "lo": np.nan, "hi": np.nan, "se": np.nan,
                "n_pairs": 0, "p_two_sided": np.nan}
    point = sums.sum() / cnts.sum()
    idx = rng.integers(0, n, size=(n_boot, n))
    boot = sums[idx].sum(axis=1) / cnts[idx].sum(axis=1)
    lo, hi = np.percentile(boot, [2.5, 97.5])
    # two-sided bootstrap p: proportion of resamples on the other side of 0
    p = 2.0 * min((boot <= 0).mean(), (boot >= 0).mean())
    return {"delta": float(point), "lo": float(lo), "hi": float(hi),
            "se": float(boot.std(ddof=1)), "n_pairs": int(len(m)),
            "p_two_sided": float(min(1.0, p))}


if __name__ == "__main__":
    df = load_results()
    print("rows:", len(df), "| examples:", df.example_id.nunique(),
          "| queries:", df.query_id.nunique())
    print(df[["policy", "itemiser", "retriever", "budget_frac"]].nunique())
