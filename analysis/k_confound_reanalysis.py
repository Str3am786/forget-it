"""D2 — the k-confound: fixed-k vs composition-invariant retrieval metrics.

Pure RE-ANALYSIS of recorded ``benchmark_full`` artifacts (read-only): no new
runs, no store replays. For the primary cell (beam × beam_turn_pair × bm25,
compression vs lru, all budgets) it computes, per scored query:

  - fixed-k hit@5 (the recorded column, relabelled fixed-k),
  - gold_rank_percentile = best observed gold rank ÷ n_resident-at-query,
  - hit@kprop with k = max(5, ceil(c·n_resident)), c calibrated so the MEDIAN
    store of the whole comparison population gets k = 5.

n_resident at query time is reconstructed from events.jsonl (resident id SET
replayed over insert/evict/clear; read at each ``gold`` event) because the
recorded CSVs predate the n_resident_at_query column. The reconstruction is
cross-checked against results.csv: every row with n_retrieved < eval depth
must satisfy n_retrieved == n_resident (retrieval returns min(depth, n)).

Censoring (gold_ranks logged only to depth 10) is carried as BOUNDS, never
imputed silently: a surviving-but-unranked gold contributes percentile
11/n (optimistic) vs 1.0 (pessimistic); an evicted gold contributes 1.0 to
both. hit@kprop optimistic counts a censored row as a hit iff its k_prop > 10.

Usage:  python analysis/k_confound_reanalysis.py [--sample-handcheck N]
Output: report/02_k_confound_tables.md (+ handcheck sample JSON on stdout)
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
BASE = REPO / "artifacts" / "benchmark_full" / "beam" / "beam_turn_pair" / "retention"
POLICIES = ("lru", "compression")
BUDGETS = ("010", "025", "050", "075", "100")
RETRIEVER = "bm25"
EVAL_DEPTH = 10          # max(EVAL_K_CURVE) at recording time
K_FLOOR = 5


def config_dir(policy: str, budget: str) -> Path:
    return BASE / policy / RETRIEVER / f"budget_{budget}pct" / "PlainTextReader"


def resident_counts_by_query(events_path: Path) -> dict[str, int]:
    """Replay the resident id SET over the event stream; snapshot its size at
    each ``gold`` event (emitted immediately before the scored retrieval, and
    scored queries fire after the example's last insert)."""
    resident: set[str] = set()
    out: dict[str, int] = {}
    with events_path.open() as f:
        for line in f:
            e = json.loads(line)
            name = e.get("name")
            if name == "insert":
                resident.add(e["id"])
            elif name in ("evict", "delete"):
                resident.discard(e["id"])
            elif name == "clear":
                resident = set()
            elif name == "gold":
                out[e["query_id"]] = len(resident)
    return out


def load_cell(policy: str, budget: str) -> pd.DataFrame:
    d = config_dir(policy, budget)
    df = pd.read_csv(d / "results.csv")
    counts = resident_counts_by_query(d / "events.jsonl")
    df["n_resident"] = df["query_id"].map(counts)
    missing = df["n_resident"].isna().sum()
    if missing:
        raise RuntimeError(f"{d}: {missing} rows without a reconstructed resident count")
    df["n_resident"] = df["n_resident"].astype(int)

    # Cross-check: whenever the store held fewer items than the eval depth,
    # the retrieved count must equal the resident count exactly.
    short = df[df["n_retrieved"] < EVAL_DEPTH]
    bad = short[short["n_retrieved"] != short["n_resident"]]
    if len(bad):
        raise RuntimeError(
            f"{d}: resident-count reconstruction disagrees with n_retrieved on "
            f"{len(bad)} rows, e.g.\n{bad[['query_id', 'n_retrieved', 'n_resident']].head()}"
        )

    df["ranks"] = df["gold_ranks"].apply(json.loads)
    df["best_rank"] = df["ranks"].apply(lambda r: min(r) if r else None)
    df["policy_name"] = policy
    df["budget_pct"] = int(budget)
    return df


def add_metrics(df: pd.DataFrame, c: float) -> pd.DataFrame:
    n = df["n_resident"]
    best = df["best_rank"]
    observed = best.notna()
    survived = df["gold_survived"].astype(bool)

    # percentile bounds: observed → r/n exactly; survived-censored → [11/n, 1];
    # evicted → 1.0 in both (the policy definitively lost the item).
    pct_opt = pd.Series(1.0, index=df.index, dtype=float)
    pct_pes = pd.Series(1.0, index=df.index, dtype=float)
    pct_opt[observed] = best[observed] / n[observed]
    pct_pes[observed] = best[observed] / n[observed]
    censored = survived & ~observed
    pct_opt[censored] = (EVAL_DEPTH + 1) / n[censored]
    df["pct_optimistic"] = pct_opt
    df["pct_pessimistic"] = pct_pes
    df["is_censored"] = censored
    df["is_evicted"] = ~survived

    kp = (c * n).apply(math.ceil).clip(lower=K_FLOOR).astype(int)
    df["k_prop"] = kp
    hit_obs = observed & (best <= kp.where(kp <= EVAL_DEPTH, EVAL_DEPTH))
    # observed hit is exact when k_prop <= depth; when k_prop > depth an
    # observed rank <= depth < k_prop is still a definite hit.
    definite_hit = observed & (best <= kp)
    df["hit_kprop_pessimistic"] = definite_hit.astype(float)
    df["hit_kprop_optimistic"] = (definite_hit | (censored & (kp > EVAL_DEPTH))).astype(float)
    del hit_obs
    return df


def _md(df: pd.DataFrame, fmt: str = ".3f") -> str:
    """Minimal markdown table (no tabulate dependency)."""
    def cell(v):
        if isinstance(v, float):
            return format(v, fmt)
        return str(v)
    cols = list(df.columns)
    lines = ["| " + " | ".join(cols) + " |",
             "|" + "|".join("---" for _ in cols) + "|"]
    for _, row in df.iterrows():
        lines.append("| " + " | ".join(cell(row[c]) for c in cols) + " |")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample-handcheck", type=int, default=10)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    cells = [load_cell(p, b) for p in POLICIES for b in BUDGETS]
    pool = pd.concat(cells, ignore_index=True)

    median_n = pool["n_resident"].median()
    c = K_FLOOR / median_n
    pool = add_metrics(pool, c)

    rows = []
    for (policy, budget), g in pool.groupby(["policy_name", "budget_pct"]):
        rows.append({
            "policy": policy,
            "budget_pct": budget,
            "n_queries": len(g),
            "mean_n_resident": g["n_resident"].mean(),
            "gold_survival": g["gold_survived"].mean(),
            "hit_at_5_fixedk": g["hit_at_5"].mean(),
            "median_k_prop": g["k_prop"].median(),
            "hit_kprop_pess": g["hit_kprop_pessimistic"].mean(),
            "hit_kprop_opt": g["hit_kprop_optimistic"].mean(),
            "pct_pess": g["pct_pessimistic"].mean(),
            "pct_opt": g["pct_optimistic"].mean(),
            "censored_frac": g["is_censored"].mean(),
            "evicted_frac": g["is_evicted"].mean(),
        })
    summary = pd.DataFrame(rows).sort_values(["budget_pct", "policy"])

    # deltas: compression − lru per budget
    deltas = []
    for budget in sorted(summary["budget_pct"].unique()):
        s = summary[summary["budget_pct"] == budget].set_index("policy")
        d = s.loc["compression"] - s.loc["lru"]
        deltas.append({
            "budget_pct": budget,
            "d_hit_at_5_fixedk": d["hit_at_5_fixedk"],
            "d_hit_kprop_pess": d["hit_kprop_pess"],
            "d_hit_kprop_opt": d["hit_kprop_opt"],
            "d_pct_pess": d["pct_pess"],      # negative = compression better
            "d_pct_opt": d["pct_opt"],
        })
    deltas = pd.DataFrame(deltas)

    out = REPO / "report" / "02_k_confound_tables.md"
    with out.open("w") as f:
        f.write("# D2 tables (generated by analysis/k_confound_reanalysis.py)\n\n")
        f.write(f"Comparison population: beam_turn_pair × bm25, policies {POLICIES}, "
                f"budgets {BUDGETS} — {len(pool)} scored rows.\n\n")
        f.write(f"median n_resident = {median_n:.0f} → c = {K_FLOOR}/{median_n:.0f} "
                f"= {c:.5f} (median store gets k = {K_FLOOR})\n\n")
        f.write("## Per-cell metrics\n\n")
        f.write(_md(summary, ".3f"))
        f.write("\n\n## Compression − LRU deltas (per budget)\n\n")
        f.write("Positive hit-deltas favor compression; NEGATIVE percentile deltas "
                "favor compression (lower percentile = gold ranked nearer the top "
                "of the resident set).\n\n")
        f.write(_md(deltas, "+.3f"))
        f.write("\n")
    print(f"wrote {out}")

    # hand-check sample: observed-rank rows only (exact percentiles)
    obs = pool[pool["best_rank"].notna()]
    sample = obs.sample(n=args.sample_handcheck, random_state=args.seed)
    handcheck = sample[["policy_name", "budget_pct", "query_id", "gold_ranks",
                        "n_resident", "pct_optimistic", "k_prop",
                        "hit_kprop_pessimistic", "hit_at_5"]]
    hc_path = REPO / "report" / "02_handcheck_sample.json"
    handcheck.to_json(hc_path, orient="records", indent=1)
    print(f"wrote {hc_path}")
    print(handcheck.to_string(index=False))


if __name__ == "__main__":
    main()
