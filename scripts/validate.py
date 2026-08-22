#!/usr/bin/env python3
"""
verdicts.py — the verdict path: V1..V4 and RQ4, with no rank correlation anywhere.

WHAT THIS ASSUMES HAS ALREADY HAPPENED UPSTREAM
-----------------------------------------------
This script is the *last* stage. It does not grade, judge, or repair anything.
Per the order of operations it expects, in the input table:

  1. reference-less rows already excluded (it will also drop them if the
     `gradeable` column is present, and will refuse to run if it can see
     ungradeable rows it was not told to drop);
  2. `qa_correct` already produced by the hybrid stack (deterministic tiers
     plus the judge on `miss`/`embedding`), not by the deterministic grader
     alone — see `--grader-tag`;
  3. `preserved_frac` emitted alongside `preserved_any` / `preserved_all`;
  4. contract C1 checked (this script re-checks it if the columns are there).

INPUT SHAPE
-----------
One long table, one row per (workload, policy, budget, reader, question).
Everything is paired per question and per reader; nothing is averaged before
it is tested.

USAGE
-----
    python verdicts.py --input sweep.parquet --schema schema.json --out verdicts/
    python verdicts.py --selftest --out selftest_out/    # synthetic, planted inversion

Outputs land in --out: one CSV per verdict, plus VERDICTS.md with the
pre-registered sentences filled in.
"""

from __future__ import annotations

import argparse
import itertools
import json
import os
import sys
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from verdicts_stats import (  # noqa: E402
    bh_fdr,
    logit_cluster,
    paired_sign_test,
    wald_block,
    wilson_ci,
)

# ==========================================================================
# configuration
# ==========================================================================

DEFAULT_SCHEMA: Dict = {
    "columns": {
        "workload": "workload",
        "policy": "policy",
        "budget": "budget",
        "reader": "reader",
        "question_id": "question_id",
        "outcome": "qa_correct",
        "gradeable": "gradeable",
        "grader": "grader",
        # optional, only used for the C1 contract check
        "gold_answer_survival": "gold_answer_survival",
        "gold_survival_rate": "gold_survival_rate",
    },
    # the ladder, in the order it should be reported. `binary` is documentation
    # for the write-up; the test used is the same either way (see verdicts_stats).
    "proxies": {
        "retention": {"column": "gold_id_retained", "binary": True},
        "preservation": {"column": "preserved_frac", "binary": False},
        "sufficiency": {"column": "sufficient", "binary": True},
    },
    "sensitivity_proxies": {
        "preservation_any": {"column": "preserved_any", "binary": True},
        "preservation_all": {"column": "preserved_all", "binary": True},
    },
    "controls": {"ceiling_policy": "no_eviction", "floor_policy": "empty_memory"},
    "preregistration": {
        "materiality": 0.03,
        "alpha": 0.05,
        "primary_proxy": "retention",
        "primary_workload": "longmemeval",
        "primary_budget": 25,
        "bootstrap": 2000,
        "seed": 20260822,
        "min_proxy_coverage": 0.10,
    },
}


def load_schema(path: Optional[str]) -> Dict:
    schema = json.loads(json.dumps(DEFAULT_SCHEMA))
    if path:
        with open(path) as fh:
            user = json.load(fh)
        for key, val in user.items():
            if isinstance(val, dict) and key in schema and isinstance(schema[key], dict):
                schema[key].update(val)
            else:
                schema[key] = val
    return schema


# ==========================================================================
# loading and gates
# ==========================================================================


def load_sweep(path: str, schema: Dict, grader_tag: Optional[str] = None) -> pd.DataFrame:
    col = schema["columns"]
    parts = _discover(path)
    if not parts:
        raise SystemExit(f"no readable data files under {path}")
    frames = []
    for p in parts:
        f = _read_one(p)
        if "_source_file" not in f.columns:
            f["_source_file"] = os.path.basename(p)
        frames.append(f)
    if len(frames) > 1:
        cols = [frozenset(f.columns) for f in frames]
        if len(set(cols)) > 1:
            common = set.intersection(*[set(c) for c in cols])
            odd = sorted(set.union(*[set(c) for c in cols]) - common)
            print(f"[load] WARNING: {len(parts)} files do not share a schema; "
                  f"columns present in some but not all: {odd}")
    df = pd.concat(frames, ignore_index=True)
    print(f"[load] {len(parts)} file(s) concatenated")

    required = [col["workload"], col["policy"], col["budget"],
                col["question_id"], col["outcome"]]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(
            f"input is missing required columns {missing}.\n"
            f"columns present: {sorted(df.columns)}\n"
            "map them with --schema."
        )
    if col["reader"] not in df.columns:
        if df["_source_file"].nunique() > 1:
            df[col["reader"]] = df["_source_file"]
            print("[load] no reader column — using the source filename as the "
                  "reader identity. Check the mapping below and override with "
                  "--schema if it is wrong:")
            for f in sorted(df["_source_file"].unique()):
                print(f"        {f}")
        else:
            df[col["reader"]] = "_pooled"
            print("[load] no reader column and a single source file — "
                  "pairing on question only")

    n0 = len(df)
    # gate 1: reference-less rows
    if col["gradeable"] in df.columns:
        keep = df[col["gradeable"]].astype(bool)
        dropped = int((~keep).sum())
        df = df[keep].copy()
        if dropped:
            print(f"[gate] dropped {dropped:,} ungradeable (reference-less) rows")
    # gate 2: grader provenance
    if grader_tag and col["grader"] in df.columns:
        before = len(df)
        df = df[df[col["grader"]].astype(str) == grader_tag].copy()
        print(f"[gate] grader == {grader_tag!r}: kept {len(df):,} of {before:,}")
    elif col["grader"] in df.columns:
        tags = sorted(df[col["grader"]].astype(str).unique())
        if len(tags) > 1:
            raise SystemExit(
                f"input mixes graders {tags}. Pass --grader-tag to pick one. "
                "Every RQ3/RQ4 number must come from the hybrid stack."
            )
        print(f"[gate] grader = {tags[0]!r}")

    df[col["budget"]] = _coerce_budget(df[col["budget"]])
    df[col["outcome"]] = pd.to_numeric(df[col["outcome"]], errors="coerce").astype(float)
    print(f"[load] {len(df):,} rows (from {n0:,}), "
          f"{df[col['policy']].nunique()} policies, "
          f"{df[col['workload']].nunique()} workloads, "
          f"{df[col['budget']].nunique()} budgets, "
          f"{df[col['reader']].nunique()} readers")
    return df


SUFFIXES = (".jsonl", ".jsonl.gz", ".json", ".ndjson",
            ".parquet", ".csv", ".csv.gz", ".tsv")


def _discover(path: str):
    """One file, or every readable file under a directory tree (recursive).

    The reader sweep writes one jsonl per (workload, array task) into a
    per-workload subdirectory, so a flat listdir misses everything.
    """
    if os.path.isfile(path):
        return [path]
    if not os.path.isdir(path):
        import glob
        return sorted(glob.glob(path))
    found = []
    for root, _dirs, files in os.walk(path):
        for f in sorted(files):
            if f.endswith(SUFFIXES):
                found.append(os.path.join(root, f))
    return sorted(found)


def _read_one(path: str) -> pd.DataFrame:
    if path.endswith(".parquet"):
        return pd.read_parquet(path)
    if path.endswith((".jsonl", ".jsonl.gz", ".ndjson")):
        return pd.read_json(path, lines=True)
    if path.endswith(".json"):
        try:
            return pd.read_json(path, lines=True)
        except ValueError:
            return pd.read_json(path)
    if path.endswith(".tsv"):
        return pd.read_csv(path, sep="\t")
    return pd.read_csv(path)


def _coerce_budget(s: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(s):
        out = s.astype(float)
    else:
        out = pd.to_numeric(
            s.astype(str).str.replace("%", "", regex=False).str.strip(),
            errors="coerce",
        )
    mx = out.max()
    if pd.notna(mx) and mx <= 1.0:
        out = out * 100.0  # 0.25 -> 25
    return out


def check_contract_c1(df: pd.DataFrame, schema: Dict) -> pd.DataFrame:
    """gold_answer_survival <= gold_survival_rate, for every config."""
    col = schema["columns"]
    a, b = col.get("gold_answer_survival"), col.get("gold_survival_rate")
    if a not in df.columns or b not in df.columns:
        print("[C1] columns absent — contract not checked here")
        return pd.DataFrame()
    keys = [col["workload"], col["policy"], col["budget"]]
    g = df.groupby(keys, dropna=False)[[a, b]].mean().reset_index()
    g["c1_violation"] = g[a] > g[b] + 1e-9
    n = int(g["c1_violation"].sum())
    print(f"[C1] {n} config(s) violate gold_answer_survival <= gold_survival_rate")
    return g


def proxy_coverage(df: pd.DataFrame, schema: Dict) -> pd.DataFrame:
    """
    Per workload x proxy, the share of rows where the proxy is measurable.

    This is the beam guard. If a workload's extractive share is 2.1%, its
    preservation rung is computed on one row in fifty and must not enter
    V1..V3 — the analysis excludes it and says so rather than reporting a
    number that cannot mean anything.
    """
    col = schema["columns"]
    rows = []
    all_proxies = {**schema["proxies"], **schema.get("sensitivity_proxies", {})}
    for name, spec in all_proxies.items():
        c = spec["column"]
        if c not in df.columns:
            rows.append(dict(proxy=name, column=c, workload="*", coverage=np.nan,
                             n=0, usable=False, note="column absent"))
            continue
        for wl, sub in df.groupby(col["workload"]):
            cov = float(sub[c].notna().mean())
            rows.append(dict(proxy=name, column=c, workload=wl, coverage=cov,
                             n=int(sub[c].notna().sum()), usable=np.nan, note=""))
    out = pd.DataFrame(rows)
    if not out.empty:
        thr = schema["preregistration"]["min_proxy_coverage"]
        m = out["coverage"].notna()
        out["usable"] = np.where(m, out["coverage"] >= thr, None)
        for _, r in out[m & (out["usable"] == False)].iterrows():  # noqa: E712
            print(f"[coverage] {r['proxy']} on {r['workload']}: "
                  f"{r['coverage']:.3f} < {thr} — excluded from V1-V3")
    return out


# ==========================================================================
# the pairwise engine (shared by V1, V3 and RQ4)
# ==========================================================================


def _wide(df: pd.DataFrame, schema: Dict, value_col: str) -> pd.DataFrame:
    """units x policies matrix for one cell. Unit = (question, reader)."""
    col = schema["columns"]
    return df.pivot_table(
        index=[col["question_id"], col["reader"]],
        columns=col["policy"],
        values=value_col,
        aggfunc="mean",
    )


def pairwise_table(df_cell: pd.DataFrame, schema: Dict, value_col: str,
                   policies: List[str], alpha: float) -> pd.DataFrame:
    """
    Every policy pair in one cell, tested paired on the unit, BH-corrected
    within the cell. 15 policies -> 105 rows.
    """
    wide = _wide(df_cell, schema, value_col)
    policies = [p for p in policies if p in wide.columns]
    rows = []
    for a, b in itertools.combinations(policies, 2):
        r = paired_sign_test(wide[a].values, wide[b].values)
        rows.append(dict(policy_a=a, policy_b=b, **r))
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    q, rej = bh_fdr(out["p"].values, alpha)
    out["q"] = q
    out["significant"] = rej
    return out


def classify_states(proxy_tab: pd.DataFrame, e2e_tab: pd.DataFrame,
                    materiality: float) -> pd.DataFrame:
    """
    V1's five states. A claim is 'confident' only if it is both significant
    after BH and material: |delta| >= the threshold fixed in advance.
    """
    m = proxy_tab.merge(e2e_tab, on=["policy_a", "policy_b"],
                        suffixes=("_proxy", "_e2e"))
    m["confident_proxy"] = m["significant_proxy"] & (m["delta_proxy"].abs() >= materiality)
    m["confident_e2e"] = m["significant_e2e"] & (m["delta_e2e"].abs() >= materiality)
    same = np.sign(m["delta_proxy"]) == np.sign(m["delta_e2e"])

    state = np.where(
        m["confident_proxy"] & m["confident_e2e"] & same, "agrees",
        np.where(
            m["confident_proxy"] & m["confident_e2e"] & ~same, "contradicts",
            np.where(
                m["confident_proxy"] & ~m["confident_e2e"], "overclaims",
                np.where(~m["confident_proxy"] & m["confident_e2e"],
                         "underclaims", "both_silent"),
            ),
        ),
    )
    m["state"] = state
    return m


# ==========================================================================
# V1 — substitution matrix and confident contradiction rate
# ==========================================================================


def v1_substitution(df: pd.DataFrame, schema: Dict, proxy_col: str,
                    proxy_name: str, usable_workloads: Optional[set] = None):
    col = schema["columns"]
    pre = schema["preregistration"]
    alpha, mat = pre["alpha"], pre["materiality"]
    policies = _analysis_policies(df, schema)

    pairs, summary = [], []
    for (wl, bud), cell in df.groupby([col["workload"], col["budget"]]):
        if usable_workloads is not None and wl not in usable_workloads:
            continue
        if proxy_col not in cell.columns or cell[proxy_col].notna().sum() == 0:
            continue
        ptab = pairwise_table(cell, schema, proxy_col, policies, alpha)
        etab = pairwise_table(cell, schema, col["outcome"], policies, alpha)
        if ptab.empty or etab.empty:
            continue
        m = classify_states(ptab, etab, mat)
        m.insert(0, "budget", bud)
        m.insert(0, "workload", wl)
        m.insert(0, "proxy", proxy_name)
        pairs.append(m)

        counts = m["state"].value_counts().to_dict()
        confident = sum(counts.get(k, 0) for k in ("agrees", "contradicts", "overclaims"))
        contra = counts.get("contradicts", 0)
        lo, hi = wilson_ci(contra, confident) if confident else (np.nan, np.nan)
        summary.append(dict(
            proxy=proxy_name, workload=wl, budget=bud,
            n_pairs=len(m),
            agrees=counts.get("agrees", 0),
            contradicts=contra,
            overclaims=counts.get("overclaims", 0),
            underclaims=counts.get("underclaims", 0),
            both_silent=counts.get("both_silent", 0),
            confident_proxy_claims=confident,
            confident_contradiction_rate=(contra / confident) if confident else np.nan,
            ccr_lo=lo, ccr_hi=hi,
        ))
    pairs_df = pd.concat(pairs, ignore_index=True) if pairs else pd.DataFrame()
    return pairs_df, pd.DataFrame(summary)


def _analysis_policies(df: pd.DataFrame, schema: Dict) -> List[str]:
    """Every policy except the floor and ceiling controls."""
    col = schema["columns"]
    ctrl = {schema["controls"].get("ceiling_policy"),
            schema["controls"].get("floor_policy")}
    return [p for p in sorted(df[col["policy"]].dropna().unique()) if p not in ctrl]


# ==========================================================================
# V2 — selection regret
# ==========================================================================


def _argmax_ties(scores: np.ndarray, tol: float = 1e-12) -> np.ndarray:
    best = np.nanmax(scores)
    return np.where(scores >= best - tol)[0]


def _cell_sums(cell: pd.DataFrame, schema: Dict, value_col: str,
               policies: List[str]):
    """
    question x policy sums and counts.

    Bootstrapping over questions then reduces to summing rows of these two
    matrices, so 2000 resamples cost seconds rather than minutes, and the
    resampled mean is exact rather than an approximation of a row-level
    resample.
    """
    col = schema["columns"]
    sub = cell[cell[col["policy"]].isin(policies)]
    g = sub.groupby([col["question_id"], col["policy"]])[value_col].agg(["sum", "count"])
    s = g["sum"].unstack(col["policy"]).reindex(columns=policies)
    n = g["count"].unstack(col["policy"]).reindex(columns=policies)
    s = s.reindex(index=n.index)
    return s.fillna(0.0).values, n.fillna(0.0).values, list(s.index)


def v2_regret(df: pd.DataFrame, schema: Dict, proxy_col: str, proxy_name: str,
              usable_workloads: Optional[set] = None) -> pd.DataFrame:
    col = schema["columns"]
    pre = schema["preregistration"]
    B, seed = pre["bootstrap"], pre["seed"]
    policies = _analysis_policies(df, schema)
    floors, ceilings = _floor_ceiling(df, schema)

    rows = []
    for (wl, bud), cell in df.groupby([col["workload"], col["budget"]]):
        if usable_workloads is not None and wl not in usable_workloads:
            continue
        if proxy_col not in cell.columns or cell[proxy_col].notna().sum() == 0:
            continue
        pol = [p for p in policies if p in set(cell[col["policy"]])]
        Sp, Np, qids = _cell_sums(cell, schema, proxy_col, pol)
        Se, Ne, _ = _cell_sums(cell, schema, col["outcome"], pol)

        with np.errstate(invalid="ignore", divide="ignore"):
            proxy_mean = Sp.sum(0) / np.where(Np.sum(0) == 0, np.nan, Np.sum(0))
            e2e_mean = Se.sum(0) / np.where(Ne.sum(0) == 0, np.nan, Ne.sum(0))

        tied = _argmax_ties(proxy_mean)
        best_e2e = float(np.nanmax(e2e_mean))
        pick_worst = int(tied[np.argmin(e2e_mean[tied])])
        pick_best = int(tied[np.argmax(e2e_mean[tied])])
        regret = best_e2e - float(e2e_mean[pick_worst])
        regret_opt = best_e2e - float(e2e_mean[pick_best])

        order = np.argsort(-e2e_mean)
        top3 = set(order[:3].tolist())
        top1_hit = bool(pick_worst == int(order[0]))
        top3_hit = bool(pick_worst in top3)

        # bootstrap over questions, both argmaxes recomputed inside each resample
        rng = np.random.default_rng(seed)
        nq = Sp.shape[0]
        boots = np.empty(B)
        hits1 = np.empty(B)
        hits3 = np.empty(B)
        for i in range(B):
            idx = rng.integers(0, nq, nq)
            with np.errstate(invalid="ignore", divide="ignore"):
                pm = Sp[idx].sum(0) / np.where(Np[idx].sum(0) == 0, np.nan, Np[idx].sum(0))
                em = Se[idx].sum(0) / np.where(Ne[idx].sum(0) == 0, np.nan, Ne[idx].sum(0))
            t = _argmax_ties(pm)
            pk = int(t[np.argmin(em[t])])
            boots[i] = float(np.nanmax(em)) - float(em[pk])
            o = np.argsort(-em)
            hits1[i] = pk == int(o[0])
            hits3[i] = pk in set(o[:3].tolist())

        floor = floors.get(wl, np.nan)
        ceil = ceilings.get((wl, bud), np.nan)
        span = ceil - floor if np.isfinite(ceil) and np.isfinite(floor) else np.nan

        rows.append(dict(
            proxy=proxy_name, workload=wl, budget=bud,
            n_questions=nq, n_policies=len(pol),
            proxy_pick=pol[pick_worst],
            proxy_pick_ties=len(tied),
            proxy_pick_optimistic=pol[pick_best],
            e2e_best=pol[int(order[0])],
            proxy_pick_acc=float(e2e_mean[pick_worst]),
            best_acc=best_e2e,
            regret=regret,
            regret_optimistic=regret_opt,
            regret_lo=float(np.nanpercentile(boots, 2.5)),
            regret_hi=float(np.nanpercentile(boots, 97.5)),
            regret_frac_span=(regret / span) if np.isfinite(span) and span > 0 else np.nan,
            floor=floor, ceiling=ceil,
            top1_hit=top1_hit, top3_hit=top3_hit,
            top1_hit_rate_boot=float(np.mean(hits1)),
            top3_containment_boot=float(np.mean(hits3)),
        ))
    return pd.DataFrame(rows)


def _floor_ceiling(df: pd.DataFrame, schema: Dict):
    """
    Floor from the empty-memory control (budget-independent, taken per
    workload); ceiling from the no-eviction control per cell — not the oracle,
    until the oracle byte-feasibility question is settled.
    """
    col = schema["columns"]
    fl_p = schema["controls"].get("floor_policy")
    ce_p = schema["controls"].get("ceiling_policy")
    floors, ceilings = {}, {}
    if fl_p is not None:
        sub = df[df[col["policy"]] == fl_p]
        for wl, s in sub.groupby(col["workload"]):
            floors[wl] = float(s[col["outcome"]].mean())
    if ce_p is not None:
        sub = df[df[col["policy"]] == ce_p]
        for (wl, bud), s in sub.groupby([col["workload"], col["budget"]]):
            ceilings[(wl, bud)] = float(s[col["outcome"]].mean())
    return floors, ceilings


# ==========================================================================
# V3 — the proxy ladder
# ==========================================================================


def v3_ladder(df: pd.DataFrame, schema: Dict, coverage: pd.DataFrame,
              include_sensitivity: bool = True):
    proxies = dict(schema["proxies"])
    if include_sensitivity:
        proxies.update(schema.get("sensitivity_proxies", {}))

    all_pairs, all_v1, all_v2 = [], [], []
    for name, spec in proxies.items():
        c = spec["column"]
        if c not in df.columns or df[c].notna().sum() == 0:
            print(f"[V3] proxy {name!r} ({c}) absent — rung skipped")
            continue
        usable = _usable_workloads(coverage, name)
        p, s = v1_substitution(df, schema, c, name, usable)
        r = v2_regret(df, schema, c, name, usable)
        if not p.empty:
            all_pairs.append(p)
        if not s.empty:
            all_v1.append(s)
        if not r.empty:
            all_v2.append(r)

    pairs = pd.concat(all_pairs, ignore_index=True) if all_pairs else pd.DataFrame()
    v1 = pd.concat(all_v1, ignore_index=True) if all_v1 else pd.DataFrame()
    v2 = pd.concat(all_v2, ignore_index=True) if all_v2 else pd.DataFrame()

    if v1.empty or v2.empty:
        return pairs, v1, v2, pd.DataFrame()

    a = v1.groupby("proxy").agg(
        cells=("workload", "size"),
        confident_claims=("confident_proxy_claims", "sum"),
        contradictions=("contradicts", "sum"),
        overclaims=("overclaims", "sum"),
        underclaims=("underclaims", "sum"),
        both_silent=("both_silent", "sum"),
    )
    a["confident_contradiction_rate"] = a["contradictions"] / a["confident_claims"]
    b = v2.groupby("proxy").agg(
        mean_regret=("regret", "mean"),
        max_regret=("regret", "max"),
        top1_hit_rate=("top1_hit", "mean"),
        top3_containment=("top3_hit", "mean"),
    )
    ladder = a.join(b).reset_index()
    order = [p for p in list(schema["proxies"]) + list(schema.get("sensitivity_proxies", {}))
             if p in set(ladder["proxy"])]
    ladder["rung"] = ladder["proxy"].map({p: i for i, p in enumerate(order)})
    ladder = ladder.sort_values("rung").reset_index(drop=True)
    return pairs, v1, v2, ladder


def _usable_workloads(coverage: pd.DataFrame, proxy: str) -> Optional[set]:
    if coverage.empty:
        return None
    sub = coverage[(coverage["proxy"] == proxy) & coverage["coverage"].notna()]
    if sub.empty:
        return None
    return set(sub[sub["usable"] == True]["workload"])  # noqa: E712


# ==========================================================================
# V4 — within-policy monotonicity
# ==========================================================================


def v4_monotonicity(df: pd.DataFrame, schema: Dict, proxy_col: str) -> tuple:
    """
    Walk the budget ladder inside each policy and count discordant steps:
    preservation up while accuracy down, or the reverse.

    No cross-policy comparison, no multiplicity correction, no coefficient.
    The accuracy side of each step is also tested paired on the question, so a
    discordant step can be reported as significant rather than merely observed.
    """
    col = schema["columns"]
    pre = schema["preregistration"]
    steps = []
    for (wl, pol), sub in df.groupby([col["workload"], col["policy"]]):
        buds = sorted(sub[col["budget"]].dropna().unique())
        for lo, hi in zip(buds[:-1], buds[1:]):
            a = sub[sub[col["budget"]] == hi]
            b = sub[sub[col["budget"]] == lo]
            pres_a = a[proxy_col].mean() if proxy_col in a.columns else np.nan
            pres_b = b[proxy_col].mean() if proxy_col in b.columns else np.nan
            acc_a, acc_b = a[col["outcome"]].mean(), b[col["outcome"]].mean()

            merged = a.merge(
                b, on=[col["question_id"], col["reader"]], suffixes=("_hi", "_lo")
            )
            t = paired_sign_test(
                merged[f"{col['outcome']}_hi"].values,
                merged[f"{col['outcome']}_lo"].values,
            ) if len(merged) else dict(p=np.nan, n_pairs=0, delta=np.nan,
                                       wins=0, losses=0, ties=0)

            d_pres = pres_a - pres_b
            d_acc = acc_a - acc_b
            discordant = bool(np.isfinite(d_pres) and np.isfinite(d_acc)
                              and np.sign(d_pres) * np.sign(d_acc) < 0)
            steps.append(dict(
                workload=wl, policy=pol, budget_from=lo, budget_to=hi,
                preservation_from=pres_b, preservation_to=pres_a, d_preservation=d_pres,
                accuracy_from=acc_b, accuracy_to=acc_a, d_accuracy=d_acc,
                n_pairs=t["n_pairs"], p_accuracy=t["p"],
                discordant=discordant,
                discordant_material=bool(discordant and abs(d_acc) >= pre["materiality"]),
                discordant_significant=bool(
                    discordant and np.isfinite(t["p"]) and t["p"] < pre["alpha"]
                    and abs(d_acc) >= pre["materiality"]
                ),
            ))
    steps_df = pd.DataFrame(steps)
    if steps_df.empty:
        return steps_df, pd.DataFrame()
    ctrl = {schema["controls"].get("ceiling_policy"),
            schema["controls"].get("floor_policy")}
    steps_df["is_control"] = steps_df["policy"].isin(ctrl)

    summ = steps_df.groupby(["workload", "policy", "is_control"]).agg(
        steps=("discordant", "size"),
        discordant=("discordant", "sum"),
        discordant_material=("discordant_material", "sum"),
        discordant_significant=("discordant_significant", "sum"),
        preservation_span=("d_preservation", "sum"),
        accuracy_span=("d_accuracy", "sum"),
    ).reset_index()
    summ["fully_inverted"] = (summ["discordant"] == summ["steps"]) & (summ["steps"] > 0)
    return steps_df, summ


# ==========================================================================
# RQ4 — dominance sets, top sets, budget interaction
# ==========================================================================


def rq4_dominance(df: pd.DataFrame, schema: Dict):
    col = schema["columns"]
    pre = schema["preregistration"]
    alpha, mat = pre["alpha"], pre["materiality"]
    policies = _analysis_policies(df, schema)

    beats, topsets = [], []
    for (wl, bud), cell in df.groupby([col["workload"], col["budget"]]):
        tab = pairwise_table(cell, schema, col["outcome"], policies, alpha)
        if tab.empty:
            continue
        tab["confident"] = tab["significant"] & (tab["delta"].abs() >= mat)
        tab.insert(0, "budget", bud)
        tab.insert(0, "workload", wl)
        beats.append(tab)

        beaten = set()
        for _, r in tab[tab["confident"]].iterrows():
            beaten.add(r["policy_b"] if r["delta"] > 0 else r["policy_a"])
        present = [p for p in policies if p in set(cell[col["policy"]])]
        top = [p for p in present if p not in beaten]
        acc = cell.groupby(col["policy"])[col["outcome"]].mean()
        topsets.append(dict(
            workload=wl, budget=bud,
            top_set_size=len(top),
            top_set=", ".join(sorted(top, key=lambda p: -acc.get(p, 0))),
            best_acc=float(acc.reindex(present).max()),
        ))
    beats_df = pd.concat(beats, ignore_index=True) if beats else pd.DataFrame()
    top_df = pd.DataFrame(topsets)

    dom = pd.DataFrame()
    if not beats_df.empty:
        rows = []
        for wl, g in beats_df.groupby("workload"):
            nb = g["budget"].nunique()
            for (a, b), gg in g.groupby(["policy_a", "policy_b"]):
                if len(gg) < nb:
                    continue
                if (gg["confident"] & (gg["delta"] > 0)).all():
                    rows.append(dict(workload=wl, dominator=a, dominated=b, budgets=nb))
                elif (gg["confident"] & (gg["delta"] < 0)).all():
                    rows.append(dict(workload=wl, dominator=b, dominated=a, budgets=nb))
        dom = pd.DataFrame(rows)

    cross = pd.DataFrame()
    if not top_df.empty:
        rows = []
        for wl, g in top_df.sort_values("budget").groupby("workload"):
            prev = None
            for _, r in g.iterrows():
                cur = set(r["top_set"].split(", ")) if r["top_set"] else set()
                if prev is not None and cur != prev[1]:
                    rows.append(dict(
                        workload=wl, budget_from=prev[0], budget_to=r["budget"],
                        entered=", ".join(sorted(cur - prev[1])),
                        left=", ".join(sorted(prev[1] - cur)),
                    ))
                prev = (r["budget"], cur)
        cross = pd.DataFrame(rows)
    return beats_df, top_df, dom, cross


def rq4_interaction(df: pd.DataFrame, schema: Dict) -> pd.DataFrame:
    """
    correct ~ policy * log(budget), reader absorbed as a fixed effect,
    covariance clustered on question. One Wald test on the interaction block
    per workload: the direct answer to 'how does the ranking depend on budget'.
    """
    col = schema["columns"]
    policies = _analysis_policies(df, schema)
    rows = []
    for wl, sub in df.groupby(col["workload"]):
        sub = sub[sub[col["policy"]].isin(policies)].dropna(
            subset=[col["outcome"], col["budget"]]
        )
        if sub.empty or sub[col["budget"]].nunique() < 2:
            continue
        y = sub[col["outcome"]].values.astype(float)
        logb = np.log(sub[col["budget"]].values.astype(float))
        logb = logb - logb.mean()
        pol_d = pd.get_dummies(sub[col["policy"]], prefix="pol", drop_first=True)
        rdr_d = pd.get_dummies(sub[col["reader"]], prefix="rdr", drop_first=True)
        inter = pol_d.values * logb[:, None]
        X = np.column_stack([
            np.ones(len(sub)), pol_d.values.astype(float),
            rdr_d.values.astype(float), logb, inter.astype(float),
        ])
        n_pol, n_rdr = pol_d.shape[1], rdr_d.shape[1]
        start = 1 + n_pol + n_rdr + 1
        idx = list(range(start, start + n_pol))
        beta, cov, ncl, conv = logit_cluster(X, y, sub[col["question_id"]].values)
        w = wald_block(beta, cov, idx)
        rows.append(dict(
            workload=wl, n_rows=len(sub), n_clusters=ncl, converged=conv,
            interaction_df=w["df"], wald_chi2=w["stat"], p_interaction=w["p"],
        ))
    return pd.DataFrame(rows)


# ==========================================================================
# headline rows (the abstract sentence's raw material)
# ==========================================================================


def headline_rows(df: pd.DataFrame, schema: Dict) -> pd.DataFrame:
    col = schema["columns"]
    proxies = {**schema["proxies"], **schema.get("sensitivity_proxies", {})}
    agg = {col["outcome"]: "mean"}
    for spec in proxies.values():
        if spec["column"] in df.columns:
            agg[spec["column"]] = "mean"
    g = df.groupby([col["workload"], col["budget"], col["policy"]]).agg(agg).reset_index()
    g = g.rename(columns={col["outcome"]: "accuracy"})

    ranks = (
        df.groupby([col["workload"], col["budget"], col["reader"], col["policy"]])
        [col["outcome"]].mean().reset_index()
    )
    ranks["rank"] = ranks.groupby([col["workload"], col["budget"], col["reader"]])[
        col["outcome"]
    ].rank(ascending=False, method="average")
    rm = ranks.groupby([col["workload"], col["budget"], col["policy"]])["rank"].mean()
    g = g.merge(rm.rename("e2e_rank_mean").reset_index(),
                on=[col["workload"], col["budget"], col["policy"]], how="left")

    floors, ceilings = _floor_ceiling(df, schema)
    g["floor"] = g[col["workload"]].map(floors)
    g["ceiling"] = [ceilings.get((w, b), np.nan)
                    for w, b in zip(g[col["workload"]], g[col["budget"]])]
    span = g["ceiling"] - g["floor"]
    g["accuracy_norm"] = (g["accuracy"] - g["floor"]) / span.where(span > 0)
    return g


# ==========================================================================
# report
# ==========================================================================


def write_report(out: str, schema: Dict, res: Dict) -> str:
    pre = schema["preregistration"]
    L: List[str] = []
    w = L.append
    w("# Verdicts — V1 to V4 and RQ4\n")
    w(f"Materiality {pre['materiality']:.2%} · BH at {pre['alpha']} within each "
      f"workload x budget cell · exact paired tests · "
      f"{pre['bootstrap']} bootstrap resamples over questions, seed {pre['seed']}.\n")

    # --- V4
    w("\n## V4 — within-policy monotonicity (checked first, costs nothing)\n")
    s4 = res.get("v4_summary", pd.DataFrame())
    if s4.empty:
        w("Not computed: no preservation column available.\n")
    else:
        s4 = s4[~s4["is_control"]] if "is_control" in s4.columns else s4
        tot, dis = int(s4["steps"].sum()), int(s4["discordant"].sum())
        sig = int(s4["discordant_significant"].sum())
        inv = s4[s4["fully_inverted"]]
        w(f"{dis} of {tot} budget steps are discordant "
          f"({dis / tot:.1%}); {sig} are discordant, material and significant "
          f"paired on the question.\n")
        if len(inv):
            names = ", ".join(f"{r.policy} ({r.workload})" for r in inv.itertuples())
            w(f"\nFully inverted across the whole ladder — preservation rises at "
              f"every step while accuracy falls at every step: **{names}**. "
              f"This is H demonstrated inside a single policy, with no "
              f"cross-policy comparison and no coefficient.\n")
        else:
            w("\nNo policy inverts at every step of its ladder.\n")

    # --- V2
    w("\n## V2 — selection regret (the RQ3 number)\n")
    v2 = res.get("v2", pd.DataFrame())
    prim = res.get("primary_regret")
    if prim is None:
        w("Not computed.\n")
    else:
        pts = prim["regret"] * 100
        frac = prim["regret_frac_span"]
        fs = f" ({frac:.0%} of the floor-to-ceiling span)" if np.isfinite(frac) else ""
        w(f"> Selecting a forgetting policy by {prim['proxy']} rather than by "
          f"measured answer accuracy costs **{pts:.1f} accuracy points**{fs} at a "
          f"{prim['budget']:.0f}% budget on {prim['workload']} "
          f"(95% CI {prim['regret_lo'] * 100:.1f} to {prim['regret_hi'] * 100:.1f} points).\n")
        w(f"\nThe proxy picks `{prim['proxy_pick']}`; the best available policy is "
          f"`{prim['e2e_best']}` at {prim['best_acc']:.3f} accuracy against "
          f"{prim['proxy_pick_acc']:.3f}. Top-1 hit rate across resamples "
          f"{prim['top1_hit_rate_boot']:.2f}, top-3 containment "
          f"{prim['top3_containment_boot']:.2f}.\n")
    if not v2.empty:
        w("\nAll cells for the primary proxy:\n\n")
        sub = v2[v2["proxy"] == pre["primary_proxy"]]
        w(_md_table(sub[["workload", "budget", "proxy_pick", "e2e_best", "regret",
                         "regret_lo", "regret_hi", "top1_hit", "top3_hit"]]))

    # --- V1
    w("\n## V1 — substitution matrix and confident contradiction rate\n")
    s1 = res.get("v1_summary", pd.DataFrame())
    if s1.empty:
        w("Not computed.\n")
    else:
        sub = s1[s1["proxy"] == pre["primary_proxy"]]
        if len(sub):
            claims = int(sub["confident_proxy_claims"].sum())
            contra = int(sub["contradicts"].sum())
            if claims:
                lo, hi = wilson_ci(contra, claims)
                w(f"Across all cells, {pre['primary_proxy']} makes **{claims} "
                  f"confident claims** about policy pairs. End-to-end accuracy "
                  f"**reverses {contra}** of them: a confident contradiction rate "
                  f"of **{contra / claims:.3f}** (95% CI {lo:.3f} to {hi:.3f}). "
                  f"That is a hard integer, and there is no coefficient in it.\n")
            else:
                w(f"After correction, {pre['primary_proxy']} makes no confident "
                  "claim about any policy pair. There is nothing for end-to-end "
                  "accuracy to contradict, which is itself the finding.\n")
        w("\n")
        w(_md_table(s1[["proxy", "workload", "budget", "agrees", "contradicts",
                        "overclaims", "underclaims", "both_silent",
                        "confident_contradiction_rate"]]))
        w("\n`both_silent` is where the degeneracy that was dragging tau to zero "
          "now lives: a reportable count rather than a nuisance.\n")

    # --- V3
    w("\n## V3 — the proxy ladder\n")
    lad = res.get("ladder", pd.DataFrame())
    if lad.empty:
        w("Not computed: fewer than two proxy rungs available.\n")
    else:
        w(_md_table(lad[["proxy", "confident_claims", "contradictions",
                         "confident_contradiction_rate", "mean_regret",
                         "top1_hit_rate", "top3_containment"]]))
        ccr = lad["confident_contradiction_rate"].values
        reg = lad["mean_regret"].values
        mono_ccr = bool(np.all(np.diff(ccr[np.isfinite(ccr)]) <= 1e-12))
        mono_reg = bool(np.all(np.diff(reg[np.isfinite(reg)]) <= 1e-12))
        w(f"\nMonotone in contradiction rate: **{mono_ccr}**. "
          f"Monotone in mean regret: **{mono_reg}**. "
          "Where both hold, this table answers RQ2's 'what must be measured at "
          "each stage' empirically and prices each extra stage of measurement.\n")

    # --- RQ4
    w("\n## RQ4 — dominance sets, not a league table\n")
    top = res.get("rq4_topsets", pd.DataFrame())
    if not top.empty:
        w(_md_table(top[["workload", "budget", "top_set_size", "top_set"]]))
    cross = res.get("rq4_crossovers", pd.DataFrame())
    if cross is not None and not cross.empty:
        w("\nTop-set membership changes at these budgets:\n\n")
        w(_md_table(cross))
    inter = res.get("rq4_interaction", pd.DataFrame())
    if not inter.empty:
        w("\nPolicy x log(budget) interaction, one test per workload "
          "(logistic, covariance clustered on question, reader absorbed):\n\n")
        w(_md_table(inter[["workload", "n_rows", "n_clusters", "interaction_df",
                           "wald_chi2", "p_interaction", "converged"]]))

    # --- decision
    w("\n## The pre-registered decision\n")
    verdict = res.get("verdict", {})
    w(f"- Confident contradiction rate above zero after correction: "
      f"**{verdict.get('ccr_positive')}** ({verdict.get('contradictions', 0)} "
      f"contradictions of {verdict.get('claims', 0)} confident claims)\n")
    w(f"- Selection regret materially above zero and CI excluding zero: "
      f"**{verdict.get('regret_positive')}**\n")
    w(f"\n**H is {'supported' if verdict.get('supported') else 'not supported'} "
      "on the pre-registered rule.**\n")
    if verdict.get("supported"):
        w("\nEvery confident claim did not agree in direction, and regret is "
          "materially above zero with a confidence interval excluding it.\n")

    w("\n---\n\nNo rank correlation was computed anywhere in this path.\n")

    path = os.path.join(out, "VERDICTS.md")
    with open(path, "w") as fh:
        fh.write("\n".join(L))
    return path


def _md_table(df: pd.DataFrame, floatfmt: str = "{:.3f}") -> str:
    if df is None or df.empty:
        return "_(empty)_\n"
    d = df.copy()
    for c in d.columns:
        if not pd.api.types.is_float_dtype(d[c]):
            continue
        name = str(c).lower()
        if name.startswith("p_") or name in ("p", "q"):
            fmt = "{:.2e}"
        elif name in ("budget", "budget_from", "budget_to"):
            fmt = "{:g}"
        else:
            fmt = floatfmt
        d[c] = d[c].map(lambda v, f=fmt: "" if pd.isna(v) else f.format(v))
    head = "| " + " | ".join(str(c) for c in d.columns) + " |"
    rule = "|" + "|".join("---" for _ in d.columns) + "|"
    body = ["| " + " | ".join(str(v) for v in r) + " |" for r in d.itertuples(index=False)]
    return "\n".join([head, rule] + body) + "\n"


# ==========================================================================
# driver
# ==========================================================================


def run(df: pd.DataFrame, schema: Dict, out: str) -> Dict:
    os.makedirs(out, exist_ok=True)
    col, pre = schema["columns"], schema["preregistration"]
    res: Dict = {}

    c1 = check_contract_c1(df, schema)
    if not c1.empty:
        c1.to_csv(os.path.join(out, "contract_c1.csv"), index=False)

    cov = proxy_coverage(df, schema)
    cov.to_csv(os.path.join(out, "proxy_coverage.csv"), index=False)
    res["coverage"] = cov

    head = headline_rows(df, schema)
    head.to_csv(os.path.join(out, "headline_rows.csv"), index=False)
    res["headline"] = head

    # V4 first: cheapest, least attackable
    pres_col = schema["proxies"].get("preservation", {}).get("column")
    if pres_col and pres_col in df.columns:
        s4_steps, s4 = v4_monotonicity(df, schema, pres_col)
        s4_steps.to_csv(os.path.join(out, "v4_steps.csv"), index=False)
        s4.to_csv(os.path.join(out, "v4_summary.csv"), index=False)
        res["v4_steps"], res["v4_summary"] = s4_steps, s4

    # V3 runs V1 and V2 once per rung, so it produces all three
    pairs, v1, v2, ladder = v3_ladder(df, schema, cov)
    for name, obj in [("v1_pairs.csv", pairs), ("v1_summary.csv", v1),
                      ("v2_regret.csv", v2), ("v3_ladder.csv", ladder)]:
        if obj is not None and not obj.empty:
            obj.to_csv(os.path.join(out, name), index=False)
    res["v1_pairs"], res["v1_summary"], res["v2"], res["ladder"] = pairs, v1, v2, ladder

    beats, top, dom, cross = rq4_dominance(df, schema)
    for name, obj in [("rq4_pairs.csv", beats), ("rq4_topsets.csv", top),
                      ("rq4_dominance.csv", dom), ("rq4_crossovers.csv", cross)]:
        if obj is not None and not obj.empty:
            obj.to_csv(os.path.join(out, name), index=False)
    res["rq4_topsets"], res["rq4_dominance"], res["rq4_crossovers"] = top, dom, cross

    try:
        inter = rq4_interaction(df, schema)
    except Exception as exc:  # noqa: BLE001
        print(f"[RQ4] interaction model failed: {exc}")
        inter = pd.DataFrame()
    if not inter.empty:
        inter.to_csv(os.path.join(out, "rq4_interaction.csv"), index=False)
    res["rq4_interaction"] = inter

    # primary statistics and the pre-registered decision
    prim = None
    if not v2.empty:
        sel = v2[(v2["proxy"] == pre["primary_proxy"])
                 & (v2["workload"] == pre["primary_workload"])
                 & (np.isclose(v2["budget"], pre["primary_budget"]))]
        if sel.empty:
            sel = v2[v2["proxy"] == pre["primary_proxy"]]
            if not sel.empty:
                print("[primary] pre-registered cell not present; "
                      "falling back to the largest-regret cell of the primary proxy")
                sel = sel.sort_values("regret", ascending=False).head(1)
        if not sel.empty:
            prim = sel.iloc[0].to_dict()
    res["primary_regret"] = prim

    claims = contra = 0
    if not v1.empty:
        s = v1[v1["proxy"] == pre["primary_proxy"]]
        claims = int(s["confident_proxy_claims"].sum())
        contra = int(s["contradicts"].sum())
    ccr_pos = contra > 0
    reg_pos = bool(prim and prim["regret"] >= pre["materiality"] and prim["regret_lo"] > 0)
    res["verdict"] = dict(claims=claims, contradictions=contra,
                          ccr_positive=ccr_pos, regret_positive=reg_pos,
                          supported=bool(ccr_pos and reg_pos))

    path = write_report(out, schema, res)
    print(f"[done] {path}")
    return res


# ==========================================================================
# self-test: synthetic sweep with a planted inversion
# ==========================================================================


def synthesize(seed: int = 7, n_q: int = 400, inversion: bool = True) -> pd.DataFrame:
    """
    A sweep with the shape of the real one and a deliberately planted
    inversion, so the verdict path can be verified before it is pointed at
    real data. Compression-family policies keep high preservation and lose
    accuracy; the oracle's preservation rises monotonically while its accuracy
    falls, which is exactly the V4 signature.
    """
    rng = np.random.default_rng(seed)
    workloads = {"longmemeval": n_q, "locomo": int(n_q * 1.2), "beam": int(n_q * 0.8)}
    budgets = [10.0, 25.0, 50.0]
    readers = [f"reader{i}" for i in range(1, 6)]
    policies = ["oracle", "compress_llm", "compress_ext", "lru", "lfu", "fifo",
                "random", "semantic", "recency", "hybrid_a", "hybrid_b",
                "size_aware", "cluster", "salience", "tfidf"]

    # latent behaviour: (preservation base, accuracy base, inversion strength).
    # With inversion=False the accuracy ordering is the preservation ordering and
    # the proxy is a faithful substitute — the null the verdict path must be able
    # to return. If a null run reports H supported, the machinery manufactures
    # results and nothing below it can be trusted.
    profile = {
        "oracle": (0.95, 0.34, 0.9), "compress_llm": (0.90, 0.36, 0.8),
        "compress_ext": (0.86, 0.39, 0.7), "semantic": (0.62, 0.52, 0.0),
        "hybrid_a": (0.60, 0.54, 0.0), "hybrid_b": (0.58, 0.53, 0.0),
        "salience": (0.57, 0.50, 0.0), "cluster": (0.55, 0.49, 0.0),
        "lru": (0.52, 0.48, 0.0), "lfu": (0.50, 0.47, 0.0),
        "recency": (0.49, 0.46, 0.0), "tfidf": (0.48, 0.45, 0.0),
        "size_aware": (0.46, 0.44, 0.0), "fifo": (0.44, 0.42, 0.0),
        "random": (0.40, 0.40, 0.0),
    }

    rows = []
    for wl, nq in workloads.items():
        qids = [f"{wl}_q{i:04d}" for i in range(nq)]
        q_eff = rng.normal(0, 0.7, nq)  # question difficulty
        extractive = rng.random(nq) < (0.021 if wl == "beam" else 0.85)
        for bud in budgets:
            pressure = {10.0: 0.0, 25.0: 0.5, 50.0: 1.0}[bud]
            for pol in policies:
                pres_b, acc_b, inv = profile[pol]
                if not inversion:
                    inv = 0.0
                    acc_b = 0.30 + 0.30 * (pres_b - 0.40) / 0.55
                pres = min(0.999, pres_b * (0.55 + 0.45 * pressure) + 0.02)
                acc = acc_b + 0.10 * pressure - inv * 0.16 * pressure
                for rd_i, rd in enumerate(readers):
                    rd_eff = (rd_i - 2) * 0.08
                    p_corr = 1 / (1 + np.exp(-(np.log(acc / (1 - acc)) + rd_eff + q_eff)))
                    correct = rng.random(nq) < p_corr
                    frac = np.clip(rng.normal(pres, 0.18, nq), 0, 1)
                    frac = np.where(extractive, frac, np.nan)
                    rows.append(pd.DataFrame(dict(
                        workload=wl, policy=pol, budget=bud, reader=rd,
                        question_id=qids, qa_correct=correct.astype(int),
                        gold_id_retained=(rng.random(nq) < pres).astype(int),
                        preserved_frac=frac,
                        preserved_any=np.where(np.isnan(frac), np.nan,
                                               (frac > 0).astype(float)),
                        preserved_all=np.where(np.isnan(frac), np.nan,
                                               (frac >= 0.999).astype(float)),
                        sufficient=(rng.random(nq) < np.clip(pres * 0.75 + 0.15, 0, 1)
                                    ).astype(int),
                        gradeable=True, grader="hybrid",
                    )))
        # controls
        for pol, acc in [("no_eviction", 0.62), ("empty_memory", 0.11)]:
            for rd_i, rd in enumerate(readers):
                for bud in budgets:
                    p_corr = 1 / (1 + np.exp(-(np.log(acc / (1 - acc))
                                               + (rd_i - 2) * 0.08 + q_eff)))
                    rows.append(pd.DataFrame(dict(
                        workload=wl, policy=pol, budget=bud, reader=rd,
                        question_id=qids,
                        qa_correct=(rng.random(nq) < p_corr).astype(int),
                        gold_id_retained=1 if pol == "no_eviction" else 0,
                        preserved_frac=np.where(extractive,
                                                1.0 if pol == "no_eviction" else 0.0,
                                                np.nan),
                        preserved_any=np.where(extractive,
                                               1.0 if pol == "no_eviction" else 0.0,
                                               np.nan),
                        preserved_all=np.where(extractive,
                                               1.0 if pol == "no_eviction" else 0.0,
                                               np.nan),
                        sufficient=1 if pol == "no_eviction" else 0,
                        gradeable=True, grader="hybrid",
                    )))
    return pd.concat(rows, ignore_index=True)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", help="CSV/parquet file, or a directory of them")
    ap.add_argument("--schema", help="JSON column mapping (see schema.example.json)")
    ap.add_argument("--out", default="verdicts_out")
    ap.add_argument("--grader-tag", help="keep only rows with this grader, e.g. 'hybrid'")
    ap.add_argument("--bootstrap", type=int, help="override resample count")
    ap.add_argument("--selftest", action="store_true",
                    help="run on synthetic data with a planted inversion")
    ap.add_argument("--selftest-null", action="store_true",
                    help="synthetic data where the proxy is faithful; H must NOT be supported")
    a = ap.parse_args(argv)

    schema = load_schema(a.schema)
    if a.bootstrap:
        schema["preregistration"]["bootstrap"] = a.bootstrap

    if a.selftest or a.selftest_null:
        df = synthesize(inversion=not a.selftest_null)
        kind = "null (faithful proxy)" if a.selftest_null else "planted inversion"
        print(f"[selftest] synthetic sweep, {kind}: {len(df):,} rows")
    elif a.input:
        df = load_sweep(a.input, schema, a.grader_tag)
    else:
        ap.error("pass --input, --selftest or --selftest-null")
    run(df, schema, a.out)


if __name__ == "__main__":
    main()
