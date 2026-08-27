#!/usr/bin/env python3
"""rq_tables.py — the canonical merge and the prespecified RQ3/RQ4 tests.

Inputs (all already on disk):
  /scratch/.../judged/judged_<workload>_<reader>.csv   answer judge over the sweep
  /scratch/.../alive/store_<workload>.csv              store-stage sufficiency
  configs/locomo_ungradeable_ids.txt                   frozen adversarial exclusion

Canonical verdict = GUARDED HYBRID:
  deterministic on exact/abstain/contains, judge on miss/content_words/reverse,
  then two deterministic guards enforcing pre-stated rules the judge
  under-applied (RULE.txt and the judge prompt, committed before results):
    G1 contradiction: reference says the record is contradictory and the
       accepted answer does not flag it -> incorrect. 
    G2 abstention: accepted answer is an abstention against a non-empty
       reference -> incorrect.
  Guard firing counts are reported, and a sample of overturned rows is written
  for audit (guards are instruments too).

Prespecified parameters (fixed before this script ever saw the data):
  MATERIALITY = 0.03   ALPHA = 0.05 (BH within cell)   N_BOOT = 10000

Outputs in report/rq/:
  rows_summary.csv     per workload x budget x policy: store, accuracy, n
  controls.csv         per workload x reader x arm (empty/parametric/gold)
  monotone.csv         vdct1: sign of d(store) vs sign d(accuracy) per step
  wrongpick.csv        vdct2: store-pick vs true-pick cost, bootstrap CI
  pairs.csv            vdct3: per-pair McNemar both sides, BH, 5-state class
  pair_counts.csv      vdct3 summary: state counts per cell
  topsets.csv          vdct4: policies not significantly beaten, per cell
  guard_audit.csv      sample of guard-overturned rows for the 10-row read
  manifest.json        parameters, commit, input files

Run (login node, CPU):
  singularity exec --bind /scratch/$PROJ "$CLIENT_SIF" python3 scripts/rq_tables.py
"""
from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import math
import re
import subprocess
import sys
import os
from itertools import combinations
from pathlib import Path

import numpy as np
import pandas as pd

# ------------------------------------------------------------ prespecified --
MATERIALITY = 0.03
ALPHA = 0.05
N_BOOT = 10_000
SEED = 42
ROUTED = ("miss", "content_words", "reverse")
DEPLOYABLE_EXCLUDES = {"oracle"}          # clairvoyant, not a candidate

JUDGED = os.environ.get("JUDGED_DIR",
                        "/scratch/project_462001123/artifacts/judged")
                        
ALIVE = os.environ.get("ALIVE_DIR",
    "/scratch/project_462001123/artifacts/alive")
REPO = Path(__file__).resolve().parents[1]
EXCLUDE_FILE = REPO / "configs" / "locomo_ungradeable_ids.txt"

# ABSOLUTE, repo-anchored. The old value was the RELATIVE Path("report/rq"), so
# the output landed wherever the script was invoked from ($HOME, in practice)
# and a rerun pointed at different INPUTS (the ALIVE_DIR override) silently
# overwrote the previous bundle in place. Override with --out.
OUT_DEFAULT = REPO / "report" / "rq"


CONTRA_REF = "contradictory information"
FLAGS = re.compile(r"contradict|inconsist|conflict|clarif|discrepan", re.I)
ABSTAIN = re.compile(
    r"\b(?:idk|i don'?t know|do(?:es)? not contain|no information|"
    r"not (?:provided|mentioned|specified|available)|cannot determine|"
    r"unable to (?:determine|find)|"
    r"don'?t have (?:enough |that )?information)\b", re.I)

BUDGET_MAP = {"budget_010pct": "010", "budget_025pct": "025",
              "budget_050pct": "050", "budget_075pct": "075",
              "budget_100pct": "100",
              "0.1": "010", "0.25": "025", "0.5": "050",
              "0.75": "075", "1.0": "100", "1": "100"}

PARAMETRIC_SHAS: set[str] = set()

NO_RENDER_SHA = ""

def norm_budget(v) -> str:
    s = str(v).strip()
    if s in BUDGET_MAP:
        return BUDGET_MAP[s]
    try:
        return {0.1: "010", 0.25: "025", 0.5: "050",
                0.75: "075", 1.0: "100"}[round(float(s), 2)]
    except (ValueError, KeyError):
        return s


def to_bool(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower().map(
        {"true": True, "false": False}).astype("boolean")


def mcnemar_p(b: int, c: int) -> float:
    """Exact two-sided binomial on the discordant counts."""
    n = b + c
    if n == 0:
        return 1.0
    k = min(b, c)
    p = 2 * sum(math.comb(n, i) for i in range(k + 1)) / 2 ** n
    return min(p, 1.0)


def bh_reject(pvals: np.ndarray, alpha: float = ALPHA) -> np.ndarray:
    """Benjamini-Hochberg within one family. Returns boolean rejections."""
    m = len(pvals)
    if m == 0:
        return np.zeros(0, dtype=bool)
    order = np.argsort(pvals)
    thresh = alpha * (np.arange(1, m + 1) / m)
    passed = pvals[order] <= thresh
    k = np.max(np.nonzero(passed)[0]) + 1 if passed.any() else 0
    out = np.zeros(m, dtype=bool)
    out[order[:k]] = True
    return out


# ------------------------------------------------------------------ loading --
def note_render_sha(seen: dict, path: str, d: pd.DataFrame) -> None:
    """Record the render contract(s) one input file carries.

    Absence of the column means the rows predate dated rendering. Nothing in
    the PATH distinguishes one contract from another, so it is read out of the
    DATA. Parametric rows are recorded separately rather than gated: they have
    no evidence to date.
    """
    if "render_sha" not in d.columns:
        seen[path] = {NO_RENDER_SHA}
        return
    ev = d[d["arm"] != "parametric"] if "arm" in d.columns else d
    pa = d[d["arm"] == "parametric"] if "arm" in d.columns else d.iloc[0:0]
    PARAMETRIC_SHAS.update(
        str(v).strip() for v in pa["render_sha"].fillna("").unique() if str(v).strip())
    vals = {(str(v).strip() or NO_RENDER_SHA)
            for v in ev["render_sha"].fillna("").unique()}
    seen[path] = vals or {NO_RENDER_SHA}


def assert_single_render_sha(seen: dict, allow_mixed: bool) -> str:
    """One evidence base per table. Mixing them silently pools two experiments."""
    groups = set().union(*seen.values()) if seen else {""}
    if len(groups) > 1:
        listing = "\n".join(
            f"    {', '.join(x or '(no render_sha column)' for x in sorted(v)):<40}"
            f" {p}"
            for p, v in sorted(seen.items()))
        shown = sorted(g or "(no render_sha column)" for g in groups)
        msg = (f"MIXED RENDER CONTRACTS: {shown}\n{listing}\n"
               f"  Rows rendered under different contracts describe different "
               f"experiments and must not be merged into one table.")
        if not allow_mixed:
            raise SystemExit(msg + "\n  (--allow-mixed-render overrides, for "
                                   "debugging only -- never for emission.)")
        print("WARNING: " + msg, file=sys.stderr)
        return "MIXED:" + "+".join(sorted(groups))
    return groups.pop()



def load_judged(audit: list, render_seen: dict | None = None,
                inputs: list | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows, controls = [], []
    rng = np.random.default_rng(SEED)
    for f in sorted(glob.glob(f"{JUDGED}/judged_*.csv")):
        w = f.split("judged_")[1].rsplit("_", 1)[0]
        d = pd.read_csv(f, low_memory=False)
        if render_seen is not None:
            note_render_sha(render_seen, f, d)
        if inputs is not None:
            inputs.append(f)
        d = d[d.ref_answer.fillna("").astype(str).str.strip() != ""].copy()
        det = to_bool(d.correct).fillna(False)
        jud = to_bool(d.judge_correct)
        routed = d.tier.isin(ROUTED)
        hyb = det.mask(routed & jud.notna(), jud).astype(bool)
        ref = d.ref_answer.astype(str)
        ans = d.reader_answer.astype(str)
        g1 = hyb & ref.str.contains(CONTRA_REF, case=False) & ~ans.str.contains(FLAGS)
        g2 = hyb & ans.str.contains(ABSTAIN)
        d["det"], d["hyb"] = det, (hyb & ~g1 & ~g2)
        d["g_contra"], d["g_abst"] = g1, g2
        d["workload"] = w

        # guard audit sample, with text, before text is dropped
        for gname, gmask in (("contra", g1), ("abstain", g2)):
            hit = d[gmask]
            if len(hit):
                take = hit.sample(min(5, len(hit)),
                                  random_state=int(rng.integers(1 << 30)))
                for _, r in take.iterrows():
                    audit.append({"workload": w, "guard": gname,
                                  "model": r.model, "query_id": r.query_id,
                                  "tier": r.tier,
                                  "question": str(r.question)[:300],
                                  "ref_answer": str(r.ref_answer)[:300],
                                  "reader_answer": str(r.reader_answer)[:400],
                                  "human_ok_to_overturn": ""})

        ctrl = d[d.arm != "retrieved"]
        for (arm, model), g in ctrl.groupby(["arm", "model"]):
            controls.append({"workload": w, "arm": arm, "reader": model,
                             "acc_det": g.det.mean(), "acc_hyb": g.hyb.mean(),
                             "n": len(g)})

        pol = d[d.arm == "retrieved"].copy()
        parts = pol.config.astype(str).str.split("/")
        pol["policy"] = parts.str[3]
        pol["budget"] = parts.str[5].map(norm_budget)
        rows.append(pol[["workload", "model", "policy", "budget", "query_id",
                         "tier", "det", "hyb", "g_contra", "g_abst"]])
    return pd.concat(rows, ignore_index=True), pd.DataFrame(controls)


def load_store(render_seen: dict | None = None,
               inputs: list | None = None) -> pd.DataFrame:
    frames = []
    for f in sorted(glob.glob(f"{ALIVE}/store_*.csv")):
        if f.endswith(".cache.csv"):
            continue
        w = f.split("store_")[1].replace(".csv", "")
        d = pd.read_csv(f)
        if render_seen is not None:
            note_render_sha(render_seen, f, d)
        if inputs is not None:
            inputs.append(f)
        d["workload"] = w
        d["budget"] = d.budget.map(norm_budget)
        frames.append(d[["workload", "policy", "budget", "query_id",
                         "answerable_store", "n_alive_ids"]])
    return pd.concat(frames, ignore_index=True)


def apply_exclusions(df: pd.DataFrame) -> pd.DataFrame:
    if EXCLUDE_FILE.is_file():
        bad = set(x.strip() for x in EXCLUDE_FILE.read_text().split() if x.strip())
        mask = df.workload.str.startswith("locomo") & df.query_id.isin(bad)
        df = df[~mask]
    else:
        print(f"WARNING: {EXCLUDE_FILE} missing, locomo adversarial NOT excluded",
              file=sys.stderr)
    return df


# ------------------------------------------------------------------ verdicts --
def vdct1_monotone(cell: pd.DataFrame) -> pd.DataFrame:
    out = []
    for (w, p), g in cell.groupby(["workload", "policy"]):
        g = g.sort_values("budget")
        for a, b in zip(g.itertuples(), list(g.itertuples())[1:]):
            ds, da = b.store - a.store, b.acc - a.acc
            out.append({"workload": w, "policy": p,
                        "step": f"{a.budget}->{b.budget}",
                        "d_store": round(ds, 4), "d_acc": round(da, 4),
                        "discordant": bool(ds * da < 0 and abs(ds) >= 0.01
                                           and abs(da) >= 0.01)})
    return pd.DataFrame(out)


def vdct2_wrongpick(qacc: pd.DataFrame, qstore: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(SEED)
    out = []
    m = qacc.merge(qstore, on=["workload", "budget", "policy", "query_id"])
    for (w, b), g in m.groupby(["workload", "budget"]):
        g = g[~g.policy.isin(DEPLOYABLE_EXCLUDES)]
        A = g.pivot_table(index="query_id", columns="policy", values="acc")
        S = g.pivot_table(index="query_id", columns="policy", values="store")
        common = A.index.intersection(S.index)
        A, S = A.loc[common].fillna(0), S.loc[common].fillna(0)
        pick_s = S.mean().idxmax()
        pick_a = A.mean().idxmax()
        cost = A.mean()[pick_a] - A.mean()[pick_s]
        boots = np.empty(N_BOOT)
        idx = np.arange(len(A))
        Av, Sv = A.to_numpy(), S.to_numpy()
        for i in range(N_BOOT):
            r = rng.integers(0, len(idx), len(idx))
            am, sm = Av[r].mean(axis=0), Sv[r].mean(axis=0)
            boots[i] = am[am.argmax()] - am[sm.argmax()]
        lo, hi = np.percentile(boots, [2.5, 97.5])
        out.append({"workload": w, "budget": b,
                    "store_pick": pick_s, "true_pick": pick_a,
                    "acc_store_pick": round(A.mean()[pick_s], 4),
                    "acc_true_pick": round(A.mean()[pick_a], 4),
                    "cost": round(cost, 4),
                    "ci_lo": round(lo, 4), "ci_hi": round(hi, 4),
                    "top1_hit": pick_s == pick_a, "n_questions": len(A)})
    return pd.DataFrame(out)


def vdct3_pairs(rows: pd.DataFrame, store: pd.DataFrame):
    pair_rows = []
    for (w, b), _ in rows.groupby(["workload", "budget"]):
        pols = sorted(set(rows[(rows.workload == w) & (rows.budget == b)].policy)
                      - DEPLOYABLE_EXCLUDES)
        sw = store[(store.workload == w) & (store.budget == b)]
        Sp = sw.pivot_table(index="query_id", columns="policy",
                            values="answerable_store")
        aw = rows[(rows.workload == w) & (rows.budget == b)]
        Ap = aw.pivot_table(index=["query_id", "model"], columns="policy",
                            values="hyb")
        for x, y in combinations(pols, 2):
            rec = {"workload": w, "budget": b, "A": x, "B": y}
            for side, P in (("store", Sp), ("acc", Ap)):
                if x not in P.columns or y not in P.columns:
                    rec.update({f"{side}_diff": np.nan, f"{side}_p": 1.0})
                    continue
                pp = P[[x, y]].dropna().astype(bool)
                bcnt = int((pp[x] & ~pp[y]).sum())
                ccnt = int((~pp[x] & pp[y]).sum())
                rec[f"{side}_diff"] = round(pp[x].mean() - pp[y].mean(), 4)
                rec[f"{side}_p"] = mcnemar_p(bcnt, ccnt)
                rec[f"{side}_n"] = len(pp)
            pair_rows.append(rec)
    pairs = pd.DataFrame(pair_rows)

    # BH within cell, each side its own family
    for side in ("store", "acc"):
        pairs[f"{side}_sig"] = False
        for _, idx in pairs.groupby(["workload", "budget"]).groups.items():
            pv = pairs.loc[idx, f"{side}_p"].to_numpy()
            pairs.loc[idx, f"{side}_sig"] = bh_reject(pv)

    def conf(side, r):
        return r[f"{side}_sig"] and abs(r[f"{side}_diff"]) >= MATERIALITY

    def classify(r):
        cs, ca = conf("store", r), conf("acc", r)
        if cs and ca:
            return ("agrees" if np.sign(r.store_diff) == np.sign(r.acc_diff)
                    else "CONTRADICTS")
        if cs:
            return "store_overclaims"
        if ca:
            return "store_underclaims"
        return "both_silent"

    pairs["state"] = pairs.apply(classify, axis=1)
    counts = (pairs.groupby(["workload", "budget", "state"]).size()
              .unstack(fill_value=0).reset_index())
    return pairs, counts


def vdct4_topsets(pairs: pd.DataFrame) -> pd.DataFrame:
    out = []
    for (w, b), g in pairs.groupby(["workload", "budget"]):
        pols = sorted(set(g.A) | set(g.B))
        beaten = set()
        for _, r in g.iterrows():
            if r.acc_sig and abs(r.acc_diff) >= MATERIALITY:
                beaten.add(r.B if r.acc_diff > 0 else r.A)
        top = [p for p in pols if p not in beaten]
        out.append({"workload": w, "budget": b,
                    "top_set": ", ".join(top), "size": len(top)})
    return pd.DataFrame(out)


# ---------------------------------------------------------------- provenance --
def _git(*args: str) -> str:
    """Run git in the REPO and return stdout, or "" if it genuinely failed.

    The old call had no returncode check, so a git failure inside the container
    produced an EMPTY commit that was written as if it were real -- and the
    FileNotFoundError path wrote the literal "unknown" that both existing
    bundle manifests still carry.
    """
    try:
        r = subprocess.run(["git", "-C", str(REPO), *args],
                           capture_output=True, text=True)
    except (OSError, ValueError):
        return ""
    return r.stdout.strip() if r.returncode == 0 else ""


def provenance(render: str, inputs: list[str], out: Path,
               allow_dirty: bool) -> dict:
    """What the numbers were computed from. Refuses to lie about a dirty tree."""
    commit = _git("rev-parse", "HEAD") or os.environ.get("GIT_COMMIT", "")
    describe = _git("describe", "--tags", "--always", "--dirty")
    dirty = bool(_git("status", "--porcelain"))
    if dirty and not allow_dirty:
        raise SystemExit(
            f"REFUSING to write {out}/manifest.json from a DIRTY tree: the "
            f"commit would not describe the code that produced these numbers. "
            f"Commit first, or set ALLOW_DIRTY=1 to write a manifest marked "
            f'"dirty": true (unusable for emission).')
    if not commit:
        print("WARNING: no git commit resolvable and $GIT_COMMIT unset — this "
              "bundle will not be traceable to code.", file=sys.stderr)
    return {
        "commit": commit or "unknown",
        "describe": describe or "unknown",
        "date": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "dirty": dirty,
        "render_sha": render,
        "render_sha_parametric": sorted(PARAMETRIC_SHAS) or None,
        "materiality": MATERIALITY, "alpha": ALPHA, "n_boot": N_BOOT,
        "seed": SEED, "routed_tiers": list(ROUTED),
        "guards": ["contradiction", "abstention"],
        "deployable_excludes": sorted(DEPLOYABLE_EXCLUDES),
        # Resolved, not the defaults: ALIVE_DIR is what told the two existing
        # bundles apart, and neither manifest recorded it.
        "judged_dir": JUDGED, "alive_dir": ALIVE,
        "exclude_file": str(EXCLUDE_FILE), "out_dir": str(out),
        "inputs": sorted(inputs),
    }


# --------------------------------------------------------------------- main --
def main() -> int:
    ap = argparse.ArgumentParser(
        description="Canonical merge + the prespecified RQ3/RQ4 tests.")
    ap.add_argument("--out", type=Path, default=OUT_DEFAULT,
                    help=f"bundle directory (default {OUT_DEFAULT}). Absolute "
                         f"and repo-anchored: a relative default is what let a "
                         f"rerun overwrite a previous bundle in place.")
    ap.add_argument("--allow-mixed-render", action="store_true",
                    help="proceed even if inputs mix render versions. Debugging "
                         "only — never for numbers that get reported.")
    args = ap.parse_args()

    OUT = args.out
    OUT.mkdir(parents=True, exist_ok=True)
    audit: list = []
    render_seen: dict[str, set] = {}
    inputs: list[str] = []
    rows, controls = load_judged(audit, render_seen, inputs)
    store = load_store(render_seen, inputs)
    render = assert_single_render_sha(render_seen, args.allow_mixed_render)
    print(f"render_sha: {render or '(pre-column rows)'} "
          f"({len(render_seen)} input files)")
    if EXCLUDE_FILE.is_file():
        inputs.append(str(EXCLUDE_FILE))
    rows, store = apply_exclusions(rows), apply_exclusions(store)
    # store side restricted to answer-side gradeable questions, so both
    # instruments share one denominator per workload
    gradeable = rows[["workload", "query_id"]].drop_duplicates()
    n0 = len(store)
    store = store.merge(gradeable, on=["workload", "query_id"])
    print(f"store rows after gradeable join: {len(store):,} (was {n0:,})")

    print(f"answer rows: {len(rows):,}   store rows: {len(store):,}")
    print("guard firings:",
          {"contra": int(rows.g_contra.sum()), "abstain": int(rows.g_abst.sum())})

    # per-question scores
    qacc = (rows.groupby(["workload", "budget", "policy", "query_id"])
            .hyb.mean().rename("acc").reset_index())
    qstore = (store.groupby(["workload", "budget", "policy", "query_id"])
              .answerable_store.max().astype(float).rename("store").reset_index())

    cell = (qacc.merge(qstore, on=["workload", "budget", "policy", "query_id"])
            .groupby(["workload", "budget", "policy"])
            .agg(acc=("acc", "mean"), store=("store", "mean"),
                 n=("query_id", "nunique")).round(4).reset_index())
    cell.to_csv(OUT / "rows_summary.csv", index=False)
    controls.round(4).to_csv(OUT / "controls.csv", index=False)

    mono = vdct1_monotone(cell)
    mono.to_csv(OUT / "monotone.csv", index=False)
    wp = vdct2_wrongpick(qacc, qstore)
    wp.to_csv(OUT / "wrongpick.csv", index=False)
    pairs, counts = vdct3_pairs(rows, store)
    pairs.round(4).to_csv(OUT / "pairs.csv", index=False)
    counts.to_csv(OUT / "pair_counts.csv", index=False)
    tops = vdct4_topsets(pairs)
    tops.to_csv(OUT / "topsets.csv", index=False)
    pd.DataFrame(audit).to_csv(OUT / "guard_audit.csv", index=False)

    (OUT / "manifest.json").write_text(json.dumps(
        provenance(render, inputs, OUT,
                   os.environ.get("ALLOW_DIRTY") == "1"), indent=2) + "\n")

    # ------------------------------------------------------------ console ----
    print("\n=== vdct2 wrong-pick cost (store-guided selection) ===")
    print(wp.to_string(index=False))
    print("\n=== vdct3 pair states per cell ===")
    print(counts.to_string(index=False))
    print("\n=== vdct1 oracle monotone steps ===")
    print(mono[mono.policy == "oracle"].to_string(index=False))
    print("\n=== vdct4 top sets ===")
    print(tops.to_string(index=False))
    print(f"\nwrote everything to {OUT}/  — guard_audit.csv is the 10-row read")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
