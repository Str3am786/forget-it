#!/usr/bin/env python3
"""Post hoc decomposition of answer accuracy into forgetting, retrieval and reading.

POST HOC and DESCRIPTIVE. Not in the pre-registered plan (commit 292a322); no
new significance tests. Intervals are cluster bootstrap percentile intervals
over examples (one example = one memory store per policy, shared by its
questions and by all five readers).

Per (config, question, reader), all under the guarded verdict of rq_tables.py:
  gold       gold items, original text            judged5_<r>, arm gold
  resident   gold items as the policy left them,  judged_resident_<r>
             no retriever
  retrieved  what the retriever delivered          judged5_<r>, arm retrieved

  loss_forgetting = gold - resident
  loss_retrieval  = resident - retrieved, split by what the retriever did
                    with the surviving gold: hit (some surviving gold item in
                    the retrieved list) or miss (none). Questions whose gold
                    was all evicted ("gone") carry no retrieval loss term of
                    their own but stay in every denominator.

SELF-CHECKS, run before anything is written; any failure stops the script:
  1. the guarded verdict computed here equals rq_tables.load_judged row for
     row on every judged5 file (same instrument, not a reimplementation);
  2. retrieved accuracy per cell reproduces report/ch5/t53_cells.csv;
  3. every resident row has its gold and retrieved partner after exclusions.

Outputs (report/ch5/ unless --out):
  t56_resident_cells.csv      every cell and policy, pooled over readers
  t56_resident_summary.csv    deployable policies pooled, with intervals
  t56_resident_compression.csv compression's retrieval loss against the mean
                              eviction policy's, with intervals
  t56_resident.manifest.json  inputs (sha256, 12 hex), commit, parameters

Run from the repo root:
    python3 scripts/ch5_resident.py --artifacts <path to artifacts>
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))
import rq_tables as rq                                    # noqa: E402  the instrument

RETRIEVERS = ("lexical", "bm25", "dense")
COLS = ["config", "query_id", "arm", "ref_answer", "reader_answer",
        "correct", "tier", "judge_correct", "model"]
SOURCE = "judged_resident_{r}+judged5_{r}"


# ── the instrument ──────────────────────────────────────────────────────────
def guarded(d: pd.DataFrame) -> pd.DataFrame:
    """rq_tables.load_judged's verdict, lines for lines, on any arm.

    Kept textually parallel to load_judged so a diff reads easily; check 1
    proves equality on every judged5 row before this function is trusted.
    """
    d = d[d.ref_answer.fillna("").astype(str).str.strip() != ""].copy()
    det = rq.to_bool(d.correct).fillna(False)
    jud = rq.to_bool(d.judge_correct)
    routed = d.tier.isin(rq.ROUTED)
    hyb = det.mask(routed & jud.notna(), jud).astype(bool)
    ref = d.ref_answer.astype(str)
    ans = d.reader_answer.astype(str)
    g1 = hyb & ref.str.contains(rq.CONTRA_REF, case=False) & ~ans.str.contains(rq.FLAGS)
    g2 = hyb & ans.str.contains(rq.ABSTAIN)
    d["hyb"] = (hyb & ~g1 & ~g2).astype(int)
    return d


def read_judged(path: str) -> pd.DataFrame:
    d = pd.read_csv(path, usecols=lambda c: c in COLS, low_memory=False)
    d["workload"] = Path(path).name.split("judged_")[1].rsplit("_", 1)[0]
    return d


def split_config(d: pd.DataFrame) -> pd.DataFrame:
    parts = d.config.astype(str).str.split("/")
    d["policy"] = parts.str[3]
    d["budget"] = parts.str[5].map(rq.norm_budget)
    return d


def sha12(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()[:12]


# ── retrieval status and clusters, from the memory store artefacts ──────────
def store_facts(cfg_dir: Path) -> tuple[dict, dict]:
    """(query_id -> gone/hit/miss, query_id -> example_id) for one config."""
    alive, ex, seen = defaultdict(set), {}, set()
    with (cfg_dir / "gold_answer_snapshot.jsonl").open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            r = json.loads(line)
            q = r["query_id"]
            seen.add(q)
            ex[q] = r.get("example_id") or q
            if r.get("resident_text") and not r.get("was_evicted"):
                alive[q].add(r["gold_id"])
    status = {q: "gone" for q in seen}
    with (cfg_dir / "retrieved_index.jsonl").open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            row = json.loads(line)
            q = row["query_id"]
            if alive.get(q):
                ids = {x["id"] for x in row["retrieved"]}
                status[q] = "hit" if ids & alive[q] else "miss"
    return status, ex


# ── bootstrap over examples ─────────────────────────────────────────────────
def boot_weights(n_ex: int, rng) -> np.ndarray:
    """(N_BOOT, n_ex) multinomial resampling counts of examples."""
    return rng.multinomial(n_ex, np.full(n_ex, 1.0 / n_ex), size=rq.N_BOOT)


def ci(stat: np.ndarray) -> tuple[float, float]:
    lo, hi = np.nanpercentile(stat, [2.5, 97.5])
    return round(float(lo), 4), round(float(hi), 4)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--artifacts", type=Path, required=True,
                    help="folder holding judged5_*, judged_resident_* and benchmark_full")
    ap.add_argument("--out", type=Path, default=REPO / "report" / "ch5")
    ap.add_argument("--cells", type=Path, default=REPO / "report" / "ch5" / "t53_cells.csv")
    args = ap.parse_args()
    A, bf = args.artifacts, args.artifacts / "benchmark_full"
    args.out.mkdir(parents=True, exist_ok=True)
    inputs: list[str] = []
    rng = np.random.default_rng(rq.SEED)

    t53 = pd.read_csv(args.cells, dtype={"budget": str})
    inputs.append(str(args.cells))
    recs = []
    for r in RETRIEVERS:
        # ── check 1: same verdict as load_judged, row for row ──────────────
        rq.JUDGED = str(A / f"judged5_{r}")
        ref_rows, _ = rq.load_judged([])
        ref_rows = rq.apply_exclusions(ref_rows)
        files5 = sorted(glob.glob(f"{A}/judged5_{r}/judged_*.csv"))
        filesR = sorted(glob.glob(f"{A}/judged_resident_{r}/judged_*.csv"))
        if not files5 or not filesR:
            sys.exit(f"STOP: no judged files for {r} under {A}")
        inputs += files5 + filesR
        old = rq.apply_exclusions(split_config(
            guarded(pd.concat([read_judged(f) for f in files5], ignore_index=True))))
        mine = old[old.arm == "retrieved"]
        key = ["workload", "model", "policy", "budget", "query_id"]
        a = ref_rows.set_index(key).hyb.astype(int).sort_index()
        b = mine.set_index(key).hyb.astype(int).sort_index()
        if not (a.index.equals(b.index) and (a.values == b.values).all()):
            diff = (a != b.reindex(a.index)).sum() if a.index.equals(b.index) else "index differs"
            sys.exit(f"STOP check 1 ({r}): guarded verdict differs from rq_tables.load_judged: {diff}")
        print(f"check 1 {r:<8} guarded verdict identical to load_judged on {len(a):,} rows")

        # ── check 2: reproduce t53_cells ───────────────────────────────────
        cell = mine.groupby(["workload", "policy", "budget"]).hyb.mean().round(4).reset_index()
        t = t53[t53.retriever == r].merge(cell, on=["workload", "policy", "budget"], how="left")
        bad = t[(t.hyb - t.acc_guarded).abs() > 1e-4]
        if len(bad) or t.hyb.isna().any():
            print(bad.head(10).to_string(), file=sys.stderr)
            sys.exit(f"STOP check 2 ({r}): {len(bad)} cells differ from t53_cells.csv")
        print(f"check 2 {r:<8} t53_cells reproduced on {len(t)} cells")

        # ── join gold, resident, retrieved ─────────────────────────────────
        gold = old[old.arm == "gold"][["workload", "model", "query_id", "hyb"]] \
            .rename(columns={"hyb": "gold"})
        ret = mine[["workload", "model", "config", "query_id", "hyb"]].rename(columns={"hyb": "retrieved"})
        res = rq.apply_exclusions(split_config(
            guarded(pd.concat([read_judged(f) for f in filesR], ignore_index=True))))
        if set(res.arm) != {"resident"}:
            sys.exit(f"STOP: {r} resident files hold arms {set(res.arm)}")
        res = res[["workload", "model", "config", "policy", "budget", "query_id", "hyb"]] \
            .rename(columns={"hyb": "resident"})
        j = res.merge(ret, on=["workload", "model", "config", "query_id"], how="left") \
               .merge(gold, on=["workload", "model", "query_id"], how="left")
        lost = j[["retrieved", "gold"]].isna().any(axis=1).sum()
        if lost:
            sys.exit(f"STOP check 3 ({r}): {lost} resident rows without a gold or retrieved partner")
        print(f"check 3 {r:<8} {len(j):,} resident rows, all paired")

        facts = {c: store_facts(bf / c) for c in j.config.unique()}
        j["status"] = [facts[c][0].get(q, "?") for c, q in zip(j.config, j.query_id)]
        j["example"] = [facts[c][1].get(q, q) for c, q in zip(j.config, j.query_id)]
        if (j.status == "?").any():
            sys.exit(f"STOP: {r} has questions missing from their snapshot")
        j["retriever"] = r
        recs.append(j)

    df = pd.concat(recs, ignore_index=True)
    for c in ("gold", "resident", "retrieved"):
        df[c] = df[c].astype(int)
    df["d_forget"] = df.gold - df.resident
    df["d_retr"] = df.resident - df.retrieved

    # ── per cell and policy, pooled over readers ───────────────────────────
    rows = []
    for (r, w, b, p), g in df.groupby(["retriever", "workload", "budget", "policy"]):
        n = len(g)
        sh = g.status.value_counts(normalize=True)
        def part(s):
            x = g[g.status == s]
            return round(x.d_retr.sum() / n, 4)     # contribution to loss_retrieval
        rows.append({"source": SOURCE.format(r=r), "retriever": r, "workload": w, "budget": b,
                     "policy": p, "n_questions": g.query_id.nunique(), "n_rows": n,
                     "acc_gold": round(g.gold.mean(), 4), "acc_resident": round(g.resident.mean(), 4),
                     "acc_retrieved": round(g.retrieved.mean(), 4),
                     "loss_forgetting": round(g.d_forget.mean(), 4),
                     "loss_retrieval": round(g.d_retr.mean(), 4),
                     "loss_retrieval_from_miss": part("miss"), "loss_retrieval_from_hit": part("hit"),
                     "loss_reading_upper": round(1 - g.gold.mean(), 4),
                     "share_gone": round(sh.get("gone", 0), 4), "share_hit": round(sh.get("hit", 0), 4),
                     "share_miss": round(sh.get("miss", 0), 4), "post_hoc": True})
    cells = pd.DataFrame(rows)
    cells.to_csv(args.out / "t56_resident_cells.csv", index=False)

    # ── pooled deployable policies, with cluster bootstrap ─────────────────
    dep = df[~df.policy.isin(rq.DEPLOYABLE_EXCLUDES)]
    summ, comp = [], []
    for (r, w, b), g in dep.groupby(["retriever", "workload", "budget"]):
        exs = np.array(sorted(g.example.unique()))
        idx = {e: i for i, e in enumerate(exs)}
        W = boot_weights(len(exs), rng)                      # same draws for every quantity
        e_i = g.example.map(idx).to_numpy()
        def per_ex(v):                                       # per-example sums of v
            return np.bincount(e_i, weights=v, minlength=len(exs))
        cnt = per_ex(np.ones(len(g)))
        out = {"source": SOURCE.format(r=r), "retriever": r, "workload": w, "budget": b,
               "n_examples": len(exs), "n_rows": len(g)}
        for name, v in (("acc_gold", g.gold), ("acc_resident", g.resident),
                        ("acc_retrieved", g.retrieved), ("loss_forgetting", g.d_forget),
                        ("loss_retrieval", g.d_retr)):
            s = per_ex(v.to_numpy(float))
            out[name] = round(s.sum() / cnt.sum(), 4)
            out[f"{name}_lo"], out[f"{name}_hi"] = ci((W @ s) / (W @ cnt))
        out["interval"] = f"cluster bootstrap over examples, percentile 95%, B={rq.N_BOOT}"
        out["post_hoc"] = True
        summ.append(out)

        # compression's retrieval loss minus the mean eviction policy's
        if "compression" in set(g.policy):
            gc, ge = g[g.policy == "compression"], g[g.policy != "compression"]
            npol = ge.policy.nunique()
            ec = np.bincount(gc.example.map(idx), weights=gc.d_retr, minlength=len(exs))
            nc = np.bincount(gc.example.map(idx), minlength=len(exs)).astype(float)
            ee = np.bincount(ge.example.map(idx), weights=ge.d_retr, minlength=len(exs))
            ne = np.bincount(ge.example.map(idx), minlength=len(exs)).astype(float)
            ef = np.bincount(gc.example.map(idx), weights=gc.d_forget, minlength=len(exs))
            ff = np.bincount(ge.example.map(idx), weights=ge.d_forget, minlength=len(exs))
            diff = ec.sum() / nc.sum() - ee.sum() / ne.sum()
            bd = (W @ ec) / (W @ nc) - (W @ ee) / (W @ ne)
            fdiff = ef.sum() / nc.sum() - ff.sum() / ne.sum()
            bf_ = (W @ ef) / (W @ nc) - (W @ ff) / (W @ ne)
            lo, hi = ci(bd); flo, fhi = ci(bf_)
            comp.append({"source": SOURCE.format(r=r), "retriever": r, "workload": w, "budget": b,
                         "n_eviction_policies": npol, "n_examples": len(exs),
                         "retrieval_loss_compression": round(ec.sum() / nc.sum(), 4),
                         "retrieval_loss_eviction_mean": round(ee.sum() / ne.sum(), 4),
                         "excess_retrieval_loss": round(diff, 4), "excess_lo": lo, "excess_hi": hi,
                         "excess_forgetting_loss": round(fdiff, 4),
                         "excess_forgetting_lo": flo, "excess_forgetting_hi": fhi,
                         "interval_excludes_zero": bool(lo > 0 or hi < 0), "post_hoc": True})

    pd.DataFrame(summ).to_csv(args.out / "t56_resident_summary.csv", index=False)
    pd.DataFrame(comp).to_csv(args.out / "t56_resident_compression.csv", index=False)

    def git(*a):
        try:
            return subprocess.check_output(["git", "-C", str(REPO), *a], text=True, stderr=subprocess.DEVNULL).strip()
        except Exception:
            return "unknown"
    manifest = {
        "script": "scripts/ch5_resident.py", "post_hoc": True,
        "commit": git("rev-parse", "HEAD"), "dirty": bool(git("status", "--porcelain")),
        "verdict": "rq_tables guarded verdict (routed tiers + contradiction and abstention guards)",
        "routed_tiers": list(rq.ROUTED), "exclusions": str(rq.EXCLUDE_FILE),
        "deployable_excludes": sorted(rq.DEPLOYABLE_EXCLUDES),
        "bootstrap": {"unit": "example", "B": rq.N_BOOT, "seed": rq.SEED, "level": 0.95},
        "checks": ["guarded == load_judged row for row", "t53_cells reproduced",
                   "every resident row paired"],
        "inputs": {p: sha12(p) for p in inputs},
    }
    (args.out / "t56_resident.manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"\nwrote {args.out}/t56_resident_{{cells,summary,compression}}.csv and manifest")

    # short screen summary: pooled deployable, every budget
    s = pd.DataFrame(summ)
    print("\nPooled deployable policies, guarded verdict, 95% cluster bootstrap intervals")
    for _, x in s.sort_values(["workload", "retriever", "budget"]).iterrows():
        print(f"{x.workload:<22}{x.retriever:<8}{x.budget:>4}  gold {x.acc_gold:.3f}  "
              f"no retr {x.acc_resident:.3f}  retr {x.acc_retrieved:.3f}  "
              f"forget {x.loss_forgetting:+.3f} [{x.loss_forgetting_lo:+.3f},{x.loss_forgetting_hi:+.3f}]  "
              f"retrieval {x.loss_retrieval:+.3f} [{x.loss_retrieval_lo:+.3f},{x.loss_retrieval_hi:+.3f}]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
