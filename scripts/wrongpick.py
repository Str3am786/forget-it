#!/usr/bin/env python3
"""Wrong-pick cost and the decision on H0, per setting and per reader.

Chapter 5, first results section. Reuses the canonical merge of
scripts/rq_tables.py (guarded hybrid verdict, LoCoMo exclusion, store side
restricted to gradeable questions) and changes one thing: the readers are
analysed separately instead of pooled.

For each setting (workload, byte budget below 100%, retriever) and reader:
  p_EP   policy with the highest evidence presence    (ties: alphabetical)
  p_Acc  policy with the highest answer accuracy      (ties: alphabetical)
  W(s)   Acc(p_Acc) - Acc(p_EP)
  95% percentile interval of W(s), 10,000 bootstrap resamples of questions,
         both picks chosen again in each resample
  exact McNemar on all 91 pairs, Benjamini-Hochberg at 0.05 within the
         setting, accuracy and evidence presence as separate families
  reject H0: W(s) >= 0.03 and the McNemar test between p_Acc and p_EP rejects
  second count: CONTRADICTS pairs, as in rq_tables.vdct3

A pseudo-reader POOLED reproduces rq_tables.py exactly (accuracy averaged
over the five readers for the picks, reader rows stacked for McNemar). Its
point estimates must equal the old wrongpick.csv. If they do, the loader is
right and the per-reader rows can be trusted.

Commands
  python3 wrongpick.py selftest
  python3 wrongpick.py run [ART] [OUT]
Defaults
  ART  = ~/lumi_backup/scratch/project_462001123/artifacts
  OUT  = ~/thesis_checks/wrongpick
"""
import datetime
import hashlib
import json
import math
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ------------------------------------------------ as in rq_tables.py ------
DELTA = 0.03                     # MATERIALITY
ALPHA = 0.05
N_BOOT = 10_000
SEED = 42
ROUTED = ("miss", "content_words", "reverse")
EXCLUDED_POLICIES = {"oracle"}
CONTRA_REF = "contradictory information"
FLAGS = re.compile(r"contradict|inconsist|conflict|clarif|discrepan", re.I)
ABSTAIN = re.compile(
    r"\b(?:idk|i don'?t know|do(?:es)? not contain|no information|"
    r"not (?:provided|mentioned|specified|available)|cannot determine|"
    r"unable to (?:determine|find)|"
    r"don'?t have (?:enough |that )?information)\b", re.I)
EXCLUDE_FILE = Path("~/lumi_backup/home/15-08-forgetit/forget-it/configs/"
                    "locomo_ungradeable_ids.txt").expanduser()

# ------------------------------------------------------------ this script --
RETRIEVERS = ["lexical", "bm25", "dense"]
BUDGETS = ["075", "050", "025", "010"]        # "100" only for the sanity check
PRIMARY = "qwen2.5-7b-instruct"
POOLED = "POOLED"
CHUNK = 500


# ------------------------------------------------------------------ helpers
def to_bool(s):
    return s.astype(str).str.strip().str.lower().map(
        {"true": True, "false": False}).astype("boolean")


def budget_code(cfg_part):
    return cfg_part.str.extract(r"budget_(\d+)pct")[0]


def store_budget(v):
    return {0.1: "010", 0.25: "025", 0.5: "050", 0.75: "075",
            1.0: "100"}[round(float(v), 2)]


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()[:16]


def exclude(df, bad):
    return df[~(df["workload"].str.startswith("locomo") & df["query_id"].isin(bad))]


# ------------------------------------------------------------------ loading
def load_judged(art, retriever):
    frames, files, fired = [], [], []
    for f in sorted(art.glob(f"judged5_{retriever}/judged_*.csv")):
        files.append(f)
        w = f.name.split("judged_")[1].rsplit("_", 1)[0]
        d = pd.read_csv(f, low_memory=False)
        d = d[d.ref_answer.fillna("").astype(str).str.strip() != ""].copy()
        # Guarded hybrid, line for line as rq_tables.load_judged.
        det = to_bool(d.correct).fillna(False)
        jud = to_bool(d.judge_correct)
        routed = d.tier.isin(ROUTED)
        hyb = det.mask(routed & jud.notna(), jud).astype(bool)
        ref, ans = d.ref_answer.astype(str), d.reader_answer.astype(str)
        g1 = hyb & ref.str.contains(CONTRA_REF, case=False) & ~ans.str.contains(FLAGS)
        g2 = hyb & ans.str.contains(ABSTAIN)
        d["acc"] = (hyb & ~g1 & ~g2).astype(float)
        d = d[d.arm == "retrieved"].copy()
        parts = d.config.astype(str).str.split("/")
        d["policy"] = parts.str[3]
        d["budget"] = budget_code(parts.str[5])
        if (parts.str[4] != retriever).any():
            sys.exit(f"{f.name}: config retriever is not {retriever}")
        d["workload"], d["reader"], d["retriever"] = w, d.model, retriever
        fired.append({"file": f.name, "g_contra": int(g1[d.index].sum()),
                      "g_abst": int(g2[d.index].sum()), "rows": len(d)})
        frames.append(d[["workload", "retriever", "reader", "policy", "budget",
                         "query_id", "acc"]])
    if not frames:
        sys.exit(f"No judged files for {retriever} under {art}")
    return pd.concat(frames, ignore_index=True), files, fired


def load_store(art, retriever):
    frames, files = [], []
    for f in sorted(art.glob(f"alive_dated_{retriever}/store_*.csv")):
        if f.name.endswith(".cache.csv"):
            continue
        files.append(f)
        w = f.name.split("store_")[1].replace(".csv", "")
        d = pd.read_csv(f, low_memory=False)
        d["workload"], d["retriever"] = w, retriever
        d["budget"] = d.budget.map(store_budget)
        d["ep"] = to_bool(d.answerable_store).astype(float)
        frames.append(d[["workload", "retriever", "policy", "budget",
                         "query_id", "ep", "n_gold"]])
    if not frames:
        sys.exit(f"No store files for {retriever} under {art}")
    return pd.concat(frames, ignore_index=True), files


# --------------------------------------------------------------- statistics
def mcnemar_p(b, c):
    n = b + c
    if n == 0:
        return 1.0
    k = min(b, c)
    return min(1.0, 2 * sum(math.comb(n, i) for i in range(k + 1)) / 2 ** n)


def bh_reject(p):
    p = np.asarray(p, float)
    m = len(p)
    order = np.argsort(p)
    passed = p[order] <= ALPHA * np.arange(1, m + 1) / m
    k = np.max(np.nonzero(passed)[0]) + 1 if passed.any() else 0
    out = np.zeros(m, bool)
    out[order[:k]] = True
    return out


def pairwise(M, pols):
    """McNemar on every pair of columns of a 0/1 matrix (rows: questions, or
    question x reader when pooled). Rows with a missing value are dropped."""
    rec = []
    for i in range(len(pols)):
        for j in range(i + 1, len(pols)):
            x, y = M[:, i], M[:, j]
            ok = ~np.isnan(x) & ~np.isnan(y)
            x, y = x[ok] == 1, y[ok] == 1
            b, c = int((x & ~y).sum()), int((~x & y).sum())
            rec.append((i, j, x.mean() - y.mean(), b, c, mcnemar_p(b, c)))
    t = pd.DataFrame(rec, columns=["i", "j", "diff", "b", "c", "p"])
    t["sig"] = bh_reject(t["p"].to_numpy())
    return t


def first_max(v):
    """Columns are alphabetical, so the first maximum is the tie-break."""
    return int(np.argmax(np.where(np.isnan(v), -np.inf, v)))


def analyse(A, E, pols, rng, A_test=None):
    """A: questions x policies accuracy (0/1, or a mean over readers when
    pooled). E: questions x policies evidence presence, same rows.
    A_test: rows used for the accuracy McNemar (defaults to A)."""
    A_test = A if A_test is None else A_test
    acc, ep = np.nanmean(A, axis=0), np.nanmean(E, axis=0)
    i_ep, i_acc = first_max(ep), first_max(acc)
    W = acc[i_acc] - acc[i_ep]

    n = A.shape[0]
    A0, Am = np.nan_to_num(A), (~np.isnan(A)).astype(float)
    E0, Em = np.nan_to_num(E), (~np.isnan(E)).astype(float)
    Wb, done = np.empty(N_BOOT), 0
    while done < N_BOOT:
        k = min(CHUNK, N_BOOT - done)
        w = rng.multinomial(n, np.full(n, 1.0 / n), size=k).astype(float)
        with np.errstate(invalid="ignore", divide="ignore"):
            ab, eb = (w @ A0) / (w @ Am), (w @ E0) / (w @ Em)
        ab, eb = np.nan_to_num(ab, nan=-np.inf), np.nan_to_num(eb, nan=-np.inf)
        r = np.arange(k)
        Wb[done:done + k] = ab[r, ab.argmax(1)] - ab[r, eb.argmax(1)]
        done += k
    lo, hi = np.percentile(Wb, [2.5, 97.5])

    pa, pe = pairwise(A_test, pols), pairwise(E, pols)
    if i_ep == i_acc:
        b = c = 0
        p, sig, reject = 1.0, False, False
    else:
        i, j = sorted((i_ep, i_acc))
        row = pa[(pa.i == i) & (pa.j == j)].iloc[0]
        b, c, p, sig = int(row.b), int(row.c), float(row.p), bool(row.sig)
        reject = bool(W >= DELTA - 1e-12 and sig)

    m = pa.merge(pe, on=["i", "j"], suffixes=("_acc", "_ep"))
    conf_a = m.sig_acc & (m.diff_acc.abs() >= DELTA)
    conf_e = m.sig_ep & (m.diff_ep.abs() >= DELTA)
    con = m[conf_a & conf_e & (np.sign(m.diff_acc) != np.sign(m.diff_ep))]
    con = pd.DataFrame({
        "ep_prefers": [pols[a] if d > 0 else pols[b_]
                       for a, b_, d in zip(con.i, con.j, con.diff_ep)],
        "acc_prefers": [pols[a] if d > 0 else pols[b_]
                        for a, b_, d in zip(con.i, con.j, con.diff_acc)],
        "diff_acc": con.diff_acc.abs().round(4).values,
        "diff_ep": con.diff_ep.abs().round(4).values})

    return {
        "p_ep": pols[i_ep], "p_acc": pols[i_acc],
        "ep_of_p_ep": ep[i_ep], "ep_of_p_acc": ep[i_acc],
        "acc_of_p_ep": acc[i_ep], "acc_of_p_acc": acc[i_acc],
        "acc_rank_of_p_ep": int((acc > acc[i_ep] + 1e-12).sum()) + 1,
        "W": W, "W_lo": lo, "W_hi": hi,
        "mcnemar_b": b, "mcnemar_c": c, "p": p, "bh_sig": sig,
        "reject_H0": reject, "n_contradicts": len(con), "n_questions": n,
    }, con


# --------------------------------------------------------------------- run
def run(art, out):
    out.mkdir(parents=True, exist_ok=True)
    if not EXCLUDE_FILE.is_file():
        sys.exit(f"{EXCLUDE_FILE} missing: LoCoMo adversarial would stay in")
    bad = {x.strip() for x in EXCLUDE_FILE.read_text().split() if x.strip()}
    rng = np.random.default_rng(SEED)
    rows, contras, files, fired, sanity = [], [], [EXCLUDE_FILE], [], []

    for r in RETRIEVERS:
        acc, f1, g = load_judged(art, r)
        st, f2 = load_store(art, r)
        files += f1 + f2
        fired += g
        acc, st = exclude(acc, bad), exclude(st, bad)
        # Store side restricted to gradeable questions, as rq_tables does.
        gradeable = acc[["workload", "query_id"]].drop_duplicates()
        st = st.merge(gradeable, on=["workload", "query_id"])
        n_nogold = int((st.n_gold == 0).sum())
        if n_nogold:
            print(f"NOTE {r}: {n_nogold} store rows with no gold item "
                  "(scored as EP 0, as in rq_tables)")

        for (w, b), a_wb in acc.groupby(["workload", "budget"]):
            s_wb = st[(st.workload == w) & (st.budget == b)]
            if b == "100":
                p = a_wb[a_wb.reader == PRIMARY].groupby("policy").acc.mean()
                e = s_wb.groupby("policy").ep.mean()
                sanity.append({"workload": w, "retriever": r,
                               "acc_spread": round(p.max() - p.min(), 4),
                               "ep_spread": round(e.max() - e.min(), 4)})
                continue
            a_wb = a_wb[~a_wb.policy.isin(EXCLUDED_POLICIES)]
            s_wb = s_wb[~s_wb.policy.isin(EXCLUDED_POLICIES)]
            pols = sorted(set(a_wb.policy) & set(s_wb.policy))
            E = s_wb.pivot_table(index="query_id", columns="policy",
                                 values="ep", aggfunc="max")
            key = {"workload": w, "budget": b, "retriever": r}

            readers = sorted(a_wb.reader.unique())
            for reader in readers + [POOLED]:
                if reader == POOLED:
                    Ap = a_wb.pivot_table(index="query_id", columns="policy",
                                          values="acc", aggfunc="mean")
                    St = a_wb.pivot_table(index=["query_id", "reader"],
                                          columns="policy", values="acc")
                else:
                    Ap = a_wb[a_wb.reader == reader].pivot_table(
                        index="query_id", columns="policy", values="acc")
                    St = None
                ids = Ap.index.intersection(E.index)
                A = Ap.reindex(index=ids, columns=pols).to_numpy(float)
                Ee = E.reindex(index=ids, columns=pols).to_numpy(float)
                At = (None if St is None else
                      St[St.index.get_level_values(0).isin(ids)]
                      .reindex(columns=pols).to_numpy(float))
                res, con = analyse(A, Ee, pols, rng, At)
                k2 = {"reader": reader, **key}
                rows.append({**k2, **res, "n_policies": len(pols)})
                if len(con):
                    contras.append(con.assign(**k2))
        print(f"done: {r}")

    res = pd.DataFrame(rows)
    num = ["ep_of_p_ep", "ep_of_p_acc", "acc_of_p_ep", "acc_of_p_acc",
           "W", "W_lo", "W_hi", "p"]
    res[num] = res[num].round(4)
    res = res.sort_values(["reader", "workload", "retriever", "budget"])
    res.to_csv(out / "wrongpick_by_setting.csv", index=False)
    con = pd.concat(contras, ignore_index=True) if contras else pd.DataFrame()
    con.to_csv(out / "contradicts_pairs.csv", index=False)
    pd.DataFrame(sanity).to_csv(out / "sanity_100pct.csv", index=False)
    pd.DataFrame(fired).to_csv(out / "guard_firings.csv", index=False)

    text = summarise(res, pd.DataFrame(sanity))
    print("\n" + text)
    (out / "summary.txt").write_text(text + "\n")
    (out / "manifest.json").write_text(json.dumps({
        "script_sha256": sha256(Path(__file__)),
        "run_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "params": {"delta": DELTA, "alpha": ALPHA, "n_boot": N_BOOT,
                   "seed": SEED, "routed": ROUTED, "primary": PRIMARY,
                   "excluded_policies": sorted(EXCLUDED_POLICIES)},
        "inputs": {str(f): sha256(f) for f in files},
    }, indent=1))
    print(f"\nOutputs in {out}")


def summarise(res, sanity):
    L = ["SANITY at 100% (EP spread must be 0, accuracy spread <= 0.009):",
         sanity.to_string(index=False), ""]
    for reader in [PRIMARY, POOLED]:
        p = res[res.reader == reader]
        L.append(f"=== {reader}: {int(p.reject_H0.sum())} of {len(p)} "
                 f"settings reject H0; picks differ in "
                 f"{int((p.p_ep != p.p_acc).sum())}; contradicting pairs "
                 f"{int(p.n_contradicts.sum())}")
        for col in ["workload", "budget", "retriever"]:
            t = p.groupby(col).reject_H0.agg(["sum", "count"])
            L.append(f"by {col}: " + ", ".join(
                f"{i} {s}/{c}" for i, (s, c) in t.iterrows()))
        L.append(p[["workload", "budget", "retriever", "p_ep", "p_acc", "W",
                    "W_lo", "W_hi", "p", "bh_sig", "reject_H0"]]
                 .to_string(index=False))
        L.append("EP picks: " + p.p_ep.value_counts().to_dict().__repr__())
        L.append("Acc picks: " + p.p_acc.value_counts().to_dict().__repr__())
        L.append("")
    prim = res[(res.reader == PRIMARY) & res.reject_H0][
        ["workload", "budget", "retriever"]].apply(tuple, axis=1)
    L.append("OTHER READERS (settings rejecting, shared with primary):")
    for reader, g in res[~res.reader.isin([PRIMARY, POOLED])].groupby("reader"):
        theirs = g[g.reject_H0][["workload", "budget", "retriever"]].apply(
            tuple, axis=1)
        L.append(f"  {reader:24s} {len(theirs):2d} of {len(g)}, shared "
                 f"{len(set(prim) & set(theirs))}, contradicting pairs "
                 f"{int(g.n_contradicts.sum())}")
    return "\n".join(L)


# ----------------------------------------------------------------- selftest
def selftest():
    rng = np.random.default_rng(0)
    pols = [f"p{i:02d}" for i in range(14)]
    n = 600
    acc_p = np.full(14, 0.45); acc_p[0] = 0.40; acc_p[5] = 0.50
    ep_p = np.full(14, 0.60); ep_p[0] = 0.80
    A = (rng.random((n, 14)) < acc_p).astype(float)
    E = (rng.random((n, 14)) < ep_p).astype(float)
    res, _ = analyse(A, E, pols, np.random.default_rng(1))
    assert res["p_ep"] == "p00" and res["W"] > 0 and res["W_lo"] <= res["W"]
    T = np.tile(A[:, [0]], (1, 14))
    r2, _ = analyse(T, T, pols, np.random.default_rng(2))
    assert r2["p_ep"] == r2["p_acc"] == "p00" and r2["W"] == 0
    from scipy.stats import binomtest
    assert abs(mcnemar_p(3, 11) - binomtest(3, 14, 0.5).pvalue) < 1e-12
    assert list(bh_reject([0.01, 0.04, 0.03, 0.2])) == [True, False, False, False]
    assert list(bh_reject([0.01, 0.02, 0.03, 0.04])) == [True] * 4
    # Pooled path: fractional A for picks, stacked 0/1 rows for the test.
    S = (rng.random((3 * n, 14)) < acc_p).astype(float)
    Ap = S.reshape(n, 3, 14).mean(1)
    r3, _ = analyse(Ap, E, pols, np.random.default_rng(3), S)
    assert r3["p_ep"] == "p00"
    print("selftest passed")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    art = Path(sys.argv[2] if len(sys.argv) > 2 else
               "~/lumi_backup/scratch/project_462001123/artifacts").expanduser()
    out = Path(sys.argv[3] if len(sys.argv) > 3 else
               "~/thesis_checks/wrongpick").expanduser()
    {"selftest": selftest, "run": lambda: run(art, out)}[cmd]()
