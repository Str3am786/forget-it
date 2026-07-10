#!/usr/bin/env python3
"""
forget-it figures — the plots behind experimentation.ipynb.

Self-contained so it doesn't collide with edits to the notebook or src/.
Import and call from a notebook cell, or run as a script to dump PNGs:

    import forgetit_figures as F
    df = F.load_results("artifacts/10-05-results/combined_results.csv")
    F.save_all(df, outdir="figs", events_glob="artifacts/**/events.jsonl")

Figures:
  1. loss funnel              — retention / retrieval / reading / answered
  2. reading loss by qtype    — abstain-despite-retrieval per question type
  3. policy comparison        — gold_survived & hit@k by policy (pressured rows only)
  4. pressure diagnostic      — per-example evictions (and inserts if present); the
                                HONEST per-example signal, not the cumulative odometer
  5. byte composition         — useful vs non-gold bytes by budget
  6. survival + value-decay   — from events.jsonl (no lifelines dependency)

Schema-tolerant: accepts `non_gold_byte_fraction` or legacy `waste_rate`; reads
the real per-query deltas (`evictions_since_last_query` / `inserts_since_last_query`)
when present, else differences the cumulative odometer within each config. Budget
figures group by `budget_frac` (per-example relative %) when present, since
`budget_bytes` is now per-example and can't label a config.
"""
from __future__ import annotations

import glob
import json
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # headless-safe; notebooks override with their own backend
import matplotlib.pyplot as plt

# ---- light house style -----------------------------------------------------
plt.rcParams.update({
    "figure.dpi": 120, "savefig.dpi": 150, "savefig.bbox": "tight",
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.alpha": 0.25, "grid.linewidth": 0.6,
    "font.size": 10, "axes.titlesize": 12, "axes.titleweight": "bold",
})
# colour-blind-safe-ish, semantically ordered loss -> good
C = {"retention": "#d1495b", "retrieval": "#edae49", "reading": "#7c6a9c",
     "answered": "#2a9d8f", "a": "#264653", "b": "#2a9d8f", "c": "#e76f51"}
def _config_keys(df: pd.DataFrame) -> list[str]:
    """Columns identifying one config. Uses budget_frac (the config-level knob)
    when present — budget_bytes is now PER-EXAMPLE, so it can't identify a config."""
    budget = "budget_frac" if "budget_frac" in df.columns else "budget_bytes"
    return [c for c in ("policy", budget, "itemiser", "retriever") if c in df.columns]


def _budget_axis(df: pd.DataFrame):
    """(column, ordered values, labels) for putting budget on an x-axis.

    Prefers budget_frac (per-example relative %); budget_bytes is per-example now
    and would scatter into many bars, so it's only the legacy fallback.
    """
    if "budget_frac" in df.columns and df["budget_frac"].notna().any():
        vals = sorted(df["budget_frac"].dropna().unique(), reverse=True)   # 100% → 10%
        return "budget_frac", vals, [f"{v*100:.0f}%" for v in vals]
    vals = sorted(df["budget_bytes"].dropna().unique())
    return "budget_bytes", vals, [f"{int(b/1024)} KB" for b in vals]


# ---- loading & schema normalisation ---------------------------------------
def load_results(path: str | Path | None = None) -> pd.DataFrame:
    if path is None:
        cands = [Path("artifacts/10-05-results/combined_results.csv"),
                 Path("../artifacts/10-05-results/combined_results.csv")]
        path = next((p for p in cands if p.exists()), cands[0])
    df = pd.read_csv(path)
    for col in ("gold_survived", "abstained"):
        if col in df.columns:
            df[col] = df[col].astype(bool)
    # unify the byte-cost column name (rename landed or not)
    if "non_gold_byte_fraction" not in df.columns and "waste_rate" in df.columns:
        df = df.rename(columns={"waste_rate": "non_gold_byte_fraction"})
    return df


def _masks(df: pd.DataFrame) -> dict:
    """Mirror the notebook (cells 11/13) exactly so figures match the tables."""
    survived  = df["gold_survived"].astype(bool)
    retrieved = survived & (df["hit_at_k"] == 1.0)
    answered  = retrieved & ~df["abstained"].astype(bool)
    reading   = retrieved & df["abstained"].astype(bool)
    return dict(survived=survived, retrieved=retrieved,
                answered=answered, reading=reading, n=len(df))


def per_example_evictions(df: pd.DataFrame) -> pd.Series:
    """Honest per-example evictions.

    Prefer an explicit per-example column if the new schema added one; else
    difference the cumulative odometer WITHIN each config (rows are in example
    order), which is what cell 4 proved is the real signal.
    """
    # New schema exposes the real per-query delta directly — prefer it.
    for c in ("evictions_since_last_query", "evictions_this_example",
              "evictions_delta", "evictions_per_example"):
        if c in df.columns:
            return df[c].astype(float)
    if "evictions_total_at_query_time" not in df.columns:
        return pd.Series(np.nan, index=df.index)
    keys = _config_keys(df)
    out = (df.groupby(keys, sort=False)["evictions_total_at_query_time"].diff())
    # first row of each config has no predecessor -> it IS that example's count
    first = df.groupby(keys, sort=False).cumcount() == 0
    out[first] = df.loc[first, "evictions_total_at_query_time"]
    return out.astype(float)


# ---- 1. loss funnel --------------------------------------------------------
def fig_loss_funnel(df: pd.DataFrame, ax=None):
    m = _masks(df); n = m["n"]
    parts = [
        ("retention loss", (~m["survived"]).sum(), C["retention"]),
        ("retrieval loss", (m["survived"] & (df["hit_at_k"] != 1.0)).sum(), C["retrieval"]),
        ("reading loss",   m["reading"].sum(), C["reading"]),
        ("answered",       m["answered"].sum(), C["answered"]),
    ]
    if ax is None:
        _, ax = plt.subplots(figsize=(8, 1.9))
    left = 0.0
    for label, val, col in parts:
        pct = 100 * val / n
        ax.barh(0, pct, left=left, color=col, edgecolor="white")
        if pct > 4:
            ax.text(left + pct / 2, 0, f"{label}\n{pct:.1f}%", ha="center",
                    va="center", color="white", fontsize=9, fontweight="bold")
        left += pct
    ax.set_xlim(0, 100); ax.set_ylim(-0.6, 0.6)
    ax.set_yticks([]); ax.set_xlabel("% of all scored queries")
    ax.set_title("Where the answer is lost: retention → retrieval → reading")
    ax.grid(False)
    return ax


# ---- 2. reading loss by question type --------------------------------------
def fig_reading_loss_by_qtype(df: pd.DataFrame, ax=None):
    if "question_type" not in df.columns:
        return None
    m = _masks(df)
    by = pd.DataFrame({
        "retrievable": m["retrieved"].groupby(df["question_type"]).sum(),
        "reading_loss": m["reading"].groupby(df["question_type"]).sum(),
    })
    by["rate"] = (by["reading_loss"] / by["retrievable"]).fillna(0)
    by = by.sort_values("rate")
    if ax is None:
        _, ax = plt.subplots(figsize=(7, 0.5 * len(by) + 1.5))
    ax.barh(by.index, by["rate"], color=C["reading"])
    for y, (r, nq) in enumerate(zip(by["rate"], by["retrievable"])):
        ax.text(min(r + 0.02, 0.98), y, f"{r:.0%}  (n={int(nq)})",
                va="center", fontsize=8)
    ax.set_xlim(0, 1); ax.set_xlabel("abstained despite gold retrieved")
    ax.set_title("Reading loss by question type\n(high = reader discards answers it holds)")
    return ax


# ---- 3. policy comparison (pressured rows only) ----------------------------
def fig_policy_comparison(df: pd.DataFrame, ax=None):
    ev = per_example_evictions(df)
    P = df[ev.fillna(0) > 0] if ev.notna().any() else df[df.get(
        "evictions_total_at_query_time", pd.Series(0, index=df.index)) > 0]
    if len(P) == 0:
        return None
    # Aggregation unit = (policy, retriever) for history policies: LRU/LFU
    # state is built from retrieval touches, so pooling them across retrievers
    # under a bare name launders the interaction into a "policy" score
    # (metrics.policy_unit — the D3 coupled-identity rule).
    from forgetit.benchmark.metrics import policy_unit
    P = P.copy()
    retr = P["retriever"] if "retriever" in P.columns else pd.Series("", index=P.index)
    P["policy_unit"] = [policy_unit(p, r) for p, r in zip(P["policy"], retr.fillna(""))]
    order = ["lru", "lfu", "random", "compression"]
    unit_order = [u for p in order for u in sorted(P["policy_unit"].unique())
                  if u == p or u.startswith(p + "@")]
    g = (P.groupby("policy_unit")[["gold_survived", "hit_at_k"]].mean()
           .reindex(unit_order))
    if ax is None:
        _, ax = plt.subplots(figsize=(6.5, 3.6))
    x = np.arange(len(g)); w = 0.38
    ax.bar(x - w / 2, g["gold_survived"], w, label="gold survived", color=C["a"])
    ax.bar(x + w / 2, g["hit_at_k"], w, label="hit@k", color=C["b"])
    for xi, (gs, hk) in enumerate(zip(g["gold_survived"], g["hit_at_k"])):
        ax.text(xi - w / 2, gs + 0.01, f"{gs:.2f}", ha="center", fontsize=8)
        ax.text(xi + w / 2, hk + 0.01, f"{hk:.2f}", ha="center", fontsize=8)
    ax.set_xticks(x); ax.set_xticklabels(g.index)
    ax.set_ylim(0, 1.05); ax.set_ylabel("mean over pressured queries")
    ax.set_title("Policy ranking where eviction actually bites")
    ax.legend(frameon=False, ncol=2, loc="lower center")
    return ax


# ---- 4. pressure diagnostic ------------------------------------------------
def fig_pressure_diagnostic(df: pd.DataFrame, ax=None):
    """Per-example evictions by budget (and inserts if the schema has them).
    This is the corrected, honest signal — not the cumulative odometer."""
    ev = per_example_evictions(df)
    tmp = df.assign(_ev=ev)
    col, vals, labels = _budget_axis(df)
    g = tmp.groupby(col)["_ev"].mean().reindex(vals)
    ins_col = next((c for c in ("inserts_since_last_query", "inserts_this_example",
                                "inserts", "inserts_per_example") if c in df.columns), None)
    if ax is None:
        _, ax = plt.subplots(figsize=(7, 3.6))
    x = np.arange(len(vals))
    if ins_col:
        gi = tmp.assign(_ins=df[ins_col]).groupby(col)["_ins"].mean().reindex(vals)
        w = 0.38
        ax.bar(x - w / 2, gi.values, w, label="inserts / example", color=C["b"])
        ax.bar(x + w / 2, g.values, w, label="evictions / example", color=C["c"])
        ax.legend(frameon=False)
    else:
        ax.bar(x, g.values, 0.6, color=C["c"], label="evictions / example")
        ax.text(0.02, 0.95, "add an inserts column to see the eviction/insert ratio",
                transform=ax.transAxes, fontsize=8, va="top", style="italic", alpha=0.7)
    ax.set_xticks(x); ax.set_xticklabels(labels)
    ax.set_ylabel("mean per example")
    ax.set_xlabel("budget (% of each example's footprint)" if col == "budget_frac" else "budget")
    ax.set_title("Pressure per example by budget (relative budgeting → every <100% forgets)")
    return ax


# ---- 5. byte composition ---------------------------------------------------
def fig_byte_composition(df: pd.DataFrame, ax=None):
    if "useful_byte_fraction" not in df.columns:
        return None
    col, vals, labels = _budget_axis(df)
    g = df.groupby(col)["useful_byte_fraction"].mean().reindex(vals)
    if ax is None:
        _, ax = plt.subplots(figsize=(7, 3.4))
    x = np.arange(len(vals))
    ax.bar(x, g.values, 0.6, color=C["answered"], label="gold (useful) bytes")
    ax.bar(x, 1 - g.values, 0.6, bottom=g.values, color="#d9d9d9",
           label="non-gold bytes")
    ax.set_xticks(x); ax.set_xticklabels(labels); ax.set_ylim(0, 1)
    ax.set_ylabel("fraction of stored bytes")
    ax.set_xlabel("budget (% of each example's footprint)" if col == "budget_frac" else "budget")
    ax.set_title("Store composition: most retained bytes aren't the answer\n"
                 "(non-gold ≠ wasted — you can't know the future)")
    ax.legend(frameon=False, loc="center right")
    return ax


# ---- 6. survival + value-decay from events.jsonl ---------------------------
def _life_table(events_path):
    """Gold-item lifetimes + censoring + V(t) replay. Mirrors section 4, no lifelines."""
    ev = pd.read_json(events_path, lines=True)
    ev = ev.sort_values("seq" if "seq" in ev.columns else "t").reset_index(drop=True)
    ev["id"] = ev["id"].map(lambda v: None if pd.isna(v) else str(v))
    gold = set()
    if "gold_ids" in ev.columns:
        for x in ev.loc[ev["name"] == "gold", "gold_ids"].dropna():
            gold |= {str(i) for i in x}
    clears = sorted(ev.loc[ev["name"] == "clear", "t_insert"].tolist())
    last = ev["t_insert"].max()

    def next_clear_after(t):
        return next((c for c in clears if c >= t), last)

    ins = ev[ev["name"] == "insert"][["id", "t_insert"]].rename(columns={"t_insert": "born"})
    evi = ev[ev["name"] == "evict"][["id", "t_insert"]].rename(columns={"t_insert": "died"})
    evicted = set(evi.id)
    life = ins.merge(evi, on="id", how="left")
    life["evicted"] = life.id.isin(evicted)
    life["exit"] = [d if (i in evicted and pd.notna(d)) else next_clear_after(b)
                    for i, b, d in zip(life.id, life.born, life.died)]
    life["lifetime"] = (life["exit"] - life["born"]).clip(lower=0)
    life["is_gold"] = life.id.isin(gold)

    # V(t): #gold resident over time
    resident, ts, Vs = set(), [], []
    for _, e in ev[ev["name"].isin(["insert", "evict", "clear"])].iterrows():
        k, rid = e["name"], e["id"]
        if k == "insert":
            resident.add(rid)
        elif k == "clear":
            resident.clear()
        else:
            resident.discard(rid)
        ts.append(e["t_insert"]); Vs.append(sum(1 for r in resident if r in gold))
    return life[life.is_gold], np.array(ts), np.array(Vs)


def _km(lifetimes, evicted):
    """Dependency-free Kaplan-Meier step function. evicted=True is the event."""
    t = np.asarray(lifetimes, float); e = np.asarray(evicted, bool)
    order = np.argsort(t); t, e = t[order], e[order]
    times, S, surv, n = [0.0], [1.0], 1.0, len(t)
    for i, ti in enumerate(t):
        n_at_risk = n - i
        if e[i] and n_at_risk > 0:
            surv *= (1 - 1 / n_at_risk)
        times.append(ti); S.append(surv)
    return np.array(times), np.array(S)


def fig_survival_and_decay(events_path, axes=None):
    g, ts, Vs = _life_table(events_path)
    if axes is None:
        _, axes = plt.subplots(1, 2, figsize=(11, 3.6))
    ax1, ax2 = axes
    if len(g):
        x, S = _km(g.lifetime.values, g.evicted.values)
        ax1.step(x, S, where="post", color=C["retention"], lw=2)
        ax1.fill_between(x, S, step="post", alpha=0.12, color=C["retention"])
        rmst = float(np.sum(S[:-1] * np.diff(x))) if len(x) > 1 else 0.0
        ax1.text(0.95, 0.9, f"RMST ≈ {rmst:.1f} inserts", transform=ax1.transAxes,
                 ha="right", fontsize=9)
    ax1.set_ylim(0, 1.02); ax1.set_xlabel("inserts since item entered")
    ax1.set_ylabel("fraction of gold still resident")
    ax1.set_title("Gold survival curve (Kaplan–Meier)")
    ax2.plot(ts, Vs, color=C["a"], lw=1.5)
    ax2.fill_between(ts, Vs, alpha=0.12, color=C["a"])
    ax2.set_xlabel("event ordinal (t_insert)"); ax2.set_ylabel("# gold resident")
    ax2.set_title("Value decay: gold held over time (∫ = retained-gold mass)")
    return axes


# ---- driver ----------------------------------------------------------------
def save_all(df: pd.DataFrame, outdir="figs", events_glob=None):
    out = Path(outdir); out.mkdir(parents=True, exist_ok=True)
    made = []
    builders = {
        "01_loss_funnel": fig_loss_funnel,
        "02_reading_loss_by_qtype": fig_reading_loss_by_qtype,
        "03_policy_comparison": fig_policy_comparison,
        "04_pressure_diagnostic": fig_pressure_diagnostic,
        "05_byte_composition": fig_byte_composition,
    }
    for name, fn in builders.items():
        ax = fn(df)
        if ax is None:
            print(f"skip {name} (needed columns/rows absent)"); continue
        fig = ax.figure
        fig.savefig(out / f"{name}.png"); plt.close(fig); made.append(name)
    if events_glob:
        logs = sorted(glob.glob(events_glob, recursive=True))
        if logs:
            axes = fig_survival_and_decay(logs[0])
            axes[0].figure.savefig(out / "06_survival_decay.png")
            plt.close(axes[0].figure); made.append("06_survival_decay")
        else:
            print("no events.jsonl yet — run a pressured config with the event log "
                  "to get the survival/decay figure")
    print(f"wrote {len(made)} figures to {out}/: {', '.join(made)}")
    return made


if __name__ == "__main__":
    import sys
    df = load_results(sys.argv[1] if len(sys.argv) > 1 else None)
    save_all(df, events_glob="artifacts/**/events.jsonl")