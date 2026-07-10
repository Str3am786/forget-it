"""Phase 3 — figures F1–F8 (+ two mechanism figures the data demands).

Consumes the Phase-2 tables and the merged frame. Global rules (protocol): x =
budget_frac on log2; oracle (proxy) dashed ceiling, random dotted floor,
no-eviction thin reference; no-pressure (frac=1.0) shown but never averaged into
the pressure band; example-cluster bootstrap bands; consistent policy colors;
PNG(300dpi)+PDF+caption per figure.

Primary config = beam_turn_pair × bm25 (middle granularity where gold≈non-gold
size, so the size lever is value-neutral → cleanest policy contrast; robustness in
findings.md). Headline metric = retrieval REACH (hit@k), because the deterministic
answer stage is degenerate on BEAM (Phase 0 F-A).
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "lib"))
import style  # noqa: E402
import frame  # noqa: E402
from beam_load import POLICIES, BUDGET_FRACS  # noqa: E402

style.apply()
TAB = HERE / "tables"
IT, RE, KNEE = "beam_turn_pair", "bm25", 0.25
PB = [0.1, 0.25, 0.5, 0.75]

met = pd.read_csv(TAB / "metrics_by_config.csv")
cei = pd.read_csv(TAB / "trace_ceilings.csv")
auc = pd.read_csv(TAB / "aucb_ranking.csv")
reg = pd.read_csv(TAB / "regret_by_budget.csv")
fun = pd.read_csv(TAB / "funnel_by_budget.csv")
cho = pd.read_csv(TAB / "cho_by_policy.csv")
fair = pd.read_csv(TAB / "fair_survival_metrics.csv")     # ANSWER-survival (compression-fair)
fair_auc = pd.read_csv(TAB / "fair_aucb_ranking.csv")
m = frame.build()

ORDER = ["compression", "lfu", "lru", "keep_smallest", "semantic_coverage",
         "semantic", "evict_smallest", "random"]


def _line(ax, met_name, itm=IT, ret=RE, policies=ORDER, band=True):
    sub = met[(met.itemiser == itm) & (met.retriever == ret) & (met.metric == met_name)]
    for pol in policies:
        p = sub[sub.policy == pol].sort_values("budget_frac")
        if p.empty:
            continue
        c = style.POLICY_COLOR[pol]
        ax.plot(p.budget_frac, p["mean"], "-o", color=c, label=style.POLICY_LABEL[pol], zorder=3)
        # hollow marker for the no-pressure control point (frac=1.0)
        np1 = p[p.budget_frac == 1.0]
        if not np1.empty:
            ax.plot(np1.budget_frac, np1["mean"], "o", mfc="white", mec=c, zorder=4)
        if band:
            ax.fill_between(p.budget_frac, p["lo"], p["hi"], color=c, alpha=0.12, zorder=1)


def _ceiling(ax, itm=IT):
    c = cei[cei.itemiser == itm]
    orc = c[c.ceiling == "union_wt"].sort_values("budget_frac")
    ax.plot(orc.budget_frac, orc.survival, "--", color=style.ORACLE_C, lw=1.4,
            label="oracle (proxy, write-time)", zorder=2)


# ── F1 — FAIR survival vs budget: ANSWER-survival (compression-fair) ──
def f1():
    fig, ax = plt.subplots(figsize=(6.2, 4.6))
    sub = fair[(fair.itemiser == IT) & (fair.retriever == RE) & (fair.metric == "ans_conj")]
    for pol in ORDER:
        p = sub[sub.policy == pol].sort_values("budget_frac")
        if p.empty:
            continue
        c = style.POLICY_COLOR[pol]
        ax.plot(p.budget_frac, p["mean"], "-o", color=c, label=style.POLICY_LABEL[pol], zorder=3)
        ax.fill_between(p.budget_frac, p["lo"], p["hi"], color=c, alpha=0.10, zorder=1)
    # compression's INFLATED id-survival (dashed) — the apples-to-oranges line we corrected
    idc = fair[(fair.itemiser == IT) & (fair.retriever == RE) &
               (fair.metric == "id_conj") & (fair.policy == "compression")].sort_values("budget_frac")
    ax.plot(idc.budget_frac, idc["mean"], "--", color=style.POLICY_COLOR["compression"], lw=1.3,
            alpha=0.8, label="compression id-survival (inflated)", zorder=2)
    ax.axhline(1.0, color=style.NOEVICT_C, lw=0.7)
    style.log2_budget_axis(ax); ax.set_ylabel("ANSWER-survival (localizable gold)")
    ax.set_title("F1 — FAIR survival: gold ANSWER survives forgetting")
    ax.legend(ncol=2, fontsize=6.5, loc="lower right")
    style.save(fig, "F1_survival",
        "F1. The COMPRESSION-FAIR survival metric: fraction of localizable gold whose "
        "ANSWER STRING is still recoverable from resident text (conjunctive), "
        "turn_pair/bm25, cluster-bootstrap bands. For pure-eviction policies this equals "
        "id-survival; for compression it is far lower than its id-survival (dashed green) "
        "because the answer is compressed away. On this fair metric compression's lead "
        "over LRU/LFU shrinks to within the bands — they are tied (F11). Caveat: "
        "localizable subset is small (~42 queries/config), so bands are wide.")


# ── F2 — HEADLINE: retrieval reach vs budget ──
def f2():
    fig, ax = plt.subplots(figsize=(6, 4.4))
    _line(ax, "reach")
    ax.axhline(m[(m.itemiser == IT) & (m.retriever == RE) & (m.budget_frac == 1.0)]["R_any"].mean(),
               color=style.NOEVICT_C, lw=0.7, ls="-")
    style.log2_budget_axis(ax); ax.set_ylabel("reach = hit@k (gold in top-k)")
    ax.set_title("F2 — HEADLINE: end-to-end retrieval reach vs budget")
    ax.legend(ncol=2, fontsize=7, loc="lower right")
    style.save(fig, "F2_reach",
        "F2. THE HEADLINE (retrieval reachability = gold reaches the reader's top-k), "
        "the furthest pipeline stage cleanly measured on BEAM (the deterministic answer "
        "stage is degenerate, Phase 0). beam_turn_pair/bm25, bootstrap bands, thin line "
        "= no-eviction ceiling (common to all policies). compression, LRU and LFU are "
        "tied at the top under pressure; compression's large survival edge (F1) does NOT "
        "convert — compressed text retrieves worse, so reach collapses to the LRU/LFU band.")


# ── F3 — oracle-normalized survival ratio ρ(B) ──
def f3():
    fig, ax = plt.subplots(figsize=(6, 4.4))
    orc = cei[(cei.itemiser == IT) & (cei.ceiling == "union_wt")].set_index("budget_frac")["survival"]
    sub = met[(met.itemiser == IT) & (met.retriever == RE) & (met.metric == "S_conj_i")]
    for pol in ORDER:
        p = sub[sub.policy == pol].sort_values("budget_frac")
        rho = [row["mean"] / orc[row.budget_frac] if orc[row.budget_frac] > 0 else np.nan
               for _, row in p.iterrows()]
        ax.plot(p.budget_frac, rho, "-o", color=style.POLICY_COLOR[pol],
                label=style.POLICY_LABEL[pol])
    ax.axhline(1.0, color=style.ORACLE_C, ls="--", lw=1.2)
    style.log2_budget_axis(ax); ax.set_ylabel("ρ = survival / oracle survival")
    ax.set_ylim(0, 1.6)
    ax.set_title("F3 — oracle-normalized survival ρ(B)")
    ax.legend(ncol=2, fontsize=7, loc="upper right")
    style.save(fig, "F3_oracle_ratio",
        "F3. Survival as a fraction of the write-time oracle (proxy) ceiling. Where "
        "ρ→1 the budget is sufficient and policies converge; the low-budget ordering is "
        "the result. ρ>1 for compression at 0.10 because compression beats the "
        "NO-compression write-time oracle by shrinking items (a different action class "
        "than the size oracle) — an honest sign the size-oracle is not compression's "
        "ceiling. Above 0.25 the oracle saturates (union fits), so ρ tracks raw survival.")


# ── F4 — loss waterfall at 3 budgets ──
def f4():
    budgets = [0.1, 0.25, 0.5]
    fig, axes = plt.subplots(1, 3, figsize=(11, 4.2), sharey=True)
    for ax, bf in zip(axes, budgets):
        f = fun[fun.budget_frac == bf].set_index("policy").reindex(ORDER)
        x = np.arange(len(ORDER))
        ls = f["lost_survival"].to_numpy()
        lr = f["lost_retrieval"].to_numpy()
        # reader loss (answered-wrong among reached+gradeable) scaled into remaining reach
        reached = f["reached"].to_numpy()
        rw = f["reader_wrong_given_reached_gradeable"].to_numpy() * reached
        ax.bar(x, ls, color="#B00020", label="lost @ survival")
        ax.bar(x, lr, bottom=ls, color="#E69F00", label="lost @ retrieval")
        ax.bar(x, rw, bottom=ls + lr, color="#999999", label="lost @ reader (of reached)")
        ax.set_xticks(x); ax.set_xticklabels(ORDER, rotation=60, ha="right", fontsize=7)
        ax.set_title(f"budget {bf:g}")
        if ax is axes[0]:
            ax.set_ylabel("fraction of queries lost")
    axes[0].legend(fontsize=7, loc="upper right")
    style.save(fig, "F4_waterfall",
        "F4. Where queries are lost, insert→retrieve→answer, at three budgets "
        "(turn_pair/bm25). Red = gold evicted; orange = survived but not retrieved; grey "
        "= reached the reader but answered wrong (deterministic grader, gradeable subset "
        "— note it is floored on BEAM, Phase 0, so this bar is a lower bound on reader "
        "loss). compression shrinks the red (survival) loss but grows the orange "
        "(retrieval) loss via compressed text; LRU/LFU carry survival loss on multi-gold "
        "queries. The grey reader bar dwarfs the rest — the BEAM answer format, not "
        "forgetting, is the terminal bottleneck.")


# ── F5 — regret decomposition (avoidable vs unavoidable survival) ──
def f5():
    fig, axes = plt.subplots(2, 4, figsize=(12, 6), sharex=True, sharey=True)
    r = reg[reg.retriever == RE]
    for ax, pol in zip(axes.ravel(), ORDER):
        p = r[r.policy == pol].sort_values("budget_frac")
        ax.fill_between(p.budget_frac, 0, p["unavoidable"], color="#CCCCCC", label="unavoidable (budget)")
        ax.fill_between(p.budget_frac, p["unavoidable"], p["unavoidable"] + p["avoidable"],
                        color=style.POLICY_COLOR[pol], alpha=0.75, label="avoidable (policy)")
        style.log2_budget_axis(ax)
        ax.set_title(style.POLICY_LABEL[pol], fontsize=8)
        ax.set_ylim(0, 1)
    axes[0, 0].legend(fontsize=7, loc="upper right")
    fig.suptitle("F5 — survival regret: avoidable (policy) over unavoidable (budget)", y=1.0)
    style.save(fig, "F5_regret",
        "F5. Total survival gap below perfect, split into unavoidable (grey — even the "
        "per-query oracle loses it at this budget) and avoidable (colored — the policy's "
        "fault), per policy, turn_pair/bm25. At 0.10 most of the gap is unavoidable "
        "(budget too small); by 0.25 the oracle is feasible so nearly all remaining gap "
        "is avoidable. compression's avoidable band is near-zero (it realizes the "
        "ceiling); random/evict_smallest are almost all avoidable.")


# ── F6 — policy × question_type heatmap at the knee (reach) ──
def f6():
    sub = m[(m.itemiser == IT) & (m.retriever == RE) & (m.budget_frac == KNEE)]
    qtypes = sorted(sub.question_type.unique())
    M = np.zeros((len(ORDER), len(qtypes)))
    ncnt = sub.drop_duplicates("query_id").groupby("question_type").size()
    for i, pol in enumerate(ORDER):
        for j, qt in enumerate(qtypes):
            M[i, j] = sub[(sub.policy == pol) & (sub.question_type == qt)]["R_any"].mean()
    fig, ax = plt.subplots(figsize=(10, 5))
    im = ax.imshow(M, cmap="magma", vmin=0, vmax=1, aspect="auto")
    ax.set_xticks(range(len(qtypes)))
    ax.set_xticklabels([f"{q}\n(n={ncnt.get(q,0)})" for q in qtypes], rotation=45, ha="right", fontsize=7)
    ax.set_yticks(range(len(ORDER))); ax.set_yticklabels([style.POLICY_LABEL[p] for p in ORDER], fontsize=8)
    for i in range(len(ORDER)):
        for j in range(len(qtypes)):
            ax.text(j, i, f"{M[i,j]:.2f}", ha="center", va="center", fontsize=6,
                    color="white" if M[i, j] < 0.55 else "black")
    fig.colorbar(im, label="reach = hit@k", fraction=0.025)
    ax.set_title(f"F6 — reach by policy × question type (budget {KNEE:g}, turn_pair/bm25)")
    style.save(fig, "F6_heatmap",
        "F6. Retrieval reach per policy × memory ability at the knee budget (0.25). "
        "event_ordering (mean 7.7 gold/query) and multi_session_reasoning punish every "
        "policy via conjunctive demand; knowledge_update / single-gold types are easier. "
        "compression/LRU/LFU lead broadly; the size baselines lag on the high-multiplicity "
        "columns. n per column annotated (small — read with F8 CIs).")


# ── F7 — C/H/O stacked bars at the knee ──
def f7():
    c = cho[cho.retriever == RE].set_index("policy").reindex(ORDER)
    fig, ax = plt.subplots(figsize=(7, 4.2))
    x = np.arange(len(ORDER))
    ax.bar(x, c["C"], color="#009E73", label="Correct")
    ax.bar(x, c["H"], bottom=c["C"], color="#B00020", label="Hallucinated (wrong)")
    ax.bar(x, c["O"], bottom=c["C"] + c["H"], color="#999999", label="Omitted (abstain)")
    ax.set_xticks(x); ax.set_xticklabels(ORDER, rotation=60, ha="right", fontsize=7)
    ax.set_ylabel("fraction of gradeable queries"); ax.set_ylim(0, 1)
    ax.set_title(f"F7 — C/H/O at budget {KNEE:g} (turn_pair/bm25, gradeable subset)")
    ax.legend(fontsize=8, loc="lower right")
    style.save(fig, "F7_cho",
        "F7. Three-way answer outcome on the gradeable subset (empty-reference types "
        "excluded) at 0.25. Correct is floored for all policies (deterministic grader vs "
        "BEAM's long-form answers, Phase 0) — so the discriminating signal is the H/O "
        "split: eviction-heavy policies (random, evict_smallest) push queries to Omitted "
        "(the reader abstains when gold is gone), while better-survival policies convert "
        "some Omitted into Hallucinated OR Correct. Wrong>right everywhere: BEAM answer "
        "quality is reader-bound, not forgetting-bound.")


# ── F8 — AUC-B ranking bar chart with bootstrap CIs ──
def f8():
    a = auc[(auc.itemiser == IT) & (auc.retriever == RE)].sort_values("aucb_reach")
    fig, ax = plt.subplots(figsize=(7, 4.4))
    y = np.arange(len(a))
    err = np.vstack([a["aucb_reach"] - a["aucb_reach_lo"], a["aucb_reach_hi"] - a["aucb_reach"]])
    ax.barh(y, a["aucb_reach"], xerr=err, color=[style.POLICY_COLOR[p] for p in a.policy],
            error_kw=dict(ecolor="#333", lw=1))
    ax.set_yticks(y); ax.set_yticklabels([style.POLICY_LABEL[p] for p in a.policy], fontsize=8)
    noev = m[(m.itemiser == IT) & (m.retriever == RE) & (m.budget_frac == 1.0)]["R_any"].mean()
    ax.axvline(noev, color=style.NOEVICT_C, ls="-", lw=0.8)
    ax.text(noev, -0.6, "no-eviction reach", fontsize=7, color=style.NOEVICT_C, ha="center")
    ax.set_xlabel("normalized AUC-B (reach over the 0.10–0.75 pressure band)")
    ax.set_title("F8 — end-to-end reach ranking (AUC-B, 95% cluster-bootstrap CI)")
    style.save(fig, "F8_aucb",
        "F8. The single-number ranking: AUC of retrieval reach over the log2 pressure "
        "band (0.10–0.75), turn_pair/bm25, 95% example-cluster-bootstrap CIs. compression, "
        "LRU and LFU form a top cluster whose CIs overlap — TIED within uncertainty "
        "(n=35). keep_smallest is a clear second tier; the semantic pair third; "
        "evict_smallest/random floor. 'Best' cannot be claimed within the top cluster.")


# ── F9 — mechanism: probe leakage (why LFU/LRU 'work') ──
def f9():
    import beam_events
    items, _, _ = beam_events.load_itemiser(IT)
    fig, ax = plt.subplots(figsize=(6, 4.2))
    bins = np.linspace(0, 40, 41)
    ax.hist(items[items.is_gold].access_count.clip(upper=40), bins=bins, density=True,
            alpha=0.6, color="#D55E00", label=f"gold (mean {items[items.is_gold].access_count.mean():.1f})")
    ax.hist(items[~items.is_gold].access_count.clip(upper=40), bins=bins, density=True,
            alpha=0.5, color="#0072B2", label=f"non-gold (mean {items[~items.is_gold].access_count.mean():.1f})")
    ax.set_xlabel("probe/query retrieval accesses per item (budget=1.0)")
    ax.set_ylabel("density"); ax.set_title("F9 — access-probe leakage onto gold")
    ax.legend()
    style.save(fig, "F9_probe_leakage",
        "F9. MECHANISM behind the pre-registration contradiction. The benchmark fires "
        "access-only probes using the real question text, so gold items are re-retrieved "
        "3.5× more than non-gold (mean 14.5 vs 4.2 accesses; Mann-Whitney p≈1e-211). LFU/"
        "LRU therefore see a frequency/recency signal that CORRELATES WITH GOLD even "
        "though gold is single-reference by scored-query links (Phase 1). Their survival "
        "advantage (F1) is an artifact of probe construction, not evidence that BEAM "
        "rewards recency — a caveat that travels with every LRU/LFU claim here.")


# ── F10 — mechanism: compression id-survival vs ANSWER-survival gap ──
def f10():
    sys.path.insert(0, str(HERE.parent / "src"))
    from forgetit.benchmark.grading import answer_in_text, is_answerable
    import json
    fracs = ["010pct", "025pct", "050pct", "075pct"]
    fmap = {"010pct": 0.1, "025pct": 0.25, "050pct": 0.5, "075pct": 0.75}
    fig, ax = plt.subplots(figsize=(6, 4.2))
    for pol, c in (("compression", "#009E73"), ("lru", "#CC79A7"), ("keep_smallest", "#0072B2")):
        xs, ans, ids = [], [], []
        for fr in fracs:
            path = (HERE.parent / f"artifacts/benchmark_full/beam/{IT}/retention/{pol}/{RE}/"
                    f"budget_{fr}/PlainTextReader/gold_answer_snapshot.jsonl")
            if not path.exists():
                continue
            rows = [json.loads(l) for l in open(path)]
            loc = [r for r in rows if is_answerable(r.get("query_id", ""), r.get("question_type", ""))
                   and r.get("original_text") and answer_in_text(r["answer"], r["original_text"])]
            if not loc:
                continue
            xs.append(fmap[fr])
            ans.append(np.mean([1.0 if (not r.get("was_evicted") and
                       answer_in_text(r["answer"], r.get("resident_text", ""))) else 0.0 for r in loc]))
            ids.append(np.mean([0.0 if r.get("was_evicted") else 1.0 for r in loc]))
        ax.plot(xs, ids, "--o", color=c, alpha=0.5, label=f"{pol} id-survival")
        ax.plot(xs, ans, "-o", color=c, label=f"{pol} ANSWER-survival")
    style.log2_budget_axis(ax); ax.set_ylabel("survival rate (localizable gold)")
    ax.set_title("F10 — compression: id-survival ≫ ANSWER-survival")
    ax.legend(fontsize=7, ncol=1, loc="lower right")
    style.save(fig, "F10_answer_survival",
        "F10. Why compression's survival lead (F1) overstates its value. For non-compress "
        "policies (LRU, keep_smallest) answer-survival == id-survival: if the item stays, "
        "the answer stays. compression keeps the item (dashed ≈1.0) but compresses the "
        "ANSWER STRING out of ~21% of items at 0.25 and ~47% at 0.10 (solid green). On "
        "answer-survival its lead over LRU shrinks from ~0.28 (id) to ~0.07 (answer) at "
        "0.25 — the honest end-to-end margin.")


# ── F11 — FAIR verdict: answer-survival AUC-B ranking with compression correction ──
def f11():
    a = fair_auc[(fair_auc.itemiser == IT) & (fair_auc.retriever == RE)].sort_values("aucb_ans")
    fig, ax = plt.subplots(figsize=(7.2, 4.6))
    y = np.arange(len(a))
    err = np.vstack([a["aucb_ans"] - a["aucb_ans_lo"], a["aucb_ans_hi"] - a["aucb_ans"]])
    ax.barh(y, a["aucb_ans"], xerr=err, color=[style.POLICY_COLOR[p] for p in a.policy],
            error_kw=dict(ecolor="#333", lw=1), zorder=3)
    # compression's inflated id-AUCB as a hollow marker → shows the correction
    for i, (_, r) in enumerate(a.iterrows()):
        if r.aucb_id - r.aucb_ans > 0.01:
            ax.plot(r.aucb_id, i, "D", mfc="white", mec=style.POLICY_COLOR[r.policy], ms=8, zorder=4)
            ax.annotate("", xy=(r.aucb_ans, i), xytext=(r.aucb_id, i),
                        arrowprops=dict(arrowstyle="->", color="#B00020", lw=1.3))
            ax.text(r.aucb_id + 0.01, i, "id-survival\n(inflated)", fontsize=6.5, va="center", color="#B00020")
    ax.set_yticks(y); ax.set_yticklabels([style.POLICY_LABEL[p] for p in a.policy], fontsize=8)
    ax.set_xlabel("normalized AUC-B (ANSWER-survival, 0.10–0.75 band)")
    ax.set_title("F11 — FAIR verdict: answer-survival ranking (95% cluster-bootstrap CI)")
    ax.set_xlim(0, 1.05)
    style.save(fig, "F11_fair_ranking",
        "F11. THE FAIR VERDICT. AUC-B of ANSWER-survival over the pressure band, "
        "turn_pair/bm25, 95% example-cluster-bootstrap CIs. Ranking compression on "
        "id-survival (hollow diamond, red arrow) would put it far ahead; correcting for "
        "answers compressed away drops it to 0.78 [0.70,0.86], whose CI OVERLAPS LRU "
        "0.68 [0.57,0.78] and LFU 0.63 [0.53,0.72] — the top three are TIED within "
        "uncertainty (n=35, ~42 localizable queries/config). keep-smallest / semantic / "
        "random form a clearly lower tier. No single 'best' policy can be declared; "
        "compression's only separable edge (id-survival) is the one answers don't keep.")


# ── F12 — why content-aware (semantic) eviction buys nothing ──
def f12():
    import beam_eviction, beam_events
    ev = beam_eviction.build(IT, RE)
    band = ev[ev.budget_frac.isin(PB)].groupby("policy")["gold_shed_rate"].mean()
    items, _, _ = beam_events.load_itemiser(IT)
    base = items.is_gold.mean()
    rnd = band["random"]
    order = band.sort_values(ascending=False).index.tolist()   # worst (most gold shed) first

    fig, (axa, axb) = plt.subplots(1, 2, figsize=(12, 4.6))

    # (a) gold-shed rate per policy vs the blind/random baseline
    y = np.arange(len(order))
    axa.barh(y, [band[p] for p in order], color=[style.POLICY_COLOR[p] for p in order], zorder=3)
    axa.axvline(rnd, color="#B00020", ls="--", lw=1.3, label=f"random (blind) = {rnd:.3f}")
    axa.axvline(base, color="#666", ls=":", lw=1.2, label=f"gold base rate = {base:.3f}")
    axa.set_yticks(y); axa.set_yticklabels([style.POLICY_LABEL[p] for p in order], fontsize=8)
    axa.set_xlabel("gold-shed rate (fraction of evictions that were GOLD) — lower is better")
    axa.set_title("(a) does the policy protect gold better than chance?")
    axa.legend(fontsize=7, loc="lower right")
    # call out the semantic policies
    for p in ("semantic", "semantic_coverage"):
        i = order.index(p)
        axa.annotate("≈ blind" if p == "semantic" else "= keep-smallest\n(size, not content)",
                     xy=(band[p], i), xytext=(band[p] + 0.012, i), fontsize=6.5, va="center", color="#B00020")

    # (b) fair answer-survival AUC-B: semantic vs its size fallback vs random (CIs)
    fa = fair_auc[(fair_auc.itemiser == IT) & (fair_auc.retriever == RE)]
    picks = ["semantic_coverage", "keep_smallest", "semantic", "random"]
    fam = pd.read_csv(TAB / "fair_aucb_ranking.csv")
    fam = fam[(fam.itemiser == IT) & (fam.retriever == RE)].set_index("policy")
    yy = np.arange(len(picks))
    vals = [fam.loc[p, "aucb_ans"] for p in picks]
    err = np.vstack([[fam.loc[p, "aucb_ans"] - fam.loc[p, "aucb_ans_lo"] for p in picks],
                     [fam.loc[p, "aucb_ans_hi"] - fam.loc[p, "aucb_ans"] for p in picks]])
    axb.barh(yy, vals, xerr=err, color=[style.POLICY_COLOR[p] for p in picks],
             error_kw=dict(ecolor="#333", lw=1), zorder=3)
    axb.set_yticks(yy); axb.set_yticklabels([style.POLICY_LABEL[p] for p in picks], fontsize=8)
    axb.set_xlabel("fair answer-survival AUC-B (95% cluster-bootstrap CI)")
    axb.set_title("(b) content signal vs its size fallback — no separation")
    axb.set_xlim(0, 0.7)
    style.save(fig, "F12_semantic_null",
        "F12. Why content-aware (semantic) eviction buys nothing on BEAM. (a) Gold-shed "
        "rate: the nearest-pair 'semantic' policy sheds gold at 0.110 — indistinguishable "
        "from the blind random baseline (0.118, red dashed) and the 0.109 gold base rate "
        "(grey dotted); 'semantic-coverage' (0.097) exactly matches keep-smallest, so its "
        "only protection comes from the SIZE fallback, not the coverage signal. The "
        "redundancy/coverage signal carries no information about which items are gold. "
        "(b) End-to-end: neither semantic policy separates from the trivial size baseline "
        "on fair answer-survival (overlapping CIs); nearest-pair even trails random. "
        "Redundancy ≠ disposability here — evicting a near-duplicate sheds gold as often "
        "as noise, because answer-bearing turns have semantic twins too.")


if __name__ == "__main__":
    for fn in (f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12):
        fn()
        print("wrote", fn.__name__)
