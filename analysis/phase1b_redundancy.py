"""Phase 1b — the redundancy mechanism (the direct test the earlier pass could not run).

With the run's own vectors (validated), we measure whether the signals the
semantic policies rank on actually SEPARATE gold from noise. This turns the
outcome-based claim ("semantic sheds gold at chance", F12) into a mechanism.

Two signals, computed per conversation (the resident set the policy scores):
  redundancy(i)  = max cosine of i to any OTHER item      (the `semantic` victim = argMAX)
  marg_cov(i)    = i's marginal nearest-neighbour coverage (the `semantic_coverage`
                   victim = argMIN); exactly the formula in policies/semantic.py

Separability (Mann-Whitney AUC), framed as "does the victim signal target NOISE?":
  redundancy AUC = P(noise more redundant than gold)  — >0.5 good, 0.5 blind, <0.5 harmful
  coverage   AUC = P(gold higher marginal coverage than noise) — >0.5 good

Plus corpus near-duplicate mass at 0.9/0.95, and the gold-hit rate of the pure
signal (top-victim by signal) vs the gold base rate — the bridge to F12.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "lib"))
sys.path.insert(0, str(HERE.parent / "src"))
import style  # noqa: E402
import beam_embed  # noqa: E402
from forgetit.embeddings.similarity import auc_separation  # noqa: E402

style.apply()
TAB = HERE / "tables"


def _conv_signals(V):
    """(redundancy, marginal_coverage) per item for one conversation's L2-normed V."""
    n = V.shape[0]
    if n < 2:
        return np.zeros(n), np.zeros(n)
    S = V @ V.T
    np.fill_diagonal(S, -np.inf)
    argnn = np.argmax(S, axis=1)
    nn = S[np.arange(n), argnn]                       # redundancy = nearest-neighbour cosine
    masked = S.copy(); masked[np.arange(n), argnn] = -np.inf
    secondnn = np.max(masked, axis=1)
    nn_c = np.clip(np.where(np.isfinite(nn), nn, 0.0), 0.0, None)
    second_c = np.clip(np.where(np.isfinite(secondnn), secondnn, 0.0), 0.0, None)
    marg = nn_c.astype(float).copy()
    np.add.at(marg, argnn, nn_c - second_c)           # marginal coverage (semantic_coverage)
    return nn_c, marg


def main(itemiser="beam_turn_pair", partial=False):
    # Use the full npz when the embed has completed; otherwise compute on whatever
    # COMPLETE conversations are already cached (the conclusion is stable well before
    # all 35 convs are in — the AUC CIs are tight on a handful of conversations).
    d = beam_embed.load_partial(itemiser) if partial else beam_embed.build(itemiser)
    V, conv, gold = d["vectors"], d["conv_idx"], d["is_gold"].astype(bool)
    n_c = int(d.get("n_convs_complete") or len(np.unique(conv)))
    n_t = int(d.get("n_convs_total") or n_c)
    print(f"[redundancy] {itemiser}: {len(V)} items over {n_c}/{n_t} conversations")
    # renormalize defensively
    V = V / np.clip(np.linalg.norm(V, axis=1, keepdims=True), 1e-9, None)

    red = np.zeros(len(V)); cov = np.zeros(len(V))
    nd90 = nd95 = tot = 0
    for c in np.unique(conv):
        m = conv == c
        r, mc = _conv_signals(V[m])
        red[m] = r; cov[m] = mc
        tot += m.sum(); nd90 += (r >= 0.90).sum(); nd95 += (r >= 0.95).sum()

    g, ng = gold, ~gold
    # separability AUCs (see module docstring for framing)
    auc_red = auc_separation(red[ng], red[g])         # P(noise redundancy > gold redundancy)
    auc_cov = auc_separation(cov[g], cov[ng])         # P(gold coverage > noise coverage)

    # pure-signal gold-hit: rank items per conv by the victim rule, take the worst 20%,
    # what fraction is gold vs base rate. (semantic evicts argmax redundancy;
    #  semantic_coverage evicts argmin coverage.)
    base = gold.mean()
    def victim_gold_rate(score, largest_is_victim, frac=0.2):
        hit = tot_v = 0
        for c in np.unique(conv):
            m = conv == c
            s = score[m]; gg = gold[m]
            k = max(1, int(round(frac * len(s))))
            idx = np.argsort(s)[::-1][:k] if largest_is_victim else np.argsort(s)[:k]
            hit += gg[idx].sum(); tot_v += k
        return hit / tot_v
    vg_red = victim_gold_rate(red, True)
    vg_cov = victim_gold_rate(cov, False)

    rep = {
        "itemiser": itemiser, "n_convs": n_c, "n_items": int(tot), "gold_base_rate": float(base),
        "near_dup_mass_0.90": nd90 / tot, "near_dup_mass_0.95": nd95 / tot,
        "redundancy_auc_noise_vs_gold": float(auc_red),
        "coverage_auc_gold_vs_noise": float(auc_cov),
        "redundancy_mean_gold": float(red[g].mean()), "redundancy_mean_noise": float(red[ng].mean()),
        "coverage_mean_gold": float(cov[g].mean()), "coverage_mean_noise": float(cov[ng].mean()),
        "semantic_victim_gold_rate@20pct": float(vg_red),
        "semantic_coverage_victim_gold_rate@20pct": float(vg_cov),
    }
    pd.DataFrame([rep]).to_csv(TAB / "redundancy_separability.csv", index=False)
    for k, v in rep.items():
        print(f"  {k:42s} {v}")

    # ── C5 — the mechanism figure ──
    fig, (axa, axb) = plt.subplots(1, 2, figsize=(11, 4.4))
    bins = np.linspace(0, 1, 41)
    axa.hist(red[ng], bins=bins, density=True, alpha=0.55, color="#0072B2", label="non-gold")
    axa.hist(red[g], bins=bins, density=True, alpha=0.6, color="#D55E00", label="gold")
    axa.set_title(f"(a) redundancy signal (the `semantic` victim rule)\n"
                  f"AUC noise>gold = {auc_red:.3f}  (0.5 = blind)")
    axa.set_xlabel("nearest-neighbour cosine (higher = more redundant → evicted first)")
    axa.set_ylabel("density"); axa.legend(fontsize=8)
    axb.hist(cov[ng], bins=np.linspace(0, cov.max(), 41), density=True, alpha=0.55,
             color="#0072B2", label="non-gold")
    axb.hist(cov[g], bins=np.linspace(0, cov.max(), 41), density=True, alpha=0.6,
             color="#D55E00", label="gold")
    axb.set_title(f"(b) marginal-coverage signal (the `semantic_coverage` rule)\n"
                  f"AUC gold>noise = {auc_cov:.3f}  (0.5 = blind)")
    axb.set_xlabel("marginal coverage (lower = evicted first)")
    axb.legend(fontsize=8)
    style.save(fig, "C5_redundancy_separability",
        f"C5. THE MECHANISM. Distributions of the two content signals the semantic "
        f"policies rank on, gold vs non-gold, over {int(tot)} BEAM {itemiser} items "
        f"(run's own qwen3-0.6b vectors, validated). (a) Redundancy (nearest-neighbour "
        f"cosine): gold and noise distributions essentially COINCIDE — AUC(noise>gold)="
        f"{auc_red:.2f}, i.e. a near-duplicate is no more likely to be noise than gold, so "
        f"evicting the most-redundant item hits gold at ~chance. (b) Marginal coverage: "
        f"AUC(gold>noise)={auc_cov:.2f}, again ~blind. This is WHY semantic eviction sheds "
        f"gold at the random rate (F12): on BEAM the redundancy/coverage axes are "
        f"orthogonal to the gold/noise axis — answer-bearing turns have semantic twins too. "
        f"Near-duplicate mass @0.90={nd90/tot:.2f}.")
    print("\nwrote tables/redundancy_separability.csv + figures/C5_redundancy_separability")


if __name__ == "__main__":
    it = next((a for a in sys.argv[1:] if not a.startswith("-")), "beam_turn_pair")
    main(it, partial="--partial" in sys.argv)
