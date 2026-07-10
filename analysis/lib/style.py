"""Shared plotting style for the BEAM analysis — consistent policy colors, a
colorblind-safe palette, log2 budget axis helpers, and a save-as-png+pdf+caption
helper. One import so every figure reads as one system (protocol Phase 3)."""
from __future__ import annotations

from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np

FIG_DIR = Path(__file__).resolve().parents[1] / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

# Okabe-Ito colorblind-safe palette, assigned to policy ROLES (stable across ALL
# figures). Oracle = black dashed, random = grey dotted, no-eviction = thin grey.
POLICY_COLOR = {
    "keep_smallest":    "#0072B2",  # blue   — size-aware (evict largest)
    "evict_smallest":   "#56B4E9",  # sky    — size-adversarial (evict smallest)
    "compression":      "#009E73",  # green  — LLM compress-to-pack
    "semantic":         "#E69F00",  # orange — nearest-pair redundancy
    "semantic_coverage":"#D55E00",  # vermil — marginal coverage
    "lru":              "#CC79A7",  # pink   — recency
    "lfu":              "#8000FF",  # purple — frequency
    "random":           "#999999",  # grey   — stochastic floor
}
POLICY_LABEL = {
    "keep_smallest": "keep-smallest (evict largest)",
    "evict_smallest": "evict-smallest",
    "compression": "LLM compression",
    "semantic": "semantic (nearest-pair)",
    "semantic_coverage": "semantic-coverage",
    "lru": "LRU", "lfu": "LFU", "random": "random",
}
ORACLE_C = "#000000"
NOEVICT_C = "#666666"


def apply():
    mpl.rcParams.update({
        "figure.dpi": 120, "savefig.dpi": 300,
        "font.size": 10, "axes.titlesize": 11, "axes.labelsize": 10,
        "axes.spines.top": False, "axes.spines.right": False,
        "axes.grid": True, "grid.alpha": 0.25, "grid.linewidth": 0.6,
        "legend.frameon": False, "legend.fontsize": 8,
        "lines.linewidth": 1.8, "lines.markersize": 5,
    })


def log2_budget_axis(ax, fracs=(0.1, 0.25, 0.5, 0.75, 1.0)):
    ax.set_xscale("log", base=2)
    ax.set_xticks(list(fracs))
    ax.set_xticklabels([f"{f:g}" for f in fracs])
    ax.set_xlabel("budget fraction  (B / footprint,  log2)")


def save(fig, name: str, caption: str):
    """Save PNG (300dpi) + PDF + a .txt caption file (protocol Phase 3)."""
    fig.tight_layout()
    for ext in ("png", "pdf"):
        fig.savefig(FIG_DIR / f"{name}.{ext}", bbox_inches="tight")
    (FIG_DIR / f"{name}.caption.txt").write_text(caption.strip() + "\n")
    plt.close(fig)


def gini(x: np.ndarray) -> float:
    x = np.sort(np.asarray(x, dtype=float))
    n = x.size
    if n == 0 or x.sum() == 0:
        return float("nan")
    return float((2 * np.arange(1, n + 1) - n - 1).dot(x) / (n * x.sum()))
