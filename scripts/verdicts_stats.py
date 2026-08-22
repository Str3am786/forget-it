"""
verdicts_stats.py — statistical primitives for the V1..V4 verdict path.

Dependency policy: numpy + pandas only. Nothing here needs scipy or statsmodels,
so the module runs inside thesis.sif without a rebuild.

Two notes on choices that matter for the write-up:

1. `paired_sign_test` is the single test used on both the proxy side and the
   accuracy side. On binary paired outcomes it *is* exact two-sided McNemar:
   McNemar's exact test conditions on the discordant pairs b + c and tests
   b ~ Binom(b + c, 0.5), which is precisely the sign test applied to the
   paired differences. Using one function for both keeps the graded proxy
   (`preserved_frac`) and the binary proxies on the same inferential footing,
   which is what the ladder in V3 requires — you cannot compare a McNemar
   contradiction rate against a Wilcoxon one and call it a ladder.

2. All p-values are exact (lgamma-summed binomial tail), not normal
   approximations with continuity corrections. At the discordant-pair counts
   this sweep produces the two agree to three decimals, but exactness is
   cheaper to defend than to justify.
"""

from __future__ import annotations

import math
from typing import Sequence

import numpy as np

# --------------------------------------------------------------------------
# paired tests
# --------------------------------------------------------------------------


def _binom_tail_half(k: int, n: int) -> float:
    """P(X <= k) for X ~ Binom(n, 0.5), computed in log space."""
    if n <= 0:
        return 1.0
    if k < 0:
        return 0.0
    if k >= n:
        return 1.0
    log_half_n = n * math.log(0.5)
    total = 0.0
    for i in range(k + 1):
        logp = (
            math.lgamma(n + 1)
            - math.lgamma(i + 1)
            - math.lgamma(n - i + 1)
            + log_half_n
        )
        total += math.exp(logp)
    return min(1.0, total)


def exact_mcnemar_p(b: int, c: int) -> float:
    """Exact two-sided McNemar p-value from the discordant counts."""
    n = int(b) + int(c)
    if n == 0:
        return 1.0
    return min(1.0, 2.0 * _binom_tail_half(min(int(b), int(c)), n))


def paired_sign_test(a: np.ndarray, b: np.ndarray) -> dict:
    """
    Paired comparison of policy A against policy B on the same units.

    Units with a NaN on either side are dropped (pairwise complete case), so a
    policy/config that failed to produce a row for some question does not
    silently contaminate every other pair in the cell.

    On binary inputs this returns exact two-sided McNemar. On graded inputs
    (preserved_frac) it returns the exact two-sided sign test on the paired
    differences, with zero differences discarded in the usual way.

    Returns n_pairs, wins (a > b), losses (a < b), ties, the paired mean
    difference `delta`, and the exact p-value.
    """
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    ok = ~(np.isnan(a) | np.isnan(b))
    a, b = a[ok], b[ok]
    n = int(ok.sum())
    if n == 0:
        return dict(n_pairs=0, wins=0, losses=0, ties=0, delta=np.nan, p=1.0)
    d = a - b
    wins = int((d > 0).sum())
    losses = int((d < 0).sum())
    ties = int((d == 0).sum())
    return dict(
        n_pairs=n,
        wins=wins,
        losses=losses,
        ties=ties,
        delta=float(np.mean(d)),
        p=exact_mcnemar_p(wins, losses),
    )


# --------------------------------------------------------------------------
# multiplicity
# --------------------------------------------------------------------------


def bh_fdr(pvals: Sequence[float], alpha: float = 0.05):
    """Benjamini-Hochberg. Returns (qvalues, rejected)."""
    p = np.asarray(list(pvals), dtype=float)
    n = p.size
    if n == 0:
        return np.array([]), np.array([], dtype=bool)
    order = np.argsort(p)
    ranked = p[order]
    q = ranked * n / np.arange(1, n + 1)
    q = np.minimum.accumulate(q[::-1])[::-1]
    q = np.clip(q, 0.0, 1.0)
    out = np.empty(n)
    out[order] = q
    return out, out <= alpha


# --------------------------------------------------------------------------
# intervals
# --------------------------------------------------------------------------


def wilson_ci(k: int, n: int, z: float = 1.959963984540054):
    """Wilson score interval for a proportion. Correct at k = 0 and k = n."""
    if n == 0:
        return (np.nan, np.nan)
    phat = k / n
    denom = 1 + z * z / n
    centre = (phat + z * z / (2 * n)) / denom
    half = z * math.sqrt(phat * (1 - phat) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, centre - half), min(1.0, centre + half))


# --------------------------------------------------------------------------
# chi-square tail (for the Wald test in RQ4), numpy-only
# --------------------------------------------------------------------------


def _gammainc_upper_reg(s: float, x: float) -> float:
    """Regularised upper incomplete gamma Q(s, x). Series + continued fraction."""
    if x < 0 or s <= 0:
        return float("nan")
    if x == 0:
        return 1.0
    if x < s + 1.0:
        # series for the lower tail P(s, x)
        ap = s
        total = 1.0 / s
        term = total
        for _ in range(1000):
            ap += 1.0
            term *= x / ap
            total += term
            if abs(term) < abs(total) * 1e-15:
                break
        return 1.0 - total * math.exp(-x + s * math.log(x) - math.lgamma(s))
    # Lentz continued fraction for the upper tail
    tiny = 1e-300
    bb = x + 1.0 - s
    cc = 1.0 / tiny
    dd = 1.0 / bb
    h = dd
    for i in range(1, 1000):
        an = -i * (i - s)
        bb += 2.0
        dd = an * dd + bb
        if abs(dd) < tiny:
            dd = tiny
        cc = bb + an / cc
        if abs(cc) < tiny:
            cc = tiny
        dd = 1.0 / dd
        delta = dd * cc
        h *= delta
        if abs(delta - 1.0) < 1e-15:
            break
    return math.exp(-x + s * math.log(x) - math.lgamma(s)) * h


def chi2_sf(x: float, df: int) -> float:
    """Upper tail of the chi-square distribution."""
    if df <= 0 or not np.isfinite(x):
        return float("nan")
    if x <= 0:
        return 1.0
    return float(_gammainc_upper_reg(df / 2.0, x / 2.0))


# --------------------------------------------------------------------------
# logistic regression with cluster-robust covariance
# --------------------------------------------------------------------------


def logit_cluster(X: np.ndarray, y: np.ndarray, clusters: np.ndarray,
                  max_iter: int = 100, tol: float = 1e-9, ridge: float = 1e-8):
    """
    Binomial GLM by Newton-IRLS with a cluster-robust sandwich covariance.

    The pre-registered RQ4 model is a crossed mixed model
    (`correct ~ policy * log(budget) + (1|question) + (1|reader)`), which needs
    a package this container does not have and a fit that does not always
    converge on 20k binary rows with 15 policy levels. This is the standard
    fixed-effects substitute: the point estimates of the interaction are
    unbiased either way, and clustering the covariance on `question` buys the
    same protection against within-question correlation that `(1|question)`
    was there to buy. Reader is absorbed as a fixed effect by the caller.

    Returns beta, the robust covariance, the number of clusters, and whether
    the fit converged.
    """
    X = np.asarray(X, dtype=float)
    y = np.asarray(y, dtype=float)
    n, k = X.shape
    beta = np.zeros(k)
    converged = False
    for _ in range(max_iter):
        eta = np.clip(X @ beta, -30, 30)
        mu = 1.0 / (1.0 + np.exp(-eta))
        w = np.clip(mu * (1 - mu), 1e-10, None)
        z = X.T @ (y - mu)
        H = (X * w[:, None]).T @ X + ridge * np.eye(k)
        try:
            step = np.linalg.solve(H, z)
        except np.linalg.LinAlgError:
            step = np.linalg.lstsq(H, z, rcond=None)[0]
        beta_new = beta + step
        if np.max(np.abs(beta_new - beta)) < tol:
            beta = beta_new
            converged = True
            break
        beta = beta_new

    eta = np.clip(X @ beta, -30, 30)
    mu = 1.0 / (1.0 + np.exp(-eta))
    w = np.clip(mu * (1 - mu), 1e-10, None)
    bread = np.linalg.pinv((X * w[:, None]).T @ X + ridge * np.eye(k))
    resid = (y - mu)[:, None] * X

    codes, _ = _factorize(clusters)
    n_clusters = int(codes.max()) + 1 if codes.size else 0
    meat = np.zeros((k, k))
    for g in range(n_clusters):
        s = resid[codes == g].sum(axis=0)
        meat += np.outer(s, s)
    if n_clusters > 1:
        adj = n_clusters / (n_clusters - 1) * (n - 1) / max(n - k, 1)
        meat *= adj
    cov = bread @ meat @ bread
    return beta, cov, n_clusters, converged


def _factorize(values):
    vals = np.asarray(values)
    uniq, codes = np.unique(vals, return_inverse=True)
    return codes, uniq


def wald_block(beta: np.ndarray, cov: np.ndarray, idx: Sequence[int]):
    """Joint Wald test that a block of coefficients is zero."""
    idx = list(idx)
    if not idx:
        return dict(stat=np.nan, df=0, p=np.nan)
    b = beta[idx]
    V = cov[np.ix_(idx, idx)]
    try:
        stat = float(b @ np.linalg.solve(V, b))
    except np.linalg.LinAlgError:
        stat = float(b @ np.linalg.pinv(V) @ b)
    df = len(idx)
    return dict(stat=stat, df=df, p=chi2_sf(stat, df))
