"""gold_answer_preserved over a finished campaign tree.

WHAT THIS MEASURES, and why it is not gold_id_retained.

    gold_id_retained     the gold identifier is in the resident set.
                         Set intersection. A compressed item is still resident
                         whatever its rewrite left behind, so a compression
                         policy saturates this metric by construction and it
                         carries no information about whether the answer
                         survived.

    gold_answer_preserved   R_g, the text still resident at final store state,
                         still supports the answer. Eviction gives an empty
                         R_g and scores zero, so eviction damage and rewrite
                         damage land on the same footing.

SCOPE. A string measure can only speak about gold items whose ORIGINAL text
contains the answer. Scoring R_g alone says nothing: an untouched copy of a
non extractive original scores the same as a husk. So O_g is graded too and
used as the denominator, and the extractive share is reported beside every
number rather than left implicit.

VALIDATION, built in and non negotiable. Policies that never rewrite text
must score preserved == retained EXACTLY on the extractive subset. A
disagreement there is a bug in the capture or the join, not a fact about
compression, and the script says STOP rather than printing a number.

THREE COLUMNS, not one:

    preserved_binary   answer_in_text(answer, R_g). The headline.
    preserved_recall   fraction of the answer's content tokens present in R_g.
                       Graded: a summary keeping "45 minute commute" against
                       an answer of "45 minutes each way" scores partial
                       rather than zero. Deterministic, no judge, and it
                       degrades gracefully where containment is brittle.
    byte_ratio         resident_bytes / orig_bytes. Compression severity as a
                       covariate, so preservation can be read against how
                       hard the item was squeezed.

Usage:
    python scripts/gold_preservation.py <run-or-subset-dir> [--out report/]
    python scripts/gold_preservation.py <dir> --by itemiser budget policy
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import csv
import json
import os
import re
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

try:
    from forgetit.benchmark.grading import answer_in_text
except Exception:                                   # keep the script runnable
    def answer_in_text(answer: str, text: str) -> bool:
        n = lambda s: re.sub(r"[^a-z0-9 ]+", " ", (s or "").lower())
        n = lambda s, _n=n: re.sub(r"\s+", " ", _n(s)).strip()
        a, t = n(answer), n(text)
        return bool(a) and a in t

GOLD_SNAPSHOT = "gold_answer_snapshot.jsonl"

# LoCoMo's adversarial stratum has no answer to preserve, so including it
# would drag every policy's preservation down by a constant and tell you
# nothing about forgetting.
ADVERSARIAL = re.compile(r"adversarial|unanswerable", re.I)

STOPWORDS = {"the", "a", "an", "of", "in", "on", "at", "to", "for", "and",
             "or", "is", "was", "were", "be", "been", "it", "its", "as",
             "with", "by", "that", "this", "each", "his", "her", "their"}


def norm(s: str) -> str:
    s = re.sub(r"[^a-z0-9 ]+", " ", (s or "").lower())
    return re.sub(r"\s+", " ", s).strip()


def content_tokens(s: str) -> list[str]:
    return [t for t in norm(s).split() if t not in STOPWORDS]


def token_recall(answer: str, text: str) -> float:
    """Fraction of the answer's content tokens present in the text.

    Graded companion to containment. Not a semantic measure: it will not
    credit a genuine paraphrase that shares no tokens. That gap is what an
    LLM judge would eventually cover, and the right way to introduce one is
    as a SECOND column calibrated against this on the extractive subset,
    never as a replacement, because a judge cannot give the exact equality
    that validates the instrument.
    """
    toks = content_tokens(answer)
    if not toks:
        return float("nan")
    have = set(content_tokens(text))
    return sum(t in have for t in toks) / len(toks)


def parse_config(run_dir: Path, root: Path) -> dict:
    """<workload>/<itemiser>/retention/<policy>/<retriever>/<budget>/none
       <workload>/<itemiser>/floor/<arm>/none"""
    parts = run_dir.resolve().parts
    out = {"config": "/".join(parts[-7:]), "workload": "", "itemiser": "",
           "family": "", "policy": "", "retriever": "", "budget": ""}
    for anchor in ("retention", "floor", "compression"):
        if anchor in parts:
            i = parts.index(anchor)
            out["family"] = anchor
            out["workload"] = parts[i - 2] if i >= 2 else ""
            out["itemiser"] = parts[i - 1] if i >= 1 else ""
            tail = parts[i + 1:]
            out["config"] = "/".join(parts[i - 2:])
            if anchor == "retention" and len(tail) >= 3:
                out["policy"], out["retriever"], out["budget"] = tail[0], tail[1], tail[2]
            elif tail:
                out["policy"] = tail[0]
                out["budget"] = "floor"
            break
    return out


def find_run_dirs(root: Path) -> list[Path]:
    """followlinks=True so a curated subset built from symlinks is visible."""
    if (root / GOLD_SNAPSHOT).is_file():
        return [root]
    out = set()
    for dirpath, _d, filenames in os.walk(root, followlinks=True):
        if GOLD_SNAPSHOT in filenames:
            out.add(Path(dirpath))
    return sorted(out)


def load_one(run_dir: Path, root: Path) -> list[dict]:
    meta = parse_config(run_dir, root)
    rows = []
    with (run_dir / GOLD_SNAPSHOT).open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            r = json.loads(line)
            if ADVERSARIAL.search(r.get("question_type", "") or ""):
                continue
            ans = r.get("answer", "")
            o_text = r.get("original_text", "") or ""
            evicted = bool(r.get("was_evicted"))
            r_text = "" if evicted else (r.get("resident_text", "") or "")
            ob, rb = r.get("orig_bytes") or 0, r.get("resident_bytes") or 0
            rows.append({
                **meta,
                "query_id": r.get("query_id", ""),
                "gold_id": r.get("gold_id", ""),
                "question_type": r.get("question_type", ""),
                "was_evicted": evicted,
                "was_compressed": bool(r.get("was_compressed")),
                # O_g supports the answer: the denominator. Everything below
                # is only meaningful where this is True.
                "orig_supports": answer_in_text(ans, o_text),
                "res_supports": (False if evicted else answer_in_text(ans, r_text)),
                "recall": 0.0 if evicted else token_recall(ans, r_text),
                "orig_recall": token_recall(ans, o_text),
                "byte_ratio": (rb / ob) if ob else float("nan"),
            })
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("root", type=Path)
    ap.add_argument("--out", type=Path, default=REPO / "report")
    ap.add_argument("--by", nargs="*",
                    default=["workload", "itemiser", "budget", "policy"])
    ap.add_argument("--workers", type=int, default=16)
    args = ap.parse_args()

    dirs = find_run_dirs(args.root)
    if not dirs:
        print(f"no {GOLD_SNAPSHOT} under {args.root}", file=sys.stderr)
        return 2
    print(f"{len(dirs)} config dir(s)")

    rows: list[dict] = []
    with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
        for chunk in ex.map(lambda d: load_one(d, args.root), dirs):
            rows.extend(chunk)
    snap = pd.DataFrame(rows)
    print(f"{len(snap):,} gold item rows")

    # ── scope ──────────────────────────────────────────────────────────
    cover = snap.groupby(["workload", "itemiser"]).orig_supports.agg(["mean", "size"])
    print("\nextractive share — gold items whose ORIGINAL text contains the answer")
    print(cover.round(3).to_string())
    print("  Everything below is computed on this subset only. Rows outside it\n"
          "  are out of scope for a string measure, not failures.\n")

    ext = snap[snap.orig_supports].copy()
    if ext.empty:
        print("STOP — no extractive gold items; containment cannot measure "
              "anything here.", file=sys.stderr)
        return 1

    # ── collapse per query: ANY gold item preserved ────────────────────
    key = [c for c in args.by if c in ext.columns] + ["query_id"]
    perq = ext.groupby(key, as_index=False).agg(
        preserved=("res_supports", "max"),
        recall=("recall", "max"),
        byte_ratio=("byte_ratio", "min"),
        any_compressed=("was_compressed", "max"),
        all_evicted=("was_evicted", "min"),
        n_gold=("gold_id", "nunique"),
    )

    # ── join gold_id_retained from results.csv ─────────────────────────
    ret_rows = []
    for d in dirs:
        f = d / "results.csv"
        if not f.is_file():
            continue
        meta = parse_config(d, args.root)
        with f.open(newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                col = r.get("gold_id_retained", r.get("gold_survived", ""))
                ret_rows.append({**meta, "query_id": r.get("query_id", ""),
                                 "retained": str(col).strip().lower() in ("true", "1")})
    j = perq
    if ret_rows:
        ret = pd.DataFrame(ret_rows).groupby(key, as_index=False).retained.max()
        j = perq.merge(ret, on=key, how="inner")
        print(f"joined {len(j):,} (config, question) rows on the extractive subset")

    # ── VALIDATION: no rewrite means preserved == retained, exactly ────
    if "retained" in j.columns:
        nc = j[(j.any_compressed == 0)]
        if len(nc):
            grp = "policy" if "policy" in nc.columns else key[0]
            d = (nc.groupby(grp)
                   .apply(lambda g: (g.retained != g.preserved).mean(),
                          include_groups=False).round(4))
            print("\nnon rewriting policies: preserved must equal retained EXACTLY")
            print(d.to_string())
            if float(d.max()) > 0.01:
                print("\n  STOP. These policies never rewrite text, so on the "
                      "extractive subset preservation must equal retention.\n"
                      "  A disagreement is a bug in the capture or the join, "
                      "not a fact about compression.", file=sys.stderr)
                return 1
            print("  Equal, as required. Any gap below is the rewrite.")

    # ── the table ──────────────────────────────────────────────────────
    gb = [c for c in args.by if c in j.columns]
    agg = {"preserved": ("preserved", "mean"),
           "recall": ("recall", "mean"),
           "byte_ratio": ("byte_ratio", "mean"),
           "n": ("query_id", "size")}
    if "retained" in j.columns:
        agg["retained"] = ("retained", "mean")
    tab = j.groupby(gb).agg(**agg)
    if "retained" in tab.columns:
        tab["gap"] = tab["retained"] - tab["preserved"]
        tab = tab[["retained", "preserved", "gap", "recall", "byte_ratio", "n"]]
    print("\n" + tab.round(3).to_string())

    # ── saturation warning ─────────────────────────────────────────────
    # If preservation has no variance there is no ranking to correlate, and
    # the honest figure is a two panel saturation comparison rather than a
    # rank correlation on a tie block.
    if "policy" in gb:
        spread = tab.groupby([c for c in gb if c != "policy"]).preserved.agg(
            lambda s: s.max() - s.min())
        flat = spread[spread < 0.02]
        if len(flat):
            print("\nWARNING: preservation is near flat across policies in "
                  f"{len(flat)} cell(s) (spread < 0.02).")
            print("  A rank correlation over a tie block is not a measurement. "
                  "Report the\n  saturation directly instead, with the end to "
                  "end spread beside it.")

    args.out.mkdir(parents=True, exist_ok=True)
    j.to_csv(args.out / "preservation.csv", index=False)
    tab.to_csv(args.out / "preservation_table.csv")
    cover.to_csv(args.out / "extractive_share.csv")
    print(f"\nwrote {args.out}/preservation.csv, preservation_table.csv, "
          "extractive_share.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
