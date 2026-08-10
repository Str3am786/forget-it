#!/usr/bin/env python3
"""
build_compression_corpus.py — the annotated corpus for the compress action.

WHY. The thesis reports numbers for a policy that rewrites items with a language
model, and nothing so far establishes that the rewrites preserve anything. The
prompt was written by hand and never evaluated; `gold_id_retained` counts a
compressed husk as survived; and the two compressing arms differ in prompt AND
selector on the grid, so they cannot be compared there. This corpus is what a
supervisor or a reviewer is pointed at when they ask how you know the summaries
are any good.

WHAT A ROW IS. One (compressed gold item, question) pair: the text before, the
text after, the question that item is evidence for, and the reference answer.
That is everything an annotator or a judge needs, and it is already on disk —
`gold_answer_snapshot.jsonl` carries O_g and R_g, `events.jsonl` carries the
compression's byte counts and its `cache_key`.

FRAME. Compressions of GOLD items only, since a question and an answer are what
make a rewrite judgeable. Two populations:

  survived         — R_g is in the snapshot. The majority.
  compressed-then-evicted — snapshot has O_g but an empty R_g; the compress
                     event still holds cache_key, so the text is recoverable
                     from the disk cache. Including these is what stops the
                     corpus grading only the compressions that happened to
                     outlive the run.

Rejected compressions (`low_saving`) are the compressor's failures and belong
here too, but runs before the cache_key annotation landed did not record their
keys, so they are unreachable in older artefacts. The counts are reported so the
gap is visible rather than assumed away.

SCOPE. Unanswerable questions are dropped: preservation is undefined where there
is no answer in the haystack (grading.is_answerable). The achieved ratio is
left-censored at 1 - min_saving_ratio, because anything compressing worse than
that was rejected and evicted rather than kept — so the ratio axis spans roughly
0.04 to 0.50 by construction, not by observation.

Usage:
    python scripts/build_compression_corpus.py artifacts/smoke6 --n 150
    python scripts/build_compression_corpus.py artifacts/smoke6 --n 150 --seed 7
"""
from __future__ import annotations

import argparse
import json
import random
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from forgetit.benchmark.grading import answer_in_text, is_answerable  # noqa: E402

# Achieved-ratio bands. The compressor behaves differently at 20x than at 2x,
# and pooling them is how "compression preserves X%" becomes uninterpretable.
BANDS = [(0.00, 0.15, "hard"), (0.15, 0.35, "medium"), (0.35, 1.01, "light")]


def band(ratio: float) -> str:
    for lo, hi, name in BANDS:
        if lo <= ratio < hi:
            return name
    return "?"


def tag_from_path(p: Path, root: Path) -> dict:
    """dataset / itemiser / retention / policy / retriever / budget_N / reader."""
    parts = p.parent.relative_to(root).parts
    tag = {"dataset": parts[0] if parts else "",
           "itemiser": parts[1] if len(parts) > 1 else "",
           "config_dir": "/".join(parts)}
    for seg in parts:
        if seg.startswith("budget_"):
            tag["budget"] = seg.removeprefix("budget_")
    if "retention" in parts:
        i = parts.index("retention")
        tag["policy"] = parts[i + 1] if i + 1 < len(parts) else ""
        tag["retriever"] = parts[i + 2] if i + 2 < len(parts) else ""
    return tag


def load_compressions(events_path: Path) -> dict[str, dict]:
    """item_id -> the compress event for it (byte counts + cache_key)."""
    out: dict[str, dict] = {}
    if not events_path.exists():
        return out
    for line in events_path.open():
        r = json.loads(line)
        if r.get("name") == "compress":
            out[r["id"]] = r
    return out


def load_rejections(events_path: Path) -> list[dict]:
    """Decisions where the model produced something the threshold discarded.

    These are the compressor's failure cases. Older artefacts carry no
    cache_key on them, so the text is unreachable; counted regardless so the
    corpus can state what it is missing.
    """
    out: list[dict] = []
    if not events_path.exists():
        return out
    for line in events_path.open():
        r = json.loads(line)
        if r.get("outcome") in ("low_saving", "empty_output"):
            out.append(r)
    return out


def recover_from_cache(cache_key: str, cache_dir: Path) -> str | None:
    """Fetch a generation from the disk cache by key.

    TODO: fill in once disk_cache.py is to hand — needs the key→path layout and
    the stored record's shape. Until then, compressed-then-evicted rows are
    counted and skipped rather than silently dropped.
    """
    return None


def collect(root: Path, cache_dir: Path | None) -> tuple[list[dict], Counter]:
    rows: list[dict] = []
    stats: Counter = Counter()
    seen: set[tuple] = set()

    for snap_path in sorted(root.rglob("gold_answer_snapshot.jsonl")):
        tag = tag_from_path(snap_path, root)
        events = snap_path.parent / "events.jsonl"
        compressions = load_compressions(events)
        stats["rejected_unreachable"] += sum(
            1 for r in load_rejections(events) if not r.get("cache_key"))

        for line in snap_path.open():
            s = json.loads(line)
            stats["snapshot_rows"] += 1

            if not is_answerable(s.get("query_id", ""), s.get("question_type", "")):
                stats["dropped_unanswerable"] += 1
                continue
            if not s.get("answer"):
                stats["dropped_no_reference"] += 1
                continue

            ev = compressions.get(s["gold_id"])
            if ev is None:
                stats["not_compressed"] += 1
                continue

            # Same item under the same prompt yields the same text (temperature
            # 0, cached), so a compression repeated across budget rungs is one
            # sample, not several. Key on the prompt-bearing arm, not the rung.
            key = (tag.get("itemiser"), tag.get("policy"), s["gold_id"], s["query_id"])
            if key in seen:
                stats["dedup_across_budgets"] += 1
                continue

            if s.get("was_compressed") and s.get("resident_text"):
                compressed_text, source = s["resident_text"], "resident"
            elif s.get("was_evicted"):
                compressed_text = recover_from_cache(ev.get("cache_key", ""),
                                                     cache_dir) if cache_dir else None
                source = "cache"
                if not compressed_text:
                    stats["evicted_unrecovered"] += 1
                    continue
            else:
                stats["compressed_flag_missing"] += 1
                continue

            seen.add(key)
            orig_b = int(ev.get("old_bytes") or s.get("orig_bytes") or 0)
            new_b = int(ev.get("new_bytes") or len(compressed_text.encode()))
            ratio = new_b / orig_b if orig_b else float("nan")

            rows.append({
                **tag,
                "example_id": s.get("example_id", ""),
                "query_id": s["query_id"],
                "gold_id": s["gold_id"],
                "question_type": s.get("question_type", ""),
                "reference_answer": s["answer"],
                "original_text": s["original_text"],
                "compressed_text": compressed_text,
                "orig_bytes": orig_b,
                "compressed_bytes": new_b,
                "ratio": round(ratio, 4),
                "ratio_band": band(ratio),
                "text_source": source,
                "cache_key": ev.get("cache_key", ""),
                # Where O_g states the answer verbatim, preservation is
                # decidable without a judge, and the eviction-only policies must
                # agree exactly — the check that the measure works at all.
                "original_extractive": bool(answer_in_text(s["answer"], s["original_text"])),
                # Annotation columns, left blank on purpose.
                "human_answer_in_original": "",
                "human_answer_in_compressed": "",
                "human_unsupported_claim": "",
                "human_notes": "",
            })
            stats["kept"] += 1

    return rows, stats


def stratified_sample(rows: list[dict], n: int, seed: int) -> list[dict]:
    """Proportional across (itemiser, policy, ratio_band), with a floor per cell.

    Uniform sampling would let the largest slice swallow the corpus, and the
    interesting question is exactly whether preservation differs BY stratum.
    """
    rng = random.Random(seed)
    strata: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        strata[(r.get("itemiser"), r.get("policy"), r["ratio_band"])].append(r)

    total = len(rows)
    picked: list[dict] = []
    for key, group in sorted(strata.items(), key=lambda kv: str(kv[0])):
        want = max(3, round(n * len(group) / total)) if total else 0
        rng.shuffle(group)
        picked.extend(group[:want])

    rng.shuffle(picked)
    return picked[:n] if len(picked) > n else picked


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("root", type=Path, help="artifacts root of a run")
    ap.add_argument("--n", type=int, default=150, help="target corpus size")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--cache-dir", type=Path, default=Path("artifacts/llm_cache"))
    ap.add_argument("--out", type=Path, default=Path("corpora/compression"))
    args = ap.parse_args()

    rows, stats = collect(args.root, args.cache_dir if args.cache_dir.exists() else None)

    print(f"frame: {stats['kept']:,} judgeable (gold, question) compressions\n")
    for k in ("snapshot_rows", "not_compressed", "dropped_unanswerable",
              "dropped_no_reference", "dedup_across_budgets", "evicted_unrecovered",
              "compressed_flag_missing", "rejected_unreachable"):
        if stats[k]:
            print(f"  {k:<26} {stats[k]:>7,}")

    if not rows:
        print("\nNothing to sample. Check that the run used --gold-snapshot "
              "AND --event-log, and that a compressing policy was in the roster.")
        return

    print("\nframe by stratum:")
    by = Counter((r.get("itemiser"), r.get("policy"), r["ratio_band"]) for r in rows)
    for (it, pol, b), c in sorted(by.items(), key=lambda kv: str(kv[0])):
        print(f"  {it:<12} {pol:<14} {b:<7} {c:>6}")

    ext = sum(r["original_extractive"] for r in rows)
    print(f"\noriginal states the answer verbatim: {ext}/{len(rows)} "
          f"({ext / len(rows):.1%}) — the subset gradeable without a judge")

    sample = stratified_sample(rows, args.n, args.seed)
    rng = random.Random(args.seed)
    rng.shuffle(sample)
    half = len(sample) // 2
    for i, r in enumerate(sample):
        r["split"] = "dev" if i < half else "test"
        r["corpus_id"] = f"c{i:04d}"

    args.out.mkdir(parents=True, exist_ok=True)
    import csv
    cols = list(sample[0].keys())
    with (args.out / "corpus.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(sample)

    (args.out / "manifest.json").write_text(json.dumps({
        "root": str(args.root), "seed": args.seed, "n_requested": args.n,
        "n_sampled": len(sample), "frame_size": len(rows),
        "bands": [[lo, hi, nm] for lo, hi, nm in BANDS],
        "stats": dict(stats),
    }, indent=2))

    print(f"\nsampled {len(sample)} ({half} dev / {len(sample) - half} test) "
          f"-> {args.out / 'corpus.csv'}")
    print("Iterate on dev. Touch test once, at the end, or the numbers are "
          "selection on your own choices.")


if __name__ == "__main__":
    main()