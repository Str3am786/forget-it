#!/usr/bin/env python3
"""Verify the R8 probe fix from a run's artefacts. Read-only; re-runs nothing.

    python scripts/verify_r8_smoke.py artifacts/smoke_r8

FOUR CHECKS, in increasing order of how much they prove:

  1. MANIFEST   the partition shipped, and probe ∩ scored = ∅ per example.
                Proves the split was CONSTRUCTED correctly.
  2. SCORED SET every graded row's query_id is in scored_ids, and no probe-half
                question was graded. Proves the split was HONOURED at run time
                — this is the structural proof the leak is closed.
  3. YIELD      graded-question count against the full question set. Should sit
                near the configured scored share; a count equal to the full set
                means the partition was built but never applied.
  4. AUC        access count as a gold classifier at eviction time, from the
                decision events. The BEHAVIOURAL signal: the leak's fingerprint
                was gold items carrying inflated access counts. Needs
                --event-log; degrades gracefully when absent.

Check 4 is the one that speaks to the science, but it is also the noisiest on a
two-conversation smoke and it is NOT expected to read 0.500 — probe questions
are topically adjacent to scored ones, so some genuine correlation survives.
Read it as "no longer extreme", and get the real number from the leaky/fixed
A/B on the full run.
"""
from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

OK, BAD, WARN, INFO = "PASS", "FAIL", "WARN", "  ·  "


def _load_manifest(cfg_dir: Path) -> dict | None:
    p = cfg_dir / "probe_partition.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception as e:
        print(f"  {BAD}  manifest unreadable: {e}")
        return None


def _load_results(cfg_dir: Path) -> list[dict]:
    p = cfg_dir / "results.csv"
    if not p.exists():
        return []
    with p.open() as fh:
        return list(csv.DictReader(fh))


def _iter_events(cfg_dir: Path):
    p = cfg_dir / "events.jsonl"
    if not p.exists():
        return
    with p.open() as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def _auc(pos: list[float], neg: list[float]) -> float | None:
    """Mann-Whitney AUC with tie correction. None when a class is empty."""
    if not pos or not neg:
        return None
    merged = sorted([(v, 1) for v in pos] + [(v, 0) for v in neg])
    ranks, i = {}, 0
    while i < len(merged):
        j = i
        while j + 1 < len(merged) and merged[j + 1][0] == merged[i][0]:
            j += 1
        avg = (i + j) / 2 + 1
        for k in range(i, j + 1):
            ranks[k] = avg
        i = j + 1
    rank_sum = sum(ranks[idx] for idx, (_, lab) in enumerate(merged) if lab == 1)
    n1, n0 = len(pos), len(neg)
    return (rank_sum - n1 * (n1 + 1) / 2) / (n1 * n0)


def check_config(cfg_dir: Path) -> dict:
    """Return a verdict dict for one config directory."""
    v = {"dir": cfg_dir, "fails": [], "warns": [], "notes": []}

    # ── 1. MANIFEST ───────────────────────────────────────────────
    man = _load_manifest(cfg_dir)
    scored_ids: set[str] = set()
    probe_ids: set[str] = set()
    if man is None:
        v["warns"].append("no probe_partition.json (LongMemEval self-query, "
                          "legacy workload, or the manifest was not written)")
    else:
        v["notes"].append(f"scheme={man.get('probe_scheme')} "
                          f"seed={man.get('partition_seed')} "
                          f"hash={man.get('partition_hash', '')[:12]}")
        for row in man.get("examples", []):
            s, p = set(row["scored_ids"]), set(row["probe_ids"])
            if s & p:
                v["fails"].append(
                    f"{row['example_id']}: probe ∩ scored = {sorted(s & p)[:3]}")
            scored_ids |= s
            probe_ids |= p
        if not v["fails"]:
            v["notes"].append(f"partition: {len(scored_ids)} scored / "
                              f"{len(probe_ids)} probe, disjoint")

    # ── 2. SCORED SET ─────────────────────────────────────────────
    rows = _load_results(cfg_dir)
    if not rows:
        v["warns"].append("no results.csv rows")
    elif man is not None:
        graded = {r.get("query_id", "") for r in rows}
        leaked = graded & probe_ids
        unknown = graded - scored_ids - probe_ids
        if leaked:
            v["fails"].append(
                f"LEAK: {len(leaked)} probe-half question(s) were GRADED, "
                f"e.g. {sorted(leaked)[:2]}")
        if unknown:
            v["warns"].append(
                f"{len(unknown)} graded query_id(s) in neither half "
                f"(id-format drift?), e.g. {sorted(unknown)[:2]}")
        if not leaked and not unknown:
            v["notes"].append(f"all {len(graded)} graded rows ⊆ scored half")

        # Scored-half questions that produced no row. A scored question whose
        # evidence resolves to nothing gets gold_item_ids=[], which is how
        # probes are marked, so the runner skips it as unscored. Silent by
        # construction, hence surfaced here with its category breakdown.
        ungraded = scored_ids - graded
        if ungraded:
            by_cat = defaultdict(int)
            for q in ungraded:
                by_cat[q.rsplit("::", 1)[-1]] += 1
            v["notes"].append(
                f"scored but ungraded: {len(ungraded)}/{len(scored_ids)} "
                f"({dict(sorted(by_cat.items()))}) — empty gold reads as a probe")

        # ── 3. YIELD ──────────────────────────────────────────────
        total = len(scored_ids | probe_ids)
        if total:
            share = len(graded) / total
            v["notes"].append(f"yield: {len(graded)}/{total} = {share:.3f} "
                              f"(manifest target {man.get('scored_share')})")
            if share > 0.99:
                v["fails"].append(
                    "every question was graded — the partition exists but is "
                    "NOT being applied (probes still recite scored questions)")

    # ── 4. AUC ────────────────────────────────────────────────────
    # The logger keys events under "name". Item ids are example-namespaced
    # (conv::…), so gold and victim ids join globally within a config without
    # an example_id field — which the event schema does not carry.
    gold_union: set[str] = set()
    victims: list[tuple[str, float]] = []        # (item_id, access_count)
    saw_events = False
    for ev in _iter_events(cfg_dir):
        saw_events = True
        kind = ev.get("name") or ev.get("event") or ev.get("type") or ev.get("kind") or ""
        if kind == "gold":
            for gid in (ev.get("gold_ids") or ev.get("gold_item_ids") or []):
                gold_union.add(str(gid))
        elif kind in ("decision", "evict"):
            ac = ev.get("victim_access_count")
            iid = ev.get("victim_id") or ev.get("id")
            if ac is not None and iid is not None:
                victims.append((str(iid), float(ac)))

    if not saw_events:
        v["warns"].append("no events.jsonl — rerun with --event-log for the AUC")
    elif not victims:
        v["warns"].append("events present but no victim_access_count "
                          "(is custom_manager.py the patched build?)")
    elif not gold_union:
        v["warns"].append("victim access counts present but no gold events to "
                          "join against — AUC not computable")
    else:
        pos = [ac for iid, ac in victims if iid in gold_union]
        neg = [ac for iid, ac in victims if iid not in gold_union]
        auc = _auc(pos, neg)
        if auc is None:
            v["warns"].append(f"AUC needs both classes (gold={len(pos)}, "
                              f"other={len(neg)})")
        else:
            v["auc"] = auc
            v["notes"].append(
                f"access-count→gold AUC = {auc:.3f} "
                f"(n_gold={len(pos)}, n_other={len(neg)})")
            if auc > 0.75:
                v["warns"].append(
                    f"AUC {auc:.3f} is high — evicted gold items still carry "
                    "markedly inflated access counts. Structurally the split "
                    "holds, so check for a second probe source before trusting "
                    "the access-consuming policies.")
    return v


def main(root: Path) -> int:
    cfg_dirs = sorted({p.parent for p in root.rglob("results.csv")})
    if not cfg_dirs:
        print(f"no results.csv anywhere under {root}")
        return 2

    print(f"\nR8 probe verification — {len(cfg_dirs)} config(s) under {root}\n")
    n_fail = n_warn = 0
    aucs = []
    for d in cfg_dirs:
        v = check_config(d)
        rel = d.relative_to(root)
        status = BAD if v["fails"] else (WARN if v["warns"] else OK)
        print(f"[{status}] {rel}")
        for n in v["notes"]:
            print(f"{INFO}{n}")
        for w in v["warns"]:
            print(f"  {WARN}  {w}")
        for f in v["fails"]:
            print(f"  {BAD}  {f}")
        print()
        n_fail += bool(v["fails"])
        n_warn += bool(v["warns"])
        if "auc" in v:
            aucs.append(v["auc"])

    print("─" * 62)
    if aucs:
        print(f"access-count→gold AUC across configs: "
              f"min={min(aucs):.3f} mean={sum(aucs)/len(aucs):.3f} max={max(aucs):.3f}")
    if n_fail:
        print(f"{BAD}: {n_fail} config(s) failed — the leak is NOT closed.")
        return 1
    print(f"{OK}: no leak detected in {len(cfg_dirs)} config(s)"
          + (f"; {n_warn} with warnings." if n_warn else "."))
    return 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1] if len(sys.argv) > 1 else "artifacts/smoke_r8")))