#!/usr/bin/env python3
"""Pre-flight capture audit. Run BEFORE the campaign; costs seconds.

    python scripts/preflight_capture.py                       # environment only
    python scripts/preflight_capture.py artifacts/smoke_r8    # + artefact audit

The failure this exists to prevent: a campaign that runs for days, reports
success, and wrote none of the artefacts the analysis needs. Every check here
is one that fails SILENTLY at run time.

  ENV   the LLM disk cache is live and wrapped, so a logged cache_key resolves
        to a real entry (compressed-then-evicted rewrites are recoverable only
        through the cache).
  HASH  the capture flags are part of the config identity, so a run with
        capture off cannot satisfy a DONE.json for a run with capture on.
  ART   for a completed run: snapshot rows exist, event logs carry the compress
        attempt records with their cache keys, those keys resolve on disk, and
        the provenance columns are populated (git not dirty).
"""
from __future__ import annotations

import csv
import json
import os
import sys
from pathlib import Path

PASS, FAIL, WARN = "PASS", "FAIL", "WARN"
_fails: list[str] = []
_warns: list[str] = []


def ok(msg: str) -> None:
    print(f"  [{PASS}] {msg}")


def bad(msg: str) -> None:
    _fails.append(msg)
    print(f"  [{FAIL}] {msg}")


def warn(msg: str) -> None:
    _warns.append(msg)
    print(f"  [{WARN}] {msg}")


# ── ENV ───────────────────────────────────────────────────────────
def check_env() -> None:
    print("\nENV — LLM cache wiring")
    try:
        from forgetit.llm.disk_cache import cache_enabled, cache_dir
    except Exception as e:
        bad(f"cannot import forgetit.llm.disk_cache: {e}")
        return

    if cache_enabled():
        ok("LLM_CACHE enabled")
    else:
        bad("LLM_CACHE is OFF — every logged cache_key will be a dangling "
            "pointer and compressed-then-evicted rewrites are unrecoverable")

    d = cache_dir()
    if d.exists():
        n = sum(1 for _ in d.rglob("*.json"))
        ok(f"cache dir {d} exists ({n} entries)")
    else:
        warn(f"cache dir {d} does not exist yet (created on first call)")

    try:
        from forgetit.llm import get_llm
        llm = get_llm()
    except Exception as e:
        warn(f"get_llm() unavailable here ({type(e).__name__}: {e}) — "
             "re-run this on the machine that will launch the campaign")
        return
    if hasattr(llm, "cache") or type(llm).__name__ == "CachedLLM":
        ok(f"get_llm() returns {type(llm).__name__} (cache-wrapped)")
    else:
        bad(f"get_llm() returns {type(llm).__name__}, NOT CachedLLM — "
            "cache keys will be logged but nothing is ever written")


# ── HASH ──────────────────────────────────────────────────────────
def check_hash() -> None:
    print("\nHASH — capture flags are part of the config identity")
    try:
        from forgetit.benchmark.pipeline import _config_hash
    except Exception as e:
        bad(f"cannot import _config_hash: {e}")
        return

    # The generation cap is NOT a field here any more: it lives in the reader
    # name (…_gen256), which is already part of this identity via `reader`.
    base = dict(dataset="d", itemiser="i", store_label="s", k=5,
                reader="reader_m_rtok16000_gen256",
                seed=0, budget_bytes=1, budget_frac=0.25,
                n_examples=2, probe_scheme="disjoint_split_v1", probe_every=1)
    for flag in ("write_events", "write_gold_snapshot"):
        on = _config_hash(**base, **{flag: True,
                                     **({} if flag == "write_events"
                                        else {"write_events": True})})
        off = _config_hash(**base, **{flag: False,
                                      **({} if flag == "write_events"
                                         else {"write_events": True})})
        if on != off:
            ok(f"{flag} changes the config hash")
        else:
            bad(f"{flag} does NOT change the config hash — a run without it "
                "would satisfy a DONE.json for a run with it")


# ── ARTEFACTS ─────────────────────────────────────────────────────
def check_artifacts(root: Path) -> None:
    print(f"\nART — artefacts under {root}")
    cfgs = sorted({p.parent for p in root.rglob("results.csv")})
    if not cfgs:
        bad(f"no results.csv anywhere under {root}")
        return
    print(f"  {len(cfgs)} config(s)")

    try:
        from forgetit.llm.disk_cache import LLMDiskCache
        cache = LLMDiskCache()
    except Exception:
        cache = None

    n_snap = n_ev = n_done = 0
    compress_events = keys_seen = keys_resolved = 0
    dirty = set()
    empty_prov: dict[str, int] = {}

    for d in cfgs:
        if (d / "gold_answer_snapshot.jsonl").exists():
            n_snap += 1
        if (d / "events.jsonl").exists():
            n_ev += 1
        if (d / "DONE.json").exists():
            n_done += 1

        ev = d / "events.jsonl"
        if ev.exists():
            for line in ev.open():
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if e.get("name") != "compress":
                    continue
                compress_events += 1
                k = e.get("cache_key")
                if k:
                    keys_seen += 1
                    if cache is not None and cache.get(k) is not None:
                        keys_resolved += 1

        with (d / "results.csv").open() as fh:
            for i, r in enumerate(csv.DictReader(fh)):
                if i > 200:
                    break
                gc = r.get("git_commit", "")
                if gc.endswith("-dirty"):
                    dirty.add(gc)
                for col in ("config_hash", "llm_model", "prompt_reader_sha"):
                    if not r.get(col):
                        empty_prov[col] = empty_prov.get(col, 0) + 1

    (ok if n_snap == len(cfgs) else bad)(
        f"gold_answer_snapshot.jsonl in {n_snap}/{len(cfgs)} configs")
    (ok if n_ev == len(cfgs) else bad)(
        f"events.jsonl in {n_ev}/{len(cfgs)} configs")
    (ok if n_done == len(cfgs) else warn)(
        f"DONE.json in {n_done}/{len(cfgs)} configs "
        "(withheld configs failed the capture gate and will re-run)")

    if compress_events == 0:
        warn("no compress events found — was a compression policy included?")
    else:
        (ok if keys_seen == compress_events else bad)(
            f"cache_key present on {keys_seen}/{compress_events} compress events")
        if cache is None:
            warn("disk cache unavailable here — key resolution not checked")
        elif keys_seen:
            (ok if keys_resolved == keys_seen else bad)(
                f"{keys_resolved}/{keys_seen} cache keys resolve to a stored "
                "entry (unresolved = that rewrite is gone)")

    if dirty:
        bad(f"git_commit is DIRTY ({', '.join(sorted(dirty))}) — artefacts "
            "cannot be tied to a reproducible tree")
    else:
        ok("git_commit clean on all sampled rows")

    for col, n in sorted(empty_prov.items()):
        warn(f"provenance column {col!r} empty on {n} sampled row(s)")


def main() -> int:
    check_env()
    check_hash()
    if len(sys.argv) > 1:
        check_artifacts(Path(sys.argv[1]))
    else:
        print("\nART — skipped (pass a run directory to audit artefacts)")

    print("\n" + "─" * 60)
    if _fails:
        print(f"{FAIL}: {len(_fails)} blocking issue(s):")
        for f in _fails:
            print(f"  · {f}")
        return 1
    print(f"{PASS}: capture wiring is sound"
          + (f"; {len(_warns)} warning(s)." if _warns else "."))
    return 0


if __name__ == "__main__":
    sys.exit(main())