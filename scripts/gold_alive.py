#!/usr/bin/env python3
"""Answerability judge: is what SURVIVED enough to answer the gold question?

One question, asked at two points in the pipeline, with the same prompt and the
same judge:

    --stage store       over the surviving text of the gold items
    --stage retrieved   over the top-k set that was actually delivered

The verdict is agnostic to HOW bytes were lost. An evicted item contributes no
text and scores false; a compressed item contributes its rewrite and is judged
on that rewrite. Nothing in the prompt reveals which happened, which is what
makes eviction and compression comparable on one axis.

Three rules, each earned from the failure of the previous judge:

  1. The ORIGINAL TEXT IS NEVER SHOWN. Given both, a judge scores "did the
     rewrite lose something relative to the original" instead of "can the
     answer be derived from this", and returns false on everything.
  2. ONE QUESTION PER CALL, ONE WORD BACK. The 7B collapsed three questions
     into one judgement.
  3. THE SET IS JUDGED, NOT THE ITEM. One call per question per config over
     all surviving gold text joined. Multi-hop questions work without a
     conjunctive/disjunctive rule, and it is cheaper.

Deterministic containment (`preserved`) is NOT replaced. Keep it as a second
column: it is the only instrument that can give the exact equality that
validates non-rewriting policies, and it calibrates this judge on the
extractive subset where both apply.

Usage
-----
    # ALWAYS run this first: prints the schema of one snapshot record and exits
    python scripts/gold_alive.py <campaign_root> --inspect

    # store stage
    python scripts/gold_alive.py /scratch/$PROJ/artifacts/sweep_subset/longmemeval_turn_pair \
        --stage store --out report/alive_store_longmemeval.csv

    # retrieval stage
    python scripts/gold_alive.py <campaign_root> \
        --stage retrieved --top-k 10 --out report/alive_retrieved_longmemeval.csv

Requires a served judge:
    LLM_BASE_URL=http://localhost:8000/v1 LLM_MODEL=... LLM_API_KEY=...
Use the SAME judge for both stages and for the answer judge, so the instrument
is a held constant rather than a per-stage variable.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import json
import os
import re
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from forgetit.benchmark.item_dates import DateMap                    # noqa: E402
from forgetit.core.schema import MemRecord                            # noqa: E402
from forgetit.llm.readers import build_evidence, render_sha            # noqa: E402

SHA_LEN = 12
SNAPSHOT_GLOB = "gold_answer_snapshot*.jsonl"
RETRIEVED_GLOB = "retrieved_index*.jsonl"

# Field aliases. --inspect prints the real keys; override with --field-* if the
# snapshot schema does not match any of these.
ALIASES = {
    "query_id": ("query_id", "question_id", "qid", "query"),
    "question": ("question", "question_text", "query_text", "prompt"),
    "answer": ("answer", "ref_answer", "gold_answer", "reference"),
    "resident": ("resident_text", "text_resident", "current_text", "text_now"),
    "item_id": ("item_id", "id", "gold_id", "memory_id"),
    "evicted": ("evicted", "is_evicted", "gone"),
}

PROMPT = """You decide whether a piece of text is enough to answer a question.

TEXT:
{context}

QUESTION: {question}

EXPECTED ANSWER: {answer}

Reading ONLY the text above, is the expected answer stated in it or directly \
implied by it?

Each numbered piece of text begins with the date of the conversation it comes \
from, in parentheses, when that date is known.

The text may be a shortened or rewritten version of something longer. That is \
not a reason to answer false. Ignore style, length and anything the text \
omits. Judge only whether the expected answer can be derived from what is \
present. If the text is empty, answer false.

Reply with exactly one word, in English: true or false"""

CONTEXT_CAP = 100_000   # chars; ~19.6k tokens at 5.1 chars/token
CONTEXT_CAP = 100_000   # chars; ~19.6k tokens at 5.1 chars/token
VERDICT_RE = re.compile(r"\b(true|false)\b", re.I)


def prompt_sha() -> str:
    return hashlib.sha256(PROMPT.encode()).hexdigest()[:SHA_LEN]


def pick(rec: dict, key: str, override: str | None) -> str:
    if override:
        return str(rec.get(override, "") or "")
    for cand in ALIASES[key]:
        if cand in rec:
            return str(rec.get(cand, "") or "")
    return ""


def iter_config_dirs(root: Path):
    """Yield every directory holding a gold snapshot. Follows symlinks: the
    campaign tree uses them, and Path.rglob does not descend into them."""
    for dirpath, _dirnames, filenames in os.walk(root, followlinks=True):
        if any(re.fullmatch(SNAPSHOT_GLOB.replace("*", ".*"), f) for f in filenames):
            yield Path(dirpath)


def config_facets(cfg_dir: Path, root: Path) -> dict:
    """Policy / budget / workload from the path. Adjust if the tree differs;
    the manifest is authoritative if one is present."""
    man = cfg_dir / "manifest.json"
    if man.is_file():
        try:
            m = json.loads(man.read_text())
            return {k: m.get(k) for k in ("workload", "itemiser", "budget",
                                          "policy", "retriever")}
        except Exception:
            pass
    f = cfg_dir / "results.csv"
    if f.is_file():
        try:
            d = pd.read_csv(f, nrows=1, dtype=str).fillna("")
            got = {k: d.iloc[0][k] for k in
                   ("dataset", "policy", "itemiser", "retriever", "budget_frac")
                   if k in d.columns}
            if got:
                got["config_path"] = "/".join(cfg_dir.relative_to(root).parts)
                got["budget"] = got.pop("budget_frac", "")
                return got
        except Exception:
            pass
    return {"config_path": "/".join(cfg_dir.relative_to(root).parts)}


def load_questions(cfg_dir: Path) -> dict:
    """query_id -> (question_text, ref_answer) from the config's OWN
    results.csv. Per config, never a global join, so the question set cannot
    drift from the snapshot it is paired with."""
    f = cfg_dir / "results.csv"
    if not f.is_file():
        return {}
    want = {"query_id", "question_text", "ref_answer"}
    try:
        d = pd.read_csv(f, usecols=lambda c: c in want, dtype=str)
    except Exception:
        return {}
    if "query_id" not in d.columns:
        return {}
    d = d.drop_duplicates("query_id").fillna("")
    return {str(r["query_id"]): (str(r.get("question_text", "")),
                                 str(r.get("ref_answer", "")))
            for _, r in d.iterrows()}


def render_context(items, date_map=None, example_id: str = "") -> str:
    """Render (item_id, text) pairs EXACTLY as the reader sees them.

    Same build_evidence, same numbering, same dated prefixes, same one-budget
    overflow rule -- so a sufficiency verdict is a statement about the reader's
    context rather than about a second, classifier-only format. Overflow now
    drops whole items instead of cutting at a character cap, which is a real
    change to this instrument: see the render-concordance follow-up.
    """
    recs = []
    for item_id, text in items:
        meta = {}
        if date_map is not None and item_id:
            ds = date_map.lookup(item_id, example_id)
            if ds:
                meta["date_str"] = ds
        recs.append(MemRecord(id=item_id or "", text=text, meta=meta))
    block, _truncated, _delivered = build_evidence(recs, CONTEXT_CAP // 4)
    return block


def load_store_contexts(cfg_dir: Path, args, date_map=None) -> list[dict]:
    """One row per question: the surviving gold text, joined.

    An item with empty resident text (evicted) contributes nothing. A question
    whose gold items are all evicted yields an empty context and is scored
    false WITHOUT a judge call.
    """
    snap = next(cfg_dir.glob(SNAPSHOT_GLOB))
    by_q: dict[str, dict] = {}
    for line in snap.open(encoding="utf-8"):
        if not line.strip():
            continue
        r = json.loads(line)
        qid = pick(r, "query_id", args.field_query_id)
        if not qid:
            continue
        q = by_q.setdefault(qid, {
            "query_id": qid,
            "question": pick(r, "question", args.field_question),
            "answer": pick(r, "answer", args.field_answer),
            "n_gold": 0, "n_alive_ids": 0, "items": [],
        })
        q["n_gold"] += 1
        if not q["question"]:
            q["question"] = pick(r, "question", args.field_question)
        if not q["answer"]:
            q["answer"] = pick(r, "answer", args.field_answer)
        resident = pick(r, "resident", args.field_resident).strip()
        if resident:
            q["n_alive_ids"] += 1
            # The id travels with the text: the render-time date lookup is keyed
            # by item id, and without it the classifier would read a DIFFERENT
            # context from the one the reader gets.
            q["items"].append((pick(r, "item_id", args.field_item_id), resident))
    qmap = load_questions(cfg_dir)
    rows = []
    for q in by_q.values():
        qt, ra = qmap.get(q["query_id"], ("", ""))
        q["question"] = q["question"] or qt
        q["answer"] = q["answer"] or ra
        q["context"] = render_context(q.pop("items"), date_map, q["query_id"])
        rows.append(q)
    return rows


def load_retrieved_contexts(cfg_dir: Path, args, date_map=None) -> list[dict]:
    """One row per question: the top-k delivered set, gold and non-gold alike.

    Non-gold items are INCLUDED on purpose. Answerability of what the reader
    actually saw is the quantity of interest, and distractors are part of it.
    """
    idx = next(cfg_dir.glob(RETRIEVED_GLOB))
    meta = {r["query_id"]: r for r in load_store_contexts(cfg_dir, args)
            for _ in (0,)} if False else {
        r["query_id"]: r for r in load_store_contexts(cfg_dir, args)}
    rows = []
    for line in idx.open(encoding="utf-8"):
        if not line.strip():
            continue
        r = json.loads(line)
        qid = pick(r, "query_id", args.field_query_id)
        items = r.get("ranked") or r.get("results") or r.get("items") or []
        texts = []
        for it in items[: args.top_k]:
            t = (it.get("text") if isinstance(it, dict) else None) or ""
            if t.strip():
                texts.append((it.get("id") or it.get("item_id") or "", t.strip()))
        m = meta.get(qid, {})
        rows.append({
            "query_id": qid,
            "question": m.get("question", "") or pick(r, "question", args.field_question),
            "answer": m.get("answer", "") or pick(r, "answer", args.field_answer),
            "n_gold": m.get("n_gold", 0),
            "n_alive_ids": m.get("n_alive_ids", 0),
            "n_retrieved": len(texts),
            "context": render_context(texts, date_map, qid),
        })
    return rows


def cache_key(row: dict) -> str:
    """Content addressed. Two configs that left the same text behind for the
    same question are one call, not two. Most of the saving is here."""
    blob = "\x00".join((f"cap={CONTEXT_CAP}", row["question"],
                        row["answer"], row["context"][:CONTEXT_CAP]))
    return hashlib.sha256(blob.encode()).hexdigest()[:24]


def judge_one(llm, ctx: dict) -> dict:
    p = PROMPT.format(context=ctx["context"][:CONTEXT_CAP],
                      question=ctx["question"][:2000],
                      answer=ctx["answer"][:1000])
    try:
        out = llm.generate(p, max_tokens=4, temperature=0.0)
    except Exception as exc:
        return {"cache_key": ctx["cache_key"], "answerable": None,
                "judge_errored": True, "judge_raw": type(exc).__name__}
    m = VERDICT_RE.search(out or "")
    return {
        "cache_key": ctx["cache_key"],
        # Unparseable is a MISSING DATUM, never a rejection.
        "answerable": (m.group(1).lower() == "true") if m else None,
        "judge_errored": m is None,
        "judge_raw": (out or "")[:40],
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("root", type=Path)
    ap.add_argument("--stage", choices=("store", "retrieved"), default="store")
    ap.add_argument("--out", type=Path)
    ap.add_argument("--top-k", type=int, default=10)
    ap.add_argument("--workers", type=int, default=32)
    ap.add_argument("--limit-configs", type=int, default=0)
    ap.add_argument("--data-dir", type=Path, default=REPO / "data",
                    help="raw dataset files, for the render-time date lookup")
    ap.add_argument("--dry-run", action="store_true",
                    help="count configs and contexts, make no LLM calls")
    ap.add_argument("--inspect", action="store_true",
                    help="print one snapshot record's keys and exit")
    for f in ("query-id", "question", "answer", "resident", "item-id"):
        ap.add_argument(f"--field-{f.replace('-', '_')}", default=None)
    args = ap.parse_args()

    cfgs = sorted(iter_config_dirs(args.root))
    if not cfgs:
        print(f"STOP - no {SNAPSHOT_GLOB} under {args.root}", file=sys.stderr)
        return 2
    print(f"{len(cfgs):,} config directories under {args.root}")

    if args.inspect:
        snap = next(cfgs[0].glob(SNAPSHOT_GLOB))
        rec = json.loads(next(l for l in snap.open() if l.strip()))
        print(f"\nfirst record of {snap}:")
        for k, v in rec.items():
            s = str(v).replace("\n", " ")
            print(f"  {k:<24} {s[:100]}")
        print("\nMap any missing field with --field-query-id / --field-question "
              "/ --field-answer / --field-resident / --field-item-id.")
        return 0

    if not args.out:
        print("STOP - --out is required unless --inspect", file=sys.stderr)
        return 2
    if args.limit_configs:
        cfgs = cfgs[: args.limit_configs]

    # -- gather every context, tagged with its config -----------------------
    load = load_store_contexts if args.stage == "store" else load_retrieved_contexts
    rows: list[dict] = []
    date_maps: dict[str, object] = {}          # workload -> DateMap, built once
    for i, cfg in enumerate(cfgs, 1):
        facets = config_facets(cfg, args.root)
        wl = DateMap.resolve_name(str(facets.get("workload")
                                      or facets.get("dataset") or ""))
        if wl and wl not in date_maps:
            try:
                date_maps[wl] = DateMap.for_workload(wl, args.data_dir)
                print(f"  dates: {wl} -> {len(date_maps[wl]):,} entries", flush=True)
            except (FileNotFoundError, ValueError) as e:
                print(f"  WARNING: no dates for {wl} ({type(e).__name__}: {e}); "
                      f"contexts render UNDATED", file=sys.stderr)
                date_maps[wl] = None
        try:
            for r in load(cfg, args, date_maps.get(wl)):
                r.update(facets)
                r["stage"] = args.stage
                r["cache_key"] = cache_key(r)
                a = (r.get("answer") or "").strip().lower()
                r["answer_in_context"] = bool(a) and a in r["context"].lower()
                r["context_chars"] = len(r["context"])
                # The reader's render contract, not this script's prompt (that
                # is prompt_sha): the column exists so one bundle's instruments
                # can be checked for describing ONE evidence base.
                r["render_sha"] = render_sha()
                rows.append(r)
        except StopIteration:
            continue
        if i % 200 == 0:
            print(f"  read {i:,}/{len(cfgs):,} configs, {len(rows):,} rows", flush=True)
    df = pd.DataFrame(rows)
    print(f"\n{len(df):,} question-config rows")

    # -- free verdicts: empty context is false without a call ---------------
    empty = df.context.str.strip() == ""
    print(f"{int(empty.sum()):,} rows have EMPTY surviving context -> false, no call")

    todo = df.loc[~empty, ["cache_key", "question", "answer", "context"]]
    todo = todo.drop_duplicates("cache_key")
    print(f"{int((~empty).sum()):,} rows need a verdict, "
          f"{len(todo):,} unique after dedup "
          f"({1 - len(todo) / max(int((~empty).sum()), 1):.1%} saved)")

    # -- resume -------------------------------------------------------------
    cache_path = args.out.with_suffix(".cache.csv")
    done: dict[str, dict] = {}
    if cache_path.is_file():
        prev = pd.read_csv(cache_path)
        done = {r["cache_key"]: r for r in prev.to_dict("records")
                if not r.get("judge_errored")}
        print(f"resuming: {len(done):,} cached verdicts")
    work = [r for r in todo.to_dict("records") if r["cache_key"] not in done]

    if args.dry_run:
        print("\n--dry-run: stopping before any LLM call")
        return 0
    from forgetit.llm import get_llm
    llm = get_llm()
    print(f"prompt sha {prompt_sha()}   model {os.environ.get('LLM_MODEL', '?')}")
    print(f"judging {len(work):,} unique contexts with {args.workers} workers ...")

    results = list(done.values())
    if work:
        with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
            for i, res in enumerate(ex.map(lambda r: judge_one(llm, r), work), 1):
                results.append(res)
                if i % 500 == 0:
                    print(f"  {i:,}/{len(work):,}", flush=True)

    jd = pd.DataFrame(results)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    jd.to_csv(cache_path, index=False)

    out = df.merge(jd[["cache_key", "answerable", "judge_errored"]],
                   on="cache_key", how="left")
    out.loc[empty, "answerable"] = False
    out.loc[empty, "judge_errored"] = False
    col = f"answerable_{args.stage}"
    out = out.rename(columns={"answerable": col})
    out.drop(columns=["context"]).to_csv(args.out, index=False)

    bad = int(out.judge_errored.fillna(False).sum())
    print(f"\nwrote {args.out}  ({len(out):,} rows, {bad} unparseable)")
    if "policy" in out.columns and "budget" in out.columns:
        print(f"\n{col} by policy and budget:")
        print(out.pivot_table(index="policy", columns="budget", values=col,
                              aggfunc="mean").round(3).to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
