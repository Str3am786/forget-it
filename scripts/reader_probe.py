"""Reader probe — what does the campaign's accuracy owe to the memory system?

An OFFLINE pass over a finished run's artefacts. No store, no policy, no
eviction, no retrieval is replayed: it reads the gold snapshot and results.csv
that a run already wrote and asks one reader four questions per query.

    parametric  no evidence AND no memories wrapper — the model asked directly.
    empty       the memories wrapper with no evidence: does it say IDK.
    retrieved   what THIS policy delivered at THIS budget, rebuilt from a
                --dump-retrieval artefact (needs --retrieval).
    gold        the gold items' ORIGINAL (pre-compression) text. The ceiling.

The deltas are the point:

    gold - parametric   what the memory system actually contributes.
    gold - retrieved    what this policy lost against perfect retrieval.
    parametric - empty  how much real knowledge the IDK instruction suppresses.

TWO THINGS THIS VERSION FIXES, both of which matter more than the speed.

1. ARM SCOPE. parametric, empty and gold depend on the QUESTION only — the
   gold snapshot's original_text is pre-compression, identical in every config.
   The previous version ran them once per config, so over 30 configs it made
   thirty identical calls per question and wrote thirty duplicate rows (the
   resume set is loaded once at startup and never updated mid-run, so the
   duplicates were not even skipped). They are now computed ONCE per unique
   query_id. Over 30 configs that is 4,326 calls instead of 129,780.

2. RETRIEVAL SCOPE. load_retrieval() merged every retrieved_index.jsonl in the
   tree into one dict with setdefault(), so across N configs only the FIRST
   config's retrieval survived per query_id and every config's 'retrieved' arm
   silently measured one config. Retrieval is now loaded PER CONFIG and rows
   carry a `config` field, so the retrieved arm means what its name says.

Rows append to <out>/<reader-name>.jsonl and are skipped if already present.
Resume identity is (config, query_id, arm); shared arms use config="".

    LLM_MODEL=qwen2.5-14b-instruct python scripts/reader_probe.py <run> \\
        --retrieval <campaign-root> --workers 32
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import csv
import itertools
import json
import os
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from forgetit.benchmark.grading import answer_in_text, grade          # noqa: E402
from forgetit.benchmark.item_dates import (                           # noqa: E402
    DateMap, longmemeval_question_dates,
)
from forgetit.core.schema import MemRecord, Question                  # noqa: E402
from forgetit.llm import get_llm                                      # noqa: E402
from forgetit.llm.readers import (                                    # noqa: E402
    ReaderConfig, build_context, build_parametric_context, build_reader,
)

ARMS = ("parametric", "empty", "retrieved", "gold")
SHARED_ARMS = ("parametric", "empty", "gold")   # question-only: config invariant
GOLD_SNAPSHOT = "gold_answer_snapshot.jsonl"
RETRIEVED_INDEX = "retrieved_index.jsonl"
EVIDENCE_GLOB = "evidence_items*.jsonl"


def find_run_dirs(root: Path) -> list[Path]:
    """Config directories holding both a results.csv and a gold snapshot."""
    if (root / GOLD_SNAPSHOT).is_file() and (root / "results.csv").is_file():
        return [root]
    # os.walk(followlinks=True), not rglob: rglob does NOT descend into
    # symlinked directories, so a curated subset built from symlinks resolves
    # to nothing.
    out = set()
    for dirpath, _dirnames, filenames in os.walk(root, followlinks=True):
        if GOLD_SNAPSHOT in filenames and "results.csv" in filenames:
            out.add(Path(dirpath))
    return sorted(out)


def config_key(run_dir: Path, root: Path) -> str:
    """Stable, human readable identity for a config dir.

    Resolved through symlinks so a curated subset directory and the campaign
    tree it points into produce the SAME key — otherwise a subset run and a
    full run could not be merged, and resume would not recognise its own rows.
    """
    real = run_dir.resolve()
    for anchor in ("retention", "floor", "compression"):
        parts = real.parts
        if anchor in parts:
            i = parts.index(anchor)
            return "/".join(parts[max(0, i - 2):])
    try:
        return str(real.relative_to(root.resolve()))
    except ValueError:
        return real.name


def load_questions(run_dir: Path, dataset_filter: str | None) -> list[dict]:
    """Join the gold snapshot (evidence) to results.csv (question metadata)."""
    meta: dict[str, dict] = {}
    with (run_dir / "results.csv").open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            qid = row.get("query_id") or ""
            if qid and qid not in meta:
                meta[qid] = row

    gold: dict[str, list[tuple[str, str]]] = defaultdict(list)
    ref: dict[str, str] = {}
    with (run_dir / GOLD_SNAPSHOT).open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            r = json.loads(line)
            qid = r.get("query_id", "")
            if not qid:
                continue
            # original_text is O_g — the item as first inserted, before any
            # compression. resident_text would measure the policy, not the
            # ceiling, and would make this arm config dependent.
            gold[qid].append((r.get("gold_id", ""), r.get("original_text", "")))
            ref.setdefault(qid, r.get("answer", ""))

    out = []
    for qid in sorted(gold):
        m = meta.get(qid, {})
        dataset = m.get("dataset", "")
        if dataset_filter and dataset_filter not in dataset:
            continue
        out.append({
            "query_id": qid,
            "dataset": dataset,
            "question_text": m.get("question_text", ""),
            "question_type": m.get("question_type", ""),
            "ref_answer": m.get("ref_answer") or ref.get(qid, ""),
            "gold": [(gid, text) for gid, text in gold[qid] if text],
        })
    return out


def load_item_table(retrieval_root: Path) -> dict[str, str]:
    """key -> text, merged across shards. Loaded ONCE for the whole run.

    Keys are content hashes, so a cross shard duplicate is the same text and
    collapses. The table lives at the campaign root, which may be several
    levels above any config dir, so walk up until shards appear.
    """
    items: dict[str, str] = {}
    probe = retrieval_root if retrieval_root.is_dir() else retrieval_root.parent
    for _ in range(8):
        for f in sorted(probe.glob(EVIDENCE_GLOB)):
            with f.open(encoding="utf-8") as fh:
                for line in fh:
                    if line.strip():
                        r = json.loads(line)
                        items[r["key"]] = r.get("text", "")
        if items or probe.parent == probe:
            break
        probe = probe.parent
    return items


def load_index_for(run_dir: Path) -> dict[str, dict]:
    """This config's OWN retrieved_index.jsonl. Never merged across configs."""
    f = run_dir / RETRIEVED_INDEX
    if not f.is_file():
        return {}
    index: dict[str, dict] = {}
    with f.open(encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                r = json.loads(line)
                index[r["query_id"]] = r
    return index


def records_from_index(row: dict, items: dict[str, str]) -> list[MemRecord]:
    """Rebuild what retrieval handed the reader, in stored rank order."""
    return [MemRecord(id=r["id"], text=items.get(r["key"], ""))
            for r in sorted(row["retrieved"], key=lambda r: r["rank"])]


def already_done(path: Path) -> set[tuple[str, str, str]]:
    """(config, query_id, arm) triples already on disk."""
    if not path.is_file():
        return set()
    seen = set()
    with path.open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue                       # partial trailing write → redo it
            seen.add((r.get("config", ""), r.get("query_id", ""), r.get("arm", "")))
    return seen


# ── concurrency ─────────────────────────────────────────────────────────────

def imap_bounded(fn, tasks, workers: int):
    """Map fn over tasks with a bounded in flight window.

    Bounded rather than ex.map(): contexts are built inside the worker, so an
    unbounded submission would materialise every prompt in memory at once.
    Yields (task, result) as they complete, in completion order. The CALLER
    writes — the pool never touches the sink, which is what keeps the append
    and flush resume guarantee intact and the file free of interleaved rows.
    """
    it = iter(tasks)
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fn, t): t for t in itertools.islice(it, workers * 3)}
        while futs:
            done, _ = cf.wait(futs, return_when=cf.FIRST_COMPLETED)
            for fut in done:
                task = futs.pop(fut)
                yield task, fut.result()
                for nxt in itertools.islice(it, 1):
                    futs[ex.submit(fn, nxt)] = nxt


def build_arm_context(task: dict, cfg: ReaderConfig, date_map=None,
                      question_date: str = "") -> tuple:
    """(context, n_dated_items) for one task. PURE: no model, no I/O.

    Dates ride in on ``MemRecord.meta['date_str']`` and are rendered by
    build_evidence; the record TEXT is never touched, so this replay stays a
    faithful rebuild of what the run stored. LongMemEval lookups are scoped by
    the example (its session ids repeat across examples with different dates),
    which is what ``query_id`` is here — for LongMemEval the query id IS the
    question id. Extracted from the worker so the golden test can rebuild a
    prompt without constructing a model.
    """
    q, arm = task["q"], task["arm"]
    qtext = q["question_text"]
    if question_date:
        # The question side of the fix: a temporal question is unanswerable
        # unless the reader knows when it was asked.
        qtext = f"(asked on {question_date}) {qtext}"
    question = Question(id=q["query_id"], text=qtext)

    if arm == "parametric":
        return build_parametric_context(question), 0
    if arm == "empty":
        return build_context(question, [], token_budget=cfg.token_budget,
                             prompt=cfg.prompt), 0
    if arm == "gold":
        recs = [MemRecord(id=gid, text=t) for gid, t in q["gold"]]
    else:                                       # retrieved
        recs = records_from_index(task["index_row"], task["items"])

    n_dated = 0
    if date_map is not None:
        for r in recs:
            ds = date_map.lookup(r.id, q["query_id"])
            if ds:
                r.meta["date_str"] = ds
                n_dated += 1
    ctx = build_context(question, recs, token_budget=cfg.token_budget,
                        prompt=cfg.prompt)
    return ctx, n_dated


def make_worker(reader, cfg: ReaderConfig, date_maps: dict | None = None,
                question_dates: dict | None = None):
    """One callable, run on many threads. Builds the context then answers.

    build_context is pure, so it is safe off the main thread and spreading it
    keeps the prompt for one task alive only as long as that task.
    """
    date_maps = date_maps or {}
    question_dates = question_dates or {}

    def work(task: dict) -> dict:
        q = task["q"]
        dm = date_maps.get(DateMap.resolve_name(q["dataset"]) or "")
        qdate = question_dates.get(q["query_id"], "")
        ctx, n_dated = build_arm_context(task, cfg, dm, qdate)
        res = reader.answer_from_context(ctx)
        return {"ctx": ctx, "res": res, "n_dated": n_dated, "question_date": qdate}
    return work


def row_for(task: dict, out: dict, cfg: ReaderConfig, reader) -> dict:
    q, ctx, res = task["q"], out["ctx"], out["res"]
    g = grade(res.answer, q["ref_answer"], abstained=res.abstained)
    idx_row = task.get("index_row") or {}
    return {
        # config is "" for the shared arms: they are question properties, not
        # config properties, and pretending otherwise would licence averaging
        # the same answer thirty times into a per config accuracy.
        "config": task["config"],
        "query_id": q["query_id"], "arm": task["arm"],
        "dataset": q["dataset"], "question_type": q["question_type"],
        # question is the text AS ASKED (date prefix included); question_raw is
        # the dataset's own wording, so grading and joins are unaffected.
        "question": ctx.question_text, "question_raw": q["question_text"],
        "ref_answer": q["ref_answer"],
        "answer": res.answer,
        "abstained": res.abstained, "errored": res.errored,
        "correct": g["correct"], "f1": round(g["f1"], 4), "tier": g["tier"],
        "contains": answer_in_text(q["ref_answer"], res.answer),
        "n_delivered": ctx.n_delivered, "n_offered": ctx.n_offered,
        "truncated": ctx.truncated,
        "replay_token_budget": cfg.token_budget,
        "replay_prompt_sha": ctx.prompt_sha,
        "render_sha": ctx.render_sha,
        "question_date": out.get("question_date", ""),
        "n_dated_items": out.get("n_dated", 0),
        "run_token_budget": idx_row.get("run_token_budget"),
        "run_prompt_sha": idx_row.get("run_prompt_sha"),
        "cap_hit": res.meta.get("cap_hit"),
        "reader": reader.name, "model": getattr(reader.llm, "model", ""),
    }


def report(rows: list[dict]) -> None:
    by_arm = {a: [r for r in rows if r["arm"] == a] for a in ARMS}
    acc = {}
    print(f"\n{'arm':<12}{'n':>8}{'accuracy':>11}{'abstain':>10}{'error':>9}"
          f"{'contains':>10}")
    for arm in ARMS:
        rs = by_arm[arm]
        if not rs:
            print(f"{arm:<12}{0:>8}{'—':>11}{'—':>10}{'—':>9}{'—':>10}")
            continue
        scored = [r for r in rs if not r["errored"]]
        n = len(scored)
        acc[arm] = sum(r["correct"] for r in scored) / n if n else float("nan")
        print(f"{arm:<12}{len(rs):>8}{acc[arm]:>11.4f}"
              f"{sum(r['abstained'] for r in scored) / n if n else 0:>10.4f}"
              f"{sum(r['errored'] for r in rs) / len(rs):>9.4f}"
              f"{sum(r['contains'] for r in scored) / n if n else 0:>10.4f}")

    if {"gold", "parametric"} <= acc.keys():
        print(f"\n  gold - parametric  = {acc['gold'] - acc['parametric']:+.4f}"
              "   what the memory system contributes")
    if {"gold", "retrieved"} <= acc.keys():
        print(f"  gold - retrieved   = {acc['gold'] - acc['retrieved']:+.4f}"
              "   what this policy lost against perfect retrieval")
    if {"parametric", "empty"} <= acc.keys():
        print(f"  parametric - empty = {acc['parametric'] - acc['empty']:+.4f}"
              "   knowledge the IDK instruction suppresses")

    # Per config retrieved accuracy: the ranking this whole exercise exists for.
    ret = [r for r in by_arm["retrieved"] if not r["errored"] and r.get("config")]
    if ret:
        per: dict[str, list[int]] = defaultdict(list)
        for r in ret:
            per[r["config"]].append(r["correct"])
        print(f"\n  retrieved accuracy by config ({len(per)} configs)")
        for cfg_name, vals in sorted(per.items(),
                                     key=lambda kv: -sum(kv[1]) / len(kv[1])):
            print(f"    {sum(vals)/len(vals):.4f}  n={len(vals):<6} {cfg_name}")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Offline parametric/empty/retrieved/gold reader probe.")
    ap.add_argument("run_dir", type=Path,
                    help="a config dir with results.csv + gold_answer_snapshot.jsonl, "
                         "or any tree containing them")
    ap.add_argument("--limit", type=int, default=0,
                    help="probe only the first N UNIQUE questions")
    ap.add_argument("--dataset", default=None, help="substring filter on the dataset column")
    ap.add_argument("--out", type=Path, default=REPO / "artifacts" / "reader_probe")
    ap.add_argument("--token-budget", type=int, default=None,
                    help="evidence budget (default: the reader's own). HOLD THIS "
                         "CONSTANT across a reader sweep: handing a larger model "
                         "more evidence confounds capacity with evidence volume.")
    ap.add_argument("--retrieval", type=Path, default=None, metavar="PATH",
                    help="campaign root holding evidence_items*.jsonl; adds the "
                         "'retrieved' arm, read per config from each config's "
                         "own retrieved_index.jsonl")
    ap.add_argument("--workers", type=int,
                    default=int(os.environ.get("READER_WORKERS", "32")),
                    help="concurrent reader calls (default 32 or $READER_WORKERS)")
    ap.add_argument("--skip-shared", action="store_true",
                    help="retrieved arm only; the shared arms are already done")
    ap.add_argument("--dry-run", action="store_true",
                    help="count the work and exit without calling the model")
    ap.add_argument("--resume", action="store_true",
                    help="proceed even though the output file already holds "
                         "every task for this reader. Without it a complete "
                         "file is refused, so a second pass under changed "
                         "settings (a new render version, a new prompt) cannot "
                         "be appended to rows it does not match.")
    ap.add_argument("--ids", type=Path, default=None, metavar="FILE",
                    help="restrict to the query ids listed in FILE, one per "
                         "line (# comments and blanks ignored) — the smoke path")
    ap.add_argument("--data-dir", type=Path, default=REPO / "data",
                    help="raw dataset files, for the render-time date lookup")
    args = ap.parse_args()

    run_dirs = find_run_dirs(args.run_dir)
    if not run_dirs:
        print(f"no results.csv + {GOLD_SNAPSHOT} pair under {args.run_dir} "
              f"(was the run made with --gold-snapshot?)", file=sys.stderr)
        return 2
    print(f"{len(run_dirs)} config dir(s) under {args.run_dir}")

    # Unique questions across configs. The gold snapshot repeats the same
    # question set in every config, so this is a dedup, not a merge.
    unique: dict[str, dict] = {}
    per_config_q: dict[str, list[str]] = {}
    for d in run_dirs:
        qs = load_questions(d, args.dataset)
        key = config_key(d, args.run_dir)
        per_config_q[key] = [q["query_id"] for q in qs]
        for q in qs:
            unique.setdefault(q["query_id"], q)
    if not unique:
        print("no questions matched", file=sys.stderr)
        return 2

    qids = sorted(unique)
    if args.ids is not None:
        wanted = {ln.strip() for ln in args.ids.read_text().splitlines()
                  if ln.strip() and not ln.lstrip().startswith("#")}
        missing = wanted - set(qids)
        qids = [q for q in qids if q in wanted]
        per_config_q = {k: [q for q in v if q in wanted]
                        for k, v in per_config_q.items()}
        print(f"--ids: {len(wanted)} requested, {len(qids)} matched"
              + (f", {len(missing)} not in this run" if missing else ""))
        if not qids:
            print("no requested ids matched", file=sys.stderr)
            return 2
    if args.limit:
        qids = qids[:args.limit]
        keep = set(qids)
        per_config_q = {k: [q for q in v if q in keep] for k, v in per_config_q.items()}
    print(f"{len(unique)} unique questions"
          f"{f' (limited to {len(qids)})' if args.limit else ''}")

    items: dict[str, str] = {}
    if args.retrieval is not None:
        items = load_item_table(args.retrieval)
        print(f"item table: {len(items)} evidence items")
        if not items:
            print(f"no {EVIDENCE_GLOB} under {args.retrieval} "
                  f"(was the run made with --dump-retrieval?)", file=sys.stderr)
            return 2

    # Render-time date lookup, built ONCE on the main thread (the workers only
    # read it). A missing raw file is loud but not fatal: the run continues
    # undated rather than dying deep in a sweep.
    date_maps: dict[str, DateMap] = {}
    question_dates: dict[str, str] = {}
    for name in {DateMap.resolve_name(unique[q]["dataset"]) for q in qids}:
        if not name:
            continue
        try:
            date_maps[name] = DateMap.for_workload(name, args.data_dir)
            print(f"dates: {name} -> {len(date_maps[name]):,} entries")
        except (FileNotFoundError, ValueError) as e:
            print(f"  WARNING: no dates for {name} ({type(e).__name__}: {e}); "
                  f"its items render UNDATED", file=sys.stderr)
    if "longmemeval" in date_maps:
        try:
            question_dates = longmemeval_question_dates(
                args.data_dir / "longmemeval_s_cleaned.json")
            print(f"dates: {len(question_dates):,} longmemeval question dates")
        except FileNotFoundError as e:
            print(f"  WARNING: no question dates ({e})", file=sys.stderr)

    cfg = ReaderConfig(token_budget=args.token_budget) if args.token_budget \
        else ReaderConfig()
    reader = build_reader(get_llm(), cfg)

    args.out.mkdir(parents=True, exist_ok=True)
    path = args.out / f"{reader.name}.jsonl"
    seen = already_done(path)

    # ── build the task list ────────────────────────────────────────────────
    tasks: list[dict] = []
    expected: set[tuple[str, str, str]] = set()      # every key this run covers

    if not args.skip_shared:
        for qid in qids:
            for arm in SHARED_ARMS:
                expected.add(("", qid, arm))
                if ("", qid, arm) in seen:
                    continue
                tasks.append({"config": "", "q": unique[qid], "arm": arm})
    n_shared = len(tasks)

    dangling = 0
    if args.retrieval is not None:
        for d in run_dirs:
            key = config_key(d, args.run_dir)
            index = load_index_for(d)
            if not index:
                print(f"  WARNING: no {RETRIEVED_INDEX} in {key}", file=sys.stderr)
                continue
            for qid in per_config_q.get(key, []):
                row = index.get(qid)
                if row is None:
                    continue
                dangling += sum(1 for r in row["retrieved"] if r["key"] not in items)
                expected.add((key, qid, "retrieved"))
                if (key, qid, "retrieved") in seen:
                    continue
                tasks.append({"config": key, "q": unique[qid], "arm": "retrieved",
                              "index_row": row, "items": items})
    n_retrieved = len(tasks) - n_shared

    if dangling:
        # Dangling keys replay as EMPTY TEXT, which deflates the retrieved arm
        # and therefore inflates the apparent policy loss. A wrong number, not
        # a missing one.
        print(f"  WARNING: {dangling} dangling keys — an incomplete item table. "
              "Those items replay as empty text and the retrieved arm is not "
              "trustworthy until every evidence_items shard is present.",
              file=sys.stderr)

    print(f"tasks: {n_shared} shared ({'/'.join(SHARED_ARMS)}, once per question) "
          f"+ {n_retrieved} retrieved (per config) = {len(tasks)}"
          f"{f', {len(seen)} already done' if seen else ''}")
    print(f"workers: {args.workers}   ->  {path}")

    if expected and expected <= seen and not args.resume:
        print(f"REFUSING: {path} already holds all {len(expected)} tasks for "
              f"this reader. Appending a second pass would mix rows from two "
              f"settings in one file — if this run differs (render version, "
              f"prompt, budget) write it to a new --out. Pass --resume to "
              f"proceed anyway.", file=sys.stderr)
        return 2

    if args.dry_run:
        return 0
    if not tasks:
        print("nothing to do")
        return 0

    # ── run ────────────────────────────────────────────────────────────────
    work = make_worker(reader, cfg, date_maps, question_dates)
    written = 0
    t0 = time.time()
    lock = threading.Lock()          # sink is main thread only; lock guards stdout
    with path.open("a", encoding="utf-8") as sink:
        for task, out in imap_bounded(work, tasks, args.workers):
            sink.write(json.dumps(row_for(task, out, cfg, reader),
                                  ensure_ascii=False) + "\n")
            sink.flush()             # resumable: a kill loses at most one row
            written += 1
            if written % 200 == 0 or written == len(tasks):
                rate = written / max(1e-9, time.time() - t0)
                eta = (len(tasks) - written) / max(1e-9, rate)
                with lock:
                    print(f"  [{written}/{len(tasks)}] {rate:.1f} rows/s  "
                          f"eta {eta/60:.1f} min", flush=True)

    with path.open(encoding="utf-8") as f:
        allrows = [json.loads(l) for l in f if l.strip()]
    report(allrows)
    print(f"\nwrote {path} ({len(allrows)} rows, {written} new, "
          f"{time.time()-t0:.0f}s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
