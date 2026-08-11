"""Reader probe — what does the campaign's accuracy owe to the memory system?

An OFFLINE pass over a finished run's artefacts. No store, no policy, no
eviction, no retrieval is replayed: it reads the gold snapshot and results.csv
that a run already wrote and asks one reader three questions per query.

    parametric  no evidence AND no memories wrapper — the model asked directly.
                What it already knew before the memory system existed.
    empty       the memories wrapper with no evidence. Not a knowledge
                measurement: it measures whether the model obeys "say IDK when
                the memories are absent".
    retrieved   what this policy actually delivered at this budget, rebuilt
                from a --dump-retrieval artefact (needs --retrieval).
    gold        the gold items' ORIGINAL (pre-compression) text as evidence.
                The ceiling — what the reader scores when forgetting is not a
                factor at all.

The deltas are the point:

    gold - parametric   what the memory system actually contributes.
    gold - retrieved    what this policy lost against perfect retrieval.
    parametric - empty  how much real knowledge the IDK instruction suppresses.

A campaign reporting gold accuracy without the parametric arm cannot tell a
memory system that works from a model that already knew the answers.

Model choice comes from the environment (LLM_MODEL, LLM_BASE_URL), so sweeping
readers means re-running this under different env — no policies replayed, and
the disk cache makes a repeat pass free:

    LLM_MODEL=qwen2.5:14b python scripts/reader_probe.py artifacts/benchmark/... --limit 50

Rows append to <out>/<reader-name>.jsonl and are skipped if already present, so
an interrupted sweep resumes where it stopped.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from forgetit.benchmark.grading import answer_in_text, grade          # noqa: E402
from forgetit.core.schema import MemRecord, Question                  # noqa: E402
from forgetit.llm import get_llm                                      # noqa: E402
from forgetit.llm.readers import (                                    # noqa: E402
    ReaderConfig, build_context, build_parametric_context, build_reader,
)

# Order is the reading order of the report: what the model knows alone, what
# the instruction suppresses, what this policy delivered, what perfect
# retrieval would have delivered.
ARMS = ("parametric", "empty", "retrieved", "gold")
GOLD_SNAPSHOT = "gold_answer_snapshot.jsonl"
RETRIEVED_INDEX = "retrieved_index.jsonl"
EVIDENCE_GLOB = "evidence_items*.jsonl"


def find_run_dirs(root: Path) -> list[Path]:
    """Config directories holding both a results.csv and a gold snapshot."""
    if (root / GOLD_SNAPSHOT).is_file() and (root / "results.csv").is_file():
        return [root]
    return sorted({p.parent for p in root.rglob(GOLD_SNAPSHOT)
                   if (p.parent / "results.csv").is_file()})


def load_questions(run_dir: Path, dataset_filter: str | None) -> list[dict]:
    """Join the gold snapshot (evidence) to results.csv (question metadata).

    The snapshot carries one row per (query_id, gold_id) and no question text;
    results.csv carries the question but repeats it once per config. Group the
    first, take the first row of the second.
    """
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
            # compression. Using resident_text would measure the policy, not
            # the ceiling.
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


def load_retrieval(path: Path) -> tuple[dict[str, str], dict[str, dict]]:
    """Load a --dump-retrieval artefact pair into ({key: text}, {qid: index_row}).

    The item table is sharded — the launchers run one process per shard against
    a shared out-dir — so every ``evidence_items*.jsonl`` is globbed and merged.
    Keys are content hashes, so a cross-shard duplicate is the same text and
    collapses to one entry.
    """
    root = path if path.is_dir() else path.parent
    index_files = ([path] if path.is_file() and path.name == RETRIEVED_INDEX
                   else sorted(root.rglob(RETRIEVED_INDEX)))

    # The item table lives at the out-dir ROOT, which may be several levels
    # above a config dir — walk up until a shard file appears.
    items: dict[str, str] = {}
    probe = root
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

    index: dict[str, dict] = {}
    for f in index_files:
        with f.open(encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    r = json.loads(line)
                    index.setdefault(r["query_id"], r)
    return items, index


def records_from_index(row: dict, items: dict[str, str]) -> list[MemRecord]:
    """Rebuild what retrieval handed the reader, in rank order.

    Sorted by the stored ``rank`` rather than trusting list order — that is the
    whole reason rank is written explicitly.
    """
    return [MemRecord(id=r["id"], text=items.get(r["key"], ""))
            for r in sorted(row["retrieved"], key=lambda r: r["rank"])]


def contexts_for(q: dict, cfg: ReaderConfig,
                 index: dict[str, dict] | None = None,
                 items: dict[str, str] | None = None):
    """The arms, built with the public reader API only.

    ``retrieved`` uses the PROBE's budget and template, which may differ from
    the run's — that is the point of storing retrieval output rather than a
    rendered prompt. A bigger model can be given more evidence than the budget
    the original run happened to use.
    """
    question = Question(id=q["query_id"], text=q["question_text"])
    gold_records = [MemRecord(id=gid, text=text) for gid, text in q["gold"]]
    out = {
        "parametric": build_parametric_context(question),
        "empty": build_context(question, [], token_budget=cfg.token_budget,
                               prompt=cfg.prompt),
        "gold": build_context(question, gold_records,
                              token_budget=cfg.token_budget, prompt=cfg.prompt),
    }
    if index is not None and q["query_id"] in index:
        row = index[q["query_id"]]
        out["retrieved"] = build_context(
            question, records_from_index(row, items or {}),
            token_budget=cfg.token_budget, prompt=cfg.prompt)
    return out


def already_done(path: Path) -> set[tuple[str, str]]:
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
            seen.add((r.get("query_id", ""), r.get("arm", "")))
    return seen


def report(rows: list[dict]) -> None:
    by_arm = {a: [r for r in rows if r["arm"] == a] for a in ARMS}
    acc = {}
    print(f"\n{'arm':<12}{'n':>6}{'accuracy':>11}{'abstain':>10}{'error':>9}"
          f"{'contains':>10}")
    for arm in ARMS:
        rs = by_arm[arm]
        if not rs:
            print(f"{arm:<12}{0:>6}{'—':>11}{'—':>10}{'—':>9}{'—':>10}")
            continue
        # Errors are neither right nor wrong: they leave both the numerator and
        # the denominator, exactly as metrics.summarize now does.
        scored = [r for r in rs if not r["errored"]]
        n = len(scored)
        acc[arm] = sum(r["correct"] for r in scored) / n if n else float("nan")
        print(f"{arm:<12}{len(rs):>6}{acc[arm]:>11.4f}"
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


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Offline gold/empty/parametric reader probe over a finished run.")
    ap.add_argument("run_dir", type=Path,
                    help="a config dir with results.csv + gold_answer_snapshot.jsonl, "
                         "or any tree containing them")
    ap.add_argument("--limit", type=int, default=0, help="probe only the first N questions")
    ap.add_argument("--dataset", default=None, help="substring filter on the dataset column")
    ap.add_argument("--out", type=Path, default=REPO / "artifacts" / "reader_probe")
    ap.add_argument("--token-budget", type=int, default=None,
                    help="evidence budget (default: the reader's own). May "
                         "differ from the run's — a larger model can be given "
                         "more evidence than the run's budget allowed.")
    ap.add_argument("--retrieval", type=Path, default=None, metavar="PATH",
                    help="a --dump-retrieval run dir or retrieved_index.jsonl; "
                         "adds the 'retrieved' arm (what this policy actually "
                         "delivered) beside the gold ceiling")
    args = ap.parse_args()

    run_dirs = find_run_dirs(args.run_dir)
    if not run_dirs:
        print(f"no results.csv + {GOLD_SNAPSHOT} pair under {args.run_dir} "
              f"(was the run made with --gold-snapshot?)", file=sys.stderr)
        return 2

    questions = [q for d in run_dirs for q in load_questions(d, args.dataset)]
    if args.limit:
        questions = questions[:args.limit]
    if not questions:
        print("no questions matched", file=sys.stderr)
        return 2

    items: dict[str, str] = {}
    index: dict[str, dict] | None = None
    if args.retrieval is not None:
        items, index = load_retrieval(args.retrieval)
        if not index:
            print(f"no {RETRIEVED_INDEX} under {args.retrieval} "
                  f"(was the run made with --dump-retrieval?)", file=sys.stderr)
            return 2
        dangling = sum(1 for row in index.values() for r in row["retrieved"]
                       if r["key"] not in items)
        print(f"retrieval: {len(index)} indexed queries, {len(items)} evidence "
              f"items, {dangling} dangling keys")
        if dangling:
            print("  WARNING: dangling keys mean an incomplete item table — "
                  "those items replay as empty text", file=sys.stderr)

    cfg = ReaderConfig(token_budget=args.token_budget) if args.token_budget \
        else ReaderConfig()
    reader = build_reader(get_llm(), cfg)

    args.out.mkdir(parents=True, exist_ok=True)
    path = args.out / f"{reader.name}.jsonl"
    seen = already_done(path)
    print(f"{len(questions)} questions x {len(ARMS)} arms -> {path}"
          f"{f' ({len(seen)} already done)' if seen else ''}")

    rows: list[dict] = []
    with path.open("a", encoding="utf-8") as sink:
        for i, q in enumerate(questions, 1):
            idx_row = (index or {}).get(q["query_id"], {})
            for arm, ctx in contexts_for(q, cfg, index, items).items():
                if (q["query_id"], arm) in seen:
                    continue
                res = reader.answer_from_context(ctx)
                g = grade(res.answer, q["ref_answer"], abstained=res.abstained)
                row = {
                    "query_id": q["query_id"], "arm": arm,
                    "dataset": q["dataset"], "question_type": q["question_type"],
                    "question": q["question_text"], "ref_answer": q["ref_answer"],
                    "answer": res.answer,
                    "abstained": res.abstained, "errored": res.errored,
                    "correct": g["correct"], "f1": round(g["f1"], 4),
                    "tier": g["tier"],
                    # Secondary signal: is the reference present in the answer
                    # at all? Catches answers the exact grader calls wrong.
                    "contains": answer_in_text(q["ref_answer"], res.answer),
                    "n_delivered": ctx.n_delivered, "n_offered": ctx.n_offered,
                    "truncated": ctx.truncated,
                    # BOTH sides recorded, never one: the replay may legitimately
                    # use a different budget or wording from the run, and a row
                    # that named only one of them could not say which.
                    "replay_token_budget": cfg.token_budget,
                    "replay_prompt_sha": ctx.prompt_sha,
                    "run_token_budget": idx_row.get("run_token_budget"),
                    "run_prompt_sha": idx_row.get("run_prompt_sha"),
                    "cap_hit": res.meta.get("cap_hit"),
                    "reader": reader.name, "model": getattr(reader.llm, "model", ""),
                }
                sink.write(json.dumps(row, ensure_ascii=False) + "\n")
                sink.flush()          # resumable: a kill loses at most one row
                rows.append(row)
            print(f"  [{i}/{len(questions)}] {q['query_id'][:48]}", flush=True)

    # Report over the whole file, so a resumed run summarises everything.
    with path.open(encoding="utf-8") as f:
        allrows = [json.loads(l) for l in f if l.strip()]
    report(allrows)
    print(f"\nwrote {path} ({len(allrows)} rows, {len(rows)} new)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
