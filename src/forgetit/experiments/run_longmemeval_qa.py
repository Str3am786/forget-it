
from __future__ import annotations

import argparse
from pathlib import Path
import json
import logging
from typing import Callable, Iterator

from forgetit.benchmark.pipeline import run_pipeline
from forgetit.benchmark.workloads.longmemeval import (
    LongMemEvalWorkload,
    SessionItemiser,
    TurnPairItemiser,
)
from forgetit.memory_system import RetentionManager
from forgetit.memory_system.wrappers.memgpt_sys import LettaWrapper
from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.logging import BenchmarkLogger
from forgetit.retrieval import LexicalOverlapRetriever, BM25Retriever
from forgetit.policies import LRUPolicy, LFUPolicy, RandomPolicy, LLMCompressionPolicy, EbbinghausPolicy, \
    MemoryBankCompressionPolicy
from forgetit.llm import get_llm
from forgetit.llm.readers import PlainTextReader
from forgetit.settings import LETTA_BASE_URL, LETTA_API_KEY


REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = REPO_ROOT / "data"


# def load_ref_answers(path: Path) -> dict[str, dict]:
#     data = json.loads(path.read_text(encoding="utf-8"))
#     refs = {}
#     for ex in data:
#         qid = str(ex.get("question_id", ""))
#         refs[qid] = {
#             "answer": ex.get("answer", ""),
#             "question_type": ex.get("question_type", ""),
#         }
#     return refs


# ── Store builders ────────────────────────────────────────────────
# Each builder returns a ``(label, store, extras)`` triple:
#   label   — store identity used in artifact paths + EvalResult.store
#   store   — fresh MemorySystem instance (never reuse — clear() runs at end)
#   extras  — kwargs forwarded to run_pipeline for reporting only
#             (budget_bytes / policy_label / retriever_label columns in the CSV)


def build_retention_store(
    *,
    budget: int,
    policy,
    retriever,
    policy_name: str,
    retriever_name: str,
) -> tuple[str, RetentionManager, dict]:
    backend = InMemoryBackend(retriever=retriever)
    logger = BenchmarkLogger()
    store = RetentionManager(
        budget_bytes=budget,
        backend=backend,
        policy=policy,
        logger=logger,
    )
    label = f"retention/{policy_name}/{retriever_name}/budget_{budget}"
    return label, store, {
        "budget_bytes": budget,
        "policy_label": policy_name,
        "retriever_label": retriever_name,
    }


def build_letta_store(
    *,
    model: str = "ollama/qwen2.5:7b-instruct-q5_K_M",
    embedding: str = "ollama/nomic-embed-text:latest",
    base_url: str = LETTA_BASE_URL,
) -> tuple[str, LettaWrapper, dict]:
    store = LettaWrapper(base_url=base_url, model=model, embedding=embedding, api_key=LETTA_API_KEY)
    # Strip the ollama/ prefix for a cleaner label
    short_model = model.split("/")[-1].split(":")[0]
    label = f"letta/{short_model}"
    return label, store, {}       # no budget / policy / retriever labels


# ── Plans ─────────────────────────────────────────────────────────

PolicySpec = tuple[str, Callable]      # (name, zero-arg factory)
RetrieverSpec = tuple[str, Callable]   # (name, zero-arg factory)


def default_policy_specs(llm, seed: int = 42) -> list[PolicySpec]:
    """Zero-arg factories for every policy. Edit here to add/remove policies."""
    specs: list[PolicySpec] = [
        ("lru",         lambda: LRUPolicy()),
        ("lfu",         lambda: LFUPolicy()),
        ("random",      lambda: RandomPolicy(seed=seed)),
        #("ebbinghaus",  lambda: EbbinghausPolicy()),
    ]
    # LLM-driven policies compress/evict via the model itself, so they need an
    # endpoint even when the reader is disabled.  Skip them when llm is None
    # (e.g. --no-reader) so retrieval-only runs need no inference server.
    if llm is not None:
        specs.append(("compression", lambda: LLMCompressionPolicy(llm=llm, selector=LFUPolicy())))
        #specs.append(("memorybank",  lambda: MemoryBankCompressionPolicy(llm=llm, selector=EbbinghausPolicy())))
    return specs


def default_retriever_specs() -> list[RetrieverSpec]:
    return [
        ("lexical", LexicalOverlapRetriever),
        ("bm25",    BM25Retriever),
    ]


def make_stores(
    policy_specs: list[PolicySpec],
    retriever_specs: list[RetrieverSpec],
    budgets: list[int],
    include_letta: bool = False,
) -> Iterator[tuple[str, Callable[[], object], dict]]:
    """
    Yield ``(label, factory, {})`` triples — one per (policy × retriever × budget).
    Each ``factory()`` returns a fresh ``(label, store, extras)`` triple.
    """
    for pname, policy_fn in policy_specs:
        for rname, retriever_fn in retriever_specs:
            for budget in budgets:
                def _factory(b=budget, pf=policy_fn, rf=retriever_fn,
                             pn=pname, rn=rname):
                    return build_retention_store(
                        budget=b, policy=pf(), retriever=rf(),
                        policy_name=pn, retriever_name=rn,
                    )
                label = f"retention/{pname}/{rname}/budget_{budget}"
                yield label, _factory, {}

    if include_letta:
        yield "letta/default", build_letta_store, {}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run the LongMemEval QA sweep. Sharding lets many workers "
                    "split the (itemiser × policy × retriever × budget) grid; "
                    "each config writes its own CSV, so shards never collide.",
    )
    p.add_argument("--num-shards", type=int, default=1,
                   help="Total number of parallel workers splitting the config grid.")
    p.add_argument("--shard", type=int, default=0,
                   help="This worker's index in [0, num-shards). "
                        "On SLURM, pass $SLURM_ARRAY_TASK_ID.")
    p.add_argument("--limit", type=int, default=None,
                   help="Only replay the first N examples (smoke test).")
    p.add_argument("--no-reader", action="store_true",
                   help="Skip the LLM reader — retrieval/eviction metrics only, "
                        "no inference endpoint required.")
    p.add_argument("--out-dir", type=str, default=str(REPO_ROOT / "artifacts" / "benchmark"),
                   help="Root directory for per-config result CSVs.")
    args = p.parse_args()
    if args.num_shards < 1:
        p.error("--num-shards must be >= 1")
    if not (0 <= args.shard < args.num_shards):
        p.error(f"--shard must be in [0, {args.num_shards})")
    return args


def main() -> None:
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger(__name__)

    data_path = DATA_DIR / "longmemeval_s_cleaned.json"
    if not data_path.exists():
        raise FileNotFoundError(f"Dataset not found: {data_path}")

    # The reader is the only thing that needs an LLM endpoint; skip it for
    # pure retrieval/eviction runs (e.g. while debugging policies on LUMI).
    if args.no_reader:
        llm = None
        reader_impl = None
        reader_name = "none"
    else:
        llm = get_llm()
        reader_impl = PlainTextReader(llm)
        reader_name = getattr(reader_impl, "name", type(reader_impl).__name__)

    itemisers = [SessionItemiser(), TurnPairItemiser()]
    budgets = [262_144, 524_288, 1_048_576, 2_097_152, 5_242_880]

    # Flip this on when your Letta server is running.  The rest of the
    # experiment — policies, budgets, retrievers — stays identical.
    INCLUDE_LETTA = False

    policy_specs = default_policy_specs(llm, seed=42)
    retriever_specs = default_retriever_specs()

    # Flatten the full grid into one ordered list of (itemiser, label, factory)
    # configs, then take this shard's stride.  Output paths embed itemiser +
    # store_label, so each config lands in its own directory regardless of shard.
    all_configs: list[tuple[object, str, Callable]] = []
    for itemiser in itemisers:
        for label, factory, _ in make_stores(
            policy_specs, retriever_specs, budgets, include_letta=INCLUDE_LETTA
        ):
            all_configs.append((itemiser, label, factory))

    my_configs = all_configs[args.shard::args.num_shards]
    log.info(
        "shard %d/%d: running %d of %d configs (limit=%s, reader=%s)",
        args.shard, args.num_shards, len(my_configs), len(all_configs),
        args.limit, reader_name,
    )

    # Build one workload per itemiser (cheap — just holds path + config) and reuse.
    # probe_every=1: access-only warmup probe after each session insert so
    # LRU/LFU get genuine reuse signal before eviction fires.
    # clear_between_examples=True: wipe the store at each example boundary so
    # examples are evaluated in isolation (intended eval semantics).
    workloads: dict[str, LongMemEvalWorkload] = {}
    for itemiser in itemisers:
        workloads[itemiser.name] = LongMemEvalWorkload(
            data_path, itemiser,
            probe_every=1,
            clear_between_examples=True,
            limit=args.limit,
        )

    for itemiser, _label, factory in my_configs:
        label, store, extras = factory()
        _, summary = run_pipeline(
            dataset="longmemeval",
            workload=workloads[itemiser.name],
            store=store,
            store_label=label,
            k=5,
            seed=42,
            itemiser=itemiser.name,
            reader=reader_name,
            reader_impl=reader_impl,
            answer_max_tokens=128,
            out_dir=args.out_dir,
            **extras,
        )
        print(
            f"itemiser={itemiser.name:<12} "
            f"store={label:<45} "
            f"reader={reader_name:<12} "
            f"gold_survival={summary['gold_survival_rate']:.3f} "
            f"hit@k={summary['hit_at_k']:.3f}"
        )


if __name__ == "__main__":
    main()