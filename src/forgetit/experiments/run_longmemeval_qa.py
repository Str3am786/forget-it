
from __future__ import annotations

import argparse
from pathlib import Path
import json
import logging
from typing import Callable, Iterator

from forgetit.benchmark.pipeline import run_pipeline_concurrent
from forgetit.benchmark.budget import example_footprints, DEFAULT_BUDGET_FRACTIONS
from forgetit.benchmark.workloads.longmemeval import (
    LongMemEvalWorkload,
    SessionItemiser,
    TurnPairItemiser,
)
from forgetit.memory_system import RetentionManager, EmptyStore
from forgetit.memory_system.wrappers.memgpt_sys import LettaWrapper
from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.logging import BenchmarkLogger
from forgetit.retrieval import LexicalOverlapRetriever, BM25Retriever, EmbeddingRetriever
from forgetit.policies import LRUPolicy, LFUPolicy, RandomPolicy, LLMCompressionPolicy, EbbinghausPolicy, \
    MemoryBankCompressionPolicy, KeepSmallestPolicy, EvictSmallestPolicy, \
    SemanticRedundancyPolicy, SemanticCoveragePolicy
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


# Sentinel initial budget for relative-budget mode: the runner overrides this
# per example before any insert, so it just needs to start effectively unbounded.
_BUDGET_SENTINEL = 1 << 62


def build_retention_store(
    *,
    budget_frac: float,
    policy,
    retriever,
    policy_name: str,
    retriever_name: str,
    embedder=None,
    attach_vectors: bool = False,
    probe_touch_mode: str = "original",
) -> tuple[str, RetentionManager, dict]:
    backend = InMemoryBackend(retriever=retriever)
    logger = BenchmarkLogger()
    # The store carries the embedder when something needs item vectors: the dense
    # retriever ranks by them, a content-aware policy (semantic*) reads them, and
    # re-embed-on-compress refreshes them. ``attach_vectors`` (--embed) forces them
    # on regardless — e.g. a semantic policy under BM25 retrieval. Otherwise the
    # store leaves it None and item-vector ingestion / re-embed are skipped.
    needs_vectors = (
        isinstance(retriever, EmbeddingRetriever)
        or getattr(policy, "needs_embeddings", False)
        or attach_vectors
    )
    store_embedder = embedder if (embedder is not None and needs_vectors) else None
    store = RetentionManager(
        budget_bytes=_BUDGET_SENTINEL,   # runner sets per-example budget = frac × footprint
        backend=backend,
        policy=policy,
        logger=logger,
        embedder=store_embedder,
        probe_touch_mode=probe_touch_mode,
    )
    pct = f"{int(round(budget_frac * 100)):03d}pct"
    label = f"retention/{policy_name}/{retriever_name}/budget_{pct}"
    # Non-default touch modes get their own artifact directory; "original" keeps
    # the legacy unlabelled path so existing runs reproduce byte-for-byte. The
    # probe_touch_mode CSV column disambiguates in either case.
    if probe_touch_mode != "original":
        label += f"/touch_{probe_touch_mode}"
    return label, store, {
        "budget_frac": budget_frac,
        "policy_label": policy_name,
        "retriever_label": retriever_name,
        "probe_touch_mode": probe_touch_mode,
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


def build_empty_store() -> tuple[str, EmptyStore, dict]:
    """E2b parametric-knowledge floor: reader answers with ZERO evidence.

    policy_label 'empty_store_floor' + the floor/ label make these rows
    unmistakable — every end-to-end number is later reported as lift over them.
    """
    return "floor/empty_store", EmptyStore(), {
        "budget_frac": 0.0,
        "policy_label": "empty_store_floor",
        "retriever_label": "none",
    }


# ── Plans ─────────────────────────────────────────────────────────

PolicySpec = tuple[str, Callable]      # (name, zero-arg factory)
RetrieverSpec = tuple[str, Callable]   # (name, zero-arg factory)


def default_policy_specs(llm, seed: int = 42) -> list[PolicySpec]:
    """Zero-arg factories for every policy. Edit here to add/remove policies."""
    specs: list[PolicySpec] = [
        ("lru",            lambda: LRUPolicy()),
        ("lfu",            lambda: LFUPolicy()),
        ("random",         lambda: RandomPolicy(seed=seed)),
        ("keep_smallest",  lambda: KeepSmallestPolicy()),    # evict largest
        ("evict_smallest", lambda: EvictSmallestPolicy()),   # evict smallest
        # Content-aware (need --dense; fall back to size on non-dense runs).
        # Two side by side so cheap-proxy vs principled-coverage is itself a result:
        ("semantic",          lambda: SemanticRedundancyPolicy()),      # nearest-pair (cheap)
        ("semantic_coverage", lambda: SemanticCoveragePolicy()),        # marginal coverage (principled)
        #("ebbinghaus",  lambda: EbbinghausPolicy()),
    ]
    # LLM-driven policies compress/evict via the model itself, so they need an
    # endpoint even when the reader is disabled.  Skip them when llm is None
    # (e.g. --no-reader) so retrieval-only runs need no inference server.
    if llm is not None:
        specs.append(("compression", lambda: LLMCompressionPolicy(llm=llm, selector=LFUPolicy())))
        #specs.append(("memorybank",  lambda: MemoryBankCompressionPolicy(llm=llm, selector=EbbinghausPolicy())))
    return specs


def default_retriever_specs(embedder=None) -> list[RetrieverSpec]:
    specs: list[RetrieverSpec] = [
        ("lexical", LexicalOverlapRetriever),
        ("bm25",    BM25Retriever),
    ]
    # Dense (vector) retriever — opt-in (--dense), needs an embedding endpoint.
    # The retriever embeds the query; items are embedded at ingestion. Fixing the
    # retriever per comparison is on the caller; both run side-by-side here so the
    # BM25-vs-dense contrast is one sweep with everything else held constant.
    if embedder is not None:
        specs.append(("dense", lambda: EmbeddingRetriever(embedder=embedder)))
    return specs


def make_stores(
    policy_specs: list[PolicySpec],
    retriever_specs: list[RetrieverSpec],
    budget_fracs: list[float],
    include_letta: bool = False,
    embedder=None,
    attach_vectors: bool = False,
    probe_touch_mode: str = "original",
) -> Iterator[tuple[str, Callable[[], object], dict]]:
    """
    Yield ``(label, factory, {})`` triples — one per (policy × retriever × budget_frac).
    Each ``factory()`` returns a fresh ``(label, store, extras)`` triple.
    """
    for pname, policy_fn in policy_specs:
        for rname, retriever_fn in retriever_specs:
            for frac in budget_fracs:
                def _factory(fr=frac, pf=policy_fn, rf=retriever_fn,
                             pn=pname, rn=rname, emb=embedder, av=attach_vectors,
                             ptm=probe_touch_mode):
                    return build_retention_store(
                        budget_frac=fr, policy=pf(), retriever=rf(),
                        policy_name=pn, retriever_name=rn, embedder=emb,
                        attach_vectors=av, probe_touch_mode=ptm,
                    )
                pct = f"{int(round(frac * 100)):03d}pct"
                label = f"retention/{pname}/{rname}/budget_{pct}"
                if probe_touch_mode != "original":
                    label += f"/touch_{probe_touch_mode}"
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
    p.add_argument("--dense", action="store_true",
                   help="Also sweep a dense (embedding) retriever alongside lexical/BM25. "
                        "Requires an embedding endpoint (EMBED_* settings). Items are "
                        "embedded per example at ingestion; the query is embedded at "
                        "retrieval; compression re-embeds. Set EMBED_MODEL to your "
                        "shipping embedder (the default nomic is the comparison model).")
    p.add_argument("--embed", action="store_true",
                   help="Attach item vectors to EVERY store (build the embedder) WITHOUT "
                        "adding the dense retriever to the sweep. Use it to run a "
                        "content-aware policy (semantic/semantic_coverage) under BM25/lexical "
                        "retrieval, to exercise re-embed-on-compress, or to count vector bytes. "
                        "Semantic policies attach vectors on their own; --embed forces them "
                        "onto all configs. Needs an embedding endpoint (EMBED_* settings).")
    p.add_argument("--policies", nargs="+", default=None, metavar="NAME",
                   help="Restrict the sweep to these policy names (e.g. lru lfu). "
                        "Default: every registered policy.")
    p.add_argument("--retrievers", nargs="+", default=None, metavar="NAME",
                   help="Restrict to these retriever names (lexical/bm25/dense). "
                        "Default: every registered retriever (dense only with --dense).")
    p.add_argument("--budgets", nargs="+", type=float, default=None, metavar="FRAC",
                   help="Override the budget fractions of each example's footprint "
                        "(e.g. 1.0 0.9). Default: 1.0 0.75 0.50 0.25 0.10.")
    p.add_argument("--out-dir", type=str, default=str(REPO_ROOT / "artifacts" / "benchmark"),
                   help="Root directory for per-config result CSVs.")
    p.add_argument("--event-log", action="store_true",
                   help="Stream a per-eviction event trace (events.jsonl) next to each "
                        "config's CSVs — enables the survival/regret/value-decay analysis.")
    p.add_argument("--probe-touch-mode", choices=["original", "probes_blind", "probes_only"],
                   default="original",
                   help="Which retrievals update the LRU/LFU recency/frequency signal "
                        "(E3 probes-blind replay). original: every retrieval touches "
                        "(legacy). probes_blind: access-only probes do NOT touch; scored "
                        "queries do. probes_only: only probes touch. Retrieval results "
                        "are identical in all modes. Non-default modes add a "
                        "touch_<mode>/ segment to the output dir and are stamped into "
                        "the probe_touch_mode column of results.csv.")
    p.add_argument("--reader-token-budget", type=int, default=None, metavar="N",
                   help="E6c fair reader: give the prompt ONE total evidence budget of "
                        "~N tokens (len//4 approximation), filled whole-item in "
                        "retrieval-rank order with an explicit marker on a final "
                        "truncated item — instead of the legacy 5×300-char per-item "
                        "truncation. Default: legacy (reproduces old runs exactly). "
                        "Budgeted is the documented choice for new runs; ~375 matches "
                        "the legacy prompt size. Recorded in the reader label "
                        "(PlainTextReader_rtokN) and the reader_token_budget column.")
    p.add_argument("--empty-store-control", action="store_true",
                   help="E2b parametric-knowledge floor: replace the policy grid with a "
                        "single empty-store config per itemiser — scored queries reach "
                        "the reader with ZERO evidence and are graded normally. Rows are "
                        "labeled policy=empty_store_floor under floor/empty_store/. "
                        "Ignores --policies/--retrievers/--budgets; requires the reader.")
    p.add_argument("--workers", type=int, default=1,
                   help="Replay this many EXAMPLES concurrently within a config (thread "
                        "pool). Each example gets its own fresh, isolated store, so the "
                        "compression LLM-call waits of different examples overlap and one "
                        "config can saturate the vLLM replicas — while every eviction "
                        "decision inside an example stays serial and identical to a "
                        "single-store run. Configs run sequentially.")
    p.add_argument("--reader-workers", type=int, default=1,
                   help="Within each config, run the QA reader as a concurrent second pass "
                        "(store-free, pure I/O). Big speedup for the non-compression configs, "
                        "where the reader is ~all the LLM cost.")
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

    if args.empty_store_control and args.no_reader:
        raise SystemExit("--empty-store-control measures the reader's parametric "
                         "floor — it cannot run with --no-reader.")

    # The reader is the only thing that needs an LLM endpoint; skip it for
    # pure retrieval/eviction runs (e.g. while debugging policies on LUMI).
    if args.no_reader:
        llm = None
        reader_impl = None
        reader_name = "none"
    else:
        llm = get_llm()
        reader_impl = PlainTextReader(llm, token_budget=args.reader_token_budget)
        reader_name = getattr(reader_impl, "name", type(reader_impl).__name__)

    itemisers = [SessionItemiser(), TurnPairItemiser()]
    # Budgets are now a fraction of EACH example's own footprint (per-example
    # relative): 100% = no-eviction control, then 75/50/25/10%. The runner sizes
    # the store per example, so every fraction < 1.0 applies uniform pressure.
    budget_fracs = args.budgets if args.budgets is not None else DEFAULT_BUDGET_FRACTIONS

    # Flip this on when your Letta server is running.  The rest of the
    # experiment — policies, budgets, retrievers — stays identical.
    INCLUDE_LETTA = False

    # Flatten the full grid into one ordered list of (itemiser, label, factory)
    # configs, then take this shard's stride.  Output paths embed itemiser +
    # store_label, so each config lands in its own directory regardless of shard.
    all_configs: list[tuple[object, str, Callable]] = []
    if args.empty_store_control:
        # E2b floor: the policy/retriever/budget grid is moot (nothing is stored),
        # so don't derive it — deriving it would also drag in the embedder for
        # content-aware policies. One store-less config per itemiser.
        for itemiser in itemisers:
            all_configs.append((itemiser, "floor/empty_store", build_empty_store))
    else:
        # Policies first, then filter — we need the *selected* policy set to decide
        # whether an embedder is required (a content-aware policy needs item vectors
        # even under BM25 retrieval, so the embedder is not a dense-only concern).
        policy_specs = default_policy_specs(llm, seed=42)
        if args.policies is not None:
            want = set(args.policies)
            policy_specs = [s for s in policy_specs if s[0] in want]
            missing = want - {s[0] for s in policy_specs}
            if missing:
                raise SystemExit(f"--policies: unknown {sorted(missing)}; "
                                 f"have {[s[0] for s in default_policy_specs(llm)]}")

        # The embedder serves three consumers, not just dense retrieval: (1) the dense
        # retriever, (2) content-aware eviction policies (semantic*) reading item
        # vectors, (3) re-embed-on-compress. Build one shared instance (shared cache +
        # httpx pool) when any is in play. --embed forces item vectors onto every store
        # WITHOUT adding the dense retriever to the sweep.
        want_dense = args.dense or (args.retrievers is not None and "dense" in args.retrievers)
        content_aware = any(getattr(pf(), "needs_embeddings", False) for _, pf in policy_specs)
        embedder = None
        if want_dense or args.embed or content_aware:
            from forgetit.embeddings import get_embedder
            embedder = get_embedder()
            log.info("embedder enabled (model=%s) [dense=%s embed=%s content_aware=%s]",
                     getattr(embedder, "model", "?"), want_dense, args.embed, content_aware)

        retriever_specs = default_retriever_specs(embedder)
        if args.retrievers is not None:
            want = set(args.retrievers)
            retriever_specs = [s for s in retriever_specs if s[0] in want]
            missing = want - {s[0] for s in retriever_specs}
            if missing:
                raise SystemExit(f"--retrievers: unknown {sorted(missing)} "
                                 f"(note: 'dense' needs an embedder — pass --dense or --embed); "
                                 f"have {[s[0] for s in retriever_specs]}")

        for itemiser in itemisers:
            for label, factory, _ in make_stores(
                policy_specs, retriever_specs, budget_fracs,
                include_letta=INCLUDE_LETTA, embedder=embedder,
                attach_vectors=args.embed, probe_touch_mode=args.probe_touch_mode,
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

    # Per-example footprints (one workload replay per itemiser) — the denominator
    # for relative budgeting.  Computed only for itemisers this shard touches.
    # Floor mode stores nothing, so there is no budget to size (skip the replay).
    footprints: dict[str, list[int]] = {}
    if not args.empty_store_control:
        for name in {it.name for it, _, _ in my_configs}:
            fp = example_footprints(workloads[name])
            footprints[name] = fp
            mean = sum(fp) / len(fp) if fp else 0
            log.info("footprints[%s]: %d examples, mean=%.0f B, max=%d B",
                     name, len(fp), mean, max(fp) if fp else 0)

    def _run_one(cfg) -> str:
        itemiser, _label, factory = cfg
        # factory() returns (label, fresh_store, extras). The concurrent driver
        # needs a store FACTORY (one fresh store per example), so we read the
        # label/extras once and hand it a closure that rebuilds the store.
        label, _store, extras = factory()
        _, summary = run_pipeline_concurrent(
            dataset="longmemeval",
            workload=workloads[itemiser.name],
            store_factory=lambda f=factory: f()[1],
            store_label=label,
            k=5,
            seed=42,
            itemiser=itemiser.name,
            reader=reader_name,
            reader_impl=reader_impl,
            answer_max_tokens=128,
            example_workers=args.workers,
            reader_workers=args.reader_workers,
            out_dir=args.out_dir,
            write_events=args.event_log,
            example_footprints=footprints.get(itemiser.name),
            reader_token_budget=args.reader_token_budget or 0,
            **extras,
        )
        return (f"itemiser={itemiser.name:<12} store={label:<45} reader={reader_name:<12} "
                f"gold_survival={summary['gold_survival_rate']:.3f} hit@k={summary['hit_at_k']:.3f}")

    # Configs run sequentially; concurrency is across examples *within* each
    # config (args.workers), so one config saturates the GPUs on its own.
    for cfg in my_configs:
        print(_run_one(cfg))


if __name__ == "__main__":
    main()