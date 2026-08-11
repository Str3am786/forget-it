
from __future__ import annotations

import argparse
from pathlib import Path
import json
import logging
from typing import Callable, Iterator

from forgetit.benchmark.pipeline import RetrievalDump, run_pipeline_concurrent
from forgetit.benchmark.budget import example_footprints, DEFAULT_BUDGET_FRACTIONS
from forgetit.benchmark.workloads.longmemeval import (
    LongMemEvalWorkload,
    SessionItemiser,
    TurnPairItemiser,
)
from forgetit.memory_system import RetentionManager
from forgetit.memory_system.wrappers.memgpt_sys import LettaWrapper
from forgetit.backend.in_memory import InMemoryBackend
from forgetit.core.logging import BenchmarkLogger
from forgetit.retrieval import LexicalOverlapRetriever, BM25Retriever, EmbeddingRetriever
from forgetit.policies import LRUPolicy, LFUPolicy, FIFOPolicy, RandomPolicy, LLMCompressionPolicy, EbbinghausPolicy, \
    MemoryBankCompressionPolicy, KeepSmallestPolicy, EvictSmallestPolicy, \
    SemanticRedundancyPolicy, SemanticCoveragePolicy, SemanticKeepSimilarPolicy, \
    SalienceDecayPolicy, GDSFPolicy, OraclePolicy
from forgetit.llm import get_llm
from forgetit.llm.readers import build_reader, prompt_sha
from forgetit.experiments._reader_cli import (
    add_reader_args,
    reader_config_from_args,
    validate_reader_args,
)
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
    )
    pct = f"{int(round(budget_frac * 100)):03d}pct"
    label = f"retention/{policy_name}/{retriever_name}/budget_{pct}"
    return label, store, {
        "budget_frac": budget_frac,
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


def default_policy_specs(llm, seed: int = 42, max_compress_bytes: int | None = None) -> list[PolicySpec]:
    """Zero-arg factories for every policy. Edit here to add/remove policies."""
    specs: list[PolicySpec] = [
        ("lru",            lambda: LRUPolicy()),
        ("lfu",            lambda: LFUPolicy()),
        ("fifo",           lambda: FIFOPolicy()),
        ("random",         lambda: RandomPolicy(seed=seed)),
        ("keep_smallest",  lambda: KeepSmallestPolicy()),    # evict largest
        ("evict_smallest", lambda: EvictSmallestPolicy()),   # evict smallest
        # Value-per-byte (density) family — metadata-only, no LLM/embeddings:
        ("salience_decay", lambda: SalienceDecayPolicy()),   # FiFA priority-decay density
        ("gdsf",           lambda: GDSFPolicy()),            # GreedyDual-Size w/ Frequency
        ("gdsf_frozen",    lambda: GDSFPolicy(freeze_L=True)),  # L≡0 ablation: pure frequency-density
        # Content-aware (need --dense; fall back to size on non-dense runs).
        # Two side by side so cheap-proxy vs principled-coverage is itself a result:
        ("semantic",          lambda: SemanticRedundancyPolicy()),      # nearest-pair (cheap)
        ("semantic_coverage", lambda: SemanticCoveragePolicy()),        # marginal coverage (principled)
        ("semantic_keep",     lambda: SemanticKeepSimilarPolicy()),     # centroid coherence (mirror)
        ("ebbinghaus",  lambda: EbbinghausPolicy()),
        # Privileged upper-bound control (runner injects gold ids) — the
        # normalized-regret ceiling, not an implementable policy:
        ("oracle",      lambda: OraclePolicy()),
    ]
    # LLM-driven policies need an endpoint even when the reader is disabled, so
    # skip them when llm is None (--no-reader) → retrieval-only runs need no server.
    if llm is not None:
        specs.append(("compression", lambda: LLMCompressionPolicy(
            llm=llm, selector=LFUPolicy(),
            max_compress_bytes=max_compress_bytes)))
        specs.append(("memorybank", lambda: MemoryBankCompressionPolicy(
            llm=llm, selector=EbbinghausPolicy(),
            max_compress_bytes=max_compress_bytes)))
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
) -> Iterator[tuple[str, Callable[[], object], dict]]:
    """
    Yield ``(label, factory, {})`` triples — one per (policy × retriever × budget_frac).
    Each ``factory()`` returns a fresh ``(label, store, extras)`` triple.
    """
    for pname, policy_fn in policy_specs:
        for rname, retriever_fn in retriever_specs:
            for frac in budget_fracs:
                def _factory(fr=frac, pf=policy_fn, rf=retriever_fn,
                             pn=pname, rn=rname, emb=embedder, av=attach_vectors):
                    
                    label, store, extras = build_retention_store(
                        budget_frac=fr, policy=pf(), retriever=rf(),
                        policy_name=pn, retriever_name=rn, embedder=emb,
                        attach_vectors=av,
                    )
                    if pn.startswith("random_s"):        # --seeds sweep: seed column
                        extras["seed"] = int(pn.rsplit("_s", 1)[1])
                    return label, store, extras
                pct = f"{int(round(frac * 100)):03d}pct"
                label = f"retention/{pname}/{rname}/budget_{pct}"
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
    p.add_argument("--seeds", nargs="+", type=int, default=None, metavar="SEED",
                   help="Seed sweep for the stochastic policy (random): one config "
                        "per seed, labeled random_s<seed> — so each seed gets its own "
                        "artifact dir, config hash (skip-if-done), and seed column. "
                        "Deterministic policies are unaffected.")
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
    p.add_argument("--include-floor", action="store_true",
                   help="Add the E2b empty-store floor config (one per itemiser): the reader answers every question with ZERO evidence, measuring parametric knowledge. Answer-stage tables report lift over this floor; a grid without its floor row per dataset+itemiser is INCOMPLETE.")
    p.add_argument("--event-log", action="store_true",
                   help="Stream a per-eviction event trace (events.jsonl) next to each "
                        "config's CSVs — enables the survival/regret/value-decay analysis.")
    p.add_argument("--gold-snapshot", action="store_true",
                   help="write gold_answer_snapshot.jsonl: original vs retained "
                        "text per (query, gold id) at final store state. Needed "
                        "to measure answer preservation under compression.")
    p.add_argument("--compress-context-tokens", type=int, default=32_000,
                   help="the compressor model's context window; items whose "
                        "prompt would not fit are skipped rather than sent.")
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
    add_reader_args(p)
    args = p.parse_args()
    if args.num_shards < 1:
        p.error("--num-shards must be >= 1")
    if not (0 <= args.shard < args.num_shards):
        p.error(f"--shard must be in [0, {args.num_shards})")
    validate_reader_args(p, args)
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
    seed = 42
    # Built UNCONDITIONALLY: --dump-retrieval records the evidence budget and
    # prompt template a reader WOULD have used, so the config is needed even
    # when no reader runs. Only build_reader() stays behind --no-reader.
    reader_cfg = reader_config_from_args(args)
    retrieval_dump = (
        RetrievalDump(shard=args.shard,
                      token_budget=reader_cfg.token_budget,
                      prompt_sha=prompt_sha(reader_cfg.prompt))
        if args.dump_retrieval else None
    )

    if args.no_reader:
        llm = None
        reader_impl = None
        reader_name = "none"
    else:
        llm = get_llm()
        try:
            reader_impl = build_reader(llm, reader_cfg)
        except ValueError as exc:
            raise SystemExit(f"--reader-prompt: {exc}")
        reader_name = getattr(reader_impl, "name", type(reader_impl).__name__)

    # LLM-driven POLICIES (compression/memorybank) are gated on an endpoint, not
    # on the reader: --no-reader turns off the ANSWER stage only. When such a
    # policy is explicitly requested, build the chat client anyway — the
    # state-forming half's compression calls need it. Without --policies, the
    # old behavior stands (retrieval-only runs need no inference server).
    _LLM_POLICY_NAMES = {"compression", "memorybank"}
    if llm is None and args.policies and set(args.policies) & _LLM_POLICY_NAMES:
        llm = get_llm()


    itemisers = [SessionItemiser(), TurnPairItemiser()]
    # Budgets are now a fraction of EACH example's own footprint (per-example
    # relative): 100% = no-eviction control, then 75/50/25/10%. The runner sizes
    # the store per example, so every fraction < 1.0 applies uniform pressure.
    budget_fracs = args.budgets if args.budgets is not None else DEFAULT_BUDGET_FRACTIONS

    # Flip this on when your Letta server is running.  The rest of the
    # experiment — policies, budgets, retrievers — stays identical.
    INCLUDE_LETTA = False

    # Policies first, then filter — we need the *selected* policy set to decide
    # whether an embedder is required (a content-aware policy needs item vectors
    # even under BM25 retrieval, so the embedder is not a dense-only concern).
    max_compress_bytes = (args.compress_context_tokens - 512) * 4

    policy_specs = default_policy_specs(llm, seed=seed,
                                        max_compress_bytes=max_compress_bytes)
    if args.policies is not None:
        want = set(args.policies)
        policy_specs = [s for s in policy_specs if s[0] in want]
        missing = want - {s[0] for s in policy_specs}
        if missing:
            raise SystemExit(f"--policies: unknown {sorted(missing)}; "
                             f"have {[s[0] for s in default_policy_specs(llm)]}")


    # --seeds: expand the stochastic policy into one config per seed. The label
    # carries the seed; make_stores parses it back out so the results rows'
    # seed column records the actual draw seed, not the module default.
    if args.seeds:
        expanded = []
        for _name, _fn in policy_specs:
            if _name == "random":
                for s in args.seeds:
                    expanded.append((f"random_s{s}", lambda s=s: RandomPolicy(seed=s)))
            else:
                expanded.append((_name, _fn))
        policy_specs = expanded

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

    # Flatten the full grid into one ordered list of (itemiser, label, factory)
    # configs, then take this shard's stride.  Output paths embed itemiser +
    # store_label, so each config lands in its own directory regardless of shard.
    all_configs: list[tuple[object, str, Callable]] = []
    for itemiser in itemisers:
        for label, factory, _ in make_stores(
            policy_specs, retriever_specs, budget_fracs,
            include_letta=INCLUDE_LETTA, embedder=embedder,
            attach_vectors=args.embed,
        ):
            all_configs.append((itemiser, label, factory))

    # E2b floor: one empty-store config per itemiser. EmptyStore retains
    # nothing, so graded answers measure the reader's parametric knowledge —
    # the floor every end-to-end number is reported as lift over.
    if args.include_floor:
        from forgetit.memory_system import EmptyStore

        def _floor_factory():
            return "floor/empty", EmptyStore(), {
                "policy_label": "floor", "retriever_label": "none",
                "budget_frac": 0.0,
            }

        for itemiser in itemisers:
            all_configs.append((itemiser, "floor/empty", _floor_factory))

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
    footprints: dict[str, list[int]] = {}
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
            seed=extras.pop("seed", 42),
            itemiser=itemiser.name,
            reader=reader_name,
            reader_impl=reader_impl,
            example_workers=args.workers,
            reader_workers=args.reader_workers,
            out_dir=args.out_dir,
            write_events=args.event_log,
            write_gold_snapshot=args.gold_snapshot,
            retrieval_dump=retrieval_dump,
            example_footprints=footprints.get(itemiser.name),
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