# forget-it

![Python](https://img.shields.io/badge/python-3.12%2B-blue.svg)
![status](https://img.shields.io/badge/status-research%20WIP-orange.svg)

**A policy-pluggable benchmarking framework for forgetting in local agentic memory under hard storage constraints.**

When an AI agent keeps persistent long-term memory, that memory eventually fills up. `forget-it` is the testbench for the decision that follows: *when you are out of room, what do you throw away, and how?* It models an agent's memory as a byte-bounded cache, lets you drop in any forgetting policy — from textbook LRU to LLM-driven summarization to cognitive forgetting curves — and measures, on a real long-conversation benchmark (LongMemEval), which policy keeps the agent able to actually answer questions.

Its distinguishing trick is **separating two failures most memory benchmarks blur together**: *"the policy deleted the answer"* versus *"the policy kept the answer but retrieval couldn't surface it."* The headline metric, `hit_at_k_given_retained`, conditions retrieval success on survival, so forgetting quality and retrieval quality can be judged independently.

---

## Contents

- [Quickstart](#quickstart)
- [Current state](#current-state-june-2026)
- [How it works](#how-it-works)
- [Architecture](#architecture)
- [Running experiments](#running-experiments)
- [Metrics](#metrics)
- [Extending the framework](#extending-the-framework)
- [Concept glossary](#concept-glossary)
- [License & citation](#license--citation)

---

## Quickstart

Requires **Python 3.12+**.

```bash
git clone https://github.com/Str3am786/forget-it
cd forget-it
pip install -e ".[dev]"

# Smoke test — no dataset, no LLM, runs in seconds
python -m forgetit.experiments.run_toy
```

`run_toy` inserts a handful of records into a tiny budget, forces evictions, and prints the result — enough to confirm the install works. From there, the [LongMemEval QA benchmark](#longmemeval-qa-benchmark-main-entry-point) is the main entry point.

---

## Current state (June 2026)

This is active research code. The core pipeline is solid and the headline experiment runs end-to-end; a few backends and metrics are still partial (flagged below).

### What works

- **End-to-end benchmark pipeline:** workload → runner → metrics → CSV, with pluggable policies, backends, retrievers, and readers.
- **Seven forgetting policies implemented and tested:** FIFO, LRU, LFU, Random, Ebbinghaus, and two LLM-driven policies (LLMCompression, MemoryBank). *The default LongMemEval QA grid enables **LRU, LFU, Random, and LLMCompression**; FIFO, Ebbinghaus, and MemoryBank are implemented and tested but commented out of the default sweep — uncomment them in `default_policy_specs` to include them.*
- **LRU/LFU diverge correctly from FIFO:** the LongMemEval workload interleaves **access-only probe queries** during inserts, so recency/frequency signal exists *before* eviction (they no longer silently collapse into FIFO — a subtle bug, now fixed and regression-tested).
- **Two-phase compress-to-pack compression:** shrink the least-useful *un-compressed* item to fit more memories; evict only when everything resident is already compressed. Each item is compressed at most once (no re-compression churn), with cost-awareness floors (`min_compress_bytes`, `min_saving_ratio`).
- **Hard byte-budget enforcement** with per-item accounting and a no-progress stall guard — a policy that frees nothing raises a precise error instead of hanging.
- **Three-layer evaluation:** policy verdict → retriever verdict → reader verdict (real LLM QA) via `PlainTextReader` / `JsonMemoryReader`.
- **Deterministic retrieval baselines:** lexical (token overlap) and BM25.
- **Validation harnesses:** a synthetic workload and an eviction-stress diagnostic that confirms policies actually diverge under pressure before you commit to a real run.
- **Built to run at scale:** `--shard/--num-shards` split the config grid (one CSV per config), `scripts/concat_results.py` merges them, and `scripts/lumi/` provides a single-node LUMI launcher (vLLM + sharded clients) for the LLM-heavy compression runs.

### Known issues & caveats

- **No automatic QA-correctness score (yet).** The reader records its raw answer alongside the reference answer and reports abstention rate and latency, but the pipeline does **not** compute answer accuracy — there is no exact-match or LLM-judge step. Treat the QA layer as *"raw answers to grade offline + abstention/latency,"* not a headline accuracy number. This is the most important missing piece if a graded correctness metric is your goal.
- **Compression vs. surface-token retrieval.** Compression rewrites text, so lexical/BM25 retrieval can miss a *surviving* compressed memory (you will see `gold_survived=True` but `hit@k=0`). A fair evaluation of the compression policies needs an embedding retriever, which is not wired in yet.
- **`FalkorBackend`** (`backend/falkor.py`) is only partially implemented; the graph migration is deprioritized. All current experiments use `InMemoryBackend`.
- **LLM-compression is call-heavy** (one LLM call per compression) and slow on a single local GPU — use sharding plus a batching server (vLLM) on a cluster.
- **Ebbinghaus approximation.** The policy measures elapsed time from `created_at` (age), not time-since-last-recall, because `last_access` is a logical tick counter not comparable to wall-clock `created_at`. Recall boosts strength but does not reset the decay clock — a deliberate approximation, not the literal MemoryBank formula.

---

## How it works

**The model: memory as a byte-bounded cache.** Each memory is a `MemRecord` with a byte cost. The `RetentionManager` enforces a fixed budget: every `insert()` runs a loop — *while over budget, ask the policy for an action and execute it.* An action is `Evict` (delete), `Compress` (replace text with a shorter summary and recompute bytes), or `Prune`. This reframes "what should an agent forget?" as a cache-eviction problem, which lets the framework borrow the whole vocabulary of cache research (FIFO/LRU/LFU/Random) and add the parts unique to language memory (LLM summarization, cognitive forgetting curves) behind one interface.

**The evaluation: three independent verdicts.** Every scored query is judged at three layers, so failures can be attributed precisely rather than lumped together:

1. **Policy / survival** — did the gold answer item survive eviction at all? (`gold_survived`) This is the ceiling on everything downstream.
2. **Retriever** — did a gold item land in the top-k? (`hit_at_k`, lexical or BM25)
3. **Reader** — given what was retrieved, what does a real LLM answer? (`reader_answer`, `abstained`)

The metric that ties it together is **`hit_at_k_given_retained`** — hit rate *conditioned on the item not having been evicted*. It cleanly separates "the policy threw the answer away" from "the policy kept it but search failed to surface it." Most memory benchmarks conflate those two; this one factors them apart.

The design keeps storage, retrieval, and policy fully **decoupled**, and the runner negotiates with whatever capabilities a store exposes (no `isinstance` branching): an introspectable store gets the full metric set, while an opaque external store (Letta/Mem0) degrades gracefully to retrieval-only metrics. New policies, datasets, and backends drop in without touching the pipeline.

---

## Architecture

```
forget-it/
├── data/                          # Datasets (not in package)
│   └── longmemeval_s_cleaned.json
│
├── artifacts/                     # Pipeline output
│   └── benchmark/
│       └── <dataset>/<itemiser>/<store_label>/<reader>/
│           ├── results.csv        # Per-query EvalResult rows (self-describing)
│           └── summary.csv        # Aggregated metrics (identified by path)
│
├── scripts/
│   ├── concat_results.py          # Merge per-config/per-shard CSVs → combined_*.csv
│   └── lumi/                      # LUMI launcher + cotainr env + setup notes
│       ├── run_lumi.sh
│       ├── forgetit-env.yml
│       └── README.md
│
├── src/forgetit/
│   ├── core/
│   │   ├── schema.py              # MemRecord, MemFeatures, Query, Question, AnswerResult
│   │   └── logging.py             # BenchmarkLogger, EventLogger protocol
│   │
│   ├── memory_system/
│   │   ├── base.py                # MemorySystem protocol
│   │   ├── custom_manager.py      # RetentionManager — budget enforcement, evict/compress loop
│   │   └── wrappers/memgpt_sys.py # LettaWrapper (opaque external store, optional)
│   │
│   ├── policies/
│   │   ├── base.py                # EvictionPolicy / SimpleEvictionPolicy
│   │   ├── actions.py             # Evict, Compress, Prune
│   │   ├── baselines.py           # FIFO, LRU, LFU, Random
│   │   ├── ebbinghaus.py          # EbbinghausPolicy + MemoryBankCompressionPolicy
│   │   └── llm_compression.py     # LLMCompressionPolicy (two-phase compress-to-pack)
│   │
│   ├── backend/
│   │   ├── backend.py             # Backend ABC
│   │   ├── in_memory.py           # Dict-based backend (used for all experiments)
│   │   └── falkor.py              # FalkorDB backend (partial, optional import)
│   │
│   ├── retrieval/
│   │   ├── base.py                # Retriever protocol
│   │   └── lexical.py             # LexicalOverlapRetriever + BM25Retriever
│   │
│   ├── benchmark/                 # ← Evaluation pipeline
│   │   ├── schema.py              # WorkloadEvent, EvalResult, QueryEval
│   │   ├── runner_core.py         # Replays workload, produces EvalResult per scored query
│   │   ├── pipeline.py            # Single-run orchestrator: store → run → metrics → CSV
│   │   ├── metrics.py             # Aggregation over EvalResult lists
│   │   └── workloads/
│   │       ├── base.py            # Workload + Itemiser protocols
│   │       ├── synthetic.py       # Controlled synthetic workload
│   │       ├── longmemeval.py     # LongMemEval adapter + Session/TurnPair itemisers
│   │       └── beam.py            # BEAM workload adapter
│   │
│   ├── experiments/               # Entry points (thin scripts that call the pipeline)
│   │   ├── run_toy.py             # Minimal smoke test (no dataset/LLM)
│   │   ├── run_synthetic.py       # Policies × budgets on the synthetic workload
│   │   ├── run_eviction_stress.py # Diagnostic: do policies diverge under pressure?
│   │   ├── run_longmemeval.py     # LongMemEval sweep (retrieval-focused)
│   │   ├── run_longmemeval_qa.py  # LongMemEval QA sweep + sharding (main entry point)
│   │   └── run_beam.py            # BEAM sweep
│   │
│   ├── llm/                       # OpenAI-compatible client + readers/
│   │   └── readers/               # PlainTextReader, JsonMemoryReader
│   ├── services/                  # MemoryQAService (higher-level orchestration)
│   └── pipelines/                 # LangGraph wiring (ingest, retrieve, QA nodes)
│
└── tests/
    ├── memory_sys/
    └── policies/                  # baselines + compression (two-phase) tests
```

### How the benchmark pipeline works

```
Experiment script (e.g. run_longmemeval_qa.py)
    │  enumerates configs: itemiser × policy × retriever × budget
    │  (--shard i --num-shards N runs this worker's slice)
    ▼
pipeline.run_pipeline()
    │  builds: RetentionManager(backend, policy, logger) + reader
    ▼
runner_core.run(workload, store, k, reader, ...)
    │  for each WorkloadEvent:
    │    INSERT  → store.insert(item)          (budget enforced; evict/compress inside)
    │    RESET   → store.clear()               (per-example isolation)
    │    QUERY (no gold)  → store.retrieve()   (access-only probe; updates LRU/LFU signal)
    │    QUERY (gold)     → SCORED:
    │        1. Policy:    gold_ids ∩ retained → gold_survived, ubf, waste
    │        2. Retriever: store.retrieve(q,k) → hit@k, mrr, hit@k_given_retained
    │        3. Reader:    LLM answer → reader_answer / abstained
    │      → append EvalResult
    ▼
list[EvalResult] → metrics.summarize() → dict
    ▼
artifacts/<dataset>/<itemiser>/<store_label>/<reader>/{results.csv, summary.csv}
    ▼
scripts/concat_results.py → combined_results.csv + combined_summary.csv
```

### How the memory system works

```
MemRecord
    │
    ▼
RetentionManager.insert()
    │  normalize bytes; stamp last_access = monotonic tick; fresh access_count = 0
    │  while over budget:  action = policy.select_action(store)
    │      Evict    → drop record, reclaim bytes
    │      Compress → replace text with summary, recompute bytes, mark meta["compressed"]
    │  stall guard: raises if an action neither frees bytes nor removes a record
    ▼
Backend.upsert()
    ▼
RetentionManager.retrieve(query, k)
    │  Backend.search(); bump access_count + last_access on hits (drives LRU/LFU/Ebbinghaus)
    ▼
list[MemRecord]
```

---

## Running experiments

### LLM configuration

The QA reader and the LLM-driven policies (compression) need an OpenAI-compatible endpoint. The client reads three env vars (a `.env` in the repo root is also loaded):

```bash
export LLM_BASE_URL="http://localhost:11434/v1"   # Ollama, vLLM, OpenAI, ...
export LLM_API_KEY="sk-your-key"                  # any non-empty string for local servers
export LLM_MODEL="your-model-name"                # must match the served model name
```

> The model must be instruction-tuned and follow extraction-style prompts. Verified to work: Llama-3-8B-Instruct, Mistral-7B-Instruct, Qwen2.5-7B-Instruct, GPT-4o-mini. Sanity-check the endpoint first:
> ```python
> from forgetit.llm import get_llm
> print(get_llm().generate("What is the capital of France?", max_tokens=10))
> # Must return something like "Paris." — not a paragraph
> ```

To run **retrieval/eviction only with no LLM at all**, use `--no-reader`; LLM-driven policies are auto-skipped in that mode.

### Diagnostics & smaller workloads

```bash
# Eviction stress — do policies actually diverge under pressure?
python -m forgetit.experiments.run_eviction_stress
#   Unprobed: all policies collapse to FIFO (no usage signal).
#   Probed:   LRU/LFU retain gold and beat FIFO/Random.
#   Run this to confirm policies diverge before a real run.

# Synthetic benchmark — policies × budgets on a controlled stream
python -m forgetit.experiments.run_synthetic
```

### LongMemEval QA benchmark (main entry point)

Download the dataset:

```bash
mkdir -p data
wget -P data/ https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/resolve/main/longmemeval_s_cleaned.json
```

Run:

```bash
python -m forgetit.experiments.run_longmemeval_qa
```

Sweeps over itemisers (session, turn_pair) × policies × retrievers (lexical, bm25) × budgets. Useful flags:

| Flag | Effect |
|---|---|
| `--shard i --num-shards N` | This worker runs configs `[i::N]`. Each config writes its own CSV (no collisions). Map `--shard` to `$SLURM_ARRAY_TASK_ID`. |
| `--limit N` | Replay only the first N examples (fast smoke test). |
| `--no-reader` | Retrieval/eviction metrics only — **no LLM endpoint needed**; LLM policies auto-skipped. |
| `--out-dir PATH` | Root for per-config CSVs. |

### The two LongMemEval entry points

Both run the same pipeline; the difference is how many of the three evaluation layers are switched on.

| | `run_longmemeval.py` | `run_longmemeval_qa.py` (main) |
|---|---|---|
| Reader (layer 3) | off (`reader="none"`) | real LLM (`PlainTextReader`), unless `--no-reader` |
| Measures | survival + retrieval | survival + retrieval + reader answer |
| Policies | LRU, LFU | LRU, LFU, Random, LLMCompression |
| Stores | RetentionManager | RetentionManager + Letta (opaque) |
| Scale flags | none | `--shard`, `--num-shards`, `--limit`, `--no-reader`, `--out-dir` |
| Needs an LLM endpoint? | no | yes (reader + compression) |

`run_longmemeval.py` is the cheap, deterministic, retrieval-only sibling — good for sweeping eviction behaviour fast with no inference server. `run_longmemeval_qa.py` adds the reader layer and the heavier machinery (compression policies, sharding, cluster runs). Note that `--no-reader` auto-drops the LLM-driven policies, because compression needs the model to summarize even when the reader is off — *"QA mode" and "uses an LLM" are different axes*.

### Example output

The budget→quality tradeoff falls straight out of the summary CSV. From a committed run (exact numbers depend on model, itemiser, and retriever):

| Budget | Evictions | Gold retained | hit@5 |
|---|---|---|---|
| 256 KiB | ~23 | 73% | 66% |
| ≥512 KiB | 0 | — | 88.5% |

At 256 KiB the store is forced to evict and quality degrades; at ≥512 KiB nothing needs evicting and retrieval sits at its ceiling. Sweeping the budget column traces the full degradation curve (`budget_quality_frontier`).

### Combining sharded results

```bash
python scripts/concat_results.py artifacts/benchmark
# → artifacts/benchmark/combined_results.csv  (+ combined_summary.csv)
```

### Running on LUMI

See `scripts/lumi/README.md`. In short: build a CPU client container with cotainr (`forgetit-env.yml`), serve the model with vLLM (ROCm) on a LUMI-G node, and launch `run_lumi.sh` — one vLLM server + N sharded clients + concat, all single-node.

---

## Metrics

Every scored query produces one `EvalResult` row (`results.csv`); `summarize()` aggregates them (`summary.csv`). Metrics group by evaluation layer.

**Layer 1 — policy / retention** *(averaged only over introspectable stores)*
- `gold_survival_rate` — fraction of questions where the gold item survived eviction (the downstream ceiling).
- `useful_byte_fraction` — of the bytes resident at query time, what fraction is the gold item (memory signal-to-noise).
- `waste_rate` — `1 − useful_byte_fraction`.
- `evictions_total_at_query_time`, `compressions` — running counts of what the policy did.

**Layer 2 — retriever**
- `hit_at_k` — did a gold item land in the top-k? (affected by both eviction *and* retrieval)
- `hit_at_k_given_retained` — hit rate conditioned on survival; isolates retrieval quality from forgetting quality. **The fairest metric for comparing policies.**
- `mean_mrr` — mean reciprocal rank of the first gold hit.

**Layer 3 — reader / QA** *(populated only in QA mode)*
- `reader_answer` — the LLM's actual answer text (stored per row, for offline grading).
- `abstained` / `abstention_rate` — did / how often the reader declined to answer.
- `qa_latency_sec` / `mean_qa_latency_sec` — reader latency, tracked separately from retrieval.

**Cost (both modes)**
- `used_bytes_end` — bytes resident at query time.
- `mean_latency_sec` — retrieval latency.

**Breakdowns** (`metrics.py`): `budget_quality_frontier` (metric vs. byte budget — the core tradeoff curve), `by_question_type`, and `by_store` (e.g. RetentionManager vs. Letta).

> Reminder (see [caveats](#known-issues--caveats)): the reader layer reports answers, abstention, and latency, **not** an automatic correctness score. Grade `reader_answer` against `ref_answer` offline if you need accuracy.

---

## Extending the framework

### Adding a new policy

**Delete-only policies** implement `SimpleEvictionPolicy.select_victim`:

```python
from forgetit.policies.base import SimpleEvictionPolicy

class MyPolicy(SimpleEvictionPolicy):
    name = "my_policy"

    def select_victim(self, store, candidates=None) -> str:
        for it in store.iter_features():          # MemFeatures(id, bytes, last_access,
            if candidates is not None and it.id not in candidates:  # access_count, created_at, compressed)
                continue
            ...
        return victim_id
```

The `candidates` filter lets two-phase policies ask "rank only the un-compressed items."

**Richer policies** (e.g. compress-or-evict) implement `EvictionPolicy.select_action` directly and return an `Evict` or `Compress` action — see `llm_compression.py`.

Add the policy to your experiment's policy list (`default_policy_specs` in the relevant entry point) and run; the pipeline handles the rest.

### Adding a new dataset

1. Write a workload class whose `events()` yields `WorkloadEvent`s (inserts, scored queries with `gold_item_ids`, optional access-only probes, optional `reset`s).
2. Optionally write an `Itemiser` if the raw data needs chunking.
3. Create an experiment entry point that passes the workload to `run_pipeline`.

See `workloads/synthetic.py` (simplest) and `workloads/longmemeval.py` (real adapter).

---

## Concept glossary

**MemRecord** — one unit of memory: text, optional embedding, timestamps, access stats, byte count, and `meta` (e.g. `compressed`).

**RetentionManager** — the budget enforcer. Wraps a Backend + Policy. On every `insert()`, runs the policy's actions until the budget fits; a stall guard turns any no-progress loop into a clear error instead of hanging.

**EvictionPolicy / SimpleEvictionPolicy** — `select_action(store) → Action`; the simple base wraps `select_victim(store, candidates=None) → id` into an `Evict`. Policies see `MemFeatures`, not full text.

**Action** — `Evict` (delete), `Compress` (replace text with a shorter summary), or `Prune` (graph, not implemented).

**Two-phase compression** — `LLMCompressionPolicy` compresses the least-useful *un-compressed* item (keeping it) and only **evicts** once everything resident is already compressed. `compress_selector` / `evict_selector` are pluggable (FIFO/LRU/LFU/…), defaulting to LFU; `selector=` sets both.

**Backend** — raw CRUD + search. `InMemoryBackend` (working), `FalkorBackend` (partial).

**Retriever** — ranks MemRecords for a query, returns top-k. `LexicalOverlapRetriever` (token overlap) and `BM25Retriever`.

**Reader** — takes a question + retrieved records, prompts the LLM, returns an `AnswerResult`. `PlainTextReader`, `JsonMemoryReader`.

**probe (access-only query)** — a `WorkloadEvent` query with empty `gold_item_ids`: it calls `retrieve` (updating recency/frequency) but is not scored. LongMemEval emits these during inserts so LRU/LFU/Ebbinghaus have a usage signal before eviction.

**gold_survived** — did the gold answer item survive eviction? A failure here means the answer was impossible to retrieve regardless of retrieval quality.

**hit_at_k** — did a gold item appear in the top-k retrieval results? Affected by both eviction and retrieval quality.

**hit_at_k_given_retained** — conditional hit rate given the gold item was not evicted. Isolates retrieval quality from eviction behaviour — the most useful metric for comparing policies.

---

## License & citation

*TODO: add a license file.* This is research code; pick a license before publishing (MIT or Apache-2.0 are common for academic tooling).

If you use `forget-it` in academic work, please cite the accompanying thesis:

```bibtex
@mastersthesis{forgetit2026,
  title  = {Policy-Pluggable Forgetting for Local Agentic Memory under Hard Storage Constraints},
  author = {TODO},
  school = {TODO},
  year   = {2026}
}
```