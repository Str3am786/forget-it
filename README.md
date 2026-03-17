# forget-it

A research framework for studying **policy-pluggable forgetting in local agentic memory** under hard storage constraints.

The core question: when an agent's memory is bounded by a fixed byte budget, which eviction policy best preserves retrieval quality? This framework lets you swap forgetting policies without touching the storage or retrieval layers, run them against the LongMemEval benchmark, and compare results across budget sizes.

---

## Current State (March 2026)

**What works:**

- Benchmark pipeline: workload → runner → metrics → CSV, with pluggable policies, backends, and retrievers
- LRU and LFU eviction policies running end-to-end
- Synthetic workload for framework validation (controlled gold items, mid-stream access signal)
- LongMemEval workload with pluggable itemisers (session-level and turn-pair granularity)
- Hard byte-budget enforcement with per-item byte accounting
- Lexical retrieval baseline (token overlap, deterministic)
- Three-layer evaluation: policy verdict → retriever verdict → reader verdict (reader is placeholder)
- Full audit logging with per-event traceability (BenchmarkLogger)
- Structured output: per-query CSVs and aggregated summaries


**Known issues / not yet stable:**
- `FalkorBackend` (`backend/falkor.py`) is partially implemented — `FalkorConfig` type is undefined, migration deprioritized
- LRU and LFU produce identical results in the default single-pass evaluation setup because no intermediate retrieval happens before eviction; a warmup pass is needed to differentiate them

---


## Architecture

```
forget-it/
├── data/                          # Datasets (not in package)
│   └── longmemeval_s_cleaned.json
│
├── artifacts/                     # Pipeline output (CSVs, summaries)
│   └── benchmark/
│       └── <dataset>/<policy>/budget_<N>/
│           ├── results.csv        # Per-query EvalResult rows
│           └── summary.csv        # Aggregated metrics
│
├── results/                       # Legacy experiment outputs
│
├── src/forgetit/
│   ├── core/
│   │   ├── schema.py              # MemRecord, Query, MemFeatures, Session, etc.
│   │   └── logging.py             # BenchmarkLogger, EventLogger protocol, LogEntry
│   │
│   ├── memory_system/
│   │   └── store.py               # RetentionManager — budget enforcement, eviction loop
│   │
│   ├── policies/
│   │   ├── base.py                # EvictionPolicy protocol
│   │   ├── lru.py                 # Least Recently Used
│   │   └── lfu.py                 # Least Frequently Used
│   │
│   ├── backend/
│   │   ├── backend.py             # Backend ABC
│   │   ├── in_memory.py           # Dict-based backend (used for all experiments)
│   │   └── falkor.py              # FalkorDB backend (partial, optional import)
│   │
│   ├── retrieval/
│   │   ├── base.py                # Retriever protocol
│   │   └── lexical.py             # Token overlap retriever (deterministic baseline)
│   │
│   ├── benchmark/                 # ← Evaluation pipeline
│   │   ├── schema.py              # WorkloadEvent, EvalResult
│   │   ├── runner.py              # Replays workload, produces EvalResult per query
│   │   ├── pipeline.py            # Single-run orchestrator: store → run → metrics → CSV
│   │   ├── metrics.py             # Aggregation functions over EvalResult lists
│   │   └── workloads/
│   │       ├── base.py            # Workload protocol, Itemiser protocol
│   │       ├── synthetic.py       # Controlled synthetic workload
│   │       └── longmemeval.py     # LongMemEval adapter + SessionItemiser, TurnPairItemiser
│   │
│   ├── experiments/               # Entry points (thin scripts that call the pipeline)
│   │   ├── run_synthetic.py       # Sweep policies × budgets on synthetic workload
│   │   ├── run_longmemeval.py     # Sweep policies × budgets × itemisers on LongMemEval
│   │   └── run_toy.py             # Minimal smoke test
│   │
│   ├── llm/                       # OpenAI-compatible LLM client + Reader protocol
│   ├── services/                  # MemoryQAService (higher-level orchestration)
│   └── pipelines/                 # LangGraph wiring (ingest, retrieve, QA nodes)
│
└── tests/
    ├── memory_sys/
    └── policies/
```

### How the benchmark pipeline works

```
Experiment script (e.g. run_longmemeval.py)
    │  defines: workload, policy, budget, k
    ▼
pipeline.run_pipeline()
    │  builds: RetentionManager(backend, policy, logger)
    ▼
runner.run(workload, store, k, ...)
    │  for each WorkloadEvent:
    │    INSERT → store.insert(item)
    │    QUERY  → three-layer evaluation
    │      1. Policy: gold_ids ∩ retained_ids → gold_survived, ubf, waste
    │      2. Retriever: store.retrieve(query, k) → hit@k, mrr
    │      3. Reader: (placeholder)
    │    → append EvalResult
    ▼
list[EvalResult]
    │
    ▼
metrics.summarize(results) → dict
    │
    ▼
save to artifacts/<dataset>/<policy>/budget_<N>/
    ├── results.csv
    └── summary.csv
```

### How the memory system works

```
MemRecord
    │
    ▼
RetentionManager.insert()
    │  checks byte budget
    │  if over budget → policy.select_victim(store) → evict
    │  logger.event("insert", ...) / logger.event("evict", ...)
    ▼
Backend.upsert()
    │
    ▼
RetentionManager.retrieve(query, k)
    │  delegates to Backend.search()
    │  updates access_count + last_access on hits (drives LRU/LFU)
    ▼
list[MemRecord]
```

---
## Installation

Requires Python 3.12+. The package is installed in editable mode:

```bash
git clone https://github.com/Str3am786/forget-it
cd forget-it
pip install -e ".[dev]"
```

### LLM configuration

The QA eval requires an OpenAI-compatible inference endpoint. Set these environment variables:

```bash
export LLM_BASE_URL="http://localhost:8000/v1"   # or https://api.openai.com/v1
export LLM_API_KEY="sk-your-key"
export LLM_MODEL="your-model-name"
```

> **Important:** the model must be instruction-tuned and able to follow extraction-style prompts. Small base models will produce garbage. Verified to work: Llama-3-8B-Instruct, Mistral-7B-Instruct, GPT-4o-mini. Before running any QA eval, confirm your endpoint works:
> ```python
> from forgetit.llm import get_llm
> print(get_llm().generate("What is the capital of France?", max_tokens=10))
> # Must return something like "Paris." — not a paragraph
> ```

---

## Running experiments

### Quick smoke test

```bash
python -m forgetit.experiments.run_toy
```

Inserts 5 records into a 250-byte budget, triggers evictions, prints results. No dataset or LLM needed.

### Synthetic benchmark

```bash
python src/forgetit/experiments/run_synthetic.py
```

Runs LRU and LFU across multiple budget levels on a controlled synthetic workload with known gold items. Validates that the pipeline works and that policies diverge under pressure. Output goes to `artifacts/benchmark/synthetic/`.

### LongMemEval benchmark

Download the dataset:

```bash
mkdir -p data
wget -P data/ https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/resolve/main/longmemeval_s_cleaned.json
```

Run the experiment:

```bash
python src/forgetit/experiments/run_longmemeval.py
```

This sweeps over policies, budgets, and itemisers (session-level and turn-pair). Output goes to `artifacts/benchmark/longmemeval/`.

Example output:

```
itemiser=session      policy=lru   budget=524288    gold_survival=0.550  hit@k=0.300
itemiser=turn_pair    policy=lfu   budget=524288    gold_survival=0.420  hit@k=0.280
```

---

## Adding a new policy

Implement the `EvictionPolicy` protocol — one method:

```python
class MyPolicy:
    name = "my_policy"

    def select_victim(self, store) -> str:
        """Return the id of the item to evict."""
        for it in store.iter_features():
            # it: MemFeatures(id, bytes, last_access, access_count, created_at)
            ...
        return victim_id
```

Then add it to your experiment script's policy list and run. The pipeline handles everything else.

## Adding a new dataset

1. Write a workload class that yields `WorkloadEvent`s (inserts and queries with `gold_item_ids`).
2. Optionally write an `Itemiser` if the raw data needs chunking.
3. Create an experiment entry point that passes the workload to `run_pipeline`.

See `workloads/synthetic.py` for the simplest example, `workloads/longmemeval.py` for a real dataset adapter.



## Concept glossary

**MemRecord** — one unit of memory. Stores text, an optional embedding vector, timestamps, access statistics, and a byte count.

**RetentionManager** — the budget enforcer. Wraps a Backend and a Policy. On every `insert()`, checks if the budget would be exceeded and calls the policy to evict items until it fits.

**EvictionPolicy** — decides which MemRecord to remove when the budget is full. Receives the store and returns a single item id. Policies only see `MemFeatures` (id, bytes, timestamps, access count) by default — not full text.

**Backend** — persistent or in-memory store. Handles raw CRUD and search. Currently: `InMemoryBackend` (working), `FalkorBackend` (partial).

**Retriever** — ranks MemRecords for a query and returns top-k. Currently: `LexicalOverlapRetriever` (word overlap, no stopwords — lower bound baseline).

**Reader** — takes a question and a list of retrieved MemRecords, builds a prompt, calls the LLM, and returns a structured `AnswerResult`.

**gold\_retained** — whether the gold answer session survived eviction. A policy failure on this metric means the answer was impossible to retrieve regardless of retrieval quality.

**hit\_at\_k** — whether the gold session appeared in the top-k retrieval results. Affected by both eviction and retrieval quality.

**hit\_at\_k\_given\_retained** — conditional hit rate: given the gold session was not evicted, did retrieval find it? This isolates retrieval quality from eviction policy behaviour and is the most useful metric for comparing policies.