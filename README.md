# forget-it

A research framework for studying **policy-pluggable forgetting in local agentic memory** under hard storage constraints.

The core question: when an agent's memory is bounded by a fixed byte budget, which eviction policy best preserves retrieval quality? This framework lets you swap forgetting policies without touching the storage or retrieval layers, run them against the LongMemEval benchmark, and compare results across budget sizes.

---

## Current State (March 2026)

**What works:**
- LRU and LFU policies running end-to-end against LongMemEval
- Hard byte-budget enforcement with per-item byte accounting
- Lexical retrieval baseline (`LexicalOverlapRetriever`)
- In-memory backend suitable for all experiments
- Retrieval-only evaluation via `run_longmemeval.py` — results are valid and reproducible


**Known issues / not yet stable:**
- `FalkorBackend` (`backend/falkor.py`) is partially implemented — `FalkorConfig` type is undefined, migration deprioritized
- LRU and LFU produce identical results in the default single-pass evaluation setup because no intermediate retrieval happens before eviction; a warmup pass is needed to differentiate them

---

## Architecture

```
forgetit/
├── core/
│   └── schema.py          # All domain dataclasses: MemRecord, Query, Session, etc.
│
├── memory_system/
│   └── store.py           # RetentionManager — the budget enforcer
│                          # Owns: insert, retrieve, evict, byte accounting
│
├── policies/
│   ├── base.py            # EvictionPolicy Protocol
│   ├── lru.py             # Least Recently Used
│   ├── lfu.py             # Least Frequently Used
│   └── compress.py        # LLM compression (stub — not functional)
│
├── backend/
│   ├── backend.py         # Abstract Backend base class
│   ├── in_memory.py       # Dict-based backend (used for all experiments)
│   └── falkor.py          # FalkorDB backend (partial — not ready)
│
├── retrieval/
│   ├── base.py            # Retriever Protocol
│   └── lexical.py         # Token overlap retriever (baseline)
│
├── llm/
│   ├── base.py            # Abstract LLM
│   ├── openai_llm.py      # OpenAI-compatible client (points to local server)
│   └── readers/
│       ├── base.py        # Reader Protocol
│       └── json_reader.py # Builds prompt, calls LLM, parses answer
│
├── services/
│   └── memory_qa_service.py  # Thin orchestrator: sessions → store → retrieve → answer
│
└── experiments/
    ├── run_longmemeval.py     # Retrieval-only eval (no LLM) ← primary eval script
    └── run_toy.py             # Minimal smoke test, good for local dev
```

### How the pieces connect

```
User session
    │
    ▼
MemoryQAService.add_session()
    │  converts Session → MemRecord
    ▼
RetentionManager.insert()
    │  checks byte budget
    │  if over budget → calls policy.select_victim(store) → evicts
    ▼
Backend.upsert()   ← stores the record
    │
    ▼
RetentionManager.retrieve(query, k)
    │  delegates to Backend.search()
    │  updates access_count + last_access on returned items (used by LFU/LRU)
    ▼
list[MemRecord]   ← top-k results
    │
    ▼
Reader.answer(question, records)   ← only in QA eval
    │  serialises records to JSON, calls LLM
    ▼
AnswerResult
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

### Quickest sanity check

```bash
python -m forgetit.experiments.run_toy
```

This inserts 5 records into a 250-byte budget store, runs a retrieval, triggers evictions, and prints the result. No LLM or dataset required.

### Retrieval-only evaluation (primary eval — no LLM needed)

For this experiment please create a /data folder within ```src/forgetit``` 
Use the following dataset/command: 
```bash 
wget https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/resolve/main/longmemeval_s_cleaned.json
```

And run this command from the root directory:
```bash
python src/forgetit/experiments/run_longmemeval.py \
  --data src/forgetit/data/longmemeval_s_cleaned.json \
  --policies lru lfu \
  --budgets 81920 98304 131072 196608 262144 \
  --ks 5 10 \
  --limit 200 \
  --out-dir results/my_run
```

```
policy=lru budget=262144 k=5 examples=200 gold_retained=0.550 hit@5=0.300 hit@5|retained=0.545 avg_evictions=27.00
```

**Metrics explained:**

| Metric | Meaning |
|---|---|
| `gold_retained` | Did the eviction policy keep the relevant memory? |
| `hit_at_k` | Did retrieval surface it in the top-k? |
| `hit_at_k_given_retained` | Given it wasn't evicted, did retrieval find it? (retrieval quality in isolation) |
| `evictions` | How many sessions were evicted to fit the budget |
| `used_bytes_end` | Bytes occupied at end of example |

`hit_at_k_given_retained` is the most informative metric for comparing retrieval quality independent of eviction policy.



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