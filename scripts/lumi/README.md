# Running forget-it on LUMI

No code changes are needed for the LLM: `forgetit.llm` already talks to any
OpenAI-compatible endpoint via `LLM_BASE_URL` / `LLM_API_KEY` / `LLM_MODEL`.
On LUMI we just stand up a vLLM server on a compute node and point at it.

## One-time setup (on a login node — it has internet, compute nodes do not)

1. **Clone the repo onto scratch** and create the venv:
   ```bash
   cd /scratch/project_465XXXXXX
   git clone <repo> forget-it && cd forget-it
   python -m venv .venv && source .venv/bin/activate
   pip install -e ".[dev]"
   ```
2. **Download the dataset** to `data/`:
   ```bash
   mkdir -p data
   wget -P data/ https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/resolve/main/longmemeval_s_cleaned.json
   ```
3. **Pre-download the model weights** to scratch (compute nodes are offline):
   ```bash
   HF_HOME=/scratch/project_465XXXXXX/hf_cache \
     huggingface-cli download meta-llama/Llama-3.1-8B-Instruct \
       --local-dir /scratch/project_465XXXXXX/models/Llama-3.1-8B-Instruct
   ```
4. **Get a ROCm vLLM container** (CUDA wheels will NOT run on MI250X). Pull a
   ROCm vLLM image to a `.sif` with apptainer, or use a LUMI-provided one, and
   note its path.

## Run

Edit the `# EDIT` placeholders at the top of `run_lumi.sh` (project account,
repo path, `.sif` path, model path, `HF_HOME`), then:

```bash
sbatch scripts/lumi/run_lumi.sh
```

This is the single-node design we settled on: one vLLM server + `NUM_SHARDS`
client processes that each run a slice of the config grid and write their own
CSVs, merged at the end by `scripts/concat_results.py`. No node-to-node
networking to get wrong while debugging.

## Useful flags (passed to `run_longmemeval_qa.py`)

- `--shard i --num-shards N` — split the grid across N workers (the sbatch sets these).
- `--limit N` — replay only the first N examples (quick smoke test).
- `--no-reader` — retrieval/eviction metrics only; **no LLM endpoint needed**
  (LLM-driven policies like compression are skipped automatically).

Smoke test before committing a full run, e.g. interactively:
```bash
LLM_BASE_URL=... LLM_API_KEY=... LLM_MODEL=... \
  python -m forgetit.experiments.run_longmemeval_qa --limit 3 --num-shards 1 --shard 0
```

## Scaling beyond one node (later)

The same `--shard/--num-shards` mechanism maps onto a SLURM **job array**
(`--shard $SLURM_ARRAY_TASK_ID`) against one persistent vLLM server. Hold off on
that until the single-node path is solid — it adds cross-node service discovery,
which is exactly the complexity we're avoiding for now.

## Two gotchas (not optional)

- **Compute nodes have no internet.** Weights must be pre-downloaded to scratch;
  the launcher sets `HF_HUB_OFFLINE=1` / `TRANSFORMERS_OFFLINE=1` so nothing hangs
  trying to reach HuggingFace.
- **Use the ROCm vLLM container**, not a pip install — LUMI-G is AMD MI250X.
