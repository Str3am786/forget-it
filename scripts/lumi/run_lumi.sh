#!/bin/bash
# ============================================================================
# forget-it on LUMI — single-node launcher (no cross-node wizardry).
#
# What it does, in order:
#   1. starts a vLLM server (ROCm container) on this node at localhost:8000,
#      serving the OpenAI-compatible API that forgetit.llm already speaks;
#   2. waits until the server is healthy;
#   3. launches NUM_SHARDS sharded client processes — each runs a slice of the
#      (itemiser × policy × retriever × budget) grid and writes its OWN CSVs
#      (no collisions); vLLM batches their concurrent requests;
#   4. concatenates the per-config CSVs into combined tables.
#
# Submit with:   sbatch scripts/lumi/run_lumi.sh
#
# >>> EDIT the placeholders marked  # EDIT  before first run. <<<
# ============================================================================
#SBATCH --job-name=forgetit
#SBATCH --account=project_465XXXXXX        # EDIT: your LUMI project
#SBATCH --partition=standard-g             # LUMI-G (AMD MI250X)
#SBATCH --nodes=1
#SBATCH --gpus-per-node=8                   # 1 MI250X = 2 GCDs; node has 8 GCDs
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=56
#SBATCH --mem=200G
#SBATCH --time=03:00:00
#SBATCH --output=logs/forgetit-%j.out

set -euo pipefail

# ---- paths (EDIT) ----------------------------------------------------------
REPO=/scratch/project_465XXXXXX/forget-it                 # EDIT: repo on scratch
VLLM_SIF=/scratch/project_465XXXXXX/containers/vllm-rocm.sif  # EDIT: ROCm vLLM image
MODEL_PATH=/scratch/project_465XXXXXX/models/Llama-3.1-8B-Instruct  # EDIT: pre-downloaded weights
export HF_HOME=/scratch/project_465XXXXXX/hf_cache        # EDIT: cache on scratch

NUM_SHARDS=8          # parallel client processes (all hit the one server)
SERVED_NAME=local     # model name the clients ask for (must match --served-model-name)
API_KEY=sk-lumi
PORT=8000

# Compute nodes have no internet — force offline so nothing tries to phone home.
export HF_HUB_OFFLINE=1
export TRANSFORMERS_OFFLINE=1

mkdir -p logs "$REPO/artifacts"
cd "$REPO"

# ---- 1. start vLLM in the background ---------------------------------------
echo "[lumi] launching vLLM (ROCm) on port $PORT ..."
apptainer exec --rocm "$VLLM_SIF" \
    vllm serve "$MODEL_PATH" \
        --served-model-name "$SERVED_NAME" \
        --host 0.0.0.0 --port "$PORT" \
        --api-key "$API_KEY" \
        --tensor-parallel-size 8 \
    > "logs/vllm-${SLURM_JOB_ID}.log" 2>&1 &
VLLM_PID=$!
# Always kill the server when this script exits (success, error, or scancel).
trap 'echo "[lumi] stopping vLLM ($VLLM_PID)"; kill $VLLM_PID 2>/dev/null || true' EXIT

# ---- 2. wait until healthy -------------------------------------------------
echo "[lumi] waiting for vLLM to come up ..."
for _ in $(seq 1 120); do                       # up to ~10 min
    if curl -sf "http://localhost:${PORT}/health" >/dev/null; then
        echo "[lumi] vLLM is up."
        break
    fi
    if ! kill -0 $VLLM_PID 2>/dev/null; then
        echo "[lumi] vLLM died during startup — see logs/vllm-${SLURM_JOB_ID}.log" >&2
        exit 1
    fi
    sleep 5
done

# ---- 3. point the pipeline at the local server & run shards ----------------
export LLM_BASE_URL="http://localhost:${PORT}/v1"
export LLM_API_KEY="$API_KEY"
export LLM_MODEL="$SERVED_NAME"

source "$REPO/.venv/bin/activate"

echo "[lumi] launching $NUM_SHARDS sharded clients ..."
pids=()
for i in $(seq 0 $((NUM_SHARDS - 1))); do
    python -m forgetit.experiments.run_longmemeval_qa \
        --shard "$i" --num-shards "$NUM_SHARDS" \
        --out-dir "$REPO/artifacts/benchmark" \
        > "logs/shard-${SLURM_JOB_ID}-${i}.log" 2>&1 &
    pids+=($!)
done

# Wait for every shard; fail the job if any shard failed.
rc=0
for pid in "${pids[@]}"; do
    wait "$pid" || rc=1
done

# ---- 4. concatenate per-config CSVs ----------------------------------------
echo "[lumi] merging per-config CSVs ..."
python scripts/concat_results.py "$REPO/artifacts/benchmark"

echo "[lumi] done (shard exit status: $rc)."
exit $rc
