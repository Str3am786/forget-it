#!/bin/bash
# ============================================================================
# forget-it on LUMI — single-node launcher.
#
# Concurrency model (all race-free by store isolation, all synchronous):
#   - ONE vLLM server, ONE logical endpoint at localhost:8000 (so there's no
#     shard->port mapping to mis-balance).
#   - A few client PROCESSES (NUM_SHARDS) split the config grid -> few dataset copies.
#   - Inside each process configs run SEQUENTIALLY; --workers replays that many
#     EXAMPLES concurrently within a config (thread pool, a fresh isolated store
#     per example) so one config's compression LLM calls saturate the replicas,
#     and --reader-workers runs the QA reader as a concurrent second pass. The
#     LLM calls are I/O-bound, so threads overlap their waits; vLLM batches them.
#     Eviction inside any one example stays serial and identical to a 1-worker run.
#
#   Peak concurrent requests ≈ NUM_SHARDS × max(WORKERS, READER_WORKERS).
#   Tune that product to the vLLM plateau-K from:
#       python scripts/llm_concurrency_probe.py --base-url http://localhost:8000/v1
#
# Runs BOTH experiments (LongMemEval + BEAM), then merges all CSVs.
#
# Submit with:   sbatch scripts/lumi/run_lumi.sh
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
#SBATCH --time=06:00:00
#SBATCH --output=logs/forgetit-%j.out

set -euo pipefail

# T3d — SLURM_JOB_ID is unset off-cluster, and under `set -u` its FIRST use
# (the vLLM log path below) aborts the script before anything can be checked.
# Default it once, here.
SLURM_JOB_ID="${SLURM_JOB_ID:-local-$$}"

# ---- paths (EDIT) ----------------------------------------------------------
REPO=/scratch/project_465XXXXXX/forget-it                       # EDIT: repo on scratch
VLLM_SIF=/scratch/project_465XXXXXX/containers/vllm-rocm.sif     # EDIT: ROCm vLLM image
MODEL_PATH=/scratch/project_465XXXXXX/models/Qwen2.5-7B-Instruct # EDIT: pre-downloaded weights
export HF_HOME=/scratch/project_465XXXXXX/hf_cache              # EDIT: cache on scratch

# ---- concurrency knobs (tune to plateau-K) ---------------------------------
NUM_SHARDS=2            # client processes (= dataset copies). Keep small.
WORKERS=8              # examples replayed concurrently within a config (fresh store each)
READER_WORKERS=8       # concurrent QA reader calls (two-pass) within a config
# Reader configuration now lives in the code, not here. The evidence budget
# (16000 tokens, D-6) is DEFAULT_TOKEN_BUDGET, and there is one unversioned
# reader — the prompt/clean/abstention switches are gone, each having been a
# choice between a measured-worse and a measured-better behaviour. Override the
# survivors per-run with --reader-token-budget / --answer-max-tokens /
# --reader-prompt if you are deliberately sweeping that axis.
# Generation cap: the old 128 truncated ~1 in 3 answered generations
# mid-sentence. 256 is validated, and rides in the reader name.
ANSWER_MAX_TOKENS=256
EVENT_LOG=""          # set to "--event-log" for per-eviction traces (large on disk)
EXPERIMENTS=(run_longmemeval_qa run_beam)

# ---- vLLM endpoint ---------------------------------------------------------
SERVED_NAME=local
API_KEY=sk-lumi
PORT=8000
# For a 7B, data-parallel (one replica per GCD, ONE endpoint) maximises
# throughput. Validate on ROCm; if DP isn't supported, fall back to
# VLLM_PARALLEL="--tensor-parallel-size 8" (one replica spanning all GCDs).
VLLM_PARALLEL="--data-parallel-size 8"

# Compute nodes have no internet — force offline so nothing phones home.
export HF_HUB_OFFLINE=1
export TRANSFORMERS_OFFLINE=1

mkdir -p logs "$REPO/artifacts"
cd "$REPO"

# ── git provenance stamp — resolved once, up front (T4) ─────────────────────
# The Python stamping site prefers $GIT_COMMIT and falls back to resolving git
# itself. Resolving here means a run is never stamped "unknown" just because
# the worker's cwd or PATH differed from this shell's.
GIT_COMMIT="$(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || true)"
if [ -n "$GIT_COMMIT" ] && [ -n "$(git -C "$REPO" status --porcelain 2>/dev/null)" ]; then
    GIT_COMMIT="${GIT_COMMIT}-dirty"
fi
export GIT_COMMIT
if [ -z "$GIT_COMMIT" ]; then
    echo "[lumi] WARNING: no git commit resolvable in $REPO — rows may be stamped 'unknown'" >&2
else
    echo "[lumi] git_commit for this run: $GIT_COMMIT"
fi

# ---- 1. start vLLM in the background ---------------------------------------
# --max-model-len 32768 PINS the serving window: READER_TOKEN_BUDGET above is
# sized against this exact number, so it must not be left to the container's
# default (which a rebuild can move without any record in the run).
echo "[lumi] launching vLLM (ROCm) on port $PORT ($VLLM_PARALLEL) ..."
apptainer exec --rocm "$VLLM_SIF" \
    vllm serve "$MODEL_PATH" \
        --served-model-name "$SERVED_NAME" \
        --host 0.0.0.0 --port "$PORT" \
        --api-key "$API_KEY" \
        --max-model-len 32768 \
        $VLLM_PARALLEL \
    > "logs/vllm-${SLURM_JOB_ID}.log" 2>&1 &
VLLM_PID=$!
trap 'echo "[lumi] stopping vLLM ($VLLM_PID)"; kill $VLLM_PID 2>/dev/null || true' EXIT

# ---- 2. wait until healthy -------------------------------------------------
echo "[lumi] waiting for vLLM to come up ..."
for _ in $(seq 1 180); do                       # up to ~15 min (DP startup is slower)
    if curl -sf "http://localhost:${PORT}/health" >/dev/null; then
        echo "[lumi] vLLM is up."; break
    fi
    if ! kill -0 $VLLM_PID 2>/dev/null; then
        echo "[lumi] vLLM died during startup — see logs/vllm-${SLURM_JOB_ID}.log" >&2
        exit 1
    fi
    sleep 5
done

# ---- 3. point the pipeline at the local server ----------------------------
export LLM_BASE_URL="http://localhost:${PORT}/v1"
export LLM_API_KEY="$API_KEY"
export LLM_MODEL="$SERVED_NAME"
# Size the client's httpx pool to the peak in-flight requests per process so it
# doesn't throttle concurrency above httpx's default of 100.
export LLM_MAX_CONNECTIONS=$(( WORKERS > READER_WORKERS ? WORKERS : READER_WORKERS ))
source "$REPO/.venv/bin/activate"

# ---- 4. run each experiment as NUM_SHARDS concurrent processes -------------
rc=0
for EXP in "${EXPERIMENTS[@]}"; do
    echo "[lumi] === $EXP : $NUM_SHARDS shards × $WORKERS workers × $READER_WORKERS reader-workers ==="
    pids=()
    for i in $(seq 0 $((NUM_SHARDS - 1))); do
        python -m "forgetit.experiments.$EXP" \
            --shard "$i" --num-shards "$NUM_SHARDS" \
            --workers "$WORKERS" --reader-workers "$READER_WORKERS" \
            --answer-max-tokens "$ANSWER_MAX_TOKENS" \
            --out-dir "$REPO/artifacts/benchmark" $EVENT_LOG \
            > "logs/${EXP}-${SLURM_JOB_ID}-${i}.log" 2>&1 &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do
        wait "$pid" || rc=1
    done
done

# ---- 5. merge per-config CSVs (combined table has a `dataset` column) ------
# T3e: only merge when every shard succeeded, matching run_lumi_reader_eval.sh.
# A failed shard means an incomplete grid; a merged CSV over it looks
# success-shaped and misleads. The next successful run redoes the merge.
if [ "$rc" -eq 0 ]; then
    echo "[lumi] merging per-config CSVs ..."
    python scripts/concat_results.py "$REPO/artifacts/benchmark" || rc=1
else
    echo "[lumi] SKIPPING merge: one or more shards failed (rc=$rc) — resubmit to resume, merge runs on the clean pass"
fi

echo "[lumi] done (shard exit status: $rc)."
exit $rc
