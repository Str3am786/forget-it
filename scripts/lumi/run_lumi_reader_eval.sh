#!/bin/bash
# ============================================================================
# forget-it on LUMI — READER-ON EVALUATION (8 GCDs) — DEDICATED-EMBED layout
#
# The ANSWER half. The state-forming grid (benchmark_full, --no-reader) is
# complete and frozen; this job re-runs the same frozen grid WITH the reader.
# Because `reader` is inside _config_hash, every config's hash differs from
# the no-reader run — the DONE guard auto-invalidates, nothing needs
# FORGETIT_FORCE_RERUN. State-forming replays warm from the persistent
# caches (emb_cache sqlite + llm_cache disk replay); the new compute is the
# answer generation (+ grading).
#
# WHAT THIS SCRIPT CHANGES vs run_lumi_full_eval.sh:
#   • OUT_DIR = benchmark_reader (FRESH TREE — benchmark_full is never
#     touched; the two artifact sets stay separately provenanced)
#   • --no-reader dropped; --reader-workers added (two-pass concurrent reader)
#   • --event-log dropped: events were captured by the state-forming run and
#     an identical replay would duplicate them; the regret report belongs to
#     the benchmark_full tree
#   • A7 smoke runs READER-ON so the gate exercises the answer path
#   • step 5 (eviction-regret) removed for the same reason as --event-log
#   • CALIBRATION MODE: LIMIT=n sbatch ... runs the grid with --limit n into
#     a side tree (benchmark_reader_calib) to measure per-query reader cost
#     before committing a full allocation. n_examples is in the config hash,
#     so calib rows can never satisfy (or pollute) the full run's DONE files.
#
# HOW TO SUBMIT (one dataset per job; calibrate FIRST):
#   export SBATCH_ACCOUNT=project_<your-id>
#   cd <repo-root>
#   # 1) calibration (~30-60 min): 2 examples across the whole grid
#   LIMIT=2 DATASETS="locomo" sbatch scripts/lumi/run_lumi_reader_eval.sh
#   # 2) full runs, time limits set from what calibration measured:
#   DATASETS="longmemeval" sbatch scripts/lumi/run_lumi_reader_eval.sh
#   DATASETS="beam"        sbatch scripts/lumi/run_lumi_reader_eval.sh
#   DATASETS="locomo"      sbatch --time=48:00:00 scripts/lumi/run_lumi_reader_eval.sh
# LoCoMo is the HEAVY one here (1,986 queries/config × 1,083 configs), the
# reverse of the state-forming run — do not treat it as the quick canary.
# ============================================================================
#SBATCH --job-name=forgetit-reader
#SBATCH --partition=standard-g
#SBATCH --nodes=1
#SBATCH --gpus-per-node=8
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=56
#SBATCH --mem=400G
#SBATCH --time=24:00:00
#SBATCH --output=forgetit-reader-%j.out

set -euo pipefail

# ── identity & paths — derived, never hardcoded ─────────────────────────────
PROJ="${SLURM_JOB_ACCOUNT:?no Slurm account — submit via sbatch with SBATCH_ACCOUNT exported (see header)}"
REPO="${SLURM_SUBMIT_DIR:?}"
if [ ! -e "$REPO/pyproject.toml" ] || [ ! -d "$REPO/src/forgetit" ]; then
    echo "[lumi] REFUSING: sbatch was not run from the repo root (SLURM_SUBMIT_DIR=$REPO)" >&2
    exit 1
fi
CLIENT_SIF="${CLIENT_SIF:-$(dirname "$REPO")/forgetit.sif}"
VLLM_SIF=/appl/local/laifs/containers/lumi-multitorch-latest.sif

CHAT_MODEL_PATH=/scratch/$PROJ/models/Qwen2.5-7B-Instruct
EMBED_MODEL_PATH=/scratch/$PROJ/models/Qwen3-Embedding-0.6B

LME_DATASET=$REPO/data/longmemeval_s_cleaned.json
BEAM_DATASET=$REPO/data/beam_500K.jsonl
LOCOMO_DATASET=$REPO/data/locomo10.json

# FRESH tree — the state-forming grid in benchmark_full stays untouched.
OUT_DIR=/scratch/$PROJ/artifacts/benchmark_reader
A7_SMOKE_DIR=/scratch/$PROJ/artifacts/a7_smoke_reader_${SLURM_JOB_ID}
LOGS=/scratch/$PROJ/logs
export HF_HOME=/scratch/$PROJ/hf_cache

# ── calibration mode ────────────────────────────────────────────────────────
# LIMIT=n → --limit n into a side tree. Different n_examples → different
# config hashes → the full run later reruns everything properly.
LIMIT="${LIMIT:-}"
LIMIT_FLAG=""
if [ -n "$LIMIT" ]; then
    OUT_DIR="${OUT_DIR}_calib"
    LIMIT_FLAG="--limit $LIMIT"
    echo "[lumi] CALIBRATION MODE: --limit $LIMIT → $OUT_DIR"
fi

# ── recorded-run guard — refuse a dirty tree ────────────────────────────────
if [ -z "${FORGETIT_ALLOW_DIRTY:-}" ]; then
    if ! git -C "$REPO" diff --quiet 2>/dev/null || \
       ! git -C "$REPO" diff --cached --quiet 2>/dev/null; then
        echo "[lumi] REFUSING: uncommitted changes in $REPO — commit+tag, or FORGETIT_ALLOW_DIRTY=1 for smoke" >&2
        exit 1
    fi
fi
if git -C "$REPO" ls-files --error-unmatch .env >/dev/null 2>&1; then
    echo "[lumi] WARNING: $REPO/.env is tracked by git — this job rewrites it" >&2
fi

# ── FROZEN GRID — identical to the state-forming run; this block IS the run
# record. memorybank deliberately omitted (V1: compression covers the cell).
POLICIES="fifo lru lfu random keep_smallest evict_smallest salience_decay gdsf gdsf_frozen semantic semantic_coverage semantic_keep ebbinghaus compression oracle"
BUDGETS="1.0 0.75 0.50 0.25 0.10"
SEEDS="42 43 44 45 46 47 48 49 50 51"

# ── topology — proven 7 chat + 1 embed layout (Bus-error safe) ──────────────
NUM_CHAT_GCDS=7
EMBED_GCD=7
CHAT_PORT_BASE=8000
EMBED_PORT=8100
NUM_SHARDS=16
SERVED_CHAT=local
SERVED_EMBED=qwen3-embedding-0.6b

LME_WORKERS=12
BEAM_WORKERS=8
LOCOMO_WORKERS=8
READER_WORKERS=8           # concurrent QA reader calls (two-pass) per config
POOL=64

if [ -z "${DATASETS:-}" ]; then
    echo "[lumi] REFUSING: DATASETS is unset — run one dataset per job:" >&2
    echo "[lumi]   DATASETS=\"longmemeval\" sbatch $0" >&2
    echo "[lumi]   DATASETS=\"beam\"        sbatch $0" >&2
    echo "[lumi]   DATASETS=\"locomo\"      sbatch $0   # heaviest — see header" >&2
    exit 1
fi

EMBED_SERVE_FLAG="${EMBED_SERVE_FLAG:-}"
[ -z "$EMBED_SERVE_FLAG" ] && EMBED_SERVE_FLAG="--runner pooling"

export HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1
mkdir -p "$LOGS" "$OUT_DIR" "$A7_SMOKE_DIR"

# ── caches — SAME paths as the state-forming run, on purpose ────────────────
# The warm-replay economics of this job depend on reusing these exact caches:
# emb_cache keys on encoder identity (V3b), llm_cache replays compression
# calls. Do NOT point these anywhere fresh.
EMB_CACHE_DIR=/scratch/$PROJ/emb_cache
LLM_CACHE_DIR=/scratch/$PROJ/llm_cache
mkdir -p "$EMB_CACHE_DIR" "$LLM_CACHE_DIR"

# ── preflight ───────────────────────────────────────────────────────────────
for f in "$VLLM_SIF" "$CLIENT_SIF" \
         "$CHAT_MODEL_PATH/config.json" "$EMBED_MODEL_PATH/config.json"; do
    [ -e "$f" ] || { echo "[lumi] MISSING: $f" >&2; exit 1; }
done
for ds in $DATASETS; do
    case "$ds" in
        longmemeval) [ -e "$LME_DATASET" ]    || { echo "[lumi] MISSING dataset: $LME_DATASET" >&2; exit 1; } ;;
        beam)        [ -e "$BEAM_DATASET" ]   || { echo "[lumi] MISSING dataset: $BEAM_DATASET" >&2; exit 1; } ;;
        locomo)      [ -e "$LOCOMO_DATASET" ] || { echo "[lumi] MISSING dataset: $LOCOMO_DATASET" >&2; exit 1; } ;;
    esac
done
if [ -z "${SKIP_A7_SMOKE:-}" ] && [ ! -e "$LME_DATASET" ]; then
    echo "[lumi] MISSING dataset: $LME_DATASET (needed by the A7 smoke; SKIP_A7_SMOKE=1 to bypass)" >&2
    exit 1
fi

[ -f "$REPO/.env" ] && [ ! -f "$REPO/.env.prelumi" ] && cp "$REPO/.env" "$REPO/.env.prelumi"
cat > "$REPO/.env" <<EOF
LLM_BASE_URL=http://localhost:${CHAT_PORT_BASE}/v1
LLM_API_KEY=sk-lumi
LLM_MODEL=$SERVED_CHAT
EMBED_BASE_URL=http://localhost:${EMBED_PORT}/v1
EMBED_API_KEY=sk-lumi
EMBED_MODEL=$SERVED_EMBED
LETTA_BASE_URL=http://localhost:8283
LETTA_API_KEY=none
EOF

set +e
module purge
module use /appl/local/laifs/modules
module load lumi-aif-singularity-bindings
set -e
echo "[lumi] embedding server will use: $EMBED_SERVE_FLAG"

# ── 1. start 7 chat servers (GCD 0-6) + 1 embed server (GCD 7) ──────────────
server_pids=()
for g in $(seq 0 $((NUM_CHAT_GCDS - 1))); do
    cport=$((CHAT_PORT_BASE + g))
    echo "[lumi] GCD $g: chat vLLM on port $cport ..."
    singularity exec -B "/scratch/$PROJ" "$VLLM_SIF" \
        bash -c "ROCR_VISIBLE_DEVICES=$g vllm serve '$CHAT_MODEL_PATH' \
                 --served-model-name '$SERVED_CHAT' --port $cport --tensor-parallel-size 1" \
        > "$LOGS/vllm-chat-${SLURM_JOB_ID}-g${g}.log" 2>&1 &
    server_pids+=($!)
done
echo "[lumi] GCD $EMBED_GCD: embed vLLM (Qwen3-Embedding-0.6B) on port $EMBED_PORT ..."
singularity exec -B "/scratch/$PROJ" "$VLLM_SIF" \
    bash -c "ROCR_VISIBLE_DEVICES=$EMBED_GCD vllm serve '$EMBED_MODEL_PATH' \
             --served-model-name '$SERVED_EMBED' --port $EMBED_PORT --tensor-parallel-size 1 \
             $EMBED_SERVE_FLAG --max-model-len 32768" \
    > "$LOGS/vllm-embed-${SLURM_JOB_ID}-g${EMBED_GCD}.log" 2>&1 &
server_pids+=($!)
# shellcheck disable=SC2329
stop_servers() {
    echo "[lumi] stopping servers"
    for p in "${server_pids[@]}"; do kill "$p" 2>/dev/null || true; done
}
trap stop_servers EXIT

# ── 2. wait for all 8 /health ───────────────────────────────────────────────
echo "[lumi] waiting for $((NUM_CHAT_GCDS + 1)) servers ..."
for g in $(seq 0 $((NUM_CHAT_GCDS - 1))); do
    port=$((CHAT_PORT_BASE + g)); ok=0
    for _ in $(seq 1 180); do
        curl -sf "http://localhost:${port}/health" >/dev/null && { ok=1; break; }
        sleep 5
    done
    [ "$ok" = 1 ] || { echo "[lumi] chat g$g (port $port) never healthy — see $LOGS/vllm-chat-${SLURM_JOB_ID}-g${g}.log" >&2; exit 1; }
    echo "[lumi]   chat GCD $g up (port $port)."
done
ok=0
for _ in $(seq 1 180); do
    curl -sf "http://localhost:${EMBED_PORT}/health" >/dev/null && { ok=1; break; }
    sleep 5
done
[ "$ok" = 1 ] || { echo "[lumi] embed (port $EMBED_PORT) never healthy — see $LOGS/vllm-embed-${SLURM_JOB_ID}-g${EMBED_GCD}.log" >&2; exit 1; }
echo "[lumi] all servers up."

# ── 2b. encoder pin record + A7 identity smoke — READER-ON ──────────────────
# The pin lives in THIS tree too: benchmark_reader must be self-describing.
cat > "$OUT_DIR/ENCODER_PIN.txt" <<EOF
model_path:            $EMBED_MODEL_PATH
model_config_sha256:   $(sha256sum "$EMBED_MODEL_PATH/config.json" | cut -d' ' -f1)
serving:               vLLM (container: $VLLM_SIF) $EMBED_SERVE_FLAG --max-model-len 32768
served_name:           $SERVED_EMBED
EMBED_MAX_INPUT_CHARS: 28000
recorded:              $(date -u +%Y-%m-%dT%H:%M:%SZ) job ${SLURM_JOB_ID}
EOF
echo "[lumi] encoder pin written to $OUT_DIR/ENCODER_PIN.txt"

if [ -z "${SKIP_A7_SMOKE:-}" ]; then
    echo "[lumi] A7 smoke — READER ON (semantic policies, 1 example) ..."
    singularity exec -B "/scratch/$PROJ,$REPO" "$CLIENT_SIF" \
        bash -c "cd '$REPO' && PYTHONPATH='$REPO/src' \
                 LLM_BASE_URL='http://localhost:${CHAT_PORT_BASE}/v1' LLM_API_KEY=sk-lumi LLM_MODEL='$SERVED_CHAT' \
                 EMBED_BASE_URL='http://localhost:${EMBED_PORT}/v1' EMBED_API_KEY=sk-lumi EMBED_MODEL='$SERVED_EMBED' \
                 EXPECTED_EMBED_MODEL='$SERVED_EMBED' \
                 EMBED_MAX_INPUT_CHARS=28000 EMBED_CACHE_PATH='$EMB_CACHE_DIR/a7_smoke_reader_${SLURM_JOB_ID}.sqlite' \
                 LLM_CACHE_DIR='$LLM_CACHE_DIR' \
                 python -m forgetit.experiments.run_longmemeval_qa \
                   --policies semantic semantic_keep --limit 1 \
                   --shard 0 --num-shards 1 --out-dir '$A7_SMOKE_DIR' --dense" \
        > "$LOGS/a7-smoke-reader-${SLURM_JOB_ID}.log" 2>&1 \
        || { echo "[lumi] REFUSING: reader-on A7 smoke failed — see $LOGS/a7-smoke-reader-${SLURM_JOB_ID}.log" >&2; exit 1; }
    echo "[lumi] A7 smoke passed — answer path is live on the wired encoder."
fi

# ── 3. run each dataset's sweep, sharded, READER ON ─────────────────────────
run_dataset () {
    local dataset="$1" module="$2" workers="$3"
    echo "[lumi] ===== $dataset: $NUM_SHARDS shards (workers=$workers, reader-workers=$READER_WORKERS, reader=ON) ====="
    local pids=() i cport
    for i in $(seq 0 $((NUM_SHARDS - 1))); do
        cport=$((CHAT_PORT_BASE + i % NUM_CHAT_GCDS))
        singularity exec -B "/scratch/$PROJ,$REPO" "$CLIENT_SIF" \
            bash -c "cd '$REPO' && PYTHONPATH='$REPO/src' \
                     LLM_BASE_URL='http://localhost:${cport}/v1' LLM_API_KEY=sk-lumi LLM_MODEL='$SERVED_CHAT' \
                     EMBED_BASE_URL='http://localhost:${EMBED_PORT}/v1' EMBED_API_KEY=sk-lumi EMBED_MODEL='$SERVED_EMBED' \
                     EXPECTED_EMBED_MODEL='$SERVED_EMBED' \
                     EMBED_MAX_INPUT_CHARS=28000 \
                     EMBED_CACHE_PATH='$EMB_CACHE_DIR/${dataset}_shard_${i}.sqlite' \
                     LLM_CACHE_DIR='$LLM_CACHE_DIR' \
                     LLM_MAX_CONNECTIONS=$POOL EMBED_MAX_CONNECTIONS=$POOL \
                     python -m $module \
                       --shard $i --num-shards $NUM_SHARDS --out-dir '$OUT_DIR' \
                       --policies $POLICIES \
                       --budgets $BUDGETS \
                       --seeds $SEEDS \
                       --include-floor \
                       --dense --workers $workers \
                       --reader-workers $READER_WORKERS $LIMIT_FLAG" \
            > "$LOGS/${dataset}-reader-${SLURM_JOB_ID}-${i}.log" 2>&1 &
        pids+=($!)
    done
    local rc=0
    for pid in "${pids[@]}"; do wait "$pid" || rc=1; done
    echo "[lumi] ===== $dataset: all shards finished (rc=$rc) ====="
    return $rc
}

rc=0
for ds in $DATASETS; do
    case "$ds" in
        longmemeval) run_dataset longmemeval forgetit.experiments.run_longmemeval_qa "$LME_WORKERS"    || rc=1 ;;
        beam)        run_dataset beam        forgetit.experiments.run_beam           "$BEAM_WORKERS"   || rc=1 ;;
        locomo)      run_dataset locomo      forgetit.experiments.run_locomo_qa      "$LOCOMO_WORKERS" || rc=1 ;;
        *) echo "[lumi] unknown dataset '$ds'" >&2; rc=1 ;;
    esac
done

# ── 4. merge CSVs over the READER tree ──────────────────────────────────────
echo "[lumi] merging CSVs ..."
singularity exec -B "/scratch/$PROJ,$REPO" "$CLIENT_SIF" \
    bash -c "cd '$REPO' && PYTHONPATH='$REPO/src' python scripts/concat_results.py '$OUT_DIR'" || rc=1

# NOTE: no eviction-regret step here — events and the regret report belong to
# the state-forming tree (benchmark_full); this tree carries the answers.

echo "[lumi] done (exit $rc)."
echo "[lumi]   CSVs:        $OUT_DIR/combined_results.csv + combined_summary.csv"
echo "[lumi]   encoder pin: $OUT_DIR/ENCODER_PIN.txt"
exit $rc
