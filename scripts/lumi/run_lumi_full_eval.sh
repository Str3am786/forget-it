#!/bin/bash
# ============================================================================
# forget-it on LUMI — FULL EVALUATION (8 GCDs) — DEDICATED-EMBED layout
#
# WHY THIS LAYOUT (read this — it's the fix for the Bus errors):
#   Co-locating a chat AND an embed vLLM on every GCD = 16 servers launching
#   at once. On these nodes /tmp and /dev/shm are ONE shared 353G RAM-backed
#   tmpfs; 16 simultaneous vLLM starts exhaust it and every process dies with
#   `Bus error` before writing a byte. The proven run_lumi.sh launches only 8
#   servers and never hits this. So here: 7 chat servers (GCD 0-6) + 1 embed
#   server (GCD 7) = 8 servers total. Each server owns its whole GCD.
#
# WHAT IT RUNS (STATE-FORMING half only):
#   • FROZEN GRID policies (explicit --policies below; never code defaults)
#   • lexical + bm25 + dense retrievers (--dense)
#   • budgets 1.0/0.75/0.50/0.25/0.10 (1.0 = unconstrained retention ceiling;
#     grader calibration needs answers, so it belongs to the READER-ON job,
#     not this one)   • full-depth ranks (uncensored)   • --include-floor
#   • --event-log → offline eviction_regret pass
#   • NO READER (--no-reader): the answer stage is gated behind the reader-swap
#     acceptance. Chat servers stay up because COMPRESSION (not gated) uses
#     them — the runners build the chat client for explicitly-requested LLM
#     policies even under --no-reader.
#
# HOW TO SUBMIT (one dataset per job; the three jobs run side by side):
#   export SBATCH_ACCOUNT=project_<your-id>    # once per shell — NEVER in the repo
#   cd <repo-root>                             # paths are derived from here
#   for ds in longmemeval beam locomo; do
#       DATASETS="$ds" sbatch scripts/lumi/run_lumi_full_eval.sh
#   done
# The container is expected at <repo-parent>/forgetit.sif; override with
# CLIENT_SIF=/path/to.sif if yours lives elsewhere.
# ============================================================================
#SBATCH --job-name=forgetit-full
#SBATCH --partition=standard-g
#SBATCH --nodes=1
#SBATCH --gpus-per-node=8
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=56
#SBATCH --mem=400G
#SBATCH --time=24:00:00
#SBATCH --output=forgetit-full-%j.out

set -euo pipefail

# ── identity & paths — derived, never hardcoded ───────────────────────────────────────────────────────────────────
# The project ID comes from the account this job was submitted under
# (SBATCH_ACCOUNT / --account at submit time), and the repo is wherever you
# ran sbatch from. Nothing project- or machine-specific lives in this file,
# so it is always safe to commit and push, and the script survives repo moves.
PROJ="${SLURM_JOB_ACCOUNT:?no Slurm account — submit via sbatch with SBATCH_ACCOUNT exported (see header)}"
REPO="${SLURM_SUBMIT_DIR:?}"
if [ ! -e "$REPO/pyproject.toml" ] || [ ! -d "$REPO/src/forgetit" ]; then
    echo "[lumi] REFUSING: sbatch was not run from the repo root (SLURM_SUBMIT_DIR=$REPO)" >&2
    echo "[lumi]   cd <repo-root> && DATASETS=... sbatch scripts/lumi/run_lumi_full_eval.sh" >&2
    exit 1
fi
CLIENT_SIF="${CLIENT_SIF:-$(dirname "$REPO")/forgetit.sif}"   # default: sibling of the repo
VLLM_SIF=/appl/local/laifs/containers/lumi-multitorch-latest.sif

CHAT_MODEL_PATH=/scratch/$PROJ/models/Qwen2.5-7B-Instruct
EMBED_MODEL_PATH=/scratch/$PROJ/models/Qwen3-Embedding-0.6B

LME_DATASET=$REPO/data/longmemeval_s_cleaned.json
BEAM_DATASET=$REPO/data/beam_500K.jsonl
LOCOMO_DATASET=$REPO/data/locomo10.json

OUT_DIR=/scratch/$PROJ/artifacts/benchmark_full
# Smoke output lives OUTSIDE OUT_DIR: step 4 runs concat_results.py over
# OUT_DIR, and the limit-1 smoke rows must never reach the merged CSVs or
# trip the INCOMPLETE-grid warning.
A7_SMOKE_DIR=/scratch/$PROJ/artifacts/a7_smoke
LOGS=/scratch/$PROJ/logs
export HF_HOME=/scratch/$PROJ/hf_cache

# ── recorded-run guard — refuse a dirty tree ────────────────────────────────
# Provenance stamps git_commit into every row; a recorded run from an
# uncommitted tree is unreproducible ("<sha>-dirty"). Commit + tag first.
# Smoke-only override:  FORGETIT_ALLOW_DIRTY=1 sbatch ...
if [ -z "${FORGETIT_ALLOW_DIRTY:-}" ]; then
    if ! git -C "$REPO" diff --quiet 2>/dev/null || \
       ! git -C "$REPO" diff --cached --quiet 2>/dev/null; then
        echo "[lumi] REFUSING: uncommitted changes in $REPO — commit+tag, or FORGETIT_ALLOW_DIRTY=1 for smoke" >&2
        exit 1
    fi
fi
# This script rewrites $REPO/.env below. If .env is TRACKED, that write
# dirties the tree and the NEXT submission fails the guard above confusingly.
if git -C "$REPO" ls-files --error-unmatch .env >/dev/null 2>&1; then
    echo "[lumi] WARNING: $REPO/.env is tracked by git — this job rewrites it; gitignore it or the next dirty-tree check will fail" >&2
fi

# ── FROZEN GRID — this block IS the run record ──────────────────────────────
# Explicit on the command line so the sbatch script, not code defaults,
# defines the grid.
# VERIFIED (V1): every name below matches the policy registry. The only
# registered policy deliberately omitted is memorybank (compression covers
# the LLM-compression cell; memorybank differs only in selector+prompt).
# gdsf_frozen = GDSF with L≡0 (frequency-density).
POLICIES="fifo lru lfu random keep_smallest evict_smallest salience_decay gdsf gdsf_frozen semantic semantic_coverage semantic_keep ebbinghaus compression oracle"
# VERIFIED (V2): runners expose --budgets and --seeds; both wired explicitly.
# Module default budgets match this ladder; stating them here freezes them.
BUDGETS="1.0 0.75 0.50 0.25 0.10"
SEEDS="42 43 44 45 46 47 48 49 50 51"     # random-policy sweep: 10 seeds

# ── topology ────────────────────────────────────────────────────────────────
NUM_CHAT_GCDS=7            # chat servers on GCD 0-6, ports 8000..8006
EMBED_GCD=7                # the embed server gets GCD 7 to itself
CHAT_PORT_BASE=8000
EMBED_PORT=8100            # single embed endpoint, all shards use it
NUM_SHARDS=16              # client shards; round-robined across the 7 chat servers
SERVED_CHAT=local
SERVED_EMBED=qwen3-embedding-0.6b

LME_WORKERS=12             # per-config example concurrency (LongMemEval)
BEAM_WORKERS=8             # leaner: BEAM conversations hold big per-example stores
LOCOMO_WORKERS=8           # LoCoMo has only 10 conversations — workers ≤ 10 by construction
POOL=64                    # httpx pool >= per-shard concurrency

# One dataset per job (see header): refuse a bare sbatch rather than silently
# packing both sweeps into one 24h allocation. DATASETS="longmemeval beam" is
# still accepted explicitly if you really want a combined job.
if [ -z "${DATASETS:-}" ]; then
    echo "[lumi] REFUSING: DATASETS is unset — run one dataset per job:" >&2
    echo "[lumi]   DATASETS=\"longmemeval\" sbatch $0" >&2
    echo "[lumi]   DATASETS=\"beam\"        sbatch $0" >&2
    echo "[lumi]   DATASETS=\"locomo\"      sbatch $0" >&2
    exit 1
fi

EMBED_SERVE_FLAG="${EMBED_SERVE_FLAG:-}"
[ -z "$EMBED_SERVE_FLAG" ] && EMBED_SERVE_FLAG="--runner pooling"

export HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1
mkdir -p "$LOGS" "$OUT_DIR" "$A7_SMOKE_DIR"

# ── embedding cache — ENABLED and PERSISTENT ────────────────────────────────
# Per-shard sqlite files on /scratch — persistent across jobs, no cross-process
# writer contention.
# VERIFIED (V3a): cache.py documents SQLite multi-writer as UNSAFE cross-process
# on filesystems without -shm support (Lustre) and warns when WAL doesn't hold —
# per-shard files are the correct layout; do NOT collapse to one shared file.
# VERIFIED (V3b): the cache key namespace is the embedder identity string
# "{model}{+l2}@{cap}" (openai_embedder.name) — model, normalisation AND
# EMBED_MAX_INPUT_CHARS are all in the key, so a later cap change can never
# serve stale head-truncated vectors. The serving stack is NOT in the key
# (unknowable client-side); it is pinned via ENCODER_PIN.txt below.
EMB_CACHE_DIR=/scratch/$PROJ/emb_cache
mkdir -p "$EMB_CACHE_DIR"

# ── LLM disk cache pinned to /scratch ───────────────────────────────────────
# Resumability (cold 2:30 → warm 9s replay) only works if the cache outlives
# the job — it must not land in the container/tmpfs default.
# VERIFIED (V4): disk_cache.py reads LLM_CACHE_DIR (this exact name).
LLM_CACHE_DIR=/scratch/$PROJ/llm_cache
mkdir -p "$LLM_CACHE_DIR"

# ── preflight ───────────────────────────────────────────────────────────────
for f in "$VLLM_SIF" "$CLIENT_SIF" \
         "$CHAT_MODEL_PATH/config.json" "$EMBED_MODEL_PATH/config.json"; do
    [ -e "$f" ] || { echo "[lumi] MISSING: $f" >&2; exit 1; }
done
for ds in $DATASETS; do
    case "$ds" in
        longmemeval) [ -e "$LME_DATASET" ]  || { echo "[lumi] MISSING dataset: $LME_DATASET" >&2; exit 1; } ;;
        beam)        [ -e "$BEAM_DATASET" ] || { echo "[lumi] MISSING dataset: $BEAM_DATASET (or set DATASETS=\"longmemeval\")" >&2; exit 1; } ;;
        locomo)      [ -e "$LOCOMO_DATASET" ] || { echo "[lumi] MISSING dataset: $LOCOMO_DATASET" >&2; exit 1; } ;;
    esac
done
# The A7 smoke (step 2b) runs run_longmemeval_qa regardless of DATASETS, so a
# beam-only job still needs the LME file — check it BEFORE booting 8 servers.
if [ -z "${SKIP_A7_SMOKE:-}" ] && [ ! -e "$LME_DATASET" ]; then
    echo "[lumi] MISSING dataset: $LME_DATASET (needed by the A7 smoke even for DATASETS=\"beam\"; SKIP_A7_SMOKE=1 to bypass)" >&2
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

# ── 1. start 7 chat servers (GCD 0-6) + 1 embed server (GCD 7) = 8 total ────
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
# --max-model-len 32768: the 28000-char client cap can exceed 8192 tokens on
# punctuation-dense chat text (sporadic 400s); Qwen3-Embedding supports 32k
# and at 0.6B the extra KV memory is trivial on a whole GCD.
echo "[lumi] GCD $EMBED_GCD: embed vLLM (Qwen3-Embedding-0.6B) on port $EMBED_PORT ..."
singularity exec -B "/scratch/$PROJ" "$VLLM_SIF" \
    bash -c "ROCR_VISIBLE_DEVICES=$EMBED_GCD vllm serve '$EMBED_MODEL_PATH' \
             --served-model-name '$SERVED_EMBED' --port $EMBED_PORT --tensor-parallel-size 1 \
             $EMBED_SERVE_FLAG --max-model-len 32768" \
    > "$LOGS/vllm-embed-${SLURM_JOB_ID}-g${EMBED_GCD}.log" 2>&1 &
server_pids+=($!)
# shellcheck disable=SC2329  # invoked indirectly via `trap stop_servers EXIT`
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
echo "[lumi]   embed GCD $EMBED_GCD up (port $EMBED_PORT)."
echo "[lumi] all servers up."

# ── 2b. encoder pin record + A7 identity smoke ──────────────────────────────
# Writes the served-encoder identity next to the results (the §0 pin is this
# triple: model snapshot + serving stack + input cap), then runs the semantic
# policies for ONE example against the real wired encoder. Skip with
# SKIP_A7_SMOKE=1. VERIFIED (V6): get_embedder() fails loudly when EMBED_MODEL
# != EXPECTED_EMBED_MODEL, so a missing/na wrong .env can't silently fall back
# to the nomic default — every client command below exports the pin.
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
    echo "[lumi] A7 encoder-identity smoke (semantic policies, 1 example, no reader) ..."
    singularity exec -B "/scratch/$PROJ,$REPO" "$CLIENT_SIF" \
        bash -c "cd '$REPO' && PYTHONPATH='$REPO/src' \
                 LLM_BASE_URL='http://localhost:${CHAT_PORT_BASE}/v1' LLM_API_KEY=sk-lumi LLM_MODEL='$SERVED_CHAT' \
                 EMBED_BASE_URL='http://localhost:${EMBED_PORT}/v1' EMBED_API_KEY=sk-lumi EMBED_MODEL='$SERVED_EMBED' \
                 EXPECTED_EMBED_MODEL='$SERVED_EMBED' \
                 EMBED_MAX_INPUT_CHARS=28000 EMBED_CACHE_PATH='$EMB_CACHE_DIR/a7_smoke.sqlite' \
                 python -m forgetit.experiments.run_longmemeval_qa \
                   --policies semantic semantic_keep --limit 1 --no-reader \
                   --shard 0 --num-shards 1 --out-dir '$A7_SMOKE_DIR' --dense" \
        > "$LOGS/a7-smoke-${SLURM_JOB_ID}.log" 2>&1 \
        || { echo "[lumi] REFUSING: A7 smoke failed — see $LOGS/a7-smoke-${SLURM_JOB_ID}.log" >&2; exit 1; }
    echo "[lumi] A7 smoke passed on the wired encoder."
fi

# ── 3. run each dataset's sweep, sharded across the 7 chat servers ──────────
run_dataset () {
    local dataset="$1" module="$2" workers="$3"
    echo "[lumi] ===== $dataset: launching $NUM_SHARDS shards (workers=$workers, reader=OFF) ====="
    local pids=() i cport
    for i in $(seq 0 $((NUM_SHARDS - 1))); do
        cport=$((CHAT_PORT_BASE + i % NUM_CHAT_GCDS))   # round-robin over 7 chat servers
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
                       --no-reader --include-floor \
                       --dense --event-log --workers $workers" \
            > "$LOGS/${dataset}-${SLURM_JOB_ID}-${i}.log" 2>&1 &
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
        longmemeval) run_dataset longmemeval forgetit.experiments.run_longmemeval_qa "$LME_WORKERS" || rc=1 ;;
        beam)        run_dataset beam        forgetit.experiments.run_beam            "$BEAM_WORKERS" || rc=1 ;;
        locomo)      run_dataset locomo      forgetit.experiments.run_locomo_qa       "$LOCOMO_WORKERS" || rc=1 ;;
        *) echo "[lumi] unknown dataset '$ds'" >&2; rc=1 ;;
    esac
done

# ── 4. merge CSVs (adds policy_identity; warns on INCOMPLETE/RAGGED grids) ──────
echo "[lumi] merging CSVs ..."
singularity exec -B "/scratch/$PROJ,$REPO" "$CLIENT_SIF" \
    bash -c "cd '$REPO' && PYTHONPATH='$REPO/src' python scripts/concat_results.py '$OUT_DIR'" || rc=1

# ── 5. eviction-regret (offline) ────────────────────────────────────────────
# VERIFIED (V5): the pass tolerates reader-off runs — the answer-survival tier
# reads the gold SNAPSHOT (resident text), not reader answers; verified green
# with exit 0 on a --no-reader artifact tree.
echo "[lumi] eviction-regret offline pass ..."
singularity exec -B "/scratch/$PROJ,$REPO" "$CLIENT_SIF" \
    bash -c "cd '$REPO' && PYTHONPATH='$REPO/src' \
             python scripts/eviction_regret.py --events-dir '$OUT_DIR' --with-results \
             --out '$OUT_DIR/eviction_regret_report.json'" \
    || echo "[lumi] WARNING: regret pass errored — inspect above"

echo "[lumi] done (exit $rc)."
echo "[lumi]   CSVs:        $OUT_DIR/combined_results.csv + combined_summary.csv"
echo "[lumi]   regret:      $OUT_DIR/eviction_regret_report.json"
echo "[lumi]   encoder pin: $OUT_DIR/ENCODER_PIN.txt"
exit $rc
