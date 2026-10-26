#!/bin/bash
#
# ── SYNC GATE — run this on LUMI BEFORE any sbatch of this script ───────────
# The dated renderer changes what every row means, so a run whose code is not
# the committed, tested code produces rows nothing can later verify. In order:
#
#   1. git -C "$REPO" pull                      # the laptop work must be pushed
#   2. singularity exec -B "/scratch/$PROJ,$REPO" "$CLIENT_SIF" bash -c \
#          "cd '$REPO' && PYTHONPATH='$REPO/src' python -c \
#           'from forgetit.llm.readers import render_sha, DEFAULT_PROMPT; \
#            print(render_sha(DEFAULT_PROMPT))'"
#      -> must print 98d35e968dcf. Anything else means the pull did not land,
#         or the renderer changed, and this sweep would write rows whose
#         evidence block is not the one the tables assume.
#   3. singularity exec --bind /scratch/$PROJ "$CLIENT_SIF" \
#          python3 -m pytest tests/ -q
#      -> the whole suite, including tests/test_dated_render.py.
#
# Only when all three pass is this script safe to submit.
# ───────────────────────────────────────────────────────────────────────────

# ============================================================================
# forget-it on LUMI — the reader sweep (model axis).
#
# One array task per reader. Each task takes a node, serves exactly one model,
# runs scripts/reader_probe.py against an existing --dump-retrieval run, and
# dies. Models never share a node, so a failure at 14B cannot poison the 1.5B
# rows.
#
# WHY THIS IS CHEAP: the policy run already wrote retrieved_index.jsonl per
# config and evidence_items.shard<N>.jsonl at the run root. reader_probe.py
# rebuilds each query's evidence in stored rank order and runs four arms
# (parametric / empty / retrieved / gold) with no store, no policy and no
# eviction. The reader model comes from LLM_MODEL, so sweeping readers is
# re-running the same script under different env. No embedder is needed, which
# is why this serves 8 data parallel replicas rather than the campaign's 7.
#
# Submit:
#   export SBATCH_ACCOUNT=project_<your-id>
#   cd <repo-root>
#   RUN_DIR=artifacts/benchmark/<run> \
#   READERS="qwen2.5-1.5b-instruct qwen2.5-7b-instruct qwen2.5-14b-instruct gemma-3-12b-it" \
#     sbatch --array=0-3 scripts/lumi/run_lumi_reader_sweep.sh
# ============================================================================
#SBATCH --job-name=fi-readers
#SBATCH --partition=standard-g
#SBATCH --nodes=1
#SBATCH --gpus-per-node=8
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=56
#SBATCH --mem=200G
#SBATCH --time=06:00:00
#SBATCH --requeue
#SBATCH --open-mode=append
#SBATCH --output=logs/fi-readers-%A_%a.out

set -euo pipefail

SLURM_JOB_ID="${SLURM_JOB_ID:-local-$$}"
SLURM_ARRAY_TASK_ID="${SLURM_ARRAY_TASK_ID:-0}"
SLURM_RESTART_COUNT="${SLURM_RESTART_COUNT:-0}"

# ---- identity & paths, derived not hardcoded (matches run_lumi_full_eval.sh)
PROJ="${SLURM_JOB_ACCOUNT:?no Slurm account — submit via sbatch with SBATCH_ACCOUNT exported}"
REPO="${SLURM_SUBMIT_DIR:?}"
if [ ! -e "$REPO/pyproject.toml" ] || [ ! -d "$REPO/src/forgetit" ]; then
    echo "[sweep] REFUSING: sbatch was not run from the repo root (SLURM_SUBMIT_DIR=$REPO)" >&2
    exit 1
fi
SCRATCH="/scratch/$PROJ"
MODELS_DIR="${MODELS_DIR:-$SCRATCH/models}"
MANIFEST="${MANIFEST:-$REPO/configs/readers.json}"
LOCK="${LOCK:-$REPO/configs/readers.lock.json}"
export HF_HOME="${HF_HOME:-$SCRATCH/hf_cache}"

# ---- containers, resolved and PINNED ---------------------------------------
# lumi-multitorch-latest.sif is a symlink into a rotating dated directory. It
# happens to resolve to 20260807_115122 today; that is luck, not provenance.
# Pinning the release is what makes "which stack served this" a recorded fact.
CONTAINER_RT="${CONTAINER_RT:-$(command -v singularity || command -v apptainer || true)}"
[ -n "$CONTAINER_RT" ] || { echo "[sweep] no container runtime on PATH" >&2; exit 1; }

module use /appl/local/laifs/modules 2>/dev/null || true
module load lumi-aif-singularity-bindings 2>/dev/null || true

LAIF_RELEASE="${LAIF_RELEASE:-lumi-multitorch-u24r70f21m50t210-20260807_115122}"
VLLM_SIF="${VLLM_SIF:-/appl/local/laifs/containers/$LAIF_RELEASE/lumi-multitorch-full-${LAIF_RELEASE#lumi-multitorch-}.sif}"
CLIENT_SIF="${CLIENT_SIF:-$(dirname "$REPO")/forgetit.sif}"

for _p in "$VLLM_SIF" "$CLIENT_SIF" "$MANIFEST" "$LOCK"; do
    [ -e "$_p" ] || { echo "[sweep] MISSING: $_p" >&2; exit 1; }
done

# ---- the run being replayed ------------------------------------------------
# reader_probe.py takes the run dir positionally and --retrieval pointing at the
# same place (or at a specific retrieved_index.jsonl). Both the gold snapshot
# and results.csv must be present: that is where the questions and the gold
# ceiling come from.
RUN_DIR="${RUN_DIR:?set RUN_DIR to the --dump-retrieval run, e.g. artifacts/benchmark/<run>}"
[ "${RUN_DIR#/}" = "$RUN_DIR" ] && RUN_DIR="$REPO/$RUN_DIR"
RETRIEVAL_DIR="${RETRIEVAL_DIR:-$RUN_DIR}"

# reader_probe.find_run_dirs() recurses: it accepts a tree and discovers every
# config dir holding BOTH files. Assert the pair exists somewhere beneath, not
# at the root — a curated subset has nothing at its own top level.
_n_cfg=$(find -L "$RUN_DIR" -name gold_answer_snapshot.jsonl \
         -execdir test -f results.csv \; -print 2>/dev/null | wc -l)
[ "$_n_cfg" -gt 0 ] || {
    echo "[sweep] REFUSING: no results.csv + gold_answer_snapshot.jsonl pair under $RUN_DIR" >&2
    exit 1; }
echo "[sweep] config dirs discovered: $_n_cfg"
_n_items=$(ls "$RETRIEVAL_DIR"/evidence_items.shard*.jsonl 2>/dev/null | wc -l)
_n_index=$(find "$RETRIEVAL_DIR" -name retrieved_index.jsonl 2>/dev/null | wc -l)
if [ "$_n_items" -eq 0 ] || [ "$_n_index" -eq 0 ]; then
    echo "[sweep] REFUSING: $RETRIEVAL_DIR has $_n_items item shard(s) and $_n_index retrieved_index.jsonl." >&2
    echo "[sweep]   The run was not produced with --dump-retrieval, or the copy is incomplete." >&2
    exit 1
fi

# ---- knobs -----------------------------------------------------------------
DATASET="${DATASET:-}"          # substring filter on the dataset column; empty = all
LIMIT="${LIMIT:-0}"             # 0 = every scored question; the only grid reducer
OUT_ROOT="${OUT_DIR:-$REPO/artifacts/reader_sweep_dated}"

# Smoke path: a file of query ids restricts the run to those questions. Made
# absolute here so a relative path is not resolved against the container's
# working directory.
IDS_FILE="${IDS_FILE:-}"
if [ -n "$IDS_FILE" ]; then
    [ "${IDS_FILE#/}" = "$IDS_FILE" ] && IDS_FILE="$REPO/$IDS_FILE"
    [ -f "$IDS_FILE" ] || { echo "[sweep] no such ids file: $IDS_FILE" >&2; exit 2; }
fi

# Raw dataset files for the render-time date lookup. reader_probe.py defaults to
# <repo>/data; the check is here because a missing directory does not fail, it
# renders every memory item without its date and reproduces the defect this
# sweep exists to remove.
DATA_DIR="${DATA_DIR:-$REPO/data}"
for _d in longmemeval_s_cleaned.json locomo10.json beam_500K.jsonl; do
    [ -f "$DATA_DIR/$_d" ] || {
        echo "[sweep] REFUSING: $DATA_DIR/$_d not found; rows would render undated" >&2
        exit 2; }
done

# Appending dated rows to a file of undated ones would merge two experiments in
# one artefact. reader_probe.py refuses a complete file unless told; RESUME=1
# passes --resume through for a genuinely interrupted task.
RESUME="${RESUME:-}"

# Per reader directory: no two array tasks share a path, and the merge
# at analysis time is a glob rather than a filename convention.

# HELD CONSTANT across the axis. The dump stores retrieval output rather than a
# rendered prompt, so a replay MAY hand a bigger reader more evidence. For a
# capacity axis that affordance is a trap: it would confound capacity with
# evidence volume. Pinned to the campaign's budget; deliberately not swept.
TOKEN_BUDGET="${TOKEN_BUDGET:-16000}"

# Which reader_probe.py arms to run. Empty = the script's default, the
# pre-registered set. ARMS=resident runs only the post hoc resident arm; give
# it its own OUT_DIR so it never appends to a finished sweep.
ARMS="${ARMS:-}"

PROMPT_FILE="${PROMPT_FILE:-}"
if [ -n "$PROMPT_FILE" ]; then
    [ "${PROMPT_FILE#/}" = "$PROMPT_FILE" ] && PROMPT_FILE="$REPO/$PROMPT_FILE"
    [ -f "$PROMPT_FILE" ] || { echo "[sweep] no such prompt file: $PROMPT_FILE" >&2; exit 2; }
fi

# ---- resolve which reader this array task owns -----------------------------
if [ -n "${READERS:-}" ]; then
    read -r -a READER_IDS <<< "$READERS"
else
    mapfile -t READER_IDS < <(python3 -c '
import json,sys
m=json.load(open(sys.argv[1]))
print("\n".join(r["id"] for r in m["readers"] if not r.get("deferred")))' "$MANIFEST")
fi
N_READERS=${#READER_IDS[@]}
if [ "$SLURM_ARRAY_TASK_ID" -ge "$N_READERS" ]; then
    echo "[sweep] REFUSING: array index $SLURM_ARRAY_TASK_ID exceeds roster size $N_READERS." >&2
    echo "[sweep]   Submit --array=0-$((N_READERS-1))." >&2
    exit 1
fi
READER_ID="${READER_IDS[$SLURM_ARRAY_TASK_ID]}"

eval "$(python3 - "$MANIFEST" "$LOCK" "$READER_ID" "$MODELS_DIR" <<'PY'
import json, shlex, sys
manifest, lock, rid, models_dir = sys.argv[1:5]
m = json.load(open(manifest))
r = next((x for x in m["readers"] if x["id"] == rid), None)
if r is None:
    print(f"echo '[sweep] reader {rid} not in manifest' >&2; exit 1"); raise SystemExit
lk = json.load(open(lock))
staged = {s["id"]: s for s in lk.get("staged", [])}
s = staged.get(rid, {})
pin = "qwen2.5-7b-instruct"
ref_cpt = next((x.get("chars_per_token") for x in lk.get("staged", [])
                if x["id"] == pin and x.get("chars_per_token")), "")
print(f"MODEL_PATH={shlex.quote(s.get('local_path') or f'{models_dir}/{rid}')}")
print(f"MODEL_REVISION={shlex.quote(s.get('revision','unstaged'))}")
print(f"MODEL_FAMILY={shlex.quote(r['family'])}")
print(f"MODEL_BAND={shlex.quote(r['band'])}")
print(f"MODEL_CPT={shlex.quote(str(s.get('chars_per_token','')))}")
print(f"REF_CPT={shlex.quote(str(ref_cpt))}")
print(f"VLLM_PARALLEL={shlex.quote(r['vllm_parallel'])}")
print(f"VLLM_EXTRA={shlex.quote(' '.join(r.get('vllm_extra', [])))}")
print(f"GPU_UTIL={shlex.quote(str(r.get('gpu_memory_utilization', 0.90)))}")
print(f"MAX_MODEL_LEN={shlex.quote(str(m['serving_window']))}")
print(f"MODEL_DTYPE={shlex.quote(m['dtype'])}")
print(f"DP_SIZE={shlex.quote(r['vllm_parallel'].split()[-1] if 'data-parallel' in r['vllm_parallel'] else '1')}")
PY
)"

[ -d "$MODEL_PATH" ] || { echo "[sweep] MISSING weights: $MODEL_PATH — run fetch_readers.py on a login node" >&2; exit 1; }
[ "$MODEL_REVISION" != "unstaged" ] || {
    echo "[sweep] REFUSING: $READER_ID has no pinned revision in $LOCK; rows would be" >&2
    echo "[sweep]   attributable to a name rather than a checkpoint." >&2; exit 1; }

# SERVED_NAME is the reader id, never 'local'. reader_probe.py folds LLM_MODEL
# into the output filename and the llm_model column, so this string IS how two
# readers' rows are told apart.
SERVED_NAME="$READER_ID"
API_KEY=sk-lumi
PORT=8000
export HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1

OUT_DIR="$OUT_ROOT/$READER_ID"
mkdir -p logs "$OUT_DIR"
cd "$REPO"

GIT_COMMIT="$(git -C "$REPO" rev-parse HEAD 2>/dev/null || true)"
if [ -n "$GIT_COMMIT" ] && [ -n "$(git -C "$REPO" status --porcelain 2>/dev/null)" ]; then
    GIT_COMMIT="${GIT_COMMIT}-dirty"
fi
export GIT_COMMIT
[ -n "$GIT_COMMIT" ] || echo "[sweep] WARNING: no git commit resolvable — rows stamped 'unknown'" >&2

cat <<EOF
[sweep] ─────────────────────────────────────────────────────────────
[sweep] task     : $SLURM_ARRAY_TASK_ID of $N_READERS   (restart #$SLURM_RESTART_COUNT)
[sweep] reader   : $READER_ID  (family=$MODEL_FAMILY band=$MODEL_BAND)
[sweep] weights  : $MODEL_PATH @ $MODEL_REVISION
[sweep] window   : $MAX_MODEL_LEN  dtype: $MODEL_DTYPE  chars/tok: ${MODEL_CPT:-unmeasured}
[sweep] stack    : $LAIF_RELEASE
[sweep] run dir  : $RUN_DIR
[sweep] budget   : $TOKEN_BUDGET (held constant across the axis)
[sweep] dataset  : ${DATASET:-<all>}   limit: $LIMIT
[sweep] data dir : $DATA_DIR
[sweep] ids      : ${IDS_FILE:-<all questions>}
[sweep] prompt   : ${PROMPT_FILE:-<default>}
[sweep] arms     : ${ARMS:-<default>}
[sweep] out      : $OUT_DIR
[sweep] git      : ${GIT_COMMIT:-unknown}
[sweep] ─────────────────────────────────────────────────────────────
EOF

# ---- 1. serve --------------------------------------------------------------
# --max-model-len is pinned from the manifest for EVERY reader, including those
# whose native window is four times larger. The serving window is a held
# constant; letting each model use its own maximum would vary it along the axis.
# LLM cache to node-local tmpfs, NEVER to home or scratch. It is one small
# JSON file per call: a previous campaign put 1.44M inodes on scratch (60% of
# the project) and exhausted home's 100K file quota tonight. Small in bytes, so
# tmpfs is free, and the inodes die with the job.
export LLM_CACHE_DIR="/tmp/llm_cache_${SLURM_JOB_ID}"
mkdir -p "$LLM_CACHE_DIR"
trap 'rm -rf "$LLM_CACHE_DIR"' EXIT

VLLM_LOG="logs/vllm-${SLURM_JOB_ID}-${READER_ID}.log"
echo "[sweep] launching vLLM ..."
# shellcheck disable=SC2086
# --generation-config vllm: ignore each checkpoint's generation_config.json.
# Qwen2.5 ships repetition_penalty (1.1 on the 1.5B, 1.05 on 7B/14B), Llama and
# Gemma ship none. The client sends temperature=0, but a repetition penalty is a
# logits processor and still alters GREEDY output, and vLLM applies it over
# prompt tokens too, which penalises the verbatim copying that containment
# grading scores. Left on, it is an uncontrolled variable sitting on the model
# axis, and it differs within the Qwen family.
"$CONTAINER_RT" exec -B "/scratch/$PROJ" "$VLLM_SIF" \
    /opt/venv/bin/vllm serve "$MODEL_PATH" \
        --served-model-name "$SERVED_NAME" \
        --generation-config vllm \
        --host 0.0.0.0 --port "$PORT" \
        --api-key "$API_KEY" \
        --max-model-len "$MAX_MODEL_LEN" \
        --dtype "$MODEL_DTYPE" \
        --gpu-memory-utilization "$GPU_UTIL" \
        $VLLM_PARALLEL $VLLM_EXTRA \
    > "$VLLM_LOG" 2>&1 &
VLLM_PID=$!
# One EXIT handler: a second `trap ... EXIT` REPLACES the first, so the cache
# cleanup on line 206 is folded in here rather than left to be silently lost.
trap 'echo "[sweep] stopping vLLM ($VLLM_PID)"; kill $VLLM_PID 2>/dev/null || true; rm -rf "$LLM_CACHE_DIR"' EXIT

echo "[sweep] waiting for vLLM ..."
UP=0
for _ in $(seq 1 "${VLLM_WAIT_TRIES:-720}"); do
    curl -sf "http://localhost:${PORT}/health" >/dev/null && { UP=1; break; }
    kill -0 $VLLM_PID 2>/dev/null || { echo "[sweep] vLLM died at startup:" >&2; tail -40 "$VLLM_LOG" >&2; exit 1; }
    sleep 5
done
[ "$UP" = 1 ] || { echo "[sweep] vLLM never became healthy" >&2; tail -40 "$VLLM_LOG" >&2; exit 1; }

grep -iE "kv cache|gpu blocks|maximum concurrency|graph captur" "$VLLM_LOG" | tail -8 \
    | tee "$OUT_DIR/serving_capacity.$READER_ID.txt" || true

# Derived from what the server reports rather than hardcoded: at one window,
# 14B has roughly a fifth of the KV budget of 1.5B, so one number cannot be
# right for both. Used for the preflight benchmark and the client pool size.
# NOTE: reader_probe.py exposes no worker flag, so how it saturates 8 replicas
# is still open; if it is serial this only affects the benchmark.
if [ -z "${READER_WORKERS:-}" ]; then
    PER_REPLICA="$(grep -oiE "maximum concurrency for [0-9,]+ tokens per request: *([0-9.]+)x" "$VLLM_LOG" \
                   | grep -oE "[0-9.]+x" | tr -d x | head -n1 || true)"
    if [ -n "$PER_REPLICA" ]; then
        READER_WORKERS=$(python3 -c "print(max(4,min(64,int(float('$PER_REPLICA')*int('${DP_SIZE:-1}')*0.6))))")
        echo "[sweep] READER_WORKERS=$READER_WORKERS derived (${PER_REPLICA}x x ${DP_SIZE} replicas)"
    else
        READER_WORKERS=16
        echo "[sweep] WARNING: could not parse concurrency from vLLM log; using 16" >&2
    fi
fi

export LLM_BASE_URL="http://localhost:${PORT}/v1"
export LLM_API_KEY="$API_KEY"
export LLM_MODEL="$SERVED_NAME"
export LLM_MAX_CONNECTIONS=$(( READER_WORKERS + 8 ))

# ---- 2. preflight ----------------------------------------------------------
# Three failures invisible in aggregate accuracy and fatal to a cross family
# comparison: a template that renders the prompt structurally differently
# (Gemma has no system role), format compliance low enough that tier one
# grading scores style rather than knowledge, and a tokeniser packing evidence
# at a different density so the held budget is not a constant.
# Inside CLIENT_SIF: the compute nodes' system python3 is 3.6 and cannot
# parse `from __future__ import annotations`. Same container as the payload,
# so both halves run on one interpreter.
"$CONTAINER_RT" exec -B "/scratch/$PROJ,$REPO" "$CLIENT_SIF" \
    python scripts/preflight_reader.py \
        --base-url "$LLM_BASE_URL" --api-key "$API_KEY" --model "$SERVED_NAME" \
        --reader-id "$READER_ID" --model-path "$MODEL_PATH" \
        ${REF_CPT:+--reference-cpt "$REF_CPT"} ${MODEL_CPT:+--measured-cpt "$MODEL_CPT"} \
        --bench 24 --bench-concurrency "$READER_WORKERS" \
        --out "$OUT_DIR/preflight.$READER_ID.json" \
        ${PREFLIGHT_STRICT:+--strict}

# ---- 3. replay -------------------------------------------------------------
# Resumability is the script's own: rows append to <out>/<reader-name>.jsonl and
# already-done (query_id, arm) pairs are skipped, so a requeued task continues.
PROBE_LOG="logs/probe-${SLURM_JOB_ID}-${READER_ID}.log"
set +e
"$CONTAINER_RT" exec -B "/scratch/$PROJ,$REPO" "$CLIENT_SIF" \
    bash -c "cd '$REPO' && PYTHONPATH='$REPO/src' \
             LLM_BASE_URL='$LLM_BASE_URL' LLM_API_KEY='$LLM_API_KEY' LLM_MODEL='$LLM_MODEL' \
             LLM_MAX_CONNECTIONS='$LLM_MAX_CONNECTIONS' \
             python scripts/reader_probe.py '$RUN_DIR' \
                 --retrieval '$RETRIEVAL_DIR' \
                 --token-budget '$TOKEN_BUDGET' \
                 --workers '$READER_WORKERS' \
                 --data-dir '$DATA_DIR' \
                 ${IDS_FILE:+--ids '$IDS_FILE'} \
                 ${RESUME:+--resume} \
                 ${PROMPT_FILE:+--prompt-file '$PROMPT_FILE'} \
                 ${ARMS:+--arms $ARMS} \
                 ${DATASET:+--dataset '$DATASET'} \
                 $( [ "$LIMIT" -gt 0 ] && echo "--limit '$LIMIT'" ) \
                 --out '$OUT_DIR'" 2>&1 | tee "$PROBE_LOG"
RC=${PIPESTATUS[0]}
set -e

# ---- 4. dangling key gate --------------------------------------------------
# Index entries whose key is absent from the merged item table replay as EMPTY
# TEXT, which silently deflates the retrieved arm and therefore inflates the
# policy loss. This must be read, not assumed.
if grep -qi "dangling" "$PROBE_LOG"; then
    echo "[sweep] ATTENTION: dangling keys reported for $READER_ID." >&2
    grep -i "dangling" "$PROBE_LOG" | head -5 >&2
    echo "[sweep]   The retrieved arm is NOT trustworthy until the item shards are complete." >&2
    echo "[sweep]   Check that every evidence_items.shard*.jsonl from the run was copied." >&2
fi

# ---- 5. provenance ---------------------------------------------------------
python3 - <<PY
import json, os
json.dump({
    "reader_id": "$READER_ID", "model_path": "$MODEL_PATH",
    "revision": "$MODEL_REVISION", "family": "$MODEL_FAMILY", "band": "$MODEL_BAND",
    "chars_per_token": "$MODEL_CPT" or None, "served_name": "$SERVED_NAME",
    "max_model_len": int("$MAX_MODEL_LEN"), "dtype": "$MODEL_DTYPE",
    "parallel": "$VLLM_PARALLEL $VLLM_EXTRA".strip(),
    "gpu_memory_utilization": float("$GPU_UTIL"),
    "laif_release": "$LAIF_RELEASE", "client_sif": "$CLIENT_SIF",
    "run_dir": "$RUN_DIR", "retrieval_dir": "$RETRIEVAL_DIR",
    "replay_token_budget": int("$TOKEN_BUDGET"),
    "dataset_filter": "$DATASET", "limit": int("$LIMIT"),
    "data_dir": "$DATA_DIR", "ids_file": "$IDS_FILE", "resume": bool("$RESUME"),
    "prompt_file": "$PROMPT_FILE",
    "arms": "$ARMS".split() or "default",
    "reader_workers_hint": int("$READER_WORKERS"),
    "git_commit": os.environ.get("GIT_COMMIT", "unknown"),
    "slurm_job": "$SLURM_JOB_ID", "restart_count": int("$SLURM_RESTART_COUNT"),
    "exit_code": int("$RC"),
}, open("$OUT_DIR/run_manifest.$READER_ID.json", "w"), indent=2)
PY

echo "[sweep] done: $READER_ID (rc=$RC) -> $OUT_DIR"
exit "$RC"
