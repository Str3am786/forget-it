#!/bin/bash
#SBATCH --account=project_462001123
#SBATCH --partition=standard-g
#SBATCH --nodes=1
#SBATCH --gpus-per-node=8
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=56
#SBATCH --mem=0
#SBATCH --time=00:40:00
#SBATCH --job-name=fi-calib
#SBATCH --output=logs/calib-%j.out
#SBATCH --error=logs/calib-%j.err
#
# run_lumi_calibration.sh — score the grading chain against the hand labels.
#
# WHAT IT ANSWERS. The pipeline's verdict on an answer is produced by one of two
# instruments: a string rule, or the language model judge when no string rule
# fires. A single agreement figure against the hand labels would pool them and
# hide which one is wrong. This job runs the SAME judge the campaign ran, over
# the routed rows of the annotated corpus, and emits agreement per instrument.
#
# The serve stanza is copied from run_lumi_answer_judge.sh unchanged. If that
# file changes, re-diff:
#   diff <(sed -n '/vllm serve/,/^$/p' scripts/lumi/run_lumi_answer_judge.sh) \
#        <(sed -n '/vllm serve/,/^$/p' scripts/lumi/run_lumi_calibration.sh)
#
# The judge model and prompt MUST match the campaign, or the number describes an
# instrument the results were not produced with.
#
#   sbatch scripts/lumi/run_lumi_calibration.sh
# ============================================================================
set -euo pipefail

SLURM_JOB_ID="${SLURM_JOB_ID:-local-$$}"
source scripts/lumi/env.sh

JUDGE_ID="${JUDGE_ID:-qwen2.5-14b-instruct}"
LABELS="${LABELS:-corpora/grader/annotate.csv}"
OUT_DIR="${OUT_DIR:-report/ch5}"
WORKERS="${WORKERS:-16}"
VLLM_WAIT_TRIES="${VLLM_WAIT_TRIES:-720}"
MANIFEST="${MANIFEST:-configs/readers.json}"
LOCK="${LOCK:-configs/readers.lock.json}"

CONTAINER_RT="${CONTAINER_RT:-$(command -v singularity || command -v apptainer || true)}"
[ -n "$CONTAINER_RT" ] || { echo "[cal] no container runtime on PATH" >&2; exit 1; }
THESIS_SIF="${THESIS_SIF:-${CLIENT_SIF:-}}"
[ -e "$THESIS_SIF" ] || { echo "[cal] no client image: $THESIS_SIF" >&2; exit 1; }
[ -f "$LABELS" ]     || { echo "[cal] no labels: $LABELS" >&2; exit 1; }

# The judge is pinned by revision, not by name: a name is not a checkpoint.
eval "$("$CONTAINER_RT" exec --bind /scratch/$PROJ "$THESIS_SIF" \
    python3 - "$MANIFEST" "$LOCK" "$JUDGE_ID" "/scratch/$PROJ/models" <<'PY'
import json, shlex, sys
manifest, lock, jid, models_dir = sys.argv[1:5]
m = json.load(open(manifest))
r = next((x for x in m["readers"] if x["id"] == jid), None)
if r is None:
    print(f"echo '[cal] judge {jid} not in manifest' >&2; exit 1"); raise SystemExit
s = {x["id"]: x for x in json.load(open(lock)).get("staged", [])}.get(jid, {})
print(f"MODEL_PATH={shlex.quote(s.get('local_path') or f'{models_dir}/{jid}')}")
print(f"MODEL_REVISION={shlex.quote(s.get('revision','unstaged'))}")
print(f"VLLM_PARALLEL={shlex.quote(r['vllm_parallel'])}")
print(f"VLLM_EXTRA={shlex.quote(' '.join(r.get('vllm_extra', [])))}")
print(f"GPU_UTIL={shlex.quote(str(r.get('gpu_memory_utilization', 0.90)))}")
print(f"MAX_MODEL_LEN={shlex.quote(str(m['serving_window']))}")
print(f"MODEL_DTYPE={shlex.quote(m['dtype'])}")
PY
)"

[ -d "$MODEL_PATH" ] || { echo "[cal] MISSING weights: $MODEL_PATH" >&2; exit 1; }
[ "$MODEL_REVISION" != "unstaged" ] || {
    echo "[cal] REFUSING: $JUDGE_ID has no pinned revision; the agreement figure" >&2
    echo "[cal]   would name a model rather than a checkpoint." >&2; exit 1; }

SERVED_NAME="$JUDGE_ID"
API_KEY=sk-lumi
PORT=8000
export HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1
export LLM_CACHE_DIR="/tmp/llm_cache_${SLURM_JOB_ID}"
mkdir -p logs "$OUT_DIR" "$LLM_CACHE_DIR"
VLLM_LOG="logs/vllm-${SLURM_JOB_ID}-${JUDGE_ID}.log"

GIT_COMMIT="$(git rev-parse HEAD 2>/dev/null || true)"
[ -z "$(git status --porcelain 2>/dev/null)" ] || GIT_COMMIT="${GIT_COMMIT}-dirty"
export GIT_COMMIT

cat <<EOS
[cal] ─────────────────────────────────────────────────────────────
[cal] job      : $SLURM_JOB_ID on $(hostname)
[cal] judge    : $JUDGE_ID @ $MODEL_REVISION
[cal] labels   : $LABELS
[cal] out      : $OUT_DIR/t51_agreement.csv
[cal] git      : ${GIT_COMMIT:-unknown}
[cal] ─────────────────────────────────────────────────────────────
EOS

# --------------------------------------------------------------- serve ------
"$CONTAINER_RT" exec --bind /scratch/$PROJ "$VLLM_SIF" \
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
trap 'echo "[cal] stopping vLLM ($VLLM_PID)"; kill $VLLM_PID 2>/dev/null || true; rm -rf "$LLM_CACHE_DIR"' EXIT

echo "[cal] waiting for vLLM ..."
UP=0
for _ in $(seq 1 "$VLLM_WAIT_TRIES"); do
    curl -sf "http://localhost:${PORT}/health" >/dev/null && { UP=1; break; }
    kill -0 $VLLM_PID 2>/dev/null || { echo "[cal] vLLM died at startup:" >&2; tail -40 "$VLLM_LOG" >&2; exit 1; }
    sleep 5
done
[ "$UP" = 1 ] || { echo "[cal] vLLM never became healthy" >&2; tail -40 "$VLLM_LOG" >&2; exit 1; }
echo "[cal] vLLM healthy"

# A vendor generation_config would apply a repetition penalty to the judge's
# own output. The campaign ran with none; this must match or the two judges
# are not the same instrument.
N_OVER=$(grep -c "sampling parameters have been overridden" "$VLLM_LOG" || true)
echo "[cal] vendor sampling overrides: $N_OVER  (MUST be 0)"
[ "$N_OVER" = "0" ] || { echo "[cal] vendor generation_config leaked" >&2; exit 1; }

export LLM_BASE_URL="http://localhost:${PORT}/v1"
export LLM_API_KEY="$API_KEY"
export LLM_MODEL="$SERVED_NAME"

# ----------------------------------------------------------------- score ----
RC=0
"$CONTAINER_RT" exec --bind "/scratch/$PROJ,$REPO" "$THESIS_SIF" \
    env LLM_BASE_URL="$LLM_BASE_URL" \
        LLM_API_KEY="$LLM_API_KEY" \
        LLM_MODEL="$LLM_MODEL" \
        LLM_CACHE_DIR="$LLM_CACHE_DIR" \
        GIT_COMMIT="$GIT_COMMIT" \
        PYTHONPATH="$REPO/src" \
    python3 scripts/calibration_rescore.py \
        --labels "$LABELS" \
        --out "$OUT_DIR" \
        --workers "$WORKERS" \
    2>&1 | tee "logs/calib_rescore-${SLURM_JOB_ID}.txt" || RC=1

echo "[cal] done, rc=$RC"
exit "$RC"
