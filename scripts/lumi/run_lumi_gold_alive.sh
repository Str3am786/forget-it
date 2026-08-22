#!/bin/bash
#SBATCH --account=project_462001123
#SBATCH --partition=standard-g
#SBATCH --nodes=1
#SBATCH --gpus-per-node=8
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=56
#SBATCH --mem=0
#SBATCH --time=01:00:00
#SBATCH --job-name=fi-alive
#SBATCH --output=logs/alive-%j.out
#SBATCH --error=logs/alive-%j.err
#
# run_lumi_gold_alive.sh — serve ONE judge, run gold_alive.py over every
# workload and stage against it, tear the server down.
#
# The serve stanza, the health wait and the model variable emit are copied from
# run_lumi_reader_sweep.sh, which ran 528,480 calls with zero errors. If that
# file changes, re-diff:
#   diff <(sed -n '/vllm serve/,/^$/p' scripts/lumi/run_lumi_reader_sweep.sh) \
#        <(sed -n '/vllm serve/,/^$/p' scripts/lumi/run_lumi_gold_alive.sh)
#
# Differences from the reader sweep, all deliberate:
#   - one model, not an array over five
#   - model chosen by JUDGE_ID rather than SLURM_ARRAY_TASK_ID
#   - the client call is gold_alive.py rather than reader_probe.py
set -euo pipefail

cd "${SLURM_SUBMIT_DIR:-$PWD}"
mkdir -p logs
source scripts/lumi/env.sh

# ---------------------------------------------------------------- knobs -----
JUDGE_ID="${JUDGE_ID:-qwen2.5-14b-instruct}"
STAGES="${STAGES:-store}"                       # "store", or "store retrieved"
WORKLOADS="${WORKLOADS:-longmemeval_turn_pair beam_beam_turn_pair locomo_turn_pair}"
SUBSET_ROOT="${SUBSET_ROOT:-/scratch/$PROJ/artifacts/sweep_subset}"
OUT_DIR="${OUT_DIR:-/scratch/$PROJ/artifacts/alive}"
LOCK="${LOCK:-configs/readers.lock.json}"
MANIFEST="${MANIFEST:-configs/readers.json}"
TOP_K="${TOP_K:-10}"
WORKERS="${WORKERS:-32}"
VLLM_WAIT_TRIES="${VLLM_WAIT_TRIES:-720}"       # 720 x 5s = 60 min

# Node-local tmpfs. NOT home (100K inode cap, failure 5.7), NOT scratch (a
# previous campaign left 1.44M inodes there). Dies with the job.
export LLM_CACHE_DIR="/tmp/llm_cache_${SLURM_JOB_ID}"
mkdir -p "$LLM_CACHE_DIR" "$OUT_DIR"

# ------------------------------------------------------------- runtime ------
CONTAINER_RT="${CONTAINER_RT:-$(command -v singularity || command -v apptainer || true)}"
[ -n "$CONTAINER_RT" ] || { echo "[alive] no container runtime on PATH" >&2; exit 1; }
[ -n "${VLLM_SIF:-}" ]   && [ -e "$VLLM_SIF" ]   || { echo "[alive] VLLM_SIF missing"   >&2; exit 1; }
THESIS_SIF="${THESIS_SIF:-${CLIENT_SIF:-}}"
[ -n "$THESIS_SIF" ] && [ -e "$THESIS_SIF" ] || {
    echo "[alive] no client image. env.sh exports CLIENT_SIF; set THESIS_SIF to override." >&2
    echo "[alive] CLIENT_SIF=${CLIENT_SIF:-<unset>}  THESIS_SIF=${THESIS_SIF:-<unset>}" >&2
    exit 1; }
[ -f "$LOCK" ] || { echo "[alive] no $LOCK" >&2; exit 1; }

# --------------------------------------------- model vars from the lock -----
# Same source of truth as the reader sweep. Nothing about the serving stack is
# hardcoded here, so swapping the judge is one variable and not an edit.
eval "$("$CONTAINER_RT" exec --bind /scratch/$PROJ "$THESIS_SIF" python3 - \
        "$LOCK" "$MANIFEST" "$JUDGE_ID" <<'PYEOF'
import json, shlex, sys
lock_p, man_p, rid = sys.argv[1], sys.argv[2], sys.argv[3]
m = json.load(open(lock_p))
models_dir = m.get("models_dir", "")
staged = {s["id"]: s for s in m.get("staged", [])}
if rid not in staged:
    sys.exit(f"[alive] {rid} is not staged in {lock_p}. staged: {sorted(staged)}")
s = staged[rid]
if s.get("status") != "ok":
    sys.exit(f"[alive] {rid} status={s.get('status')}, not ok")

# readers.json shape varies: {id: {...}}, {"readers": [...]}, or [...].
# Walk all three rather than chaining .get, which explodes on a list.
def _find(obj, rid):
    if isinstance(obj, list):
        return next((x for x in obj
                     if isinstance(x, dict) and x.get("id") == rid), {})
    if isinstance(obj, dict):
        v = obj.get(rid)
        if isinstance(v, dict):
            return v
        for key in ("readers", "models", "entries"):
            if key in obj:
                got = _find(obj[key], rid)
                if got:
                    return got
    return {}

try:
    man = json.load(open(man_p))
except (FileNotFoundError, json.JSONDecodeError) as exc:
    print(f"[alive] {man_p}: {exc}, using defaults", file=sys.stderr)
    man = None
r = _find(man, rid) or {}
if not r:
    print(f"[alive] no entry for {rid} in {man_p}, using defaults", file=sys.stderr)

print(f"MODEL_PATH={shlex.quote(s.get('local_path') or f'{models_dir}/{rid}')}")
print(f"MODEL_REVISION={shlex.quote(str(s.get('revision', 'unknown')))}")
print(f"VLLM_PARALLEL={shlex.quote(r.get('vllm_parallel', '--data-parallel-size 8'))}")
print(f"VLLM_EXTRA={shlex.quote(' '.join(r.get('vllm_extra', [])))}")
print(f"GPU_UTIL={shlex.quote(str(r.get('gpu_memory_utilization', 0.90)))}")
print(f"MAX_MODEL_LEN={shlex.quote(str(m['serving_window']))}")
print(f"MODEL_DTYPE={shlex.quote(m['dtype'])}")
PYEOF
)"

[ -d "$MODEL_PATH" ] || {
    echo "[alive] MISSING weights: $MODEL_PATH — run fetch_readers.py on a login node" >&2
    echo "[alive] and NEVER with --only: it overwrites the lock (failure 5.3)" >&2
    exit 1; }

SERVED_NAME="$JUDGE_ID"
API_KEY=sk-lumi
PORT=8000
VLLM_LOG="logs/vllm-${SLURM_JOB_ID}-${JUDGE_ID}.log"

cat <<EOS
[alive] job      : $SLURM_JOB_ID on $(hostname)
[alive] commit   : $(git rev-parse --short HEAD)  dirty=$(git status --porcelain | wc -l)
[alive] judge    : $JUDGE_ID @ $MODEL_REVISION
[alive] weights  : $MODEL_PATH
[alive] window   : $MAX_MODEL_LEN  dtype: $MODEL_DTYPE  util: $GPU_UTIL
[alive] parallel : $VLLM_PARALLEL $VLLM_EXTRA
[alive] stages   : $STAGES
[alive] workloads: $WORKLOADS
[alive] out      : $OUT_DIR
EOS

# --------------------------------------------------------------- serve ------
# --generation-config vllm is NOT optional. Three of five checkpoints ship a
# repetition_penalty that survives greedy decoding (failure 5.6); it silently
# contaminated an entire earlier job.
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
# ONE EXIT handler: a second `trap ... EXIT` REPLACES the first, so the cache
# cleanup is folded in here rather than left to be silently lost.
trap 'echo "[alive] stopping vLLM ($VLLM_PID)"; kill $VLLM_PID 2>/dev/null || true; rm -rf "$LLM_CACHE_DIR"' EXIT

echo "[alive] waiting for vLLM ..."
UP=0
for _ in $(seq 1 "$VLLM_WAIT_TRIES"); do
    curl -sf "http://localhost:${PORT}/health" >/dev/null && { UP=1; break; }
    kill -0 $VLLM_PID 2>/dev/null || { echo "[alive] vLLM died at startup:" >&2; tail -40 "$VLLM_LOG" >&2; exit 1; }
    sleep 5
done
[ "$UP" = 1 ] || { echo "[alive] vLLM never became healthy" >&2; tail -40 "$VLLM_LOG" >&2; exit 1; }
echo "[alive] vLLM healthy"

grep -iE "kv cache|gpu blocks|maximum concurrency|graph captur" "$VLLM_LOG" | tail -8 || true

# Failure 5.6 was invisible without this line. A non-zero count means the
# vendor generation_config leaked past --generation-config vllm.
N_OVER=$(grep -c "sampling parameters have been overridden" "$VLLM_LOG" || true)
echo "[alive] vendor sampling overrides: $N_OVER  (MUST be 0)"
[ "$N_OVER" = "0" ] || { echo "[alive] vendor generation_config leaked" >&2; exit 1; }

export LLM_BASE_URL="http://localhost:${PORT}/v1"
export LLM_API_KEY="$API_KEY"
export LLM_MODEL="$SERVED_NAME"

# ---------------------------------------------------------------- judge -----
RC=0
for w in $WORKLOADS; do
    ROOT="$SUBSET_ROOT/$w"
    [ -d "$ROOT" ] || { echo "[alive] SKIP missing $ROOT" >&2; RC=1; continue; }
    for s in $STAGES; do
        echo "[alive] === $w / $s"
        "$CONTAINER_RT" exec --bind /scratch/$PROJ "$THESIS_SIF" \
            env LLM_BASE_URL="$LLM_BASE_URL" \
                LLM_API_KEY="$LLM_API_KEY" \
                LLM_MODEL="$LLM_MODEL" \
                LLM_CACHE_DIR="$LLM_CACHE_DIR" \
            python3 scripts/gold_alive.py "$ROOT" \
                --stage "$s" \
                --top-k "$TOP_K" \
                --workers "$WORKERS" \
                --out "$OUT_DIR/${s}_${w}.csv" \
            2>&1 | tee "logs/alive_${s}_${w}-${SLURM_JOB_ID}.txt" || RC=1
    done
done

# ------------------------------------------------------------- manifest -----
"$CONTAINER_RT" exec --bind /scratch/$PROJ "$THESIS_SIF" python3 - <<PYEOF
import json, pathlib
pathlib.Path("$OUT_DIR/manifest.json").write_text(json.dumps({
    "job_id": "$SLURM_JOB_ID",
    "git_commit": "$(git rev-parse HEAD)",
    "judge_id": "$JUDGE_ID",
    "judge_revision": "$MODEL_REVISION",
    "model_path": "$MODEL_PATH",
    "dtype": "$MODEL_DTYPE",
    "max_model_len": int("$MAX_MODEL_LEN"),
    "parallel": "$VLLM_PARALLEL $VLLM_EXTRA".strip(),
    "gpu_memory_utilization": float("$GPU_UTIL"),
    "stages": "$STAGES".split(),
    "workloads": "$WORKLOADS".split(),
    "top_k": int("$TOP_K"),
    "temperature": 0.0,
    "vendor_sampling_overrides": int("$N_OVER"),
}, indent=2))
print("[alive] wrote $OUT_DIR/manifest.json")
PYEOF

echo "[alive] done, rc=$RC"
ls -lh "$OUT_DIR"
exit "$RC"
