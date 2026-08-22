#!/bin/bash
#SBATCH --account=project_462001123
#SBATCH --partition=standard-g
#SBATCH --nodes=1
#SBATCH --gpus-per-node=8
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=56
#SBATCH --mem=0
#SBATCH --time=05:00:00
#SBATCH --job-name=fi-ansjudge
#SBATCH --output=logs/ansjudge-%j.out
#SBATCH --error=logs/ansjudge-%j.err
#
# run_lumi_answer_judge.sh — serve ONE judge, run judge_reader_answers.py over
# every reader row file in the sweep, tear the server down.
#
# Same skeleton as run_lumi_gold_alive.sh with all its fixes: no --rocm (the
# container's own ROCm must win), CLIENT_SIF fallback, readers.json _find
# lookup. The client is judge_reader_answers.py, prompt sha cd216911a12f,
# UNCHANGED from calibration: its resume is keyed on row identity, not prompt,
# so an edited prompt would silently mix two instruments in one output.
#
# The judge (14B) is also a reader in the roster, so it judges its own
# answers on one fifth of rows. Common-mode across policies, declared in the
# methods, not a blocker.
set -euo pipefail

cd "${SLURM_SUBMIT_DIR:-$PWD}"
mkdir -p logs
source scripts/lumi/env.sh

# ---------------------------------------------------------------- knobs -----
JUDGE_ID="${JUDGE_ID:-qwen2.5-14b-instruct}"
WORKLOADS="${WORKLOADS:-longmemeval_turn_pair beam_beam_turn_pair locomo_turn_pair}"
SWEEP_ROOT="${SWEEP_ROOT:-/scratch/$PROJ/artifacts/reader_sweep_full}"
OUT_DIR="${OUT_DIR:-/scratch/$PROJ/artifacts/judged}"
LOCK="${LOCK:-configs/readers.lock.json}"
MANIFEST="${MANIFEST:-configs/readers.json}"
WORKERS="${WORKERS:-32}"
VLLM_WAIT_TRIES="${VLLM_WAIT_TRIES:-720}"

export LLM_CACHE_DIR="/tmp/llm_cache_${SLURM_JOB_ID}"   # tmpfs, dies with job
mkdir -p "$LLM_CACHE_DIR" "$OUT_DIR"

# ------------------------------------------------------------- runtime ------
CONTAINER_RT="${CONTAINER_RT:-$(command -v singularity || command -v apptainer || true)}"
[ -n "$CONTAINER_RT" ] || { echo "[aj] no container runtime on PATH" >&2; exit 1; }
[ -n "${VLLM_SIF:-}" ] && [ -e "$VLLM_SIF" ] || { echo "[aj] VLLM_SIF missing" >&2; exit 1; }
THESIS_SIF="${THESIS_SIF:-${CLIENT_SIF:-}}"
[ -n "$THESIS_SIF" ] && [ -e "$THESIS_SIF" ] || {
    echo "[aj] no client image. CLIENT_SIF=${CLIENT_SIF:-<unset>}" >&2; exit 1; }
[ -f "$LOCK" ] || { echo "[aj] no $LOCK" >&2; exit 1; }

# --------------------------------------------- model vars from the lock -----
eval "$("$CONTAINER_RT" exec --bind /scratch/$PROJ "$THESIS_SIF" python3 - \
        "$LOCK" "$MANIFEST" "$JUDGE_ID" <<'PYEOF'
import json, shlex, sys
lock_p, man_p, rid = sys.argv[1], sys.argv[2], sys.argv[3]
m = json.load(open(lock_p))
staged = {s["id"]: s for s in m.get("staged", [])}
if rid not in staged:
    sys.exit(f"[aj] {rid} not staged; staged: {sorted(staged)}")
s = staged[rid]
if s.get("status") != "ok":
    sys.exit(f"[aj] {rid} status={s.get('status')}")

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
except (FileNotFoundError, json.JSONDecodeError):
    man = None
r = _find(man, rid) or {}

print(f"MODEL_PATH={shlex.quote(s.get('local_path') or '')}")
print(f"MODEL_REVISION={shlex.quote(str(s.get('revision', 'unknown')))}")
print(f"VLLM_PARALLEL={shlex.quote(r.get('vllm_parallel', '--data-parallel-size 8'))}")
print(f"VLLM_EXTRA={shlex.quote(' '.join(r.get('vllm_extra', [])))}")
print(f"GPU_UTIL={shlex.quote(str(r.get('gpu_memory_utilization', 0.90)))}")
print(f"MAX_MODEL_LEN={shlex.quote(str(m['serving_window']))}")
print(f"MODEL_DTYPE={shlex.quote(m['dtype'])}")
PYEOF
)"
[ -d "$MODEL_PATH" ] || { echo "[aj] MISSING weights: $MODEL_PATH" >&2; exit 1; }

SERVED_NAME="$JUDGE_ID"
API_KEY=sk-lumi
PORT=8000
VLLM_LOG="logs/vllm-${SLURM_JOB_ID}-${JUDGE_ID}.log"

cat <<EOS
[aj] job      : $SLURM_JOB_ID on $(hostname)
[aj] commit   : $(git rev-parse --short HEAD)  dirty=$(git status --porcelain | wc -l)
[aj] judge    : $JUDGE_ID @ $MODEL_REVISION
[aj] sweep    : $SWEEP_ROOT
[aj] out      : $OUT_DIR
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
trap 'echo "[aj] stopping vLLM ($VLLM_PID)"; kill $VLLM_PID 2>/dev/null || true; rm -rf "$LLM_CACHE_DIR"' EXIT

echo "[aj] waiting for vLLM ..."
UP=0
for _ in $(seq 1 "$VLLM_WAIT_TRIES"); do
    curl -sf "http://localhost:${PORT}/health" >/dev/null && { UP=1; break; }
    kill -0 $VLLM_PID 2>/dev/null || { echo "[aj] vLLM died at startup:" >&2; tail -40 "$VLLM_LOG" >&2; exit 1; }
    sleep 5
done
[ "$UP" = 1 ] || { echo "[aj] vLLM never became healthy" >&2; tail -40 "$VLLM_LOG" >&2; exit 1; }
echo "[aj] vLLM healthy"

N_OVER=$(grep -c "sampling parameters have been overridden" "$VLLM_LOG" || true)
echo "[aj] vendor sampling overrides: $N_OVER  (MUST be 0)"
[ "$N_OVER" = "0" ] || { echo "[aj] vendor generation_config leaked" >&2; exit 1; }

export LLM_BASE_URL="http://localhost:${PORT}/v1"
export LLM_API_KEY="$API_KEY"
export LLM_MODEL="$SERVED_NAME"

# ---------------------------------------------------------------- judge -----
RC=0
for w in $WORKLOADS; do
    for rdir in "$SWEEP_ROOT/$w"/*/; do
        rd="$(basename "$rdir")"
        [ -d "$rdir" ] || continue
        mapfile -t files < <(ls "$rdir"reader_*.jsonl 2>/dev/null)
        if [ "${#files[@]}" -eq 0 ]; then
            echo "[aj] SKIP $w/$rd: no reader_*.jsonl" >&2; continue
        fi
        if [ "${#files[@]}" -gt 1 ]; then
            echo "[aj] STOP $w/$rd: ${#files[@]} row files, refusing to guess:" >&2
            printf '  %s\n' "${files[@]}" >&2; RC=1; continue
        fi
        echo "[aj] === $w / $rd"
        "$CONTAINER_RT" exec --bind /scratch/$PROJ "$THESIS_SIF" \
            env LLM_BASE_URL="$LLM_BASE_URL" \
                LLM_API_KEY="$LLM_API_KEY" \
                LLM_MODEL="$LLM_MODEL" \
                LLM_CACHE_DIR="$LLM_CACHE_DIR" \
            python3 scripts/judge_reader_answers.py "${files[0]}" \
                --out "$OUT_DIR/judged_${w}_${rd}.csv" \
                --tiers miss content_words reverse \
                --workers "$WORKERS" \
            2>&1 | tee "logs/ansjudge_${w}_${rd}-${SLURM_JOB_ID}.txt" || RC=1
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
    "routed_tiers": ["miss", "content_words", "reverse"],
    "workloads": "$WORKLOADS".split(),
    "temperature": 0.0,
    "vendor_sampling_overrides": int("$N_OVER"),
}, indent=2))
print("[aj] wrote $OUT_DIR/manifest.json")
PYEOF

echo "[aj] done, rc=$RC"
ls -lh "$OUT_DIR"
exit "$RC"
