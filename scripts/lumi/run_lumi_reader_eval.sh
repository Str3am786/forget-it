#!/bin/bash
# ============================================================================
# forget-it on LUMI — READER-ON EVALUATION (8 GCDs) — DEDICATED-EMBED layout
#
# The ANSWER half. The state-forming grid (benchmark_full, --no-reader) is
# complete and frozen; this job re-runs the same frozen grid WITH the reader.
# Because `reader` is inside _config_hash, every config's hash differs from
# the no-reader run — the DONE guard auto-invalidates, nothing needs
# FORGETIT_FORCE_RERUN.
#
# v2 (2026-07-18, post-inode-incident):
#   • LLM cache moved to NODE TMPFS: per-answer cache files on scratch blew
#     the project's 2.0M-inode quota and killed 4 jobs. Within-job dedup
#     (the valuable part) survives; cross-job persistence is deliberately
#     sacrificed — resume works at config granularity via the DONE guard.
#   • Inode-headroom preflight: refuse to start if scratch is near its file
#     quota, instead of dying 8 hours in.
#   • Per-job A7 smoke dir + smoke embed-cache (fixes the sqlite lock when
#     several jobs start simultaneously).
#
# v3 (2026-07-21, reader evidence window):
#   • --reader-token-budget 16000 (READER_TOKEN_BUDGET to override). The legacy
#     evidence path handed the reader 5 items × 300 chars ≈ 375 tokens — about
#     ONE retrieved item — so gold that retrieval had already found at rank 2-5
#     was cut before the model saw it (abstain ≈ 0.70 against hit@5 ≈ 0.86).
#     Now: one total budget, whole items in rank order. The budget rides in the
#     reader name (PlainTextReader_rtok16000), which is a path component and part
#     of the config hash, so budgeted rows can never be mixed with legacy ones.
#     D-6 (2026-07-22): 16000, not the first-cut 1.5k budget, whose measured
#     k-list evidence delivery was 0.4567 — session itemisers overflow it, so
#     gold was STILL being cut. 32k leaves no headroom against the confirmed
#     32768 serving window (T2 pins it), so 16000 is the largest safe value.
#   • --reader-prompt v2 (READER_PROMPT to override). The legacy v1 wrapper
#     forbids derivation ("using ONLY the memories") and caps answers at one
#     sentence, so answers the reader had the evidence for — a sum of two
#     figures, a difference of two dates, an ordered list — scored as misses.
#     v2 permits combining/computing and drops the cap, keeping the exact-form
#     IDK line. Also in the reader name (…_pv2).
#
# HOW TO SUBMIT (one dataset per job; calibrate with LIMIT=n if needed):
#   export SBATCH_ACCOUNT=project_<your-id>
#   cd <repo-root>
#   DATASETS="longmemeval" sbatch scripts/lumi/run_lumi_reader_eval.sh
#   DATASETS="beam"        sbatch scripts/lumi/run_lumi_reader_eval.sh
#   DATASETS="locomo"      sbatch scripts/lumi/run_lumi_reader_eval.sh
# LoCoMo is the HEAVY one here (1,986 queries/config × 1,083 configs).
# Chain BEAM continuations with: sbatch --dependency=afterany:<prev-jobid> ...
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
# T3d — SLURM_JOB_ID is unset off-cluster, and under `set -u` its FIRST use
# aborts the script. That made every guard below unreachable in a local dry
# run. Default it once, here, before anything interpolates it.
SLURM_JOB_ID="${SLURM_JOB_ID:-local-$$}"
CLIENT_SIF="${CLIENT_SIF:-$(dirname "$REPO")/forgetit.sif}"
VLLM_SIF=/appl/local/laifs/containers/lumi-multitorch-latest.sif

CHAT_MODEL_PATH=/scratch/$PROJ/models/Qwen2.5-7B-Instruct
EMBED_MODEL_PATH=/scratch/$PROJ/models/Qwen3-Embedding-0.6B

LME_DATASET=$REPO/data/longmemeval_s_cleaned.json
BEAM_DATASET=$REPO/data/beam_500K.jsonl
LOCOMO_DATASET=$REPO/data/locomo10.json

# FRESH tree — the state-forming grid in benchmark_full stays untouched.
# _v2: a SEPARATE tree from the v1 reader run in benchmark_reader. The v2 reader
# name (rtok16000_pv2_absexact_cleanfull) already separates the leaf dirs, so
# v1 rows were never at risk of being overwritten — but scripts/concat_results.py
# walks OUT_DIR recursively, so a shared root would merge BOTH generations into
# one combined_results.csv. Separate roots, separate merges, no mixed-vintage CSV.
OUT_DIR=/scratch/$PROJ/artifacts/benchmark_reader_v2
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

# ── inode-headroom preflight — refuse to start into a near-full quota ───────
# The 2026-07-18 incident: scratch hit its 2.0M-file quota mid-run and every
# writer died with Errno 122 hours into its allocation. Fail at minute zero
# instead.
#
# T3b — SELF-CONTAINED PARSE. The previous version read `awk 'NR==3{print $5,$6}'`,
# i.e. a fixed row at fixed field numbers. `lfs quota` ships in (at least) two
# column layouts and only ONE of them puts the inode counters at $5/$6:
#
#   LAYOUT A (7 fields):  fs kbytes quota limit files limit grace
#                         -> used=$5, limit=$6          (what the old awk assumed)
#   LAYOUT B (9 fields):  fs kbytes quota limit grace files quota limit grace
#                         -> used=$6, soft=$7, hard=$8  (standard Lustre)
#
# On LAYOUT B the old parse read $5 = the KBYTES GRACE column ("-"), failed its
# ^[0-9]+$ test, and took the "could not parse" branch — so on the standard
# layout this guard silently never fired at all. Now: find the data row by the
# MOUNT STRING, pick the columns from the row's field count, tolerate the '*'
# over-quota marker and '-' placeholders, and when the shape matches neither
# layout print the raw lfs output and WARN. A preflight that cannot parse must
# never refuse a valid run — it must never be silent either.
# >>> T3b-BLOCK-START (extracted verbatim by verification/t3_launcher_guards.sh)
INODE_FLOOR=100000          # refuse below this many free inodes (not overridable)

_inode_num () {             # echo an lfs numeric field, or nothing if it isn't one
    local v="${1:-}"
    v="${v%\*}"             # drop the over-quota marker: "1950000*" -> "1950000"
    if [[ "$v" =~ ^[0-9]+$ ]]; then printf '%s' "$v"; fi
}

_inode_raw_dump () {        # never warn about output without showing it
    printf '[lumi]   raw lfs output >>>\n' >&2
    if [ -n "${1:-}" ]; then printf '%s\n' "$1" >&2; else printf '(no output)\n' >&2; fi
    printf '[lumi]   <<< end raw lfs output\n' >&2
}

inode_preflight () {        # $1 raw lfs text, $2 mount string, $3 label for messages
    local raw="$1" mount="$2" label="$3"
    local row nf used lim used_n lim_n free
    # Data row = the one whose FIRST field starts with the mount we asked about.
    # ("Disk quotas for prj ..." and the "Filesystem ..." header never match.)
    row="$(printf '%s\n' "$raw" | awk -v m="$mount" 'index($1, m) == 1 { print; exit }')"
    if [ -z "$row" ]; then
        echo "[lumi] WARNING: no '$mount' row in lfs quota output — inode preflight SKIPPED" >&2
        _inode_raw_dump "$raw"
        return 0
    fi
    # shellcheck disable=SC2086  # deliberate word-splitting: $row is lfs columns
    set -- $row
    nf=$#
    case "$nf" in
        7) used="$5"; lim="$6" ;;                            # LAYOUT A
        9) used="$6"; lim="$8"                               # LAYOUT B: hard limit,
           [ "$(_inode_num "$8")" = "0" ] && lim="$7" ;;      #   soft limit if hard is 0
        *) echo "[lumi] WARNING: unrecognised lfs quota layout ($nf fields on the '$mount' row;" >&2
           echo "[lumi]          expected 7 or 9) — inode preflight SKIPPED, NOT refusing" >&2
           _inode_raw_dump "$raw"
           return 0 ;;
    esac
    used_n="$(_inode_num "$used")"
    lim_n="$(_inode_num "$lim")"
    if [ -z "$used_n" ] || [ -z "$lim_n" ]; then
        echo "[lumi] WARNING: non-numeric inode columns (used='$used' limit='$lim') in a" >&2
        echo "[lumi]          ${nf}-field row — inode preflight SKIPPED, NOT refusing" >&2
        _inode_raw_dump "$raw"
        return 0
    fi
    if [ "$lim_n" -eq 0 ]; then
        echo "[lumi] WARNING: no inode limit set on $label (limit=0, used=$used_n) —" >&2
        echo "[lumi]          headroom is unknowable, inode preflight SKIPPED" >&2
        return 0
    fi
    free=$((lim_n - used_n))
    echo "[lumi] scratch inodes: $used_n/$lim_n ($free free)  [lfs layout: ${nf} fields]"
    if [ "$free" -lt "$INODE_FLOOR" ]; then
        echo "[lumi] REFUSING: <100k inodes free on $label — purge before submitting" >&2
        exit 1
    fi
}

# The stat-failure warning lives OUTSIDE the gid guard: when stat fails (dir
# absent, wrong account, non-GNU stat) the old block produced zero bytes on
# stdout AND stderr, so the preflight the header advertises FIRST vanished
# without a trace. It must announce that it could not run.
SCRATCH_GID="$(stat -c %g "/scratch/$PROJ" 2>/dev/null || true)"
if [ -z "$SCRATCH_GID" ]; then
    echo "[lumi] WARNING: stat -c %g /scratch/$PROJ failed (missing dir, wrong account," >&2
    echo "[lumi]          or non-GNU stat) — inode preflight SKIPPED" >&2
else
    inode_preflight "$(lfs quota -p "$SCRATCH_GID" /scratch 2>/dev/null || true)" \
                    "/scratch" "/scratch/$PROJ"
fi
# >>> T3b-BLOCK-END

# ── recorded-run guard — refuse a dirty tree ────────────────────────────────
# T3c: UNTRACKED files count as dirty too. _git_commit() stamps rows from
# `git status --porcelain`, which reports untracked files — so a tree that is
# clean by `diff` but carries new files stamps every row "<sha>-dirty" while
# this guard waves the job through. The two must agree.
if [ -z "${FORGETIT_ALLOW_DIRTY:-}" ]; then
    if ! git -C "$REPO" diff --quiet 2>/dev/null || \
       ! git -C "$REPO" diff --cached --quiet 2>/dev/null; then
        echo "[lumi] REFUSING: uncommitted changes in $REPO — commit+tag, or FORGETIT_ALLOW_DIRTY=1 for smoke" >&2
        exit 1
    fi
    UNTRACKED="$(git -C "$REPO" ls-files --others --exclude-standard 2>/dev/null || true)"
    if [ -n "$UNTRACKED" ]; then
        echo "[lumi] REFUSING: untracked files in $REPO — commit+tag, or FORGETIT_ALLOW_DIRTY=1 for smoke" >&2
        printf '%s\n' "$UNTRACKED" | sed 's/^/[lumi]     /' >&2
        exit 1
    fi
fi
if git -C "$REPO" ls-files --error-unmatch .env >/dev/null 2>&1; then
    echo "[lumi] WARNING: $REPO/.env is tracked by git — this job rewrites it" >&2
fi

# ── git provenance stamp — resolved HERE, OUTSIDE the container (T4) ────────
# The client image need not carry a git binary and the bind mount need not
# include .git, so in-container resolution wrote git_commit="unknown" for whole
# runs — silently. Resolve once out here, where both are known good, and hand
# the value to every client process; the Python site prefers it and falls back
# to its own resolution only when it is absent.
GIT_COMMIT="$(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || true)"
if [ -n "$GIT_COMMIT" ] && [ -n "$(git -C "$REPO" status --porcelain 2>/dev/null)" ]; then
    GIT_COMMIT="${GIT_COMMIT}-dirty"     # only reachable under FORGETIT_ALLOW_DIRTY=1
fi
export GIT_COMMIT
if [ -z "$GIT_COMMIT" ]; then
    echo "[lumi] WARNING: no git commit resolvable in $REPO — rows fall back to" >&2
    echo "[lumi]          in-container resolution and may be stamped 'unknown'" >&2
else
    echo "[lumi] git_commit for this run: $GIT_COMMIT"
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

# ── reader evidence window ──────────────────────────────────────────────────
# ONE total budget, filled whole-item in retrieval-rank order. Without it the
# reader runs the legacy context (5 items × 300 chars ≈ 375 tokens ≈ ONE item),
# which is what produced abstain ≈ 0.70 against hit@5 ≈ 0.86: gold was
# retrieved, then cut before the reader saw it. The budget is in the reader
# name (PlainTextReader_rtok16000), so this tree never mixes with legacy rows.
# D-6: 16000 is the SIGNED value. The first-cut 1.5k budget measured k-list
# evidence delivery at 0.4567 — better than legacy, still cutting gold on
# session-itemised rows. 32k is refused: the chat serving window is pinned at
# 32768, leaving no room for the instruction wrapper + generation. Check
# evidence_delivery_rate, not just abstain, when reading the results.
READER_TOKEN_BUDGET="${READER_TOKEN_BUDGET:-16000}"

# ── reader instruction wrapper (D4 fix iv) ──────────────────────────────────
# v1 (legacy) says "using ONLY the memories … one sentence maximum": it forbids
# DERIVING an answer the evidence supports (summing two figures, differencing
# two dates) and truncates ordered lists, so those score as misses even when
# retrieval delivered every needed item. v2 permits combining/computing and
# drops the cap, keeping the exact-form IDK line abstention detection matches.
# Rides in the reader name (…_pv2), so v1 and v2 rows never mix.
READER_PROMPT="${READER_PROMPT:-v2}"

# ── reader answer cleaning (D4 fix ii) ──────────────────────────────────────
# firstline (legacy) keeps only the first line after noise-stripping, so every
# multi-line answer is decapitated — an ordered list scores as its first item.
# full keeps the whole stripped answer (same fence/asterisk/quote/preamble
# removal). Pairs with prompt v2, which is what stops forbidding lists in the
# first place. Rides in the reader name (…_cleanfull).
READER_CLEAN="${READER_CLEAN:-full}"

# ── reader abstention detection (D4 fix vii, detection half) ────────────────
# loose (legacy) flags an answer as a refusal if a refusal phrase appears
# ANYWHERE in it, so "the rent was 2,100, IDK if there were others" is recorded
# as an abstention and scores as no answer. exact requires the WHOLE answer to
# be a refusal — sound because both prompts instruct an exact-form IDK. This
# matters more with READER_CLEAN=full, which gives loose more text to match in.
# Rides in the reader name (…_absexact).
READER_ABSTENTION="${READER_ABSTENTION:-exact}"

# ── answer generation cap (D4 fix vii, cap half) ────────────────────────────
# The legacy 128 cut ~1 in 3 answered generations off mid-sentence in the probe
# (10 of 32), worst on multi-item answers. 256 is the validated value. NOT in
# the reader name — it is a config-hash field, so raising it re-runs affected
# configs on its own. Cap-HITS are not recorded per row (the LLM layer discards
# finish_reason), so this is set-high-enough, not audit-after.
ANSWER_MAX_TOKENS="${ANSWER_MAX_TOKENS:-256}"

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

# ── embedding cache — scratch, persistent, per-shard sqlite (67 inodes total;
# this cache is the model citizen and stays exactly where it is) ─────────────
EMB_CACHE_DIR=/scratch/$PROJ/emb_cache
mkdir -p "$EMB_CACHE_DIR"

# ── LLM cache — NODE-LOCAL TMPFS, NOT scratch (v2 change) ───────────────────
# Scratch has a 2.0M-inode project quota; per-answer cache files blew it on
# 2026-07-18 and killed 4 jobs mid-run. Node tmpfs (/tmp, RAM-backed, ~353G
# shared with /dev/shm) has no such quota. Within-job dedup survives —
# identical contexts across policies at budget_100pct still collapse to one
# generation. The cache dies with the job, by design: resume is the DONE
# guard's job, not the cache's.
LLM_CACHE_DIR=/tmp/llm_cache_${SLURM_JOB_ID}
mkdir -p "$LLM_CACHE_DIR"

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
        # T3a: without this arm a typo ("longmemval") matched nothing, passed
        # preflight, booted 8 servers, and only then fell through the dispatch
        # case at step 3 with rc=1 — an hour of allocation for a typo.
        *)           echo "[lumi] REFUSING: unknown dataset '$ds' — expected one of: longmemeval beam locomo" >&2; exit 1 ;;
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
    # --max-model-len 32768: PIN the serving window (T2). The reader's evidence
    # budget (16000) is sized against this number; leaving the window implicit
    # means a container or config change silently moves the ceiling the budget
    # was chosen for, and long prompts start 400-ing mid-grid.
    singularity exec -B "/scratch/$PROJ" "$VLLM_SIF" \
        bash -c "ROCR_VISIBLE_DEVICES=$g vllm serve '$CHAT_MODEL_PATH' \
                 --served-model-name '$SERVED_CHAT' --port $cport --tensor-parallel-size 1 \
                 --max-model-len 32768" \
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
cat > "$OUT_DIR/ENCODER_PIN.txt" <<EOF
model_path:            $EMBED_MODEL_PATH
model_config_sha256:   $(sha256sum "$EMBED_MODEL_PATH/config.json" | cut -d' ' -f1)
serving:               vLLM (container: $VLLM_SIF) $EMBED_SERVE_FLAG --max-model-len 32768
served_name:           $SERVED_EMBED
EMBED_MAX_INPUT_CHARS: 28000
recorded:              $(date -u +%Y-%m-%dT%H:%M:%SZ) job ${SLURM_JOB_ID}
EOF
# T5: ENCODER_PIN.txt is OVERWRITTEN by every job that writes into this tree,
# so the pin describing the rows already in it is destroyed by the next job
# that appends to them. Keep a per-job copy as well; the unsuffixed file stays
# in place as the "latest" pointer everything downstream already reads.
cp "$OUT_DIR/ENCODER_PIN.txt" "$OUT_DIR/ENCODER_PIN_${SLURM_JOB_ID}.txt"
echo "[lumi] encoder pin written to $OUT_DIR/ENCODER_PIN.txt (+ ENCODER_PIN_${SLURM_JOB_ID}.txt)"

if [ -z "${SKIP_A7_SMOKE:-}" ]; then
    echo "[lumi] A7 smoke — READER ON (semantic policies, 1 example) ..."
    singularity exec -B "/scratch/$PROJ,$REPO" "$CLIENT_SIF" \
        bash -c "cd '$REPO' && PYTHONPATH='$REPO/src' \
                 LLM_BASE_URL='http://localhost:${CHAT_PORT_BASE}/v1' LLM_API_KEY=sk-lumi LLM_MODEL='$SERVED_CHAT' \
                 EMBED_BASE_URL='http://localhost:${EMBED_PORT}/v1' EMBED_API_KEY=sk-lumi EMBED_MODEL='$SERVED_EMBED' \
                 EXPECTED_EMBED_MODEL='$SERVED_EMBED' \
                 EMBED_MAX_INPUT_CHARS=28000 EMBED_CACHE_PATH='$EMB_CACHE_DIR/a7_smoke_reader_${SLURM_JOB_ID}.sqlite' \
                 LLM_CACHE_DIR='$LLM_CACHE_DIR' \
                 GIT_COMMIT='$GIT_COMMIT' \
                 python -m forgetit.experiments.run_longmemeval_qa \
                   --policies semantic semantic_keep --limit 1 \
                   --reader-token-budget $READER_TOKEN_BUDGET \
                   --reader-prompt $READER_PROMPT \
                   --reader-clean $READER_CLEAN \
                   --reader-abstention $READER_ABSTENTION \
                   --answer-max-tokens $ANSWER_MAX_TOKENS \
                   --shard 0 --num-shards 1 --out-dir '$A7_SMOKE_DIR' --dense" \
        > "$LOGS/a7-smoke-reader-${SLURM_JOB_ID}.log" 2>&1 \
        || { echo "[lumi] REFUSING: reader-on A7 smoke failed — see $LOGS/a7-smoke-reader-${SLURM_JOB_ID}.log" >&2; exit 1; }
    echo "[lumi] A7 smoke passed — answer path is live on the wired encoder."
fi

# ── 3. run each dataset's sweep, sharded, READER ON ─────────────────────────
run_dataset () {
    local dataset="$1" module="$2" workers="$3"
    echo "[lumi] ===== $dataset: $NUM_SHARDS shards (workers=$workers, reader-workers=$READER_WORKERS, reader=ON, reader-token-budget=$READER_TOKEN_BUDGET, reader-prompt=$READER_PROMPT, reader-clean=$READER_CLEAN, reader-abstention=$READER_ABSTENTION, answer-max-tokens=$ANSWER_MAX_TOKENS) ====="
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
                     GIT_COMMIT='$GIT_COMMIT' \
                     LLM_MAX_CONNECTIONS=$POOL EMBED_MAX_CONNECTIONS=$POOL \
                     python -m $module \
                       --shard $i --num-shards $NUM_SHARDS --out-dir '$OUT_DIR' \
                       --policies $POLICIES \
                       --budgets $BUDGETS \
                       --seeds $SEEDS \
                       --include-floor \
                       --dense --workers $workers \
                       --reader-workers $READER_WORKERS \
                       --reader-token-budget $READER_TOKEN_BUDGET \
                       --reader-prompt $READER_PROMPT \
                       --reader-clean $READER_CLEAN \
                       --reader-abstention $READER_ABSTENTION \
                       --answer-max-tokens $ANSWER_MAX_TOKENS $LIMIT_FLAG" \
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

# ── 4. merge CSVs over the READER tree — only if every shard succeeded ──────
# (A failed shard means an incomplete grid; a merged CSV over it looks
# success-shaped and misleads. The next successful run redoes the merge.)
if [ "$rc" -eq 0 ]; then
    echo "[lumi] merging CSVs ..."
    singularity exec -B "/scratch/$PROJ,$REPO" "$CLIENT_SIF" \
        bash -c "cd '$REPO' && PYTHONPATH='$REPO/src' python scripts/concat_results.py '$OUT_DIR'" || rc=1
else
    echo "[lumi] SKIPPING merge: one or more shards failed (rc=$rc) — resubmit to resume, merge runs on the clean pass"
fi

# NOTE: no eviction-regret step — events and the regret report belong to the
# state-forming tree (benchmark_full); this tree carries the answers.

echo "[lumi] done (exit $rc)."
echo "[lumi]   CSVs:        $OUT_DIR/combined_results.csv + combined_summary.csv"
echo "[lumi]   encoder pin: $OUT_DIR/ENCODER_PIN.txt"
exit $rc
