# Session environment for the forget-it LUMI work.
#   source scripts/lumi/env.sh
#
# Every failure in this project has been an unset or placeholder variable
# rather than a wrong path, and the shell substitutes empty without warning.
# This file is the fix.
#
# Every value below is a DEFAULT, never an override. These are sourced from
# inside launchers that have already read their own environment, so a bare
# `export X=...` silently discards what the caller asked for: that is how a
# classifier run pointed at sweep_subset_lexical read sweep_subset instead and
# produced 45 configs at three budgets without saying so.
#
# Nothing machine- or project-specific lives here. The repo is found from this
# file's own location, the client image is expected beside the repo (the same
# rule as run_lumi_full_eval.sh and run_lumi_reader_sweep.sh), and the project
# comes from the Slurm account you submit under.

_ENV_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export REPO="${REPO:-$(cd "$_ENV_DIR/../.." && pwd)}"
unset _ENV_DIR

export PROJ="${PROJ:-${SLURM_JOB_ACCOUNT:-${SBATCH_ACCOUNT:-}}}"
if [ -z "$PROJ" ]; then
    echo "[env] WARNING: no project. export SBATCH_ACCOUNT=project_<id> before sourcing." >&2
fi
export SBATCH_ACCOUNT="${SBATCH_ACCOUNT:-$PROJ}"

export CLIENT_SIF="${CLIENT_SIF:-$(dirname "$REPO")/forgetit.sif}"
export RETRIEVAL_ROOT="${RETRIEVAL_ROOT:-/scratch/$PROJ/artifacts/benchmark_full}"
export SUBSET_ROOT="${SUBSET_ROOT:-/scratch/$PROJ/artifacts/sweep_subset}"
export SUB="${SUB:-$SUBSET_ROOT/longmemeval_turn_pair}"
export MODELS_DIR="${MODELS_DIR:-/scratch/$PROJ/models}"
export HF_HOME="${HF_HOME:-/scratch/$PROJ/hf_cache}"
export LAIF_RELEASE="${LAIF_RELEASE:-lumi-multitorch-u24r70f21m50t210-20260807_115122}"
export VLLM_SIF="${VLLM_SIF:-/appl/local/laifs/containers/$LAIF_RELEASE/lumi-multitorch-full-${LAIF_RELEASE#lumi-multitorch-}.sif}"

echo "[env] REPO=$REPO"
echo "[env] PROJ=${PROJ:-<unset>}"
echo "[env] RETRIEVAL_ROOT=$RETRIEVAL_ROOT  ($([ -d "$RETRIEVAL_ROOT" ] && echo ok || echo MISSING))"
echo "[env] SUB=$SUB  ($(ls "$SUB" 2>/dev/null | wc -l) configs)"
echo "[env] VLLM_SIF  ($([ -e "$VLLM_SIF" ] && echo ok || echo MISSING))"
echo "[env] CLIENT_SIF=$CLIENT_SIF ($([ -e "$CLIENT_SIF" ] && echo ok || echo MISSING))"