# Session environment for the forget-it LUMI work.
#   source scripts/lumi/env.sh
#
# Every failure in this project over the past week has been an unset or
# placeholder variable rather than a wrong path, and the shell substitutes
# empty without warning. This file is the fix.

# Every value below is a DEFAULT, never an override. These are sourced from
# inside launchers that have already read their own environment, so a bare
# `export X=...` silently discards what the caller asked for: that is how a
# classifier run pointed at sweep_subset_lexical read sweep_subset instead and
# produced 45 configs at three budgets without saying so.
export PROJ="${PROJ:-project_462001123}"
export REPO="${REPO:-$HOME/15-08-forgetit/forget-it}"
export CLIENT_SIF="${CLIENT_SIF:-$HOME/15-08-forgetit/forgetit.sif}"
export RETRIEVAL_ROOT="${RETRIEVAL_ROOT:-/scratch/$PROJ/artifacts/benchmark_full}"
export SUBSET_ROOT="${SUBSET_ROOT:-/scratch/$PROJ/artifacts/sweep_subset}"
export SUB="${SUB:-$SUBSET_ROOT/longmemeval_turn_pair}"
export MODELS_DIR="${MODELS_DIR:-/scratch/$PROJ/models}"
export HF_HOME="${HF_HOME:-/scratch/$PROJ/hf_cache}"
export LAIF_RELEASE="${LAIF_RELEASE:-lumi-multitorch-u24r70f21m50t210-20260807_115122}"
export VLLM_SIF="${VLLM_SIF:-/appl/local/laifs/containers/$LAIF_RELEASE/lumi-multitorch-full-${LAIF_RELEASE#lumi-multitorch-}.sif}"
export SBATCH_ACCOUNT="${SBATCH_ACCOUNT:-$PROJ}"

echo "[env] PROJ=$PROJ"
echo "[env] RETRIEVAL_ROOT=$RETRIEVAL_ROOT  ($([ -d "$RETRIEVAL_ROOT" ] && echo ok || echo MISSING))"
echo "[env] SUB=$SUB  ($(ls "$SUB" 2>/dev/null | wc -l) configs)"
echo "[env] VLLM_SIF  ($([ -e "$VLLM_SIF" ] && echo ok || echo MISSING))"
echo "[env] CLIENT_SIF ($([ -e "$CLIENT_SIF" ] && echo ok || echo MISSING))"
