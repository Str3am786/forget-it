# Session environment for the forget-it LUMI work.
#   source scripts/lumi/env.sh
#
# Every failure in this project over the past week has been an unset or
# placeholder variable rather than a wrong path, and the shell substitutes
# empty without warning. This file is the fix.

export PROJ=project_462001123
export REPO="$HOME/15-08-forgetit/forget-it"
export CLIENT_SIF="$HOME/15-08-forgetit/forgetit.sif"

# The campaign output: 2,888 config dirs, 16 evidence shards, gold snapshots.
export RETRIEVAL_ROOT="/scratch/$PROJ/artifacts/benchmark_full"

# Curated 30 config subset: 15 policies x {010pct, 050pct}, lexical, longmemeval/turn_pair.
export SUBSET_ROOT="/scratch/$PROJ/artifacts/sweep_subset"
export SUB="$SUBSET_ROOT/longmemeval_turn_pair"

export MODELS_DIR="/scratch/$PROJ/models"
export HF_HOME="/scratch/$PROJ/hf_cache"

# Serving stack, PINNED. lumi-multitorch-latest.sif is a symlink into a
# rotating dated directory; it resolves here today, which is luck rather than
# provenance. vLLM 0.22.1, transformers 5.10.1, Gemma3 and Qwen2 registered.
export LAIF_RELEASE=lumi-multitorch-u24r70f21m50t210-20260807_115122
export VLLM_SIF="/appl/local/laifs/containers/$LAIF_RELEASE/lumi-multitorch-full-${LAIF_RELEASE#lumi-multitorch-}.sif"

export SBATCH_ACCOUNT="$PROJ"

echo "[env] PROJ=$PROJ"
echo "[env] RETRIEVAL_ROOT=$RETRIEVAL_ROOT  ($([ -d "$RETRIEVAL_ROOT" ] && echo ok || echo MISSING))"
echo "[env] SUB=$SUB  ($(ls "$SUB" 2>/dev/null | wc -l) configs)"
echo "[env] VLLM_SIF  ($([ -e "$VLLM_SIF" ] && echo ok || echo MISSING))"
echo "[env] CLIENT_SIF ($([ -e "$CLIENT_SIF" ] && echo ok || echo MISSING))"
