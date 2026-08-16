#!/bin/bash
# Preservation pass over the campaign tree. CPU only: no model, no GPU.
#
# Why a job rather than the login node: every gold_answer_snapshot.jsonl row
# carries original_text AND resident_text in full, so the tree is roughly 25 GB
# of JSON across 2,888 files. IO plus json parsing, not compute.
#
#   sbatch scripts/lumi/run_preservation.sh
#   RUN_ROOT=$SUB sbatch scripts/lumi/run_preservation.sh     # subset instead
#
#SBATCH --job-name=fi-preserve
#SBATCH --partition=small
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=64G
#SBATCH --time=02:00:00
#SBATCH --output=logs/fi-preserve-%j.out

set -euo pipefail

PROJ="${SLURM_JOB_ACCOUNT:?submit with SBATCH_ACCOUNT exported}"
REPO="${SLURM_SUBMIT_DIR:?}"
CLIENT_SIF="${CLIENT_SIF:-$(dirname "$REPO")/forgetit.sif}"
RUN_ROOT="${RUN_ROOT:-/scratch/$PROJ/artifacts/benchmark_full}"
OUT="${OUT:-$REPO/report}"

CONTAINER_RT="$(command -v singularity || command -v apptainer)"
mkdir -p logs "$OUT"
cd "$REPO"

echo "[preserve] root : $RUN_ROOT"
echo "[preserve] out  : $OUT"
echo "[preserve] cpus : ${SLURM_CPUS_PER_TASK:-64}"

"$CONTAINER_RT" exec -B "/scratch/$PROJ,$REPO" "$CLIENT_SIF" \
    bash -c "cd '$REPO' && PYTHONPATH='$REPO/src' \
             python scripts/gold_preservation.py '$RUN_ROOT' \
                 --out '$OUT' --workers ${SLURM_CPUS_PER_TASK:-64}"

echo "[preserve] done -> $OUT"
