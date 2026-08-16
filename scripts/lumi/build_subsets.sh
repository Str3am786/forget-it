#!/bin/bash
# Build the curated config subsets the reader sweep and the preservation pass
# both run over.
#
# WHY A SUBSET AT ALL. The campaign holds 2,888 config dirs. reader_probe.py
# flattens questions across every config it discovers, so pointing at the root
# is not a replay, it is a second campaign. The subset is a reduction of
# REPLICATION, never of the ranking axis.
#
# WHAT IS CUT, AND WHY EACH IS SAFE
#   random seeds  10 -> 1   random_s42..s51 are seed replicates of ONE policy.
#                           A ranking needs random's position, not its variance,
#                           and the variance is already established elsewhere.
#   retrievers    n  -> 1   Retriever invariance is a different question. Fixed
#                           and declared, not swept.
#   budgets       5  -> 3   010/025/050. 075 and 100 sit near the no eviction
#                           control where policies compress together, which is
#                           where a ranking has least to say.
#   policies      15 -> 15  NOT CUT. Kendall's tau ranks policies; cutting them
#                           would be cutting the measurement.
#
# WHY SYMLINKS. The subset must be a view, not a copy: the tree is ~25 GB and a
# copy would drift from the campaign. Note that Path.rglob does NOT follow
# symlinks, which is why both reader_probe.py and gold_preservation.py use
# os.walk(followlinks=True). Do not "fix" that back.
#
#   source scripts/lumi/env.sh
#   bash scripts/lumi/build_subsets.sh              # build
#   bash scripts/lumi/build_subsets.sh --check      # verify only
# ============================================================================
set -euo pipefail

: "${PROJ:?source scripts/lumi/env.sh first}"
: "${RETRIEVAL_ROOT:?source scripts/lumi/env.sh first}"

SUBSET_ROOT="${SUBSET_ROOT:-/scratch/$PROJ/artifacts/sweep_subset}"
RETRIEVER="${RETRIEVER:-lexical}"
BUDGETS="${BUDGETS:-budget_010pct budget_025pct budget_050pct}"

# 24 policy dirs minus nine surplus random seeds. random_s42 is kept as THE
# random baseline; the other nine are replicates of it.
POLICIES="${POLICIES:-compression ebbinghaus evict_smallest fifo gdsf gdsf_frozen \
keep_smallest lfu lru oracle random_s42 salience_decay semantic \
semantic_coverage semantic_keep}"

# workload/itemiser pairs. beam names its itemisers with a beam_ prefix, so the
# triple cannot be derived from the workload name alone.
PAIRS="${PAIRS:-longmemeval/turn_pair beam/beam_turn_pair locomo/turn_pair}"

CHECK=0
[ "${1:-}" = "--check" ] && CHECK=1

n_pol=$(echo $POLICIES | wc -w)
n_bud=$(echo $BUDGETS | wc -w)
n_pair=$(echo $PAIRS | wc -w)
echo "expecting $n_pol policies x $n_bud budgets = $((n_pol*n_bud)) configs per subset,"
echo "across $n_pair workload/itemiser pairs, retriever=$RETRIEVER"
echo

total_ok=0; total_missing=0
for pair in $PAIRS; do
    workload="${pair%%/*}"; itemiser="${pair##*/}"
    name="${workload}_${itemiser}"
    sub="$SUBSET_ROOT/$name"
    src_root="$RETRIEVAL_ROOT/$workload/$itemiser/retention"

    if [ ! -d "$src_root" ]; then
        echo "SKIP $name — no such tree: $src_root"
        echo "     check the itemiser name; beam prefixes its itemisers."
        continue
    fi

    [ "$CHECK" = 1 ] || { rm -rf "$sub"; mkdir -p "$sub"; }

    ok=0; missing=()
    for pol in $POLICIES; do
        for b in $BUDGETS; do
            src="$src_root/$pol/$RETRIEVER/$b/none"
            if [ -d "$src" ]; then
                # Both files are required: reader_probe needs results.csv plus
                # the gold snapshot, and a config with only one of them would
                # be discovered by one script and not the other, silently
                # misaligning the x and y of the scatter.
                if [ -f "$src/results.csv" ] && [ -f "$src/gold_answer_snapshot.jsonl" ]; then
                    [ "$CHECK" = 1 ] || ln -sfn "$src" "$sub/${pol}__${b}"
                    ok=$((ok+1))
                else
                    missing+=("$pol/$b (incomplete: missing results.csv or snapshot)")
                fi
            else
                missing+=("$pol/$b")
            fi
        done
    done

    printf "%-28s %3d/%3d configs -> %s\n" "$name" "$ok" "$((n_pol*n_bud))" "$sub"
    for m in "${missing[@]:-}"; do [ -n "$m" ] && echo "      missing: $m"; done
    total_ok=$((total_ok+ok)); total_missing=$((total_missing+${#missing[@]}))
done

echo
echo "total: $total_ok configs linked, $total_missing missing"
[ "$CHECK" = 1 ] && echo "(check only, nothing written)"

# The subset definition is provenance: it records exactly which configs the
# reader sweep and the preservation join covered. Written next to the links so
# it travels with them.
if [ "$CHECK" = 0 ]; then
    cat > "$SUBSET_ROOT/SUBSET.json" <<EOF
{
  "built": "$(date -Iseconds)",
  "source": "$RETRIEVAL_ROOT",
  "retriever": "$RETRIEVER",
  "budgets": [$(printf '"%s",' $BUDGETS | sed 's/,$//')],
  "policies": [$(printf '"%s",' $POLICIES | sed 's/,$//')],
  "pairs": [$(printf '"%s",' $PAIRS | sed 's/,$//')],
  "n_configs": $total_ok,
  "rationale": "replication cut (seeds, retrievers, slack budgets); policy axis intact"
}
EOF
    echo "wrote $SUBSET_ROOT/SUBSET.json"
fi
