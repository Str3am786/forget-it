#!/bin/bash
# Build the curated config subsets the reader sweep and the preservation pass
# both run over.
#
# WHY A SUBSET AT ALL. The campaign holds 2,888 config dirs. reader_probe.py
# flattens questions across every config it discovers, so pointing at the root
# is not a replay, it is a second campaign. The subset is a reduction of
# REPLICATION, never of the ranking axis.
#
# WHAT IS SWEPT
#   retrievers     3   lexical, bm25, dense. Each gets its OWN subset tree
#                      (--retriever, SUBSET_ROOT suffixed by the name), so the
#                      three are built and read side by side rather than one
#                      being fixed and declared.
#   budgets        5   010/025/050/075/100 (--budgets, bare percents). The upper
#                      two sit near the no-eviction control where policies
#                      compress together; they are swept anyway, because "the
#                      ranking degenerates as budget grows" is a result and not
#                      a reason to stop measuring.
#   policies      15   NOT CUT. Kendall's tau ranks policies; cutting them would
#                      be cutting the measurement.
#
# WHAT IS STILL REDUCED, AND WHY IT IS SAFE
#   random seeds  10 -> 1   random_s42..s51 are seed replicates of ONE policy.
#                           A ranking needs random's position, not its variance.
#                           The seed spread is measured separately, so it is
#                           reported rather than folded into this grid.
#
# WHY SYMLINKS. The subset must be a view, not a copy: the tree is ~25 GB and a
# copy would drift from the campaign. Note that Path.rglob does NOT follow
# symlinks, which is why both reader_probe.py and gold_preservation.py use
# os.walk(followlinks=True). Do not "fix" that back.
#
#   source scripts/lumi/env.sh
#   bash scripts/lumi/build_subsets.sh                                  # build
#   bash scripts/lumi/build_subsets.sh --check                          # verify
#   bash scripts/lumi/build_subsets.sh --retriever bm25 \
#        --budgets "010 025 050 075 100"                                # sweep
#
# Each build stamps SUBSET.json with the retriever, the budgets and the full
# git commit, so a subset tree can always say what produced it.
# ============================================================================
set -euo pipefail

: "${PROJ:?source scripts/lumi/env.sh first}"
: "${RETRIEVAL_ROOT:?source scripts/lumi/env.sh first}"

SUBSET_ROOT_BASE="${SUBSET_ROOT:-/scratch/$PROJ/artifacts/sweep_subset}"
RETRIEVER="${RETRIEVER:-lexical}"
BUDGETS="${BUDGETS:-budget_010pct budget_025pct budget_050pct budget_075pct budget_100pct}"

# 24 policy dirs minus nine surplus random seeds. random_s42 is kept as THE
# random baseline; the other nine are replicates of it.
POLICIES="${POLICIES:-compression ebbinghaus evict_smallest fifo gdsf gdsf_frozen \
keep_smallest lfu lru oracle random_s42 salience_decay semantic \
semantic_coverage semantic_keep}"

# workload/itemiser pairs. beam names its itemisers with a beam_ prefix, so the
# triple cannot be derived from the workload name alone.
PAIRS="${PAIRS:-longmemeval/turn_pair beam/beam_turn_pair locomo/turn_pair}"

CHECK=0
# Flags beat env vars; env vars still work when no flag is given, so existing
# invocations keep their meaning. Budgets are given as bare percents (010 025)
# and expanded to the budget_NNNpct directory names the campaign wrote.
while [ $# -gt 0 ]; do
    case "$1" in
        --check)     CHECK=1 ;;
        --retriever) shift; RETRIEVER="${1:?--retriever needs a value}" ;;
        --budgets)   shift; BUDGETS="$(for b in ${1:?--budgets needs a value}; do
                            case "$b" in budget_*) printf '%s ' "$b" ;;
                                         *) printf 'budget_%spct ' "$b" ;; esac
                        done)" ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
    shift
done
case "$RETRIEVER" in
    lexical|bm25|dense) ;;
    *) echo "--retriever must be lexical, bm25 or dense (got '$RETRIEVER')" >&2
       exit 2 ;;
esac

n_pol=$(echo $POLICIES | wc -w)
# One subset tree per retriever: the retriever is part of what the subset IS,
# and a shared path let a second build overwrite the first in place.
SUBSET_ROOT="${SUBSET_ROOT_BASE}_${RETRIEVER}"
BS_REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GIT_COMMIT="$(git -C "$BS_REPO" rev-parse HEAD 2>/dev/null || true)"
export GIT_COMMIT
[ -n "$GIT_COMMIT" ] || echo "WARNING: no git commit resolvable — SUBSET.json will say 'unknown'" >&2

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
  "git_commit": "${GIT_COMMIT:-unknown}",
  "budgets": [$(printf '"%s",' $BUDGETS | sed 's/,$//')],
  "policies": [$(printf '"%s",' $POLICIES | sed 's/,$//')],
  "pairs": [$(printf '"%s",' $PAIRS | sed 's/,$//')],
  "n_configs": $total_ok,
  "rationale": "replication cut (seeds, retrievers, slack budgets); policy axis intact"
}
EOF
    echo "wrote $SUBSET_ROOT/SUBSET.json"
fi
