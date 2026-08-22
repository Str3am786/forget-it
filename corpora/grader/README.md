# Grader calibration corpus

Two complementary halves of one calibration set. Both are needed: neither
alone lets you ask whether a new grader beats the deterministic one.

    corpus_annotated_130.csv   the DETERMINISTIC verdict: tier, similarity,
                               regraded_correct, qa_correct. No human labels.
    annotate.csv               the HUMAN labels: human_correct, human_note,
                               plus question_text. All 130 rows filled.
    annotate_template.csv      the blank form annotate.csv was made from.
    corpus_draw2_unlabelled.csv  a LATER, SEPARATE draw. Not annotated. Its
                               row_key holds reader answer text rather than a
                               hash, so it does not join to anything. Shares
                               only 48 query_ids with the annotated draw.
    RULE.txt                   the labelling rule the human labels were made under.
    manifest.json              sampling parameters for the draw.

## Provenance, verified rather than assumed

corpus_annotated_130.csv joins annotate.csv on row_key for 121 of 130 rows.
On those 121, query_id / reader_answer / dataset / question_type are identical
in 121/121 and ref_answer in 120/121 (one NaN-vs-empty artefact). That content
match is what establishes these as the same sample; the filenames do not.

## Sampling caveat, load bearing

manifest.json records sim_threshold 0.83 with boundary 0.06, so the draw is
ENRICHED at the embedding tier's decision boundary. Error rates measured here
are boundary-case rates and must not be quoted as population rates.

## What the deterministic grader scores on it (n=120 gradeable)

    exact       n=12   agreement 1.000
    abstain     n=17   agreement 1.000
    contains    n=16   agreement 0.812    3 false accepts (abstention leakage)
    embedding   n=12   agreement 0.583    5 false accepts (near-numeric misses)
    miss        n=63   agreement 0.698   19 false rejects (paraphrase)

    overall     0.775   FP 8 (6.7%)   FN 19 (15.8%)

Hence scripts/judge_reader_answers.py routes only `embedding` and `miss` to a
judge. exact/abstain are perfect and stay deterministic; the contains failures
are abstention detection, not grading.

## Ungradeable rows

Rows with no reference answer cannot be graded and are not incorrect. In the
full corpus manifest.json counts 227,429 of them, all in three beam question
types (instruction_following, preference_following, summarization). locomo's
adversarial stratum is likewise reference-free by design. Exclude before
grading; a verdict against a missing reference is meaningless whichever way
it falls.
