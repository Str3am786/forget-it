"""Containment and veto regression tests for grade().

Every rejection case below was a live FALSE POSITIVE across the whole campaign.
`contains_match` and `answer_in_text` matched SUBSTRINGS, so the reference "No"
was accepted by any answer containing "nothing", "50" by "150", "May" by
"maybe" — and digit_normalize's month folding turned "May" into "5", which then
matched inside "2015". These fire hardest on short-span references (yes/no,
dates, quantities), which is where the deterministic tier does most of its work.

They survived a 130-row human audit undetected, because that corpus was
stratified on the grader's own tiers and a false accept never lands in the miss
stratum. Only direct probing of the primitives found them. Hence this file: the
audit cannot be the regression net.

The acceptance cases pin the recovery paths added alongside — each is
veto-gated, so a future loosening that admits the rejection cases above will
fail here rather than in a rerun.
"""
from __future__ import annotations

import pytest

from forgetit.benchmark.grading import (
    answer_in_text, contains_match, grade, normalized_em,
)


class TestSubstringFalsePositives:
    """Token-sequence containment, not substring."""

    @pytest.mark.parametrize("answer,ref", [
        ("the answer is nothing", "No"),        # "no" inside "nothing"
        ("I know that already", "no"),          # inside "know"
        ("he went north", "no"),                # inside "north"
        ("maybe he left on tuesday", "May"),    # inside "maybe"
        ("I paid 150 dollars", "50"),           # inside "150"
        ("the cost was 2015", "20"),            # inside "2015"
        ("she ran the marathon", "an"),         # inside "ran"
    ])
    def test_partial_word_is_not_a_match(self, answer, ref):
        assert not contains_match(answer, ref)
        assert not grade(answer, ref)["correct"]

    def test_month_folding_does_not_match_inside_a_year(self):
        """digit_normalize maps May -> 5; substring matching then found it in
        2015. Both the fold and the match must be token-scoped."""
        assert not answer_in_text("May", "it was 2015")
        assert not answer_in_text("March", "there were 3000 attendees")

    def test_real_containment_still_accepts(self):
        assert contains_match("you should see a cardiologist", "cardiologist")
        assert contains_match("she moved to Paris last year", "Paris")
        assert answer_in_text("Paris", "she moved to Paris last year")

    def test_month_matches_its_own_digit_form(self):
        """The fold is still wanted where it was aimed: a date answer against
        the same date written the other way."""
        assert answer_in_text("5", "scheduled for May")
        assert answer_in_text("May", "scheduled for 5 March")   # KNOWN LIMIT

    @pytest.mark.xfail(reason="digit_normalize maps month names to bare "
                              "numerals, so a month reference matches an "
                              "unrelated day-of-month with the same value. "
                              "Narrow, documented, fixing it needs positional "
                              "date parsing rather than token rewriting.",
                       strict=True)
    def test_month_does_not_match_an_unrelated_day_number(self):
        assert not answer_in_text("May", "the meeting was on 5 June")


class TestRecoveryPaths:
    """Deterministic paraphrase recovery, each behind the vetoes."""

    def test_reverse_containment(self):
        """Answer inside the reference — the direction contains_match cannot
        see. Audited row: 'The University of Melbourne.'"""
        assert grade("The University of Melbourne.",
                     "University of Melbourne in Australia")["correct"]

    def test_reverse_containment_needs_substance(self):
        """A three-token floor, so terse noise cannot ride the reverse path."""
        assert not grade("yes", "yes he did go to the shop on tuesday")["correct"]

    def test_content_words_order_free(self):
        """Terse conjunctive golds answered in the other order. Audited row:
        'photography and cooking'."""
        assert grade("cooking and photography clubs",
                     "photography and cooking")["correct"]

    def test_content_words_need_contiguity(self):
        """Scattered mention is not an assertion: the reference's words each
        appear in the answer but in unrelated clauses, so the assertion the
        reference makes was never made."""
        from forgetit.benchmark.grading import content_word_containment
        assert not content_word_containment(
            "Whispering Falls. Joanna visited this location in May and later "
            "took a photo of a large waterfall on a different trip entirely.",
            "Whispering Falls waterfall")


class TestVetoes:
    """No non-verbatim path may accept when a veto fires."""

    def test_numeric_veto_blocks_near_misses(self):
        """Semantically near, factually wrong — the class no threshold fixes."""
        assert not grade("You studied 7 hours and scored 93%",
                         "You studied 6 hours and scored 90%")["correct"]
        assert not grade("first term is 1 and the ratio is 2",
                         "first term is 3 and the ratio is 2")["correct"]

    def test_entity_veto_blocks_substitution(self):
        """The modal failure: the named thing swapped, topic preserved."""
        assert not grade("The restaurant is called Plant Based.",
                         "By Chloe")["correct"]

    def test_entity_veto_ignores_sentence_initial_capitals(self):
        """'Their' and 'Joining' are capitalised by grammar, not by being
        names. Treating them as entities vetoed three correct answers in the
        audit. Tested on the veto directly: end-to-end acceptance also needs a
        recovery path to match, which is a separate concern."""
        from forgetit.benchmark.grading import _entity_veto
        assert not _entity_veto(
            "Caroline joined an activist group and went to pride parades",
            "Joining activist group and going to pride parades")
        assert not _entity_veto(
            "Evan's experience with health challenges shaped his outlook",
            "Their experiences lead them to view challenges as opportunities")

    def test_entity_veto_still_catches_mid_sentence_names(self):
        from forgetit.benchmark.grading import _entity_veto
        assert _entity_veto("the restaurant is Plant Based",
                            "It is called By Chloe")

    def test_quoted_span_veto_blocks_list_substitution(self):
        """A reference quoting its payload is an exact-recall probe. All spans
        must survive — `any` was defeated by one span recurring in trailing
        prose (acceptance-by-mention)."""
        answer = ("I suggested sexual addiction, compulsive sexual behavior, "
                  "hypersexuality, and impulse control disorder. These describe "
                  "problematic sexual behaviors.")
        ref = ("I suggested 'sexual fixations', 'problematic sexual behaviors', "
               "'sexual impulsivity', and 'compulsive sexuality'.")
        assert not grade(answer, ref)["correct"]


class TestUnchangedContract:
    """The rungs that predate the fix must behave exactly as before."""

    def test_exact_and_abstain(self):
        assert grade("Paris", "Paris")["tier"] == "exact"
        assert not grade("I don't know", "Paris", abstained=True)["correct"]
        assert grade("I don't know", "Paris", abstained=True)["tier"] == "abstain"

    def test_empty_inputs(self):
        assert not grade("", "Paris")["correct"]
        assert not grade("Paris", "")["correct"]
        assert not normalized_em("Paris", "")

    def test_alias_path(self):
        v = grade("cardiologist", "heart doctor", aliases=("cardiologist",))
        assert v["correct"] and v["tier"] == "alias"

    def test_clause_tier_is_gone(self):
        """Deleted: zero fires on both corpus halves. If it returns, it needs
        evidence, not inertia."""
        import forgetit.benchmark.grading as g
        assert not hasattr(g, "clause_containment")