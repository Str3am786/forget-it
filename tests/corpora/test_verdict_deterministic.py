"""verdict_triplets — the DETERMINISTIC grader tiers must reproduce their
subset at 100% (VERIFICATION.md S8). Judge-tier triplets are exercised by the
real judge (author-side κ run), not here.

Deterministic replication: abstention detection = exact-form (D4 fix v);
answerable = the fixture carries a non-null reference; verdict =
grade_with_abstention (the abstention-aware regime, which j005's class exists
to pin).
"""
from __future__ import annotations

import pytest

from forgetit.benchmark.grading import grade_with_abstention
from forgetit.llm.readers.text_reader import _is_abstention

from tests.corpora.harness import load_fixtures

_DET_TIERS = {"deterministic", "deterministic_and_judge"}
_FIXTURES = {fx["id"]: fx for fx in load_fixtures("verdict_triplets")
             if fx.get("tier") in _DET_TIERS}


@pytest.mark.parametrize("fixture_id", sorted(_FIXTURES))
def test_deterministic_verdict(fixture_id):
    fx = _FIXTURES[fixture_id]
    inp = fx["input"]
    ref = inp.get("reference")
    answer = inp["answer"]
    abstained = _is_abstention(answer, mode="exact")
    g = grade_with_abstention(answer, ref or "", abstained=abstained,
                              answerable=ref is not None)
    want_correct = fx["expected"] == "CORRECT"
    assert g["correct"] == want_correct, (
        f"{fixture_id} ({fx['class']}): grader said correct={g['correct']} "
        f"(tier {g['tier']}), fixture expects {fx['expected']}"
    )
