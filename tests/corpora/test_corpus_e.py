"""corpus_e exemplars (e001–e008) as parametrized tests against the REAL store.

Deterministic stage → the acceptance bar is 100% (VERIFICATION.md §7); a
failure here is a bug (in code, fixture, or harness), never a rate.

e005 is SKIPPED as OPEN, per AGENT.md rule 10: its primary ``expected`` block
assumes the incoming item is eviction-eligible, but the implementation runs
the budget loop BEFORE admitting the incoming item (custom_manager.py
``_ensure_budget`` precedes the upsert), so the incoming item can never be its
own victim. The fixture's own ``assumptions_note`` anticipated exactly this
fork ("expected becomes [A]"). Which reading is intended is the author's call
— see report/QUESTIONS.md Q1. The measured behavior is pinned by
``test_e005_measured_behavior_incoming_exempt`` below so the resolution is a
one-line fixture edit, not an investigation.
"""
from __future__ import annotations

import pytest

from tests.corpora.harness import load_fixtures, run_e_fixture

_FIXTURES = {fx["id"]: fx for fx in load_fixtures("corpus_e")}

OPEN_FIXTURES = {
    "e005": "OPEN — QUESTIONS.md Q1: incoming-item eligibility "
            "(fixture assumes eligible; implementation exempts it)",
}


@pytest.mark.parametrize("fixture_id", sorted(_FIXTURES))
def test_corpus_e_exemplar(fixture_id):
    if fixture_id in OPEN_FIXTURES:
        pytest.skip(OPEN_FIXTURES[fixture_id])
    fx = _FIXTURES[fixture_id]
    got = run_e_fixture(fx)
    expected = fx["expected"]
    assert got["evictions"] == list(expected["evictions"]), (
        f"{fixture_id}: eviction sequence {got['evictions']} != "
        f"expected {expected['evictions']}"
    )
    assert got["resident"] == sorted(expected["resident"]), (
        f"{fixture_id}: residents {got['resident']} != "
        f"expected {sorted(expected['resident'])}"
    )
    if "compressions" in expected:
        assert got["compressions"] == list(expected["compressions"]), (
            f"{fixture_id}: compressions {got['compressions']} != "
            f"expected {expected['compressions']}"
        )
    if "resident_bytes" in expected:
        assert got["resident_bytes"] == dict(expected["resident_bytes"]), (
            f"{fixture_id}: resident_bytes {got['resident_bytes']} != "
            f"expected {expected['resident_bytes']}"
        )


def test_e005_measured_behavior_incoming_exempt():
    """Pins what the implementation ACTUALLY does on e005's trace, so the code
    path is regression-guarded while the fixture's expected value stays OPEN.
    Under incoming-exemption the LFU candidates at C's insert are {A: freq 1,
    B: freq 2} → victim A (the fixture's own alternative reading)."""
    got = run_e_fixture(_FIXTURES["e005"])
    assert got["evictions"] == ["A"]
    assert got["resident"] == ["B", "C"]
