"""corpus_e exemplars (e001–e008) as parametrized tests against the REAL store.

Deterministic stage → the acceptance bar is 100% (VERIFICATION.md §7); a
failure here is a bug (in code, fixture, or harness), never a rate.

e005: the author ruled (sprint 2, Q1) that incoming-item EXEMPTION is the
intended semantics — the fixture's ``expected`` is now [A] and the
eligible-variant reading is preserved as its ``assumptions_note``. The former
OPEN skip is retired; ``test_e005_measured_behavior_incoming_exempt`` remains
as the code-path regression twin.
"""
from __future__ import annotations

import pytest

from tests.corpora.harness import load_fixtures, run_e_fixture

_FIXTURES = {fx["id"]: fx for fx in load_fixtures("corpus_e")}


@pytest.mark.parametrize("fixture_id", sorted(_FIXTURES))
def test_corpus_e_exemplar(fixture_id):
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
    """Regression twin of e005 (author ruling Q1: incoming item exempt).
    The LFU candidates at C's insert are the residents {A: freq 1, B: freq 2}
    → victim A. Kept alongside the fixture so a future budget-loop reordering
    fails two independent tests, not one."""
    got = run_e_fixture(_FIXTURES["e005"])
    assert got["evictions"] == ["A"]
    assert got["resident"] == ["B", "C"]
