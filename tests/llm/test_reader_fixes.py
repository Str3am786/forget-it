"""D4 reader repair set — deterministic halves of the five fixes.

Each fix is behind a flag with the legacy default pinned byte-for-byte:
  (i)   abstention accounting  → grading.grade_with_abstention + the
        scored-abstention-row path in runner_core (flag: abstention_accounting)
  (ii)  raw+cleaned dual grade → reader clean_mode flag + *_raw columns
  (iii) generation cap         → per-type max-tokens override (list types)
  (iv)  prompt revision        → prompt_version="v2" (v1 pinned exactly)
  (v)   abstention matching    → abstention_match="exact" (loose pinned)

The model-facing halves (does v2 actually reduce false refusals, etc.) are
measured on the probe bench — report/04_reader_fixes.md.
"""
from __future__ import annotations

import pytest

from forgetit.benchmark.grading import grade_with_abstention, is_answerable
from forgetit.core.schema import AnswerResult, MemRecord, Query, Question
from forgetit.llm.readers.text_reader import (
    _clean_answer, _is_abstention, _wrap_prompt, _wrap_prompt_v2,
    LIST_ANSWER_TYPES, PlainTextReader,
)


# ── (v) abstention matching ──────────────────────────────────────────


def test_loose_matching_flags_hedged_answer_the_defect():
    """Reproduction (probe finding vii): an INFORMATIVE answer containing an
    abstention phrase as a substring flags as abstention under legacy loose
    matching. Pinned so the legacy default never silently changes."""
    hedged = "The blue vendor, but IDK if there are other options"
    assert _is_abstention(hedged) is True                    # legacy default
    assert _is_abstention(hedged, mode="loose") is True


def test_exact_matching_does_not_flag_hedged_answers():
    hedged = "The blue vendor, but IDK if there are other options"
    assert _is_abstention(hedged, mode="exact") is False
    # real abstentions still flag under exact
    assert _is_abstention("IDK", mode="exact") is True
    assert _is_abstention("idk.", mode="exact") is True
    assert _is_abstention("I don't know", mode="exact") is True


# ── (ii) cleaning modes ──────────────────────────────────────────────


LIST_ANSWER = "1. freeze the branch\n2. run the dry-run\n3. apply\n4. restart workers"


def test_firstline_cleaning_destroys_lists_the_defect():
    """Reproduction (probe finding vi): legacy first-line cleaning reduces a
    correct enumeration to its first item. Pinned as the legacy default."""
    assert _clean_answer(LIST_ANSWER) == "1. freeze the branch"


def test_full_cleaning_preserves_lists():
    cleaned = _clean_answer(LIST_ANSWER, mode="full")
    for step in ("freeze", "dry-run", "apply", "restart"):
        assert step in cleaned


def test_full_cleaning_still_strips_noise():
    noisy = "Sure, here is the answer: **May 14th**"
    assert _clean_answer(noisy, mode="full") == _clean_answer(noisy)


# ── (iv) prompt versions ─────────────────────────────────────────────


def test_v1_prompt_is_pinned_byte_for_byte():
    q = Question(id="q", text="When?", timestamp=0.0, meta={})
    assert _wrap_prompt(q, "[1] evidence") == (
        "Answer the question using ONLY the memories below.\n"
        "If the answer is not in the memories, say exactly: IDK\n"
        "Be brief — one sentence maximum.\n"
        "\n"
        "Memories:\n"
        "[1] evidence\n"
        "\n"
        "Question: When?\n"
        "Answer:"
    )


def test_v2_prompt_permits_derivation_and_keeps_exact_idk():
    q = Question(id="q", text="When?", timestamp=0.0, meta={})
    p = _wrap_prompt_v2(q, "[1] evidence")
    assert "combine" in p and "compute" in p        # derivation permitted
    assert "say exactly: IDK" in p                  # exact-form abstention kept
    assert "one sentence maximum" not in p          # list answers not forbidden


def test_reader_name_encodes_every_nondefault_mode():
    class _NoLLM:  # never called here
        def generate(self, *a, **k): raise AssertionError

    default = PlainTextReader(_NoLLM())
    assert default.name == "PlainTextReader"        # legacy label untouched
    assert PlainTextReader(_NoLLM(), token_budget=375).name == "PlainTextReader_rtok375"
    full = PlainTextReader(_NoLLM(), token_budget=375, prompt_version="v2",
                           abstention_match="exact", clean_mode="full")
    assert full.name == "PlainTextReader_rtok375_pv2_absexact_cleanfull"


# ── (i) abstention-aware grading ─────────────────────────────────────


def test_is_answerable_conventions():
    assert not is_answerable("q17_abs", "temporal-reasoning")   # LongMemEval suffix
    assert not is_answerable("1::q::3::abstention", "abstention")  # BEAM ability
    assert is_answerable("q17", "temporal-reasoning")


def test_unanswerable_correct_iff_abstained():
    g = grade_with_abstention("IDK", "", abstained=True, answerable=False)
    assert g["correct"] and g["tier"] == "abstain_correct"
    g = grade_with_abstention("The Hilton", "", abstained=False, answerable=False)
    assert not g["correct"] and g["tier"] == "abstain_missed"


def test_answerable_defers_to_legacy_grade():
    g = grade_with_abstention("May 14th", "May 14th", abstained=False, answerable=True)
    assert g["correct"] and g["tier"] == "exact"
    g = grade_with_abstention("IDK", "May 14th", abstained=True, answerable=True)
    assert not g["correct"] and g["tier"] == "abstain"


# ── (iii) per-type generation cap + (i) scored abstention rows ───────


class _StubReader:
    """Records the max_tokens each call received; answers IDK/abstain."""
    name = "stub"

    def __init__(self):
        self.calls = []

    def answer(self, question, records, *, max_tokens=None):
        self.calls.append(max_tokens)
        return AnswerResult(query_id=question.id, answer="IDK", abstained=True,
                            latency_sec=0.0, meta={"raw_output": "IDK"})


def _events():
    from forgetit.benchmark.schema import WorkloadEvent
    item = MemRecord(id="i1", text="alpha filler", bytes=20, created_at=1.0)
    ev_insert = WorkloadEvent(type="insert", timestamp=0.0, item=item,
                              query=None, gold_item_ids=[])
    q_abs = WorkloadEvent(
        type="query", timestamp=1.0, item=None,
        query=Query(id="c::q::0::abstention", text="what hotel?", timestamp=1.0, meta={}),
        gold_item_ids=[], ref_answer="", question_type="abstention")
    q_list = WorkloadEvent(
        type="query", timestamp=2.0, item=None,
        query=Query(id="c::q::1::event_ordering", text="alpha", timestamp=2.0, meta={}),
        gold_item_ids=["i1"], ref_answer="alpha", question_type="event_ordering")
    return [ev_insert, q_abs, q_list]


def _store():
    from forgetit.backend.in_memory import InMemoryBackend
    from forgetit.memory_system import RetentionManager
    from forgetit.policies import LRUPolicy
    from forgetit.retrieval.lexical import LexicalOverlapRetriever

    class _L:
        def event(self, *a, **k): pass
    return RetentionManager(budget_bytes=1_000_000,
                            backend=InMemoryBackend(retriever=LexicalOverlapRetriever()),
                            policy=LRUPolicy(), logger=_L())


def test_default_path_drops_abstention_rows_the_defect():
    """Reproduction: BEAM abstention questions vanish (access-only replay)."""
    from forgetit.benchmark.runner_core import run_one_example
    results, ctx, _, _ = run_one_example(
        _events(), _store(), k=5, dataset="beam", store_label="t",
        collect_reader_context=True)
    assert [r.question_type for r in results] == ["event_ordering"]


def test_abstention_accounting_gives_scored_rows_and_correct_grades():
    from forgetit.benchmark.pipeline import _answer_contexts_inline
    from forgetit.benchmark.runner_core import run_one_example
    stub = _StubReader()
    results, ctx, _, _ = run_one_example(
        _events(), _store(), k=5, dataset="beam", store_label="t",
        collect_reader_context=True, abstention_accounting=True)
    assert [r.question_type for r in results] == ["abstention", "event_ordering"]
    _answer_contexts_inline(ctx, reader_impl=stub, answer_max_tokens=128,
                            abstention_accounting=True, answer_max_tokens_list=0)
    abs_row = results[0]
    assert abs_row.grading_mode == "abstention_aware"
    assert abs_row.abstained and abs_row.qa_correct     # abstained on abstention q = correct
    assert abs_row.n_gold == 0


def test_list_type_gets_raised_generation_cap():
    from forgetit.benchmark.pipeline import _answer_contexts_inline
    from forgetit.benchmark.runner_core import run_one_example
    stub = _StubReader()
    assert "event_ordering" in LIST_ANSWER_TYPES
    _, ctx, _, _ = run_one_example(
        _events(), _store(), k=5, dataset="beam", store_label="t",
        collect_reader_context=True, abstention_accounting=True)
    _answer_contexts_inline(ctx, reader_impl=stub, answer_max_tokens=128,
                            abstention_accounting=True, answer_max_tokens_list=384)
    assert stub.calls == [128, 384]        # abstention q normal, list q raised
