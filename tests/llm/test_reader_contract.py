"""The reader's contract. Each case pins a measured defect that is now fixed.

There is ONE reader, unversioned. Historical behaviour lives in the archive and
in git history — nothing here reproduces it.
"""
from __future__ import annotations

import json

import pytest

from forgetit.core.schema import MemRecord, Question
from forgetit.llm.readers import (
    DEFAULT_PROMPT, PARAMETRIC_PROMPT, PlainTextReader, ReaderConfig,
    ReaderContext, build_context, build_parametric_context, build_reader,
    prompt_sha, resolve_prompt,
)
from forgetit.llm.readers.text_reader import TRUNCATION_MARKER

Q = Question(id="q1", text="What is the capital of France?")


class _Stub:
    """Plain client: generate() only, no metadata call."""
    model = "stub-model"

    def __init__(self, reply="Paris"):
        self.reply = reply
        self.seen_max_tokens = None

    def generate(self, prompt, *, max_tokens=None, temperature=None):
        self.seen_max_tokens = max_tokens
        return self.reply


class _MetaStub(_Stub):
    """Client that reports finish_reason."""

    def __init__(self, reply="Paris", finish_reason="stop"):
        super().__init__(reply)
        self.finish_reason = finish_reason

    def generate_with_meta(self, prompt, *, max_tokens=None, temperature=None):
        self.seen_max_tokens = max_tokens
        return self.reply, self.finish_reason


class _Boom(_Stub):
    def generate(self, *a, **k):
        raise RuntimeError("connection reset")


# ── 1. an error is not an abstention ─────────────────────────────────

def test_llm_error_is_errored_not_abstained():
    """A timeout on an unanswerable question used to grade as a correct refusal."""
    res = PlainTextReader(_Boom()).answer(Q, [])
    assert res.errored is True
    assert res.abstained is False
    assert res.answer == ""
    assert "connection reset" in res.meta["error"]


# ── 2. cap_hit ───────────────────────────────────────────────────────

def test_cap_hit_true_when_generation_cut_at_the_cap():
    res = PlainTextReader(_MetaStub(finish_reason="length")).answer(Q, [])
    assert res.meta["cap_hit"] is True
    assert res.meta["finish_reason"] == "length"


def test_cap_hit_false_when_generation_completed():
    res = PlainTextReader(_MetaStub(finish_reason="stop")).answer(Q, [])
    assert res.meta["cap_hit"] is False


def test_cap_hit_none_when_the_client_cannot_report_it():
    """Unknown is not the same as not-cut — a client without the metadata call
    must leave cap_hit null rather than claiming the cap did not bind."""
    res = PlainTextReader(_Stub()).answer(Q, [])
    assert res.meta["cap_hit"] is None
    assert res.meta["finish_reason"] is None


# ── 3. abstention is whole-answer ────────────────────────────────────

def test_hedged_informative_answer_is_not_an_abstention():
    """Substring matching scored this informative answer as a refusal."""
    res = PlainTextReader(_Stub("Paris, though IDK about the rest")).answer(Q, [])
    assert res.abstained is False


@pytest.mark.parametrize("reply", ["IDK.", "idk", "I do not know", "  I don't know.  "])
def test_whole_answer_refusal_is_an_abstention(reply):
    assert PlainTextReader(_Stub(reply)).answer(Q, []).abstained is True


# ── 4. cleaning never truncates ──────────────────────────────────────

def test_multiline_answer_survives_cleaning_verbatim():
    """The old first-line rule scored an ordered list as its first item."""
    reply = "1. Alice\n2. Bob\n3. Carol"
    res = PlainTextReader(_Stub(reply)).answer(Q, [])
    assert res.answer == reply


def test_cleaning_strips_noise_without_cutting_content():
    res = PlainTextReader(_Stub("**Based on the memories, Paris**")).answer(Q, [])
    assert res.answer == "Paris"


# ── 5. build_context is pure and serialisable ────────────────────────

def _records(n=3, size=20):
    return [MemRecord(id=f"m{i}", text=f"item {i} " + "x" * size) for i in range(n)]


def test_build_context_is_deterministic():
    recs = _records()
    a = build_context(Q, recs, token_budget=500)
    b = build_context(Q, recs, token_budget=500)
    assert a.prompt == b.prompt
    assert a.item_ids == b.item_ids == ["m0", "m1", "m2"]


def test_context_survives_a_json_round_trip():
    ctx = build_context(Q, _records(), token_budget=500)
    assert ReaderContext.from_json(ctx.to_json()) == ctx
    json.loads(ctx.to_json())          # genuinely JSON, not just repr-able


# ── 6. the prompt is a hashed template, and an experimental axis ─────

def test_non_default_template_becomes_a_distinct_arm():
    tmpl = "Different wording.\n{evidence}\nQ: {question}\nA:"
    reader = PlainTextReader(_Stub(), prompt=tmpl)
    assert reader.name.endswith(f"_p{prompt_sha(tmpl)[:6]}")
    assert reader.answer(Q, []).meta["prompt_sha"] == prompt_sha(tmpl)


def test_default_template_gets_no_suffix():
    assert "_p" not in PlainTextReader(_Stub()).name.rsplit("_gen", 1)[0]


@pytest.mark.parametrize("tmpl", [
    "no placeholders at all",
    "only evidence: {evidence}",
    "only question: {question}",
])
def test_incomplete_template_is_rejected_at_construction(tmpl):
    with pytest.raises(ValueError):
        PlainTextReader(_Stub(), prompt=tmpl)


# ── 7. evidence: whole items, then one marked head ───────────────────

def test_oversized_item_is_delivered_as_a_marked_head():
    """Never silently mutilated: cut items say so, in the prompt and in the row."""
    long_item = MemRecord(id="big", text="y" * 4000)
    ctx = build_context(Q, [long_item], token_budget=100)   # 100 tok ≈ 400 chars
    assert ctx.truncated is True
    assert ctx.item_ids == ["big"]
    assert ctx.n_offered == 1 and ctx.n_delivered == 1
    assert TRUNCATION_MARKER in ctx.prompt


def test_items_that_do_not_fit_are_dropped_not_reordered():
    recs = [MemRecord(id=f"m{i}", text="z" * 200) for i in range(5)]
    ctx = build_context(Q, recs, token_budget=100)          # ≈ 400 chars
    assert ctx.n_offered == 5
    assert ctx.n_delivered < 5
    assert ctx.item_ids == [f"m{i}" for i in range(ctx.n_delivered)]


# ── 8. the parametric arm ────────────────────────────────────────────

def test_parametric_context_carries_no_evidence():
    ctx = build_parametric_context(Q)
    assert ctx.item_ids == [] and ctx.n_offered == 0 and ctx.n_delivered == 0
    assert ctx.evidence_chars == 0 and ctx.truncated is False
    assert Q.text in ctx.prompt
    assert "Memories:" not in ctx.prompt
    assert ctx.prompt_sha == prompt_sha(PARAMETRIC_PROMPT)


def test_parametric_template_is_not_a_valid_constructor_prompt():
    """It deliberately has no {evidence} — that is the whole point of it."""
    with pytest.raises(ValueError):
        PlainTextReader(_Stub(), prompt=PARAMETRIC_PROMPT)


# ── 9. the configuration surface ─────────────────────────────────────

@pytest.mark.parametrize("kwargs", [{"token_budget": 0}, {"token_budget": -1},
                                    {"max_tokens": 0}, {"max_tokens": -5}])
def test_reader_config_rejects_nonpositive(kwargs):
    """Validation lives in __post_init__ so it holds for probes and sweeps,
    not only for the command line."""
    with pytest.raises(ValueError):
        ReaderConfig(**kwargs)


def test_resolve_prompt_reads_a_file_passes_a_string_defaults_on_none(tmp_path):
    f = tmp_path / "wording.txt"
    f.write_text("From a file.\n{evidence}\n{question}", encoding="utf-8")
    assert resolve_prompt(str(f)) == f.read_text(encoding="utf-8")
    assert resolve_prompt("inline {evidence} {question}") == "inline {evidence} {question}"
    assert resolve_prompt(None) == DEFAULT_PROMPT


def test_build_reader_wires_every_config_field():
    tmpl = "Wording.\n{evidence}\n{question}"
    reader = build_reader(_Stub(), ReaderConfig(token_budget=321, max_tokens=77,
                                                prompt=tmpl))
    assert reader.token_budget == 321
    assert reader.max_tokens == 77
    assert reader.prompt == tmpl


def test_build_reader_with_no_config_is_the_default_reader():
    assert build_reader(_Stub()).name == PlainTextReader(_Stub()).name


# ── 10. the reader's name cannot lie about its cap ───────────────────

def test_name_encodes_the_cap_that_is_actually_sent():
    """The pipeline used to pass its own max_tokens while the name kept the
    constructor's, so a row could be labelled with a cap it never used."""
    llm = _Stub()
    reader = PlainTextReader(llm, max_tokens=64)
    assert "_gen64" in reader.name
    reader.answer(Q, [])
    assert llm.seen_max_tokens == 64


def test_name_encodes_model_and_budget():
    reader = PlainTextReader(_Stub(), token_budget=1234, max_tokens=99)
    assert reader.name == "reader_stub-model_rtok1234_gen99"
