"""D4 fix iv — prompt wrapper selection, legacy pinned byte-for-byte.

The v1/v2 pins here are recovered verbatim from tests/llm/test_reader_fixes.py
on the `verification-sprint` branch, which never merged to main (the rest of
that file tests abstention-accounting machinery main does not have). They are
the contract the wrappers were written against: v1 must not drift, and v2 must
keep the exact-form IDK line that _is_abstention matches on.
"""
from forgetit.core.schema import Question
from forgetit.llm.readers.text_reader import (
    PROMPT_BUILDERS,
    PlainTextReader,
    _is_abstention,
    _wrap_prompt,
    _wrap_prompt_v2,
)

Q = Question(id="q", text="When?", timestamp=0.0, meta={})


class _NoLLM:
    def generate(self, *a, **k):
        raise AssertionError("prompt-only test: the LLM must not be called")


def test_v1_prompt_is_pinned_byte_for_byte():
    assert _wrap_prompt(Q, "[1] evidence") == (
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
    p = _wrap_prompt_v2(Q, "[1] evidence")
    assert "combine" in p and "compute" in p        # derivation permitted
    assert "say exactly: IDK" in p                  # exact-form abstention kept
    assert "one sentence maximum" not in p          # list answers not forbidden


def test_v2_idk_line_still_matches_the_abstention_detector():
    """The whole point of keeping the exact-form line: a model that follows v2's
    instruction to the letter must still be scored as an abstention. If the
    wrapper's IDK wording ever drifts, abstentions become hallucinations in the
    grades, silently."""
    p = _wrap_prompt_v2(Q, "[1] evidence")
    instructed = p.split("say exactly: ")[1].split("\n")[0].strip()
    assert _is_abstention(instructed) is True
    assert _is_abstention(instructed, mode="exact") is True


def test_default_is_v1_and_name_encodes_v2():
    assert PlainTextReader(_NoLLM()).prompt_version == "v1"
    assert PlainTextReader(_NoLLM()).name == "PlainTextReader"
    assert PlainTextReader(_NoLLM(), prompt_version="v2").name == "PlainTextReader_pv2"
    # Combined with the evidence budget, both modes stay in the label.
    both = PlainTextReader(_NoLLM(), token_budget=1500, prompt_version="v2")
    assert both.name == "PlainTextReader_rtok1500_pv2"


def test_selected_wrapper_is_the_one_used():
    for version, wrapper in PROMPT_BUILDERS.items():
        class _EchoLLM:
            seen = ""

            def generate(self, prompt, **kw):
                _EchoLLM.seen = prompt
                return "ok"

        llm = _EchoLLM()
        PlainTextReader(llm, prompt_version=version).answer(Q, [])
        assert _EchoLLM.seen == wrapper(Q, "")


def test_unknown_version_is_rejected():
    import pytest
    with pytest.raises(ValueError):
        PlainTextReader(_NoLLM(), prompt_version="v3")
