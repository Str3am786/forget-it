"""E6c token-budgeted reader: one total evidence budget, rank-order fill,
whole items preferred, explicit truncation marker, legacy path untouched."""
from forgetit.core.schema import MemRecord, Question
from forgetit.llm.readers.text_reader import (
    PlainTextReader,
    TRUNCATION_MARKER,
    _CHARS_PER_TOKEN,
    _build_prompt,
    _build_prompt_budgeted,
)


class FakeLLM:
    def __init__(self, reply="Paris."):
        self.reply = reply
        self.prompts = []

    def generate(self, prompt, **kwargs):
        self.prompts.append(prompt)
        return self.reply


def rec(item_id, text):
    return MemRecord(id=item_id, text=text, created_at=1.0, bytes=len(text))


QUESTION = Question(id="q1", text="Where?", timestamp=2.0, meta={})

# Very unequal lengths, as the spec asks: a short note, a long compression-
# summary-like item, and another short one.
RECORDS = [
    rec("r1", "short first " + "a" * 40),          # ~52 chars, rank 1
    rec("r2", "LONG summary " + "b" * 2000),       # ~2013 chars, rank 2
    rec("r3", "short third " + "c" * 40),          # ~52 chars, rank 3
]


def evidence_block(prompt: str) -> str:
    return prompt.split("Memories:\n", 1)[1].rsplit("\n\nQuestion:", 1)[0]


def test_budget_respected_and_marker_on_truncated_tail():
    budget = 100                                    # 400 chars: r1 + part of r2
    prompt, truncated = _build_prompt_budgeted(QUESTION, RECORDS, budget)
    ev = evidence_block(prompt)
    assert len(ev) <= budget * _CHARS_PER_TOKEN
    assert truncated is True
    assert ev.endswith(TRUNCATION_MARKER)
    assert "[1] short first" in ev
    assert "[2] LONG summary" in ev                 # tail of next item included
    assert "[3]" not in ev                          # filling stopped at the cut


def test_rank_order_and_no_per_item_cap():
    # Budget big enough for everything: the 2000-char item must appear WHOLE
    # (the legacy path would have cut it at 300 chars).
    prompt, truncated = _build_prompt_budgeted(QUESTION, RECORDS, 10_000)
    ev = evidence_block(prompt)
    assert truncated is False
    assert TRUNCATION_MARKER not in ev
    assert ev.index("[1]") < ev.index("[2]") < ev.index("[3]")
    assert RECORDS[1].text.rstrip() in ev           # long item intact


def test_tiny_budget_takes_head_of_first_item_only():
    prompt, truncated = _build_prompt_budgeted(QUESTION, RECORDS, 10)   # 40 chars
    ev = evidence_block(prompt)
    assert len(ev) <= 10 * _CHARS_PER_TOKEN
    assert truncated is True
    assert ev.startswith("[1] ") and ev.endswith(TRUNCATION_MARKER)


def test_legacy_prompt_unchanged_and_default():
    reader = PlainTextReader(FakeLLM())
    assert reader.token_budget is None
    assert reader.name == "PlainTextReader"
    # Legacy builder: per-item 300-char truncation with the old ellipsis.
    prompt = _build_prompt(QUESTION, RECORDS[:2])
    ev = evidence_block(prompt)
    lines = ev.split("\n")
    assert lines[0] == "[1] " + RECORDS[0].text     # short item untouched
    assert lines[1] == "[2] " + RECORDS[1].text[:300].rstrip() + "…"


def test_budgeted_reader_end_to_end_uses_budget_and_reports_meta():
    llm = FakeLLM()
    reader = PlainTextReader(llm, token_budget=100)
    assert reader.name == "PlainTextReader_rtok100"
    result = reader.answer(QUESTION, RECORDS)
    assert result.answer == "Paris."
    assert result.meta["token_budget"] == 100
    assert result.meta["evidence_truncated"] is True
    ev = evidence_block(llm.prompts[0])
    assert len(ev) <= 100 * _CHARS_PER_TOKEN


def test_same_budget_fits_more_short_items_than_long_ones():
    # Parity is the point: under one budget, compression-style short items
    # pack; one long item eats the window. 8 × 100-char vs 1 × 2000-char.
    shorts = [rec(f"s{i}", f"item{i} " + "x" * 93) for i in range(8)]
    prompt_s, _ = _build_prompt_budgeted(QUESTION, shorts, 250)         # 1000 chars
    prompt_l, _ = _build_prompt_budgeted(QUESTION, RECORDS[1:2], 250)
    n_short = len(evidence_block(prompt_s).split("\n"))
    n_long = len(evidence_block(prompt_l).split("\n"))
    assert n_short >= 8 and n_long == 1
