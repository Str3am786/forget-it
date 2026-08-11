"""Evidence budgeting: ONE total budget, rank-order fill, whole items
preferred, and an explicit marker on the one item that gets cut.

The semantics are unchanged from when these were written; only the call sites
moved, from the private prompt builders onto build_evidence/build_context.
"""
from forgetit.core.schema import MemRecord, Question
from forgetit.llm.readers.text_reader import (
    DEFAULT_TOKEN_BUDGET,
    PlainTextReader,
    TRUNCATION_MARKER,
    _CHARS_PER_TOKEN,
    build_context,
    build_evidence,
)


class FakeLLM:
    model = "fake"

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
    ev, truncated, ids = build_evidence(RECORDS, budget)
    assert len(ev) <= budget * _CHARS_PER_TOKEN
    assert truncated is True
    assert ev.endswith(TRUNCATION_MARKER)
    assert "[1] short first" in ev
    assert "[2] LONG summary" in ev                 # head of next item included
    assert "[3]" not in ev                          # filling stopped at the cut
    assert ids == ["r1", "r2"]                      # 2 of 3 retrieved reached the model


def test_rank_order_and_no_per_item_cap():
    # Budget big enough for everything: the 2000-char item must appear WHOLE.
    # A per-item cap would have shredded it into a stump.
    ev, truncated, ids = build_evidence(RECORDS, 10_000)
    assert truncated is False
    assert TRUNCATION_MARKER not in ev
    assert ev.index("[1]") < ev.index("[2]") < ev.index("[3]")
    assert RECORDS[1].text.rstrip() in ev           # long item intact
    assert ids == [r.id for r in RECORDS]           # every retrieved item delivered


def test_tiny_budget_takes_head_of_first_item_only():
    ev, truncated, ids = build_evidence(RECORDS, 10)          # 40 chars
    assert len(ev) <= 10 * _CHARS_PER_TOKEN
    assert truncated is True
    assert ev.startswith("[1] ") and ev.endswith(TRUNCATION_MARKER)
    assert ids == ["r1"]


def test_delivered_ids_are_the_audit_trail():
    """A delivered-evidence claim must be checkable against the retrieval log,
    not inferred from a count."""
    ctx = build_context(QUESTION, RECORDS, token_budget=100)
    assert ctx.item_ids == ["r1", "r2"]
    assert ctx.n_offered == 3 and ctx.n_delivered == 2
    assert ctx.truncated is True


def test_budgeted_reader_end_to_end_uses_budget_and_reports_meta():
    llm = FakeLLM()
    reader = PlainTextReader(llm, token_budget=100)
    assert reader.name == "reader_fake_rtok100_gen256"
    result = reader.answer(QUESTION, RECORDS)
    assert result.answer == "Paris."
    assert result.meta["token_budget"] == 100
    assert result.meta["evidence_truncated"] is True
    # Offered vs delivered: 3 retrieved, only 2 fit in a 100-token window.
    assert result.meta["evidence_count"] == 3
    assert result.meta["evidence_delivered"] == 2
    assert result.meta["item_ids"] == ["r1", "r2"]
    ev = evidence_block(llm.prompts[0])
    assert len(ev) <= 100 * _CHARS_PER_TOKEN


def test_default_budget_delivers_a_realistic_top_k_whole():
    """The abstain≈0.70-against-hit@5≈0.86 regression, pinned.

    Five LongMemEval-sized session items (~900 chars each) are what top-k
    retrieval hands the reader. The default budget must deliver all five
    intact — retrieving them and then cutting them is the confound that made
    hit@k and reader accuracy describe different evidence.
    """
    topk = [rec(f"g{i}", f"session {i}: " + "w" * 900) for i in range(5)]
    ev, truncated, ids = build_evidence(topk, DEFAULT_TOKEN_BUDGET)

    assert truncated is False
    assert TRUNCATION_MARKER not in ev
    assert all(r.text in ev for r in topk)
    assert len(ev) <= DEFAULT_TOKEN_BUDGET * _CHARS_PER_TOKEN
    assert ids == [r.id for r in topk]


def test_same_budget_fits_more_short_items_than_long_ones():
    # Parity is the point: under one budget, compression-style short items
    # pack; one long item eats the window. 8 × 100-char vs 1 × 2000-char.
    ev_s, _, ids_s = build_evidence([rec(f"s{i}", f"item{i} " + "x" * 93)
                                     for i in range(8)], 250)   # 1000 chars
    ev_l, _, ids_l = build_evidence(RECORDS[1:2], 250)
    assert len(ids_s) >= 8 and len(ids_l) == 1


def test_empty_evidence_is_a_valid_context():
    ev, truncated, ids = build_evidence([], 100)
    assert ev == "" and truncated is False and ids == []
