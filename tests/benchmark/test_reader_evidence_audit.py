"""Reader-context audit columns: every row records how much of the retrieved
list actually reached the model.

Why it's a column and not a log line: an abstention is ambiguous without it.
`abstained=True, hit_at_5=1.0` reads as a memory-policy failure, but if the
prompt only carried item 1 of 5 it is a reader-context artifact. The answer
stage cannot separate those two claims after the fact unless the row says so.
"""
from forgetit.benchmark.runner_core import _run_reader
from forgetit.core.schema import MemRecord, Query
from forgetit.llm.readers import PlainTextReader


class FakeLLM:
    model = "stub"

    def generate(self, prompt, **kwargs):
        return "IDK"


QUERY = Query(text="Where?", id="q1", timestamp=1.0)
# Five ~800-char items — the top-5 a session-itemised run hands the reader.
TOPK = [MemRecord(id=f"i{n}", text=f"item {n}: " + "z" * 800,
                  created_at=1.0, bytes=800) for n in range(5)]


def run(reader):
    return _run_reader(reader_impl=reader, query=QUERY, retrieved=TOPK,
                       ref_answer="somewhere",
                       question_type="single_hop")


def test_generous_budget_records_full_delivery():
    qa = run(PlainTextReader(FakeLLM(), token_budget=1500))
    assert qa.reader_evidence_delivered == len(TOPK)
    assert qa.reader_evidence_truncated is False


def test_tight_budget_records_partial_delivery():
    # 250 tokens = 1000 chars: item 1 whole, a head of item 2, items 3-5 never
    # sent. `delivered` counts items the prompt carried at all (the cut one
    # included — it is marked in the text); `truncated` says the tail was cut.
    # The row must not look like a run that showed the model all five.
    qa = run(PlainTextReader(FakeLLM(), token_budget=250))
    assert qa.reader_evidence_delivered == 2
    assert qa.reader_evidence_truncated is True


def test_no_reader_leaves_audit_at_defaults():
    qa = _run_reader(reader_impl=None, query=QUERY, retrieved=TOPK,
                     ref_answer="somewhere",
                     question_type="single_hop")
    assert qa.reader_evidence_delivered == 0
    assert qa.reader_evidence_truncated is False


def test_llm_error_does_not_crash_the_audit():
    class BoomLLM:
        model = "stub"

        def generate(self, prompt, **kwargs):
            raise RuntimeError("endpoint down")

    qa = run(PlainTextReader(BoomLLM(), token_budget=1500))
    # An infrastructure failure is neither an answer nor a refusal. Recording
    # it as an abstention scored a timeout on an unanswerable question as a
    # correct refusal — the row must say "errored", not "declined".
    assert qa.reader_errored is True
    assert qa.abstained is False
    assert qa.reader_answer == ""
    # The audit survives the failure. The context is assembled before the call,
    # so a failed row still says what evidence WOULD have been read — which is
    # what makes a retry comparable to the run it is replacing.
    assert qa.reader_evidence_delivered == len(TOPK)
    assert qa.reader_evidence_truncated is False
    assert qa.reader_item_ids == '["i0", "i1", "i2", "i3", "i4"]'
