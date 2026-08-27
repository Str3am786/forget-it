"""The dated evidence renderer: prefix, purity, id->date resolution, goldens.

The load-bearing invariant is that adding dates changed the renderer ONLY where
a date exists. A record with no date_str must render byte-identically to the
undated campaign, or every archived artefact silently stops being comparable.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "data"

from forgetit.benchmark.item_dates import (          # noqa: E402
    DateMap, beam_dates, locomo_dates, longmemeval_dates,
    longmemeval_dates_by_example, longmemeval_question_dates,
)
from forgetit.core.schema import MemRecord, Question  # noqa: E402
from forgetit.llm.readers import (                    # noqa: E402
    DEFAULT_PROMPT, ReaderConfig, build_context, build_evidence, prompt_sha,
    render_sha,
)
from forgetit.llm.readers.text_reader import TRUNCATION_MARKER  # noqa: E402

LOCOMO = DATA / "locomo10.json"
LME = DATA / "longmemeval_s_cleaned.json"
BEAM = DATA / "beam_500K.jsonl"
needs_data = pytest.mark.skipif(
    not (LOCOMO.is_file() and LME.is_file() and BEAM.is_file()),
    reason="raw dataset files not present")


# ── rendering ───────────────────────────────────────────────────────────────

def test_dated_prefix_only_when_meta_present():
    dated = MemRecord(id="a", text="alpha", meta={"date_str": "20 July, 2023"})
    plain = MemRecord(id="b", text="beta")
    block, truncated, delivered = build_evidence([dated, plain], 1000)
    assert block == "[1] (20 July, 2023) alpha\n[2] beta"
    assert not truncated and delivered == ["a", "b"]


def test_undated_rendering_is_unchanged():
    """No date_str anywhere -> exactly the old evidence block."""
    recs = [MemRecord(id=f"i{i}", text=f"text {i}") for i in range(3)]
    block, _, _ = build_evidence(recs, 1000)
    assert block == "[1] text 0\n[2] text 1\n[3] text 2"


def test_render_never_mutates_record_text():
    rec = MemRecord(id="a", text="alpha", meta={"date_str": "1 Jan, 2020"})
    before = rec.text.encode("utf-8")
    build_evidence([rec], 1000)
    assert rec.text.encode("utf-8") == before


def test_date_string_counts_against_the_budget():
    """The dated prefix is charged like any other prefix -- no free bytes.

    Same record, same budget: it fits whole undated and is cut once the date is
    there, so the date provably consumes evidence budget.
    """
    budget = 20                                  # 80 chars
    text = "y" * 50

    block, truncated, delivered = build_evidence([MemRecord(id="a", text=text)],
                                                 budget)
    assert block == "[1] " + text
    assert not truncated and delivered == ["a"]

    dated = MemRecord(id="a", text=text, meta={"date_str": "D" * 40})
    block2, truncated2, delivered2 = build_evidence([dated], budget)
    assert truncated2 and delivered2 == ["a"]
    assert block2.startswith("[1] (" + "D" * 40 + ") ")
    assert block2.endswith(TRUNCATION_MARKER)


def test_default_prompt_is_a_usable_template():
    """The old undated wording is NOT kept in source: the evidence-v1 git tag
    holds it and every archived row carries prompt_sha f152adc50791."""
    assert prompt_sha(DEFAULT_PROMPT) != "f152adc50791"
    assert "{evidence}" in DEFAULT_PROMPT and "{question}" in DEFAULT_PROMPT


def test_context_carries_derived_render_sha():
    ctx = build_context(Question(id="q", text="t?"), [], token_budget=100)
    assert ctx.render_sha == render_sha(DEFAULT_PROMPT) != ""


def test_render_sha_tracks_both_template_and_block_shape():
    """It must move when EITHER half of the contract moves."""
    assert render_sha(DEFAULT_PROMPT) != render_sha(DEFAULT_PROMPT + " ")

    import forgetit.llm.readers.text_reader as tr
    base = render_sha(DEFAULT_PROMPT)
    original = tr.TRUNCATION_MARKER
    real_build = tr.build_evidence
    try:                                    # perturb the block shape only
        tr.build_evidence = lambda recs, budget: ("[1] X", False, ["a"])
        assert render_sha(DEFAULT_PROMPT) != base
    finally:
        tr.build_evidence = real_build
        tr.TRUNCATION_MARKER = original
    assert render_sha(DEFAULT_PROMPT) == base


# ── id -> date resolution ───────────────────────────────────────────────────

@needs_data
def test_locomo_pinned_id():
    dm = DateMap.for_workload("locomo", DATA)
    assert dm.lookup("conv-42::pair::28::0") == "5:54 pm on 9 November, 2022"
    assert dm.lookup("conv-42::session::28") == "5:54 pm on 9 November, 2022"
    assert dm.lookup("nonsense") is None


@needs_data
def test_beam_pinned_id_all_granularities():
    dm = DateMap.for_workload("beam", DATA)
    for item_id in ("1::pair::0::0", "1::turn::0::0", "1::batch::0"):
        assert dm.lookup(item_id) == "January-05-2024", item_id


@needs_data
def test_longmemeval_is_example_scoped():
    """Session ids repeat across examples with DIFFERENT dates (20.5% of them),
    so an unscoped lookup must never guess."""
    dm = DateMap.for_workload("longmemeval", DATA)
    assert dm.lookup("answer_d00ba6d0_1::pair::0",
                     "gpt4_59149c77") == "2023/01/08 (Sun) 12:49"
    scoped, unambiguous = longmemeval_dates_by_example(LME)
    ambiguous = {s for (_q, s) in scoped} - set(unambiguous)
    assert ambiguous, "expected ambiguous sids in this dataset"
    sid = sorted(ambiguous)[0]
    assert dm.lookup(f"{sid}::pair::0") is None          # unscoped -> no guess
    qid = next(q for (q, s) in scoped if s == sid)
    assert dm.lookup(f"{sid}::pair::0", qid) == scoped[(qid, sid)]


@needs_data
def test_strict_global_map_still_detects_the_ambiguity():
    with pytest.raises(ValueError, match="two different dates"):
        longmemeval_dates(LME)


@needs_data
def test_question_dates_present():
    qd = longmemeval_question_dates(LME)
    assert qd["gpt4_59149c77"] == "2023/02/01 (Wed) 10:20"


# ── goldens: the two prompts the audit said were undatable ──────────────────

def _reader_probe():
    spec = importlib.util.spec_from_file_location(
        "reader_probe_mod", REPO / "scripts" / "reader_probe.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def gold_tasks():
    """Rebuild both audited questions from the workload loaders (one parse)."""
    from forgetit.benchmark.workloads.locomo import (
        LocomoTurnPairItemiser, LocomoWorkload)
    from forgetit.benchmark.workloads.longmemeval import (
        LongMemEvalWorkload, TurnPairItemiser)

    def task_for(evs, qid, dataset):
        first: dict[str, str] = {}
        for e in evs:
            if e.type == "insert" and e.item is not None:
                first.setdefault(e.item.id, e.item.text)
        ev = next(e for e in evs
                  if e.type == "query" and e.query and e.query.id == qid)
        return {"config": "", "arm": "gold", "q": {
            "query_id": qid, "dataset": dataset,
            "question_text": ev.query.text, "question_type": "",
            "ref_answer": ev.ref_answer or "",
            "gold": [(g, first.get(g, "")) for g in ev.gold_item_ids]}}

    out = {}
    lq = "conv-42::q::186::single_hop"
    for _x, evs in LocomoWorkload(LOCOMO, LocomoTurnPairItemiser()).iter_examples():
        if any(e.type == "query" and e.query and e.query.id == lq for e in evs):
            out["locomo"] = task_for(evs, lq, "locomo")
            break
    for _x, evs in LongMemEvalWorkload(LME, TurnPairItemiser()).iter_examples():
        scored = [e for e in evs if e.type == "query" and e.query and e.gold_item_ids]
        if scored and scored[-1].query.id == "gpt4_59149c77":
            out["longmemeval"] = task_for(evs, "gpt4_59149c77", "longmemeval")
            break
    return out


@needs_data
def test_golden_locomo_prompt_carries_its_session_date(gold_tasks):
    rp = _reader_probe()
    dm = DateMap.for_workload("locomo", DATA)
    ctx, n_dated = rp.build_arm_context(gold_tasks["locomo"], ReaderConfig(), dm, "")
    assert n_dated == 1
    assert "[1] (5:54 pm on 9 November, 2022) " in ctx.prompt
    assert ctx.render_sha == render_sha(DEFAULT_PROMPT)


@needs_data
def test_golden_longmemeval_temporal_becomes_answerable(gold_tasks):
    """Both visit dates AND the asked-on date now reach the reader; before the
    fix none of the three did, so '7 days' was underivable from the evidence."""
    rp = _reader_probe()
    dm = DateMap.for_workload("longmemeval", DATA)
    qdate = longmemeval_question_dates(LME)["gpt4_59149c77"]
    ctx, n_dated = rp.build_arm_context(
        gold_tasks["longmemeval"], ReaderConfig(), dm, qdate)
    assert n_dated == 2
    assert "[1] (2023/01/08 (Sun) 12:49) " in ctx.prompt
    assert "[2] (2023/01/15 (Sun) 00:27) " in ctx.prompt
    assert "Question: (asked on 2023/02/01 (Wed) 10:20) " in ctx.prompt
