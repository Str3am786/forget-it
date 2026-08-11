"""The reader-configuration flags are wired identically in every runner.

The bug this guards: the budgeted evidence window was implemented and tested
in text_reader.py, but no runner could reach it — so every answer-stage run
silently took the unbudgeted path. A parsed-but-unused flag is the same failure
wearing a flag, so these tests check the value reaches the reader, not just
that argparse accepts it.

The flags now come from ONE definition (experiments/_reader_cli.py) that all
three runners call, so drift between runners is no longer possible to write.
"""
import inspect
import sys

import pytest

from forgetit.experiments import run_beam, run_locomo_qa, run_longmemeval_qa
from forgetit.experiments._reader_cli import reader_config_from_args
from forgetit.llm.readers import (
    DEFAULT_ANSWER_MAX_TOKENS, DEFAULT_PROMPT, DEFAULT_TOKEN_BUDGET,
)

RUNNERS = [run_beam, run_locomo_qa, run_longmemeval_qa]
RUNNER_IDS = [m.__name__.rsplit(".", 1)[-1] for m in RUNNERS]


def parse(monkeypatch, module, argv):
    monkeypatch.setattr(sys, "argv", ["prog", *argv])
    return module.parse_args()


# ── parsing ──────────────────────────────────────────────────────────


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_budget_parsed(monkeypatch, module):
    assert parse(monkeypatch, module, ["--reader-token-budget", "1500"]
                 ).reader_token_budget == 1500


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_defaults_are_the_measured_values(monkeypatch, module):
    """Omitted flags give the measured configuration, not a legacy one. The
    default IS the answer — anything else hides the good setting behind a
    launcher override."""
    args = parse(monkeypatch, module, [])
    assert args.reader_token_budget == DEFAULT_TOKEN_BUDGET
    assert args.answer_max_tokens == DEFAULT_ANSWER_MAX_TOKENS
    assert args.reader_prompt is None


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_answer_max_tokens_parsed(monkeypatch, module):
    assert parse(monkeypatch, module, ["--answer-max-tokens", "512"]
                 ).answer_max_tokens == 512


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
@pytest.mark.parametrize("argv", [
    ["--reader-token-budget", "0"],
    ["--reader-token-budget", "-1"],
    ["--answer-max-tokens", "0"],
    ["--answer-max-tokens", "-5"],
])
def test_nonpositive_is_rejected(monkeypatch, module, argv):
    with pytest.raises(SystemExit):
        parse(monkeypatch, module, argv)


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
@pytest.mark.parametrize("argv", [
    [],
    ["--no-reader"],
    ["--no-reader", "--reader-token-budget", "1500"],
    ["--no-reader", "--answer-max-tokens", "512"],
    ["--no-reader", "--reader-prompt", "custom {evidence} {question}"],
])
def test_reader_flags_are_inert_with_no_reader_not_an_error(monkeypatch, module, argv):
    """The old guard rejected the DEFAULT prompt under --no-reader, which made
    every retrieval-only run unlaunchable. A flag for a stage that is switched
    off is unused, not contradictory."""
    parse(monkeypatch, module, argv)


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
@pytest.mark.parametrize("flag", ["--reader-clean", "--reader-abstention",
                                  "--reader-prompt-version"])
def test_deleted_switch_flags_are_gone(monkeypatch, module, flag):
    """These selected between a measured-worse and a measured-better behaviour.
    Only the better one exists now, so the flags must not be resurrectable."""
    with pytest.raises(SystemExit):
        parse(monkeypatch, module, [flag, "full"])


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_help_renders(monkeypatch, module, capsys):
    with pytest.raises(SystemExit):
        parse(monkeypatch, module, ["--help"])
    out = capsys.readouterr().out
    assert "--reader-token-budget" in out
    assert "--answer-max-tokens" in out
    assert "--reader-prompt" in out


# ── the flags actually reach the reader ──────────────────────────────


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_main_builds_the_reader_from_the_parsed_args(module):
    """main() can't be called in a test (it runs a whole sweep), so assert on
    the wiring: one shared definition in, one shared builder out."""
    src = inspect.getsource(module.main)
    assert "reader_cfg = reader_config_from_args(args)" in src
    assert "build_reader(llm, reader_cfg)" in src
    src_parse = inspect.getsource(module.parse_args)
    assert "add_reader_args(p)" in src_parse
    assert "validate_reader_args(p, args)" in src_parse


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_reader_config_is_built_even_under_no_reader(module):
    """--dump-retrieval records the budget and template a reader WOULD have
    used, so the config must be built outside the --no-reader branch. If it
    moved back inside, a retrieval-only dump would have no provenance to write."""
    src = inspect.getsource(module.main)
    cfg_at = src.index("reader_cfg = reader_config_from_args(args)")
    guard_at = src.index("if args.no_reader:")
    assert cfg_at < guard_at, "reader config must be built before the no-reader guard"
    assert "retrieval_dump=retrieval_dump" in src


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_parsed_args_become_the_reader_config(monkeypatch, module):
    args = parse(monkeypatch, module,
                 ["--reader-token-budget", "999", "--answer-max-tokens", "77"])
    cfg = reader_config_from_args(args)
    assert cfg.token_budget == 999
    assert cfg.max_tokens == 77
    assert cfg.prompt == DEFAULT_PROMPT


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_inline_prompt_template_reaches_the_config(monkeypatch, module):
    tmpl = "Custom.\n{evidence}\nQ: {question}"
    args = parse(monkeypatch, module, ["--reader-prompt", tmpl])
    assert reader_config_from_args(args).prompt == tmpl


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_prompt_file_is_read_from_disk(monkeypatch, module, tmp_path):
    f = tmp_path / "wording.txt"
    f.write_text("From file.\n{evidence}\n{question}", encoding="utf-8")
    args = parse(monkeypatch, module, ["--reader-prompt", str(f)])
    assert reader_config_from_args(args).prompt == f.read_text(encoding="utf-8")


# ── the cap lives in the reader, and therefore in its name ───────────


def test_cap_is_in_the_reader_name_and_no_longer_a_separate_hash_field():
    """Inverted on purpose. The cap used to be threaded to run_pipeline and
    into the config hash while the reader name stayed blind to it — so the
    pipeline's value won at call time and the `reader` column could name a cap
    the run never used. Now the reader owns it, and the name carries it."""
    from forgetit.benchmark.pipeline import _config_hash
    from forgetit.llm.readers import PlainTextReader

    class _NoLLM:
        model = "m"

        def generate(self, *a, **k):
            raise AssertionError

    assert PlainTextReader(_NoLLM(), max_tokens=128).name != \
           PlainTextReader(_NoLLM(), max_tokens=256).name
    assert PlainTextReader(_NoLLM(), max_tokens=128).name.endswith("_gen128")

    # The cap enters run identity through `reader`, not as its own field.
    base = dict(dataset="d", itemiser="i", store_label="s", k=5, seed=0,
                budget_bytes=1, budget_frac=1.0, n_examples=1, write_events=False)
    assert _config_hash(**base, reader="reader_m_rtok16000_gen128") != \
           _config_hash(**base, reader="reader_m_rtok16000_gen256")


def test_run_reader_does_not_override_the_readers_cap():
    """_run_reader must not pass max_tokens: a caller-supplied override is what
    let the name and the sent cap disagree."""
    from forgetit.benchmark.runner_core import _run_reader
    src = inspect.getsource(_run_reader)
    assert "max_tokens=" not in src
    assert "answer_max_tokens" not in inspect.signature(_run_reader).parameters
