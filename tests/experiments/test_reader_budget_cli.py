"""The reader-configuration flags are wired identically in every runner.

The bug this guards, twice over: both the budgeted evidence window (E6c) and
the repaired prompt wrapper (D4 fix iv) were implemented and tested in
text_reader.py, but no runner could reach either — so every answer-stage run
silently took the legacy path. A parsed-but-unused flag is the same failure
wearing a flag, so these tests check the value reaches the reader, not just
that argparse accepts it.
"""
import inspect
import sys

import pytest

from forgetit.experiments import run_beam, run_locomo_qa, run_longmemeval_qa
from forgetit.llm.readers import READER_PROMPT_HELP, READER_TOKEN_BUDGET_HELP

RUNNERS = [run_beam, run_locomo_qa, run_longmemeval_qa]
RUNNER_IDS = [m.__name__.rsplit(".", 1)[-1] for m in RUNNERS]


def parse(monkeypatch, module, argv):
    monkeypatch.setattr(sys, "argv", ["prog", *argv])
    return module.parse_args()


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_budget_parsed(monkeypatch, module):
    assert parse(monkeypatch, module, ["--reader-token-budget", "1500"]
                 ).reader_token_budget == 1500


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_default_is_legacy_mode(monkeypatch, module):
    # Omitted = None = legacy context. Pinned so historical runs reproduce;
    # the runner warns loudly at startup instead of changing under anyone.
    assert parse(monkeypatch, module, []).reader_token_budget is None


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
@pytest.mark.parametrize("argv", [
    ["--reader-token-budget", "0"],
    ["--reader-token-budget", "-1"],
    ["--reader-token-budget", "1500", "--no-reader"],   # contradictory
])
def test_rejected_configs(monkeypatch, module, argv):
    with pytest.raises(SystemExit):
        parse(monkeypatch, module, argv)


def test_help_text_is_shared_verbatim():
    """One help string per flag, imported by all three — they cannot drift."""
    for module in RUNNERS:
        assert module.READER_TOKEN_BUDGET_HELP is READER_TOKEN_BUDGET_HELP
        assert module.READER_PROMPT_HELP is READER_PROMPT_HELP


# ── --reader-prompt (D4 fix iv) ──────────────────────────────────────


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_prompt_version_parsed(monkeypatch, module):
    assert parse(monkeypatch, module, ["--reader-prompt", "v2"]).reader_prompt == "v2"


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_prompt_default_is_legacy_v1(monkeypatch, module):
    # A no-flag run must stay byte-identical to historical prompts.
    assert parse(monkeypatch, module, []).reader_prompt == "v1"


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_branch_era_flag_name_still_works(monkeypatch, module):
    # `--reader-prompt-version` is what the verification-sprint branch called
    # this flag; commands written against it must not die on "unrecognized".
    assert parse(monkeypatch, module,
                 ["--reader-prompt-version", "v2"]).reader_prompt == "v2"


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
@pytest.mark.parametrize("argv", [
    ["--reader-prompt", "v3"],                  # unknown version
    ["--reader-prompt", "v2", "--no-reader"],   # contradictory
])
def test_rejected_prompt_configs(monkeypatch, module, argv):
    with pytest.raises(SystemExit):
        parse(monkeypatch, module, argv)


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_help_renders(monkeypatch, module, capsys):
    """--help must actually print. argparse %-formats help strings, so a bare
    '%' anywhere in a help text raises TypeError and takes the whole CLI down —
    a failure no other test reaches, since nothing else formats the parser."""
    with pytest.raises(SystemExit) as exc:
        parse(monkeypatch, module, ["--help"])
    assert exc.value.code == 0
    assert "--reader-token-budget" in capsys.readouterr().out


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_main_passes_the_budget_to_the_reader(module):
    """A parsed-but-unused flag is exactly the failure mode being fixed, and
    main() can't be called in a test (it runs a whole sweep) — so assert on the
    wiring itself: the budget reaches the reader, and the mode gets logged."""
    src = inspect.getsource(module.main)
    assert "token_budget=args.reader_token_budget" in src
    assert "prompt_version=args.reader_prompt" in src
    assert "log_reader_context(log, args.reader_token_budget)" in src
    assert "log_reader_prompt(log, args.reader_prompt)" in src
