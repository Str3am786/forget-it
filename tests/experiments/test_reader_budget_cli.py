"""--reader-token-budget is wired identically in every runner.

The bug this guards: the budgeted reader existed but no runner could reach it,
so every answer-stage run silently used the legacy context (5 items, each cut
to 300 chars ≈ 375 tokens ≈ ONE retrieved item) and abstained on gold that
retrieval had already delivered. Two things must stay true — the flag exists
with the same semantics in all three runners, and the mode reaches the reader.
"""
import inspect
import sys

import pytest

from forgetit.experiments import run_beam, run_locomo_qa, run_longmemeval_qa
from forgetit.llm.readers import READER_TOKEN_BUDGET_HELP

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
    """One help string, imported by all three — the flag cannot drift apart."""
    for module in RUNNERS:
        assert module.READER_TOKEN_BUDGET_HELP is READER_TOKEN_BUDGET_HELP


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
    assert "PlainTextReader(llm, token_budget=args.reader_token_budget)" in src
    assert "log_reader_context(log, args.reader_token_budget)" in src
