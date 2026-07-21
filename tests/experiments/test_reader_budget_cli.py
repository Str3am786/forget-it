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
from forgetit.llm.readers import (
    ANSWER_MAX_TOKENS_HELP,
    READER_ABSTENTION_HELP,
    READER_CLEAN_HELP,
    READER_PROMPT_HELP,
    READER_TOKEN_BUDGET_HELP,
)

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
        assert module.READER_CLEAN_HELP is READER_CLEAN_HELP
        assert module.READER_ABSTENTION_HELP is READER_ABSTENTION_HELP
        assert module.ANSWER_MAX_TOKENS_HELP is ANSWER_MAX_TOKENS_HELP


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


# ── --reader-clean (D4 fix ii) ───────────────────────────────────────


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_clean_mode_parsed(monkeypatch, module):
    assert parse(monkeypatch, module, ["--reader-clean", "full"]).reader_clean == "full"


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_clean_default_is_legacy_firstline(monkeypatch, module):
    assert parse(monkeypatch, module, []).reader_clean == "firstline"


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_branch_era_clean_flag_name_still_works(monkeypatch, module):
    # `--clean-mode` is the verification-sprint branch's name for this flag.
    assert parse(monkeypatch, module, ["--clean-mode", "full"]).reader_clean == "full"


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
@pytest.mark.parametrize("argv", [
    ["--reader-clean", "partial"],              # unknown mode
    ["--reader-clean", "full", "--no-reader"],  # contradictory
])
def test_rejected_clean_configs(monkeypatch, module, argv):
    with pytest.raises(SystemExit):
        parse(monkeypatch, module, argv)


# ── --reader-abstention (D4 fix vii, detection half) ─────────────────


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_abstention_mode_parsed(monkeypatch, module):
    assert parse(monkeypatch, module,
                 ["--reader-abstention", "exact"]).reader_abstention == "exact"


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_abstention_default_is_legacy_loose(monkeypatch, module):
    assert parse(monkeypatch, module, []).reader_abstention == "loose"


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_branch_era_abstention_flag_name_still_works(monkeypatch, module):
    # `--abstention-match` is the verification-sprint branch's name.
    assert parse(monkeypatch, module,
                 ["--abstention-match", "exact"]).reader_abstention == "exact"


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
@pytest.mark.parametrize("argv", [
    ["--reader-abstention", "fuzzy"],               # unknown mode
    ["--reader-abstention", "exact", "--no-reader"],  # contradictory
])
def test_rejected_abstention_configs(monkeypatch, module, argv):
    with pytest.raises(SystemExit):
        parse(monkeypatch, module, argv)


# ── --answer-max-tokens (D4 fix vii, cap half) ───────────────────────


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_answer_max_tokens_parsed(monkeypatch, module):
    assert parse(monkeypatch, module,
                 ["--answer-max-tokens", "256"]).answer_max_tokens == 256


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_answer_max_tokens_default_is_legacy_128(monkeypatch, module):
    assert parse(monkeypatch, module, []).answer_max_tokens == 128


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
@pytest.mark.parametrize("argv", [
    ["--answer-max-tokens", "0"],
    ["--answer-max-tokens", "-8"],
    ["--answer-max-tokens", "256", "--no-reader"],   # contradictory
])
def test_rejected_answer_max_tokens_configs(monkeypatch, module, argv):
    with pytest.raises(SystemExit):
        parse(monkeypatch, module, argv)


@pytest.mark.parametrize("module", RUNNERS, ids=RUNNER_IDS)
def test_cap_reaches_the_pipeline_not_the_reader_name(module):
    """The cap is threaded to run_pipeline, NOT into the reader label — it has
    always lived in the config hash instead. Pinned so nobody 'fixes' the
    asymmetry and silently renames every artifact directory."""
    src = inspect.getsource(module.main)
    assert "answer_max_tokens=args.answer_max_tokens" in src
    assert "answer_max_tokens=128" not in src          # literal really gone
    assert "log_answer_max_tokens(log, args.answer_max_tokens)" in src


def test_cap_is_a_config_hash_field_and_not_in_the_reader_name():
    from forgetit.benchmark.pipeline import _config_hash
    from forgetit.llm.readers import PlainTextReader

    class _NoLLM:
        def generate(self, *a, **k):
            raise AssertionError

    base = dict(dataset="d", itemiser="i", store_label="s", reader="r",
                k=5, seed=0, budget_bytes=1, budget_frac=1.0,
                n_examples=1, write_events=False)
    assert _config_hash(**base, answer_max_tokens=128) != \
           _config_hash(**base, answer_max_tokens=256)
    # ...and the reader name is blind to it, by design.
    assert PlainTextReader(_NoLLM(), max_tokens=128).name == \
           PlainTextReader(_NoLLM(), max_tokens=256).name == "PlainTextReader"


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
    assert "clean_mode=args.reader_clean" in src
    assert "log_reader_clean(log, args.reader_clean)" in src
    assert "abstention_match=args.reader_abstention" in src
    assert "log_reader_abstention(log, args.reader_abstention)" in src
