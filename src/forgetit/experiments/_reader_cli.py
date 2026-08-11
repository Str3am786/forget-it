"""Reader flags for the experiment runners. One definition, three callers."""
from __future__ import annotations

import argparse

from forgetit.llm.readers import (
    DEFAULT_ANSWER_MAX_TOKENS, DEFAULT_TOKEN_BUDGET, ReaderConfig,
    resolve_prompt,
)


def add_reader_args(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("reader")
    g.add_argument("--reader-token-budget", type=int,
                   default=DEFAULT_TOKEN_BUDGET, metavar="N",
                   help="total token budget for the evidence block (default "
                        f"{DEFAULT_TOKEN_BUDGET}); items fill whole in rank "
                        "order, the first that does not fit is cut with an "
                        "explicit marker")
    g.add_argument("--answer-max-tokens", type=int,
                   default=DEFAULT_ANSWER_MAX_TOKENS, metavar="N",
                   help="generation cap for the answer (default "
                        f"{DEFAULT_ANSWER_MAX_TOKENS}); cap_hit records when "
                        "it bound")
    g.add_argument("--reader-prompt", type=str, default=None, metavar="TMPL",
                   help="prompt template: a file path or an inline string "
                        "containing {evidence} and {question}; the template's "
                        "sha lands in provenance either way")
    # An ARTEFACT switch, not reader behaviour: it changes what is written, so
    # it never reaches ReaderConfig, the reader name or prompt_sha.
    g.add_argument("--dump-retrieval", action="store_true",
                   help="write retrieved_index.jsonl per config and "
                        "evidence_items.shard<N>.jsonl at the output root: what "
                        "each scored query retrieved, in rank order. Costs no "
                        "inference and works under --no-reader, so one policy "
                        "run can afterwards be read by any number of models.")


def reader_config_from_args(args: argparse.Namespace) -> ReaderConfig:
    """Namespace -> ReaderConfig. ValueError from __post_init__ is the only
    validation path, so CLI and programmatic construction cannot diverge."""
    return ReaderConfig(token_budget=args.reader_token_budget,
                        max_tokens=args.answer_max_tokens,
                        prompt=resolve_prompt(args.reader_prompt))


_FIELD_FLAGS = {"token_budget": "--reader-token-budget",
                "max_tokens": "--answer-max-tokens"}


def validate_reader_args(p: argparse.ArgumentParser,
                         args: argparse.Namespace) -> None:
    """Run ReaderConfig's own validation at parse time, as a CLI error.

    Construction stays the single validation path; this only translates the
    field name in the exception into the flag the user actually typed. Reader
    flags passed together with --no-reader are inert, not an error: the old
    guard rejected the DEFAULT prompt under --no-reader and so blocked every
    retrieval-only run.
    """
    try:
        reader_config_from_args(args)
    except ValueError as exc:
        field, _, rest = str(exc).partition(" ")
        flag = _FIELD_FLAGS.get(field)
        p.error(f"{flag} {rest}" if flag else str(exc))
