"""The reader's configuration surface. No CLI, no argparse.

Everything the reader can be told lives here, so discovering the knobs means
reading one small file. Validation is in __post_init__, which means it holds
for programmatic construction (probes, sweeps, tests) and not only for the
command line.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .text_reader import (
    DEFAULT_ANSWER_MAX_TOKENS,
    DEFAULT_PROMPT,
    DEFAULT_TOKEN_BUDGET,
    PlainTextReader,
)


@dataclass(frozen=True)
class ReaderConfig:
    token_budget: int = DEFAULT_TOKEN_BUDGET
    max_tokens: int = DEFAULT_ANSWER_MAX_TOKENS
    prompt: str = DEFAULT_PROMPT

    def __post_init__(self) -> None:
        if self.token_budget <= 0:
            raise ValueError("token_budget must be > 0")
        if self.max_tokens <= 0:
            raise ValueError("max_tokens must be > 0")


def resolve_prompt(value: str | None) -> str:
    """A file path if it points at one, otherwise an inline template.

    Lets a sweep keep its wordings as files under version control without the
    reader knowing anything about paths.
    """
    if value is None:
        return DEFAULT_PROMPT
    p = Path(value)
    return p.read_text(encoding="utf-8") if p.is_file() else value


def build_reader(llm, cfg: ReaderConfig | None = None) -> PlainTextReader:
    cfg = cfg or ReaderConfig()
    return PlainTextReader(llm, token_budget=cfg.token_budget,
                           max_tokens=cfg.max_tokens, prompt=cfg.prompt)
