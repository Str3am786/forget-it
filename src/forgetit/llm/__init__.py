import os

from .base import LLM
from .openai_llm import OpenAiLLM
from .readers import Reader, JsonMemoryReader, PlainTextReader

from forgetit.settings import LLM_API_KEY, LLM_BASE_URL, LLM_MODEL

def get_llm() -> OpenAiLLM:
    # Validate at the point of use (not import time): --no-reader / dense-only
    # runs never call get_llm and shouldn't be forced to set LLM_MODEL, but when
    # we DO build the chat client, model=None would only fail later as a cryptic
    # API error — so fail fast here with a clear message.
    if not LLM_MODEL:
        raise RuntimeError("LLM_MODEL is unset (expected in .env or env). It is "
                           "required to build the chat LLM; set it or run --no-reader.")
    # LLM_MAX_CONNECTIONS lets the launcher size the httpx pool to the concurrent
    # in-flight count (≈ example-workers, and reader-workers in Pass 2). Unset →
    # SDK default (100), which is plenty until you tune workers past ~100.
    mc = os.environ.get("LLM_MAX_CONNECTIONS")
    return OpenAiLLM(
        base_url=LLM_BASE_URL,
        api_key=LLM_API_KEY,
        model=LLM_MODEL,
        max_connections=int(mc) if mc and mc.isdigit() else None,
    )

__all__ = [
    "LLM",
    "OpenAiLLM",
    "get_llm",
    "Reader",
    "JsonMemoryReader",
    "PlainTextReader"
]