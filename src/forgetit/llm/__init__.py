from .base import LLM
from .openai_llm import OpenAiLLM
from .readers import Reader, JsonMemoryReader

from forgetit.settings import LLM_API_KEY, LLM_BASE_URL, LLM_MODEL

def get_llm() -> OpenAiLLM:
    return OpenAiLLM(
        base_url=LLM_BASE_URL,
        api_key=LLM_API_KEY,
        model=LLM_MODEL
    )

__all__ = [
    "LLM",
    "OpenAiLLM",
    "get_llm",
    "Reader",
    "JsonMemoryReader",
]