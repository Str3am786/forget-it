"""Embedder abstraction — a sibling of :class:`forgetit.llm.base.LLM`.

An Embedder turns text into dense vectors.  It is deliberately provider-agnostic:
the same interface covers a local Ollama/vLLM server, OpenAI, or an in-process
model.  Everything downstream (ingestion-time vectors, in-store redundancy,
gold/noise separability, a future value-scorer) depends only on this interface,
so the provider can be swapped by changing one factory.

Contract:
  * ``embed(texts)`` returns one vector per input, in input order, length-aligned.
  * Vectors are plain ``list[float]`` so they round-trip through ``MemRecord``
    and the backends unchanged.
  * Implementations SHOULD batch internally; callers may pass thousands of texts.
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class Embedder(ABC):
    #: stable identifier of the underlying model — used as part of the cache key
    #: so vectors from different models never collide.
    model: str
    #: embedding dimensionality once known (None until the first call resolves it).
    dim: int | None = None

    @abstractmethod
    def embed(self, texts: list[str]) -> list[list[float]]:
        """Return one vector per text, in the same order as the input."""
        ...

    def embed_one(self, text: str) -> list[float]:
        return self.embed([text])[0]

    @property
    def name(self) -> str:
        """Cache-namespacing identity. Override if a provider needs more than
        the model id to disambiguate (e.g. a normalisation flag)."""
        return self.model
