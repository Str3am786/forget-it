import json
import time

from typing import TYPE_CHECKING
from forgetit.llm.base import LLM
from forgetit.core import MemRecord, Question, AnswerResult

if TYPE_CHECKING:
    from forgetit.llm.base import LLM
    
class JsonMemoryReader:
    def __init__(self, llm: LLM) -> None:
        self.llm = llm

    def _serialise_records(self, records: list[MemRecord]) -> list[dict]:
        return [
            {
                "id": r.id,
                "created_at": r.created_at,
                "text": r.text,
                "meta": {
                    "compressed": r.meta.get("compressed", False),
                    "source_date": r.meta.get("source_date"),
                    "question_type": r.meta.get("question_type"),
                    "session_index": r.meta.get("session_index"),
                },
            }
            for r in records
        ]

    def _build_prompt(self, question: Question, records: list[MemRecord]) -> str:
        payload = {
            "question_id": question.id,
            "question": question.text,
            "instructions": [
                "Use only the provided retrieved memories.",
                "Prefer more recent evidence when memories conflict.",
                "If the answer is not supported by the memories, answer exactly: I do not know.",
                "Keep the answer short and precise.",
            ],
            "retrieved_memories": self._serialise_records(records),
        }
        return (
            "You are a memory reader.\n"
            "Read the JSON object below and answer the question using only the retrieved memories.\n\n"
            f"{json.dumps(payload, ensure_ascii=False, indent=2)}\n\n"
            "Return only the answer text."
        )

    def _is_abstention(self, text: str) -> bool:
        t = text.strip().lower()
        return t in {
            "i do not know",
            "i do not know.",
            "i don't know",
            "i don't know.",
        }

    def answer(
        self,
        question: Question,
        records: list[MemRecord],
        *,
        max_tokens: int = 128,
    ) -> AnswerResult:
        prompt = self._build_prompt(question, records)

        t0 = time.perf_counter()
        raw = self.llm.generate(prompt, max_tokens=max_tokens, temperature=0.0).strip()
        latency = time.perf_counter() - t0

        return AnswerResult(
            query_id=question.id,
            answer=raw,
            abstained=self._is_abstention(raw),
            latency_sec=latency,
            meta={
                "raw_output": raw,
                "prompt_text": prompt,
            },
        )