# It is an LLM-facing component that:
# 	•	serialises retrieved records,
# 	•	builds the model input,
# 	•	calls the LLM,
# 	•	parses the answer.
from __future__ import annotations


from typing import Protocol

from forgetit.core.schema import AnswerResult, MemRecord, Question



class Reader(Protocol):
    def answer(
                self,
                question: Question,
                records: list[MemRecord],
                *,
                max_tokens: int = 128
    ) -> AnswerResult:
        ...