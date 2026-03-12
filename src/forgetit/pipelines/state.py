from __future__ import annotations

from typing import TypedDict, Optional

from forgetit.core.schema import Session, Question, MemRecord, AnswerResult
from forgetit.services.memory_qa_service import MemoryQAService


class IngestState(TypedDict, total=False):
    service: MemoryQAService
    session: Session
    inserted_record: MemRecord
    error: Optional[str]


class RetrievalState(TypedDict, total=False):
    service: MemoryQAService
    question: Question
    k: int
    records: list[MemRecord]
    error: Optional[str]


class QAState(TypedDict, total=False):
    service: MemoryQAService
    question: Question
    k: int
    max_tokens: int
    result: AnswerResult
    error: Optional[str]