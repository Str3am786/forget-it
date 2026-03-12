from __future__ import annotations

from typing import Iterable

from forgetit.memory_system import RetentionManager
from forgetit.core.schema import MemRecord, Session, Question, Query, AnswerResult
from forgetit.llm.readers import Reader


class MemoryQAService:
    def __init__(self, *, retention_manager: RetentionManager, reader: Reader):
        self.store = retention_manager
        self.reader = reader
        self.store.connect()

    def close(self) -> None:
        self.store.close()

    def session_to_record(self, session: Session) -> MemRecord:
        text = "\n".join(f"{t.role}: {t.content}" for t in session.turns)
        return MemRecord(
            id=session.id,
            text=text,
            created_at=session.created_at,
            last_access=session.created_at,
            meta=session.meta.copy(),
        )

    def add_session(self, session: Session) -> MemRecord:
        rec = self.session_to_record(session)
        self.store.insert(rec)
        return rec

    def add_sessions(self, sessions: Iterable[Session]) -> None:
        for s in sessions:
            self.add_session(s)

    def retrieve(self, question: Question, k: int) -> list[MemRecord]:
        query = Query(
            id=question.id,
            text=question.text,
            timestamp=question.timestamp,
            meta=question.meta.copy(),
        )
        return self.store.retrieve(query, k=k)

    def answer(self, question: Question, k: int = 5, max_tokens: int = 128) -> AnswerResult:
        records = self.retrieve(question, k=k)
        return self.reader.answer(question, records, max_tokens=max_tokens)