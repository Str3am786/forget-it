from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set
import time
import uuid 

@dataclass
class MemRecord:
    id: str
    text: str
    embedding: Optional[list[float]] = None
    created_at: float = field(default_factory=lambda: time.time())
    last_access: float = field(default_factory=lambda: 0.0)
    access_count: int = 0
    bytes: int = 0
    meta: Dict[str, Any] = field(default_factory=dict)

    # optional graph hooks
    neighbors: Set[str] = field(default_factory=set)

@dataclass(frozen=True)
class MemFeatures:
    """
    Lightweight projection of MemRecord used by eviction policies.
    """
    id: str
    bytes: int
    last_access: float
    access_count: int
    created_at: Optional[float] = None

# TODO placeholder 
@dataclass
class Query:
    text: str
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=lambda: time.time())
    embedding: Optional[list[float]] = None
    meta: Dict[str, Any] = field(default_factory=dict)

@dataclass
class SessionTurn:
    role: str
    content: str
    meta: Dict[str, Any] = field(default_factory=dict)
    


@dataclass
class Session:
    id: str
    turns: list[SessionTurn]
    created_at: float = field(default_factory=lambda: time.time())
    meta: Dict[str, Any] = field(default_factory=dict)

@dataclass
class Question:
    id: str
    text: str
    timestamp: float = field(default_factory=lambda: time.time())
    meta: Dict[str, Any] = field(default_factory=dict)

@dataclass
class AnswerResult:
    query_id: str
    answer: str
    abstained: bool
    latency_sec: float
    meta: Dict[str, Any] = field(default_factory=dict)


