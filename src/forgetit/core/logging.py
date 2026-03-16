# core/logging.py

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol


class EventLogger(Protocol):
    """Interface that RetentionManager expects."""
    def event(self, name: str, payload: dict) -> None: ...


@dataclass
class LogEntry:
    seq: int
    timestamp: float
    name: str
    payload: dict[str, Any]


class BenchmarkLogger:
    """
    Counts events, stores full audit trail in memory,
    optionally persists to JSONL on disk.
    """

    def __init__(self, path: str | Path | None = None) -> None:
        self.counts: dict[str, int] = {}
        self.log: list[LogEntry] = []
        self._seq = 0
        self._file = None

        if path is not None:
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            self._file = open(p, "a", encoding="utf-8")

    def event(self, name: str, payload: dict | None = None) -> None:
        payload = payload or {}
        entry = LogEntry(
            seq=self._seq,
            timestamp=time.time(),
            name=name,
            payload=payload,
        )
        self._seq += 1
        self.counts[name] = self.counts.get(name, 0) + 1
        self.log.append(entry)

        if self._file is not None:
            record = {
                "seq": entry.seq,
                "t": entry.timestamp,
                "name": entry.name,
                **entry.payload,
            }
            self._file.write(json.dumps(record) + "\n")
            self._file.flush()

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

class JsonlLogger:
    def __init__(self, path):
        self.path = path

    def event(self, kind: str, payload: dict):
        rec = {"t": time.time(), "kind": kind, **payload}
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")
