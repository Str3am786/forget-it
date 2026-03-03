from __future__ import annotations

import json
import logging
from typing import Iterable, Optional, override

try:
    from falkordb import FalkorDB
except Exception:  # pragma: no cover
    from falkor import FalkorDB

from forgetit.backend.backend import Backend
from forgetit.core.schema import MemFeatures, MemRecord, Query

logger = logging.getLogger(__name__)


class Falkor(Backend):
    def __init__(self, cfg: FalkorConfig):
        self.cfg = cfg
        self._db: Optional[FalkorDB] = None
        self._graph: Optional[Graph] = None

    @property
    @override
    def is_persistent(self) -> bool:
        return True

    def _ensure_connected(self) -> None:
        if self._graph is None:
            raise RuntimeError("Not connected.")

    def _rows(self, res) -> list:
        return getattr(res, "result_set", None) or []

    @override
    def connect(self) -> None:
        if self._db is not None or self._graph is not None:
            self.close()

        try:
            self._db = FalkorDB(
                host=self.cfg.host,
                port=self.cfg.port,
                username=self.cfg.username,
                password=self.cfg.password,
                socket_connect_timeout=getattr(self.cfg, "socket_connect_timeout", 2),
                socket_timeout=getattr(self.cfg, "socket_timeout", 2),
            )

            self._db.connection.ping()
            self._graph = self._db.select_graph(self.cfg.graph)
            self._graph.query("RETURN 1")

            logger.info(
                "Connected to FalkorDB host=%s port=%s graph=%s",
                self.cfg.host,
                self.cfg.port,
                self.cfg.graph,
            )

        except Exception:
            self._db = None
            self._graph = None
            logger.exception(
                "Failed to connect to FalkorDB host=%s port=%s graph=%s",
                self.cfg.host,
                self.cfg.port,
                self.cfg.graph,
            )
            raise

    @override
    def close(self) -> None:
        if self._db is not None:
            try:
                pool = getattr(self._db.connection, "connection_pool", None)
                if pool is not None:
                    pool.disconnect()
            finally:
                self._db = None
                self._graph = None

    @override
    def upsert(self, item: MemRecord) -> None:
        self._ensure_connected()

        if item.bytes is None or item.bytes <= 0:
            raise ValueError("MemRecord.bytes must be > 0 before upsert to FalkorDB.")

        meta_json = json.dumps(item.meta or {}, ensure_ascii=False)

        q = """
        MERGE (m:Mem {id: $id})
        SET
          m.text = $text,
          m.embedding = $embedding,
          m.created_at = $created_at,
          m.last_access = $last_access,
          m.access_count = $access_count,
          m.bytes = $bytes,
          m.meta_json = $meta_json
        """
        params = {
            "id": item.id,
            "text": item.text,
            "embedding": item.embedding if item.embedding is not None else [],
            "created_at": float(item.created_at),
            "last_access": float(item.last_access),
            "access_count": int(item.access_count),
            "bytes": int(item.bytes),
            "meta_json": meta_json,
        }
        self._graph.query(q, params)

    @override
    def get(self, item_id: str) -> Optional[MemRecord]:
        self._ensure_connected()

        q = """
        MATCH (m:Mem {id: $id})
        RETURN
          m.id AS id,
          m.text AS text,
          m.embedding AS embedding,
          m.created_at AS created_at,
          m.last_access AS last_access,
          m.access_count AS access_count,
          m.bytes AS bytes,
          m.meta_json AS meta_json
        LIMIT 1
        """
        res = self._graph.query(q, {"id": item_id})
        rows = self._rows(res)
        if not rows:
            return None

        r = rows[0]
        meta_json = r[7] if len(r) > 7 and r[7] is not None else "{}"
        try:
            meta = json.loads(meta_json)
            if not isinstance(meta, dict):
                meta = {}
        except Exception:
            meta = {}

        emb = r[2]
        embedding = list(emb) if emb is not None and emb != [] else None

        return MemRecord(
            id=str(r[0]),
            text=str(r[1]) if r[1] is not None else "",
            embedding=embedding,
            created_at=float(r[3]) if r[3] is not None else 0.0,
            last_access=float(r[4]) if r[4] is not None else 0.0,
            access_count=int(r[5]) if r[5] is not None else 0,
            bytes=int(r[6]) if r[6] is not None else 0,
            meta=meta,
        )

    @override
    def delete(self, item_id: str) -> None:
        self._ensure_connected()

        q = """
        MATCH (m:Mem {id: $id})
        DETACH DELETE m
        """
        self._graph.query(q, {"id": item_id})

    @override
    def iter_ids(self) -> Iterable[str]:
        self._ensure_connected()

        q = """
        MATCH (m:Mem)
        RETURN m.id AS id
        """
        res = self._graph.query(q)
        for row in self._rows(res):
            if row and row[0] is not None:
                yield str(row[0])

    @override
    def scan_accounting(self) -> list[tuple[str, int]]:
        self._ensure_connected()

        q = """
        MATCH (m:Mem)
        RETURN m.id AS id, m.bytes AS bytes
        """
        res = self._graph.query(q)
        out: list[tuple[str, int]] = []
        for row in self._rows(res):
            item_id = str(row[0])
            b = int(row[1]) if len(row) > 1 and row[1] is not None else 0
            out.append((item_id, b))
        return out

    @override
    def scan_features(self) -> Iterable[MemFeatures]:
        self._ensure_connected()

        q = """
        MATCH (m:Mem)
        RETURN
          m.id AS id,
          m.bytes AS bytes,
          m.last_access AS last_access,
          m.access_count AS access_count,
          m.created_at AS created_at
        """
        res = self._graph.query(q)
        for row in self._rows(res):
            yield MemFeatures(
                id=str(row[0]),
                bytes=int(row[1]) if row[1] is not None else 0,
                last_access=float(row[2]) if row[2] is not None else 0.0,
                access_count=int(row[3]) if row[3] is not None else 0,
                created_at=float(row[4]) if row[4] is not None else None,
            )

    @override
    def search(self, query: Query, k: int) -> list[MemRecord]:
        self._ensure_connected()

        q = """
        MATCH (m:Mem)
        WHERE toLower(m.text) CONTAINS toLower($q)
        RETURN
          m.id AS id,
          m.text AS text,
          m.embedding AS embedding,
          m.created_at AS created_at,
          m.last_access AS last_access,
          m.access_count AS access_count,
          m.bytes AS bytes,
          m.meta_json AS meta_json
        LIMIT $k
        """
        res = self._graph.query(q, {"q": query.text, "k": int(k)})
        rows = self._rows(res)

        out: list[MemRecord] = []
        for r in rows:
            meta_json = r[7] if len(r) > 7 and r[7] is not None else "{}"
            try:
                meta = json.loads(meta_json)
                if not isinstance(meta, dict):
                    meta = {}
            except Exception:
                meta = {}

            emb = r[2]
            embedding = list(emb) if emb is not None and emb != [] else None

            out.append(
                MemRecord(
                    id=str(r[0]),
                    text=str(r[1]) if r[1] is not None else "",
                    embedding=embedding,
                    created_at=float(r[3]) if r[3] is not None else 0.0,
                    last_access=float(r[4]) if r[4] is not None else 0.0,
                    access_count=int(r[5]) if r[5] is not None else 0,
                    bytes=int(r[6]) if r[6] is not None else 0,
                    meta=meta,
                )
            )
        return out

    def get_many(self, ids: list[str]) -> list[MemRecord]:
        self._ensure_connected()
        if not ids:
            return []

        q = """
        MATCH (m:Mem)
        WHERE m.id IN $ids
        RETURN
          m.id AS id,
          m.text AS text,
          m.embedding AS embedding,
          m.created_at AS created_at,
          m.last_access AS last_access,
          m.access_count AS access_count,
          m.bytes AS bytes,
          m.meta_json AS meta_json
        """
        res = self._graph.query(q, {"ids": ids})
        rows = self._rows(res)

        out: list[MemRecord] = []
        for r in rows:
            meta_json = r[7] if len(r) > 7 and r[7] is not None else "{}"
            try:
                meta = json.loads(meta_json)
                if not isinstance(meta, dict):
                    meta = {}
            except Exception:
                meta = {}

            emb = r[2]
            embedding = list(emb) if emb is not None and emb != [] else None

            out.append(
                MemRecord(
                    id=str(r[0]),
                    text=str(r[1]) if r[1] is not None else "",
                    embedding=embedding,
                    created_at=float(r[3]) if r[3] is not None else 0.0,
                    last_access=float(r[4]) if r[4] is not None else 0.0,
                    access_count=int(r[5]) if r[5] is not None else 0,
                    bytes=int(r[6]) if r[6] is not None else 0,
                    meta=meta,
                )
            )
        return out