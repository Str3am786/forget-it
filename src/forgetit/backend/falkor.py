import logging
from typing import Optional, override
from falkor import FalkorDB
from forgetit.backend.backend import Backend

logger = logging.getLogger(__name__)

class Falkor(Backend):
    def __init__(self, cfg: FalkorConfig):
        self.cfg = cfg
        self._db: Optional[FalkorDB] = None
        self._graph: Optional[Graph] = None

    @override
    def connect(self) -> None:
        # Optional: make connect idempotent
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
                # health_check_interval=... (optional)
            )

            # 1) Fail fast on network/auth
            self._db.connection.ping()

            # 2) Create graph handle (this does NOT validate existence)
            self._graph = self._db.select_graph(self.cfg.graph)

            # 3) Fail fast on Falkor graph engine path
            # Option A: module-level check
            # self._db.list_graphs()

            # Option B: query path check
            self._graph.query("RETURN 1")

            logger.info(
                "Connected to FalkorDB host=%s port=%s graph=%s",
                self.cfg.host, self.cfg.port, self.cfg.graph
            )

        except Exception:
            # Reset state so callers don't think we're connected
            self._db = None
            self._graph = None

            logger.exception(
                "Failed to connect to FalkorDB host=%s port=%s graph=%s",
                self.cfg.host, self.cfg.port, self.cfg.graph
            )
            raise  # fail fast means propagate the error

    @override
    def close(self) -> None:
        # Depending on redis-py version, connection pool can be disconnected.
        if self._db is not None:
            try:
                pool = getattr(self._db.connection, "connection_pool", None)
                if pool is not None:
                    pool.disconnect()
            finally:
                self._db = None
                self._graph = None
