from __future__ import annotations

import hashlib
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from letta_client import Letta, LettaError
from letta_client.types import EmbeddingConfig

from forgetit.memory_system.base import MemorySystem
from forgetit.core.schema import MemRecord, Query
from forgetit.policies.base import EvictionPolicy
from forgetit.settings import LLM_BASE_URL as _DEFAULT_EMBEDDING_ENDPOINT, LETTA_MAX_CHUNK_CHARS


if TYPE_CHECKING:
    from letta_client.types import AgentState, Passage


_MAX_INSERT_CHARS = LETTA_MAX_CHUNK_CHARS

class LettaWrapper(MemorySystem):
    """
    Letta (formerly MemGPT) wrapper for the ForgetIt benchmark.

    Letta manages its own memory internally — it has core memory blocks
    (in-context, editable by the agent) and archival memory (vector-searched,
    unlimited via pgvector). For the benchmark we use archival memory as the
    primary insert/retrieve mechanism, since it maps cleanly to MemRecord.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8283",
        api_key: str | None = None,
        model: str = "ollama/qwen2.5:7b-instruct-q5_K_M",
        embedding: str = "ollama/nomic-embed-text:latest",
        embedding_endpoint: str = _DEFAULT_EMBEDDING_ENDPOINT,
        agent_name: str = "benchmark_agent",
    ):
        """
        Initialize the LettaWrapper.

        :param base_url: Base URL of the Letta API endpoint.
        :param api_key: API key used for authentication with the Letta service.
        :param model: Q5+ recommended. Name or identifier of the language model.
        :param embedding: Name or identifier of the embedding model.
        :param agent_name: Identifier or name of the agent instance.
        """
        if "://" not in base_url:
            base_url = "http://" + base_url

        self.client: Letta = Letta(
            base_url=base_url,
            **({"api_key": api_key} if api_key else {}),
        )

        self.model = model
        self.embedding = embedding
        self.embedding_endpoint = embedding_endpoint
        self._tracked_bytes = 0
        self._access_counts: dict[str, int] = {}
        self._last_access: dict[str, float] = {}


        config_hash = hashlib.md5(
            f"{self.model}|{self.embedding}|nomic-embed-text-8k|2000".encode()
        ).hexdigest()[:8]
        self.agent_name = f"{agent_name}_{config_hash}"
        self.agent = self._get_or_create_agent(self.agent_name)

        self._MAX_INSERT_CHARS = _MAX_INSERT_CHARS


    def _get_or_create_agent(self, name: str) -> AgentState:
        try:
            agents = self.client.agents.list()
            for a in agents:
                if a.name == name:
                    ec = getattr(a, "embedding_config", None)
                    print(f"[LettaWrapper] REUSING agent {a.id} name={a.name}")
                    print(f"[LettaWrapper]   embedding_model={getattr(ec, 'embedding_model', None)}")
                    print(f"[LettaWrapper]   endpoint={getattr(ec, 'embedding_endpoint', None)}")
                    print(f"[LettaWrapper]   endpoint_type={getattr(ec, 'embedding_endpoint_type', None)}")
                    return a
        except LettaError:
            pass

        new = self.client.agents.create(
            **({"name": name} if name else {}),
            model=self.model,
            embedding_config=EmbeddingConfig(
                embedding_endpoint_type="ollama",
                embedding_endpoint=self.embedding_endpoint,
                embedding_model="nomic-embed-text-8k",
                embedding_dim=768,
                embedding_chunk_size=512,
            ),
            memory_blocks=[
                {"label": "human", "value": "The user is a memory benchmark system."},
                {"label": "persona", "value": "I am an agent used for memory benchmarking. I store and retrieve facts faithfully."},
            ],
        )
        ec = getattr(new, "embedding_config", None)
        print(f"[LettaWrapper] CREATED agent {new.id} name={new.name}")
        print(f"[LettaWrapper]   embedding_model={getattr(ec, 'embedding_model', None)}")
        print(f"[LettaWrapper]   endpoint={getattr(ec, 'embedding_endpoint', None)}")
        print(f"[LettaWrapper]   endpoint_type={getattr(ec, 'embedding_endpoint_type', None)}")
        return new


    def _passage_to_record(self, passage: Passage) -> MemRecord:
        """Map a full Letta Passage (from .list()) to a MemRecord."""
        pid = passage.id or ""
        now = time.time()

        self._access_counts[pid] = self._access_counts.get(pid, 0) + 1
        self._last_access[pid] = now

        created_at = now
        if passage.created_at is not None:
            created_at = passage.created_at.timestamp()

        text = passage.text or ""

        return MemRecord(
            id=pid,
            text=text,
            embedding=passage.embedding,
            created_at=created_at,
            last_access=self._last_access[pid],
            access_count=self._access_counts[pid],
            bytes=len(text.encode("utf-8")),
            meta={
                "source": "letta",
                "archive_id": passage.archive_id,
                "file_id": passage.file_id,
                "file_name": passage.file_name,
                "tags": passage.tags or [],
                "letta_metadata": passage.metadata or {},
            },
        )

    def _result_to_record(self, result) -> MemRecord:
        """Map a Letta search Result (from .search()) to a MemRecord.

        Result fields: id, content, timestamp (str), tags.
        Unlike Passage it has no embedding, archive_id, file_id, or is_deleted.
        """
        rid = result.id or ""
        now = time.time()

        self._access_counts[rid] = self._access_counts.get(rid, 0) + 1
        self._last_access[rid] = now

        created_at = now
        ts = getattr(result, "timestamp", None)
        if ts:
            try:
                created_at = datetime.fromisoformat(ts).timestamp()
            except (ValueError, TypeError):
                pass

        text = result.content or ""

        return MemRecord(
            id=rid,
            text=text,
            embedding=None,
            created_at=created_at,
            last_access=self._last_access[rid],
            access_count=self._access_counts[rid],
            bytes=len(text.encode("utf-8")),
            meta={
                "source": "letta",
                "tags": getattr(result, "tags", None) or [],
            },
        )

    def _chunk_text(self, text: str) -> list[str]:
        if len(text) <= self._MAX_INSERT_CHARS:
            return [text]
        parts, buf, cur = [], [], 0
        for para in text.split("\n\n"):
            if cur + len(para) + 2 > self._MAX_INSERT_CHARS and buf:
                parts.append("\n\n".join(buf))
                buf, cur = [], 0
            if len(para) > self._MAX_INSERT_CHARS:
                for i in range(0, len(para), self._MAX_INSERT_CHARS):
                    parts.append(para[i:i + self._MAX_INSERT_CHARS])
                continue
            buf.append(para)
            cur += len(para) + 2
        if buf:
            parts.append("\n\n".join(buf))
        return parts

    def insert(self, item: MemRecord) -> None:
        self._ensure_agent()
        base_kwargs = {"agent_id": self.agent.id, "tags": item.meta.get("tags", [])}
        if item.created_at > 0:
            base_kwargs["created_at"] = datetime.fromtimestamp(item.created_at, tz=timezone.utc)

        for chunk in self._chunk_text(item.text):
            self.client.agents.passages.create(**base_kwargs, text=chunk)
            self._tracked_bytes += len(chunk.encode("utf-8"))

    def retrieve(self, query: Query, k: int | None = None) -> list[MemRecord]:
        """Search Letta's archival memory with semantic search."""
        try:
            self._ensure_agent()
            kwargs = {"agent_id": self.agent.id, "query": query.text}
            if k is not None:
                kwargs["top_k"] = k
            response = self.client.agents.passages.search(**kwargs)
            return [self._result_to_record(r) for r in response.results]
        except LettaError:
            return []

    def get_used_bytes(self) -> int:
        return self._tracked_bytes


    def clear(self) -> None:
        """Delete the agent."""
        try:
            agents = self.client.agents.list()
            for a in agents:
                if a.name == self.agent_name:
                    self.client.agents.delete(agent_id=a.id)
                    break
        except LettaError:
            pass

        self.agent = None  # mark as uninitialized
        self._tracked_bytes = 0
        self._access_counts.clear()
        self._last_access.clear()

    def _ensure_agent(self):
        """Lazy: create on first use after clear()."""
        if self.agent is None:
            self.agent = self._get_or_create_agent(self.agent_name)
        return self.agent

    def get_agent_state(self) -> dict:
        """Extra benchmark metadata — includes message_ids."""
        
        self._ensure_agent()
        agent = self.client.agents.retrieve(agent_id=self.agent.id)
        return {
            "agent_id": agent.id,
            "message_ids": getattr(agent, "message_ids", []),
            "message_count": len(getattr(agent, "message_ids", [])),
            "total_passages_inserted": len(self._access_counts),
            "tracked_bytes": self._tracked_bytes,
        }


    def send_message(self, text: str) -> list[str]:
        """
        Send a user message through the full Letta agent loop.

        The agent will reason, potentially update its own memory blocks,
        and respond. This triggers the LLM — useful for qualitative
        benchmarking but slow.
        """
        self._ensure_agent()

        response = self.client.agents.messages.create(
            agent_id=self.agent.id,
            messages=[{"role": "user", "content": text}],
        )
        out: list[str] = []
        for msg in response.messages:
            if getattr(msg, "message_type", None) == "assistant_message":
                content = msg.content
                if isinstance(content, str):
                    out.append(content)
                elif isinstance(content, list):
                    out.append(
                        " ".join(
                            block.text
                            for block in content
                            if hasattr(block, "text")
                        )
                    )
        return out
    


class BoundedLetta(LettaWrapper):
    """
    A budget-enforcing Letta wrapper. 
    It intercepts insertions to check against a byte budget and triggers 
    explicit HTTP DELETE requests to the Letta server based on a policy.
    """

    def __init__(
        self,
        budget_bytes: int,
        policy: EvictionPolicy,
        logger,
        **kwargs  # Pass-through for LettaWrapper args (base_url, model, etc.)
    ):
        super().__init__(**kwargs)
        self.budget_bytes = budget_bytes
        self.policy = policy
        self.logger = logger
        
        # Local state to allow the Policy to make decisions without 
        # hammering the Letta API for every decision.
        self._record_map: dict[str, MemRecord] = {}

    def insert(self, item: MemRecord) -> None:
        """
        Inserts a record while enforcing the budget.
        Note: If LettaWrapper chunks the text, we track the total bytes 
        as one logic unit for the benchmark.
        """
        self._ensure_agent()
        
        # 1. Calculate and normalize bytes
        b = item.bytes if item.bytes > 0 else len(item.text.encode("utf-8"))
        item.bytes = b

        if b > self.budget_bytes:
            raise ValueError(f"Item {item.id} exceeds total budget.")

        # 2. Check and enforce budget (The Eviction Loop)
        self._ensure_budget(incoming_bytes=b)

        # 3. Perform physical insertion
        # We override the base insert to capture the passage IDs Letta generates
        base_kwargs = {"agent_id": self.agent.id, "tags": item.meta.get("tags", [])}
        if item.created_at > 0:
            base_kwargs["created_at"] = datetime.fromtimestamp(item.created_at, tz=timezone.utc)

        # Letta might create multiple passages if chunking is triggered
        for chunk in self._chunk_text(item.text):
            response = self.client.agents.passages.create(**base_kwargs, text=chunk)
            
            # Map Letta's internal ID to our record for deletion later
            letta_id = response.id
            
            # We store a "shadow" of the record locally for the Policy to inspect
            # We use the Letta ID as the key so we can delete it specifically
            shadow_item = MemRecord(
                id=letta_id,
                text=chunk,
                bytes=len(chunk.encode("utf-8")),
                created_at=item.created_at or time.time(),
                last_access=item.created_at or time.time()
            )
            self._record_map[letta_id] = shadow_item
            self._tracked_bytes += shadow_item.bytes

        self.logger.event("insert", {
            "id": item.id, 
            "bytes": b, 
            "used_bytes": self._tracked_bytes, 
            "budget": self.budget_bytes
        })

    def retrieve(self, query: Query, k: int | None = 5) -> list[MemRecord]:
        """Search and update access metadata for LRU/LFU policies."""
        records = super().retrieve(query, k=k)
        
        # Update local metadata so the policy knows what was recently hit
        for rec in records:
            if rec.id in self._record_map:
                self._record_map[rec.id].access_count += 1
                self._record_map[rec.id].last_access = query.timestamp
                
        return records

    def _ensure_budget(self, incoming_bytes: int) -> None:
        """Triggers the policy until there is enough room."""
        while self._tracked_bytes + incoming_bytes > self.budget_bytes:
            # The policy calls select_action(self)
            # This works because we implement iter_items() below
            action = self.policy.select_action(self)
            self._execute_action(action)

    def _execute_action(self, action: Action) -> None:
        if isinstance(action, Evict):
            self._do_evict(action.id)
        else:
            raise NotImplementedError(f"Action {type(action)} not supported for Letta.")

    def _do_evict(self, letta_id: str) -> None:
        """Physical deletion from the remote Letta server."""
        try:
            # 1. API Call: HTTP DELETE
            self.client.agents.passages.delete(
                agent_id=self.agent.id, 
                passage_id=letta_id
            )
            
            # 2. Update Accounting
            record = self._record_map.pop(letta_id, None)
            if record:
                self._tracked_bytes -= record.bytes
                
            self.logger.event("evict", {
                "id": letta_id,
                "freed": record.bytes if record else 0,
                "used": self._tracked_bytes
            })
        except LettaError as e:
            print(f"[BoundedLetta] Failed to evict {letta_id}: {e}")

    # --- Policy Support Methods ---

    def iter_items(self) -> Iterable[MemRecord]:
        """Required by policies like LRU/LFU to find the 'victim'."""
        return self._record_map.values()

    def clear(self) -> None:
        """Nuke the agent and clear local accounting."""
        super().clear()
        self._record_map.clear()
        self._tracked_bytes = 0