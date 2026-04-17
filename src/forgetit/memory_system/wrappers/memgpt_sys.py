from __future__ import annotations
 
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING
 
from letta_client import Letta, LettaError

 
from forgetit.memory_system.base import MemorySystem
from forgetit.core.schema import MemRecord, Query
 
if TYPE_CHECKING:
    from letta_client.types import AgentState, Passage


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
        base_url: str = "localhost:8283",
        api_key: str | None = None,
        model: str = "ollama/qwen2.5:7b-instruct-q5_K_M",
        embedding: str = "ollama/nomic-embed-text:latest",
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
        self.client: Letta = Letta(
            base_url=base_url,
            **({"api_key": api_key} if api_key else {}),
        )

        self.model = model
        self.embedding = embedding
        self._tracked_bytes = 0
        self._access_counts: dict[str, int] = {}
        self._last_access: dict[str, float] = {}

        self.agent_name = agent_name
        self.agent = self._get_or_create_agent(agent_name)



    def _get_or_create_agent(self, name: str) -> AgentState:
        """Find an existing agent by name or create a fresh one."""
        try:
            agents = self.client.agents.list()
            for a in agents:
                if a.name == name:
                    return a
        except LettaError:
            pass

        return self.client.agents.create(
            **({"name": name} if name else {}),
            model=self.model,
            embedding=self.embedding,
            memory_blocks=[
                {
                    "label": "human",
                    "value": "The user is a memory benchmark system.",
                },
                {
                    "label": "persona",
                    "value": (
                        "I am an agent used for memory benchmarking. "
                        "I store and retrieve facts faithfully."
                    ),
                },
            ],
        )


    def _passage_to_record(self, passage: Passage) -> MemRecord:
        """Map a Letta Passage to a MemRecord with full field coverage."""
        pid = passage.id or ""
        now = time.time()

        # Track access locally since Letta does not
        self._access_counts[pid] = self._access_counts.get(pid, 0) + 1
        self._last_access[pid] = now

        # Convert datetime -> epoch float
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

    def insert(self, item: MemRecord) -> None:
        kwargs = {
            "agent_id": self.agent.id,
            "text": item.text,
            "tags": item.meta.get("tags", []),
        }
        # Only forward created_at if it looks real (not the epoch-zero sentinel)
        if item.created_at > 0:
            kwargs["created_at"] = datetime.fromtimestamp(
                item.created_at, tz=timezone.utc
            )
        self.client.agents.passages.create(**kwargs)
        self._tracked_bytes += len(item.text.encode("utf-8"))

    def retrieve(self, query: Query) -> list[MemRecord]:
        """Search Letta's archival memory with semantic search."""
        try:
            results = self.client.agents.passages.search(
                agent_id=self.agent.id,
                query=query.text,
            )
            return [
                self._passage_to_record(passage)
                for passage in results
                if not passage.is_deleted
            ]
        except LettaError:
            return []

    def get_used_bytes(self) -> int:
        return self._tracked_bytes


    def clear(self) -> None:
        """Delete the agent and recreate it for a clean benchmark run."""
        try:
            agents = self.client.agents.list()
            for a in agents:
                if a.name == self.agent_name:
                    self.client.agents.delete(agent_id=a.id)
                    break
        except LettaError:
            pass

        self.agent = self._get_or_create_agent(self.agent_name)
        self._tracked_bytes = 0
        self._access_counts.clear()
        self._last_access.clear()

    def get_agent_state(self) -> dict:
        """Extra benchmark metadata — includes message_ids."""
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