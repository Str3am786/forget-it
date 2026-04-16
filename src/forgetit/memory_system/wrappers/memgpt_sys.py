import time 

from __future__ import annotations

from letta_client import Letta, LettaError, APIError
from datetime import datetime

from forgetit.memory_system.base import MemorySystem
from forgetit.core.schema import MemRecord, Query

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from letta_client.types import AgentState, Passage


class LettaWrapper(MemorySystem):
    """
    Formerly MemGPT wrapper for forgetit benchmark
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8283",
        api_key: str | None = None,
        model: str = "ollama/mistral:7b-instruct-v0.3-q6_K",
        embedding: str = "ollama/nomic-embed-text",
        agent_name: str = "benchmark_agent",
    ):
        """
        Initialize the LettaWrapper.

        :param base_url: Base URL of the Letta API endpoint.
        :param api_key: API key used for authentication with the Letta service.
        :param model: Q5+ recommended, Name or identifier of the language model to use.
        :param embedding: Name or identifier of the embedding model to use.
        :param agent_name: Identifier or name of the agent instance.

        """
        self.client: Letta = Letta(
            base_url=base_url,
            **({"api_key": api_key} if api_key else {})
        )

        self.model = model
        self.embedding = embedding
        self._tracked_bytes = 0
        self._access_counts: dict[str, int] = {}
        self._last_access: dict[str, float] = {}

        self.agent = self._get_or_create_agent(agent_name)



    def _get_or_create_agent(self, name: str) -> AgentState | None:
        # TODO might want to add the agent type
        try:
            agents = self.client.agents.list()

            for a in agents: 
                if a.name == name:
                    return a

        except:
            pass

        return self.client.agents.create(
            **({"name": self.agent_name} if self.agent_name else {}),
            model=self.model,
            embedding=self.embedding,
            memory_blocks=[
                {
                    "label": "human",
                    "value": "The user is a memory benchmark system"
                },
                {
                    "label": "persona",
                    "value": (
                        "I am an agent used for memory benchmarking. "
                        "I store and retrieve facts faithfully."
                    ),
                }
            ]
        )
    
    def _passage_to_record(self, passage: Passage) -> MemRecord:
        pid = passage.id or ""  #TODO check
        now  = time.time() 

        # We need to track as letta does not
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
                "letta_metadata": passage.metadata or {}
            }
        )
    
    def insert(self, item: MemRecord) -> None:
        self.client.agents.passages.create(
            agent_id=self.agent.id,
            text=item.text,
            created_at=datetime.fromtimestamp(item.created_at),
            tags=item.meta.get("tags", [])
        )
        self._tracked_bytes += len(item.text.encode("utf-8"))


    
    def retrieve(self, query: Query) -> list[MemRecord]:
        try:
            results = self.client.agents.passages.search(
                agent_id = self.agent.id,
                query = query.text
            )
            return[
                self._passage_to_rec(passage)
                for passage in results
                if not passage.is_deleted
            ] 
        
        except:
            pass

    def get_used_bytes(self) -> int:
        return self._tracked_bytes
    
    
    def clear(self) -> None: # TODO rename reset?
            """Delete the agent and recreate it for a clean benchmark run."""
            # Only try to delete if we already know the ID, or search by name to destroy it
            try:
                agents = self.client.agents.list()
                for a in agents:
                    if a.name == self.agent_name:
                        self.client.agents.delete(agent_id=a.id)
            except APIError:
                pass

            self.agent = self._get_or_create_agent()
            self._tracked_bytes = 0
    
    def get_agent_state(self) -> dict:
        """Extra benchmark metadata."""
        agent = self.client.agents.retrieve(agent_id=self.agent.id)
        return {
            "agent_id": agent.id,
            "message_ids": getattr(agent, "message_ids", []),
            "message_count": len(getattr(agent, "message_ids", [])),
            "total_passages_inserted": len(self._access_counts),
            "tracked_bytes": self._tracked_bytes,
        }
    
    def send_message(self, text: str) -> list[str]:
        response = self.client.agents.messages.create(
            agent_id = self.agent.id,
            messages=[{"role": "user", "content": text}]
        )
        return [
            msg.content
            for msg in response.messages
            if getattr(msg, "message_type", None) == "assistant_message"
        ]

