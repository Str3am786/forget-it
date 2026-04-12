from forgetit.memory_system.base import MemorySystem
from forgetit.core.schema import MemRecord, Query
# TODO Import MemGPT's python client



class MemGPTWrapper(MemorySystem):
    def __init__(self, agent_name="benchmark_agent"):
        # Initialize MemGPT client and create a headless agent for the benchmark
        self.client = None #create_client()
        self.agent = # TODO self.client.create_agent(name=agent_name)
        self._tracked_bytes = 0  # MemGPT doesn't track strict bytes, so we must

    def insert(self, item: MemRecord) -> None:
        # Translate your MemRecord into a MemGPT message/memory insertion
        self.client.user_message(agent_id=self.agent.id, message=item.text)

        # Calculate bytes to satisfy your benchmark's tracking
        self._tracked_bytes += len(item.text.encode('utf-8'))

        # NOTE: MemGPT handles its own "forgetting/archiving" internally.
        # You just feed it data.

    def retrieve(self, query: Query) -> list[MemRecord]:
        # Ask MemGPT to retrieve relevant context
        # (This depends on how MemGPT exposes its retriever via API)
        response = self.client.run_command(agent_id=self.agent.id, command=query.text)

        # Wrap MemGPT's response back into your MemRecord format
        return [MemRecord()]

    def get_used_bytes(self) -> int:
        return self._tracked_bytes