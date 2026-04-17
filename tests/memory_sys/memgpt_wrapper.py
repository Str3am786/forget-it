import pytest
import time
from unittest.mock import MagicMock, patch

from letta_client import LettaError
from forgetit.core.schema import MemRecord, Query
# Adjust this import path based on where you placed the file!
from forgetit.memory_system.wrappers.memgpt_sys import LettaWrapper

@pytest.fixture
def mock_letta_client():
    """Fixture to inject a fully mocked Letta client into the wrapper."""
    with patch("forgetit.memory_system.wrappers.memgpt_sys.Letta") as MockLetta:
        mock_instance = MockLetta.return_value
        
        # Setup default mock agent
        mock_agent = MagicMock()
        mock_agent.id = "agent-123"
        mock_agent.name = "benchmark_agent"
        
        # Default behavior: pretend the agent doesn't exist yet, so it gets created
        mock_instance.agents.list.return_value = []
        mock_instance.agents.create.return_value = mock_agent
        
        yield mock_instance

@pytest.fixture
def wrapper(mock_letta_client):
    """Fixture to provide a fresh LettaWrapper for each test."""
    return LettaWrapper(agent_name="benchmark_agent")


class TestLettaWrapperInitialization:
    def test_init_creates_new_agent(self, mock_letta_client):
        """Test that if the agent does not exist, it creates one."""
        mock_letta_client.agents.list.return_value = []
        
        wrapper = LettaWrapper(agent_name="new_agent")
        
        # Ensure list was called, and create was triggered
        mock_letta_client.agents.list.assert_called_once()
        mock_letta_client.agents.create.assert_called_once()
        assert wrapper.agent.id == "agent-123"
        assert wrapper.agent_name == "new_agent"

    def test_init_finds_existing_agent(self, mock_letta_client):
        """Test that if the agent exists, it does NOT create a new one."""
        existing_agent = MagicMock()
        existing_agent.id = "agent-999"
        existing_agent.name = "existing_bench_agent"
        
        mock_letta_client.agents.list.return_value = [existing_agent]
        
        wrapper = LettaWrapper(agent_name="existing_bench_agent")
        
        # It should find it and skip creation
        mock_letta_client.agents.create.assert_not_called()
        assert wrapper.agent.id == "agent-999"


class TestLettaWrapperInsertion:
    def test_insert_tracks_bytes_correctly(self, wrapper, mock_letta_client):
        """Test that insertion calls the API and strictly counts UTF-8 bytes."""
        text = "Hello, this is a memory with an emoji 🧠."
        record = MemRecord(id="rec1", text=text, created_at=0.0)
        
        wrapper.insert(record)
        
        # Verify API call
        mock_letta_client.agents.passages.create.assert_called_once_with(
            agent_id="agent-123",
            text=text,
            tags=[]
        )
        
        # Verify byte accounting (emoji changes byte length vs string length)
        expected_bytes = len(text.encode("utf-8"))
        assert wrapper.get_used_bytes() == expected_bytes


class TestLettaWrapperRetrieval:
    def test_retrieve_filters_deleted_and_maps_correctly(self, wrapper, mock_letta_client):
        """Test mapping Letta Passages to MemRecords and filtering 'is_deleted'."""
        
        # Mock Letta returning two passages: one valid, one deleted
        p1 = MagicMock(id="pass1", text="Valid memory", created_at=MagicMock(timestamp=lambda: 1000.0), is_deleted=False)
        p2 = MagicMock(id="pass2", text="Deleted memory", is_deleted=True)
        
        mock_letta_client.agents.passages.search.return_value = [p1, p2]
        
        query = Query(id="id-test-1", text="Search term")
        results = wrapper.retrieve(query)
        
        # Assertions
        mock_letta_client.agents.passages.search.assert_called_once()
        assert len(results) == 1  # The deleted one should be filtered out
        
        record = results[0]
        assert record.id == "pass1"
        assert record.text == "Valid memory"
        assert record.created_at == 1000.0
        
        # Meaningful test: Check internal tracking state updated during retrieval
        assert wrapper._access_counts["pass1"] == 1
        assert "pass1" in wrapper._last_access

    def test_retrieve_handles_api_failure_gracefully(self, wrapper, mock_letta_client):
        """Test that if the Letta server crashes, the benchmark runner doesn't."""
        mock_letta_client.agents.passages.search.side_effect = LettaError("Server 500 error")
        
        query = Query(id="test-query-1", text="Search term")
        results = wrapper.retrieve(query)
        
        assert results == []  # Should return empty list, not crash


class TestLettaWrapperStateManagement:
    def test_clear_wipes_all_state(self, wrapper, mock_letta_client):
        """Critical test: Ensure clear() prevents state leaks between benchmark runs."""
        
        # 1. Contaminate the state
        wrapper._tracked_bytes = 5000
        wrapper._access_counts = {"pass1": 5}
        wrapper._last_access = {"pass1": time.time()}
        
        # 2. Setup mock to pretend the agent exists so delete is called
        mock_agent = MagicMock()
        mock_agent.name = "benchmark_agent"
        mock_agent.id = "agent-123"
        mock_letta_client.agents.list.return_value = [mock_agent]
        
        # 3. Call clear
        wrapper.clear()
        
        # 4. Verify API deletion
        mock_letta_client.agents.delete.assert_called_once_with(agent_id="agent-123")
        
        # 5. Verify INTERNAL state is completely reset (Crucial for the benchmark)
        assert wrapper.get_used_bytes() == 0
        assert len(wrapper._access_counts) == 0
        assert len(wrapper._last_access) == 0