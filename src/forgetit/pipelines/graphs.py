from __future__ import annotations

from langgraph.graph import START, END, StateGraph

from forgetit.pipelines.nodes import ingest_session_node, retrieve_node, answer_node
from forgetit.pipelines.state import IngestState, RetrievalState, QAState


def build_ingest_graph():
    graph = StateGraph(IngestState)
    graph.add_node("ingest_session", ingest_session_node)
    graph.add_edge(START, "ingest_session")
    graph.add_edge("ingest_session", END)
    return graph.compile()


def build_retrieval_graph():
    graph = StateGraph(RetrievalState)
    graph.add_node("retrieve", retrieve_node)
    graph.add_edge(START, "retrieve")
    graph.add_edge("retrieve", END)
    return graph.compile()


def build_qa_graph():
    graph = StateGraph(QAState)
    graph.add_node("answer", answer_node)
    graph.add_edge(START, "answer")
    graph.add_edge("answer", END)
    return graph.compile()