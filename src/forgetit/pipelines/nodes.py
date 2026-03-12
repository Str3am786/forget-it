from __future__ import annotations

from typing import Any

from forgetit.pipelines.state import IngestState, RetrievalState, QAState


def ingest_session_node(state: IngestState) -> dict[str, Any]:
    rec = state["service"].add_session(state["session"])
    return {"inserted_record": rec}


def retrieve_node(state: RetrievalState) -> dict[str, Any]:
    k = state.get("k", 5)
    records = state["service"].retrieve(state["question"], k=k)
    return {"records": records}


def answer_node(state: QAState) -> dict[str, Any]:
    k = state.get("k", 5)
    max_tokens = state.get("max_tokens", 128)

    result = state["service"].answer(
        state["question"],
        k=k,
        max_tokens=max_tokens,
    )
    return {"result": result}