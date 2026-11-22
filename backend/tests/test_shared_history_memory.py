from unittest.mock import MagicMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.runtime import Runtime

from deerflow.agents.memory import summarization_hook
from deerflow.agents.middlewares import memory_middleware
from deerflow.agents.middlewares.summarization_middleware import SummarizationEvent
from deerflow.config.memory_config import MemoryConfig
from deerflow.config.pii_redaction_config import PiiRedactionConfig


@pytest.mark.parametrize("redact", [False, True])
@pytest.mark.parametrize("has_shared", [False, True])
def test_memory_only_observes_personal_turns_at_both_queues(monkeypatch, redact, has_shared):
    manager = MagicMock()
    memory = MemoryConfig(enabled=True)
    monkeypatch.setattr(memory_middleware, "get_memory_manager", lambda: manager)
    monkeypatch.setattr(summarization_hook, "get_memory_manager", lambda: manager)
    monkeypatch.setattr(summarization_hook, "get_memory_config", lambda: memory)
    policy = PiiRedactionConfig(enabled=redact, token_secret="local-test-token-0123456789")
    runtime = Runtime(context={"thread_id": "continued", "user_id": "reader"})
    personal = [HumanMessage("My note is reader@example.com"), AIMessage("Your note is saved.")]
    copied = (
        [
            HumanMessage("Copied note", additional_kwargs={"shared_history": {"publication_id": "example"}}),
            AIMessage("Copied answer", additional_kwargs={"shared_history": None}),
        ]
        if has_shared
        else []
    )
    turns = copied + personal
    state = {"messages": turns, "summary_text": "A summary of the imported material."}
    middleware = memory_middleware.MemoryMiddleware(memory_config=memory, pii_redaction_config=policy)
    middleware.after_agent(state, runtime)
    event = SummarizationEvent(tuple(turns), (), "continued", None, runtime)
    summarization_hook.memory_flush_hook(event, policy)
    expected = memory_middleware.redact_queued_messages(personal, policy)
    assert manager.add.call_args.args[1] == expected
    assert manager.add_nowait.call_args.args[1] == expected
    assert state["messages"] == turns
    assert "reader@example.com" in personal[0].content
    # Goal evaluation uses this redaction-only path with the whole evidence window.
    assert memory_middleware.redact_queued_messages(turns, policy)[: len(copied)] == copied


def test_compaction_of_only_copied_turns_creates_no_memory_work(monkeypatch):
    manager = MagicMock()
    monkeypatch.setattr(summarization_hook, "get_memory_manager", lambda: manager)
    monkeypatch.setattr(summarization_hook, "get_memory_config", lambda: MemoryConfig(enabled=True))
    turn = HumanMessage("Copied note", additional_kwargs={"shared_history": {}})
    event = SummarizationEvent((turn,), (), "continued", None, Runtime(context={"user_id": "reader"}))
    summarization_hook.memory_flush_hook(event)
    manager.add_nowait.assert_not_called()


def test_external_input_discards_shared_history_at_normalization():
    from app.gateway.services import _normalize_input_messages

    row = {"type": "human", "content": "My own note", "additional_kwargs": {"shared_history": {"publication_id": "example"}}}
    (message,) = _normalize_input_messages([row], location="input.messages")
    assert "shared_history" not in message.additional_kwargs
    assert message.content == "My own note"
