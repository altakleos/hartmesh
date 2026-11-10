"""File contents are not a JSON error envelope from the read_file tool."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from langchain.agents import create_agent
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from deerflow.agents.middlewares.read_before_write_middleware import ReadBeforeWriteMiddleware
from deerflow.agents.middlewares.tool_error_handling_middleware import ToolErrorHandlingMiddleware
from deerflow.agents.middlewares.tool_progress_middleware import ToolProgressMiddleware
from deerflow.agents.middlewares.tool_receipt import TOOL_RECEIPT_KEY
from deerflow.agents.middlewares.tool_receipt_middleware import ToolReceiptMiddleware
from deerflow.agents.middlewares.tool_result_meta import TOOL_META_KEY, normalize_tool_message
from deerflow.agents.thread_state import ThreadState
from deerflow.config.tool_progress_config import ToolProgressConfig


@pytest.mark.parametrize("error", ["Unauthorized", {"code": 401, "message": "Unauthorized"}, ["missing config"], 500, True, "no results found"])
def test_read_file_json_error_field_is_document_data(error):
    content = json.dumps({"error": error})
    message = ToolMessage(content=content, name="read_file", tool_call_id="read")

    result = normalize_tool_message(message)

    assert result.content == content
    assert result.additional_kwargs[TOOL_META_KEY] == {
        "status": "success",
        "error_type": None,
        "recoverable_by_model": True,
        "recommended_next_action": "continue",
        "source": "tool_return",
    }


@pytest.mark.parametrize("name,status", [("read_file", "error"), ("web_search", "success"), ("custom_tool", "success")])
def test_actual_tool_json_errors_are_still_classified(name, status):
    message = ToolMessage(content='{"error": {"code": 401, "message": "Unauthorized"}}', name=name, tool_call_id="call", status=status)

    meta = normalize_tool_message(message).additional_kwargs[TOOL_META_KEY]

    assert meta["status"] == "error"
    assert meta["error_type"] == "unknown"
    assert meta["recoverable_by_model"] is True
    assert meta["recommended_next_action"] == "try_alternative"


@pytest.mark.parametrize("source", ["exception", "tool_return", "custom"])
def test_read_file_preserves_explicit_failure_metadata(source):
    metadata = {"status": "error", "source": source, "error_type": "auth"}
    message = ToolMessage(content='{"error":"Unauthorized"}', name="read_file", tool_call_id="read", additional_kwargs={TOOL_META_KEY: metadata})

    assert normalize_tool_message(message).additional_kwargs[TOOL_META_KEY] is metadata


class _ScriptedModel(GenericFakeChatModel):
    def bind_tools(self, tools, **kwargs):
        return self


class _ModelRequests(BaseCallbackHandler):
    def __init__(self):
        self.messages = []

    def on_chat_model_start(self, serialized, messages, **kwargs):
        self.messages.append(messages[0])


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("progress_enabled", [False, True])
@pytest.mark.parametrize("first_kind", ["json_error", "error_prose", "marker_prose", "success", "missing", "truncated"])
@pytest.mark.asyncio
async def test_native_read_file_keeps_followup_reads_available(tmp_path, monkeypatch, mode, progress_enabled, first_kind):
    from deerflow.sandbox import tools as sandbox_tools
    from deerflow.sandbox.local.local_sandbox import LocalSandbox

    outputs = tmp_path / "outputs"
    outputs.mkdir()
    first_content = '{"error": {"code": 401, "message": "Unauthorized"}}\n'
    if first_kind == "success":
        first_content = '{"result": "OK"}\n'
    elif first_kind == "error_prose":
        first_content = "Error: Permission denied is the example message in this manual.\n"
    elif first_kind == "marker_prose":
        first_content = "A truncation example: ... [truncated: showing 1 of 10 chars] ...\n"
    elif first_kind == "truncated":
        first_content = json.dumps({"error": "Unauthorized", "details": "sample line\n" * 10000})
    if first_kind != "missing":
        (outputs / "response.json").write_text(first_content, encoding="utf-8")
    notes = "The saved response belongs to an earlier request.\n"
    (outputs / "notes.txt").write_text(notes, encoding="utf-8")

    sandbox = LocalSandbox("json-result-test")
    reads = []
    original_read = sandbox.read_file

    def trace_read(path, *args, **kwargs):
        reads.append(path)
        return original_read(path, *args, **kwargs)

    monkeypatch.setattr(sandbox, "read_file", trace_read)
    monkeypatch.setattr(sandbox_tools, "ensure_sandbox_initialized", lambda *_args, **_kwargs: sandbox)
    monkeypatch.setattr(sandbox_tools, "ensure_sandbox_initialized_async", AsyncMock(return_value=sandbox))
    responses = [AIMessage(content="", tool_calls=[{"name": "read_file", "args": {"path": f"/mnt/user-data/outputs/{filename}"}, "id": f"read-{index}"}]) for index, filename in enumerate(("response.json", "notes.txt"))]
    responses.append(AIMessage(content="Done."))
    middleware = [ToolReceiptMiddleware(render_mode="delegation_only"), ReadBeforeWriteMiddleware()]
    if progress_enabled:
        middleware.append(ToolProgressMiddleware.from_config(ToolProgressConfig(enabled=True)))
    middleware.append(ToolErrorHandlingMiddleware())
    graph = create_agent(_ScriptedModel(messages=iter(responses)), tools=[sandbox_tools.read_file_tool], middleware=middleware, state_schema=ThreadState)
    state = {
        "messages": [HumanMessage(content="Read the saved API response and then the notes explaining it.")],
        "sandbox": {"sandbox_id": "local:json-result-test"},
        "thread_data": {"outputs_path": str(outputs)},
        "thread_directories_created": True,
    }
    context = {"thread_id": "json-result-test", "user_id": "default"}
    captured = _ModelRequests()
    config = {"callbacks": [captured]}
    result = graph.invoke(state, context=context, config=config) if mode == "sync" else await graph.ainvoke(state, context=context, config=config)
    first, second = [message for message in result["messages"] if isinstance(message, ToolMessage)]

    visible = [message for message in captured.messages[-1] if isinstance(message, ToolMessage)]
    assert len(visible) == 2
    for stored, projected in zip((first, second), visible, strict=True):
        assert json.loads(projected.content) == {
            TOOL_META_KEY: {key: stored.additional_kwargs[TOOL_META_KEY][key] for key in ("status", "source")},
            "content": stored.content,
        }
        assert projected is not stored
        assert projected.tool_call_id == stored.tool_call_id
        assert projected.additional_kwargs[TOOL_RECEIPT_KEY] == stored.additional_kwargs[TOOL_RECEIPT_KEY]

    assert second.content.replace("\r\n", "\n") == notes
    assert str(outputs / "notes.txt") in reads
    assert second.additional_kwargs[TOOL_RECEIPT_KEY]["status"] == "success"
    expected_status = {"missing": "error", "truncated": "partial_success"}.get(first_kind, "success")
    assert first.additional_kwargs[TOOL_META_KEY]["status"] == expected_status
    assert first.additional_kwargs[TOOL_RECEIPT_KEY]["status"] == expected_status
    if first_kind == "missing":
        assert first.content.startswith("Error: File not found:")
        assert first.additional_kwargs[TOOL_META_KEY]["error_type"] == "not_found"
    elif first_kind == "truncated":
        assert len(first.content) < len(first_content)
        assert "truncated" in first.content.lower()
    else:
        assert first.content.replace("\r\n", "\n") == first_content


@pytest.mark.asyncio
@pytest.mark.parametrize("exc,category", [(FileNotFoundError("missing provider"), "not_found"), (PermissionError("denied"), "permission"), (TimeoutError("slow"), "transient"), (RuntimeError("403 forbidden"), "unknown")])
async def test_read_file_async_initialization_failure_has_producer_state(monkeypatch, exc, category):
    from deerflow.sandbox import tools as sandbox_tools

    @asynccontextmanager
    async def authorized(_runtime):
        yield

    monkeypatch.setattr(sandbox_tools, "sandbox_authorization_scope_async", authorized)
    monkeypatch.setattr(sandbox_tools, "ensure_sandbox_initialized_async", AsyncMock(side_effect=exc))
    runtime = SimpleNamespace(tool_call_id="read-init", context={}, state={})
    message = await sandbox_tools._read_file_tool_async(runtime, "/mnt/user-data/outputs/notes.txt")
    assert isinstance(message, ToolMessage)
    assert message.status == "error"
    assert message.additional_kwargs[TOOL_META_KEY]["status"] == "error"
    assert message.additional_kwargs[TOOL_META_KEY]["error_type"] == category
    assert message.additional_kwargs[TOOL_META_KEY]["source"] == "tool_return"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("ranged", [False, True])
@pytest.mark.parametrize("kind,category", [("validation", "unknown"), ("response_failed", "unknown"), ("missing", "not_found"), ("permission", "permission"), ("timeout", "transient"), ("unknown", "unknown"), ("literal", None)])
async def test_remote_read_status_comes_from_provider_outcome_not_file_text(monkeypatch, mode, ranged, kind, category):
    from unittest.mock import MagicMock, patch

    from agent_sandbox.types import ResponseFileReadResult
    from pydantic import ValidationError

    from deerflow.agents.middlewares.read_before_write_middleware import READ_MARK_KEY
    from deerflow.community.aio_sandbox.aio_sandbox import AioSandbox
    from deerflow.config.app_config import AppConfig, pop_current_app_config, push_current_app_config
    from deerflow.sandbox import tools

    push_current_app_config(AppConfig(sandbox={"use": "deerflow.community.aio_sandbox:AioSandboxProvider"}))
    try:
        text = "Error: permission denied is literal file content.\n"
        failure = {"missing": FileNotFoundError("arbitrary wording"), "permission": PermissionError("arbitrary wording"), "timeout": TimeoutError("arbitrary wording"), "unknown": RuntimeError("no such file: prose proves no category")}.get(
            kind
        )
        if kind == "validation":
            try:
                ResponseFileReadResult.model_validate({"success": False, "message": "read refused", "data": {}})
            except ValidationError as exc:
                failure = exc
        with patch("deerflow.community.aio_sandbox.aio_sandbox.AioSandboxClient"):
            sandbox = AioSandbox("remote-read-status", "http://sandbox.invalid")
        sandbox._client.file.read_file = MagicMock(side_effect=failure, return_value=SimpleNamespace(data=SimpleNamespace(content=text)))
        if kind == "response_failed":
            sandbox._client.file.read_file.return_value = ResponseFileReadResult.model_validate({"success": False, "message": "read refused", "data": {"file": "/mnt/user-data/outputs/response.txt", "content": text}})
        monkeypatch.setattr(tools, "ensure_sandbox_initialized", lambda *_args, **_kwargs: sandbox)
        monkeypatch.setattr(tools, "ensure_sandbox_initialized_async", AsyncMock(return_value=sandbox))
        monkeypatch.setattr(tools, "is_local_sandbox", lambda *_args: False)
        arguments = {"path": "/mnt/user-data/outputs/response.txt", **({"start_line": 1, "end_line": 2} if ranged else {})}
        model = _ScriptedModel(messages=iter([AIMessage(content="", tool_calls=[{"name": "read_file", "args": arguments, "id": "remote-read"}]), AIMessage(content="Done.")]))
        graph = create_agent(model, tools=[tools.read_file_tool], middleware=[ToolReceiptMiddleware(render_mode="delegation_only"), ReadBeforeWriteMiddleware(), ToolErrorHandlingMiddleware()], state_schema=ThreadState)
        state = {"messages": [HumanMessage(content="Read the file.")], "sandbox": {"sandbox_id": sandbox.id}, "thread_directories_created": True}
        context = {"thread_id": "remote-read-status", "user_id": "default"}
        result = graph.invoke(state, context=context) if mode == "sync" else await graph.ainvoke(state, context=context)
        message = next(message for message in result["messages"] if isinstance(message, ToolMessage))
        expected = "success" if kind == "literal" else "error"
        assert message.additional_kwargs[TOOL_META_KEY]["status"] == expected
        assert message.additional_kwargs[TOOL_META_KEY]["error_type"] == category
        assert message.additional_kwargs[TOOL_RECEIPT_KEY]["status"] == expected
        if kind == "literal":
            assert message.content == text
        else:
            assert message.content.startswith("Error:")
            assert READ_MARK_KEY not in message.additional_kwargs
        if ranged:
            assert any(call.kwargs.get("start_line") == 0 and call.kwargs.get("end_line") == 2 for call in sandbox._client.file.read_file.call_args_list)
    finally:
        pop_current_app_config()


@pytest.mark.parametrize(
    "module,name,api",
    [
        ("aio_sandbox.aio_sandbox", "AioSandbox", "aio"),
        ("e2b_sandbox.e2b_sandbox", "E2BSandbox", "e2b"),
        ("opensandbox.sandbox", "OpenSandboxSandbox", "open"),
        ("tenki.sandbox", "TenkiSandbox", "tenki"),
        ("boxlite.box", "BoxliteBox", "box"),
    ],
)
@pytest.mark.parametrize("ranged", [False, True])
def test_legacy_provider_errors_remain_strings_and_keep_their_source_exception(module, name, api, ranged):
    import importlib
    from unittest.mock import Mock

    from deerflow.sandbox.read_file_contract import SandboxReadFailure

    cls = getattr(importlib.import_module("deerflow.community." + module), name)
    sandbox = cls.__new__(cls)
    sandbox._resolve_path = lambda path: path
    failure = PermissionError("synthetic provider read refusal")
    read = Mock(side_effect=failure)
    if api == "aio":
        sandbox._client = SimpleNamespace(file=SimpleNamespace(read_file=read))
    elif api == "e2b":
        sandbox._client = SimpleNamespace(files=SimpleNamespace(read=read))
    elif api == "open":
        sandbox._file_op = lambda call: call(SimpleNamespace(read_file=read))
    elif api == "box":
        sandbox._exec = read
    else:
        sandbox._fs_op = lambda call: call(SimpleNamespace(read_text=read))
    result = sandbox.read_file("/mnt/user-data/outputs/data.txt", **({"start_line": 1, "end_line": 2} if ranged else {}))
    assert isinstance(result, str)
    assert result == "Error: synthetic provider read refusal"
    assert isinstance(result, SandboxReadFailure)
    assert result.error is failure


@pytest.mark.parametrize("ranged", [False, True])
def test_boxlite_nonzero_read_is_failed_without_guessing_from_stderr(ranged):
    from unittest.mock import Mock

    from deerflow.community.boxlite.box import BoxliteBox
    from deerflow.sandbox.read_file_contract import SandboxReadFailure

    sandbox = BoxliteBox.__new__(BoxliteBox)
    sandbox._resolve_path = lambda path: path
    sandbox._exec = Mock(return_value=SimpleNamespace(exit_code=1, stdout="", stderr="synthetic refusal"))
    result = sandbox.read_file("/mnt/user-data/outputs/file", **({"start_line": 1, "end_line": 2} if ranged else {}))
    assert result == "Error: synthetic refusal"
    assert isinstance(result, SandboxReadFailure)
