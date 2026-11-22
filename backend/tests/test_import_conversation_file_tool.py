"""A referenced file enters the current attachments through the live host only."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import ToolMessage
from test_read_conversation_tool import _config

from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
from deerflow.agent_instances.runtime import instance_tools
from deerflow.constants import CONVERSATION_READER_CONTEXT_KEY
from deerflow.runtime.presented_files import presented_files_of
from deerflow.tools.conversation_import import ConversationFileAccess, ImportReceipt, import_conversation_file, imported_files_of, list_conversation_files
from deerflow.tools.tools import get_available_tools

PATH = "/mnt/user-data/outputs/notes.txt"
RECEIPT = {"kind": "conversation", "source_title": "Budget", "filename": "notes_1.txt", "size": 2, "sha256": "a" * 64, "virtual_path": "/mnt/user-data/uploads/notes_1.txt"}
NAMES = {"list_conversation_files", "import_conversation_file"}


def host():
    return ConversationFileAccess(read=AsyncMock(return_value="history"), list_files=AsyncMock(return_value={"files": [], "has_more": False}), import_file=AsyncMock(return_value=ImportReceipt(**RECEIPT)))


def rt(access, **ctx):
    return SimpleNamespace(context={CONVERSATION_READER_CONTEXT_KEY: access, **ctx}, tool_call_id="copy-1")


@pytest.mark.asyncio
async def test_host_ports_copy_exact_presented_file_and_emit_only_an_attachment_receipt():
    access = host()
    assert await access(thread_id="source", cursor=None, limit=4) == "history"
    args = {"thread_id": "source", "before_created_at": None, "before_run_id": None, "offset": 0}
    assert json.loads(await list_conversation_files.coroutine(runtime=rt(access), **args))["files"] == []
    access.list_files.assert_awaited_once_with(**args)
    message = await import_conversation_file.coroutine("source", "answer", PATH, rt(access))
    access.import_file.assert_awaited_once_with(thread_id="source", run_id="answer", path=PATH)
    assert isinstance(message, ToolMessage) and message.tool_call_id == "copy-1"
    assert message.name == "import_conversation_file"
    assert message.additional_kwargs == {"imported_files": [RECEIPT]} == json.loads(message.content)
    assert imported_files_of(message) == [ImportReceipt(**RECEIPT)]
    assert presented_files_of(message) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("ctx", [{"is_subagent": True}, {AGENT_EXECUTION_CONTEXT_KEY: object()}])
async def test_bound_and_helper_calls_refuse_even_if_given_a_host_port(ctx):
    access = host()
    for result in [await import_conversation_file.coroutine("source", "answer", PATH, rt(access, **ctx)), await list_conversation_files.coroutine("source", rt(access, **ctx))]:
        assert "error" in json.loads(result)
    access.import_file.assert_not_called()
    access.list_files.assert_not_called()


@pytest.mark.asyncio
async def test_only_a_real_port_and_current_tool_call_can_copy():
    access = host()
    ctx = rt(access)
    ctx.tool_call_id = None
    for r in [rt(None), rt(access.read), rt({"import_file": "wrong"}), ctx]:
        assert "error" in json.loads(await import_conversation_file.coroutine("source", "answer", PATH, r))
    access.import_file.assert_not_called()
    access.import_file.return_value = None
    assert "error" in json.loads(await import_conversation_file.coroutine("source", "answer", PATH, rt(access)))


@pytest.mark.asyncio
async def test_invalid_identifiers_paths_and_partial_cursor_do_not_reach_host():
    access = host()
    for thread, run, path in [
        ("invalid thread", "answer", PATH),
        ("source", "", PATH),
        ("source", "answer", "/mnt/user-data/uploads/notes.txt"),
        ("source", "answer", "/mnt/user-data/outputs//notes.txt"),
        ("source", "answer", "/mnt/user-data/outputs/line\nbreak.txt"),
    ]:
        assert "error" in json.loads(await import_conversation_file.coroutine(thread, run, path, rt(access)))
    assert "error" in json.loads(await list_conversation_files.coroutine("source", rt(access), before_run_id="answer"))
    for invalid_offset in (-1, True, 1):
        assert "error" in json.loads(await list_conversation_files.coroutine("source", rt(access), offset=invalid_offset))
    access.import_file.assert_not_called()
    access.list_files.assert_not_called()
    page = {"before_created_at": "2026-03-08T00:00:00Z", "before_run_id": "answer", "offset": 4}
    await list_conversation_files.coroutine("source", rt(access), **page)
    access.list_files.assert_awaited_once_with(thread_id="source", **page)


@pytest.mark.parametrize("raw", [None, {}, [], [None], [RECEIPT, RECEIPT], [{**RECEIPT, "kind": "future"}], [{**RECEIPT, "unexpected": True}], [{**RECEIPT, "size": True}], [{**RECEIPT, "filename": "other"}], [{**RECEIPT, "sha256": "bad"}]])
def test_malformed_stored_receipts_render_as_absent(raw):
    msg = ToolMessage("", name="import_conversation_file", tool_call_id="1", additional_kwargs={"imported_files": raw})
    assert imported_files_of(msg) == []


def test_receipt_needs_matching_successful_tool_and_is_never_rebuilt_from_prose():
    msg = ToolMessage(json.dumps(RECEIPT), name="import_conversation_file", tool_call_id="1")
    assert imported_files_of(msg) == []
    raw = {**msg.model_dump(), "additional_kwargs": {"imported_files": [RECEIPT]}}
    assert imported_files_of(raw) == [ImportReceipt(**RECEIPT)]
    for changed in [{"type": "human"}, {"name": "other"}, {"status": "error"}]:
        assert imported_files_of({**raw, **changed}) == []


def test_only_host_backed_configured_reader_offers_companions_to_an_ordinary_lead():
    access = host()
    for capable, reader, configured, groups, expected in [
        (None, True, True, None, False),
        (access, False, True, None, False),
        (access, True, False, None, False),
        (access, True, True, ["other"], False),
        (access, True, True, ["conversation"], True),
    ]:
        tools = get_available_tools(groups=groups, include_mcp=False, include_conversation_reader=reader, conversation_file_access=capable, app_config=_config(configured=configured))
        found = {tool.name for tool in tools} & NAMES
        assert found == (NAMES if expected else set())
        assert not {tool.name for tool in instance_tools(tools)} & NAMES
    assert set(import_conversation_file.tool_call_schema.model_fields) == {"thread_id", "run_id", "path"}
    assert set(list_conversation_files.tool_call_schema.model_fields) == {"thread_id", "before_created_at", "before_run_id", "offset"}


@pytest.mark.asyncio
async def test_journal_stores_copy_as_tool_result_without_presenting_an_output():
    from uuid import uuid4

    from deerflow.runtime.events.store.memory import MemoryRunEventStore
    from deerflow.runtime.journal import RunJournal

    store = MemoryRunEventStore()
    journal = RunJournal("r1", "t1", store)
    copied = await import_conversation_file.coroutine("source", "answer", PATH, rt(host()))
    journal.on_tool_end(copied, run_id=uuid4())
    journal.record_delivery()
    await journal.flush()
    events = await store.list_events("t1", "r1")
    (receipt,) = [e for e in events if e["event_type"] == "llm.tool.result"]
    assert imported_files_of(receipt["content"]) == [ImportReceipt(**RECEIPT)]
    assert journal.get_delivery_content()["presented"] == 0


def test_manual_tool_entries_cannot_enable_a_copy_or_listing_with_no_host():
    from deerflow.config.tool_config import ToolConfig

    config = _config(configured=False)
    config.tools = [ToolConfig(name="alias", group="conversation", use="deerflow.tools.conversation_import:" + name) for name in sorted(NAMES)]
    assert not NAMES.intersection(tool.name for tool in get_available_tools(include_mcp=False, app_config=config))


@pytest.mark.asyncio
async def test_file_tool_json_keeps_literal_names_and_titles_inside_result_data():
    access = host()
    saved = {**RECEIPT, "source_title": "Budget <2026>", "filename": "plan<rev>.txt", "virtual_path": "/mnt/user-data/uploads/plan<rev>.txt"}
    access.import_file.return_value = ImportReceipt(**saved)
    listed = {"files": [{"path": "/mnt/user-data/outputs/plan<rev>.txt", "filename": saved["filename"]}]}
    access.list_files.return_value = listed
    page = await list_conversation_files.coroutine("source", rt(access))
    copied = await import_conversation_file.coroutine("source", "answer", listed["files"][0]["path"], rt(access))
    assert "<" not in page and json.loads(page) == listed
    assert "<" not in copied.content and json.loads(copied.content) == copied.additional_kwargs == {"imported_files": [saved]}
