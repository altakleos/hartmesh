"""Reader ABI retains the actual caller capability without provider metadata."""

import asyncio

import pytest

from app.channels import inbound_files, manager
from app.channels.execution import ChannelExecutionContext, ChannelExecutionScope
from app.channels.message_bus import InboundMessage
from deerflow.uploads import manager as uploads


def context(identity):
    return ChannelExecutionContext(ChannelExecutionScope(identity * 32, "owner", "connection", "chat", "thread", "endpoint", "lead_agent", None), object(), object())


@pytest.mark.asyncio
async def test_parallel_readers_keep_original_context_then_reset(tmp_path, monkeypatch):
    first, second = context("a"), context("b")
    entered, release = asyncio.Event(), asyncio.Event()
    seen = []
    destination = tmp_path / "uploads"
    destination.mkdir()
    monkeypatch.setattr(uploads, "ensure_uploads_dir", lambda *args, **kwargs: destination)

    async def reader(file, client):
        if file["filename"] == "first.txt":
            entered.set()
            await release.wait()
            seen.append(inbound_files.current_inbound_file_context())
            return b"a"
        seen.append(inbound_files.current_inbound_file_context())
        release.set()
        return None

    monkeypatch.setitem(manager.INBOUND_FILE_READERS, "context-test", reader)

    def message(name, cap):
        return InboundMessage(channel_name="context-test", chat_id="chat", user_id="person", text="Input", files=[{"filename": name}], execution_context=cap)

    one = asyncio.create_task(manager._ingest_inbound_files("thread", message("first.txt", first)))
    try:
        await asyncio.wait_for(entered.wait(), 15)
        assert inbound_files.current_inbound_file_context() is None
        await manager._ingest_inbound_files("thread", message("second.txt", second))
        await one
        assert seen == [second, first]
        assert inbound_files.current_inbound_file_context() is None
        with inbound_files.inbound_file_context(first):
            with inbound_files.inbound_file_context(None):
                assert inbound_files.current_inbound_file_context() is None
            assert inbound_files.current_inbound_file_context() is first
        assert inbound_files.current_inbound_file_context() is None
    finally:
        release.set()
        one.cancel()
        await asyncio.gather(one, return_exceptions=True)


@pytest.mark.asyncio
async def test_preparation_notifier_and_cancelled_reader_keep_scope_until_reset(tmp_path, monkeypatch):
    cap = context("a")
    observed = []

    def refuse(*args, **kwargs):
        raise OSError("synthetic native refusal")

    async def notify(file, error):
        observed.append(inbound_files.current_inbound_file_context())

    monkeypatch.setattr(uploads, "ensure_uploads_dir", refuse)
    monkeypatch.setitem(manager.INBOUND_FILE_WRITE_FAILED, "context-test", notify)
    monkeypatch.setitem(manager.INBOUND_FILE_PREPARE_FILTERS, "context-test", lambda file: True)
    message = InboundMessage(channel_name="context-test", chat_id="chat", user_id="person", text="Input", files=[{"filename": "file.txt"}], execution_context=cap)
    assert await manager._ingest_inbound_files("thread", message) == []
    assert observed == [cap] and inbound_files.current_inbound_file_context() is None
    destination = tmp_path / "uploads"
    destination.mkdir()
    monkeypatch.setattr(uploads, "ensure_uploads_dir", lambda *args, **kwargs: destination)
    entered = asyncio.Event()

    async def read(*args):
        try:
            entered.set()
            await asyncio.Event().wait()
        finally:
            observed.append(inbound_files.current_inbound_file_context())

    monkeypatch.setitem(manager.INBOUND_FILE_READERS, "context-test", read)
    task = asyncio.create_task(manager._ingest_inbound_files("thread", message))
    try:
        await asyncio.wait_for(entered.wait(), 15)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert observed == [cap, cap] and inbound_files.current_inbound_file_context() is None
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
