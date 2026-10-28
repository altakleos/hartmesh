"""Adapter composition with a labeled capped-transport/context protocol double.

The actual stream and actual publication custody have their own production-path
tests. These cases isolate file state, SDK getFile and byte lease handoff.
"""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _telegram_bot_test_support import RecordingRequest
from _telegram_repository_test_support import db as db
from test_telegram_bot_adapter import build, start

from app.channels.execution import ChannelExecutionContext, ChannelExecutionScope

pytestmark = pytest.mark.asyncio


class CappedProtocol(RecordingRequest):
    def __init__(self):
        super().__init__()
        self.reads = []
        self.data = b"abc"

    async def read_file_capped(self, url, *, ceiling, max_bytes, before_dispatch, on_failed_close=None):
        await before_dispatch()
        self.reads.append((ceiling, max_bytes))
        return self.data

    def retains_failed_close(self, callback):
        return False


async def fixture(db, monkeypatch, *, size=3, features=None, normal_request=None):
    from app.telegram_bots import authority
    from app.telegram_bots.files import FileBytesBudget

    e = await build(db, normal_request=normal_request or CappedProtocol())
    if normal_request is None:
        await start(e)
    else:
        await e.channel.start()
    e.file = {"type": "file", "file_id": "input-id", "file_unique_id": "stable-id", "filename": "sample.csv", "mime_type": "text/csv", "size": size, "telegram_bot_id": e.link.bot_id, "message_id": 301}
    e.cap = ChannelExecutionContext(
        ChannelExecutionScope("a" * 32, e.link.owner_user_id, e.link.connection_id, str(e.link.chat_id), "thread-original", "endpoint", "lead_agent", e.link.agent_ref),
        object(),
        SimpleNamespace(authorize=AsyncMock(), current=lambda operation: True),
    )
    e.context = SimpleNamespace(link=e.link, record=SimpleNamespace(publication_id=e.cap.publication_id, scope=e.cap.scope), file=e.file, text="Use this", files=[e.file], origins=(), current=lambda: True)
    e.services.turns = SimpleNamespace(file_context=AsyncMock(return_value=e.context), file_notice=AsyncMock(), retire=AsyncMock())
    e.services.file_bytes_budget = FileBytesBudget(200_000_000)
    e.services.thread_client = SimpleNamespace(get_features=AsyncMock(return_value=features or {"storage_spaces": {"state": "ready", "own": "ready", "reason": None}}))
    e.services.prepare_live_owner = AsyncMock()
    e.services.product_name = "HartMesh"
    e.services.prepare_ingress_values = AsyncMock(return_value={"product": "HartMesh"})
    monkeypatch.setattr(authority, "require", AsyncMock(return_value=frozenset({"threads:read", "threads:write"})))
    monkeypatch.setattr(authority, "is_administrator", AsyncMock(return_value=False))
    return e


@pytest.mark.parametrize("storage_state", ["ready", "incomplete"])
async def test_reader_reserves_before_getfile_and_transfers_charge_until_manager_close(db, monkeypatch, storage_state):
    e = await fixture(db, monkeypatch, features={"storage_spaces": {"state": storage_state, "own": "ready", "reason": None}})
    e.request.enqueue("getFile", result={"file_id": "input-id", "file_unique_id": "stable-id", "file_size": 3, "file_path": "documents/sample.csv"})
    try:
        lease = await e.channel.read_inbound_file(e.file, execution_context=e.cap)
        assert lease.data == b"abc"
        assert e.services.file_bytes_budget.used == 3
        assert e.request.reads == [(3, 20_000_000)]
        assert [c.endpoint for c in e.request.calls] == ["getMe", "getFile"]
        assert e.updates.calls == []
        await lease.aclose()
        assert e.services.file_bytes_budget.used == 0
        assert e.services.turns.file_notice.await_count == 0
    finally:
        await e.channel.stop()


@pytest.mark.parametrize("state,own,key", [("ready", "not-assigned", "file.own_preparing"), ("ready", "no-seat", "file.own_no_seat"), ("ready", "unverified", "file.own_unchecked"), ("unavailable", "ready", "file.own_unavailable")])
async def test_reader_storage_state_refuses_before_getfile(db, monkeypatch, state, own, key):
    e = await fixture(db, monkeypatch, features={"storage_spaces": {"state": state, "own": own, "reason": None}})
    try:
        assert await e.channel.read_inbound_file(e.file, execution_context=e.cap) is None
        assert [c.endpoint for c in e.request.calls] == ["getMe"]
        assert e.services.turns.file_notice.await_args.kwargs["key"] == key
        assert e.services.file_bytes_budget.used == 0
    finally:
        await e.channel.stop()


async def test_declared_oversize_never_gets_file(db, monkeypatch):
    e = await fixture(db, monkeypatch, size=20_000_001)
    monkeypatch.setattr("app.telegram_bots.pages.page_address", lambda *a, **kw: "https://product.example/conversation")
    try:
        assert await e.channel.read_inbound_file(e.file, execution_context=e.cap) is None
        assert e.services.turns.file_notice.await_args.kwargs["key"] == "file.too_large_in"
        assert e.services.turns.file_notice.await_args.kwargs["url_button"]["page"] == "web.conversation"
        assert [c.endpoint for c in e.request.calls] == ["getMe"]
    finally:
        await e.channel.stop()


@pytest.mark.parametrize("path", ["https://other.example/file", "file:///tmp/local", "../private", "https://api.telegram.org/unrelated", "documents/%2e%2e/private"])
async def test_reader_refuses_unexpected_getfile_location_without_download(db, monkeypatch, path):
    e = await fixture(db, monkeypatch)
    e.request.enqueue("getFile", result={"file_id": "input-id", "file_unique_id": "stable-id", "file_size": 3, "file_path": path})
    try:
        assert await e.channel.read_inbound_file(e.file, execution_context=e.cap) is None
        assert e.request.reads == []
        assert e.services.file_bytes_budget.used == 0
        assert e.services.turns.file_notice.await_args.kwargs["key"] == "file.not_saved"
        assert e.services.turns.file_notice.await_args.kwargs["retry"] is True
    finally:
        await e.channel.stop()


async def test_stale_file_context_has_no_transfer_or_notice(db, monkeypatch):
    e = await fixture(db, monkeypatch)
    e.services.turns.file_context.return_value = None
    try:
        assert await e.channel.read_inbound_file(e.file, execution_context=e.cap) is None
        assert e.request.reads == [] and e.services.turns.file_notice.await_count == 0
    finally:
        await e.channel.stop()


async def test_budget_wait_has_no_getfile_and_rechecks_original_context(db, monkeypatch):
    e = await fixture(db, monkeypatch)
    held = await e.services.file_bytes_budget.reserve_batch([200_000_000], timeout=1)
    entered = asyncio.Event()
    reserve = e.services.file_bytes_budget.reserve_batch

    async def observe(*args, **kwargs):
        entered.set()
        return await reserve(*args, **kwargs)

    monkeypatch.setattr(e.services.file_bytes_budget, "reserve_batch", observe)
    read = asyncio.create_task(e.channel.read_inbound_file(e.file, execution_context=e.cap))
    try:
        await asyncio.wait_for(entered.wait(), 15)
        assert [c.endpoint for c in e.request.calls] == ["getMe"]
        assert not read.done() and e.request.reads == []
        e.services.turns.file_context.return_value = None
        await held.aclose()
        assert await read is None
        assert [c.endpoint for c in e.request.calls] == ["getMe"]
        assert e.services.file_bytes_budget.used == 0
    finally:
        await held.aclose()
        read.cancel()
        await asyncio.gather(read, return_exceptions=True)
        await e.channel.stop()


async def test_adapter_stop_cancels_and_drains_getfile_before_clients_close(db, monkeypatch):
    e = await fixture(db, monkeypatch)
    gate = asyncio.Event()
    reply = e.request.enqueue("getFile", result={"file_id": "input-id", "file_unique_id": "stable-id", "file_path": "documents/input.csv"}, release=gate)
    read = asyncio.create_task(e.channel.read_inbound_file(e.file, execution_context=e.cap))
    try:
        await asyncio.wait_for(reply.started.wait(), 15)
        assert e.services.file_bytes_budget.used == 3
        await e.channel.stop()
        results = await asyncio.gather(read, return_exceptions=True)
        assert isinstance(results[0], asyncio.CancelledError)
        assert reply.cancelled.is_set() and e.request.closed and e.updates.closed
        assert e.services.file_bytes_budget.used == 0 and not e.channel._file_tasks
    finally:
        gate.set()
        read.cancel()
        await asyncio.gather(read, return_exceptions=True)
        await e.channel.stop()


@pytest.mark.parametrize("status", [401, 404, 403])
async def test_file_http_failure_verifies_token_only_for_401_404_and_never_blocks_chat(db, monkeypatch, status):
    from app.telegram_bots.capped_download import CappedDownloadError

    e = await fixture(db, monkeypatch)
    e.request.enqueue("getFile", result={"file_id": "input-id", "file_unique_id": "stable-id", "file_size": 3, "file_path": "documents/sample.csv"})
    if status != 403:
        e.request.enqueue_get_me(bot_id=e.link.bot_id)

    async def fail(*args, **kwargs):
        raise CappedDownloadError("status", status=status)

    monkeypatch.setattr(e.request, "read_file_capped", fail)
    try:
        assert await e.channel.read_inbound_file(e.file, execution_context=e.cap) is None
        assert [c.endpoint for c in e.request.calls] == ["getMe", "getFile", *(["getMe"] if status != 403 else [])]
        assert (await db.links.get(e.link.id)).state == "connected"
        assert e.channel._first_401 is None
        assert e.services.turns.file_notice.await_args.kwargs["key"] == "file.not_saved"
        assert e.services.file_bytes_budget.used == 0
    finally:
        await e.channel.stop()


@pytest.mark.parametrize(
    "reason,word", [("storage-pool-missing", "storage-missing"), ("inventory-unreadable", "storage-unreadable"), ("host-unqualified", "server-not-ready"), ("admission-failed", "server-check-failed"), ("unknown-internal-reason", None)]
)
async def test_administrator_storage_notice_uses_public_reference_only(db, monkeypatch, reason, word):
    e = await fixture(db, monkeypatch, features={"storage_spaces": {"state": "unavailable", "own": "ready", "reason": reason}})
    monkeypatch.setattr("app.telegram_bots.authority.is_administrator", AsyncMock(return_value=True))
    try:
        assert await e.channel.read_inbound_file(e.file, execution_context=e.cap) is None
        values = e.services.turns.file_notice.await_args.kwargs["values"]
        assert values["reader_is_administrator"] is True and values.get("code") == word
        assert reason not in values.values()
    finally:
        await e.channel.stop()


async def test_identical_descriptor_cannot_switch_original_manager_publication_or_chat(db, monkeypatch):
    e = await fixture(db, monkeypatch)
    original = e.cap
    # The live source may have the same bot/file/message IDs after reconnect;
    # the original manager's server capability still identifies its own input.
    for changed in (replace(original.scope, publication_id="b" * 32), replace(original.scope, chat_id="other-chat"), replace(original.scope, thread_id="new-thread")):
        e.cap = replace(original, scope=changed)
        assert await e.channel.read_inbound_file(e.file, execution_context=e.cap) is None
        await e.channel.file_own_full(e.file, execution_context=e.cap)
    assert e.request.reads == [] and e.services.turns.file_notice.await_count == 0
    assert [c.endpoint for c in e.request.calls] == ["getMe"]
    await e.channel.stop()


async def test_retirement_while_shrinking_drops_bytes_before_releasing_charge(db, monkeypatch):
    e = await fixture(db, monkeypatch)
    e.request.enqueue("getFile", result={"file_id": "input-id", "file_unique_id": "stable-id", "file_size": 3, "file_path": "documents/sample.csv"})
    reserve = e.services.file_bytes_budget.reserve_batch

    async def observe(*args, **kwargs):
        reservation = await reserve(*args, **kwargs)
        shrink = reservation.shrink

        async def retire(*values):
            await shrink(*values)
            e.context.current = lambda: False

        monkeypatch.setattr(reservation, "shrink", retire)
        return reservation

    monkeypatch.setattr(e.services.file_bytes_budget, "reserve_batch", observe)
    try:
        assert await e.channel.read_inbound_file(e.file, execution_context=e.cap) is None
        assert e.request.reads == [(3, 20_000_000)]
        assert e.services.file_bytes_budget.used == 0
    finally:
        await e.channel.stop()
