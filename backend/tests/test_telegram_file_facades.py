"""Lazy managed file readers borrow an adapter and preserve cancellation."""

import asyncio
import errno
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.channels.execution import ChannelUploadRefused
from app.channels.inbound_files import InboundFileLease
from app.telegram_bots.files import read_inbound, write_failed


def install(monkeypatch, channel=None, *, supervisor=True):
    seen = []

    def channel_for_bot(identity):
        seen.append(identity)
        return channel

    host = SimpleNamespace(channel_for_bot=channel_for_bot) if supervisor else None
    monkeypatch.setitem(sys.modules, "app.telegram_bots.supervisor", SimpleNamespace(current=lambda: host))
    return seen


def test_importing_files_loads_neither_sdk_nor_supervisor():
    code = "import sys; import app.telegram_bots.files; assert 'telegram' not in sys.modules; assert 'app.telegram_bots.supervisor' not in sys.modules"
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
async def test_read_uses_actual_bot_adapter_and_returns_same_live_lease(monkeypatch):
    lease = InboundFileLease(b"Downloaded bytes", AsyncMock())
    channel = SimpleNamespace(read_inbound_file=AsyncMock(return_value=lease))
    seen = install(monkeypatch, channel)
    descriptor = {"telegram_bot_id": 9000000001, "file_id": "remote-id"}
    assert await read_inbound(descriptor, object()) is lease
    assert seen == [9000000001] and lease.data == b"Downloaded bytes"
    channel.read_inbound_file.assert_awaited_once_with(descriptor, execution_context=None)
    await lease.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("host,bot", [(False, 9000000001), (True, 9000000001), (True, None), (True, True)])
async def test_missing_or_invalid_bot_has_no_transport_effect(monkeypatch, host, bot):
    install(monkeypatch, supervisor=host)
    assert await read_inbound({"telegram_bot_id": bot}, object()) is None
    await write_failed({"telegram_bot_id": bot}, RuntimeError("ignored"))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure,method",
    [
        (OSError(errno.ENOSPC, "full"), "file_own_full"),
        (OSError(errno.EDQUOT, "quota"), "file_own_full"),
        (ChannelUploadRefused(507), "file_own_full"),
        (ChannelUploadRefused(503), "file_own_unavailable"),
        (ChannelUploadRefused(409), "file_not_saved"),
        (ChannelUploadRefused(None), "file_not_saved"),
        (OSError("other"), "file_not_saved"),
        (RuntimeError("other"), "file_not_saved"),
    ],
)
async def test_write_failure_selects_fixed_notice_from_actual_errno(monkeypatch, failure, method):
    notices = ("file_own_full", "file_own_unavailable", "file_not_saved")
    channel = SimpleNamespace(**{notice: AsyncMock() for notice in notices})
    seen = install(monkeypatch, channel)
    descriptor = {"telegram_bot_id": 9000000001, "file_id": "remote-id"}
    await write_failed(descriptor, failure)
    getattr(channel, method).assert_awaited_once_with(descriptor, execution_context=None)
    for notice in notices:
        if notice != method:
            getattr(channel, notice).assert_not_called()
    assert seen == [9000000001]


@pytest.mark.asyncio
async def test_facade_failures_are_class_only_but_cancellation_propagates(monkeypatch, caplog):
    channel = SimpleNamespace(read_inbound_file=AsyncMock(side_effect=RuntimeError("https://private.example/token")), file_not_saved=AsyncMock(side_effect=RuntimeError("https://private.example/token")))
    install(monkeypatch, channel)
    descriptor = {"telegram_bot_id": 9000000001}
    assert await read_inbound(descriptor, object()) is None
    await write_failed(descriptor, RuntimeError("write"))
    assert "private.example" not in caplog.text
    channel.read_inbound_file.side_effect = asyncio.CancelledError
    channel.file_not_saved.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await read_inbound(descriptor, object())
    with pytest.raises(asyncio.CancelledError):
        await write_failed(descriptor, RuntimeError("write"))


@pytest.mark.asyncio
async def test_facades_forward_the_exact_original_typed_execution_context(monkeypatch):
    from app.channels.execution import ChannelExecutionContext, ChannelExecutionScope
    from app.channels.inbound_files import current_inbound_file_context, inbound_file_context

    scope = ChannelExecutionScope("a" * 32, "original-owner", "original-connection", "original-chat", "original-thread", "original-endpoint", "lead_agent", None)
    context = ChannelExecutionContext(scope, SimpleNamespace(), SimpleNamespace(authorize=AsyncMock(), current=lambda _operation: True))
    channel = SimpleNamespace(read_inbound_file=AsyncMock(return_value=None), file_own_full=AsyncMock(), file_not_saved=AsyncMock())
    install(monkeypatch, channel)
    descriptor = {"telegram_bot_id": 9000000001}
    with inbound_file_context(context):
        await read_inbound(descriptor, object())
        await write_failed(descriptor, OSError(errno.ENOSPC, "full"))
        await write_failed(descriptor, RuntimeError("other"))
    for method in (channel.read_inbound_file, channel.file_own_full, channel.file_not_saved):
        method.assert_awaited_once_with(descriptor, execution_context=context)
    assert current_inbound_file_context() is None
