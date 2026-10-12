"""Account download acquisition, reads and cleanup run on filesystem workers."""

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.gateway.routers import account_export


@pytest.mark.asyncio
async def test_account_download_open_read_and_close_leave_the_event_loop(monkeypatch):
    loop_thread = threading.get_ident()
    calls = []

    class Handle:
        def read(self, _size):
            assert threading.get_ident() != loop_thread
            calls.append("read")
            return b"part" if calls.count("read") == 1 else b""

        def close(self):
            assert threading.get_ident() != loop_thread
            calls.append("close")

    class Source:
        def open(self, mode):
            assert mode == "rb" and threading.get_ident() != loop_thread
            calls.append("open")
            return Handle()

    job = SimpleNamespace(user_id="alice", parts=[Source()], native_sources=None, cancel=threading.Event())
    download = SimpleNamespace(job=job, path=job.parts[0], number=1, size=4)
    ended = []
    service = SimpleNamespace(open_part=lambda *_args: download, validate_job=AsyncMock(), status=lambda _owner: job, close_part=lambda _download, **kwargs: ended.append(kwargs["complete"]))
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(account_export=service)), state=SimpleNamespace())
    monkeypatch.setattr(account_export, "_person", lambda _request: SimpleNamespace(id="alice"))
    response = await account_export.download_part(1, request)
    sent = []

    async def send(message):
        sent.append(message)

    await asyncio.wait_for(response({"type": "http", "asgi": {"spec_version": "2.4"}, "method": "GET", "headers": []}, AsyncMock(), send), 2)
    assert calls == ["open", "read", "read", "close"]
    assert ended == [True]
    assert b"".join(message.get("body", b"") for message in sent) == b"part"
