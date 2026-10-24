"""Prepared mappings feed the existing manager without activating a run."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_threads import close, environment, inbound, origin

from app.channels.manager import ChannelManager
from app.channels.message_bus import MessageBus
from app.channels.store import ChannelStore

pytestmark = pytest.mark.asyncio


async def test_actual_manager_reuses_the_prepared_sql_mapping_without_sdk_creation(db, monkeypatch, tmp_path):
    e = await environment(db, monkeypatch)
    manager = ChannelManager(MessageBus(), ChannelStore(str(tmp_path / "manager.json")), connection_repo=db.connections, langgraph_url="https://runtime.example/base/api", gateway_url="https://gateway.example")
    try:
        message = inbound(e)
        prepared = await e.threads.ensure_thread(e.link, message, origin=await origin(e))
        before = len(e.http_calls)
        # Exercise the real consumer against the same already-owned public SDK
        # object. Lookup must return before it can invoke threads.create/runs.
        identity, created = await manager._get_or_create_thread(e.http_client._client, message)
        assert identity == prepared and created is False
        assert identity != message.metadata["preferred_thread_id"]
        assert len(e.http_calls) == before
        assert sum(request.method == "POST" for request in e.http_calls) == 1
    finally:
        await manager.stop()
        await close(e)
