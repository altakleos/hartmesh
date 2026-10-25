"""The existing supervisor owns the sole conversation client and coordinator."""

import asyncio
import json

import httpx
import pytest
from _telegram_repository_test_support import db as db
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_supervisor import build
from test_telegram_threads import environment, inbound, origin

from app.telegram_bots.gateway_client import GatewayClientOwner

pytestmark = pytest.mark.asyncio


async def test_supervisor_composes_and_closes_one_runtime_client(db, monkeypatch):
    e = build(db, monkeypatch)
    e.supervisor.channel_service.manager.langgraph_url = "https://runtime.example/base/api"
    try:
        await e.supervisor.check()
        await e.supervisor.start()
        client = e.supervisor.thread_client
        assert client is not None and e.supervisor.threads.client is client
        assert not client.closed
        assert client.gateway_url == "http://127.0.0.1:8001"
        await e.supervisor.begin_shutdown()
        await e.supervisor.stop()
        assert client.closed
    finally:
        await e.supervisor.stop()


async def test_repeated_supervisor_stop_cancellation_retains_active_http_and_sdk_cleanup(db, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def respond(request):
        calls.append(request)
        entered.set()
        await release.wait()
        body = json.loads(request.content)
        return httpx.Response(200, json={"thread_id": body["thread_id"], "owner_user_id": request.headers["X-DeerFlow-Owner-User-Id"], "metadata": body["metadata"], "agent_instance": None})

    owner = build(db, monkeypatch)
    supervisor = owner.supervisor
    client = GatewayClientOwner(gateway_url=owner.service.manager.gateway_url, langgraph_url=owner.service.manager.langgraph_url, transport=httpx.MockTransport(respond))
    supervisor.thread_client = client
    e, operation, first, second = None, None, None, None
    try:
        await supervisor.check()
        await supervisor.start()
        await asyncio.wait_for(supervisor._send_event.wait(), 3)
        e = await environment(db, monkeypatch)
        await e.http_client.stop()
        e.http_client, e.threads = client, supervisor.threads
        supervisor.threads.authority = e.live.authority
        # Register the actual running pinned Application as the supervisor's
        # current transport; this test admits no input/model/polling work.
        supervisor._channels[e.link.id] = e.channel
        operation = asyncio.create_task(e.threads.ensure_thread(e.link, inbound(e), origin=await origin(e)))
        await asyncio.wait_for(entered.wait(), 3)
        first = asyncio.create_task(supervisor.stop())
        while not supervisor._stopping:
            await asyncio.sleep(0)
        first.cancel()
        second = asyncio.create_task(supervisor.stop())
        await asyncio.sleep(0)
        second.cancel()
        assert not client.closed and not e.request.closed
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await first
        with pytest.raises(asyncio.CancelledError):
            await second
        with pytest.raises(ValueError, match="thread-attempt-changed"):
            await operation
        await supervisor.stop()
        assert client.closed and e.request.closed and e.updates.closed
        assert [request.method for request in calls] == ["POST"]
        assert await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id)) is None
    finally:
        release.set()
        await asyncio.gather(*(task for task in (operation, first, second) if task is not None), return_exceptions=True)
        await supervisor.stop()
        if e is not None:
            await e.channel.stop()


async def test_actual_manager_drains_a_dispatched_wait_before_supervisor_closes_the_shared_client(db, monkeypatch, tmp_path):
    from test_channel_execution_client import Admission

    from app.channels.execution import ChannelExecutionContext, ChannelExecutionScope
    from app.channels.execution_client import ChannelExecutionFacade
    from app.channels.manager import ChannelManager
    from app.channels.message_bus import InboundMessage
    from app.channels.store import ChannelStore

    e = build(db, monkeypatch)
    entered, release, delivered = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = []

    async def respond(request):
        calls.append(request)
        if request.method == "POST":
            entered.set()
            await release.wait()
            return httpx.Response(200, json={"messages": [{"type": "ai", "content": "synthetic completed reply"}]})
        return httpx.Response(200, json={"thread_id": "original", "owner_user_id": "human", "agent_instance": None, "metadata": {"channel_agent_name": "lead_agent"}})

    async def outbound(message):
        if message.is_final and message.text == "synthetic completed reply":
            delivered.set()

    e.service.bus.subscribe_outbound(outbound)
    manager = ChannelManager(
        e.service.bus, ChannelStore(str(tmp_path / "channels.json")), connection_repo=db.connections, langgraph_url="https://runtime.example/api", gateway_url="https://gateway.example", max_concurrency=1, shutdown_grace_period_seconds=3
    )
    e.service.manager = manager
    client = GatewayClientOwner(gateway_url=manager.gateway_url, langgraph_url=manager.langgraph_url, transport=httpx.MockTransport(respond))
    e.supervisor.thread_client = client
    scope = ChannelExecutionScope("a" * 32, "human", None, "chat", "original", client.endpoint_id, "lead_agent", None)
    admission = Admission()
    facade = ChannelExecutionFacade(client, scope=scope, admission=admission)
    msg = InboundMessage(channel_name="slack", chat_id="chat", user_id="sender", text="synthetic input", owner_user_id="human", execution_required=True, execution_context=ChannelExecutionContext(scope, facade, admission))
    stopping = None
    try:
        await e.supervisor.check()
        await e.supervisor.start()
        await manager.start()
        await e.service.bus.publish_inbound(msg)
        await asyncio.wait_for(entered.wait(), 3)
        await e.supervisor.begin_shutdown()
        assert e.supervisor._subscribed and not client.closed
        assert e.supervisor.threads._stopped
        stopping = asyncio.create_task(manager.stop())
        await asyncio.sleep(0)
        admission.permitted.remove("run_create")
        result = await client.request_json(operation="run_read", owner_user_id="human", method="GET", path="threads/original/runs/retained", before_dispatch=lambda: admission.authorize("run_read"))
        assert result.status_code == 200 and not client.closed
        release.set()
        await asyncio.wait_for(stopping, 3)
        await asyncio.wait_for(delivered.wait(), 3)
        assert not client.closed
        assert sum(request.method == "POST" for request in calls) == 1
        await e.supervisor.stop()
        assert client.closed and not e.supervisor._subscribed
    finally:
        release.set()
        if stopping is not None:
            await asyncio.gather(stopping, return_exceptions=True)
        await manager.stop()
        await e.supervisor.stop()
