"""Thread HTTP, decoding, SQL guards and teardown stay on their owning loop."""

import asyncio
import threading
from zoneinfo import ZoneInfo

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_threads import close, environment, inbound, origin

from app.telegram_bots.gateway_client import GatewayClientOwner

pytestmark = pytest.mark.asyncio


async def test_actual_default_httpx_tls_construction_and_close_are_off_loop(monkeypatch):
    from app.telegram_bots import gateway_client as module

    thread, calls = threading.get_ident(), []
    original = module._construct

    def construct(*args):
        calls.append(threading.get_ident())
        return original(*args)

    monkeypatch.setattr(module, "_construct", construct)
    client = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/api")
    try:
        await client.start()
        assert calls and all(identity != thread for identity in calls)
    finally:
        await client.stop()
    assert client.closed


async def test_actual_sql_sdk_and_cold_owner_custom_config_share_the_gateway_loop(db, monkeypatch, tmp_path):
    from deerflow.config import agents_config

    await asyncio.to_thread(ZoneInfo, "America/New_York")
    e = await environment(db, monkeypatch, lead=False, off_loop=True)
    path = tmp_path / "owned-agent.json"
    await asyncio.to_thread(path.write_text, '{"name":' + '"' + e.link.agent_ref + '"}', encoding="utf-8")
    thread, loop, reads = threading.get_ident(), asyncio.get_running_loop(), []

    def load(name, *, user_id):
        assert user_id == e.link.owner_user_id and name == e.link.agent_ref
        reads.append(threading.get_ident())
        return agents_config.AgentConfig.model_validate_json(path.read_text(encoding="utf-8"))

    monkeypatch.setattr(agents_config, "load_agent_config", load)
    try:
        identity = await e.threads.ensure_thread(e.channel.link, inbound(e), origin=await origin(e))
        assert identity and asyncio.get_running_loop() is loop
        assert reads and all(value != thread for value in reads)
        assert [request.method for request in e.http_calls] == ["POST", "GET"]
    finally:
        await close(e)
