"""Actual ingress SQL/SDK owners keep cold work off the Gateway loop."""

import asyncio
import json
import threading
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_bot_handlers import command_sender, ingress, membership, message, sent
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.supervisor import TelegramSupervisor
from blocking_io.test_telegram_live_owner_strict import cold_owner_config as cold_owner_config
from deerflow.config import app_config
from deerflow.config.agents_config import AgentConfig
from deerflow.utils.file_io import await_drained

pytestmark = pytest.mark.asyncio


@pytest.fixture
def cold_ingress_files(cold_owner_config, tmp_path):
    path = cold_owner_config
    path.write_text(path.read_text(encoding="utf-8") + "telegram_bots:\n  enabled: true\n  public_base_url: https://example.test\nui:\n  product_name: Cold Product\n", encoding="utf-8")
    return path, tmp_path / "custom-agent.json"


async def test_actual_application_prepares_cold_custom_config_and_navigation_off_loop(db, monkeypatch, cold_ingress_files):
    from app.telegram_bots import pages
    from deerflow.config import agents_config

    path, custom = cold_ingress_files
    cold_loader = app_config.get_app_config
    # The existing repository fixture validates its initial preferences on the
    # caller loop; prepare that fixture's zone, not the configuration under test.
    await asyncio.to_thread(ZoneInfo, "America/New_York")
    e = await ingress(db, monkeypatch=monkeypatch, off_loop=True)
    monkeypatch.setattr(app_config, "get_app_config", cold_loader)
    monkeypatch.setattr(pages, "get_app_config", cold_loader)
    await asyncio.to_thread(custom.write_text, json.dumps({"name": e.link.agent_ref, "display_name": "Cold custom target"}), encoding="utf-8")
    thread, loop, reads = threading.get_ident(), asyncio.get_running_loop(), []

    def read_agent(name, *, user_id):
        assert name == e.link.agent_ref and user_id == e.link.owner_user_id
        reads.append(threading.get_ident())
        return AgentConfig.model_validate_json(custom.read_text(encoding="utf-8"))

    monkeypatch.setattr(agents_config, "load_agent_config", read_agent)
    prepared = await await_drained(asyncio.to_thread(cold_loader))
    supervisor = TelegramSupervisor(SimpleNamespace(state=SimpleNamespace()), config=db.config, channel_service=None, session_factory=db.sessions, app_config=prepared, authority=e.live.authority)
    e.services.prepare_ingress_values = supervisor.prepare_ingress_values
    sender = None
    try:
        assert await e.channel.handle_update(message(e, 700)) is True
        assert asyncio.get_running_loop() is loop
        assert reads and all(identity != thread for identity in reads)
        e.request.enqueue("sendMessage", result=sent(e))
        sender = await command_sender(db, e)
        await sender.tick()
        await sender.drain()
        assert "Cold custom target" in e.request.calls[-1].parameters["text"]
        assert e.request.calls[-1].parameters["link_preview_options"]["is_disabled"] is True
    finally:
        if sender is not None:
            await sender.stop()
        await e.channel.stop()
        await supervisor.stop()


@pytest.mark.parametrize("operation", ["delete", "leave"])
async def test_disposable_sdk_attempt_is_cancelled_and_drained_before_client_close(db, operation):
    import secrets

    await asyncio.to_thread(ZoneInfo, "America/New_York")
    e = await ingress(db, off_loop=True)
    release = asyncio.Event()
    reply = e.request.enqueue("deleteMessage" if operation == "delete" else "leaveChat", result=True, release=release)
    if operation == "delete":
        update = message(e, 701, content="123456:" + secrets.token_urlsafe(30))
    else:
        update = membership(e, 701, status="member", chat=-100123, kind="supergroup", old="left")
    try:
        assert await e.channel.handle_update(update) is True
        await asyncio.wait_for(reply.started.wait(), 3)
        await e.channel.stop()
        assert reply.cancelled.is_set()
        assert not e.channel._ingress_tasks
        assert e.request.closed and e.updates.closed
        assert [call.endpoint for call in e.request.calls] == ["getMe", "deleteMessage" if operation == "delete" else "leaveChat"]
    finally:
        release.set()
        await e.channel.stop()
