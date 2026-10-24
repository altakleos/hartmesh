"""Host-side removal retires a live managed client without a final chat notice."""

import asyncio

import pytest
from _telegram_repository_test_support import bind_link, rows
from _telegram_repository_test_support import db as db
from test_telegram_cli import accounts as accounts
from test_telegram_cli import app_config
from test_telegram_cli import isolated_engine as isolated_engine
from test_telegram_copy_guard_supervisor import environment, owned_link, stop_environment, wait_until
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots import cli
from deerflow.persistence.telegram_bots.model import TelegramBotLinkEventRow, TelegramBotLinkRow, TelegramPersonSettingsRow


@pytest.mark.asyncio
async def test_cli_removal_is_observed_by_live_reconcile_without_transport_or_recreation(db, accounts, monkeypatch):
    def script(link, normal, updates):
        updates.enqueue("getUpdates", result=[], release=asyncio.Event())

    instance = environment(db, monkeypatch, script=script)
    link, token = await owned_link(db, instance, bot=9_000_002_001, owner=accounts)
    link = await bind_link(db, link)
    try:
        await instance.supervisor.check()
        await instance.supervisor.start()
        await wait_until(lambda: bool(instance.channels.created[link.id].updates.calls))
        assert instance.supervisor.channel(link.id) is not None
        removed = await cli.remove(db.sessions, config=app_config(), local_account="fixture@example.com")
        assert removed["removed"] == 1 and removed["bots"][0]["link"] == link.id
        assert await db.links.get(link.id) is None
        await instance.supervisor.reconcile()
        assert instance.supervisor.channel(link.id) is None
        assert instance.supervisor.poll_task(link.id) is None
        assert instance.supervisor.status()["bots"] == 0
        transport = instance.channels.created[link.id]
        assert transport.normal.closed and transport.updates.closed
        assert [call.endpoint for call in transport.normal.calls] == ["getMe"]
        assert not await rows(db, TelegramBotLinkRow)
        assert [row.owner_user_id for row in await rows(db, TelegramPersonSettingsRow)] == [accounts]
        events = [row for row in await rows(db, TelegramBotLinkEventRow) if row.action == "remove"]
        assert len(events) == 1 and events[0].actor_id == "host" and events[0].detail == {"by": "operator"}
        await instance.supervisor.reconcile()
        assert not await rows(db, TelegramBotLinkRow)
        assert [call.endpoint for call in transport.normal.calls] == ["getMe"]
    finally:
        await stop_environment(instance)
