"""Normal startup builds callback services from the default repositories."""

from types import SimpleNamespace

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_supervisor import ImmediateCopyDecision
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.channels.message_bus import MessageBus
from app.telegram_bots.acts import Acts
from app.telegram_bots.callbacks import CallbackRepository, Prompts
from app.telegram_bots.commands import CallbackCommands
from app.telegram_bots.supervisor import TelegramSupervisor


@pytest.mark.asyncio
async def test_default_repositories_are_wired_before_supervisor_check_returns(db, monkeypatch):
    from app.telegram_bots import supervisor as module

    monkeypatch.setattr(module, "static_cause", lambda *_args: None)
    monkeypatch.setattr(module, "get_session_factory", lambda: db.sessions)
    manager = SimpleNamespace(connection_repo=db.connections, gateway_url="http://127.0.0.1:8001", langgraph_url="http://127.0.0.1:8001/api")
    service = SimpleNamespace(bus=MessageBus(), manager=manager)
    supervisor = TelegramSupervisor(SimpleNamespace(state=SimpleNamespace()), config=db.config, channel_service=service, copy_guard=ImmediateCopyDecision(), app_config=object(), environ={})
    try:
        assert supervisor.settings is supervisor.outbox is None
        await supervisor.check()
        assert supervisor.cause() is None
        assert isinstance(supervisor.callbacks, CallbackRepository)
        assert isinstance(supervisor.prompts, Prompts)
        assert isinstance(supervisor.acts, Acts)
        assert isinstance(supervisor.commands, CallbackCommands)
        assert supervisor.commands.services is supervisor
        assert supervisor.acts.settings is supervisor.settings
        assert supervisor.acts.outbox is supervisor.outbox
        callbacks = supervisor.callbacks
        await supervisor.check()
        assert supervisor.callbacks is callbacks
    finally:
        await supervisor.stop()
