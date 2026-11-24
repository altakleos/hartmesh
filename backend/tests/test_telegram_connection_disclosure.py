from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.telegram_bots import presentation
from app.telegram_bots.adapter import TelegramBotChannel


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("key", "locale", "text"),
    [
        ("new.done", "en", "Send /privacy"),
        ("new.done_instance", "en", "Send /privacy"),
        ("hello.employee", "en", "Send /privacy"),
        ("hello.assistant", "zh-CN", "/privacy"),
        ("privacy.full", "en", "the conversation on the web"),
        ("privacy.full", "zh-CN", "网页"),
    ],
)
async def test_render_uses_live_authority_and_only_current_connection_names(monkeypatch, key, locale, text):
    link = SimpleNamespace(agent_kind="instance", agent_ref="agent", owner_user_id="person", binding_generation=1)
    authority = SimpleNamespace(instance_send_policy=AsyncMock(return_value="allowed"), instance_view=AsyncMock(return_value=SimpleNamespace(name="Robin")))
    services = SimpleNamespace(product_name="Workspace", authority=authority, settings=SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(locale=locale))))
    monkeypatch.setattr(presentation, "connections_enabled", lambda: True)
    monkeypatch.setattr(presentation, "visible_connections", lambda instance: [{"name": "Notes & Files", "host": "private.example.test"}])
    value = {"key": key, "values": {"agent": "Robin", "name": "Robin", "role": None, "role_description": None, "email": "member@example.test", "connection_names": "stale-name"}}
    channel = SimpleNamespace(_services=services)
    out = await TelegramBotChannel._render_source(channel, link, value)
    assert text in out and "Notes &amp; Files" in out
    assert "private.example" not in out and "stale-name" not in out
    authority.instance_send_policy.assert_awaited_once_with(link, require_use=True)
    assert "Notes" not in str(value)


@pytest.mark.asyncio
async def test_no_names_when_access_or_feature_off_and_no_old_chat_broadcast(monkeypatch):
    link = SimpleNamespace(agent_kind="instance", agent_ref="a", owner_user_id="person", binding_generation=1)
    auth = SimpleNamespace(instance_send_policy=AsyncMock(return_value="off"), instance_view=AsyncMock())
    channel = SimpleNamespace(_services=SimpleNamespace(product_name="Workspace", authority=auth, settings=SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(locale="en")))))
    monkeypatch.setattr(presentation, "visible_connections", lambda *_: pytest.fail("no connection read is authorized"))
    for enabled, kind, key in [(True, "instance", "new.done"), (False, "instance", "privacy.full"), (True, "lead_agent", "privacy.full"), (True, "instance", "hello.again")]:
        monkeypatch.setattr(presentation, "connections_enabled", lambda: enabled)
        link.agent_kind = kind
        out = await TelegramBotChannel._render_source(channel, link, {"key": key, "values": {"connection_names": "old-secret-name"}})
        assert "old-secret-name" not in out and "can use:" not in out
    auth.instance_send_policy.assert_awaited_once()


@pytest.mark.asyncio
async def test_long_roster_is_bounded_and_unreadable_store_is_explicit(monkeypatch):
    from app.telegram_bots.render import render_plain
    from app.telegram_bots.split import utf16_units

    link = SimpleNamespace(agent_kind="instance", agent_ref="agent")
    services = SimpleNamespace(authority=SimpleNamespace(instance_send_policy=AsyncMock(return_value="allowed"), instance_view=AsyncMock(return_value=SimpleNamespace(name="Robin"))))
    monkeypatch.setattr(presentation, "connections_enabled", lambda: True)
    monkeypatch.setattr(presentation, "visible_connections", lambda _: [{"name": "Notes"}, {"name": "📚" * 200}])
    text = await presentation.connection_disclosure(services, link, "privacy.full", "en", render_plain)
    assert "Notes, 1 more" in text and utf16_units(text) < 500

    def unavailable(_):
        raise ValueError("private configuration")

    monkeypatch.setattr(presentation, "visible_connections", unavailable)
    text = await presentation.connection_disclosure(services, link, "privacy.full", "en", render_plain)
    assert "temporarily unavailable" in text and "private configuration" not in text
