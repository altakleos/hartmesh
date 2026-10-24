"""Live owners and exact SQL bindings authorize only supported saved sources."""

from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _telegram_repository_test_support import add_link, bind_link, code_of
from _telegram_repository_test_support import db as db
from sqlalchemy import select

from app.telegram_bots import authority
from app.telegram_bots.adapter import SendContext
from app.telegram_bots.producer import OutboxItem
from deerflow.config.app_config import AppConfig
from deerflow.persistence.channel_connections.model import ChannelConnectionRow
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

pytestmark = pytest.mark.asyncio


@pytest.fixture
def current_owner(monkeypatch):
    user = SimpleNamespace(id="owner-a", system_role="user", disabled_at=None, released_at=None, oauth_provider=None, needs_setup=True)
    provider = SimpleNamespace(get_user=AsyncMock(return_value=user))
    monkeypatch.setattr(authority, "get_local_provider", lambda: provider)
    config = AppConfig(sandbox={"use": "deerflow.sandbox.local:LocalSandboxProvider"})
    monkeypatch.setattr("deerflow.config.app_config.get_app_config", lambda: config)
    monkeypatch.setattr("deerflow.config.agents_config.load_agent_config", lambda name, *, user_id: SimpleNamespace(name=name))
    return user, provider


async def basis(db, *, kind="custom_agent"):
    if kind == "custom_agent":
        link = await bind_link(db, await add_link(db))
    else:
        await db.settings.ensure("owner-a", timezone="America/New_York", locale="en-US")
        link = await db.links.add(
            owner_user_id="owner-a", agent_kind=kind, agent_ref="lead_agent" if kind == "lead_agent" else "instance-a", bot_id=9_000_000_001, bot_username="fixture_bot", encrypted_token="wrapped-fixture", actor_id="owner-a"
        )
        code = code_of(await db.links.mint_code(link))
        link = await db.links.bind(
            link,
            telegram_user_id=7_000_000_001,
            chat_id=7_000_000_001,
            first_name="Fixture",
            username="fixture_person",
            code=code,
            start_identity={"update_id": 201, "message_id": 101, "chat_id": 7_000_000_001},
            hello_parts=[{"source_ref": {"key": "hello.chat" if kind == "lead_agent" else "hello.assistant", "values": {"product": "HartMesh"}}}, {"source_ref": {"key": "hello.privacy", "values": {}}}],
        )
    async with db.sessions() as session:
        row = await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id))
        item = OutboxItem(**{name: getattr(row, name) for name in OutboxItem.__dataclass_fields__})
    return link, item


@pytest.mark.parametrize("kind", ["lead_agent", "custom_agent"])
async def test_live_owner_with_pending_session_setup_can_receive_fixed_ack(db, current_owner, kind):
    link, item = await basis(db, kind=kind)
    policy = authority.TelegramAuthority(links=db.links, connections=db.connections)
    decision = await policy.delivery(link, item)
    assert decision.decision == "allow" and decision.protect_content is False


@pytest.mark.parametrize("condition", ["disabled", "released", "missing"])
async def test_each_delivery_rereads_current_account_and_skips_disabled(db, current_owner, condition):
    link, item = await basis(db)
    user, provider = current_owner
    policy = authority.TelegramAuthority(links=db.links, connections=db.connections)
    assert (await policy.delivery(link, item)).decision == "allow"
    if condition == "missing":
        provider.get_user.return_value = None
    else:
        setattr(user, condition + "_at", datetime.now(UTC))
    assert (await policy.delivery(link, item)).reason == "disabled"


@pytest.mark.parametrize("field,value", [("status", "held"), ("owner_user_id", "other-owner"), ("workspace_id", "other-bot"), ("external_account_id", "other-person"), ("bot_user_id", "other-bot")])
async def test_exact_connection_authority_is_checked_each_time(db, current_owner, field, value):
    link, item = await basis(db)
    async with db.sessions.begin() as session:
        row = await session.get(ChannelConnectionRow, link.connection_id)
        setattr(row, field, value)
    decision = await authority.TelegramAuthority(links=db.links, connections=db.connections).delivery(link, item)
    assert decision.reason == "link-not-connected"


async def test_stale_saved_generation_is_not_adopted_by_current_binding(db, current_owner):
    link, item = await basis(db)
    decision = await authority.TelegramAuthority(links=db.links, connections=db.connections).delivery(link, replace(item, binding_generation=item.binding_generation - 1))
    assert decision.reason == "link-not-connected"


async def test_unreadable_configuration_waits_instead_of_granting_or_discarding(db, current_owner, monkeypatch):
    link, item = await basis(db)

    def unreadable():
        raise FileNotFoundError("not readable")

    monkeypatch.setattr("deerflow.config.app_config.get_app_config", unreadable)
    assert (await authority.TelegramAuthority(links=db.links, connections=db.connections).delivery(link, item)).decision == "wait"


async def test_missing_custom_target_waits_but_reserved_status_has_live_authority(db, current_owner, monkeypatch):
    link, item = await basis(db)

    def unavailable(*args, **kwargs):
        raise FileNotFoundError("target removed")

    monkeypatch.setattr("deerflow.config.agents_config.load_agent_config", unavailable)
    policy = authority.TelegramAuthority(links=db.links, connections=db.connections)
    assert await policy.target(link) == "agent_unavailable"
    assert (await policy.delivery(link, item)).decision == "wait"
    status = replace(item, payload={"delivery_kind": "status_notice"})
    # Producer owns the reserved status key/payload structural validation.
    assert (await policy.delivery(link, status)).decision == "allow"


async def test_private_model_sources_remain_unsupported(db, current_owner):
    link, item = await basis(db)
    private = {"kind": "text", "source_ref": {"thread_id": "private", "assistant_message_id": "model"}}
    item = replace(item, parts=[private])
    assert (await authority.TelegramAuthority(links=db.links, connections=db.connections).delivery(link, item)).decision == "wait"


async def test_execution_permission_withdrawal_does_not_govern_fixed_saved_text(db, current_owner, monkeypatch):
    link, item = await basis(db)
    monkeypatch.setattr("app.gateway.authz.resolve_route_permissions", AsyncMock(return_value=["threads:read"]))
    assert (await authority.TelegramAuthority(links=db.links, connections=db.connections).delivery(link, item)).decision == "allow"


async def test_require_checks_named_read_ceiling_without_imposing_it_on_fixed_text(current_owner, monkeypatch):
    monkeypatch.setattr("app.gateway.authz.resolve_route_permissions", AsyncMock(return_value=["runs:create"]))
    with pytest.raises(authority.CeilingRefused):
        await authority.require("owner-a", "threads:read")
    assert await authority.require("owner-a") == frozenset({"runs:create"})


async def test_access_off_notice_can_select_protection_after_disable_but_never_retarget(db, current_owner):
    link, item = await basis(db)
    current_owner[0].disabled_at = datetime.now(UTC)
    policy = authority.TelegramAuthority(links=db.links, connections=db.connections)
    context = SendContext(link.id, link.chat_id, link.binding_generation, link.token_revision, "bound_message")
    assert await policy.protect_notice(context) is False
    with pytest.raises(Exception) as caught:
        await policy.protect_notice(replace(context, binding_generation=context.binding_generation + 1))
    assert type(caught.value).__name__ == "DeliveryClaimRetired"


async def test_held_target_requires_external_connection_restore_and_current_live_owner(db, current_owner):
    link, item = await basis(db)
    held = await db.links.hold_if_current(link.id, token_revision=link.token_revision, binding_generation=link.binding_generation)
    policy = authority.TelegramAuthority(links=db.links, connections=db.connections)
    assert await policy.target(held) == "held"
    assert await db.connections.restore_held_connection(held.connection_id, owner_user_id=held.owner_user_id) == "restored"
    assert await policy.target(held) == "ok"
    # Restoration alone is never positive account authority.
    current_owner[0].disabled_at = datetime.now(UTC)
    assert await policy.target(held) == "held"


async def test_instance_delivery_and_notice_protection_fail_closed_until_policy_consumer(db, current_owner):
    from app.telegram_bots.producer import DeliveryClaimRetired

    link, item = await basis(db, kind="instance")
    policy = authority.TelegramAuthority(links=db.links, connections=db.connections)
    assert await policy.target(link) == "unsupported"
    assert (await policy.delivery(link, item)).decision == "wait"
    with pytest.raises(DeliveryClaimRetired):
        await policy.protect_notice(SendContext(link.id, link.chat_id, link.binding_generation, link.token_revision, "bound_message"))
