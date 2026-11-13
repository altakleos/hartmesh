"""Rejected credentials produce one other-bot notice through the real sender."""

from copy import deepcopy

import pytest
from _telegram_repository_test_support import add_link, bind_link
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_handlers import sent
from test_telegram_callback_commands import drain
from test_telegram_callback_commands import setup as command_setup
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramOutboxRow


async def setup(db, monkeypatch, **options):
    from deerflow.config import app_config

    e = await command_setup(db, monkeypatch, **options)
    monkeypatch.setattr(app_config, "peek_loaded_app_config", lambda: e.live.config)
    return e


@pytest.mark.asyncio
@pytest.mark.parametrize("repaired", [False, True])
async def test_rejected_notice_prefers_chat_and_deduplicates_revision(db, monkeypatch, repaired, *, off_loop=False):
    from app.telegram_bots.rejected_notices import RejectedNotices

    e = await setup(db, monkeypatch, off_loop=off_loop)
    e.services.authority = e.live.authority
    e.sender.proactive = True
    try:
        rejected = await bind_link(db, await add_link(db, owner=e.live.id, bot=9_000_000_022))
        async with db.sessions.begin() as writer:
            row = await writer.get(TelegramBotLinkRow, rejected.id)
            row.health = "token_rejected"
            for item in await writer.scalars(select(TelegramOutboxRow)):
                item.state, item.skip_reason = "skipped", "superseded"
        owner = RejectedNotices(e.services)
        identity = await owner.plan(await db.links.get(rejected.id))
        assert identity and await owner.plan(await db.links.get(rejected.id)) == identity
        async with db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            assert row.link_id == e.link.id and row.class_ == "service"
            assert row.dedupe_key == f"rejected:{rejected.id}:{rejected.token_revision}"
        if repaired:
            async with db.sessions.begin() as writer:
                (await writer.get(TelegramBotLinkRow, rejected.id)).health = "ok"
        else:
            e.request.enqueue("sendMessage", result=sent(e, identity=501))
        before = len(e.request.calls)
        await drain(e)
        async with db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            assert row.state == ("skipped" if repaired else "sent")
            if repaired:
                assert row.skip_reason == "superseded"
        assert len(e.request.calls) == before + (0 if repaired else 1)
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["account", "unhealthy", "gone"])
async def test_rejected_notice_has_no_ineligible_recipient(db, monkeypatch, change):
    from app.telegram_bots.rejected_notices import RejectedNotices

    e = await setup(db, monkeypatch)
    e.services.authority = e.live.authority
    try:
        rejected = await bind_link(db, await add_link(db, owner=e.live.id, bot=9_000_000_022), account=7_000_000_002 if change == "account" else 7_000_000_001)
        async with db.sessions.begin() as writer:
            (await writer.get(TelegramBotLinkRow, rejected.id)).health = "token_rejected"
            if change == "unhealthy":
                (await writer.get(TelegramBotLinkRow, e.link.id)).health = "stopped"
            elif change == "gone":
                (await writer.get(TelegramBotLinkRow, e.link.id)).state = "blocked"
        assert await RejectedNotices(e.services).plan(await db.links.get(rejected.id)) is None
        async with db.sessions() as reader:
            assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.startswith("rejected:"))))
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_rejected_notice_rechecks_source_after_final_provider(db, monkeypatch):
    from app.gateway import authz
    from app.telegram_bots import notification_authority
    from app.telegram_bots.rejected_notices import RejectedNotices

    e = await setup(db, monkeypatch)
    e.services.authority = e.live.authority
    e.sender.proactive = True
    try:
        rejected = await bind_link(db, await add_link(db, owner=e.live.id, bot=9_000_000_022))
        async with db.sessions.begin() as writer:
            (await writer.get(TelegramBotLinkRow, rejected.id)).health = "token_rejected"
            for item in await writer.scalars(select(TelegramOutboxRow)):
                item.state, item.skip_reason = "skipped", "superseded"
        assert await RejectedNotices(e.services).plan(await db.links.get(rejected.id))
        original, resolve = notification_authority.authorize_notification, authz.resolve_route_permissions
        armed, changed = [], []

        async def final(*args, **kwargs):
            armed.append(True)
            return await original(*args, **kwargs)

        async def provider(*args, **kwargs):
            permissions = await resolve(*args, **kwargs)
            if armed and not changed:
                async with db.sessions.begin() as writer:
                    (await writer.get(TelegramBotLinkRow, rejected.id)).health = "ok"
                changed.append(True)
            return permissions

        monkeypatch.setattr(notification_authority, "authorize_notification", final)
        monkeypatch.setattr(authz, "resolve_route_permissions", provider)
        before = len(e.request.calls)
        await drain(e)
        assert changed and len(e.request.calls) == before
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
async def test_rejected_notice_uses_unprotected_instance_when_no_ordinary_chat(work_delivery, protected):
    from app.telegram_bots.rejected_notices import RejectedNotices
    from deerflow.persistence.agent_instances.model import AgentInstanceRow
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    e = work_delivery
    services = e.channel._services

    async def values(link):
        return {"name": "Other agent", "product": "HartMesh"}

    services.prepare_ingress_values = values
    services.prepare_live_owner = e.live.authority.prepare_live_owner
    rejected = await bind_link(e.db, await add_link(e.db, owner=e.link.owner_user_id, bot=9_000_000_022))
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramBotLinkRow, rejected.id)).health = "token_rejected"
        if protected:
            (await writer.get(AgentInstanceRow, e.link.agent_ref)).custody = "company"
            writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=e.sender.outbox.clock()))
        for item in await writer.scalars(select(TelegramOutboxRow)):
            item.state, item.skip_reason = "skipped", "superseded"
    identity = await RejectedNotices(services).plan(await e.db.links.get(rejected.id))
    assert bool(identity) is not protected
    if identity:
        from test_telegram_bot_adapter import message as sdk_message

        sdk_message(e.channel.bot.request, identity=5701)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            assert row.state == "sent", e.dispatch_errors


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["name", "url"])
async def test_rejected_notice_cannot_change_frozen_recipe(db, monkeypatch, change):
    from app.telegram_bots.rejected_notices import RejectedNotices

    e = await setup(db, monkeypatch)
    e.live.config.model_extra["telegram_bots"] = {"public_base_url": "https://example.test"}
    e.services.authority = e.live.authority
    e.sender.proactive = True
    try:
        rejected = await bind_link(db, await add_link(db, owner=e.live.id, bot=9000000022))
        async with db.sessions.begin() as writer:
            (await writer.get(TelegramBotLinkRow, rejected.id)).health = "token_rejected"
            for row in await writer.scalars(select(TelegramOutboxRow)):
                row.state, row.skip_reason = "skipped", "superseded"
        identity = await RejectedNotices(e.services).plan(await db.links.get(rejected.id))
        assert identity
        async with db.sessions.begin() as writer:
            row = await writer.get(TelegramOutboxRow, identity)
            parts = deepcopy(row.parts)
            if change == "name":
                parts[0]["source_ref"]["values"]["name"] = "A substituted bot identity"
            else:
                assert "url_button" in parts[0]
                parts[0]["url_button"]["url"] += "&agent=unrelated"
            row.parts = parts
        before = len([c for c in e.request.calls if c.endpoint == "sendMessage"])
        e.request.enqueue("sendMessage", result=sent(e, identity=501))
        await drain(e)
        calls = [c for c in e.request.calls if c.endpoint == "sendMessage"]
        assert len(calls) == before, calls[-1].parameters if calls else ""
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_publication_final_query_binds_actual_notice(db, monkeypatch):
    from app.telegram_bots.authority import _PreparedLiveOwner
    from app.telegram_bots.producer import DeliveryClaimRetired
    from app.telegram_bots.rejected_notices import RejectedNotices

    e = await setup(db, monkeypatch)
    e.services.authority = e.live.authority
    try:
        rejected = await bind_link(db, await add_link(db, owner=e.live.id, bot=9000000022))
        async with db.sessions.begin() as writer:
            (await writer.get(TelegramBotLinkRow, rejected.id)).health = "token_rejected"
        key = f"rejected:{rejected.id}:{rejected.token_revision}"
        original = _PreparedLiveOwner.__call__
        changed = []

        async def owner(self, who, writer):
            await original(self, who, writer)
            row = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == key))
            if row is not None and not changed:
                changed.append(True)
                parts = deepcopy(row.parts)
                parts[0]["source_ref"]["values"]["name"] = "Changed after preparation"
                row.parts = parts
                await writer.flush()

        monkeypatch.setattr(_PreparedLiveOwner, "__call__", owner)
        with pytest.raises(DeliveryClaimRetired):
            await RejectedNotices(e.services).plan(await db.links.get(rejected.id))
        assert changed
        async with db.sessions() as reader:
            assert await reader.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.dedupe_key == key)) is None
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_rejected_basis_cannot_authorize_foreign_origin(db, monkeypatch):
    from app.telegram_bots.rejected_notices import RejectedNotices

    e = await setup(db, monkeypatch)
    e.services.authority = e.live.authority
    e.sender.proactive = True
    try:
        rejected = await bind_link(db, await add_link(db, owner=e.live.id, bot=9000000022))
        async with db.sessions.begin() as writer:
            (await writer.get(TelegramBotLinkRow, rejected.id)).health = "token_rejected"
            for row in await writer.scalars(select(TelegramOutboxRow)):
                row.state, row.skip_reason = "skipped", "superseded"
        identity = await RejectedNotices(e.services).plan(await db.links.get(rejected.id))
        assert identity
        async with db.sessions.begin() as writer:
            row = await writer.get(TelegramOutboxRow, identity)
            parts = deepcopy(row.parts)
            from app.telegram_bots.links import BotLink
            from app.telegram_bots.rejected_notices import recipe

            payload = deepcopy(row.payload)
            payload["rejected_basis"]["public_base_url"] = "https://unrelated.example"
            source = await writer.get(TelegramBotLinkRow, rejected.id)
            parts[0].update(recipe(payload["rejected_basis"], BotLink.from_row(source)))
            row.payload = payload
            row.parts = parts
        before = len([c for c in e.request.calls if c.endpoint == "sendMessage"])
        e.request.enqueue("sendMessage", result=sent(e, identity=501))
        await drain(e)
        calls = [c for c in e.request.calls if c.endpoint == "sendMessage"]
        assert len(calls) == before, calls[-1].parameters if calls else ""
    finally:
        await e.sender.stop()
        await e.channel.stop()
