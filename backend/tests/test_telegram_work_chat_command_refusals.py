"""Definitive command refusal owns a durable reply and completes ingress."""

import pytest
from _telegram_bot_test_support import make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_chat_runtime import installed_work_runtime
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("word", ["work", "status", "assign"])
async def test_use_only_command_gets_one_fixed_refusal_and_no_private_work(work_delivery, word):
    e = work_delivery
    owner = await installed_work_runtime(e)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1
    try:
        update = make_update(6301, account=e.link.chat_id, text="/" + word + (" Prepare the guide" if word == "assign" else ""))
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6301, e.link.chat_id))
            assert mark is not None and mark.route_complete
            assert len(mark.delivery_ids) == 1
            row = await reader.get(TelegramOutboxRow, mark.delivery_ids[0])
            assert row is not None and row.parts
            e.identity = row.id
            keys = [part["source_ref"]["key"] for part in row.parts]
            assert keys == ["assign.no_access" if word == "assign" else "work.need_inspect"]
            assert not list(await reader.scalars(select(TelegramActRow)))
        await dispatch(e, 6302)
        assert not e.dispatch_errors, e.dispatch_errors
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            assert len(list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.state != "skipped")))) == 1
    finally:
        await owner.stop()


@pytest.mark.asyncio
async def test_disabled_assignments_get_a_fixed_manager_settings_link_without_a_model(work_delivery, monkeypatch):
    from deerflow.agent_instances.contract import DefinitionSnapshot

    e = work_delivery
    e.sender.outbox.config = e.sender.outbox.config.model_copy(update={"public_base_url": "https://product.example.test"})
    owner = await installed_work_runtime(e)

    async def unexpected(*args, **kwargs):
        pytest.fail("Disabled Work reached assignment extraction")

    monkeypatch.setattr("app.telegram_bots.assignment_requests.draft_assignment", unexpected)
    async with e.db.sessions.begin() as writer:
        instance = await writer.get(AgentInstanceRow, e.instance.id)
        old = await writer.get(AgentDefinitionRevisionRow, instance.definition_revision)
        snapshot = DefinitionSnapshot.capture(owner_id=old.owner_id, config={**old.config, "work_policy": {"enabled": False}}, soul=old.soul)
        writer.add(AgentDefinitionRevisionRow(revision=snapshot.revision, owner_id=snapshot.owner_id, config=snapshot.config, soul=snapshot.soul))
        await writer.flush()
        instance.definition_revision = snapshot.revision
    try:
        update = make_update(6311, account=e.link.chat_id, text="/assign Review the guide")
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6311, e.link.chat_id))
            assert mark.route_complete
            assert len(mark.delivery_ids) == 1
            row = await reader.get(TelegramOutboxRow, mark.delivery_ids[0])
            e.identity = row.id
            assert row.parts[0]["source_ref"]["key"] == "assign.no_work"
            assert row.parts[0]["url_button"]["page"] == "web.agentSettings"
            assert not list(await reader.scalars(select(TelegramActRow)))
        await dispatch(e, 6312)
        assert not e.dispatch_errors, e.dispatch_errors
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
    finally:
        await owner.stop()
