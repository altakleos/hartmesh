"""Known save refusals retain current wording and the original retry owner."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_assignment_card_delivery import prepare_card, save_card
from test_telegram_work_execution_outcomes import dispatch_edit
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.agent_instances.contract import DefinitionSnapshot
from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.agent_instances.work import AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


async def set_work_policy(e, enabled):
    async with e.db.sessions.begin() as writer:
        instance = await writer.get(AgentInstanceRow, e.instance.id)
        old = await writer.get(AgentDefinitionRevisionRow, instance.definition_revision)
        snapshot = DefinitionSnapshot.capture(owner_id=old.owner_id, config={**old.config, "work_policy": {**old.config["work_policy"], "enabled": enabled}}, soul=old.soul)
        if await writer.get(AgentDefinitionRevisionRow, snapshot.revision) is None:
            writer.add(AgentDefinitionRevisionRow(revision=snapshot.revision, owner_id=snapshot.owner_id, config=snapshot.config, soul=snapshot.soul))
            await writer.flush()
        instance.definition_revision = snapshot.revision


@pytest.mark.asyncio
@pytest.mark.parametrize("manager", [False, True])
async def test_saved_work_off_refusal_is_delivered_and_can_retry_same_operation(work_delivery, manager):
    e = work_delivery
    e.sender.outbox.config = e.sender.outbox.config.model_copy(update={"public_base_url": "https://product.example.test"})
    plans, draft, _ = await prepare_card(e)
    await dispatch(e, 6431)
    _button, admitted = await save_card(e, plans)
    async with e.db.sessions.begin() as writer:
        if not manager:
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
    await set_work_policy(e, False)
    refused = await e.acts.check(admitted.id)
    assert (refused.state, refused.reason) == ("conflict", "work-off")
    await plans.observe(e.link, draft.id)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        source = row.payload["work_news_basis"]["source"]
        assert source["work"]["notice"]["key"] == "assign.no_work"
        controls = [button for line in row.payload["product_keyboard_basis"] for button in line]
        assert any(button.get("kind") == "act_check" and button["target_id"] == admitted.id for button in controls)
        assert any("tab=settings" in button.get("url", "") for button in controls) is manager
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == admitted.id)) == 0
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1
    await dispatch_edit(e, 6431)
    assert not e.dispatch_errors, e.dispatch_errors
    await set_work_policy(e, True)
    retry = await selected(e, "act_check")
    saved = await plans.retry(e.link, retry.id, chat_id=e.link.chat_id, message_id=retry.message_id, message_thread_id=None, guard=await guard_for(e))
    assert saved.state == "done" and saved.id == admitted.id
    await plans.observe(e.link, draft.id)
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == admitted.id)) == 1


@pytest.mark.asyncio
async def test_work_off_wording_is_retired_when_current_policy_is_enabled(work_delivery):
    e = work_delivery
    plans, draft, _ = await prepare_card(e)
    await dispatch(e, 6441)
    _button, admitted = await save_card(e, plans)
    await set_work_policy(e, False)
    assert (await e.acts.check(admitted.id)).reason == "work-off"
    await plans.observe(e.link, draft.id)
    await set_work_policy(e, True)
    await plans.observe(e.link, draft.id)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, e.identity)
        source = row.payload["work_news_basis"]["source"]
        assert source["work"]["notice"]["key"] == "assign.not_saved"
        assert not source["work"].get("settings_url")
    await dispatch_edit(e, 6441)
    assert not e.dispatch_errors, e.dispatch_errors
    assert not any("switched off" in call.parameters.get("text", "") for call in e.channel.bot.request.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["manager", "inspect", "receipt"])
async def test_refusal_last_sdk_fence_rechecks_grants_and_actual_receipt(work_delivery, monkeypatch, change):
    from test_telegram_bot_adapter import message

    e = work_delivery
    e.sender.outbox.config = e.sender.outbox.config.model_copy(update={"public_base_url": "https://product.example.test"})
    plans, draft, _ = await prepare_card(e)
    await dispatch(e, 6451)
    _button, admitted = await save_card(e, plans)
    await set_work_policy(e, False)
    assert (await e.acts.check(admitted.id)).reason == "work-off"
    await plans.observe(e.link, draft.id)
    sources = e.channel._services.assignment_sources
    original, changed = sources.settings.get, []

    async def retire(*args, **kwargs):
        settings = await original(*args, **kwargs)
        if not changed:
            changed.append(True)
            if change == "receipt":
                await set_work_policy(e, True)
                current = await e.acts._work_record(admitted.id)
                saved = await e.acts.check(admitted.id, expected={"id": current.id, "kind": current.kind, "body": current.body, "attempts": current.attempts})
                changed.append(saved.state)
            else:
                async with e.db.sessions.begin() as writer:
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3 if change == "manager" else 1
        return settings

    monkeypatch.setattr(sources.settings, "get", retire)
    before = len(e.channel.bot.request.calls)
    message(e.channel.bot.request, "editMessageText", identity=6451)
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed
    if change == "receipt":
        assert changed == [True, "done"]
    calls = [call.parameters for call in e.channel.bot.request.calls[before:] if call.endpoint in {"editMessageText", "sendMessage"}]
    assert not calls
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == admitted.id)) == (1 if change == "receipt" else 0)
