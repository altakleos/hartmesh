"""Original canonical suggestion and actual SDK receipt survive every wait."""

import json

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_callback_commands import press
from test_telegram_notification_runtime import runtime
from test_telegram_result_suggestion_drafts import ready_suggestion
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


async def damage_source(e, identity, selected, damage):
    async with e.db.sessions.begin() as writer:
        if damage == "event":
            event = await writer.get(AgentWorkEventRow, selected.payload["event_id"])
            event.request = {**event.request, "statement": "A different, unseen suggestion"}
        elif damage == "ack":
            parent = await writer.get(TelegramOutboxRow, identity)
            parent.parts = [{**part, "message_ids": []} for part in parent.parts]
        elif damage == "control":
            await writer.delete(await writer.get(TelegramCallbackRow, selected.id))
        elif damage == "marker":
            control = await writer.get(TelegramCallbackRow, selected.id)
            control.payload = {key: value for key, value in control.payload.items() if key != "suggestion_draft_id"}
        else:
            grant = await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))
            grant.permissions = 1


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
async def test_suggestion_callback_message_cannot_replace_the_actual_sdk_receipt(work_exports, monkeypatch):
    e, calls = work_exports, []
    owner = await runtime(e)

    async def model(**kwargs):
        calls.append(kwargs)
        return json.dumps({"objective": "Review the older evidence next.", "success_criteria": "List verified references"})

    monkeypatch.setattr("app.telegram_bots.assign.run_oneshot_llm", model)
    monkeypatch.setattr("app.telegram_bots.assign.get_app_config", lambda: e.live.config)
    try:
        _plans, identity, selected = await ready_suggestion(e)
        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramCallbackRow, selected.id)).message_id = 9999
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 930, selected.id, message_id=9999))
        await e.channel._drain_ingress()
        assert not calls
        async with e.db.sessions.begin() as writer:
            assert not list(await writer.scalars(select(TelegramActRow)))
            (await writer.get(TelegramCallbackRow, selected.id)).message_id = 911
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 931, selected.id, message_id=911))
        await e.channel._drain_ingress()
        assert len(calls) == 1
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
@pytest.mark.parametrize("damage", ["event", "ack", "control", "marker", "grant"])
async def test_suggestion_final_sdk_fence_refuses_lost_original_proof(work_exports, monkeypatch, damage):
    e, calls = work_exports, []
    owner = await runtime(e)

    async def model(**kwargs):
        calls.append(kwargs)
        return json.dumps({"objective": "Review the older evidence next.", "success_criteria": "List verified references"})

    monkeypatch.setattr("app.telegram_bots.assign.run_oneshot_llm", model)
    monkeypatch.setattr("app.telegram_bots.assign.get_app_config", lambda: e.live.config)
    try:
        _plans, identity, selected = await ready_suggestion(e)
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 912, selected.id, message_id=911))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            root = await reader.scalar(select(TelegramActRow))
            assert root is not None and root.body["version"] == 1
            card = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string() == root.id))
            assert card.parts and card.state == "pending"
        assert len(calls) == 1
        sources = owner.host.assignment_sources
        original, changed = sources.settings.get, []

        async def change(*args, **kwargs):
            result = await original(*args, **kwargs)
            if not changed:
                changed.append(True)
                await damage_source(e, identity, selected, damage)
            return result

        monkeypatch.setattr(sources.settings, "get", change)
        before = len(e.channel.bot.request.calls)
        await e.sender.tick()
        await e.sender.drain()
        assert changed
        assert not [call for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"]
        async with e.db.sessions() as reader:
            root = await reader.get(TelegramActRow, root.id)
            assert not root.operation_ids
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
@pytest.mark.parametrize("fallback", [False, True])
async def test_lost_suggestion_marker_during_extraction_never_repairs_or_repeats_admission(work_exports, monkeypatch, fallback):
    e, calls = work_exports, []
    owner = await runtime(e)
    identity = selected = None

    async def model(**kwargs):
        calls.append(kwargs)
        await damage_source(e, identity, selected, "marker")
        if fallback:
            return "invalid extraction"
        return json.dumps({"objective": "Review the older evidence next.", "success_criteria": "List verified references"})

    monkeypatch.setattr("app.telegram_bots.assign.run_oneshot_llm", model)
    monkeypatch.setattr("app.telegram_bots.assign.get_app_config", lambda: e.live.config)
    try:
        _plans, identity, selected = await ready_suggestion(e)
        for update in (916, 917):
            e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
            assert await e.channel.handle_update(press(e, update, selected.id, message_id=911))
            await e.channel._drain_ingress()
        assert len(calls) == 1
        async with e.db.sessions() as reader:
            root = await reader.scalar(select(TelegramActRow))
            assert root is not None and root.body["version"] == 2 and root.body["request"]["phase"] == "extracting"
            assert "suggestion_draft_id" not in (await reader.get(TelegramCallbackRow, selected.id)).payload
            assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string().is_not(None))))
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
@pytest.mark.parametrize("damage", ["event", "grant"])
@pytest.mark.parametrize("when", ["before", "during"])
async def test_suggestion_request_rechecks_canonical_source_and_inspect_across_extraction(work_exports, monkeypatch, damage, when):
    e, calls = work_exports, []
    owner = await runtime(e)
    identity = selected = None

    async def model(**kwargs):
        calls.append(kwargs)
        if when == "during":
            await damage_source(e, identity, selected, damage)
        return json.dumps({"objective": "Review the older evidence next.", "success_criteria": "List verified references"})

    monkeypatch.setattr("app.telegram_bots.assign.run_oneshot_llm", model)
    monkeypatch.setattr("app.telegram_bots.assign.get_app_config", lambda: e.live.config)
    try:
        _plans, identity, selected = await ready_suggestion(e)
        if when == "before":
            await damage_source(e, identity, selected, damage)
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 914, selected.id, message_id=911))
        await e.channel._drain_ingress()
        assert len(calls) == int(when == "during")
        async with e.db.sessions() as reader:
            roots = list(await reader.scalars(select(TelegramActRow)))
            assert len(roots) == int(when == "during")
            assert all(root.body["version"] == 2 and not root.operation_ids for root in roots)
            assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string().is_not(None))))
    finally:
        await owner.stop()
