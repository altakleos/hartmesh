"""Damaged owned locators retain their own proof without pinning foreign groups."""

import json
from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_callback_commands import press
from test_telegram_notification_runtime import runtime
from test_telegram_result_suggestion_drafts import ready_suggestion
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.assignment_retention import assignment_dependencies
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
@pytest.mark.parametrize("foreign", ["binding", "topic", "anchor"])
async def test_suggestion_source_group_retention_never_follows_foreign_locators(work_exports, monkeypatch, foreign):
    e = work_exports
    owner = await runtime(e)

    async def model(**kwargs):
        return json.dumps({"objective": "Review the older evidence next.", "success_criteria": "List verified references"})

    monkeypatch.setattr("app.telegram_bots.assign.run_oneshot_llm", model)
    monkeypatch.setattr("app.telegram_bots.assign.get_app_config", lambda: e.live.config)
    try:
        _plans, identity, selected = await ready_suggestion(e)
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 918, selected.id, message_id=911))
        await e.channel._drain_ingress()
        async with e.db.sessions.begin() as writer:
            control = await writer.get(TelegramCallbackRow, selected.id)
            fields = {column.name: getattr(control, column.name) for column in control.__table__.columns}
            foreign_group = uuid4().hex
            fields.update(id=uuid4().hex, group_id=foreign_group, payload=dict(control.payload))
            if foreign == "binding":
                fields["binding_generation"] += 1
            elif foreign == "topic":
                fields["message_thread_id"] = 42
            else:
                fields["payload"] = {**fields["payload"], "anchor_outbox_id": "f" * 32}
            writer.add(TelegramCallbackRow(**fields))
            anchor = await writer.get(TelegramOutboxRow, identity)
            anchor.parts = [*anchor.parts[:-1], {**anchor.parts[-1], "callback_group_id": foreign_group}]
        async with e.db.sessions() as reader:
            retained = await assignment_dependencies(reader, e.sender.outbox.clock(), max_age_days=7)
            assert identity in retained.deliveries and selected.group_id in retained.groups
            assert foreign_group not in retained.groups
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
@pytest.mark.parametrize("shape", ["payload", "parts", "member"])
async def test_suggestion_retention_keeps_owned_anchor_when_source_json_is_damaged(work_exports, monkeypatch, shape):
    e = work_exports
    owner = await runtime(e)

    async def model(**kwargs):
        return json.dumps({"objective": "Review the older evidence next.", "success_criteria": "List verified references"})

    monkeypatch.setattr("app.telegram_bots.assign.run_oneshot_llm", model)
    monkeypatch.setattr("app.telegram_bots.assign.get_app_config", lambda: e.live.config)
    try:
        _plans, identity, selected = await ready_suggestion(e)
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 932, selected.id, message_id=911))
        await e.channel._drain_ingress()
        async with e.db.sessions.begin() as writer:
            anchor = await writer.get(TelegramOutboxRow, identity)
            if shape == "payload":
                anchor.payload = []
            elif shape == "parts":
                anchor.parts = None
            else:
                (await writer.get(TelegramCallbackRow, selected.id)).payload = []
        async with e.db.sessions() as reader:
            retained = await assignment_dependencies(reader, e.sender.outbox.clock(), max_age_days=7)
            assert identity in retained.deliveries and retained.acts
    finally:
        await owner.stop()
