"""Stopping summaries changes one person's preference and edits the actual card."""

import traceback

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


async def setup(e, protected=False):
    from test_telegram_summary_items import summary_anchor

    from app.telegram_bots.acts import Acts
    from app.telegram_bots.commands import CallbackCommands
    from app.telegram_bots.summary_preferences import SummaryPreferences

    services = e.channel._services
    plans, clock, identity, request_id = await summary_anchor(e, protected)
    services.commands = CallbackCommands(services)
    services.acts = Acts(e.db.sessions, settings=services.settings, callbacks=services.callbacks, outbox=services.outbox)
    services.prepare_live_owner = e.live.authority.prepare_live_owner

    async def values(link):
        return {"product": "HartMesh", "name": "Fixture"}

    services.prepare_ingress_values = values
    owner = SummaryPreferences(plans, services)
    services.summary_preferences = owner
    async with e.db.sessions() as reader:
        anchor = await reader.get(TelegramOutboxRow, identity)
        callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == anchor.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "summary_off"))
    return owner, clock, identity, callback.id, request_id


async def stop(e, protected=False):
    owner, clock, anchor, callback, request_id = await setup(e, protected)
    await e.channel.execute_callback(await e.channel._services.callbacks.get(callback))
    assert not (await e.channel._services.settings.get(e.link.owner_user_id)).summary_enabled
    async with e.db.sessions() as reader:
        act = await reader.get(TelegramActRow, callback)
        assert act.state == "done" and act.kind == "settings"
        identity = act.result["outbox_id"]
    return owner, clock, anchor, callback, request_id, identity


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
async def test_summary_stop_keeps_body_and_other_buttons(work_delivery, protected):
    from app.telegram_bots.work_digest_items import DigestItems
    from app.telegram_bots.work_request_source import WorkRequestSources

    e = work_delivery
    owner, clock, anchor, callback, request_id, identity = await stop(e, protected)
    before = len(e.channel.bot.request.calls)
    e.channel.bot.request.enqueue("editMessageText", result={"message_id": 7501, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Summary"})
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    edits = [call for call in e.channel.bot.request.calls[before:] if call.endpoint == "editMessageText"]
    assert len(edits) == 1, "\n".join("".join(traceback.format_exception(error)) for error in e.dispatch_errors)
    assert "Good morning" in edits[-1].parameters["text"] and "/settings" in edits[-1].parameters["text"]
    assert callback not in str(edits[-1].parameters["reply_markup"])
    assert request_id in str(edits[-1].parameters["reply_markup"])
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"
        original = await reader.get(TelegramOutboxRow, anchor)
        assert original.parts[-1]["protect_content"] is protected
    # The retained request control still opens the actual question after Stop.
    items = DigestItems(digests=owner.plans, requests=e.plans, summaries=owner.plans)
    e.channel._services.work_request_sources = WorkRequestSources(reader=e.reader, outbox=e.sender.outbox, callbacks=e.plans.callbacks, navigation=items.navigation)
    live = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    guard = e.db.links.publication_guard(e.link, check_live_owner=live, check_attempt_current=lambda: True)
    opened = await items.open(e.link, callback_id=request_id, query_id="after-summary-stop", chat_id=e.link.chat_id, message_id=7501, message_thread_id=None, guard=guard)
    message(e.channel.bot.request, identity=7901)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, opened)).state == "sent", e.dispatch_errors
    assert e.request["question"] in [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][-1].parameters["text"]


@pytest.mark.asyncio
async def test_summary_stop_required_edit_failure_rolls_back_preference(work_delivery, monkeypatch):
    e = work_delivery
    owner, _clock, _anchor, callback, _request = await setup(e)

    def fail(*args):
        raise RuntimeError("edit planning failed")

    monkeypatch.setattr(owner, "recipe", fail)
    with pytest.raises(RuntimeError, match="edit planning failed"):
        await owner.execute_callback(e.channel, await e.channel._services.callbacks.get(callback))
    assert (await e.channel._services.settings.get(e.link.owner_user_id)).summary_enabled
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, callback) is None
        assert (await reader.get(TelegramCallbackRow, callback)).consumed_at is None


@pytest.mark.asyncio
async def test_summary_stop_duplicate_press_never_creates_second_edit(work_delivery):
    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.producer import ProductSourceRetired

    e = work_delivery
    owner, _clock, _anchor, callback, _request, identity = await stop(e)
    with pytest.raises((CallbackRefused, ProductSourceRetired)):
        await owner.execute_callback(e.channel, await e.channel._services.callbacks.get(callback))
    async with e.db.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:settings:" + callback)))
        assert [row.id for row in rows] == [identity]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["anchor", "callback", "claim", "grant", "locale"])
async def test_summary_stop_final_provider_prevents_stale_edit(work_delivery, monkeypatch, change):
    from app.gateway import authz
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.agent_instances.work import AgentWorkRow
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    owner, clock, anchor, callback, _request, identity = await stop(e, True)
    authorize, resolve = owner.authorize, authz.resolve_route_permissions
    armed, changed = [], []

    async def final(*args, **kwargs):
        armed.append(True)
        return await authorize(*args, **kwargs)

    async def provider(*args, **kwargs):
        permissions = await resolve(*args, **kwargs)
        if armed and not changed:
            async with e.db.sessions.begin() as writer:
                if change == "anchor":
                    (await writer.get(TelegramOutboxRow, anchor)).message_id = 9999
                elif change == "callback":
                    (await writer.get(TelegramCallbackRow, callback)).closed_reason = "superseded"
                elif change == "claim":
                    (await writer.get(TelegramOutboxRow, identity)).claim_id = "foreign"
                elif change == "grant":
                    (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 1
                elif change == "locale":
                    (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
                else:
                    (await writer.get(AgentWorkRow, e.work["id"])).objective = "Changed while stopping summaries"
            changed.append(True)
        return permissions

    monkeypatch.setattr(owner, "authorize", final)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    before = len(e.channel.bot.request.calls)
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed and len(e.channel.bot.request.calls) == before


@pytest.mark.asyncio
async def test_summary_stop_expired_controls_do_not_change_preference(work_delivery):
    from datetime import timedelta

    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.producer import ProductSourceRetired

    e = work_delivery
    owner, clock, _anchor, callback, _request = await setup(e)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramCallbackRow, callback)
        row.created_at = clock.now() - timedelta(days=owner.plans.callbacks.config.callbacks.max_age_days + 1)
    with pytest.raises((CallbackRefused, ProductSourceRetired)):
        await owner.execute_callback(e.channel, await e.channel._services.callbacks.get(callback))
    assert (await e.channel._services.settings.get(e.link.owner_user_id)).summary_enabled


@pytest.mark.asyncio
async def test_summary_stop_edit_falls_back_from_omitted_work_url(work_delivery):
    from copy import deepcopy
    from datetime import timedelta
    from uuid import uuid4

    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    e.live.config.model_extra["telegram_bots"] = {"public_base_url": "https://app.example.test"}
    now = e.sender.outbox.clock()
    async with e.db.sessions.begin() as writer:
        work = await writer.get(AgentWorkRow, e.work["id"])
        request = await writer.get(HumanInputRequestRow, e.request["id"])
        work.created_at = request.created_at = now - timedelta(minutes=10)
        for i in range(5):
            work_id, request_id, basis_id = uuid4().hex, uuid4().hex, uuid4().hex
            fields = {col.name: deepcopy(getattr(work, col.name)) for col in work.__table__.columns}
            fields.update(id=work_id, blocker={**fields["blocker"], "id": basis_id}, created_at=now - timedelta(minutes=i + 1))
            writer.add(AgentWorkRow(**fields))
            await writer.flush()
            fields = {col.name: deepcopy(getattr(request, col.name)) for col in request.__table__.columns}
            fields.update(id=request_id, work_id=work_id, basis_id=basis_id, created_at=now - timedelta(minutes=i + 1))
            writer.add(HumanInputRequestRow(**fields))
    _owner, clock, anchor, _callback, _request, identity = await stop(e, True)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, anchor)
        assert any(button.get("url") for line in row.payload["summary_view_basis"]["keyboard"] for button in line)
    e.channel.bot.request.enqueue("editMessageText", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: button url invalid"})
    clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "pending" and row.parts[-1].get("url_button_fallback") is True, (row.state, row.parts[-1], e.dispatch_errors)
    e.channel.bot.request.enqueue("editMessageText", result={"message_id": 7501, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Full summary"})
    clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent", e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    assert "https://app.example.test" in calls[-1].parameters["text"]


@pytest.mark.asyncio
async def test_summary_stop_still_disables_after_work_changes(work_delivery):
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    owner, clock, anchor, callback, request_id = await setup(e)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentWorkRow, e.work["id"])).objective = "The assignment has changed since this summary"
    await e.channel.execute_callback(await e.channel._services.callbacks.get(callback))
    assert not (await e.channel._services.settings.get(e.link.owner_user_id)).summary_enabled
    e.channel.bot.request.enqueue("editMessageText", result={"message_id": 7501, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Summary stopped"})
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    edit = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"][-1]
    assert e.work["objective"] in edit.parameters["text"]  # Preserve the acknowledged historical summary.
    assert "/settings" in edit.parameters["text"]
    assert request_id in str(edit.parameters["reply_markup"])


@pytest.mark.asyncio
async def test_summary_stop_edit_requires_its_committed_settings_receipt(work_delivery):
    e = work_delivery
    _owner, clock, _anchor, callback, _request, _identity = await stop(e)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramActRow, callback)).result = {"outbox_id": "f" * 32, "field": "summary", "value": {"enabled": False, "minute": 0}}
    before = len(e.channel.bot.request.calls)
    e.channel.bot.request.enqueue("editMessageText", result={"message_id": 7501, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Summary"})
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert len(e.channel.bot.request.calls) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("when", ["before_press", "before_edit"])
async def test_summary_stop_without_inspect_saves_preference_and_clears_private_content(work_delivery, when):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    owner, clock, _anchor, callback, _request = await setup(e)

    async def revoke():
        async with e.db.sessions.begin() as writer:
            (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 1

    if when == "before_press":
        await revoke()
    await e.channel.execute_callback(await e.channel._services.callbacks.get(callback))
    assert not (await e.channel._services.settings.get(e.link.owner_user_id)).summary_enabled
    if when == "before_edit":
        await revoke()
    before = len(e.channel.bot.request.calls)
    clock.advance(2)
    await e.sender.tick()
    await e.sender.drain()
    assert len(e.channel.bot.request.calls) == before  # Persist redaction before sending.
    e.channel.bot.request.enqueue("editMessageText", result={"message_id": 7501, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Summaries off"})
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    edit = [call for call in e.channel.bot.request.calls[before:] if call.endpoint == "editMessageText"][-1]
    assert "/settings" in edit.parameters["text"] and e.work["objective"] not in edit.parameters["text"]
    assert edit.parameters["reply_markup"] == {"inline_keyboard": []}


@pytest.mark.asyncio
async def test_summary_stop_missing_message_does_not_send_a_new_summary(work_delivery):
    e = work_delivery
    _owner, clock, _anchor, _callback, _request, identity = await stop(e)
    e.channel.bot.request.enqueue("editMessageText", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: message to edit not found"})
    before = len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"])
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "skipped" and row.skip_reason == "superseded"
    assert not (await e.channel._services.settings.get(e.link.owner_user_id)).summary_enabled
    assert len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]) == before


@pytest.mark.asyncio
async def test_stop_survives_normal_locale_change(work_delivery):
    e = work_delivery
    owner, clock, anchor, callback, request = await setup(e)
    await e.channel._services.settings.update(e.link.owner_user_id, locale="zh-CN")
    await owner.execute_callback(e.channel, await e.channel._services.callbacks.get(callback))
    assert not (await e.channel._services.settings.get(e.link.owner_user_id)).summary_enabled


@pytest.mark.asyncio
async def test_stop_edit_ack_must_match_target(work_delivery):
    e = work_delivery
    owner, clock, anchor, callback, request, identity = await stop(e)
    e.channel.bot.request.enqueue("editMessageText", result={"message_id": 9999, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Off"})
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state != "sent", "Wrong-target edit receipt sealed required summary edit"


@pytest.mark.asyncio
async def test_delivered_stop_retains_complete_old_group(work_delivery):
    e = work_delivery
    owner, clock, anchor, callback, request = await setup(e)
    owner.plans.callbacks.clock = clock.now
    await owner.execute_callback(e.channel, await e.channel._services.callbacks.get(callback))
    e.channel.bot.request.enqueue("editMessageText", result={"message_id": 7501, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Off"})
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    clock.advance(8 * 86400)
    await owner.plans.callbacks.sweep(clock.now())
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramCallbackRow, request) is not None
        assert await reader.get(TelegramCallbackRow, callback) is not None, "Consumed Stop vanished while live item controls require complete group"


@pytest.mark.asyncio
async def test_late_work_change_does_not_retire_historical_stop(work_delivery, monkeypatch):
    from app.gateway import authz
    from app.telegram_bots import summary_preferences as module
    from app.telegram_bots.summary_preferences import StopProjection
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    owner, clock, anchor, callback, request, identity = await stop(e)
    original_prepare, original_provider = module.prepare_attention, authz.resolve_route_permissions
    active = []
    changed = []

    async def prepare(*args, **kwargs):
        admission = await original_prepare(*args, **kwargs)

        async def check(**options):
            projection = options.get("projection")
            additional = getattr(projection, "additional", projection)
            selected = isinstance(additional, StopProjection) and additional.captured is not None
            if selected:
                active.append(True)
            try:
                return await admission(**options)
            finally:
                if selected:
                    active.pop()

        return check

    async def provider(*args, **kwargs):
        held = await original_provider(*args, **kwargs)
        if active and not changed:
            changed.append(True)
            async with e.db.sessions.begin() as writer:
                (await writer.get(AgentWorkRow, e.work["id"])).objective = "A normal newer assignment title"
        return held

    monkeypatch.setattr(module, "prepare_attention", prepare)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    before = len([c for c in e.channel.bot.request.calls if c.endpoint == "editMessageText"])
    e.channel.bot.request.enqueue("editMessageText", result={"message_id": 7501, "date": 1893456000, "chat": {"id": e.link.chat_id, "type": "private"}, "text": "Off"})
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed
    calls = [c for c in e.channel.bot.request.calls if c.endpoint == "editMessageText"]
    assert len(calls) == before + 1, "Unrelated current-section drift retired the historical required edit"
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"
