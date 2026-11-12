"""Input-owned decision navigation retires the old input without an action."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_inbox import incoming
from test_telegram_work_actions import management_request
from test_telegram_work_callback_dispatch import press_work
from test_telegram_work_control_admission import admissions, stage
from test_telegram_work_input_refresh import deliver_refreshed
from test_telegram_work_interaction_delivery import guard_for, open_view, selected
from test_telegram_work_preview_recovery import dispatch, install
from test_telegram_work_retained_drafts import draft
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_typed_input import install_inputs
from test_telegram_work_typing_prompts import clear_old
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.producer import ProductSourceRetired
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


async def typed_draft(e, monkeypatch):
    from app.telegram_bots import inbox

    install(e)
    _record, request, _seen = await management_request(e, "decision")
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramOutboxRow, e.identity)).state = "skipped"
    e.identity = await e.plans.plan(e.link, request_id=request["id"], dedupe_key="navigation-decision")
    await dispatch(e, 801)
    e.identity = await open_view(e, await selected(e, "decide_own"))
    await clear_old(e)
    await dispatch(e, 802)
    monkeypatch.setattr(inbox, "_now", e.clock)
    _plans, inputs = install_inputs(e)
    assert await e.channel.handle_update(incoming(message=900, update_id=1900, account=e.link.chat_id, reply=802))
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        mark = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, 900, e.link.chat_id)))
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_request_basis"]["input"]["operation_id"].as_string() == mark.operation_ids[0]))
        e.identity = row.id
    await dispatch(e, 803)
    return request, inputs, mark


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["back", "decide_note"])
async def test_typed_decision_navigation_settles_input_and_keeps_new_view(work_delivery, monkeypatch, kind):
    e = work_delivery
    request, inputs, origin = await typed_draft(e, monkeypatch)
    control = await selected(e, kind)
    await press_work(e, control, message_id=803)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))
        assert mark.route_complete
        assert mark.route_basis == origin.route_basis
        assert not list(await reader.scalars(select(TelegramActRow)))
        fresh = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:work-interaction:" + control.id))
        assert fresh.payload["work_request_basis"]["version"] == 3
        state = fresh.payload["work_request_basis"]["interaction"]
        assert state["view"] == ("request" if kind == "back" else "prompt")
        if kind == "decide_note":
            assert state["decision"] == origin.route_basis["work_input"]["text"]
        e.identity = fresh.id
    await deliver_refreshed(e, 804)
    assert await inputs.resume(e.channel, origin)
    if kind == "decide_note":
        update = incoming(message=901, update_id=1901, account=e.link.chat_id, reply=804)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 901, e.link.chat_id))
            fresh = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_request_basis"]["input"]["operation_id"].as_string() == mark.operation_ids[0]))
            assert not list(await reader.scalars(select(TelegramActRow)))
            assert fresh.payload["work_request_basis"]["interaction"]["note"] == update.message.text
            e.identity = fresh.id
        await dispatch(e, 805)
        action = await stage(e, admissions(e), await selected(e, "decide_confirm"))
        assert action.request_id == request["id"] and (await admissions(e).acts.check(action.id)).state == "done"


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["back", "decide_note", "changes_open", "dismiss"])
async def test_navigation_final_input_drift_rolls_back_every_transition(work_delivery, monkeypatch, kind):
    from app.telegram_bots.work_retained_drafts import WorkRetainedDrafts

    e = work_delivery
    if kind in {"back", "decide_note"}:
        _request, _inputs, origin = await typed_draft(e, monkeypatch)
    else:
        _request, _inbox, origin = await draft(e, monkeypatch)
    control = await selected(e, kind)
    host = WorkRetainedDrafts(e.plans)
    original, changed = host.inbox.finish_route, []

    async def drift(*args, **kwargs):
        result = await original(*args, **kwargs)
        writer = kwargs["session"]
        mark = await writer.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))
        mark.sanitized_input = {"text": "Changed after navigation"}
        await writer.flush()
        changed.append(True)
        return result

    monkeypatch.setattr(host.inbox, "finish_route", drift)
    with pytest.raises(ProductSourceRetired):
        await host.navigate(e.link, control.id, chat_id=e.link.chat_id, message_id=803, message_thread_id=None, guard=await guard_for(e))
    assert changed
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))
        assert not mark.route_complete and mark.sanitized_input == origin.sanitized_input
        assert (await reader.get(TelegramCallbackRow, control.id)).consumed_at is None
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:work-interaction:" + control.id)))


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["back", "decide_note"])
@pytest.mark.parametrize("paused", [False, True])
async def test_changed_decision_navigation_retains_text_for_another_explicit_choice(work_delivery, monkeypatch, kind, paused):
    from test_telegram_work_refusal_recovery import comment

    e = work_delivery
    request, _inputs, origin = await typed_draft(e, monkeypatch)
    previous, control = e.identity, await selected(e, kind)
    await comment(e, request)
    if paused:
        from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramBotLinkRow, e.link.id)).acts_paused_since = e.clock()
    await press_work(e, control, message_id=803)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, control.id)).consumed_at is not None
        mark = await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))
        assert not mark.route_complete
        assert not list(await reader.scalars(select(TelegramActRow)))
        fresh = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.id != previous, TelegramOutboxRow.payload["work_request_basis"]["input"]["operation_id"].as_string() == origin.operation_ids[0]))
        assert fresh is not None
        state = fresh.payload["work_request_basis"]["interaction"]
        assert state["view"] == "decision" and state["decision"] == origin.route_basis["work_input"]["text"] and state["refreshed"]
        e.identity = fresh.id
    await deliver_refreshed(e, 804)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["back", "decide_note"])
async def test_admitted_confirmation_holds_decision_navigation(work_delivery, monkeypatch, kind):
    e = work_delivery
    _request, _inputs, origin = await typed_draft(e, monkeypatch)
    action = await stage(e, admissions(e), await selected(e, "decide_confirm"))
    control = await selected(e, kind)
    await press_work(e, control, message_id=803)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, control.id)).consumed_at is None
        assert not (await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))).route_complete
        assert [row.id for row in await reader.scalars(select(TelegramActRow))] == [action.id]
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:work-interaction:" + control.id)))
