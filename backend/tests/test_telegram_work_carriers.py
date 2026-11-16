"""Work event carriers retain complete previews and actual source gestures."""

import pytest
from _telegram_bot_test_support import ManualClock
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_work_news_delivery import prepare
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_stop_news import stopped
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow


async def stopped_carrier(e, cause, *, multipart=False):
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    # Carrier custody needs one stable calendar, without changing gesture ages.
    # The same instant is at least six hours from midnight in one of these zones.
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).timezone = "UTC" if 6 <= clock.now().hour < 18 else "Etc/GMT-12"
    item = await stopped(e, cause)
    if multipart:
        from test_agent_work_records import command

        current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=item.work_id)
        await e.human.work.command(actor=e.actor, instance_id=e.instance.id, work_id=item.work_id, request=command(current, "edit", objective="Review <>&" * 380, success_criteria="Verify <>&" * 380))
    _plans, identity = await prepare(e, item=item)
    current = await e.human.work.get(actor=e.actor, instance_id=e.link.agent_ref, work_id=item.work_id)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert current["success_criteria"] in "".join(part["visible_text"] for part in row.parts)
        control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == identity, TelegramCallbackRow.kind == "work_start"))
        assert control is not None
        assert control.payload["assignment_revision"] == current["assignment_revision"]
        assert control.message_id is None
        assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "activate")))
    count = len(row.parts)
    if multipart:
        assert count > 1
    for index in range(count):
        clock.advance(2)
        message(e.channel.bot.request, identity=9501 + index)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        if index < count - 1:
            async with e.db.sessions() as reader:
                assert (await reader.get(TelegramCallbackRow, control.id)).message_id is None
    assert not e.dispatch_errors, e.dispatch_errors
    async with e.db.sessions() as reader:
        control = await reader.get(TelegramCallbackRow, control.id)
        assert control.message_id == 9500 + count
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"
    return control


@pytest.mark.asyncio
@pytest.mark.parametrize("cause", ["stop", "edit"])
@pytest.mark.parametrize("multipart", [False, True])
async def test_stopped_news_offers_resume_only_under_its_complete_assignment(work_delivery, cause, multipart):
    await stopped_carrier(work_delivery, cause, multipart=multipart)


@pytest.mark.asyncio
@pytest.mark.parametrize("timezone", ["America/New_York", "UTC", "Etc/GMT-12"])
async def test_stopped_carrier_keeps_its_instant_near_fixture_midnight(work_delivery, timezone):
    from datetime import UTC, datetime, time, timedelta
    from zoneinfo import ZoneInfo

    e = work_delivery
    zone = ZoneInfo(timezone)
    next_date = e.sender.outbox.clock().astimezone(zone).date() + timedelta(days=1)
    midnight = datetime.combine(next_date, time(), tzinfo=zone).astimezone(UTC)
    before_midnight = midnight - timedelta(seconds=1)
    e.sender.outbox.clock = lambda: before_midnight
    await stopped_carrier(e, "stop")
    assert e.sender.outbox.clock() == before_midnight + timedelta(seconds=2)
    actual_zone = ZoneInfo((await e.db.settings.get(e.link.owner_user_id)).timezone)
    assert before_midnight.astimezone(actual_zone).date() == e.sender.outbox.clock().astimezone(actual_zone).date()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["none", "before-stage", "during-stage", "before-native", "during-native", "missing-callback", "missing-parent", "missing-marker", "missing-callback-and-marker", "missing-parent-and-marker", "missing-witness"]
)
async def test_news_resume_qualifies_its_actual_event_at_each_entry(work_delivery, monkeypatch, change):
    """Actual SQL/SDK/admission with an explicit in-memory host launcher."""
    from datetime import timedelta

    from test_telegram_work_interaction_delivery import guard_for
    from test_telegram_work_start import prepare_startable
    from test_telegram_work_view_delivery import prepare_view

    from app.gateway import work_execution
    from app.telegram_bots.acts import Acts
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_start_controls import WorkStartAdmissions
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    button = await stopped_carrier(e, "stop")
    target = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=button.target_id)
    await prepare_startable(e, monkeypatch)
    e.work = target
    # View plans supply the current detail reader; the original carrier remains
    # the actual news row and callback, never a manufactured view gesture.
    views, _ = await prepare_view(e)
    acts = Acts(e.db.sessions, settings=e.db.settings, callbacks=e.channel._services.callbacks, outbox=e.sender.outbox, work=e.human.work, authority=e.live.authority, links=e.db.links)
    controller = WorkStartAdmissions(views, acts)
    changed = []

    async def damage(writer):
        event = await writer.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.work_id == target["id"], AgentWorkEventRow.action == "stop"))
        event.created_at += timedelta(seconds=1)
        await writer.flush()
        changed.append(True)

    if change == "before-stage":
        async with e.db.sessions.begin() as writer:
            await damage(writer)
    elif change == "during-stage":
        original = acts._stage_work_act

        async def damaged_stage(*args, **kwargs):
            result = await original(*args, **kwargs)
            await damage(kwargs["session"])
            return result

        monkeypatch.setattr(acts, "_stage_work_act", damaged_stage)
    if change in {"before-stage", "during-stage"}:
        with pytest.raises(ProductSourceRetired):
            await controller.stage(e.link, button.id, chat_id=e.link.chat_id, message_id=button.message_id, message_thread_id=None, guard=await guard_for(e))
        async with e.db.sessions() as reader:
            assert await reader.get(TelegramActRow, button.id) is None
        assert changed and not e.launches
        return
    act = await controller.stage(e.link, button.id, chat_id=e.link.chat_id, message_id=button.message_id, message_thread_id=None, guard=await guard_for(e))
    assert act.state == "pending" and not e.launches
    if change == "before-native":
        async with e.db.sessions.begin() as writer:
            await damage(writer)
    elif change.startswith("missing-"):
        async with e.db.sessions.begin() as writer:
            if "callback" in change:
                await writer.delete(await writer.get(TelegramCallbackRow, button.id))
            if "parent" in change:
                await writer.delete(await writer.get(TelegramOutboxRow, button.payload["anchor_outbox_id"]))
            if "marker" in change:
                (await writer.get(TelegramActRow, act.id)).continuation = None
            if "witness" in change:
                from app.telegram_bots.work_news_carriers import news_witness_id

                await writer.delete(await writer.get(TelegramCallbackRow, news_witness_id(act.id)))
            changed.append(True)
    original = work_execution.activate_work
    native_errors = []

    async def qualified_native(*args, **kwargs):
        if change == "during-native":
            async with e.db.sessions.begin() as writer:
                await damage(writer)
        try:
            return await original(*args, **kwargs)
        except Exception as exc:
            native_errors.append(exc)
            raise

    monkeypatch.setattr(work_execution, "activate_work", qualified_native)
    result = await acts.check(act.id)
    if change == "none":
        assert result.state == "done", (result.state, result.reason, native_errors)
        assert len(e.launches) == 1
        assert (await acts.check(act.id)).result == result.result and len(e.launches) == 1
    else:
        assert changed and result.state != "done" and not e.launches
