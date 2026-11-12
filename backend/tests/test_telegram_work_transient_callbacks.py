"""A conclusive temporary failure preserves the person's explicit next press."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_callback_dispatch import install_dispatch, press_work
from test_telegram_work_control_admission import admissions
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_outcome_reads import review_confirmation
from test_telegram_work_preview_recovery import dispatch, install
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans, WorkOutcomeSources
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("committed", [False, True])
async def test_failed_first_tap_reports_failure_and_keeps_its_button(work_delivery, monkeypatch, committed):
    e = work_delivery
    install(e)
    controls = admissions(e)
    callbacks = install_dispatch(e, WorkOutcomePlans(e.plans, controls.acts))
    await dispatch(e, 801)
    callback = await selected(e, "hi_cant_open")
    original = callbacks.interactions.open

    async def unavailable(*args, **kwargs):
        if committed:
            await original(*args, **kwargs)
        raise TimeoutError("source read unavailable")

    with monkeypatch.context() as fault:
        fault.setattr(callbacks.interactions if committed else callbacks.interactions.recovery, "open" if committed else "_source", unavailable)
        await press_work(e, callback, message_id=801)
    async with e.db.sessions() as reader:
        assert ((await reader.get(TelegramCallbackRow, callback.id)).consumed_at is not None) == committed
        assert not list(await reader.scalars(select(TelegramActRow)))
        failures = [row for row in await reader.scalars(select(TelegramOutboxRow)) if any(part.get("source_ref", {}).get("key") == "tap.failed" for part in row.parts)]
        assert len(failures) == (0 if committed else 1)
    if committed:
        return
    await dispatch(e, 802)
    assert "Tap again" in [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][-1].parameters["text"]
    await press_work(e, callback, message_id=801, update_id=2902)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, callback.id)).consumed_at is not None
        assert await reader.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.dedupe_key == "notice:work-interaction:" + callback.id))
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_state", ["success", "grant", "receipt", "unknown", "cancel", "pause", "revision"])
async def test_storage_busy_waits_for_explicit_press_and_reuses_original_operation(work_delivery, monkeypatch, retry_state):
    from app.telegram_bots.work_actions import ActsWorker, request_for, same_body
    from deerflow.agent_instances.contract import ResidentFilesBusy
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

    e = work_delivery
    work, _request, _seen, callback = await review_confirmation(e)
    controls = admissions(e)
    plans = WorkOutcomePlans(e.plans, controls.acts)
    install_dispatch(e, plans)
    e.channel._services.work_outcome_sources = WorkOutcomeSources(plans)
    original, calls = e.human.work.command, []

    async def busy_once(*args, **kwargs):
        calls.append(kwargs["request"].model_dump(mode="json", exclude_unset=True))
        if len(calls) == 1:
            raise ResidentFilesBusy("Temporarily unavailable")
        if retry_state == "unknown":
            raise TimeoutError("Unknown service result")
        return await original(*args, **kwargs)

    monkeypatch.setattr(e.human.work, "command", busy_once)
    await press_work(e, callback, message_id=802)
    async with e.db.sessions() as reader:
        act = await reader.get(TelegramActRow, callback.id)
        assert (act.state, act.reason, act.attempts) == ("refused", "storage-busy", 1)
        body, first = act.body, act.first_attempt_at
        assert (await reader.get(TelegramCallbackRow, callback.id)).consumed_at is None
        reply = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == act.id))
        assert reply is not None and "Tap again" in "".join(part["visible_text"] for part in reply.parts)
    await dispatch(e, 803)
    e.clock.advance(3601)
    await ActsWorker(controls.acts).tick()
    assert len(calls) == 1
    if retry_state == "receipt":
        await original(actor=e.actor, instance_id=e.instance.id, work_id=work["id"], request=request_for("accept", body, identity=callback.id), can_stop=False)
    elif retry_state == "revision":
        from test_telegram_work_refusal_recovery import comment

        await comment(e, _request)
    else:
        async with e.db.sessions.begin() as writer:
            if retry_state == "grant":
                (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
            elif retry_state == "cancel":
                (await writer.get(TelegramActRow, callback.id)).cancel_requested_at = e.clock()
            elif retry_state == "pause":
                (await writer.get(TelegramBotLinkRow, e.link.id)).acts_paused_since = e.clock()
    # A restarted controller still needs this actual press, not a timer.
    fresh = admissions(e)
    install_dispatch(e, WorkOutcomePlans(e.plans, fresh.acts))
    await press_work(e, callback, message_id=802, update_id=2902)
    if retry_state == "unknown":
        await press_work(e, callback, message_id=802, update_id=2903)
    async with e.db.sessions() as reader:
        act = await reader.get(TelegramActRow, callback.id)
        expected = {"success": ("done", 2), "grant": ("refused", 2), "receipt": ("done", 1), "unknown": ("pending", 2), "cancel": ("refused", 1), "pause": ("refused", 1), "revision": ("conflict", 2)}[retry_state]
        assert (act.state, act.attempts, act.first_attempt_at) == (*expected, first)
        assert same_body(act.body, body)
        if retry_state in {"success", "receipt"}:
            assert (await reader.get(TelegramCallbackRow, callback.id)).consumed_at is not None
        if retry_state == "unknown":
            assert act.reason is None
        assert len(list(await reader.scalars(select(TelegramActRow)))) == 1
    assert len(calls) == (1 if retry_state in {"receipt", "cancel", "pause"} else 2)
    assert all(same_body(calls[0], call) for call in calls)
    assert ((await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=work["id"]))["status"] == "completed") == (retry_state in {"success", "receipt"})


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["stage", "receipt"])
async def test_admitted_retry_cannot_submit_a_retired_input(work_delivery, monkeypatch, boundary):
    import asyncio

    from test_telegram_work_callback_recovery import typed_refused
    from test_telegram_work_input_previews import previews
    from test_telegram_work_interaction_delivery import guard_for

    from app.telegram_bots.inbox import InboxRepository
    from app.telegram_bots.work_outcome_transfer import transferred
    from deerflow.agent_instances.contract import ResidentFilesBusy

    e = work_delivery
    controls, _request, callback, captured, _first = await typed_refused(e, monkeypatch)
    original = controls.acts.work.command

    async def busy(*args, **kwargs):
        raise ResidentFilesBusy("Temporarily unavailable")

    with monkeypatch.context() as fault:
        fault.setattr(controls.acts.work, "command", busy)
        refused = await controls.acts.check(callback.id)
    assert (refused.state, refused.reason) == ("refused", "storage-busy")
    callbacks = install_dispatch(e, WorkOutcomePlans(e.plans, controls.acts))
    calls = []

    async def observe(*args, **kwargs):
        calls.append(kwargs["request"].operation_id)
        return await original(*args, **kwargs)

    monkeypatch.setattr(controls.acts.work, "command", observe)
    ready, release = asyncio.Event(), asyncio.Event()
    owner, method = (callbacks.controls, "stage") if boundary == "stage" else (controls.acts, "receipt_for")
    operation = getattr(owner, method)

    async def held(*args, **kwargs):
        result = await operation(*args, **kwargs)
        ready.set()
        await release.wait()
        return result

    monkeypatch.setattr(owner, method, held)
    task = asyncio.create_task(press_work(e, callback, message_id=803, update_id=3001))
    try:
        await asyncio.wait_for(ready.wait(), 10)
        if boundary == "stage":
            back = await selected(e, "back")
            await callbacks.drafts.navigate(e.link, back.id, chat_id=e.link.chat_id, message_id=803, message_thread_id=None, guard=await guard_for(e))
        else:
            # A real replacement preview under a new locale transfers ownership
            # without changing the Work revision or manufacturing a conflict.
            await e.db.settings.update(e.link.owner_user_id, locale="zh-CN")
            assert await previews(e, InboxRepository(e.db.sessions), captured) != e.identity
            assert transferred(await controls.acts._work_record(callback.id))
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramCallbackRow, callback.id)).consumed_at is not None
    finally:
        release.set()
        await task
    assert calls == []
    result = await controls.acts._work_record(callback.id)
    assert (result.state, result.reason, result.attempts) == ("refused", "storage-busy", refused.attempts)
