"""Status controls belong to the actual acknowledged attempt presentation."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_status_updates import progress, shown
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_status import status_attempt
from test_telegram_work_status_delivery import prepare
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
async def test_status_stop_becomes_usable_only_after_its_actual_sdk_ack(work_delivery):
    e = work_delivery
    item, work, _execution, scope, _run, _worker = await status_attempt(e)
    await prepare(e, item)
    async with e.db.sessions() as reader:
        stop = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == item.id, TelegramCallbackRow.kind == "stop_attempt"))
        assert stop is not None
        assert stop.message_id is None
        assert (stop.target_kind, stop.target_id) == ("attempt", scope.attempt_id)
        assert stop.payload["work_id"] == work["id"]
        assert not list(await reader.scalars(select(TelegramActRow)))
    message(e.channel.bot.request, identity=9701)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1
    assert [button["text"] for row in calls[0].parameters["reply_markup"]["inline_keyboard"] for button in row] == ["Stop"]
    assert calls[0].parameters["disable_notification"] is True
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, stop.id)).message_id == 9701
        assert (await reader.get(TelegramOutboxRow, item.id)).state == "sent"


@pytest.mark.asyncio
async def test_status_progress_edit_replaces_stop_only_after_its_complete_ack(work_delivery):
    e = work_delivery
    root, _work, execution, scope, _run, worker, clock, updates = await shown(e)
    async with e.db.sessions() as reader:
        original = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == root.id, TelegramCallbackRow.kind == "stop_attempt"))
        assert original is not None and original.message_id == 3101
    change = await progress(e, execution, scope, worker, "Checked the complete guide.")
    clock.advance(30)
    assert await updates.materialize(e.link, change)
    async with e.db.sessions() as reader:
        control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == change, TelegramCallbackRow.kind == "stop_attempt"))
        assert control is not None and control.message_id is None
        assert control.target_id == scope.attempt_id
        assert (await reader.get(TelegramCallbackRow, original.id)).consumed_at is None
    message(e.channel.bot.request, "editMessageText", identity=3101)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    assert len(calls) == 1
    assert [button["text"] for line in calls[0].parameters["reply_markup"]["inline_keyboard"] for button in line] == ["Stop"]
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, control.id)).message_id == 3101
        assert (await reader.get(TelegramCallbackRow, original.id)).closed_reason == "superseded"
    from app.telegram_bots.work_status_updates import acknowledge_status_update

    async with e.db.sessions.begin() as writer:
        await acknowledge_status_update(writer, await writer.get(TelegramOutboxRow, change))
    async with e.db.sessions() as reader:
        current = await reader.get(TelegramCallbackRow, control.id)
        assert current.consumed_at is None and current.closed_reason is None
        assert current.message_id == 3101 and current.payload["presentation_pending"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("edited", [False, True])
async def test_status_stop_stages_only_its_acknowledged_original_attempt(work_delivery, edited):
    from test_telegram_work_interaction_delivery import guard_for
    from test_telegram_work_view_delivery import prepare_view

    from app.telegram_bots.acts import Acts
    from app.telegram_bots.work_execution_sources import qualify_execution_origin
    from app.telegram_bots.work_stop_controls import WorkStopAdmissions

    e = work_delivery
    root, work, execution, scope, _run, worker, clock, updates = await shown(e)
    parent_id = root.id
    if edited:
        parent_id = await progress(e, execution, scope, worker, "Progress before Stop.")
        clock.advance(30)
        assert await updates.materialize(e.link, parent_id)
        message(e.channel.bot.request, "editMessageText", identity=3101)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    async with e.db.sessions() as reader:
        button = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == parent_id, TelegramCallbackRow.kind == "stop_attempt"))
        assert button is not None and button.message_id == 3101
    views, _ = await prepare_view(e)
    acts = Acts(e.db.sessions, settings=e.db.settings, callbacks=e.channel._services.callbacks, outbox=e.sender.outbox, work=e.human.work, authority=e.live.authority, links=e.db.links)
    controller = WorkStopAdmissions(views, acts)
    arguments = dict(chat_id=e.link.chat_id, message_id=3101, message_thread_id=None, guard=await guard_for(e))
    act = await controller.stage(e.link, button.id, **arguments)
    assert act.id == button.id and act.body["attempt_id"] == scope.attempt_id and act.work_id == work["id"]
    assert act.attempts == 0 and act.stop_dispatch["state"] == "awaiting_command"
    assert (await controller.stage(e.link, button.id, **arguments)).body == act.body
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, parent_id)
        control = await reader.get(TelegramCallbackRow, button.id)
        members = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == control.group_id)))
        origin = qualify_execution_origin(e.link, act, control, parent, members)
        assert origin["anchor_outbox_id"] == parent_id
        assert (await reader.get(TelegramActRow, act.id)).body["attempt_id"] == scope.attempt_id


@pytest.mark.asyncio
async def test_terminal_status_edit_explicitly_clears_its_original_stop(work_delivery):
    from uuid import uuid4

    from deerflow.agent_instances.work_execution_contract import ReportWork

    e = work_delivery
    root, work, execution, scope, run, worker, clock, updates = await shown(e)
    async with e.db.sessions() as reader:
        original = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == root.id, TelegramCallbackRow.kind == "stop_attempt"))
        assert original is not None
    current = (await scope.context(execution))["work"]
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="outcome", statement="Verified every finding."))
    await scope.finish(execution, run, settled=True)
    await worker.tick()
    async with e.db.sessions() as reader:
        change = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.work_id == work["id"], TelegramOutboxRow.payload["work_event_intent"]["key"].as_string() == "status.finished"))
        assert change is not None
        identity = change.id
    clock.advance(30)
    assert await updates.materialize(e.link, identity)
    message(e.channel.bot.request, "editMessageText", identity=3101)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    assert len(calls) == 1 and calls[0].parameters["reply_markup"] == {"inline_keyboard": []}
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"
        assert (await reader.get(TelegramCallbackRow, original.id)).closed_reason == "superseded"


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["none", "callback", "source", "ceiling", "original_ack", "group"])
async def test_status_final_sdk_rechecks_its_actual_control_and_source(work_delivery, monkeypatch, damage):
    from uuid import uuid4

    from app.gateway import authz
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    root, _work, execution, scope, _run, worker, clock, updates = await shown(e)
    identity = await progress(e, execution, scope, worker, "Progress with a source-owned Stop.")
    clock.advance(30)
    assert await updates.materialize(e.link, identity)
    original_get, original_permissions = updates.reader.settings.get, authz.resolve_route_permissions
    calls, changed = [], []

    async def permissions(*args, **kwargs):
        current = await original_permissions(*args, **kwargs)
        return frozenset(current) - {"agents:write"} if changed and damage == "ceiling" else current

    async def move(*args, **kwargs):
        result = await original_get(*args, **kwargs)
        calls.append(True)
        if len(calls) == 2:
            changed.append(True)
            async with e.db.sessions.begin() as writer:
                row = await writer.get(TelegramOutboxRow, identity)
                control = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
                assert control is not None and control.message_id is None
                if damage == "callback":
                    await writer.delete(control)
                elif damage == "source":
                    (await writer.get(AgentWorkEventRow, row.event_id)).actor_id = "another-source"
                elif damage == "original_ack":
                    parent = await writer.get(TelegramOutboxRow, root.id)
                    parent.parts = [{**part, "message_ids": []} for part in parent.parts]
                elif damage == "group":
                    control.group_id = uuid4().hex
        return result

    monkeypatch.setattr(authz, "resolve_route_permissions", permissions)
    monkeypatch.setattr(updates.reader.settings, "get", move)
    message(e.channel.bot.request, "editMessageText", identity=3101)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed
    sent = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    assert len(sent) == (1 if damage == "none" else 0), e.dispatch_errors
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))


@pytest.mark.asyncio
async def test_status_replacement_clears_only_the_previous_actual_keyboard(work_delivery):
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    e = work_delivery
    root, _work, execution, scope, _run, worker, clock, updates = await shown(e)
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="protected", updated_by=e.link.owner_user_id, updated_at=clock.now()))
    identity = await progress(e, execution, scope, worker, "Progress now requires protected delivery.")
    clock.advance(30)
    assert await updates.materialize(e.link, identity)
    message(e.channel.bot.request, identity=9702)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    async with e.db.sessions() as reader:
        current = await reader.get(TelegramOutboxRow, identity)
        assert current.state == "sent" and current.parts[-1]["message_ids"] == [9702]
        cleanup = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["kind"].as_string() == "keyboard_cleanup", TelegramOutboxRow.payload["work_request_parent_id"].as_string() == identity))
        assert cleanup is not None and cleanup.state == "pending"
        assert cleanup.parts[0]["target_message_id"] == 3101
    from app.telegram_bots.retention import sweep_rows
    from app.telegram_bots.work_status_updates import acknowledge_status_update

    # A replay cannot replace the cleanup, and aging an ended attempt cannot
    # delete the actual messages still needed by that pending remote edit.
    async with e.db.sessions.begin() as writer:
        await acknowledge_status_update(writer, await writer.get(TelegramOutboxRow, identity))
        saved = await writer.get(TelegramOutboxRow, identity)
        assert saved.payload["status_control_cleanup"]["outbox_id"] == cleanup.id
    await scope.finish(execution, _run, settled=True)
    clock.advance((e.channel._services.callbacks.config.callbacks.max_age_days + 1) * 86400)
    await e.channel._services.callbacks.sweep(clock.now())
    await sweep_rows(e.db.sessions, clock.now(), callback_max_age_days=e.channel._services.callbacks.config.callbacks.max_age_days)
    async with e.db.sessions() as reader:
        for retained in (root.id, identity, cleanup.id):
            assert await reader.get(TelegramOutboxRow, retained) is not None
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageReplyMarkup"]
    e.channel.bot.request.enqueue("editMessageReplyMarkup", body={"ok": True, "result": True})
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageReplyMarkup"]
    assert len(calls) == 1 and calls[0].parameters["message_id"] == 3101
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, cleanup.id)).state == "sent"
        root = await reader.get(TelegramOutboxRow, root.id)
        assert root.payload["status_anchor"]["outbox_id"] == identity


async def stopped_status(e, *, mode="update"):
    from test_agent_work_records import command

    if mode in {"root", "revealed"}:
        from _telegram_bot_test_support import ManualClock

        root, work, execution, scope, run, worker = await status_attempt(e)
        clock = ManualClock(e.sender.outbox.clock())
        e.sender.outbox.clock, e.channel._now = clock.now, clock.now
    else:
        root, work, execution, scope, run, worker, clock, updates = await shown(e)
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=work["id"])
    await e.human.work.command(actor=e.actor, instance_id=e.instance.id, work_id=work["id"], request=command(current, "stop", attempt_id=scope.attempt_id), can_stop=True)
    run.status = "interrupted"
    await scope.finish(execution, run, settled=True)
    if mode == "multipart":
        current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=work["id"])
        await e.human.work.command(actor=e.actor, instance_id=e.instance.id, work_id=work["id"], request=command(current, "edit", objective="o" * 3000, success_criteria="c" * 3000))
    await worker.tick()
    if mode in {"root", "revealed"}:
        identity = root.id
        if mode == "revealed":
            from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

            async with e.db.sessions.begin() as writer:
                writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="protected", updated_by=e.link.owner_user_id, updated_at=clock.now()))
        await prepare(e, root)
    else:
        async with e.db.sessions() as reader:
            change = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.work_id == work["id"], TelegramOutboxRow.payload["work_event_intent"]["key"].as_string() == "status.stopped"))
            assert change is not None
            identity = change.id
        clock.advance(30)
        assert await updates.materialize(e.link, identity)
    if mode == "revealed":
        from test_telegram_work_interaction_delivery import guard_for

        clock.advance(2)
        message(e.channel.bot.request, identity=9799)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        async with e.db.sessions() as reader:
            parent = await reader.get(TelegramOutboxRow, identity)
            show = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "reveal"))
            assert show is not None and show.message_id == 9799
            assert parent.payload["work_news_basis"]["concealed"] is True
        await e.channel._services.work_status_sources.recovery.reveal(e.link, show.id, chat_id=e.link.chat_id, message_id=9799, message_thread_id=None, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        change = await reader.get(TelegramOutboxRow, identity)
        parts = [part for part in change.parts if part["presentation_version"] == change.payload["presentation_version"]]
        control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == identity, TelegramCallbackRow.kind == "work_start"))
        assert control is not None and control.message_id is None
        assert ("c" * 3000 if mode == "multipart" else "Provide evidence") in "".join(part["visible_text"] for part in parts)
        if mode == "multipart":
            assert len(parts) > 1
        if mode == "revealed":
            assert change.class_ == "reply" and change.payload["work_news_basis"]["concealed"] is False
    for index, part in enumerate(parts):
        clock.advance(2)
        message(e.channel.bot.request, "editMessageText" if part["kind"] == "edit" else "sendMessage", identity=part.get("target_message_id", 9801 + index))
        assert await e.sender.tick() == 1
        await e.sender.drain()
        if index < len(parts) - 1:
            async with e.db.sessions() as reader:
                assert (await reader.get(TelegramCallbackRow, control.id)).message_id is None
    assert not e.dispatch_errors, e.dispatch_errors
    async with e.db.sessions() as reader:
        control = await reader.get(TelegramCallbackRow, control.id)
        assert control.message_id is not None
    return control


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["none", "before-stage", "before-native", "missing-cause", "before-stage-intent", "before-native-intent", "before-native-intent-and-markers", "before-native-intent-and-all-source"])
async def test_stopped_status_resume_keeps_original_stop_proof(work_delivery, monkeypatch, damage, mode="update"):
    """Real SQL/admission and Gateway entry with the explicit offline launcher."""
    from datetime import timedelta

    from test_telegram_work_interaction_delivery import guard_for
    from test_telegram_work_start import prepare_startable
    from test_telegram_work_view_delivery import prepare_view

    from app.gateway import work_execution
    from app.telegram_bots import work_start_actions
    from app.telegram_bots.acts import Acts
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_start_controls import WorkStartAdmissions
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    button = await stopped_status(e, mode=mode)
    target = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=button.target_id)
    await prepare_startable(e, monkeypatch)
    e.work = target
    views, _ = await prepare_view(e)
    acts = Acts(e.db.sessions, settings=e.db.settings, callbacks=e.channel._services.callbacks, outbox=e.sender.outbox, work=e.human.work, authority=e.live.authority, links=e.db.links)
    controller = WorkStartAdmissions(views, acts)
    original_activation, native_errors = work_execution.activate_work, []

    async def observed_activation(*args, **kwargs):
        try:
            return await original_activation(*args, **kwargs)
        except Exception as error:
            native_errors.append(error)
            if damage == "none":
                import logging

                logging.getLogger(__name__).exception("Actual Resume activation failed")
            raise

    monkeypatch.setattr(work_execution, "activate_work", observed_activation)
    original_admission = work_start_actions.activation_admission

    async def observed_admission(*args, **kwargs):
        try:
            return await original_admission(*args, **kwargs)
        except Exception as error:
            native_errors.append(error)
            raise

    monkeypatch.setattr(work_start_actions, "activation_admission", observed_admission)

    async def damage_cause():
        async with e.db.sessions.begin() as writer:
            if "intent" in damage:
                parent = await writer.get(TelegramOutboxRow, button.payload["anchor_outbox_id"])
                parent.payload = {key: value for key, value in parent.payload.items() if key != "work_event_intent"}
                if "all-source" in damage:
                    parent.payload = {key: value for key, value in parent.payload.items() if key not in {"work_status_update", "work_news_basis"}}
                if "markers" in damage or "all-source" in damage:
                    from app.telegram_bots.work_news_carriers import news_witness_id

                    journal = await writer.get(TelegramActRow, button.id)
                    remaining = {key: value for key, value in journal.continuation.items() if key != "news_origin"}
                    journal.continuation = None if remaining == {"version": 1} else remaining
                    await writer.delete(await writer.get(TelegramCallbackRow, news_witness_id(button.id)))
                return
            cause = await writer.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.work_id == target["id"], AgentWorkEventRow.action == "stop"))
            assert cause is not None
            if damage == "missing-cause":
                await writer.delete(cause)
            else:
                cause.created_at += timedelta(seconds=1)

    if damage.startswith("before-stage"):
        await damage_cause()
        with pytest.raises(ProductSourceRetired):
            await controller.stage(e.link, button.id, chat_id=e.link.chat_id, message_id=button.message_id, message_thread_id=None, guard=await guard_for(e))
        assert not e.launches
        return
    act = await controller.stage(e.link, button.id, chat_id=e.link.chat_id, message_id=button.message_id, message_thread_id=None, guard=await guard_for(e))
    assert act.state == "pending" and not e.launches
    if damage != "none":
        await damage_cause()
        result = await acts.check(act.id)
        assert result.state != "done" and not e.launches
        assert any(isinstance(error, ProductSourceRetired) for error in native_errors), native_errors
    else:
        import traceback

        result = await acts.check(act.id)
        details = [(type(error).__name__, str(error), [(frame.name, frame.lineno) for frame in traceback.extract_tb(error.__traceback__)[-8:]]) for error in native_errors]
        assert result.state == "done" and len(e.launches) == 1, (result.state, result.reason, details)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["root", "multipart"])
@pytest.mark.parametrize("damage", ["none", "before-native-intent-and-all-source"])
async def test_initial_and_multipart_status_resume_keep_full_ack_and_origin(work_delivery, monkeypatch, mode, damage):
    await test_stopped_status_resume_keeps_original_stop_proof(work_delivery, monkeypatch, damage, mode=mode)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["none", "before-native-intent-and-all-source"])
async def test_revealed_status_resume_retains_its_original_source_family(work_delivery, monkeypatch, damage):
    await test_stopped_status_resume_keeps_original_stop_proof(work_delivery, monkeypatch, damage, mode="revealed")
