"""A real stale Start hands off the current review without new admission."""

from dataclasses import replace
from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_callback_commands import press
from test_telegram_notification_runtime import runtime
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start_controls import start_control
from test_work_attention_pages import work_pages as work_pages
from test_work_execution import activation, run_record

from deerflow.agent_instances.work_execution import WorkExecution
from deerflow.agent_instances.work_execution_contract import ReportWork
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow


async def submit_current_work(e, *, sources=()):
    conversations = e.http_app.state.agent_conversations
    chat = await conversations.create(actor=e.actor, instance_id=e.instance.id, creation_id=uuid4().hex)
    execution = await conversations.execution(actor=e.actor, thread_id=chat["thread_id"])
    record = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"])
    scope = await WorkExecution(e.human.work).reserve(execution=execution, work_id=record["id"], request=activation(execution, record))
    execution = replace(execution, work=scope)
    run = run_record(execution, scope)
    run.user_id = e.actor.subject_id
    await scope.bind(execution, run, execution.thread_incarnation)
    context = await scope.context(execution)
    current = context["work"]
    if current["status"] == "blocked":
        assert context["request"]["purpose"] == "information" and context["responses"]
        basis = {**context["request"], "response_ids": [response["id"] for response in context["responses"]]}
        basis.pop("purpose")
        current = await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="assess_input", statement="The supplied reference resolves the blocker.", request_basis=basis))
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="outcome", statement="The complete current review result.", sources=list(sources)))
    await scope.finish(execution, run, settled=True)
    return await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=record["id"])


async def foreign_promise(e, operation_id):
    from deerflow.agent_instances.work_contract import DelegateWork
    from deerflow.agent_instances.work_execution_contract import ActivateWork

    foreign = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Independent work", success_criteria="Report its result"))
    conversations = e.http_app.state.agent_conversations
    chat = await conversations.create(actor=e.actor, instance_id=e.instance.id, creation_id=uuid4().hex)
    execution = await conversations.execution(actor=e.actor, thread_id=chat["thread_id"])
    return await WorkExecution(e.human.work).reserve(
        execution=execution, work_id=foreign["id"], request=ActivateWork(operation_id=operation_id, expected_revision=foreign["revision"], expected_assignment_revision=foreign["assignment_revision"], thread_id=execution.thread_id)
    )


async def saved_start_refusal(e, monkeypatch, *, composite=False):
    if composite:
        from test_telegram_assignment_card_delivery import prepare_card, save_card
        from test_telegram_work_start import prepare_startable

        from app.gateway import work_execution
        from app.telegram_bots.work_continuation_state import continuation_state

        plans, _draft, _inbox = await prepare_card(e)
        await prepare_startable(e, monkeypatch)
        await dispatch(e, 6461)
        assert not e.dispatch_errors, e.dispatch_errors
        button, staged = await save_card(e, plans, kind="assign_start")
        primary = await e.acts.check(staged.id)
        assert primary.state == "done" and not e.launches
        e.work = dict(primary.result)
        activate, results = work_execution.activate_work, []

        async def submitted(*args, **kwargs):
            results.append(await submit_current_work(e))
            return await activate(*args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(work_execution, "activate_work", submitted)
            settled = await e.acts.resume_continuation(primary.id)
        refused = await e.acts._work_record(continuation_state(settled)["child_id"])
        assert len(results) == 1
        return button, refused, results[0]
    from test_telegram_work_start_controls import stage

    _plans, controller, button = await start_control(e, monkeypatch)
    staged = await stage(e, controller, button)
    result = await submit_current_work(e)
    refused = await e.acts.check(staged.id)
    return button, refused, result


@pytest.mark.asyncio
@pytest.mark.parametrize("composite", [False, True])
@pytest.mark.parametrize("reopened", [False, True])
async def test_actual_saved_start_refusal_opens_the_current_full_review(work_delivery, monkeypatch, composite, reopened):
    from datetime import timedelta

    from app.telegram_bots.retention import OUTBOX_RETENTION_DAYS, sweep_rows

    e = work_delivery
    button, refused, result = await saved_start_refusal(e, monkeypatch, composite=composite)
    assert refused.state in {"refused", "conflict"} and refused.reason == "awaiting-review"
    owner = await runtime(e, preserve=True)
    if reopened:

        async def interrupted(*args, **kwargs):
            raise RuntimeError("interrupted after journal-backed reply committed")

        with monkeypatch.context() as patch:
            patch.setattr(owner.chat.start_reviews, "materialize", interrupted)
            identity = await owner.chat.executions.observe(e.link, refused.id, guard=owner.chat.guard(e.channel, e.link))
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramOutboxRow, identity)).parts == []
        await owner.stop()
        owner = await runtime(e, preserve=True)
        assert await owner.prepare_deliveries() == 1
    try:
        identity = await owner.chat.executions.observe(e.link, refused.id, guard=owner.chat.guard(e.channel, e.link))
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            assert row.payload.get("work_start_refusal_intent", {}).get("mode") == "act"
            assert row.request_id == result["human_input_request_id"]
            assert row.payload["work_start_refusal_intent"]["act_id"] == refused.id
            assert any("The complete current review result." in part.get("html", "") for part in row.parts)
            original = await reader.get(TelegramActRow, refused.id)
            assert original.body == dict(refused.body) and original.attempts == refused.attempts
            assert await reader.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id == refused.id)) is None
            consent = await reader.get(TelegramCallbackRow, button.id)
            assert consent.payload["work_action_id"] == (button.id if composite else refused.id)
            assert consent.closed_reason != "start-review"
        e.identity = identity
        await dispatch(e, 6462)
        assert not e.dispatch_errors, e.dispatch_errors
        later = e.clock.now() + timedelta(days=OUTBOX_RETENTION_DAYS + 1)
        await owner.host.callbacks.sweep(later)
        await sweep_rows(e.db.sessions, later, callback_max_age_days=owner.host.callbacks.config.callbacks.max_age_days)
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            origin = row.payload["work_request_basis"]["navigation_origin"]
            assert await reader.get(TelegramCallbackRow, button.id) is not None
            assert await reader.get(TelegramActRow, refused.id) is not None
            assert await reader.get(TelegramOutboxRow, origin["anchor_outbox_id"]) is not None
        restored = await owner.host.work_digest_items.navigation.restore(e.link, origin)
        assert restored.target.work_id == result["id"]
        assert not e.launches
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("topic", [None, 77])
async def test_actual_start_delivers_full_current_review_as_reply_with_notifications_off(work_delivery, monkeypatch, topic):
    e = work_delivery
    _plans, _controller, button = await start_control(e, monkeypatch, topic=topic)
    result = await submit_current_work(e)
    assert result["status"] == "submitted" and result["human_input_request_id"]
    owner = await runtime(e, preserve=True)
    e.acts = owner.host.acts
    failures = []
    report_failure = type(e.channel)._tell_press_failed

    async def observed(channel, row, error, **kwargs):
        failures.append(error)
        await report_failure(channel, row, error, **kwargs)

    monkeypatch.setattr(type(e.channel), "_tell_press_failed", observed)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).notify = "none"
    try:
        update = press(e, 6461, button.id, message_id=button.message_id, topic=topic)
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        if failures:
            raise failures[0]
        async with e.db.sessions() as reader:
            row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_start_refusal_intent"]["reason"].as_string() == "awaiting-review"))
            assert row is not None
            e.identity = row.id
            assert row.message_thread_id == topic
            assert row.class_ == "reply" and row.request_id == result["human_input_request_id"]
            assert "result_files" in row.payload["work_request_basis"]
            assert any("The complete current review result." in part.get("html", "") for part in row.parts)
            assert any("This assignment is waiting for a review" in part.get("html", "") for part in row.parts)
            controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == row.id)))
            assert controls and all(control.payload["presentation_pending"] for control in controls)
            assert await reader.get(TelegramActRow, button.id) is None
            assert await reader.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id == button.id)) is None
        e.channel.bot.request.enqueue("editMessageReplyMarkup", result=True)
        e.clock.advance(5)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        await dispatch(e, 6462, topic=topic)
        assert not e.dispatch_errors, e.dispatch_errors
        async with e.db.sessions() as reader:
            for control in controls:
                assert (await reader.get(TelegramCallbackRow, control.id)).payload["presentation_pending"] is False
        assert not e.launches
        sent = [call.parameters for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
        assert all(call.get("message_thread_id") == topic for call in sent)
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("topic", [None, 77])
@pytest.mark.parametrize("repair", [False, True])
async def test_review_reply_exports_actual_files_before_arming_controls(work_delivery, monkeypatch, tmp_path, topic, repair, *, accept_result=False):
    from test_telegram_bot_adapter import message
    from test_telegram_instance_file_admission import prepare_file_home

    from app.telegram_bots.files import FileBytesBudget
    from deerflow.agent_instances.work_contract import WorkSource
    from deerflow.persistence.spaces.model import SpaceGrantRow
    from deerflow.spaces.contract import Permission
    from deerflow.spaces.filesystem import ConfinedFilesystem

    e = await prepare_file_home(work_delivery, tmp_path, monkeypatch)
    e.http_app.state.storage_spaces = e.files
    _plans, _controller, button = await start_control(e, monkeypatch, topic=topic)
    instance = await e.file_service.get(actor=e.actor, instance_id=e.instance.id)
    home = await e.files.registry.get(actor=instance.principal, space_id=instance.home_id)
    await e.files.mkdir(actor=instance.principal, space_id=home.id, expected_generation=home.generation, operation_id=uuid4().hex, path="outputs")
    await e.files.write(actor=instance.principal, space_id=home.id, expected_generation=home.generation, operation_id=uuid4().hex, path="outputs/review.txt", content=b"verified review bytes", create=True)
    async with e.db.sessions.begin() as writer:
        writer.add(SpaceGrantRow(space_id=home.id, principal_kind="human", subject_id=e.actor.subject_id, permissions=int(Permission.READ | Permission.EXPORT)))
    await submit_current_work(e, sources=[WorkSource(space_id=home.id, path="outputs/review.txt")])
    reads, read = [], ConfinedFilesystem.read_bytes

    def recorded(filesystem, path, **kwargs):
        reads.append(path)
        return read(filesystem, path, **kwargs)

    monkeypatch.setattr(ConfinedFilesystem, "read_bytes", recorded)
    owner = await runtime(e, preserve=True)
    owner.host.file_bytes_budget = FileBytesBudget(owner.host.config.files.in_memory_max_bytes)
    try:
        identity = await owner.chat.start_reviews.open(
            e.link,
            button.id,
            query_id="review-files-query",
            chat_id=e.link.chat_id,
            message_id=button.message_id,
            message_thread_id=topic,
            guard=owner.chat.guard(e.channel, e.link),
        )
        e.identity = identity
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            assert [part["kind"] for part in row.parts] == ["document", "text"]
            controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == identity)))
        e.channel.bot.request.enqueue("editMessageReplyMarkup", result=True)
        e.clock.advance(5)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        message(e.channel.bot.request, "sendDocument", identity=6492, topic=topic)
        e.clock.advance(5)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        async with e.db.sessions() as reader:
            for control in controls:
                assert (await reader.get(TelegramCallbackRow, control.id)).payload["presentation_pending"] is True
        if repair:
            e.channel.bot.request.enqueue("sendMessage", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: message thread not found"})
            e.clock.advance(5)
            assert await e.sender.tick() == 1
            await e.sender.drain()
            async with e.db.sessions() as reader:
                held = await reader.get(TelegramOutboxRow, identity)
                assert (held.state, held.failed_reason) == ("failed", "work-result-refresh-required")
            restored = await owner.host.work_callbacks.result_recovery.repair(e.link, identity, guard=owner.chat.guard(e.channel, e.link))
            assert restored == identity
        await dispatch(e, 6493, topic=topic)
        if repair:
            from telegram.error import BadRequest

            assert len(e.dispatch_errors) == 1 and isinstance(e.dispatch_errors[0], BadRequest)
        else:
            assert not e.dispatch_errors, e.dispatch_errors
        assert reads == ["outputs/review.txt"]
        async with e.db.sessions() as reader:
            current = await reader.get(TelegramOutboxRow, identity)
            assert current.state == "sent" and current.message_thread_id == topic
            current_controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == current.parts[-1]["callback_group_id"])))
            assert current_controls and all(not control.payload["presentation_pending"] for control in current_controls)
            if repair:
                for control in controls:
                    assert (await reader.get(TelegramCallbackRow, control.id)).consumed_at is not None
        delivered = [call for call in e.channel.bot.request.calls if call.endpoint in {"sendDocument", "sendMessage"}][-(3 if repair else 2) :]
        assert [call.endpoint for call in delivered] == (["sendDocument", "sendMessage", "sendMessage"] if repair else ["sendDocument", "sendMessage"])
        assert all(call.parameters.get("message_thread_id") == topic for call in delivered)
        assert owner.host.file_bytes_budget.used == 0
        if accept_result:
            from sqlalchemy import func

            from deerflow.persistence.agent_instances.human_input import HumanInputEventRow

            opening = next(control for control in current_controls if control.kind == "accept_open")
            e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
            assert await e.channel.handle_update(press(e, 6501, opening.id, message_id=6493, topic=topic))
            await e.channel._drain_ingress()
            async with e.db.sessions() as reader:
                preview = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_request_basis"]["origin"]["callback_id"].as_string() == opening.id))
                assert preview is not None and preview.parts
                e.identity = preview.id
                e.channel.bot.request.enqueue("editMessageReplyMarkup", result=True)
                e.clock.advance(5)
                assert await e.sender.tick() == 1
                await e.sender.drain()
            await dispatch(e, 6502, topic=topic)
            async with e.db.sessions() as reader:
                confirm = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == preview.id, TelegramCallbackRow.kind == "accept_confirm"))
                assert confirm is not None and not confirm.payload["presentation_pending"]
            update = press(e, 6503, confirm.id, message_id=6502, topic=topic)
            e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
            assert await e.channel.handle_update(update)
            await e.channel._drain_ingress()
            current_work = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"])
            assert current_work["status"] == "completed"
            e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
            assert await e.channel.handle_update(update)
            await e.channel._drain_ingress()
            async with e.db.sessions() as reader:
                assert await reader.scalar(select(func.count()).select_from(HumanInputEventRow).where(HumanInputEventRow.operation_id == confirm.id)) == 1
                assert await reader.get(TelegramActRow, button.id) is None
        assert not e.launches
    finally:
        await owner.stop()


@pytest.mark.asyncio
async def test_review_handoff_accepts_once_through_actual_acknowledged_sdk_controls(work_delivery, monkeypatch, tmp_path):
    await test_review_reply_exports_actual_files_before_arming_controls(work_delivery, monkeypatch, tmp_path, 77, False, accept_result=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_controls", [False, True])
async def test_live_review_reply_retains_its_real_start_and_cleanup_through_sweeps(work_delivery, monkeypatch, missing_controls):
    from datetime import timedelta

    from app.telegram_bots.retention import OUTBOX_RETENTION_DAYS, sweep_rows

    e = work_delivery
    _plans, _controller, button = await start_control(e, monkeypatch)
    await submit_current_work(e)
    owner = await runtime(e, preserve=True)
    try:
        identity = await owner.chat.start_reviews.open(
            e.link,
            button.id,
            query_id="retained-review-query",
            chat_id=e.link.chat_id,
            message_id=button.message_id,
            message_thread_id=None,
            guard=owner.chat.guard(e.channel, e.link),
        )
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            origin = row.payload["work_request_basis"]["navigation_origin"]
            cleanup_id = row.payload["work_start_refusal_intent"]["cleanup_id"]
        e.identity = identity
        e.channel.bot.request.enqueue("editMessageReplyMarkup", result=True)
        e.clock.advance(5)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        await dispatch(e, 6482)
        assert not e.dispatch_errors, e.dispatch_errors
        if missing_controls:
            async with e.db.sessions.begin() as writer:
                for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == identity)):
                    await writer.delete(control)
        later = e.clock.now() + timedelta(days=OUTBOX_RETENTION_DAYS + 1)
        await owner.host.callbacks.sweep(later)
        await sweep_rows(e.db.sessions, later, callback_max_age_days=owner.host.callbacks.config.callbacks.max_age_days)
        async with e.db.sessions() as reader:
            assert await reader.get(TelegramCallbackRow, button.id) is not None
            assert await reader.get(TelegramOutboxRow, origin["anchor_outbox_id"]) is not None
            assert await reader.get(TelegramOutboxRow, cleanup_id) is not None
        restored = await owner.host.work_digest_items.navigation.restore(e.link, origin)
        assert restored.target.work_id == e.work["id"]
        assert not e.launches
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["gesture", "role", "age"])
async def test_original_source_loss_rolls_back_first_review_publication(work_delivery, monkeypatch, change):
    import app.gateway.authz as authz
    import app.telegram_bots.work_start_review_replies as module
    from app.telegram_bots.callbacks import CallbackRefused
    from app.telegram_bots.producer import ProductSourceRetired

    e = work_delivery
    _plans, _controller, button = await start_control(e, monkeypatch)
    await submit_current_work(e)
    owner = await runtime(e, preserve=True)
    prepare, changed, active, calls = module.prepare_attention, [], [], []
    resolve = authz.resolve_route_permissions

    async def current_permissions(*args, **kwargs):
        held = await resolve(*args, **kwargs)
        return [value for value in held if value != "agents:write"] if active and change == "role" else held

    monkeypatch.setattr(authz, "resolve_route_permissions", current_permissions)

    async def prepared(*args, **kwargs):
        current = await prepare(*args, **kwargs)

        async def check(**options):
            writer = options.get("session")
            if writer is not None:
                calls.append(True)
                if change == "age" and len(calls) == 2:
                    e.clock.advance((owner.host.callbacks.config.callbacks.max_age_days + 1) * 86400)
            if writer is not None and not changed:
                if change == "gesture":
                    parent = await writer.get(TelegramOutboxRow, button.payload["anchor_outbox_id"])
                    gesture = await writer.get(TelegramCallbackRow, parent.payload["work_view_intent"]["origin"]["gesture_id"])
                    assert gesture is not None
                    await writer.delete(gesture)
                    await writer.flush()
                changed.append(True)
            if writer is not None:
                active.append(writer)
            try:
                return await current(**options)
            finally:
                active.clear()

        return check

    monkeypatch.setattr(module, "prepare_attention", prepared)
    try:
        with pytest.raises((ProductSourceRetired, CallbackRefused) if change == "age" else ProductSourceRetired):
            await owner.chat.start_reviews.open(
                e.link,
                button.id,
                query_id="lost-original-query",
                chat_id=e.link.chat_id,
                message_id=button.message_id,
                message_thread_id=None,
                guard=owner.chat.guard(e.channel, e.link),
            )
        assert changed
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramCallbackRow, button.id)).consumed_at is None
            assert await reader.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.payload["work_start_refusal_intent"]["reason"].as_string() == "awaiting-review")) is None
            assert await reader.get(TelegramActRow, button.id) is None
        assert not e.launches
    finally:
        await owner.stop()


@pytest.mark.asyncio
async def test_staged_review_reply_is_materialized_by_a_reopened_runtime(work_delivery, monkeypatch):
    from test_telegram_work_interaction_delivery import guard_for

    e = work_delivery
    _plans, _controller, button = await start_control(e, monkeypatch)
    result = await submit_current_work(e)
    first = await runtime(e, preserve=True)

    async def interrupted(*args, **kwargs):
        raise RuntimeError("interrupted after the real reply intent committed")

    monkeypatch.setattr(first.chat.start_reviews, "materialize", interrupted)
    try:
        with pytest.raises(RuntimeError, match="reply intent committed"):
            await first.chat.start_reviews.open(
                e.link,
                button.id,
                query_id="recorded-restart-query",
                chat_id=e.link.chat_id,
                message_id=button.message_id,
                message_thread_id=None,
                guard=await guard_for(e),
            )
        async with e.db.sessions() as reader:
            row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_start_refusal_intent"]["reason"].as_string() == "awaiting-review"))
            assert row is not None and row.parts == []
            identity = row.id
    finally:
        await first.stop()
    second = await runtime(e, preserve=True)
    try:
        assert await second.prepare_deliveries() == 1
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            assert row.parts and row.request_id == result["human_input_request_id"]
            assert row.payload["work_request_basis"]["navigation_origin"]["start_id"] == identity
            assert await reader.get(TelegramActRow, button.id) is None
        e.identity = identity
        e.channel.bot.request.enqueue("editMessageReplyMarkup", result=True)
        e.clock.advance(5)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        await dispatch(e, 6472)
        assert not e.dispatch_errors, e.dispatch_errors
        assert not e.launches
    finally:
        await second.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["inspect", "manager", "recipient", "cleanup", "operation", "witness_message", "witness_expiry"])
async def test_last_review_reply_fence_rechecks_actual_owners(work_delivery, monkeypatch, change, *, journal=False, composite=False):
    from app.gateway.auth.models import User
    from deerflow.agent_instances.human_input_contract import RequestCommand
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    if journal:
        button, refused, result = await saved_start_refusal(e, monkeypatch, composite=composite)
        operation_id = refused.id
    else:
        _plans, _controller, button = await start_control(e, monkeypatch)
        result = await submit_current_work(e)
        operation_id = button.id
    owner = await runtime(e, preserve=True)
    try:
        if journal:
            await owner.chat.executions.observe(e.link, refused.id, guard=owner.chat.guard(e.channel, e.link))
        else:
            await owner.chat.start_reviews.open(
                e.link,
                button.id,
                query_id="last-review-query",
                chat_id=e.link.chat_id,
                message_id=button.message_id,
                message_thread_id=None,
                guard=owner.chat.guard(e.channel, e.link),
            )
        async with e.db.sessions() as reader:
            row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_start_refusal_intent"]["reason"].as_string() == "awaiting-review"))
            assert row is not None and row.parts
            e.identity = row.id
            cleanup_id = row.payload["work_start_refusal_intent"]["cleanup_id"]
            witness_id = row.payload["work_start_refusal_intent"]["gesture_id"]
        if not journal:
            e.channel.bot.request.enqueue("editMessageReplyMarkup", result=True)
            e.clock.advance(5)
            assert await e.sender.tick() == 1
            await e.sender.drain()
        sources = e.channel._services.work_result_sources
        original, changed = sources.settings.get, []

        async def retire(*args, **kwargs):
            settings = await original(*args, **kwargs)
            if not changed:
                changed.append(True)
                if change == "recipient":
                    colleague = await e.live.users.create_user(User(email="current-reviewer@example.test"))
                    async with e.db.sessions.begin() as writer:
                        writer.add(AgentInstanceGrantRow(instance_id=e.instance.id, user_id=str(colleague.id), permissions=7))
                    source = await owner.requests.reader.read_request(e.link, result["human_input_request_id"])
                    await e.human.command(
                        actor=e.actor,
                        request_id=source.request["id"],
                        request=RequestCommand(
                            operation_id=uuid4().hex,
                            action="route",
                            expected_revision=source.seen["revision"],
                            expected_request_revision=source.seen["request_revision"],
                            recipient_id=str(colleague.id),
                        ),
                    )
                elif change == "operation":
                    await foreign_promise(e, operation_id)
                else:
                    async with e.db.sessions.begin() as writer:
                        if change == "cleanup":
                            (await writer.get(TelegramOutboxRow, cleanup_id)).chat_id += 1
                        elif change == "witness_message":
                            (await writer.get(TelegramCallbackRow, witness_id)).message_id += 1
                        elif change == "witness_expiry":
                            from datetime import timedelta

                            (await writer.get(TelegramCallbackRow, witness_id)).expires_at += timedelta(days=1)
                        elif change == "journal":
                            (await writer.get(TelegramActRow, operation_id)).attempts += 1
                        elif change == "origin":
                            await writer.delete(await writer.get(TelegramCallbackRow, button.id))
                        else:
                            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1 if change == "inspect" else 3
            return settings

        monkeypatch.setattr(sources.settings, "get", retire)
        before = len(e.channel.bot.request.calls)
        e.clock.advance(5)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        assert changed
        assert not [call for call in e.channel.bot.request.calls[before:] if call.endpoint in {"sendMessage", "sendDocument", "sendMediaGroup"}]
        async with e.db.sessions() as reader:
            if not journal:
                assert await reader.get(TelegramActRow, button.id) is None
            else:
                assert (await reader.get(TelegramActRow, operation_id)).body == dict(refused.body)
            promise = await reader.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == operation_id))
            if change == "operation":
                assert promise is not None and promise.work_id != result["id"] and promise.run_id is None
            else:
                assert promise is None
        assert not e.launches
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("composite,change", [(False, "inspect"), (False, "operation"), (False, "journal"), (True, "manager"), (True, "recipient"), (True, "origin"), (True, "witness_expiry")])
async def test_saved_refusal_review_rechecks_source_and_native_absence(work_delivery, monkeypatch, composite, change):
    await test_last_review_reply_fence_rechecks_actual_owners(work_delivery, monkeypatch, change, journal=True, composite=composite)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["expired", "late_expired", "role"])
async def test_ineligible_saved_start_commits_no_review_owner(work_delivery, monkeypatch, change):
    import app.gateway.authz as authz
    import app.telegram_bots.work_start_review_replies as module

    e = work_delivery
    _button, refused, _result = await saved_start_refusal(e, monkeypatch)
    owner = await runtime(e, preserve=True)
    prepare, resolve, active, calls = module.prepare_attention, authz.resolve_route_permissions, [], []

    async def held_permissions(*args, **kwargs):
        held = await resolve(*args, **kwargs)
        return [value for value in held if value != "agents:write"] if active and change == "role" else held

    async def prepared(*args, **kwargs):
        current = await prepare(*args, **kwargs)

        async def check(**options):
            if options.get("session") is not None:
                active.append(True)
                calls.append(True)
                if change == "expired" and len(calls) == 1 or change == "late_expired" and len(calls) == 2:
                    e.clock.advance((owner.host.callbacks.config.callbacks.max_age_days + 1) * 86400)
            try:
                return await current(**options)
            finally:
                active.clear()

        return check

    monkeypatch.setattr(authz, "resolve_route_permissions", held_permissions)
    monkeypatch.setattr(module, "prepare_attention", prepared)
    try:
        assert await owner.chat.start_reviews.from_act(e.link, refused.id, guard=owner.chat.guard(e.channel, e.link)) is None
        assert calls
        async with e.db.sessions() as reader:
            assert await reader.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.payload["work_start_refusal_intent"]["act_id"].as_string() == refused.id)) is None
            actual = await reader.get(TelegramActRow, refused.id)
            assert actual.body == dict(refused.body) and actual.attempts == refused.attempts
        assert not e.launches
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("composite", [False, True])
@pytest.mark.parametrize("missing_intent", [False, True])
async def test_aged_pending_review_keeps_actual_saved_admission_family(work_delivery, monkeypatch, composite, missing_intent):
    from datetime import timedelta

    from app.telegram_bots.retention import sweep_rows
    from app.telegram_bots.work_delegate_actions import delegate_source

    e = work_delivery
    button, refused, _result = await saved_start_refusal(e, monkeypatch, composite=composite)
    required = {refused.id}
    if composite:
        primary = await e.acts._work_record(button.id)
        required.update({primary.id, delegate_source(primary)["draft_id"]})
    owner = await runtime(e, preserve=True)

    async def interrupted(*args, **kwargs):
        raise RuntimeError("preserve the actual committed handoff before materialization")

    monkeypatch.setattr(owner.chat.start_reviews, "materialize", interrupted)
    try:
        identity = await owner.chat.executions.observe(e.link, refused.id, guard=owner.chat.guard(e.channel, e.link))
        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramOutboxRow, identity)
            assert row.parts == []
            assert await writer.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.payload["work_execution_intent"]["act_id"].as_string() == refused.id)) is None
            if missing_intent:
                row.payload = {"presentation_version": 0}
        later = e.clock.now() + timedelta(days=401)
        for _ in range(2):
            await owner.host.callbacks.sweep(later)
            await sweep_rows(e.db.sessions, later, callback_max_age_days=owner.host.callbacks.config.callbacks.max_age_days)
        async with e.db.sessions() as reader:
            for actual_id in required:
                assert await reader.get(TelegramActRow, actual_id) is not None, actual_id
            assert await reader.get(TelegramCallbackRow, button.id) is not None
            assert await reader.get(TelegramOutboxRow, button.payload["anchor_outbox_id"]) is not None
            assert await reader.get(TelegramOutboxRow, identity) is not None
        assert not e.launches
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_intent", [False, True])
@pytest.mark.parametrize("foreign_kind", ["direct", "composite"])
async def test_saved_review_retention_rejects_another_actual_work_ack(work_delivery, monkeypatch, missing_intent, foreign_kind):
    from copy import deepcopy
    from datetime import timedelta

    from _telegram_bot_test_support import make_update
    from test_telegram_work_interaction_delivery import guard_for
    from test_telegram_work_start import prepare_startable

    from app.telegram_bots.producer import _item
    from app.telegram_bots.work_news_delivery import WorkNewsSources
    from app.telegram_bots.work_start_refresh_retention import refresh_dependencies
    from app.telegram_bots.work_view_delivery import WorkViewPlans
    from app.telegram_bots.work_view_origins import anchor_signature
    from app.telegram_bots.work_view_sources import WorkViewReader

    e = work_delivery
    _own_button, refused, _result = await saved_start_refusal(e, monkeypatch)
    owner = await runtime(e, preserve=True)

    async def interrupted(*args, **kwargs):
        raise RuntimeError("keep the actual review intent pending")

    monkeypatch.setattr(owner.chat.start_reviews, "materialize", interrupted)
    try:
        identity = await owner.chat.executions.observe(e.link, refused.id, guard=owner.chat.guard(e.channel, e.link))
        async with e.db.sessions.begin() as writer:
            for pending in await writer.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.state == "pending")):
                pending.not_before = owner.host.outbox.clock() + timedelta(hours=1)
        if foreign_kind == "composite":
            from test_telegram_assignment_card_delivery import prepare_card, save_card

            plans, _draft, _inbox = await prepare_card(e, objective="An independent assignment")
            await prepare_startable(e, monkeypatch)
            await dispatch(e, 6611)
            foreign_control, saved = await save_card(e, plans, kind="assign_start")
            primary = await e.acts.check(saved.id)
            assert primary.state == "done"
            foreign_id = foreign_control.id
        else:
            await prepare_startable(e, monkeypatch)
            plans = WorkViewPlans(reader=WorkViewReader(work=e.human.work, authority=e.live.authority, links=e.db.links, clock=e.sender.outbox.clock), outbox=e.sender.outbox, callbacks=e.channel._services.callbacks)
            e.channel._services.work_view_sources = WorkNewsSources(plans)
            inbox, _ = await e.sender.outbox.inbox.accept(e.link, make_update(6600, account=e.link.chat_id, text="/work"), sanitized_input={"command": "work"})
            e.identity = await plans.command(e.link, inbox, guard=await guard_for(e))
            await dispatch(e, 6601)
            async with e.db.sessions() as reader:
                detail = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "work_details", TelegramCallbackRow.target_id == e.work["id"]))
            e.identity = await plans.open(e.link, detail.id, query_id="independent-work-preview", chat_id=e.link.chat_id, message_id=6601, message_thread_id=None, guard=await guard_for(e))
            await dispatch(e, 6602)
            async with e.db.sessions() as reader:
                foreign_id = await reader.scalar(select(TelegramCallbackRow.id).where(TelegramCallbackRow.kind == "work_start", TelegramCallbackRow.target_id == e.work["id"]))
        assert not e.dispatch_errors, e.dispatch_errors
        async with e.db.sessions.begin() as writer:
            foreign = await writer.get(TelegramCallbackRow, foreign_id)
            parent = await writer.get(TelegramOutboxRow, foreign.payload["anchor_outbox_id"])
            reply = await writer.get(TelegramOutboxRow, identity)
            intent = deepcopy(reply.payload["work_start_refusal_intent"])
            intent.update(callback_id=foreign.id, anchor_outbox_id=parent.id, presentation_version=foreign.payload["presentation_version"], anchor_signature=anchor_signature(_item(parent)))
            witness = await writer.get(TelegramCallbackRow, intent["gesture_id"])
            witness.payload, witness.message_id = intent, foreign.message_id
            reply.payload = {"presentation_version": 0} if missing_intent else {**reply.payload, "work_start_refusal_intent": intent}
            foreign_group, foreign_parent = foreign.group_id, parent.id
        async with e.db.sessions() as reader:
            pinned = await refresh_dependencies(reader, e.sender.outbox.clock() + timedelta(days=401))
            assert foreign_group not in pinned.groups
            assert foreign_parent not in pinned.deliveries
            assert identity in pinned.deliveries
        assert not e.launches
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("composite", [False, True])
@pytest.mark.parametrize("later_replay", [None, "before", "after"])
async def test_context_recovered_start_uses_its_actual_refused_child_review(work_delivery, monkeypatch, composite, later_replay, *, damage=None, fallback=False):
    from test_telegram_start_context_delivery import consent, start
    from test_telegram_start_context_ownership import arm_conversation_change

    from app.gateway import work_execution
    from app.telegram_bots.work_start_conversations import context_plan

    e = work_delivery
    parent, button = await consent(e, monkeypatch, "composite" if composite else "news")
    if composite:
        actual = await e.acts._work_record(parent.id)
        e.work = dict(actual.result)
    calls = arm_conversation_change(e, monkeypatch)
    native, results = work_execution.activate_work, []

    async def submit_before_effective_call(*args, **kwargs):
        if len(calls) == 1:
            results.append(await submit_current_work(e))
        return await native(*args, **kwargs)

    monkeypatch.setattr(work_execution, "activate_work", submit_before_effective_call)
    root = await start(e, parent, "composite" if composite else "news")
    assert len(calls) == 2 and len(results) == 1
    effective = await e.acts._work_record(context_plan(root)["child_id"])
    assert effective.kind == "activate_context" and effective.reason == "awaiting-review" and effective.state in {"refused", "conflict"}
    if later_replay == "before":
        e.clock.advance(7201)
        root = await e.acts.check(root.id)
        assert root.state == "uncertain"
    owner = await runtime(e, preserve=True)
    if fallback:
        import app.gateway.authz as authz

        resolve = authz.resolve_route_permissions

        async def read_only(*args, **kwargs):
            return [permission for permission in await resolve(*args, **kwargs) if permission != "agents:write"]

        monkeypatch.setattr(authz, "resolve_route_permissions", read_only)
    try:
        identity = await owner.chat.executions.observe(e.link, root.id, guard=owner.chat.guard(e.channel, e.link))
        if later_replay == "after":
            e.clock.advance(7201)
            root = await e.acts.check(root.id)
            assert root.state == "uncertain"
            await owner.stop()
            owner = await runtime(e, preserve=True)
            assert await owner.chat.executions.observe(e.link, root.id, guard=owner.chat.guard(e.channel, e.link)) == identity
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            if fallback:
                assert row.payload["work_execution_intent"]["act_id"] == root.id
                assert "work_start_refusal_intent" not in row.payload
                assert row.work_id == root.work_id
            else:
                assert row.payload.get("work_start_refusal_intent", {}).get("mode") == "act"
                assert row.payload["work_start_refusal_intent"]["act_id"] == root.id
                assert row.request_id == results[0]["human_input_request_id"]
            assert await reader.get(TelegramCallbackRow, effective.id) is None
            assert await reader.scalar(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id.in_([root.id, effective.id]))) is None
            assert (await reader.get(TelegramCallbackRow, button.id)).payload["work_action_id"] == (parent.id if composite else root.id)
        e.identity = identity
        from datetime import timedelta

        # Native activity also queues real status plans. Keep those background
        # deliveries queued while this SDK probe sends the manual reply.
        async with e.db.sessions.begin() as writer:
            for pending in await writer.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.id != identity, TelegramOutboxRow.state == "pending")):
                pending.not_before = owner.host.outbox.clock() + timedelta(hours=1)
        changed = []
        if damage is not None:
            sources = e.channel._services.work_execution_sources if fallback else e.channel._services.work_result_sources
            settings_get = sources.settings.get

            async def retire(*args, **kwargs):
                settings = await settings_get(*args, **kwargs)
                if not changed:
                    changed.append(True)
                    if damage == "native":
                        await foreign_promise(e, effective.id)
                    else:
                        async with e.db.sessions.begin() as writer:
                            if damage == "root":
                                await writer.delete(await writer.get(TelegramActRow, root.id))
                            elif damage == "origin":
                                await writer.delete(await writer.get(TelegramCallbackRow, button.id))
                            else:
                                (await writer.get(TelegramActRow, effective.id)).attempts += 1
                return settings

            monkeypatch.setattr(sources.settings, "get", retire)
        before = len(e.channel.bot.request.calls)
        await dispatch(e, 6463)
        if damage is not None:
            assert changed
            assert not [call for call in e.channel.bot.request.calls[before:] if call.endpoint in {"sendMessage", "sendDocument", "sendMediaGroup"}]
        elif e.dispatch_errors:
            raise e.dispatch_errors[0]
        elif fallback:
            sent = [call.parameters["text"] for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"]
            assert sent and all("The complete current review result." not in text for text in sent)
        assert not e.launches
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("composite,damage", [(False, "child"), (True, "root"), (False, "native")])
async def test_context_review_last_sdk_fence_requires_both_actual_journals(work_delivery, monkeypatch, composite, damage):
    await test_context_recovered_start_uses_its_actual_refused_child_review(work_delivery, monkeypatch, composite, None, damage=damage)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "origin"])
async def test_ineligible_context_review_keeps_qualified_execution_notice(work_delivery, monkeypatch, damage):
    await test_context_recovered_start_uses_its_actual_refused_child_review(work_delivery, monkeypatch, False, None, damage=damage, fallback=True)


async def ready_response(e, monkeypatch):
    from test_telegram_voice_continuations import pending_voice_child

    from app.gateway import work_execution

    # This existing recorded transport fails before native admission. Its
    # original intent/unknown-call count survive into the real refused retry.
    acts, primary, original = await pending_voice_child(e, monkeypatch)
    e.acts = acts
    monkeypatch.setattr(work_execution, "activate_work", original)
    record = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=primary.work_id)
    return record, None, primary, primary.continuation["request_activation"]["outcome_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["decide", "respond"])
async def test_aged_request_review_handoff_retains_actual_request_confirmation(work_delivery, monkeypatch, kind, *, delivered=False):
    from datetime import timedelta

    from test_telegram_request_continuations import ready_decision
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup

    from app.gateway import work_execution
    from app.telegram_bots.retention import sweep_rows

    e = work_delivery
    native, results, native_errors = work_execution.activate_work, [], []

    async def submitted(*args, **kwargs):
        e.work = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=kwargs["work_id"])
        results.append(await submit_current_work(e))
        assert results[-1]["status"] == "submitted", (kind, results[-1], kwargs["work_id"], e.work["id"])
        try:
            return await native(*args, **kwargs)
        except Exception as error:
            import traceback

            native_errors.append("".join(traceback.format_exception(error)))
            raise

    # The voice fixture replaces/restores this same hook while recording its
    # initial unavailable transport. One patch stack must own the whole lifetime.
    monkeypatch.setattr(work_execution, "activate_work", submitted)
    record, _request, parent, outcome_id = await (ready_decision(e, monkeypatch) if kind == "decide" else ready_response(e, monkeypatch))
    e.work, e.outcome_identity, e.identity = record, outcome_id, outcome_id
    await clear_outcome_cleanup(e)
    await dispatch(e, 6464)
    assert not e.dispatch_errors, e.dispatch_errors
    parent = await e.acts._work_record(parent.id)
    assert "work_outcome" in parent.continuation
    primary = await e.acts.resume_continuation(parent.id)
    child = await e.acts._work_record(primary.continuation["activation_continuation"]["child_id"])
    assert results
    assert results[0]["id"] == child.work_id, (results, child.work_id, e.work["id"])
    assert results[0]["status"] == "submitted", results[0]
    assert child.state in {"refused", "conflict"} and child.reason == "awaiting-review" and len(results) == 1, "\n".join(native_errors)
    owner = await runtime(e, preserve=True)

    async def interrupted(*args, **kwargs):
        raise RuntimeError("keep the real pending review handoff")

    if not delivered:
        monkeypatch.setattr(owner.chat.start_reviews, "materialize", interrupted)
    try:
        identity = await owner.chat.executions.observe(e.link, child.id, guard=owner.chat.guard(e.channel, e.link))
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            assert row.payload.get("work_start_refusal_intent", {}).get("mode") == "act"
            assert bool(row.parts) is delivered
            assert await reader.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.payload["work_execution_intent"]["act_id"].as_string() == child.id)) is None
        if delivered:
            e.identity = identity
            await dispatch(e, 6466)
            assert not e.dispatch_errors, e.dispatch_errors
            async with e.db.sessions() as reader:
                row = await reader.get(TelegramOutboxRow, identity)
                assert row.state == "sent" and any("The complete current review result." in part.get("html", "") for part in row.parts)
                groups = [part["callback_group_id"] for part in row.parts if "callback_group_id" in part]
                controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id.in_(groups))))
                assert controls and all(control.payload["presentation_pending"] is False for control in controls)
                assert await reader.get(TelegramCallbackRow, child.id) is None
            assert not e.launches
            return
        later = e.clock() + timedelta(days=401)
        for _ in range(2):
            await owner.host.callbacks.sweep(later)
            await sweep_rows(e.db.sessions, later, callback_max_age_days=owner.host.callbacks.config.callbacks.max_age_days)
        async with e.db.sessions() as reader:
            assert await reader.get(TelegramActRow, parent.id) is not None
            assert await reader.get(TelegramActRow, child.id) is not None
            for outbox_id in (identity, outcome_id, primary.continuation["request_activation"]["origin"]["anchor_outbox_id"]):
                assert await reader.get(TelegramOutboxRow, outbox_id) is not None
            assert await reader.get(TelegramCallbackRow, parent.id) is not None
        assert not e.launches
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["decide", "respond"])
async def test_request_start_refusal_delivers_actual_current_full_review(work_delivery, monkeypatch, kind):
    await test_aged_request_review_handoff_retains_actual_request_confirmation(work_delivery, monkeypatch, kind, delivered=True)
