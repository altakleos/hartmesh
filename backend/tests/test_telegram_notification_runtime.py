"""Installed notification owners use the real Work journal and one SDK sender."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.agent_instances.human_input_contract import CreateRequest
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow, TelegramWorkCursorRow


async def runtime(e, *, preserve=False):
    from app.telegram_bots.acts import Acts
    from app.telegram_bots.callbacks import Prompts
    from app.telegram_bots.commands import CallbackCommands
    from app.telegram_bots.notification_runtime import NotificationRuntime

    host = e.channel._services
    host.app, host.thread_client, host.config = e.http_app, e.http_client, e.db.config
    host.links, host.settings = e.db.links, e.db.settings
    host.send_allowed = lambda: e.active[0]
    host.channel = lambda identity: e.channel if identity == e.link.id else None

    async def values(link):
        return {"name": "Fixture agent", "product": "HartMesh"}

    host.prepare_ingress_values = values
    host.prepare_live_owner = e.live.authority.prepare_live_owner
    host.acts = Acts(e.db.sessions, settings=host.settings, callbacks=host.callbacks, outbox=host.outbox)
    host.prompts = Prompts(host.callbacks)
    host.commands = CallbackCommands(host)
    owner = NotificationRuntime(host)
    owner.install()
    if not preserve:
        async with e.db.sessions.begin() as writer:
            for row in await writer.scalars(select(TelegramOutboxRow)):
                row.state, row.skip_reason = "skipped", "superseded"
    return owner


@pytest.mark.asyncio
async def test_installed_runtime_baselines_history_then_delivers_new_question(work_delivery):
    e = work_delivery
    owner = await runtime(e)
    owner.baselines.install_binding_hook()
    assert await owner.baselines.initialize() == 1
    assert await owner.events.tick() == 0
    work = await e.human.work.delegate(actor=e.actor, instance_id=e.link.agent_ref, request=DelegateWork(operation_id=uuid4().hex, objective="Prepare the next guide", success_criteria="Give verified references"))
    from app.gateway.auth.models import User
    from deerflow.agent_instances.contract import PrincipalRef
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    colleague = await e.live.users.create_user(User(email="notification-colleague@example.com"))
    async with e.db.sessions.begin() as writer:
        writer.add(AgentInstanceGrantRow(instance_id=e.link.agent_ref, user_id=str(colleague.id), permissions=7))
    request = await e.human.create(
        actor=PrincipalRef("human", str(colleague.id)),
        instance_id=e.link.agent_ref,
        work_id=work["id"],
        request=CreateRequest(
            operation_id=uuid4().hex,
            expected_work_revision=1,
            expected_assignment_revision=1,
            purpose="information",
            question="Which new reference?",
            reason="A reference is required",
            expected_response="A link",
            recipient_id=e.actor.subject_id,
        ),
    )
    # Global refusal gates effects, while the metadata reader still advances.
    e.active[0] = False
    await owner.events.tick()
    assert await owner.before_send() == 0
    await owner.tick_acts()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    async with e.db.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.request_id == request["id"])))
        assert len(rows) == 1 and not rows[0].parts
        identity = rows[0].id
    e.active[0] = True
    await owner.before_send()
    message(e.channel.bot.request, identity=9001)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent", e.dispatch_errors
        assert (await reader.get(TelegramWorkCursorRow, (e.link.id, work["id"]))).revision == 2
    sent = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][-1]
    assert "Which new reference?" in sent.parameters["text"]
    assert sent.parameters["reply_markup"]
    assert await owner.baselines.initialize() == 0
    await owner.stop()


@pytest.mark.asyncio
async def test_runtime_refusal_prevents_planning_and_action_retry(work_delivery, monkeypatch):
    e = work_delivery
    owner = await runtime(e)
    e.active[0] = False

    def forbidden():
        raise AssertionError("refused runtime entered storage")

    monkeypatch.setattr(owner.host.outbox, "sessions", forbidden)
    assert await owner.prepare_deliveries() == 0
    assert await owner.before_send() == 0
    await owner.tick_maintenance()
    monkeypatch.setattr(owner.host.acts, "session_factory", forbidden)
    await owner.tick_acts()
    await owner.worker.tick()
    assert await owner.summaries.tick() == 0
    await owner.stop()


@pytest.mark.asyncio
async def test_runtime_digest_reserves_only_installed_source_families(work_delivery):
    from copy import deepcopy
    from datetime import timedelta

    e = work_delivery
    owner = await runtime(e)
    outbox = owner.host.outbox
    outbox.config = outbox.config.model_copy(update={"outbox": outbox.config.outbox.model_copy(update={"digest_threshold": 1})})
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, e.identity)
        row.state, row.skip_reason = "pending", None
        fields = {column.key: deepcopy(getattr(row, column.key)) for column in row.__mapper__.column_attrs}
        fields.update(id=uuid4().hex, dedupe_key="rejected:" + uuid4().hex, class_="service", payload={"presentation_version": 0}, created_at=row.created_at - timedelta(seconds=1))
        unrelated = TelegramOutboxRow(**fields)
        writer.add(unrelated)
        unrelated_id = unrelated.id
    await owner.before_send()
    async with e.db.sessions() as reader:
        digest = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.class_ == "digest"))
        assert digest.payload["member_ids"] == [e.identity]
        assert "digest_id" not in (await reader.get(TelegramOutboxRow, unrelated_id)).payload
    await owner.stop()


@pytest.mark.asyncio
async def test_runtime_refusal_prevents_digest_reservation(work_delivery, monkeypatch):
    e = work_delivery
    owner = await runtime(e)
    e.active[0] = False

    def forbidden(*args, **kwargs):
        raise AssertionError("refused digest entered storage")

    monkeypatch.setattr(owner.host.outbox, "_writer", forbidden)
    assert await owner.reservations.reserve(e.link, category="essential") is None
    await owner.stop()


@pytest.mark.asyncio
async def test_runtime_plans_a_pin_only_after_actual_status_ack(work_delivery):
    from _telegram_bot_test_support import ManualClock
    from test_telegram_work_status import status_attempt

    e = work_delivery
    clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = clock.now
    e.channel._now = clock.now
    owner = await runtime(e)
    item, _work, _execution, _scope, _run, _worker = await status_attempt(e)
    clock.advance(2)
    await owner.before_send()
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("status-pin:%"))))
    message(e.channel.bot.request, identity=9011)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, item.id)).state == "sent", e.dispatch_errors
    await owner.before_send()
    e.channel.bot.request.enqueue("pinChatMessage")
    clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "pinChatMessage"]
    assert len(calls) == 1 and calls[0].parameters["message_id"] == 9011
    assert calls[0].parameters["disable_notification"] is True
    await owner.stop()


@pytest.mark.asyncio
async def test_runtime_recovers_rejected_token_notice_through_healthy_bot(work_delivery):
    from _telegram_repository_test_support import add_link, bind_link

    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

    e = work_delivery
    owner = await runtime(e)
    rejected = await bind_link(e.db, await add_link(e.db, owner=e.link.owner_user_id, bot=9_000_000_022))
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramBotLinkRow, rejected.id)).health = "token_rejected"
    await owner.tick_maintenance()
    await owner.tick_maintenance()
    async with e.db.sessions() as reader:
        notices = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("rejected:%"))))
        assert len(notices) == 1 and notices[0].link_id == e.link.id
        identity = notices[0].id
    message(e.channel.bot.request, identity=9021)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent", e.dispatch_errors
    await owner.stop()


@pytest.mark.asyncio
async def test_runtime_keeps_navigation_preview_refresh_owner(work_delivery):
    from test_telegram_digest_items import pending

    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    _items, identity, _anchor, _callback, _target = await pending(e)
    async with e.db.sessions() as reader:
        saved_states = {row.id: (row.state, row.skip_reason) for row in await reader.scalars(select(TelegramOutboxRow))}
    owner = await runtime(e)
    async with e.db.sessions.begin() as writer:
        for saved_id, (state, reason) in saved_states.items():
            saved = await writer.get(TelegramOutboxRow, saved_id)
            saved.state, saved.skip_reason = state, reason
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert (row.state, row.payload["presentation_version"]) == ("pending", 1), (row.state, row.skip_reason, type(e.channel._services.work_request_sources.recovery).__name__, e.dispatch_errors)
    await owner.stop()


@pytest.mark.asyncio
async def test_closed_before_materialization_does_not_remain_pending_forever(work_delivery):
    from app.gateway.auth.models import User
    from deerflow.agent_instances.contract import PrincipalRef
    from deerflow.agent_instances.human_input_contract import Respond
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    owner = await runtime(e)
    owner.baselines.install_binding_hook()
    await owner.baselines.initialize()
    work = await e.human.work.delegate(actor=e.actor, instance_id=e.link.agent_ref, request=DelegateWork(operation_id=uuid4().hex, objective="Obsolete notification", success_criteria="Verified"))
    colleague = await e.live.users.create_user(User(email="review-obsolete-peer@example.com"))
    async with e.db.sessions.begin() as writer:
        writer.add(AgentInstanceGrantRow(instance_id=e.link.agent_ref, user_id=str(colleague.id), permissions=7))
    request = await e.human.create(
        actor=PrincipalRef("human", str(colleague.id)),
        instance_id=e.link.agent_ref,
        work_id=work["id"],
        request=CreateRequest(
            operation_id=uuid4().hex, expected_work_revision=1, expected_assignment_revision=1, purpose="information", question="Which reference?", reason="Need source", expected_response="A link", recipient_id=e.actor.subject_id
        ),
    )
    await owner.events.tick()
    async with e.db.sessions() as reader:
        original = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.request_id == request["id"]))
        identity = original.id
        assert original.state == "pending" and not original.parts
    current = await e.human.get(actor=e.actor, request_id=request["id"])
    await e.human.respond(
        actor=e.actor, request_id=request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=current["request_revision"], expected_assignment_revision=current["assignment_revision"], text="Use the guide.")
    )
    await owner.events.tick()
    for _ in range(3):
        await owner.before_send()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "skipped", (row.state, row.skip_reason, row.parts)
        edits = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.request_id == request["id"], TelegramOutboxRow.dedupe_key.startswith("edit:"))))
        assert len(edits) == 1
        assert edits[0].state == "skipped", (edits[0].state, edits[0].skip_reason, edits[0].parts)
    await owner.stop()


@pytest.mark.asyncio
async def test_runtime_recovers_captured_note_with_original_operation(work_delivery):
    from test_telegram_work_input_capture import accept, capture, prompt

    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = work_delivery
    request, prompt_id, _seen = await prompt(e)
    inbox, mark = await accept(e)
    saved = await capture(e, inbox, mark, prompt_id)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, saved.operation_ids[0]) is None
    owner = await runtime(e, preserve=True)
    await owner.recover_inputs(lambda: True)
    await owner.recover_inputs(lambda: True)
    async with e.db.sessions() as reader:
        actions = list(await reader.scalars(select(TelegramActRow)))
        assert len(actions) == 1 and actions[0].id == saved.operation_ids[0]
        assert actions[0].state == "done", (actions[0].state, actions[0].reason)
    assert (await e.human.get(actor=e.actor, request_id=request["id"]))["closed_reason"] == "superseded"
    await owner.stop()


@pytest.mark.asyncio
async def test_runtime_saved_input_phase_fences_first_action_attempt(work_delivery, monkeypatch):
    from test_telegram_work_input_capture import accept, capture, prompt

    from deerflow.persistence.telegram_bots.model import TelegramActRow

    e = work_delivery
    request, prompt_id, _seen = await prompt(e)
    inbox, mark = await accept(e)
    saved = await capture(e, inbox, mark, prompt_id)
    owner = await runtime(e, preserve=True)
    active = [True]
    original = owner.host.work_text_inputs.admissions.stage

    async def stage(*args, **kwargs):
        result = await original(*args, **kwargs)
        active[0] = False
        return result

    monkeypatch.setattr(owner.host.work_text_inputs.admissions, "stage", stage)
    await owner.recover_inputs(lambda: active[0])
    async with e.db.sessions() as reader:
        action = await reader.get(TelegramActRow, saved.operation_ids[0])
        assert action is not None and action.attempts == 0
    assert (await e.human.get(actor=e.actor, request_id=request["id"]))["state"] == "pending"
    await owner.stop()


@pytest.mark.asyncio
async def test_runtime_delivery_retry_rebuilds_missing_digest_view(work_delivery):
    from test_telegram_digest_view_repair import missing

    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    _views, _root, _members, identity, _guard = await missing(e)
    owner = await runtime(e, preserve=True)
    await owner.before_send()
    message(e.channel.bot.request, identity=9031)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        selected = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "delivery_retry", TelegramCallbackRow.target_id == identity))
        assert selected is not None and selected.message_id == 9031
        callback_id = selected.id
    selected = await owner.host.callbacks.get(callback_id)
    await owner.host.commands.execute(e.channel, selected, query_id="repair-view")
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "pending" and row.parts[-1]["kind"] == "text"
        assert row.payload["presentation_version"] == 1
    message(e.channel.bot.request, identity=9032)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "sent" and row.message_id == 9032, e.dispatch_errors
    await owner.stop()


@pytest.mark.asyncio
async def test_saved_closed_album_uses_runtime_guard(work_delivery, monkeypatch, tmp_path):
    from test_telegram_work_album_answers import collected_album
    from test_telegram_work_album_recovery import cancel_question

    from app.telegram_bots.notification_runtime import _RuntimeGuard

    e = work_delivery
    async with collected_album(e, monkeypatch, tmp_path) as (request, root, _reads):
        await cancel_question(e, request)
        owner = await runtime(e, preserve=True)
        guard = _RuntimeGuard(owner, owner.host.commands._guard(e.channel, e.link))
        assert await owner.host.work_text_inputs.resume(e.channel, root, recovery_guard=guard)
        await owner.stop()


@pytest.mark.asyncio
async def test_saved_input_recovery_rotates_past_slow_prefix(work_delivery, monkeypatch):
    from test_telegram_work_input_capture import accept

    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = work_delivery
    for identity in (910, 911, 912):
        _inbox, mark = await accept(e, message_id=identity)
        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramInboundMarkRow, (mark.link_id, mark.message_id, mark.chat_id))
            row.route_kind = "work_input"
    owner = await runtime(e, preserve=True)
    phase, seen = [True], []

    async def resume(_channel, origin, **_kwargs):
        seen.append(origin.message_id)
        phase[0] = False
        raise RuntimeError("Temporarily unavailable")

    monkeypatch.setattr(owner.host.work_text_inputs, "resume", resume)
    for _ in range(3):
        phase[0] = True
        await owner.recover_inputs(lambda: phase[0])
    assert seen == [910, 911, 912]
    await owner.stop()


@pytest.mark.asyncio
async def test_retry_gesture_is_still_owned_after_final_provider(work_delivery, monkeypatch):
    from test_telegram_digest_view_repair import missing

    from app.gateway import authz
    from app.telegram_bots import work_digest_view_recovery as module
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    _views, _root, _members, identity, _guard = await missing(e)
    owner = await runtime(e, preserve=True)
    await owner.before_send()
    message(e.channel.bot.request, identity=9031)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        selected = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "delivery_retry", TelegramCallbackRow.target_id == identity))
        assert selected is not None and selected.message_id == 9031
        callback_id = selected.id
    selected = await owner.host.callbacks.get(callback_id)
    original_prepare, original_provider = module.prepare_attention, authz.resolve_route_permissions
    active = []
    changed = []

    async def prepare(*args, **kwargs):
        admission = await original_prepare(*args, **kwargs)

        async def check(**options):
            projection = options.get("projection")
            item = getattr(projection, "item", None)
            selected_phase = isinstance(projection, module._RepairProjection) and item is not None and item.payload["presentation_version"] == 1
            if selected_phase:
                active.append(options["session"])
            try:
                return await admission(**options)
            finally:
                if selected_phase:
                    active.pop()

        return check

    async def provider(*args, **kwargs):
        held = await original_provider(*args, **kwargs)
        if active and not changed:
            writer = active[-1]
            control = await writer.get(TelegramCallbackRow, callback_id)
            control.consumed_at, control.closed_reason = e.delivery_clock.now(), "superseded"
            await writer.flush()
            changed.append(True)
        return held

    monkeypatch.setattr(module, "prepare_attention", prepare)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    caught = []
    try:
        await owner.host.commands.execute(e.channel, selected, query_id="late-retry-change")
    except Exception as error:
        caught.append(error)
    assert changed
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.payload["presentation_version"] == 0, (row.state, row.payload["presentation_version"], caught)
    await owner.stop()


@pytest.mark.asyncio
async def test_outcome_retry_old_button_finishes_after_replacement_ack(work_delivery):
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup, outcome

    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    _plans, act_id, identity = await outcome(e)
    await clear_outcome_cleanup(e)
    e.channel.bot.request.enqueue("sendMessage", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: message thread not found"})
    e.clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert (row.state, row.failed_reason) == ("failed", "work-outcome-refresh-required")
    owner = await runtime(e, preserve=True)
    await owner.before_send()
    message(e.channel.bot.request, identity=9031)
    e.clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "delivery_retry", TelegramCallbackRow.target_id == identity))
        assert control is not None and control.message_id == 9031
        callback_id = control.id
    callback = await owner.host.callbacks.get(callback_id)
    await owner.host.commands.execute(e.channel, callback, query_id="outcome-repair")
    async with e.db.sessions() as reader:
        pending = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == act_id, TelegramOutboxRow.state == "pending")))
        assert len(pending) == 1
        fresh_id = pending[0].id
        assert fresh_id != identity
        from datetime import timedelta

        from app.telegram_bots.retention import _pinned_deliveries

        # Even after the old warning expires, an unsettled successor retains
        # its original failure evidence and eventual warning cleanup target.
        assert identity in await _pinned_deliveries(reader, e.clock.now + timedelta(days=181), 180)
    message(e.channel.bot.request, identity=9032)
    e.clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, fresh_id)).state == "sent", e.dispatch_errors
        original = await reader.get(TelegramCallbackRow, callback_id)
        assert original.consumed_at is not None, "ACKed replacement left original Retry permanently usable"
    await owner.stop()


@pytest.mark.asyncio
async def test_retry_requires_the_acknowledged_notices_actual_group(work_delivery):
    from copy import deepcopy

    from test_telegram_digest_view_repair import missing

    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    _views, _root, _members, identity, _guard = await missing(e)
    owner = await runtime(e, preserve=True)
    await owner.before_send()
    message(e.channel.bot.request, identity=9031)
    e.delivery_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions.begin() as writer:
        original = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "delivery_retry", TelegramCallbackRow.target_id == identity))
        assert original is not None and original.message_id == 9031
        values = {column.key: deepcopy(getattr(original, column.key)) for column in original.__mapper__.column_attrs}
        values.update(id=uuid4().hex, group_id=uuid4().hex)
        impostor = TelegramCallbackRow(**values)
        writer.add(impostor)
        callback_id = impostor.id
    callback = await owner.host.callbacks.get(callback_id)
    errors = []
    try:
        await owner.host.commands.execute(e.channel, callback, query_id="parallel-repair-group")
    except Exception as error:
        errors.append(error)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.payload["presentation_version"] == 0, (row.state, row.payload["presentation_version"], errors)
    await owner.stop()


@pytest.mark.asyncio
async def test_file_outcome_retry_preserves_evidence_and_closes_warning(work_delivery, monkeypatch, tmp_path):
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup

    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    from test_telegram_work_file_inputs import file_reply

    from deerflow.persistence.telegram_bots.model import TelegramActRow

    await file_reply(e, monkeypatch, tmp_path, caption="The requested statement.")
    async with e.db.sessions() as reader:
        act = await reader.scalar(select(TelegramActRow).where(TelegramActRow.kind == "respond"))
        assert act is not None and act.state == "done"
        act_id = act.id
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == act_id))
        assert row is not None
        identity = row.id
    e.outcome_identity = identity
    await clear_outcome_cleanup(e)
    e.channel.bot.request.enqueue("sendMessage", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: message thread not found"})
    e.clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert (row.state, row.failed_reason) == ("failed", "work-outcome-refresh-required")
    owner = await runtime(e, preserve=True)
    await owner.before_send()
    message(e.channel.bot.request, identity=9031)
    e.clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        control = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "delivery_retry", TelegramCallbackRow.target_id == identity))
        assert control is not None and control.message_id == 9031
        callback_id = control.id
    callback = await owner.host.callbacks.get(callback_id)
    await owner.host.commands.execute(e.channel, callback, query_id="outcome-repair")
    async with e.db.sessions() as reader:
        pending = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_outcome_basis"]["context"]["act"]["id"].as_string() == act_id, TelegramOutboxRow.state == "pending")))
        assert len(pending) == 1
        fresh_id = pending[0].id
        assert fresh_id != identity
        from datetime import timedelta

        from app.telegram_bots.retention import _pinned_deliveries

        # Even after the old warning expires, an unsettled successor retains
        # its original failure evidence and eventual warning cleanup target.
        assert identity in await _pinned_deliveries(reader, e.clock.now + timedelta(days=181), 180)
    message(e.channel.bot.request, identity=9032)
    e.clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, fresh_id)).state == "sent", e.dispatch_errors
        original = await reader.get(TelegramCallbackRow, callback_id)
        assert original is not None and original.consumed_at is not None
    await owner.stop()
