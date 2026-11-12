"""Canonical web changes edit only their acknowledged question anchor."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_work_event_delivery import queued
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.work_event_delivery import WorkEventDelivery
from app.telegram_bots.work_event_intents import WorkEventIntents
from app.telegram_bots.work_event_reader import WorkEventReader
from app.telegram_bots.work_request_source import WorkRequestSources
from deerflow.agent_instances.human_input_contract import RequestCommand, Respond
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow


async def changed_question(e, action, *, notify=None, protected=False):
    from _telegram_bot_test_support import ManualClock

    e.question_clock = ManualClock(e.sender.outbox.clock())
    e.sender.outbox.clock = e.question_clock.now
    e.channel._now = e.question_clock.now
    if protected:
        from datetime import UTC, datetime

        from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

        async with e.db.sessions.begin() as writer:
            writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="protected", updated_by=e.actor.subject_id, updated_at=datetime.now(UTC)))
    identity, request = await queued(e)
    assert await WorkEventDelivery(e.plans).materialize(e.link, identity)
    e.channel._services.work_request_sources = WorkRequestSources(reader=e.reader, outbox=e.plans.outbox, callbacks=e.plans.callbacks)
    message(e.channel.bot.request, identity=3001)
    e.question_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    e.question_clock.advance(2)
    if notify is not None:
        from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).notify = notify
    if action in {"answer", "answer_by"}:
        actor = e.actor
        if action == "answer_by":
            from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
            from deerflow.spaces.contract import PrincipalRef

            async with e.db.sessions() as reader:
                actor = PrincipalRef("human", (await reader.get(HumanInputRequestRow, request["id"])).creator_id)
        await e.human.respond(actor=actor, request_id=request["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text="Use the maintained guide."))
    else:
        await e.human.command(
            actor=e.actor,
            request_id=request["id"],
            request=RequestCommand(
                operation_id=uuid4().hex,
                action="route" if action == "unroute" else action,
                expected_revision=1,
                expected_request_revision=1,
                **({"recipient_id": None} if action == "unroute" else {"recipient_id": e.actor.subject_id} if action == "route" else {"note": "No longer needed."}),
            ),
        )
    projection = WorkEventIntents(links=e.db.links, human=e.human)
    await WorkEventReader(links=e.db.links, project=projection.project).tick()
    async with e.db.sessions() as reader:
        edit = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.request_id == request["id"], TelegramOutboxRow.dedupe_key.startswith("edit:")))
        assert edit is not None
        return identity, edit.id


@pytest.mark.asyncio
async def test_question_cleanup_is_queued_when_notifications_are_off(work_delivery):
    from app.telegram_bots.notifications import preference_skip
    from app.telegram_bots.producer import _item

    e = work_delivery
    _parent, identity = await changed_question(e, "answer", notify="none")
    settings = await e.plans.settings.get(e.link.owner_user_id)
    async with e.db.sessions() as reader:
        item = _item(await reader.get(TelegramOutboxRow, identity))
    assert preference_skip(item, settings, e.sender.outbox.clock()) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("action,text", [("answer", "Answered by you on the web"), ("answer_by", "Answered by event-peer@example.com"), ("route", "Passed on to"), ("withdraw", "This question was withdrawn.")])
async def test_web_question_change_edits_original_acknowledged_message(work_delivery, action, text):
    from app.telegram_bots.work_question_edits import WorkQuestionEdits

    e = work_delivery
    parent_id, identity = await changed_question(e, action)
    owner = WorkQuestionEdits(e.plans)
    e.channel._services.work_question_edit_sources = owner
    assert await owner.materialize(e.link, identity)
    message(e.channel.bot.request, "editMessageText", identity=3001)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    async with e.db.sessions() as reader:
        diagnostic = (await reader.get(TelegramOutboxRow, identity)).to_dict()
        diagnostic = {key: diagnostic[key] for key in ("state", "skip_reason", "failed_reason", "attempts", "not_before")}
    assert len(calls) == 1 and calls[0].parameters["message_id"] == 3001, diagnostic
    assert "Which source?" in calls[0].parameters["text"] and text in calls[0].parameters["text"]
    assert not calls[0].parameters.get("reply_markup", {}).get("inline_keyboard")
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"
        parent = await reader.get(TelegramOutboxRow, parent_id)
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"])))
        assert controls and all(row.consumed_at is not None for row in controls)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["missing", "wrong_ack"])
async def test_question_update_never_replaces_a_missing_or_wrong_edit_anchor(work_delivery, mode):
    from app.telegram_bots.work_question_edits import WorkQuestionEdits

    e = work_delivery
    _parent, identity = await changed_question(e, "withdraw")
    owner = WorkQuestionEdits(e.plans)
    e.channel._services.work_question_edit_sources = owner
    assert await owner.materialize(e.link, identity)
    if mode == "missing":
        e.channel.bot.request.enqueue("editMessageText", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: message to edit not found"})
    else:
        message(e.channel.bot.request, "editMessageText", identity=9999)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == ("skipped" if mode == "missing" else "failed"), (row.state, row.skip_reason, row.failed_reason)
        if mode == "missing":
            assert row.skip_reason == "superseded"
    assert len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]) == 1


@pytest.mark.asyncio
async def test_unopened_protected_question_closes_without_disclosing_its_text(work_delivery):
    from app.telegram_bots.work_question_edits import WorkQuestionEdits

    e = work_delivery
    _parent, identity = await changed_question(e, "withdraw", protected=True)
    owner = WorkQuestionEdits(e.plans)
    e.channel._services.work_question_edit_sources = owner
    assert await owner.materialize(e.link, identity)
    message(e.channel.bot.request, "editMessageText", identity=3001)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    calls = [call for call in e.channel.bot.request.calls if call.endpoint in {"sendMessage", "editMessageText"}]
    assert calls[0].parameters["protect_content"] is True
    assert all("Which source?" not in call.parameters["text"] for call in calls)
    assert calls[-1].parameters["text"] == "Nothing is waiting on this any more."


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["actor", "command", "receipt", "response", "request", "intent"])
async def test_question_update_rejects_initial_source_corruption(work_delivery, change):
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_question_edits import WorkQuestionEdits
    from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputRequestRow, HumanInputResponseRow
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    _parent, identity = await changed_question(e, "answer")
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        event = await writer.get(AgentWorkEventRow, row.event_id)
        receipt = await writer.scalar(select(HumanInputEventRow).where(HumanInputEventRow.operation_id == event.operation_id))
        if change == "actor":
            event.actor_id = "another-person"
        elif change == "command":
            event.request = {**event.request, "operation_id": "b" * 32}
        elif change == "receipt":
            receipt.receipt = {**receipt.receipt, "request_revision": 2}
        elif change == "response":
            (await writer.get(HumanInputResponseRow, receipt.receipt["response_id"])).text = "Changed canonical answer"
        elif change == "request":
            request = await writer.get(HumanInputRequestRow, row.request_id)
            request.purpose, request.state, request.closed_reason = "decision", "closed", "resolved"
        else:
            row.payload = {**row.payload, "work_event_intent": {**row.payload["work_event_intent"], "edit_request_revision": 2}}
    with pytest.raises(ProductSourceRetired):
        await WorkQuestionEdits(e.plans).materialize(e.link, identity)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["event", "claim", "answer_time"])
async def test_question_update_final_snapshot_rejects_change_after_source_read(work_delivery, monkeypatch, change):
    from app.telegram_bots.work_question_edits import WorkQuestionEdits
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    _parent, identity = await changed_question(e, "answer" if change == "answer_time" else "withdraw")
    owner = WorkQuestionEdits(e.plans)
    e.channel._services.work_question_edit_sources = owner
    assert await owner.materialize(e.link, identity)
    original, calls = owner.settings.get, []

    async def changed(*args, **kwargs):
        result = await original(*args, **kwargs)
        calls.append(True)
        if len(calls) == 2:
            async with e.db.sessions.begin() as writer:
                row = await writer.get(TelegramOutboxRow, identity)
                if change == "event":
                    (await writer.get(AgentWorkEventRow, row.event_id)).actor_id = "replacement-actor"
                elif change == "answer_time":
                    from datetime import timedelta

                    from deerflow.persistence.agent_instances.human_input import HumanInputResponseRow

                    response = await writer.scalar(select(HumanInputResponseRow).where(HumanInputResponseRow.request_id == row.request_id))
                    response.created_at -= timedelta(days=1)
                else:
                    row.claim_id = "foreign-claim"
        return result

    monkeypatch.setattr(owner.settings, "get", changed)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert len(calls) >= 2
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    if change == "answer_time":
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramOutboxRow, identity)).state == "skipped"


@pytest.mark.asyncio
async def test_question_update_refreshes_locale_without_replaying_the_web_action(work_delivery):
    from app.telegram_bots.work_question_edits import WorkQuestionEdits
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    _parent, identity = await changed_question(e, "withdraw")
    owner = WorkQuestionEdits(e.plans)
    e.channel._services.work_question_edit_sources = owner
    assert await owner.materialize(e.link, identity)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
    assert await e.sender.tick() == 1
    await e.sender.drain()
    message(e.channel.bot.request, "editMessageText", identity=3001)
    e.question_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    assert len(calls) == 1 and "此问题已撤回。" in calls[0].parameters["text"]


@pytest.mark.asyncio
async def test_question_update_protection_upgrade_never_edits_new_text_into_old_message(work_delivery):
    from datetime import UTC, datetime

    from app.telegram_bots.work_question_edits import WorkQuestionEdits
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    e = work_delivery
    _parent, identity = await changed_question(e, "withdraw")
    owner = WorkQuestionEdits(e.plans)
    e.channel._services.work_question_edit_sources = owner
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="protected", updated_by=e.actor.subject_id, updated_at=datetime.now(UTC)))
    assert await owner.materialize(e.link, identity)
    message(e.channel.bot.request, "editMessageReplyMarkup", identity=3001)
    message(e.channel.bot.request, identity=3002)
    for _ in range(2):
        e.question_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 2 and calls[-1].parameters["protect_content"] is True
    assert "Which source?" not in calls[-1].parameters["text"]


@pytest.mark.asyncio
async def test_later_terminal_update_follows_acknowledged_protected_replacement(work_delivery):
    from datetime import UTC, datetime

    from app.telegram_bots.work_question_edits import WorkQuestionEdits
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow, HumanInputResponseRow
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    e = work_delivery
    parent_id, identity = await changed_question(e, "answer")
    owner = WorkQuestionEdits(e.plans)
    e.channel._services.work_question_edit_sources = owner
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="protected", updated_by=e.actor.subject_id, updated_at=datetime.now(UTC)))
    assert await owner.materialize(e.link, identity)
    message(e.channel.bot.request, "editMessageReplyMarkup", identity=3001)
    message(e.channel.bot.request, identity=3002)
    for _ in range(2):
        e.question_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, parent_id)
        request = await reader.get(HumanInputRequestRow, parent.request_id)
        responses = list(await reader.scalars(select(HumanInputResponseRow.id).where(HumanInputResponseRow.request_id == request.id)))
        request_id, revision, request_revision = request.id, request.revision, request.request_revision
    await e.human.command(
        actor=e.actor, request_id=request_id, request=RequestCommand(operation_id=uuid4().hex, action="withdraw", expected_revision=revision, expected_request_revision=request_revision, note="No longer needed.", response_ids=responses)
    )
    projection = WorkEventIntents(links=e.db.links, human=e.human)
    await WorkEventReader(links=e.db.links, project=projection.project).tick()
    async with e.db.sessions() as reader:
        update = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.request_id == request_id, TelegramOutboxRow.payload["work_event_intent"]["key"].as_string() == "request.withdrawn"))
        next_id = update.id
    assert await owner.materialize(e.link, next_id)
    message(e.channel.bot.request, "editMessageText", identity=3002)
    e.question_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    assert len(calls) == 1 and calls[0].parameters["message_id"] == 3002


@pytest.mark.asyncio
async def test_question_update_retries_a_late_language_change(work_delivery, monkeypatch):
    from app.telegram_bots.work_question_edits import WorkQuestionEdits
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = work_delivery
    _parent, identity = await changed_question(e, "withdraw")
    owner = WorkQuestionEdits(e.plans)
    e.channel._services.work_question_edit_sources = owner
    assert await owner.materialize(e.link, identity)
    original, calls = owner.settings.get, []

    async def changed(*args, **kwargs):
        calls.append(True)
        if len(calls) == 2:
            async with e.db.sessions.begin() as writer:
                (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
        return await original(*args, **kwargs)

    monkeypatch.setattr(owner.settings, "get", changed)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "pending"


@pytest.mark.asyncio
@pytest.mark.parametrize("materialized", [False, True])
async def test_unfinished_question_update_retains_its_original_anchor(work_delivery, materialized):
    from datetime import timedelta

    from app.telegram_bots.retention import _sweep_outbox
    from app.telegram_bots.work_question_edits import WorkQuestionEdits

    e = work_delivery
    parent, identity = await changed_question(e, "withdraw")
    if materialized:
        assert await WorkQuestionEdits(e.plans).materialize(e.link, identity)
    future = e.question_clock.now() + timedelta(days=31)
    await e.plans.callbacks.sweep(now=future)
    await _sweep_outbox(e.db.sessions, future, 30)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramOutboxRow, parent) is not None
        saved = await reader.get(TelegramOutboxRow, parent)
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == saved.parts[-1]["callback_group_id"])))
        assert controls


@pytest.mark.asyncio
async def test_question_update_rechecks_access_to_original_references(work_delivery, monkeypatch):
    from types import SimpleNamespace

    from app.telegram_bots.work_question_edits import WorkQuestionEdits
    from deerflow.agent_instances.work_contract import WorkSource
    from deerflow.persistence.spaces.model import SpaceGrantRow
    from deerflow.spaces.registry import SpaceRegistry

    e = work_delivery
    e.human.agents.files.registry = SimpleNamespace(_view=SpaceRegistry._view)
    async with e.db.sessions.begin() as writer:
        writer.add(SpaceGrantRow(space_id=e.instance.home_id, principal_kind="human", subject_id=e.link.owner_user_id, permissions=1))
    create = e.human.create

    async def referenced(**kwargs):
        from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

        async with e.db.sessions.begin() as writer:
            for person in await writer.scalars(select(AgentInstanceGrantRow.user_id).where(AgentInstanceGrantRow.instance_id == e.instance.id)):
                if await writer.get(SpaceGrantRow, (e.instance.home_id, "human", person)) is None:
                    writer.add(SpaceGrantRow(space_id=e.instance.home_id, principal_kind="human", subject_id=person, permissions=1))
        kwargs["request"] = kwargs["request"].model_copy(update={"sources": [WorkSource(space_id=e.instance.home_id, path="private-guide.txt")]})
        return await create(**kwargs)

    monkeypatch.setattr(e.human, "create", referenced)
    _parent, identity = await changed_question(e, "withdraw")
    owner = WorkQuestionEdits(e.plans)
    e.channel._services.work_question_edit_sources = owner
    assert await owner.materialize(e.link, identity)
    async with e.db.sessions.begin() as writer:
        await writer.delete(await writer.get(SpaceGrantRow, (e.instance.home_id, "human", e.link.owner_user_id)))
    message(e.channel.bot.request, "editMessageText", identity=3001)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["reply_to_message_id", "plain_fallback", "presentation_version"])
async def test_question_update_rejects_unsealed_transport_fields(work_delivery, change):
    from copy import deepcopy

    from app.telegram_bots.work_question_edits import WorkQuestionEdits

    e = work_delivery
    _parent, identity = await changed_question(e, "withdraw")
    owner = WorkQuestionEdits(e.plans)
    e.channel._services.work_question_edit_sources = owner
    assert await owner.materialize(e.link, identity)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        parts = deepcopy(row.parts)
        parts[0][change] = {"reply_to_message_id": 8888, "plain_fallback": "yes", "presentation_version": False}[change]
        row.parts = parts
        from sqlalchemy.orm.attributes import flag_modified

        flag_modified(row, "parts")
    message(e.channel.bot.request, "editMessageText", identity=3001)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]


async def actual_assessment(e):
    from dataclasses import replace

    from test_work_execution import activation, run_record

    from app.telegram_bots.work_event_intents import WorkEventIntents
    from app.telegram_bots.work_event_reader import WorkEventReader
    from deerflow.agent_instances.work_contract import RequestBasis
    from deerflow.agent_instances.work_execution import WorkExecution
    from deerflow.agent_instances.work_execution_contract import ReportWork
    from deerflow.persistence.agent_instances.human_input import HumanInputResponseRow
    from deerflow.persistence.spaces.model import SpaceGrantRow
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow
    from deerflow.spaces.principals import HostPrincipalResolver
    from deerflow.spaces.registry import SpaceRegistry

    parent, _answer_update = await changed_question(e, "answer")
    async with e.db.sessions() as reader:
        old = await reader.get(TelegramOutboxRow, parent)
        work_id, request_id = old.work_id, old.request_id
    directory = e.human.agents.directory
    e.human.agents.files.registry = SpaceRegistry(e.db.sessions, HostPrincipalResolver(human=directory.human, nonhuman=directory.lookup, nonhuman_in_session=directory.lookup_in_session))
    async with e.db.engine.begin() as connection:
        await connection.run_sync(lambda sync: SpaceGrantRow.__table__.create(sync, checkfirst=True))
    async with e.db.sessions.begin() as writer:
        writer.add(SpaceGrantRow(space_id=e.instance.home_id, principal_kind="nonhuman", subject_id=e.instance.principal.subject_id, permissions=3))
    conversations = e.http_app.state.agent_conversations
    chat = await conversations.create(actor=e.actor, instance_id=e.instance.id, creation_id=uuid4().hex)
    execution = await conversations.execution(actor=e.actor, thread_id=chat["thread_id"])
    work = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=work_id)
    scope = await WorkExecution(e.human.work).reserve(execution=execution, work_id=work_id, request=activation(execution, work))
    execution = replace(execution, work=scope)
    run = run_record(execution, scope)
    run.user_id = e.actor.subject_id
    await scope.bind(execution, run, execution.thread_incarnation)
    work = (await scope.context(execution))["work"]
    request = await e.human.get(actor=e.actor, request_id=request_id)
    async with e.db.sessions() as reader:
        response_ids = list(await reader.scalars(select(HumanInputResponseRow.id).where(HumanInputResponseRow.request_id == request_id)))
    command = ReportWork(
        operation_id=uuid4().hex,
        expected_revision=work["revision"],
        action="assess_input",
        statement="Applied the maintained guide.",
        request_basis=RequestBasis(id=request_id, revision=request["revision"], request_revision=request["request_revision"], response_ids=response_ids),
    )
    await scope.report(execution, command)
    await WorkEventReader(links=e.db.links, project=WorkEventIntents(links=e.db.links, human=e.human).project).tick()
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.request_id == request_id, TelegramOutboxRow.payload["work_event_intent"]["key"].as_string() == "request.assessed"))
        assert row is not None
        return row.id


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["basis_revision", "response_ids", "missing_assess", "purpose"])
async def test_review_assessment_uses_exact_canonical_hi_effect(work_delivery, damage):
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_question_edits import WorkQuestionEdits
    from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputRequestRow
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = work_delivery
    identity = await actual_assessment(e)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        event = await writer.get(AgentWorkEventRow, row.event_id)
        if damage in {"basis_revision", "response_ids"}:
            basis = dict(event.request["request_basis"])
            basis["revision" if damage == "basis_revision" else "response_ids"] = 1 if damage == "basis_revision" else []
            event.request = {**event.request, "request_basis": basis}
        elif damage == "missing_assess":
            effect = await writer.scalar(select(HumanInputEventRow).where(HumanInputEventRow.request_id == row.request_id, HumanInputEventRow.action == "assess"))
            assert effect is not None
            await writer.delete(effect)
        else:
            (await writer.get(HumanInputRequestRow, row.request_id)).purpose = "review"
    with pytest.raises(ProductSourceRetired):
        await WorkQuestionEdits(e.plans).materialize(e.link, identity)


@pytest.mark.asyncio
async def test_actual_assessment_edits_the_acknowledged_question(work_delivery):
    from app.telegram_bots.work_question_edits import WorkQuestionEdits

    e = work_delivery
    identity = await actual_assessment(e)
    owner = WorkQuestionEdits(e.plans)
    e.channel._services.work_question_edit_sources = owner
    assert await owner.materialize(e.link, identity)
    message(e.channel.bot.request, "editMessageText", identity=3001)
    e.question_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    assert len(calls) == 1 and "has read the answers and closed the question." in calls[0].parameters["text"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["timestamp", "multiple_ids", "pending_receipt"])
async def test_question_update_acknowledgement_requires_complete_typed_receipts(work_delivery, damage):
    from copy import deepcopy

    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_question_edits import WorkQuestionEdits, acknowledge_question_update

    e = work_delivery
    _parent, identity = await changed_question(e, "withdraw")
    owner = WorkQuestionEdits(e.plans)
    e.channel._services.work_question_edit_sources = owner
    assert await owner.materialize(e.link, identity)
    message(e.channel.bot.request, "editMessageText", identity=3001)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramOutboxRow, identity)
        parts = deepcopy(row.parts)
        if damage == "timestamp":
            parts[0]["sent_at"] = "yesterday"
        elif damage == "multiple_ids":
            parts[0]["message_ids"] = [3001, 3002]
        else:
            parts[0]["state"] = "pending"
        row.parts = parts
        with pytest.raises(ProductSourceRetired):
            await acknowledge_question_update(writer, row)


@pytest.mark.asyncio
async def test_late_protection_upgrade_repairs_only_the_retained_delivery(work_delivery):
    from datetime import UTC, datetime

    from app.telegram_bots.work_question_edits import WorkQuestionEdits
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    e = work_delivery
    _parent, identity = await changed_question(e, "withdraw")
    owner = WorkQuestionEdits(e.plans)
    e.channel._services.work_question_edit_sources = owner
    assert await owner.materialize(e.link, identity)
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="protected", updated_by=e.actor.subject_id, updated_at=datetime.now(UTC)))
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert (row.state, row.failed_reason) == ("failed", "work-question-refresh-required")
    assert await owner.repair(e.link, identity)
    message(e.channel.bot.request, "editMessageReplyMarkup", identity=3001)
    message(e.channel.bot.request, identity=3002)
    for _ in range(2):
        e.question_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent"


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["pending", "message", "anchor"])
async def test_question_cleanup_never_consumes_an_unacknowledged_or_foreign_group(work_delivery, damage):
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_question_edits import WorkQuestionEdits

    e = work_delivery
    parent_id, identity = await changed_question(e, "withdraw")
    async with e.db.sessions.begin() as writer:
        parent = await writer.get(TelegramOutboxRow, parent_id)
        controls = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"])))
        for control in controls:
            if damage == "message":
                control.message_id = 3999
            else:
                control.payload = {**control.payload, **({"presentation_pending": True} if damage == "pending" else {"anchor_outbox_id": "another-anchor"})}
    with pytest.raises(ProductSourceRetired):
        await WorkQuestionEdits(e.plans).materialize(e.link, identity)
    async with e.db.sessions() as reader:
        stored = [await reader.get(TelegramCallbackRow, control.id) for control in controls]
        assert all(control.consumed_at is None for control in stored)


@pytest.mark.asyncio
@pytest.mark.parametrize("newer_first", [False, True])
async def test_question_updates_never_overwrite_a_later_terminal_footer(work_delivery, newer_first):
    from app.telegram_bots.producer import DeliveryRetry, ProductSourceRetired
    from app.telegram_bots.work_question_edits import WorkQuestionEdits

    e = work_delivery
    latest = await actual_assessment(e)
    owner = WorkQuestionEdits(e.plans)
    e.channel._services.work_question_edit_sources = owner
    async with e.db.sessions() as reader:
        new = await reader.get(TelegramOutboxRow, latest)
        old = await reader.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.request_id == new.request_id, TelegramOutboxRow.payload["work_event_intent"]["key"].as_string() == "request.answered_elsewhere"))
    if newer_first:
        assert await owner.materialize(e.link, latest)
        with pytest.raises(ProductSourceRetired):
            await owner.materialize(e.link, old)
    else:
        assert await owner.materialize(e.link, old)
        with pytest.raises(DeliveryRetry):
            await owner.materialize(e.link, latest)
        message(e.channel.bot.request, "editMessageText", identity=3001)
        e.question_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        assert await owner.materialize(e.link, latest)
    message(e.channel.bot.request, "editMessageText", identity=3001)
    e.question_clock.advance(2)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    assert "has read the answers and closed the question." in calls[-1].parameters["text"]
    assert "Answered by you" not in calls[-1].parameters["text"]


@pytest.mark.asyncio
async def test_protected_replacement_cannot_acknowledge_the_old_unprotected_message(work_delivery):
    from datetime import UTC, datetime

    from app.telegram_bots.work_question_edits import WorkQuestionEdits
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    e = work_delivery
    parent_id, identity = await changed_question(e, "withdraw")
    owner = WorkQuestionEdits(e.plans)
    e.channel._services.work_question_edit_sources = owner
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="protected", updated_by=e.actor.subject_id, updated_at=datetime.now(UTC)))
    assert await owner.materialize(e.link, identity)
    message(e.channel.bot.request, "editMessageReplyMarkup", identity=3001)
    message(e.channel.bot.request, identity=3001)
    for _ in range(2):
        e.question_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert (row.state, row.failed_reason) == ("failed", "invalid-work-question-receipt")
        assert "question_anchor" not in (await reader.get(TelegramOutboxRow, parent_id)).payload


@pytest.mark.asyncio
@pytest.mark.parametrize("upgrade", [False, True])
async def test_full_question_keeps_every_character_when_closure_requires_another_message(work_delivery, monkeypatch, upgrade):
    from app.telegram_bots.render import render
    from app.telegram_bots.split import visible_text
    from app.telegram_bots.work_question_edits import WorkQuestionEdits

    e = work_delivery
    base = render("ask.info", "en-US", objective="Investigate the guide", name=e.instance.name, question="", reason="Need context", expected_response="Source identity")
    question = "Q" * (4090 - len(visible_text(base)))
    create = e.human.create

    async def full(**kwargs):
        kwargs["request"] = kwargs["request"].model_copy(update={"question": question})
        return await create(**kwargs)

    monkeypatch.setattr(e.human, "create", full)
    parent_id, identity = await changed_question(e, "withdraw")
    owner = WorkQuestionEdits(e.plans)
    e.channel._services.work_question_edit_sources = owner
    assert await owner.materialize(e.link, identity)
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, parent_id)
        assert len(parent.parts) == 1 and question in parent.parts[0]["html"]
        assert len((await reader.get(TelegramOutboxRow, identity)).parts) == 2
    message(e.channel.bot.request, "editMessageReplyMarkup", identity=3001)
    message(e.channel.bot.request, identity=3002)
    for index in range(2):
        e.question_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        if index == 0 and upgrade:
            from datetime import UTC, datetime

            from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

            async with e.db.sessions.begin() as writer:
                writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="protected", updated_by=e.actor.subject_id, updated_at=datetime.now(UTC)))
    if upgrade:
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramOutboxRow, identity)).state == "pending"
        message(e.channel.bot.request, "editMessageReplyMarkup", identity=3001)
        for _ in range(2):
            e.question_clock.advance(2)
            assert await e.sender.tick() == 1
            await e.sender.drain()
    calls = e.channel.bot.request.calls
    assert not [call for call in calls if call.endpoint == "editMessageText"]
    sends = [call for call in calls if call.endpoint == "sendMessage"]
    assert question in sends[0].parameters["text"]
    assert sends[-1].parameters["text"] == ("Nothing is waiting on this any more." if upgrade else "This question was withdrawn.")
    if upgrade:
        assert sends[-1].parameters["protect_content"] is True


@pytest.mark.asyncio
async def test_removing_a_recipient_does_not_claim_the_question_went_to_someone(work_delivery):
    from app.telegram_bots.work_question_edits import WorkQuestionEdits

    e = work_delivery
    _parent, identity = await changed_question(e, "unroute")
    owner = WorkQuestionEdits(e.plans)
    e.channel._services.work_question_edit_sources = owner
    assert await owner.materialize(e.link, identity)
    message(e.channel.bot.request, "editMessageText", identity=3001)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    calls = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"]
    assert len(calls) == 1 and calls[0].parameters["text"].endswith("Nothing is waiting on this any more.")
    assert "Passed on to" not in calls[0].parameters["text"]
