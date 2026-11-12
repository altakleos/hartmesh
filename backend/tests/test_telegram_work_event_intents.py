"""Recipient intents from canonical services; no chat/materializer installation."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_actions import actions as actions

from app.telegram_bots.work_event_cursors import WorkEventCursors
from app.telegram_bots.work_event_reader import WorkEventReader
from deerflow.agent_instances.human_input_contract import CreateRequest, Respond
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow
from deerflow.spaces.contract import PrincipalRef


async def setup(e):
    from app.gateway.auth.models import User
    from app.telegram_bots.work_event_intents import WorkEventIntents

    person = await e.live.users.create_user(User(email="event-peer@example.com"))
    peer = PrincipalRef("human", str(person.id))
    async with e.db.sessions.begin() as writer:
        writer.add(AgentInstanceGrantRow(instance_id=e.instance.id, user_id=peer.subject_id, permissions=7))
    await WorkEventCursors(e.db.links).initialize()
    projection = WorkEventIntents(links=e.db.links, human=e.human)
    return peer, WorkEventReader(links=e.db.links, project=projection.project)


async def request(e, peer, purpose="information"):
    work = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Investigate the guide", success_criteria="Explain findings"))
    result = await e.human.create(
        actor=peer,
        instance_id=e.instance.id,
        work_id=work["id"],
        request=CreateRequest(
            operation_id=uuid4().hex, expected_work_revision=1, expected_assignment_revision=1, purpose=purpose, question="Which source?", reason="Need context", expected_response="Source identity", recipient_id=e.actor.subject_id
        ),
    )
    return work, result


async def intents(e):
    async with e.db.sessions() as reader:
        return list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.event_id.is_not(None)).order_by(TelegramOutboxRow.created_at, TelegramOutboxRow.id)))


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose,key", [("information", "ask.info"), ("decision", "ask.decision")])
async def test_canonical_question_creates_only_recipient_metadata(actions, purpose, key):
    e = actions
    peer, worker = await setup(e)
    work, req = await request(e, peer, purpose)
    assert await worker.tick() == 1
    rows = await intents(e)
    assert len(rows) == 1
    row = rows[0]
    assert (row.owner_user_id, row.class_, row.request_id, row.work_id) == (e.actor.subject_id, "question", req["id"], work["id"])
    assert row.payload["work_event_intent"]["key"] == key
    assert row.parts == [] and row.state == "pending"
    assert "Which source?" not in str(row.payload)
    assert await worker.tick() == 0


@pytest.mark.asyncio
async def test_web_answer_uses_exact_receipt_and_edits_the_actors_question(actions):
    e = actions
    peer, worker = await setup(e)
    _work, req = await request(e, peer)
    await worker.tick()
    result = await e.human.respond(actor=e.actor, request_id=req["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text="Use the published guide."))
    assert await worker.tick() == 1
    rows = await intents(e)
    edited = [row for row in rows if row.dedupe_key.startswith("edit:")]
    reminders = [row for row in rows if row.class_ == "reminder"]
    assert len(edited) == len(reminders) == 1
    assert edited[0].payload["work_event_intent"]["key"] == "request.answered_elsewhere"
    assert reminders[0].dedupe_key == "remind:" + result["receipt"]["response_id"]
    assert reminders[0].silent is True


@pytest.mark.asyncio
async def test_management_comment_does_not_claim_the_question_was_answered(actions):
    e = actions
    peer, worker = await setup(e)
    _work, req = await request(e, peer, "decision")
    await worker.tick()
    await e.human.respond(actor=e.actor, request_id=req["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, text="Consider the old guide too."))
    assert await worker.tick() == 1
    assert [row.payload["work_event_intent"]["key"] for row in await intents(e)] == ["ask.decision"]


@pytest.mark.asyncio
@pytest.mark.parametrize("disposition", ["cannot_provide", "wrong_recipient"])
async def test_unsupplied_response_tells_managers_without_reporting_an_answer(actions, disposition):
    e = actions
    peer, worker = await setup(e)
    _work, req = await request(e, peer)
    await worker.tick()
    result = await e.human.respond(actor=peer, request_id=req["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=1, expected_assignment_revision=1, disposition=disposition, text="A colleague knows this."))
    assert await worker.tick() == 1
    rows = [row for row in await intents(e) if row.payload["work_event_intent"]["key"] == "request.unrouted"]
    assert len(rows) == 1 and rows[0].class_ == "question"
    assert rows[0].payload["work_event_intent"]["response_id"] == result["receipt"]["response_id"]
    assert not any(row.class_ == "reminder" for row in await intents(e))


@pytest.mark.asyncio
async def test_actor_and_preferences_do_not_replay_when_settings_change(actions):
    from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

    e = actions
    peer, worker = await setup(e)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.actor.subject_id)).notify = "none"
    await request(e, peer)
    assert await worker.tick() == 1
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramPersonSettingsRow, e.actor.subject_id)).notify = "all"
    assert await worker.tick() == 0 and not await intents(e)
    await request(e, e.actor)
    assert await worker.tick() == 1 and not await intents(e)


@pytest.mark.asyncio
async def test_unmaterialized_intent_cannot_be_claimed_or_acknowledged(actions):
    from test_telegram_outbox_workers import allowed

    from app.telegram_bots.producer import Outbox, OutboxSender

    e = actions
    peer, worker = await setup(e)
    await request(e, peer)
    await worker.tick()
    async with e.db.sessions.begin() as writer:
        for row in await writer.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.event_id.is_(None))):
            row.state = "sent"

    async def forbidden(**kwargs):
        raise AssertionError("an intent is not a delivery plan")

    sender = OutboxSender(Outbox(e.db.sessions), run_id="intent-test", deliver_part=forbidden, eligibility=allowed, proactive=True)
    try:
        assert await sender.tick() == 0
    finally:
        await sender.drain()
    row = (await intents(e))[0]
    assert row.state == "pending" and row.claim_id is None and row.sent_at is None


async def activate(e):
    from dataclasses import replace

    from test_work_execution import activation, run_record

    from deerflow.agent_instances.work_execution import WorkExecution
    from deerflow.persistence.spaces.model import SpaceGrantRow
    from deerflow.spaces.principals import HostPrincipalResolver
    from deerflow.spaces.registry import SpaceRegistry

    # Real SQL Home authority, without a native store, model or container.
    directory = e.human.agents.directory
    e.human.agents.files.registry = SpaceRegistry(e.db.sessions, HostPrincipalResolver(human=directory.human, nonhuman=directory.lookup, nonhuman_in_session=directory.lookup_in_session))
    async with e.db.engine.begin() as connection:
        await connection.run_sync(lambda sync: SpaceGrantRow.__table__.create(sync, checkfirst=True))
    async with e.db.sessions.begin() as writer:
        writer.add(SpaceGrantRow(space_id=e.instance.home_id, principal_kind="nonhuman", subject_id=e.instance.principal.subject_id, permissions=3))

    record = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Report findings", success_criteria="Provide evidence"))
    conversations = e.http_app.state.agent_conversations
    chat = await conversations.create(actor=e.actor, instance_id=e.instance.id, creation_id=uuid4().hex)
    execution = await conversations.execution(actor=e.actor, thread_id=chat["thread_id"])
    host = WorkExecution(e.human.work)
    scope = await host.reserve(execution=execution, work_id=record["id"], request=activation(execution, record))
    execution = replace(execution, work=scope)
    run = run_record(execution, scope)
    run.user_id = e.actor.subject_id
    await scope.bind(execution, run, execution.thread_incarnation)
    return record, execution, scope, run


@pytest.mark.asyncio
async def test_actual_activation_and_review_settlement_resolve_exact_attempt(actions):
    from deerflow.agent_instances.work_execution_contract import ReportWork

    e = actions
    _peer, worker = await setup(e)
    work, execution, scope, run = await activate(e)
    assert await worker.tick() == 1
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=work["id"])
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="outcome", statement="The findings are verified."))
    await scope.finish(execution, run, settled=True)
    assert await worker.tick() == 1
    rows = await intents(e)
    assert {row.payload["work_event_intent"]["key"] for row in rows} == {"status.starting", "status.finished", "review.ready"}
    assert all(row.payload["work_event_intent"]["attempt_id"] == scope.attempt_id for row in rows)
    assert next(row for row in rows if row.class_ == "progress" and not row.dedupe_key.startswith("edit:")).dedupe_key == "progress:" + scope.attempt_id


@pytest.mark.asyncio
async def test_unrouted_review_has_intent_even_without_a_separate_request_event(actions):
    from deerflow.agent_instances.work_execution_contract import ReportWork
    from deerflow.persistence.agent_instances.model import AgentInstanceRow

    e = actions
    _peer, worker = await setup(e)
    work, execution, scope, run = await activate(e)
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=work["id"])
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="outcome", statement="Verified findings."))
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceRow, e.instance.id)).supervisor_id = "missing-person"
    await scope.finish(execution, run, settled=True)
    assert await worker.tick() == 1
    keys = {row.payload["work_event_intent"]["key"] for row in await intents(e)}
    assert {"request.unrouted", "result.waiting_review"} <= keys


@pytest.mark.asyncio
async def test_routing_to_self_edits_old_question_and_publishes_new_question(actions):
    from deerflow.agent_instances.human_input_contract import RequestCommand

    e = actions
    peer, worker = await setup(e)
    _work, req = await request(e, peer)
    await worker.tick()
    await e.human.command(actor=e.actor, request_id=req["id"], request=RequestCommand(operation_id=uuid4().hex, action="route", expected_revision=1, expected_request_revision=1, recipient_id=e.actor.subject_id))
    assert await worker.tick() == 1
    rows = await intents(e)
    assert sorted(row.payload["work_event_intent"]["key"] for row in rows) == ["ask.info", "ask.info", "request.rerouted"]
    assert sorted(row.payload["work_event_intent"]["request_revision"] for row in rows if not row.dedupe_key.startswith("edit:")) == [1, 2]


@pytest.mark.asyncio
async def test_delayed_reader_does_not_reannounce_a_question_superseded_by_routing(actions):
    from deerflow.agent_instances.human_input_contract import RequestCommand

    e = actions
    peer, worker = await setup(e)
    _work, req = await request(e, peer)
    await e.human.command(actor=e.actor, request_id=req["id"], request=RequestCommand(operation_id=uuid4().hex, action="route", expected_revision=1, expected_request_revision=1, recipient_id=e.actor.subject_id))
    assert await worker.tick() == 1
    rows = await intents(e)
    assert len(rows) == 1 and rows[0].payload["work_event_intent"]["request_revision"] == 2


@pytest.mark.asyncio
async def test_missing_response_receipt_holds_cursor_without_guessing_from_latest_response(actions):
    from sqlalchemy import delete

    from deerflow.persistence.agent_instances.human_input import HumanInputEventRow
    from deerflow.persistence.telegram_bots.model import TelegramWorkCursorRow

    e = actions
    peer, worker = await setup(e)
    work, req = await request(e, peer)
    await worker.tick()
    operation = uuid4().hex
    await e.human.respond(actor=e.actor, request_id=req["id"], request=Respond(operation_id=operation, expected_request_revision=1, expected_assignment_revision=1, text="The source."))
    async with e.db.sessions.begin() as writer:
        await writer.execute(delete(HumanInputEventRow).where(HumanInputEventRow.operation_id == operation))
    assert await worker.tick() == 0
    assert len(await intents(e)) == 1
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramWorkCursorRow, (e.link.id, work["id"]))).revision == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cause,own,key",
    [
        ("stop", True, "status.stopped"),
        ("stop", False, "attempt.stopped"),
        ("edit", True, "status.ended"),
        ("edit", False, "attempt.stopped_changed"),
        ("cancel", True, "status.ended"),
        ("cancel", False, "attempt.stopped_withdrawn"),
        (None, False, "attempt.stopped_restart"),
    ],
)
async def test_cancelled_attempt_uses_its_intervening_command_actor(actions, cause, own, key):
    from test_agent_work_records import command

    e = actions
    peer, worker = await setup(e)
    work, execution, scope, run = await activate(e)
    if cause:
        current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=work["id"])
        extra = {"attempt_id": scope.attempt_id} if cause == "stop" else {"objective": "Changed assignment"} if cause == "edit" else {}
        await e.human.work.command(actor=e.actor if own else peer, instance_id=e.instance.id, work_id=work["id"], request=command(current, cause, **extra), can_stop=True)
    run.status = "interrupted"
    await scope.finish(execution, run, settled=True)
    assert await worker.tick() == 1
    keys = {row.payload["work_event_intent"]["key"] for row in await intents(e)}
    assert key in keys
    if own:
        assert keys == {"status.starting", key}


@pytest.mark.asyncio
async def test_later_activation_cannot_take_earlier_attempt_notifications(actions):
    from dataclasses import replace

    from test_work_execution import activation

    from deerflow.agent_instances.work_execution import WorkExecution

    e = actions
    _peer, worker = await setup(e)
    work, execution, scope, run = await activate(e)
    await scope.finish(execution, run, settled=True)
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=work["id"])
    second_execution = replace(execution, work=None)
    second = await WorkExecution(e.human.work).reserve(execution=second_execution, work_id=work["id"], request=activation(second_execution, current))
    assert await worker.tick() == 1
    rows = await intents(e)
    no_result = next(row for row in rows if row.payload["work_event_intent"]["key"] == "attempt.no_result")
    assert no_result.payload["work_event_intent"]["attempt_id"] == scope.attempt_id
    starts = [row for row in rows if row.payload["work_event_intent"]["key"] == "status.starting"]
    assert {row.payload["work_event_intent"]["attempt_id"] for row in starts} == {scope.attempt_id, second.attempt_id}


@pytest.mark.asyncio
@pytest.mark.parametrize("action,key", [("accept", "result.accepted"), ("changes_requested", "result.changes")])
async def test_review_commands_notify_original_requester_once(actions, action, key):
    from test_agent_work_records import command

    from deerflow.agent_instances.work_execution_contract import ReportWork

    e = actions
    peer, worker = await setup(e)
    work, execution, scope, run = await activate(e)
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=work["id"])
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="outcome", statement="The finding is verified."))
    await scope.finish(execution, run, settled=True)
    await worker.tick()
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=work["id"])
    extra = dict(outcome_id=current["outcome_id"], evidence_revision=current["outcome"]["evidence_revision"], basis="outcome_statement", acknowledge_unchecked_sources=True) if action == "accept" else {"note": "Explain the missing context."}
    if action == "accept":
        req = await e.human.get(actor=peer, request_id=current["human_input_request_id"])
        extra["request_basis"] = {"id": req["id"], "revision": req["revision"], "request_revision": req["request_revision"], "response_ids": []}
    await e.human.work.command(actor=peer, instance_id=e.instance.id, work_id=work["id"], request=command(current, action, **extra))
    assert await worker.tick() == 1
    rows = [row for row in await intents(e) if row.payload["work_event_intent"]["key"] == key]
    assert len(rows) == 1 and rows[0].payload["work_event_intent"]["attempt_id"] == scope.attempt_id


@pytest.mark.asyncio
async def test_later_recipient_answer_does_not_replace_old_recipients_rerouted_notice(actions):
    from deerflow.agent_instances.human_input_contract import RequestCommand

    e = actions
    peer, worker = await setup(e)
    _work, req = await request(e, peer)
    await worker.tick()
    await e.human.command(actor=peer, request_id=req["id"], request=RequestCommand(operation_id=uuid4().hex, action="route", expected_revision=1, expected_request_revision=1, recipient_id=peer.subject_id))
    await worker.tick()
    await e.human.respond(actor=peer, request_id=req["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=2, expected_assignment_revision=1, text="The correct reference."))
    assert await worker.tick() == 1
    assert [row.payload["work_event_intent"]["key"] for row in await intents(e) if row.dedupe_key.startswith("edit:")] == ["request.rerouted"]


@pytest.mark.asyncio
async def test_assessment_uses_routed_request_revision_and_blocked_end_is_not_no_result(actions):
    from dataclasses import replace

    from test_work_execution import activation, run_record

    from deerflow.agent_instances.human_input_contract import RequestCommand
    from deerflow.agent_instances.work_execution import WorkExecution
    from deerflow.agent_instances.work_execution_contract import ReportWork

    e = actions
    _peer, worker = await setup(e)
    work, execution, scope, run = await activate(e)
    current = (await scope.context(execution))["work"]
    blocked = await scope.report(
        execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="request_input", statement="Which guide?", purpose="information", reason="Need a source", expected_response="A reference")
    )
    await scope.finish(execution, run, settled=True)
    await worker.tick()
    req = await e.human.get(actor=e.actor, request_id=blocked["human_input_request_id"])
    await e.human.command(actor=e.actor, request_id=req["id"], request=RequestCommand(operation_id=uuid4().hex, action="route", expected_revision=1, expected_request_revision=1, recipient_id=e.actor.subject_id))
    await worker.tick()
    await e.human.respond(actor=e.actor, request_id=req["id"], request=Respond(operation_id=uuid4().hex, expected_request_revision=2, expected_assignment_revision=1, text="The published guide."))
    await worker.tick()
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=work["id"])
    admission = replace(execution, work=None)
    resumed = await WorkExecution(e.human.work).reserve(execution=admission, work_id=work["id"], request=activation(admission, current))
    admission = replace(admission, work=resumed)
    next_run = run_record(admission, resumed)
    next_run.user_id = e.actor.subject_id
    await resumed.bind(admission, next_run, admission.thread_incarnation)
    context = await resumed.context(admission)
    basis = {**context["request"], "response_ids": [response["id"] for response in context["responses"]]}
    basis.pop("purpose")
    await resumed.report(admission, ReportWork(operation_id=uuid4().hex, expected_revision=context["work"]["revision"], action="assess_input", statement="Source identified.", request_basis=basis))
    assert await worker.tick() == 1
    rows = await intents(e)
    assessment = next(row for row in rows if row.payload["work_event_intent"]["key"] == "request.assessed")
    assert assessment.payload["work_event_intent"]["request_revision"] == 2
    assert not any(row.payload["work_event_intent"]["key"] == "attempt.no_result" for row in rows)


@pytest.mark.asyncio
async def test_request_correlation_does_not_depend_on_wall_clock_order(actions):
    from datetime import UTC, datetime, timedelta

    from deerflow.agent_instances.work_execution_contract import ReportWork
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow

    e = actions
    _peer, worker = await setup(e)
    work, execution, scope, run = await activate(e)
    current = (await scope.context(execution))["work"]
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="outcome", statement="Verified result."))
    await scope.finish(execution, run, settled=True)
    async with e.db.sessions.begin() as writer:
        req = await writer.scalar(select(HumanInputRequestRow).where(HumanInputRequestRow.work_id == work["id"]))
        # Deterministically represent a backwards clock step between flushes.
        req.created_at = datetime.now(UTC) + timedelta(days=1)
    assert await worker.tick() == 1
    assert "review.ready" in {row.payload["work_event_intent"]["key"] for row in await intents(e)}


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["actor", "action", "body", "receipt", "revision_type"])
async def test_mismatched_route_receipt_holds_the_pair(actions, damage):
    from deerflow.agent_instances.human_input_contract import RequestCommand
    from deerflow.persistence.agent_instances.human_input import HumanInputEventRow
    from deerflow.persistence.telegram_bots.model import TelegramWorkCursorRow

    e = actions
    peer, worker = await setup(e)
    work, req = await request(e, peer)
    await worker.tick()
    operation = uuid4().hex
    await e.human.command(actor=e.actor, request_id=req["id"], request=RequestCommand(operation_id=operation, action="route", expected_revision=1, expected_request_revision=1, recipient_id=e.actor.subject_id))
    async with e.db.sessions.begin() as writer:
        receipt = await writer.scalar(select(HumanInputEventRow).where(HumanInputEventRow.operation_id == operation))
        if damage == "actor":
            receipt.actor_id = peer.subject_id
        elif damage == "action":
            receipt.action = "withdraw"
        elif damage == "body":
            receipt.request = {**receipt.request, "recipient_id": peer.subject_id}
        elif damage == "receipt":
            receipt.receipt = {**receipt.receipt, "request_id": uuid4().hex}
        else:
            receipt.receipt = {**receipt.receipt, "request_revision": True}
    assert await worker.tick() == 0
    assert len(await intents(e)) == 1
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramWorkCursorRow, (e.link.id, work["id"]))).revision == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose,permissions,expected", [("information", 1, None), ("information", 2, None), ("information", 3, "ask.info"), ("information", 6, "request.unrouted"), ("decision", 3, None), ("decision", 6, "ask.decision")])
async def test_recipient_and_manager_roles_use_current_distinct_grants(actions, purpose, permissions, expected):
    e = actions
    peer, worker = await setup(e)
    await request(e, peer, purpose)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.actor.subject_id))).permissions = permissions
    assert await worker.tick() == 1
    keys = [row.payload["work_event_intent"]["key"] for row in await intents(e)]
    assert keys == ([expected] if expected else [])
