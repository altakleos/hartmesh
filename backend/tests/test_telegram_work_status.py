"""Status belongs to the exact admitted attempt, never the latest conversation."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_event_intents import activate, setup
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.producer import _item
from deerflow.agent_instances.work_execution_contract import ReportWork
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


async def status_attempt(e):
    from deerflow.persistence.run.model import RunRow

    async with e.db.engine.begin() as connection:
        await connection.run_sync(lambda sync: RunRow.__table__.create(sync, checkfirst=True))
    _peer, worker = await setup(e)
    work, execution, scope, run = await activate(e)
    await worker.tick()
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "progress:" + scope.attempt_id))
        assert row is not None
        return _item(row), work, execution, scope, run, worker


@pytest.mark.asyncio
async def test_status_reads_exact_attempt_and_current_progress(work_delivery):
    from app.telegram_bots.work_status import WorkStatusReader

    e = work_delivery
    item, work, execution, scope, _run, _worker = await status_attempt(e)
    owner = WorkStatusReader(e.reader, settings=e.plans.settings, clock=e.sender.outbox.clock)
    initial = await owner.read(e.link, item)
    assert initial.key == "status.starting" and initial.values["objective"] == "Report findings"
    current = (await scope.context(execution))["work"]
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="progress", statement="Checked <three> sources."))
    shown = await owner.read(e.link, item)
    assert shown.key == "status.working" and shown.values["progress"] == "Checked <three> sources."
    assert shown.origin_signature == initial.origin_signature
    assert shown.signature != initial.signature


@pytest.mark.asyncio
async def test_status_finishes_from_its_own_settlement(work_delivery):
    from app.telegram_bots.work_status import WorkStatusReader

    e = work_delivery
    item, work, execution, scope, run, _worker = await status_attempt(e)
    current = (await scope.context(execution))["work"]
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="outcome", statement="Verified the findings."))
    await scope.finish(execution, run, settled=True)
    shown = await WorkStatusReader(e.reader, settings=e.plans.settings, clock=e.sender.outbox.clock).read(e.link, item)
    assert shown.key == "status.finished" and shown.terminal is True


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["requester", "activation_actor", "activation_body", "attempt_intent", "trigger_key"])
async def test_status_refuses_mismatched_activation_proof(work_delivery, damage):
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_status import WorkStatusReader
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow

    e = work_delivery
    item, _work, _execution, scope, _run, _worker = await status_attempt(e)
    async with e.db.sessions.begin() as writer:
        attempt = await writer.get(AgentWorkAttemptRow, scope.attempt_id)
        event = await writer.get(AgentWorkEventRow, item.event_id)
        if damage == "requester":
            attempt.requester_id = "someone-else"
        elif damage == "activation_actor":
            event.actor_id = "someone-else"
        elif damage == "activation_body":
            event.request = {**event.request, "expected_revision": 99}
        elif damage == "attempt_intent":
            row = await writer.get(TelegramOutboxRow, item.id)
            row.payload = {**row.payload, "work_event_intent": {**row.payload["work_event_intent"], "attempt_id": "different-attempt"}}
            item = _item(row)
        else:
            row = await writer.get(TelegramOutboxRow, item.id)
            row.payload = {**row.payload, "work_event_intent": {**row.payload["work_event_intent"], "key": "status.finished"}}
            item = _item(row)
    with pytest.raises(ProductSourceRetired):
        await WorkStatusReader(e.reader, settings=e.plans.settings, clock=e.sender.outbox.clock).read(e.link, item)


@pytest.mark.asyncio
async def test_status_uses_its_report_even_when_work_has_unrelated_progress(work_delivery):
    from app.telegram_bots.work_status import WorkStatusReader
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    item, work, execution, scope, _run, _worker = await status_attempt(e)
    current = (await scope.context(execution))["work"]
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="progress", statement="Checked the admitted source."))
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentWorkRow, work["id"])).progress = "An unrelated later attempt."
    shown = await WorkStatusReader(e.reader, settings=e.plans.settings, clock=e.sender.outbox.clock).read(e.link, item)
    assert shown.values["progress"] == "Checked the admitted source."


@pytest.mark.asyncio
@pytest.mark.parametrize("own", [False, True])
async def test_status_distinguishes_own_settled_stop_from_other_cancellation(work_delivery, own):
    from test_work_execution import command

    from app.telegram_bots.work_status import WorkStatusReader

    e = work_delivery
    item, work, execution, scope, run, _worker = await status_attempt(e)
    current = (await scope.context(execution))["work"]
    if own:
        await e.human.work.command(actor=e.actor, instance_id=e.instance.id, work_id=work["id"], request=command(current, "stop", attempt_id=scope.attempt_id), can_stop=True)
        stopping = await WorkStatusReader(e.reader, settings=e.plans.settings, clock=e.sender.outbox.clock).read(e.link, item)
        assert stopping.key == "status.stopping" and not stopping.terminal
    run.status = "interrupted"
    await scope.finish(execution, run, settled=True)
    shown = await WorkStatusReader(e.reader, settings=e.plans.settings, clock=e.sender.outbox.clock).read(e.link, item)
    assert shown.key == ("status.stopped" if own else "status.ended") and shown.terminal


@pytest.mark.parametrize("locale", ["en-US", "zh-CN"])
def test_status_text_preserves_full_progress_and_omits_empty_latest_line(locale):
    from dataclasses import replace

    from app.telegram_bots.split import visible_text
    from app.telegram_bots.work_status import WorkStatusRead, render_status

    source = WorkStatusRead("status.working", {"objective": "Long objective " * 30, "progress": "<Verified> " * 500, "time": "09:11", "name": "Analyst", "person": None}, False, False, "a" * 32, "b" * 64, "c" * 64)
    text = render_status(source, locale=locale)
    assert "<Verified> " * 500 in visible_text(text) and "&lt;Verified&gt;" in text
    assert len(visible_text(text).splitlines()[0]) <= 60
    empty = render_status(replace(source, values={**source.values, "progress": ""}), locale=locale)
    assert len(empty.splitlines()) == 2


@pytest.mark.asyncio
async def test_status_waits_for_the_actual_question_recipient(work_delivery):
    from app.telegram_bots.work_status import WorkStatusReader

    e = work_delivery
    item, _work, execution, scope, _run, _worker = await status_attempt(e)
    current = (await scope.context(execution))["work"]
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="request_input", statement="Which source?", purpose="information", reason="Need context", expected_response="A guide"))
    shown = await WorkStatusReader(e.reader, settings=e.plans.settings, clock=e.sender.outbox.clock).read(e.link, item)
    assert shown.key == "status.waiting_you" and not shown.terminal


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["status.starting", "status.finished", "concealed.progress"])
async def test_generic_reply_cannot_impersonate_work_status(work_delivery, key):
    from test_telegram_bot_adapter import message

    e = work_delivery
    async with e.db.sessions.begin() as writer:
        for row in await writer.scalars(select(TelegramOutboxRow)):
            row.state = "sent"
    await e.sender.outbox.plan_product(e.link, source_id="unqualified-work-status", parts=[{"kind": "text", "source_ref": {"key": key, "values": {"objective": "Fabricated assignment", "name": "Analyst", "time": "09:11"}}}])
    message(e.channel.bot.request, identity=2101)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["extra_intent", "boolean_assignment"])
async def test_status_rejects_unsealed_intent_and_typed_activation_receipt(work_delivery, damage):
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_status import WorkStatusReader
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    item, _work, _execution, _scope, _run, _worker = await status_attempt(e)
    async with e.db.sessions.begin() as writer:
        if damage == "extra_intent":
            row = await writer.get(TelegramOutboxRow, item.id)
            row.payload = {**row.payload, "work_event_intent": {**row.payload["work_event_intent"], "unsealed": True}}
            item = _item(row)
        else:
            from sqlalchemy.orm.attributes import flag_modified

            event = await writer.get(AgentWorkEventRow, item.event_id)
            event.result = {**event.result, "assignment_revision": True}
            flag_modified(event, "result")
    with pytest.raises(ProductSourceRetired):
        await WorkStatusReader(e.reader, settings=e.plans.settings, clock=e.sender.outbox.clock).read(e.link, item)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["foreign_attempt", "bad_blocker_revision", "missing_request"])
async def test_status_question_trigger_and_current_blocker_require_exact_proof(work_delivery, damage):
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_status import WorkStatusReader
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow, AgentWorkRow

    e = work_delivery
    item, work, execution, scope, _run, worker = await status_attempt(e)
    current = (await scope.context(execution))["work"]
    operation = uuid4().hex
    await scope.report(execution, ReportWork(operation_id=operation, expected_revision=current["revision"], action="request_input", statement="Which source?", purpose="information", reason="Need context", expected_response="A guide"))
    await worker.tick()
    async with e.db.sessions.begin() as writer:
        event = await writer.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == operation))
        row = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "edit:" + event.id))
        item = _item(row)
        if damage == "foreign_attempt":
            event.request = {**event.request, "attempt_id": "unrelated-attempt"}
        elif damage == "bad_blocker_revision":
            current = await writer.get(AgentWorkRow, work["id"])
            current.blocker = {**current.blocker, "revision": "9" * 80}
        else:
            from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow

            request = await writer.get(HumanInputRequestRow, item.request_id)
            request.basis_id = uuid4().hex
    with pytest.raises(ProductSourceRetired):
        await WorkStatusReader(e.reader, settings=e.plans.settings, clock=e.sender.outbox.clock).read(e.link, item)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "receipt", "command"])
async def test_status_human_question_uses_its_creation_receipt_and_prior_attempt(work_delivery, damage):
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_status import WorkStatusReader
    from deerflow.agent_instances.human_input_contract import CreateRequest
    from deerflow.persistence.agent_instances.human_input import HumanInputEventRow
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    _item0, work, execution, scope, run, worker = await status_attempt(e)
    await scope.finish(execution, run, settled=True)
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=work["id"])
    operation = uuid4().hex
    await e.human.create(
        actor=e.actor,
        instance_id=e.instance.id,
        work_id=work["id"],
        request=CreateRequest(
            operation_id=operation,
            expected_work_revision=current["revision"],
            expected_assignment_revision=current["assignment_revision"],
            purpose="information",
            question="Which guide?",
            reason="Need context",
            expected_response="A source",
            recipient_id=e.actor.subject_id,
        ),
    )
    await worker.tick()
    async with e.db.sessions.begin() as writer:
        event = await writer.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == operation))
        row = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "edit:" + event.id))
        item = _item(row)
        if damage == "receipt":
            receipt = await writer.scalar(select(HumanInputEventRow).where(HumanInputEventRow.operation_id == operation))
            receipt.actor_id = "another-person"
        elif damage == "command":
            event.request = {**event.request, "expected_work_revision": 999}
    owner = WorkStatusReader(e.reader, settings=e.plans.settings, clock=e.sender.outbox.clock)
    if damage:
        with pytest.raises(ProductSourceRetired):
            await owner.read(e.link, item)
    else:
        assert (await owner.read(e.link, item)).key == "status.ended"


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["event", "claim", "account", "grant", "progress"])
async def test_status_final_snapshot_refuses_provider_time_changes(work_delivery, monkeypatch, damage):
    from datetime import UTC, datetime

    import app.gateway.authz as authz
    from app.telegram_bots.attention_authority import prepare_attention
    from app.telegram_bots.authority import AccessOff
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_status import StatusProjection, WorkStatusReader
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow
    from deerflow.persistence.user.model import UserRow

    e = work_delivery
    item, _work, execution, scope, _run, _worker = await status_attempt(e)
    current = (await scope.context(execution))["work"]
    operation = uuid4().hex
    await scope.report(execution, ReportWork(operation_id=operation, expected_revision=current["revision"], action="progress", statement="Checked sources."))
    owner = WorkStatusReader(e.reader, settings=e.plans.settings, clock=e.sender.outbox.clock)
    source = await owner.read(e.link, item)
    original, active, changed = authz.resolve_route_permissions, {}, []

    async def provider(*args, **kwargs):
        permissions = await original(*args, **kwargs)
        writer = active.get("writer")
        if writer is not None and not changed:
            if damage == "event":
                (await writer.get(AgentWorkEventRow, item.event_id)).actor_id = "foreign-actor"
            elif damage == "claim":
                (await writer.get(TelegramOutboxRow, item.id)).claim_id = "foreign-claim"
            elif damage == "account":
                (await writer.get(UserRow, e.link.owner_user_id)).released_at = datetime.now(UTC)
            elif damage == "grant":
                (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 1
            else:
                event = await writer.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == operation))
                event.request = {**event.request, "statement": "Different sources."}
                event.result = {**event.result, "progress": "Different sources."}
            await writer.flush()
            changed.append(True)
        return permissions

    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    admission = await prepare_attention(e.reader, e.link)
    clock = await owner.prepare(e.link)
    with pytest.raises((ProductSourceRetired, AgentDenied, AccessOff)):
        async with e.db.sessions.begin() as writer:
            active["writer"] = writer
            await admission(session=writer, projection=StatusProjection(e.link, item, clock, source))
    assert changed


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing_run", "run_owner", "run_thread", "run_attempt", "run_status"])
async def test_status_terminal_read_rejects_foreign_retained_run(work_delivery, damage):
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_status import WorkStatusReader
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
    from deerflow.persistence.run.model import RunRow

    e = work_delivery
    item, _work, execution, scope, run, _worker = await status_attempt(e)
    current = (await scope.context(execution))["work"]
    await scope.report(execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="outcome", statement="Verified findings."))
    await scope.finish(execution, run, settled=True)
    async with e.db.sessions.begin() as writer:
        if damage == "missing_run":
            (await writer.get(AgentWorkAttemptRow, scope.attempt_id)).run_id = None
            event = await writer.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.work_id == item.work_id, AgentWorkEventRow.action == "attempt_settled"))
            event.request = {**event.request, "run_id": None}
        else:
            row = RunRow(run_id=run.run_id, thread_id=run.thread_id, user_id=run.user_id, status=run.status, idempotency_key=run.idempotency_key, metadata_json={**run.metadata, "agent_instance_id": e.instance.id})
            if damage == "run_owner":
                row.user_id = "another-person"
            elif damage == "run_thread":
                row.thread_id = "another-thread"
            elif damage == "run_attempt":
                row.metadata_json = {**row.metadata_json, "agent_work_attempt_id": "another-attempt"}
            else:
                row.status = "error"
            writer.add(row)
    with pytest.raises(ProductSourceRetired):
        await WorkStatusReader(e.reader, settings=e.plans.settings, clock=e.sender.outbox.clock).read(e.link, item)
