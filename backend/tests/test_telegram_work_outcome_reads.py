"""Outcome reads qualify canonical receipts without dispatching an action."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_control_admission import admissions, stage
from test_telegram_work_control_admission import review_confirmation as _review_confirmation
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.producer import ProductSourceRetired
from deerflow.persistence.telegram_bots.model import TelegramActRow


async def review_confirmation(e):
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    result = await _review_confirmation(e)
    record = result[0]
    async with e.db.sessions.begin() as writer:
        attempt = await writer.get(AgentWorkAttemptRow, record["attempt"]["id"])
        # The shared seeded report predates activation-request qualification.
        # Supply its original captured Work revision for outcome ordering.
        attempt.request = {"expected_revision": 1}
    return result


@pytest.mark.asyncio
async def test_completed_acceptance_reads_exact_receipt_without_repeating_it(work_delivery, monkeypatch):
    from app.telegram_bots.work_outcome_reads import WorkOutcomeReader

    e = work_delivery
    _record, _request, _seen, control = await review_confirmation(e)
    actions = admissions(e)
    staged = await stage(e, actions, control)
    done = await actions.acts.check(staged.id)
    assert done.state == "done"

    async def forbidden(*args, **kwargs):
        raise AssertionError("an outcome read cannot call a command")

    monkeypatch.setattr(e.human.work, "command", forbidden)
    host = WorkOutcomeReader(e.reader, actions.acts)
    result = await host.read(e.link, done.id)
    assert result.act.id == done.id and result.act.body == done.body
    assert result.permissions == 7 and not result.attempt_unresolved
    assert result.source.request["state"] == "closed"
    assert result.act.result["review"]["accepted_at"]
    assert (await actions.acts._work_record(done.id)).attempts == done.attempts


@pytest.mark.asyncio
async def test_done_journal_without_canonical_service_receipt_cannot_claim_success(work_delivery):
    from app.telegram_bots.work_outcome_reads import WorkOutcomeReader

    e = work_delivery
    _record, _request, _seen, control = await review_confirmation(e)
    actions = admissions(e)
    saved = await stage(e, actions, control)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramActRow, saved.id)
        row.state, row.result = "done", {"claimed": "success"}
    with pytest.raises(ProductSourceRetired):
        await WorkOutcomeReader(e.reader, actions.acts).read(e.link, saved.id)


@pytest.mark.asyncio
async def test_uncertain_read_never_reissues_the_action(work_delivery, monkeypatch):
    from app.telegram_bots.work_outcome_reads import WorkOutcomeReader

    e = work_delivery
    _record, _request, _seen, control = await review_confirmation(e)
    actions = admissions(e)
    saved = await stage(e, actions, control)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramActRow, saved.id)
        row.state, row.attempts = "uncertain", 3

    async def forbidden(*args, **kwargs):
        raise AssertionError("an unknown outcome is not an explicit retry")

    monkeypatch.setattr(e.human.work, "command", forbidden)
    result = await WorkOutcomeReader(e.reader, actions.acts).read(e.link, saved.id)
    assert result.act.state == "uncertain" and result.act.attempts == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["grant", "receipt", "journal"])
async def test_final_outcome_read_rejects_late_authority_or_receipt_change(work_delivery, monkeypatch, damage):
    from sqlalchemy import select

    import app.telegram_bots.work_outcome_reads as module
    from app.telegram_bots.producer import DeliveryRetry
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    _record, _request, _seen, control = await review_confirmation(e)
    actions = admissions(e)
    saved = await stage(e, actions, control)
    await actions.acts.check(saved.id)
    original = module.current_attention
    changed = []

    async def mutate(*args, **kwargs):
        async with e.db.sessions.begin() as writer:
            if damage == "grant":
                (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 1
            elif damage == "journal":
                (await writer.get(TelegramActRow, saved.id)).reason = "changed-during-read"
            else:
                row = await writer.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == saved.id))
                row.actor_id = "another-person"
        changed.append(True)
        return await original(*args, **kwargs)

    monkeypatch.setattr(module, "current_attention", mutate)
    with pytest.raises((ProductSourceRetired, AgentDenied, DeliveryRetry)):
        await module.WorkOutcomeReader(e.reader, actions.acts).read(e.link, saved.id)
    assert changed


@pytest.mark.asyncio
async def test_later_attempt_is_ordered_by_work_revision_even_with_older_wall_clock(work_delivery):
    from datetime import UTC, datetime
    from uuid import uuid4

    from app.telegram_bots.work_outcome_reads import WorkOutcomeReader
    from deerflow.agent_instances.work_contract import WorkCommand
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    e = work_delivery
    record, _request, _seen, control = await review_confirmation(e)
    actions = admissions(e)
    saved = await stage(e, actions, control)
    await actions.acts.check(saved.id)
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=record["id"])
    reopened = await e.human.work.command(
        actor=e.actor,
        instance_id=e.instance.id,
        work_id=record["id"],
        request=WorkCommand(operation_id=uuid4().hex, action="reopen", expected_revision=current["revision"], expected_assignment_revision=current["assignment_revision"], note="Follow-up review"),
        can_stop=False,
    )
    # Synthetic host attempt projection, not runtime/native execution evidence.
    async with e.db.sessions.begin() as writer:
        old = await writer.get(AgentWorkAttemptRow, record["attempt"]["id"])
        old.created_at = datetime(2000, 1, 1, tzinfo=UTC)
        writer.add(
            AgentWorkAttemptRow(
                id=uuid4().hex,
                work_id=record["id"],
                activation_id=uuid4().hex,
                requester_id=e.link.owner_user_id,
                assignment_revision=reopened["assignment_revision"],
                instance_generation=1,
                definition_revision=reopened["definition_revision"],
                status="running",
                request={"expected_revision": reopened["revision"]},
                created_at=datetime(2001, 1, 1, tzinfo=UTC),
            )
        )
    shown = await WorkOutcomeReader(e.reader, actions.acts).read(e.link, saved.id)
    assert shown.continued and shown.attempt_unresolved and shown.attempt_status == "running"


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["respond", "route"])
async def test_request_actions_match_both_request_and_work_events(work_delivery, kind):
    from uuid import uuid4

    from app.telegram_bots.work_outcome_reads import WorkOutcomeReader

    e = work_delivery
    actions = admissions(e)
    options = dict(operation_id=uuid4().hex, request_id=e.request["id"], request_revision=1)
    if kind == "respond":
        result = await actions.acts.respond(e.link, **options, assignment_revision=1, text="The complete answer")
    else:
        result = await actions.acts.route(e.link, **options, recipient_id=e.link.owner_user_id)
    assert result.state == "done"
    read = await WorkOutcomeReader(e.reader, actions.acts).read(e.link, result.id)
    assert read.receipt["receipt"]["operation_id"] == result.id and not read.continued
