"""Work action journal and service receipts over real shared SQL storage."""

import pytest
import pytest_asyncio
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select

from app.telegram_bots.acts import Acts
from app.telegram_bots.producer import Outbox
from deerflow.persistence.agent_instances.human_input import HumanInputResponseRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow


@pytest_asyncio.fixture
async def actions(db, monkeypatch):
    from _telegram_work_test_support import work_environment
    from test_telegram_threads import close

    e = await work_environment(db, monkeypatch)
    e.acts = Acts(db.sessions, settings=db.settings, callbacks=None, outbox=Outbox(db.sessions), work=e.human.work, authority=e.live.authority, links=db.links)
    try:
        yield e
    finally:
        await close(e)


@pytest.mark.asyncio
async def test_response_is_attributed_once_and_never_starts_work(actions):
    e = actions
    identity = operation()
    kwargs = dict(operation_id=identity, request_id=e.request["id"], request_revision=e.request["request_revision"], assignment_revision=e.request["assignment_revision"], text="Use the current guide.")
    first = await e.acts.respond(e.link, **kwargs)
    replay = await e.acts.respond(e.link, **kwargs)
    assert first.state == replay.state == "done"
    assert first.id == identity and first.body["operation_id"] == identity
    async with e.db.sessions() as session:
        response = await session.scalar(select(HumanInputResponseRow))
        assert response.actor_id == e.link.owner_user_id
        assert await session.scalar(select(func.count()).select_from(HumanInputResponseRow)) == 1
        assert await session.scalar(select(func.count()).select_from(AgentWorkAttemptRow)) == 0
        assert (await session.get(TelegramActRow, identity)).attempts == 1
    with pytest.raises(ValueError, match="act-source-conflict"):
        await e.acts.respond(e.link, **{**kwargs, "text": "Different reply"})


@pytest.mark.asyncio
async def test_lost_response_recovers_exact_receipt_without_another_mutation(actions, monkeypatch):
    from deerflow.agent_instances.human_input import HumanInput

    e = actions
    original = HumanInput.respond
    calls = []

    async def lose(self, **kwargs):
        calls.append(kwargs["request"].operation_id)
        await original(self, **kwargs)
        raise RuntimeError("lost response after commit")

    monkeypatch.setattr(HumanInput, "respond", lose)
    identity = operation()
    first = await e.acts.respond(e.link, operation_id=identity, request_id=e.request["id"], request_revision=1, assignment_revision=1, text="The current guide.")
    assert first.state == "pending"
    recovered = await e.acts.check(identity)
    assert recovered.state == "done" and calls == [identity]
    assert (await e.human.get(actor=e.actor, request_id=e.request["id"]))["state"] == "answered"


@pytest.mark.asyncio
async def test_changed_request_refuses_original_revision_without_rewriting_the_journal(actions):
    from deerflow.agent_instances.human_input_contract import RequestCommand

    e = actions
    await e.human.command(actor=e.actor, request_id=e.request["id"], request=RequestCommand(operation_id=operation(), action="route", expected_revision=1, expected_request_revision=1, recipient_id=e.actor.subject_id))
    identity = operation()
    result = await e.acts.respond(e.link, operation_id=identity, request_id=e.request["id"], request_revision=1, assignment_revision=1, text="Old answer.")
    assert result.state == "conflict"
    assert result.body["expected_request_revision"] == 1
    async with e.db.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(HumanInputResponseRow)) == 0


async def management_request(e, purpose):
    from test_agent_work_records import submit_fixture

    from deerflow.agent_instances.human_input_contract import CreateRequest
    from deerflow.agent_instances.work_contract import DelegateWork

    record = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=operation(), objective="Review the guide", success_criteria="Explain gaps"))
    if purpose == "review":
        await submit_fixture(e.db.sessions, record)
    request = await e.human.create(
        actor=e.actor,
        instance_id=e.instance.id,
        work_id=record["id"],
        request=CreateRequest(
            operation_id=operation(),
            expected_work_revision=1,
            expected_assignment_revision=1,
            purpose=purpose,
            question="Review the current result",
            reason="A human decision is required",
            expected_response="An explicit decision",
            recipient_id=e.link.owner_user_id,
        ),
    )
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=record["id"])
    seen = dict(
        work_revision=current["revision"],
        assignment_revision=current["assignment_revision"],
        request_id=request["id"],
        revision=request["revision"],
        request_revision=request["request_revision"],
        response_ids=[],
        outcome_id=current.get("outcome_id"),
        evidence_revision=(current.get("outcome") or {}).get("evidence_revision"),
        blocker_id=(current.get("blocker") or {}).get("id"),
        blocker_revision=(current.get("blocker") or {}).get("revision"),
    )
    return current, request, seen


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,expected", [("decide", "open"), ("accept", "completed"), ("request_changes", "open")])
async def test_management_actions_apply_captured_basis_without_starting_an_attempt(actions, kind, expected):
    e = actions
    record, request, seen = await management_request(e, "decision" if kind == "decide" else "review")
    kwargs = dict(operation_id=operation(), request_id=request["id"], work_id=record["id"], seen=seen)
    if kind == "decide":
        kwargs.update(decision="Proceed with the guide", note="Keep the scope bounded.")
    elif kind == "accept":
        kwargs.update(outcome_id=seen["outcome_id"], evidence_revision=seen["evidence_revision"])
    else:
        kwargs.update(note="Include deployment evidence.")
    before = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=record["id"])
    result = await getattr(e.acts, kind)(e.link, **kwargs)
    assert result.state == "done"
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=record["id"])
    assert current["status"] == expected
    assert current["attempt"] == before["attempt"]
    assert (await e.human.get(actor=e.actor, request_id=request["id"]))["state"] == "closed"
    replay = await getattr(e.acts, kind)(e.link, **kwargs)
    assert replay.id == result.id and replay.state == "done"
    if kind == "accept":
        assert result.body["acknowledge_unchecked_sources"] is True


@pytest.mark.asyncio
async def test_worker_retries_only_eligible_service_actions_with_original_body_and_limit(actions, monkeypatch):
    from datetime import timedelta

    from test_telegram_outbox_workers import Clock

    from app.telegram_bots.work_actions import ActsWorker
    from deerflow.agent_instances.human_input import HumanInput

    e = actions
    clock = Clock()
    e.acts.outbox.clock = clock
    calls = []

    async def unavailable(self, **kwargs):
        calls.append(kwargs["request"].model_dump(mode="json", exclude_unset=True))
        raise RuntimeError("service outcome unknown")

    monkeypatch.setattr(HumanInput, "respond", unavailable)
    identity = operation()
    first = await e.acts.respond(e.link, operation_id=identity, request_id=e.request["id"], request_revision=1, assignment_revision=1, text="The guide.")
    assert first.state == "pending"
    worker = ActsWorker(e.acts)
    async with e.db.sessions.begin() as session:
        session.add(
            TelegramActRow(
                id=operation(),
                link_id=e.link.id,
                bot_id=e.link.bot_id,
                owner_user_id=e.link.owner_user_id,
                binding_generation=e.link.binding_generation,
                telegram_user_id=e.link.telegram_user_id,
                kind="network",
                body={},
                state="pending",
                attempts=1,
                first_attempt_at=clock.now,
                created_at=clock.now,
                updated_at=clock.now,
            )
        )
    for _ in range(3):
        clock.advance(5)
        await worker.tick()
    saved = await e.acts._work_record(identity)
    assert saved.state == "uncertain" and saved.attempts == 3
    assert len(calls) == 3 and all(body == calls[0] for body in calls)
    # A long-stopped process never executes an hour-old uncertain action anew.
    async with e.db.sessions.begin() as session:
        row = await session.get(TelegramActRow, identity)
        row.state, row.attempts = "pending", 1
        row.first_attempt_at = clock.now - timedelta(hours=1)
        row.updated_at = clock.now - timedelta(seconds=5)
    await worker.tick()
    assert (await e.acts._work_record(identity)).state == "uncertain" and len(calls) == 3


@pytest.mark.asyncio
async def test_changes_request_cas_rejects_a_review_comment_after_preview_check(actions, monkeypatch):
    from test_work_human_input import reply

    e = actions
    record, request, seen = await management_request(e, "review")
    original = e.human.work.command
    changed = []

    async def race(**kwargs):
        if not changed:
            await e.human.respond(actor=e.actor, request_id=request["id"], request=reply(request))
            changed.append(True)
        return await original(**kwargs)

    monkeypatch.setattr(e.human.work, "command", race)
    identity = operation()
    result = await e.acts.request_changes(e.link, operation_id=identity, request_id=request["id"], work_id=record["id"], note="Keep this unsubmitted note.", seen=seen)
    assert changed and result.state == "conflict"
    assert result.body["expected_revision"] == seen["work_revision"]
    assert result.body["note"] == "Keep this unsubmitted note."
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=record["id"])
    assert current["status"] == "submitted" and current["outcome_id"] == seen["outcome_id"]
    assert current["revision"] == seen["work_revision"] + 1
    await e.human.mark_read(actor=e.actor, request_id=request["id"], revision=1)
    after_read = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=record["id"])
    assert after_read["revision"] == current["revision"]


@pytest.mark.asyncio
async def test_binding_retired_after_journal_admission_cannot_commit_the_response(actions, monkeypatch):
    from sqlalchemy import update

    from deerflow.agent_instances.human_input import HumanInput
    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

    e = actions
    original = HumanInput.respond

    async def retire(self, **kwargs):
        async with e.db.sessions.begin() as session:
            await session.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id).values(binding_generation=e.link.binding_generation + 1))
        return await original(self, **kwargs)

    monkeypatch.setattr(HumanInput, "respond", retire)
    result = await e.acts.respond(e.link, operation_id=operation(), request_id=e.request["id"], request_revision=1, assignment_revision=1, text="Must not be recorded after disconnect.")
    assert result.state == "refused"
    async with e.db.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(HumanInputResponseRow)) == 0


@pytest.mark.asyncio
async def test_pause_blocks_decision_but_not_reply_and_same_intent_can_be_retried(actions):
    from datetime import UTC, datetime

    from app.telegram_bots.acts import ActsWorker
    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

    e = actions
    record, request, seen = await management_request(e, "decision")
    async with e.db.sessions.begin() as session:
        (await session.get(TelegramBotLinkRow, e.link.id)).acts_paused_since = datetime.now(UTC)
    kwargs = dict(operation_id=operation(), request_id=request["id"], work_id=record["id"], decision="Proceed", note=None, seen=seen)
    paused = await e.acts.decide(e.link, **kwargs)
    assert paused.state == "pending" and paused.reason == "actions-paused" and paused.attempts == 0
    await ActsWorker(e.acts).tick()
    assert (await e.acts._work_record(paused.id)).attempts == 0
    response = await e.acts.respond(e.link, operation_id=operation(), request_id=e.request["id"], request_revision=1, assignment_revision=1, text="Still allowed to answer.")
    assert response.state == "done"
    async with e.db.sessions.begin() as session:
        (await session.get(TelegramBotLinkRow, e.link.id)).acts_paused_since = None
    retried = await e.acts.decide(e.link, **kwargs)
    assert retried.id == paused.id and retried.state == "done"
    assert retried.body == paused.body


@pytest.mark.asyncio
async def test_recovery_refuses_an_operation_id_that_does_not_match_its_journal(actions, monkeypatch):
    from deerflow.agent_instances.human_input import HumanInput

    e = actions

    async def unavailable(self, **kwargs):
        raise RuntimeError("unknown response")

    monkeypatch.setattr(HumanInput, "respond", unavailable)
    first = await e.acts.respond(e.link, operation_id=operation(), request_id=e.request["id"], request_revision=1, assignment_revision=1, text="Original answer.")
    async with e.db.sessions.begin() as session:
        row = await session.get(TelegramActRow, first.id)
        row.body = {**row.body, "operation_id": operation()}
    result = await e.acts.check(first.id)
    assert result.state == "refused" and result.reason == "invalid"
    async with e.db.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(HumanInputResponseRow)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["respond", "changes"])
async def test_generic_admission_rolls_back_at_the_final_boundary_and_skips_exact_receipts(actions, kind):
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.agent_instances.human_input_contract import Respond
    from deerflow.agent_instances.work_contract import WorkCommand

    e = actions
    if kind == "respond":
        body = Respond(operation_id=operation(), expected_request_revision=1, expected_assignment_revision=1, text="One reply.")

        async def call(admission):
            return await e.human.respond(actor=e.actor, request_id=e.request["id"], request=body, admission=admission)
    else:
        record, request, seen = await management_request(e, "review")
        body = WorkCommand(operation_id=operation(), expected_revision=seen["work_revision"], expected_assignment_revision=seen["assignment_revision"], action="changes_requested", note="Add evidence.")

        async def call(admission):
            return await e.human.work.command(actor=e.actor, instance_id=e.instance.id, work_id=record["id"], request=body, admission=admission)

    calls = []

    async def final_refusal(writer):
        assert writer.in_transaction()
        calls.append(True)
        if len(calls) == 2:
            raise AgentDenied("Caller custody retired")

    with pytest.raises(AgentDenied):
        await call(final_refusal)
    assert len(calls) == 2
    # The whole command, including its operation receipt, rolled back; an
    # identical call may now commit, rather than replaying a ghost receipt.
    calls.clear()

    async def allowed(writer):
        calls.append(True)

    first = await call(allowed)
    assert len(calls) == 2

    async def forbidden(writer):
        pytest.fail("An exact receipt must not admit a fresh effect")

    replay = await call(forbidden)
    if kind == "respond":
        assert first["receipt"] == replay["receipt"]
    else:
        assert first == replay


@pytest.mark.asyncio
async def test_final_admission_rejects_a_changed_journal_scope(actions, monkeypatch):
    from deerflow.agent_instances.human_input import HumanInput

    e = actions
    identity = operation()
    original = HumanInput._project
    changed = []

    async def project(self, session, *args, **kwargs):
        result = await original(self, session, *args, **kwargs)
        row = await session.get(TelegramActRow, identity)
        if row is not None and row.attempts and not changed:
            row.request_id = operation()
            await session.flush()
            changed.append(True)
        return result

    monkeypatch.setattr(HumanInput, "_project", project)
    result = await e.acts.respond(e.link, operation_id=identity, request_id=e.request["id"], request_revision=1, assignment_revision=1, text="Exact original target only.")
    assert changed and result.state == "refused"
    async with e.db.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(HumanInputResponseRow)) == 0
        assert (await session.get(TelegramActRow, identity)).request_id == e.request["id"]


@pytest.mark.asyncio
async def test_ceiling_withdrawn_after_journal_admission_refuses_the_domain_effect(actions, monkeypatch):
    from app.gateway import authz
    from deerflow.agent_instances.human_input import HumanInput
    from deerflow.authz.provider import AuthzDecision

    e = actions
    permitted = [True]
    e.live.config.authorization.enabled = True

    class Provider:
        async def aauthorize(self, request):
            return AuthzDecision(allow=permitted[0] or request.target != "agents:write")

    provider = Provider()

    async def prepared(config):
        return provider

    monkeypatch.setattr(authz, "_aget_cached_route_provider", prepared)
    original = HumanInput.respond

    async def revoke(self, **kwargs):
        permitted[0] = False
        return await original(self, **kwargs)

    monkeypatch.setattr(HumanInput, "respond", revoke)
    result = await e.acts.respond(e.link, operation_id=operation(), request_id=e.request["id"], request_revision=1, assignment_revision=1, text="Cannot write after withdrawal.")
    assert result.state == "refused" and result.reason == "ceiling"
    async with e.db.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(HumanInputResponseRow)) == 0


@pytest.mark.asyncio
async def test_restart_recovers_an_admitted_journal_before_its_first_domain_call(actions):
    from test_telegram_outbox_workers import Clock

    from app.telegram_bots.acts import ActsWorker
    from deerflow.agent_instances.human_input_contract import Respond

    e = actions
    clock = Clock()
    e.acts.outbox.clock = clock
    body = Respond(operation_id=operation(), expected_request_revision=1, expected_assignment_revision=1, text="Recover the original intent.")
    saved = await e.acts._save_work_act(e.link, kind="respond", request_id=e.request["id"], work_id=e.work["id"], request=body)
    assert saved.attempts == 0 and saved.first_attempt_at is not None
    # A new journal/worker has no process-local record of the prior admission.
    restarted = Acts(e.db.sessions, settings=e.db.settings, callbacks=None, outbox=Outbox(e.db.sessions, clock=clock), work=e.human.work, authority=e.live.authority, links=e.db.links)
    clock.advance(5)
    await ActsWorker(restarted).tick()
    result = await restarted._work_record(saved.id)
    assert result.state == "done" and result.attempts == 1
    assert result.body == saved.body


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["replacement", "sign_on_only"])
async def test_configuration_change_after_preparation_retires_fresh_action_authority(actions, monkeypatch, change):
    from deerflow.agent_instances.human_input import HumanInput

    e = actions
    original = HumanInput.respond

    async def replace_config(self, **kwargs):
        if change == "replacement":
            replacement = e.live.config.model_copy(deep=True)
            replacement.authorization.enabled = True
            e.live.config = replacement
        else:
            e.live.config.auth.local.enabled = False
        return await original(self, **kwargs)

    monkeypatch.setattr(HumanInput, "respond", replace_config)
    result = await e.acts.respond(e.link, operation_id=operation(), request_id=e.request["id"], request_revision=1, assignment_revision=1, text="No stale provider or account mode.")
    assert result.state != "done"
    async with e.db.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(HumanInputResponseRow)) == 0


@pytest.mark.asyncio
async def test_worker_stop_drains_and_retires_an_uncommitted_service_admission(actions, monkeypatch):
    import asyncio

    from test_telegram_outbox_workers import Clock

    from app.telegram_bots.acts import ActsWorker
    from deerflow.agent_instances.human_input import HumanInput
    from deerflow.agent_instances.human_input_contract import Respond

    e = actions
    clock = Clock()
    e.acts.outbox.clock = clock
    body = Respond(operation_id=operation(), expected_request_revision=1, expected_assignment_revision=1, text="Original durable response.")
    saved = await e.acts._save_work_act(e.link, kind="respond", request_id=e.request["id"], work_id=e.work["id"], request=body)
    entered, release = asyncio.Event(), asyncio.Event()
    original = HumanInput.respond

    async def held(self, **kwargs):
        entered.set()
        await release.wait()
        return await original(self, **kwargs)

    monkeypatch.setattr(HumanInput, "respond", held)
    worker = ActsWorker(e.acts)
    clock.advance(5)
    running = asyncio.create_task(worker.tick())
    await asyncio.wait_for(entered.wait(), 3)
    stopping = asyncio.create_task(worker.stop())
    await asyncio.sleep(0)
    assert not stopping.done()
    release.set()
    await asyncio.gather(running, stopping)
    async with e.db.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(HumanInputResponseRow)) == 0
    pending = await e.acts._work_record(saved.id)
    assert pending.state == "pending" and pending.body == saved.body
    clock.advance(5)
    await ActsWorker(e.acts).tick()
    assert (await e.acts._work_record(saved.id)).state == "done"
