"""Stable refusal codes preserve existing exception and HTTP contracts."""

from dataclasses import replace

import httpx
import pytest
from _storage_spaces_test_support import ALICE, operation
from fastapi import APIRouter, FastAPI
from test_agent_instances import instances as instances
from test_agent_work_records import command, submit_fixture
from test_agent_work_records import work as work
from test_work_execution import activation, run_record
from test_work_execution import execution as execution

from app.gateway.routers.agent_instances import InstanceRoute
from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.agent_instances.work_execution_contract import ReportWork
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow


@pytest.mark.parametrize("exception", [AgentDenied, AgentConflict])
def test_reason_preserves_exception_arguments(exception):
    plain = exception("Existing sentence", 7)
    coded = exception("Existing sentence", 7, reason="work-changed")
    assert coded.args == plain.args == ("Existing sentence", 7)
    assert str(coded) == str(plain)
    assert coded.reason == "work-changed"
    assert plain.reason is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,reason,status,body",
    [
        (AgentConflict, "work-changed", 409, {"detail": "Existing sentence", "reason": "work-changed"}),
        (AgentConflict, None, 409, {"detail": "Existing sentence"}),
        (AgentDenied, "grant-required", 404, {"detail": "Agent instance is unavailable"}),
    ],
)
async def test_http_reason_preserves_denial_privacy(kind, reason, status, body):
    app = FastAPI()
    router = APIRouter(route_class=InstanceRoute)

    @router.get("/refusal")
    async def refusal():
        raise kind("Existing sentence", reason=reason)

    app.include_router(router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/refusal")
    assert response.status_code == status
    assert response.json() == body


@pytest.mark.asyncio
@pytest.mark.parametrize("status,reason", [("starting", "attempt-running"), ("running", "attempt-running"), ("stopping", "attempt-unsettled"), ("uncertain", "attempt-unsettled")])
async def test_reserve_distinguishes_unresolved_attempts(execution, status, reason):
    service, admitted, row, sf = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    async with sf() as session, session.begin():
        (await session.get(AgentWorkAttemptRow, scope.attempt_id)).status = status
    current = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    with pytest.raises(AgentConflict) as caught:
        await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, current))
    assert caught.value.reason == reason
    assert str(caught.value) == "Reconcile the prior attempt before activating"


async def _running(service, admitted, row):
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    admitted = replace(admitted, work=scope)
    run = run_record(admitted, scope)
    await scope.bind(admitted, run, admitted.thread_incarnation)
    return scope, admitted, run


async def _outcome(service, admitted, row):
    scope, admitted, run = await _running(service, admitted, row)
    current = (await scope.context(admitted))["work"]
    await scope.report(admitted, ReportWork(operation_id=operation(), expected_revision=current["revision"], action="outcome", statement="Reviewed the guide."))
    await scope.finish(admitted, run, settled=True)
    return await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])


@pytest.mark.asyncio
async def test_change_request_context_survives_edits_and_later_attempts(execution):
    service, admitted, row, _ = execution
    result = await _outcome(service, admitted, row)
    changed = await service.work.command(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], request=command(result, "changes_requested", note="Include the missing deployment checks."))
    history = await service.work.history(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    event = next(event for event in history if event["action"] == "changes_requested")
    edited = await service.work.command(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], request=command(changed, "edit", objective="Review the updated guide"))
    expected = {"note": "Include the missing deployment checks.", "actor_id": "alice", "at": event["created_at"]}
    for _ in range(2):
        scope, current_admitted, run = await _running(service, admitted, edited)
        assert (await scope.context(current_admitted))["changes_requested"] == expected
        await scope.finish(current_admitted, run, settled=True)
        edited = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])


@pytest.mark.asyncio
async def test_cancel_and_reopen_clear_change_request_context(execution):
    service, admitted, row, _ = execution
    result = await _outcome(service, admitted, row)
    current = await service.work.command(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], request=command(result, "changes_requested", note="Add checks."))
    for action in ("cancel", "reopen"):
        current = await service.work.command(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], request=command(current, action, note="Review again."))
    scope, current_admitted, _ = await _running(service, admitted, current)
    assert (await scope.context(current_admitted))["changes_requested"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["decide", "accept"])
async def test_human_decisions_refuse_unresolved_attempt_with_code(execution, action):
    service, admitted, row, _ = execution
    await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    current = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    fields = {"note": "Use this answer", "blocker_id": operation(), "blocker_revision": 1} if action == "decide" else {"outcome_id": operation(), "evidence_revision": 1, "basis": "outcome_statement", "acknowledge_unchecked_sources": True}
    with pytest.raises(AgentConflict) as caught:
        await service.work.command(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], request=command(current, action, **fields))
    assert caught.value.reason == "attempt-unresolved"
    assert str(caught.value) == "An unresolved attempt requires host reconciliation"


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["accept", "cancel", "reopen"])
async def test_latest_terminal_event_clears_note_even_after_unrelated_events(execution, action):
    from datetime import UTC, datetime

    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    service, admitted, row, sf = execution
    scope, current_admitted, _ = await _running(service, admitted, row)
    # Seed historical events to isolate each clearing action. A real terminal
    # assignment must be reopened before another live context can be read.
    async with sf() as session, session.begin():
        for revision, event_action in enumerate(("changes_requested", action, "edit"), start=100):
            session.add(
                AgentWorkEventRow(
                    id=operation(),
                    instance_id=admitted.instance.id,
                    work_id=row["id"],
                    actor_id="alice",
                    actor_kind="human",
                    operation_id=operation(),
                    action=event_action,
                    revision=revision,
                    assignment_revision=1,
                    request={"note": "Old note"},
                    result={},
                    created_at=datetime.now(UTC),
                )
            )
    assert (await scope.context(current_admitted))["changes_requested"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"status": "suspended", "generation": 99, "definition_revision": "changed"}, "agent-not-active"),
        ({"generation": 99, "definition_revision": "changed"}, "execution-stale"),
        ({"definition_revision": "changed"}, "execution-stale"),
        ({"work_definition": "changed"}, "brief-changed"),
        ({"enabled": False}, "work-off"),
    ],
)
async def test_activation_mandate_refusal_precedence(execution, monkeypatch, changes, reason):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from deerflow.persistence.agent_instances.model import AgentInstanceRow
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    service, admitted, row, sf = execution
    async with sf() as session:
        parent = await session.get(AgentInstanceRow, admitted.instance.id)
        record = await session.get(AgentWorkRow, row["id"])
        for key, value in changes.items():
            if key == "work_definition":
                record.definition_revision = value
            elif key != "enabled":
                setattr(parent, key, value)
        monkeypatch.setattr(service.work, "_policy", AsyncMock(return_value=SimpleNamespace(enabled=changes.get("enabled", True))))
        with pytest.raises(AgentDenied) as caught:
            await service._admit(session, admitted, parent, record)
    assert caught.value.reason == reason
    assert str(caught.value) == "Work requires an active reconciled mandate"


@pytest.mark.asyncio
@pytest.mark.parametrize("status,reason", [("submitted", "awaiting-review"), ("completed", "work-closed"), ("cancelled", "work-closed")])
async def test_activation_closed_work_reason(execution, status, reason):
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    service, admitted, row, sf = execution
    if status != "cancelled":
        await submit_fixture(sf, row)
    async with sf() as session, session.begin():
        saved = await session.get(AgentWorkRow, row["id"])
        saved.status = status
        if status == "completed":
            saved.review_required, saved.review_state = False, "reported"
    with pytest.raises(AgentConflict) as caught:
        await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    assert caught.value.reason == reason
    assert str(caught.value) == "Work requires Changes requested or Reopen before activation"


@pytest.mark.asyncio
async def test_activation_revision_and_identity_reasons(execution):
    service, admitted, row, _ = execution
    body = activation(admitted, row)
    with pytest.raises(AgentConflict) as caught:
        await service.reserve(execution=admitted, work_id=row["id"], request=body.model_copy(update={"expected_revision": 99}))
    assert caught.value.reason == "work-changed"
    await service.reserve(execution=admitted, work_id=row["id"], request=body)
    with pytest.raises(AgentConflict) as caught:
        await service.reserve(execution=admitted, work_id=row["id"], request=body.model_copy(update={"expected_revision": 99}))
    assert caught.value.reason == "operation-mismatch"


@pytest.mark.asyncio
async def test_activation_requires_exact_conversation(execution):
    service, admitted, row, _ = execution
    with pytest.raises(AgentDenied) as caught:
        await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row).model_copy(update={"thread_id": operation()}))
    assert caught.value.reason == "conversation-mismatch"


def test_revision_exhaustion_reason():
    from types import SimpleNamespace

    from deerflow.agent_instances.work_execution import WorkExecution

    row = SimpleNamespace(revision=2147483647)
    with pytest.raises(AgentConflict) as caught:
        WorkExecution._advance(row)
    assert caught.value.reason == "revision-exhausted"
    assert row.revision == 2147483647


@pytest.mark.asyncio
async def test_delegation_codes_keep_existing_authority_and_sentences(work, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from _storage_spaces_test_support import BOB

    from deerflow.agent_instances.work_contract import DelegateWork, WorkSource
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow

    service, instance, sf = work
    body = DelegateWork(operation_id=operation(), objective="Review guide", success_criteria="List gaps")

    async def refused(actor, request, kind, reason, sentence, target=None):
        with pytest.raises(kind) as caught:
            await service.delegate(actor=actor, instance_id=target or instance.id, request=request)
        assert caught.value.reason == reason
        assert str(caught.value) == sentence

    await refused(ALICE, body, AgentDenied, "not-visible", "Work requires current instance visibility", operation())
    first = await service.delegate(actor=ALICE, instance_id=instance.id, request=body)
    await refused(ALICE, body.model_copy(update={"objective": "Another guide"}), AgentConflict, "operation-mismatch", "Operation identity belongs to another actor, Work or request")
    await refused(ALICE, body.model_copy(update={"operation_id": operation(), "responsibility": "missing"}), AgentConflict, "responsibility-unknown", "Responsibility is absent from the adopted mandate")
    await refused(ALICE, body.model_copy(update={"operation_id": operation(), "review_required": False}), AgentConflict, "review-required", "The adopted policy requires review")
    await refused(ALICE, body.model_copy(update={"operation_id": operation(), "sources": [WorkSource(space_id=operation(), path="guide.txt")]}), AgentDenied, "source-unavailable", "Source is unavailable")
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (instance.id, "bob"))).permissions = 3
    await refused(BOB, body.model_copy(update={"operation_id": operation(), "priority": "urgent"}), AgentDenied, "manage-required", "Assignment overrides require Manage plus Inspect")
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (instance.id, "bob"))).permissions = 1
    await refused(BOB, body.model_copy(update={"operation_id": operation()}), AgentDenied, "grant-required", "Work requires Inspect and the relevant Use or Manage grant")
    with monkeypatch.context() as patch:
        patch.setattr(service, "_policy", AsyncMock(return_value=SimpleNamespace(enabled=False)))
        await refused(ALICE, body.model_copy(update={"operation_id": operation()}), AgentConflict, "work-off", "New Work requires an active AI employee with an enabled adopted Work policy")
        async with sf() as session, session.begin():
            (await session.get(AgentInstanceRow, instance.id)).status = "suspended"
        await refused(ALICE, body.model_copy(update={"operation_id": operation()}), AgentConflict, "agent-not-active", "New Work requires an active AI employee with an enabled adopted Work policy")
    assert first["revision"] == 1


@pytest.mark.asyncio
async def test_locked_activation_rechecks_exact_scope_with_codes(execution, monkeypatch):
    from unittest.mock import AsyncMock

    service, admitted, row, sf = execution
    cases = [
        (replace(admitted, thread_incarnation="changed"), "conversation-changed", "Work conversation binding changed"),
        (replace(admitted, home_generation=admitted.home_generation + 1), "space-changed", "Work Home changed"),
    ]
    for changed, reason, sentence in cases:
        async with sf() as session, session.begin():
            with pytest.raises(AgentDenied) as caught:
                await service._locked(session, changed, row["id"])
            assert caught.value.reason == reason
            assert str(caught.value) == sentence
    with monkeypatch.context() as patch:
        patch.setattr(service, "_sources", AsyncMock(side_effect=[[], [{"space_id": admitted.instance.home_id, "path": "changed.txt"}]]))
        async with sf() as session, session.begin():
            with pytest.raises(AgentConflict) as caught:
                await service._locked(session, admitted, row["id"])
            assert caught.value.reason == "sources-changed"
            assert str(caught.value) == "Work sources changed; reload before acting"


@pytest.mark.asyncio
async def test_pending_lifecycle_refuses_delegation_and_activation(execution):
    from deerflow.agent_instances.work_contract import DelegateWork
    from deerflow.persistence.agent_instances.model import AgentLifecycleRow

    service, admitted, row, sf = execution
    async with sf() as session, session.begin():
        session.add(
            AgentLifecycleRow(
                instance_id=admitted.instance.id,
                operation_id=operation(),
                actor_id="alice",
                generation=admitted.instance.generation,
                home_generation=admitted.home_generation,
                home_id=admitted.instance.home_id,
                request={},
                attachment_ids=[],
                phase="pending",
                prior_status="active",
            )
        )
    with pytest.raises(AgentConflict) as caught:
        await service.work.delegate(actor=ALICE, instance_id=admitted.instance.id, request=DelegateWork(operation_id=operation(), objective="Review", success_criteria="List gaps"))
    assert caught.value.reason == "lifecycle-pending"
    assert str(caught.value) == "Resolve the pending lifecycle operation before delegating"
    with pytest.raises(AgentDenied) as caught:
        await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    assert caught.value.reason == "lifecycle-pending"
    assert str(caught.value) == "Lifecycle containment is pending"


@pytest.mark.asyncio
async def test_first_work_context_requires_inspect_with_code(execution):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    _service, admitted, _row, sf = execution
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (admitted.instance.id, "alice"))).permissions = 1
    with pytest.raises(AgentDenied) as caught:
        await admitted.authority.context_admission(admitted, mark_work=True)
    assert caught.value.reason == "grant-required"
    assert str(caught.value) == "Work context requires current Inspect"
