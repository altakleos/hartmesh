"""Real SQL and authority for records-only Work; no simulated execution claims."""

import pytest
import pytest_asyncio
from _storage_spaces_test_support import ALICE, BOB, operation
from test_agent_instances import create, definition
from test_agent_instances import instances as instances

from deerflow.agent_instances.contract import AgentConflict, AgentDenied, DefinitionSnapshot
from deerflow.agent_instances.work import AgentWork
from deerflow.agent_instances.work_contract import DelegateWork, WorkCommand
from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputReadRow, HumanInputRequestRow, HumanInputResponseRow
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow


@pytest_asyncio.fixture
async def work(instances):
    agents, files, sf, catalog = instances
    async with sf.kw["bind"].begin() as connection:
        await connection.run_sync(lambda c: AgentWorkRow.__table__.create(c))
        await connection.run_sync(lambda c: AgentWorkAttemptRow.__table__.create(c))
        await connection.run_sync(lambda c: AgentWorkEventRow.__table__.create(c))
        for model in (HumanInputRequestRow, HumanInputResponseRow, HumanInputEventRow, HumanInputReadRow):
            await connection.run_sync(model.__table__.create)
    snapshot = DefinitionSnapshot.capture(owner_id="alice", config={"name": "analyst", "work_policy": {"enabled": True}}, soul="Maintain useful technical documentation.")
    instance = await create(agents, custody="company", supervisor=BOB, snapshot=snapshot)
    return AgentWork(agents), instance, sf


async def delegate(service, instance, **kwargs):
    return await service.delegate(actor=ALICE, instance_id=instance.id, request=DelegateWork(operation_id=operation(), objective="Review the deployment guide", success_criteria="Explain the verified gaps", **kwargs))


def command(record, action, **kwargs):
    return WorkCommand(operation_id=operation(), expected_revision=record["revision"], expected_assignment_revision=record["assignment_revision"], action=action, **kwargs)


@pytest.mark.asyncio
async def test_delegation_is_idempotent_attributed_and_records_only(work):
    service, instance, sf = work
    body = DelegateWork(operation_id=operation(), objective="Review guide", success_criteria="List gaps")
    first = await service.delegate(actor=ALICE, instance_id=instance.id, request=body)
    assert await service.delegate(actor=ALICE, instance_id=instance.id, request=body) == first
    assert first["status"] == "open" and first["review_required"] is True
    assert first["assignment_revision"] == first["revision"] == 1
    assert first["creator_id"] == "alice" and first["execution_available"] is True
    assert first["attempt"] is None
    history = await service.history(actor=BOB, instance_id=instance.id, work_id=first["id"])
    assert len(history) == 1 and history[0]["actor_id"] == "alice"
    with pytest.raises(AgentConflict):
        await service.delegate(actor=ALICE, instance_id=instance.id, request=body.model_copy(update={"objective": "Different"}))


@pytest.mark.asyncio
async def test_current_grants_filter_work_before_pagination_and_retry(work):
    service, instance, sf = work
    first = await delegate(service, instance)
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (instance.id, "bob"))).permissions = 1
    assert await service.list(actor=BOB, limit=1) == []
    with pytest.raises(AgentDenied):
        await service.get(actor=BOB, instance_id=instance.id, work_id=first["id"])
    with pytest.raises(AgentDenied):
        await service.delegate(actor=BOB, instance_id=instance.id, request=DelegateWork(operation_id=operation(), objective="Hidden", success_criteria="Hidden"))


@pytest.mark.asyncio
async def test_assignment_and_row_revisions_fence_edits_and_exact_retries(work):
    service, instance, sf = work
    first = await delegate(service, instance)
    edit = command(first, "edit", objective="Review the revised deployment guide")
    changed = await service.command(actor=BOB, instance_id=instance.id, work_id=first["id"], request=edit)
    assert changed["assignment_revision"] == changed["revision"] == 2
    assert await service.command(actor=BOB, instance_id=instance.id, work_id=first["id"], request=edit) == changed
    with pytest.raises(AgentConflict):
        await service.command(actor=BOB, instance_id=instance.id, work_id=first["id"], request=command(first, "cancel"))
    cancelled = await service.command(actor=BOB, instance_id=instance.id, work_id=first["id"], request=command(changed, "cancel"))
    assert cancelled["status"] == "cancelled"
    with pytest.raises(AgentConflict):
        await service.command(actor=BOB, instance_id=instance.id, work_id=first["id"], request=command(cancelled, "edit", objective="Changed behind completion"))
    reopened = await service.command(actor=BOB, instance_id=instance.id, work_id=first["id"], request=command(cancelled, "reopen", note="Continue after review"))
    assert reopened["status"] == "open" and reopened["assignment_revision"] == 4


@pytest.mark.asyncio
async def test_disabled_policy_preserves_inspect_history_and_management(work):
    from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceRow

    service, instance, sf = work
    first = await delegate(service, instance)
    disabled = definition()
    async with sf() as session, session.begin():
        session.add(AgentDefinitionRevisionRow(revision=disabled.revision, owner_id=disabled.owner_id, config=disabled.config, soul=disabled.soul))
        await session.flush()
        (await session.get(AgentInstanceRow, instance.id)).definition_revision = disabled.revision
    with pytest.raises(AgentConflict):
        await delegate(service, instance)
    current = await service.get(actor=BOB, instance_id=instance.id, work_id=first["id"])
    assert current["needs_mandate_reconciliation"] is True
    changed = await service.command(actor=BOB, instance_id=instance.id, work_id=first["id"], request=command(current, "cancel"))
    assert changed["status"] == "cancelled"
    assert len(await service.history(actor=BOB, instance_id=instance.id, work_id=first["id"])) == 2


@pytest.mark.asyncio
async def test_sources_require_shared_audience_and_are_redacted_after_revocation(work):
    from deerflow.agent_instances.work_contract import WorkSource
    from deerflow.persistence.spaces.model import SpaceGrantRow
    from deerflow.spaces.contract import Custody, MutationMode

    service, instance, sf = work
    private = await service.agents.files.create(actor=ALICE, name="Private source", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
    with pytest.raises(AgentDenied):
        await delegate(service, instance, sources=[WorkSource(space_id=private.id, path="private.txt")])
    ref = WorkSource(space_id=instance.home_id, path="docs/guide.txt")
    request = DelegateWork(operation_id=operation(), objective="Review guide", success_criteria="List gaps", sources=[ref])
    first = await service.delegate(actor=ALICE, instance_id=instance.id, request=request)
    assert first["sources"][0]["current_contents"] == "not_checked"
    async with sf() as session, session.begin():
        await session.delete(await session.get(SpaceGrantRow, (instance.home_id, "human", "alice")))
    retry = await service.delegate(actor=ALICE, instance_id=instance.id, request=request)
    assert retry["id"] == first["id"] and retry["sources"] == [{"kind": "unavailable", "current_contents": "not_checked"}]
    history = await service.history(actor=ALICE, instance_id=instance.id, work_id=first["id"])
    assert history[0]["record"]["sources"] == retry["sources"]
    changed = await service.command(actor=ALICE, instance_id=instance.id, work_id=first["id"], request=command(first, "cancel"))
    assert changed["status"] == "cancelled"


@pytest.mark.asyncio
async def test_defaults_and_current_authority_hold_on_retry(work):
    service, instance, sf = work
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (instance.id, "bob"))).permissions = 3
    body = DelegateWork(operation_id=operation(), objective="Colleague request", success_criteria="Explain result")
    first = await service.delegate(actor=BOB, instance_id=instance.id, request=body)
    with pytest.raises(AgentDenied):
        await service.delegate(actor=BOB, instance_id=instance.id, request=body.model_copy(update={"operation_id": operation(), "priority": "urgent"}))
    with pytest.raises(AgentDenied):
        await service.command(actor=BOB, instance_id=instance.id, work_id=first["id"], request=command(first, "cancel"))
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (instance.id, "bob"))).permissions = 2
    with pytest.raises(AgentDenied):
        await service.delegate(actor=BOB, instance_id=instance.id, request=body)
    assert (await service.get(actor=BOB, instance_id=instance.id, work_id=first["id"]))["id"] == first["id"]


@pytest.mark.asyncio
async def test_concurrent_delegation_and_edits_have_one_effect(work):
    import asyncio

    from sqlalchemy import func, select

    service, instance, sf = work
    body = DelegateWork(operation_id=operation(), objective="Once", success_criteria="One record")
    one, two = await asyncio.gather(*(service.delegate(actor=ALICE, instance_id=instance.id, request=body) for _ in range(2)))
    assert one == two
    changes = [command(one, "edit", objective=value) for value in ("First change", "Second change")]
    results = await asyncio.gather(*(service.command(actor=BOB, instance_id=instance.id, work_id=one["id"], request=c) for c in changes), return_exceptions=True)
    assert sum(isinstance(r, AgentConflict) for r in results) == 1
    async with sf() as session:
        assert await session.scalar(select(func.count()).select_from(AgentWorkRow)) == 1
        assert await session.scalar(select(func.count()).select_from(AgentWorkEventRow)) == 2
        assert await session.scalar(select(func.count()).select_from(AgentWorkAttemptRow)) == 0


async def submit_fixture(sf, record):
    """Seed a future host report directly; no public submission shortcut exists."""
    outcome_id = operation()
    attempt_id = operation()
    async with sf() as session, session.begin():
        row = await session.get(AgentWorkRow, record["id"])
        session.add(
            AgentWorkAttemptRow(
                id=attempt_id, work_id=row.id, activation_id=operation(), requester_id="alice", assignment_revision=row.assignment_revision, instance_generation=1, definition_revision=row.definition_revision, status="succeeded", request={}
            )
        )
        await session.flush()
        row.status, row.review_state = "submitted", "pending"
        row.outcome_id = outcome_id
        row.outcome_assignment_revision = row.assignment_revision
        row.outcome = {"id": outcome_id, "attempt_id": attempt_id, "assignment_revision": row.assignment_revision, "statement": "Documentation checked", "evidence_revision": 1, "sources": record["sources"]}
    return outcome_id


@pytest.mark.asyncio
async def test_acceptance_binds_exact_outcome_and_remains_statement_only(work):
    service, instance, sf = work
    first = await delegate(service, instance)
    outcome = await submit_fixture(sf, first)
    with pytest.raises(AgentConflict):
        await service.command(actor=BOB, instance_id=instance.id, work_id=first["id"], request=command(first, "edit", objective="Change submitted assignment"))
    for wrong in ({"outcome_id": operation()}, {"evidence_revision": 2}):
        args = {"outcome_id": outcome, "evidence_revision": 1, "basis": "outcome_statement", "acknowledge_unchecked_sources": True, **wrong}
        with pytest.raises(AgentConflict):
            await service.command(actor=BOB, instance_id=instance.id, work_id=first["id"], request=command(first, "accept", **args))
    accepted = await service.command(actor=BOB, instance_id=instance.id, work_id=first["id"], request=command(first, "accept", outcome_id=outcome, evidence_revision=1, basis="outcome_statement", acknowledge_unchecked_sources=True))
    assert accepted["status"] == "completed" and accepted["review"]["actor_id"] == "bob"
    assert accepted["review"]["assignment_revision"] == 1
    assert accepted["review"]["current_contents"] == "not_checked"
    reopened = await service.command(actor=BOB, instance_id=instance.id, work_id=first["id"], request=command(accepted, "reopen", note="Recheck the guide"))
    assert reopened["outcome"] is None and reopened["review"] is None
    history = await service.history(actor=BOB, instance_id=instance.id, work_id=first["id"])
    assert history[1]["record"]["review"]["outcome_id"] == outcome


@pytest.mark.asyncio
async def test_factual_input_does_not_become_decision_or_clear_blocker(work):
    service, instance, sf = work
    first = await delegate(service, instance)
    blocker_id = operation()
    async with sf() as session, session.begin():
        row = await session.get(AgentWorkRow, first["id"])
        row.status = "blocked"
        row.blocker = {"id": blocker_id, "revision": 1, "assignment_revision": 1, "kind": "decision", "question": "Which document is the priority?", "sources": []}
        (await session.get(AgentInstanceGrantRow, (instance.id, "bob"))).permissions = 3
    args = {"blocker_id": blocker_id, "blocker_revision": 1, "note": "The guide was updated."}
    given = await service.command(actor=BOB, instance_id=instance.id, work_id=first["id"], request=command(first, "input", **args))
    assert given["status"] == "blocked" and given["assignment_revision"] == 1 and given["revision"] == 2
    with pytest.raises(AgentDenied):
        await service.command(actor=BOB, instance_id=instance.id, work_id=first["id"], request=command(given, "decide", **args))
    with pytest.raises(AgentConflict):
        await service.command(actor=ALICE, instance_id=instance.id, work_id=first["id"], request=command(given, "decide", **{**args, "blocker_revision": 2}))
    decided = await service.command(actor=ALICE, instance_id=instance.id, work_id=first["id"], request=command(given, "decide", **args))
    assert decided["status"] == "open" and decided["assignment_revision"] == 1 and decided["review_required"]
    assert decided["blocker"]["resolution"]["actor_id"] == "alice" and decided["attempt"] is None


@pytest.mark.asyncio
async def test_database_rejects_invalid_review_pairs_and_unresolved_attempts(work):
    from sqlalchemy.exc import IntegrityError

    service, instance, sf = work
    first = await delegate(service, instance)
    invalid = [
        {"status": "submitted", "review_state": "pending", "outcome_id": operation()},
        {"status": "submitted", "review_state": "pending", "outcome_id": operation(), "outcome_assignment_revision": 1},
        {"status": "completed", "review_state": "accepted", "outcome_id": operation(), "outcome_assignment_revision": 1, "outcome": {}},
        {"outcome": {"stale": True}},
        {"review": {"stale": True}},
        {"status": "submitted", "review_required": False},
    ]
    for fields in invalid:
        with pytest.raises(IntegrityError):
            async with sf() as session, session.begin():
                row = await session.get(AgentWorkRow, first["id"])
                for key, value in fields.items():
                    setattr(row, key, value)
                await session.flush()

    def attempt():
        return AgentWorkAttemptRow(
            id=operation(),
            work_id=first["id"],
            activation_id=operation(),
            requester_id="alice",
            assignment_revision=1,
            instance_generation=instance.generation,
            definition_revision=instance.definition_revision,
            status="uncertain",
            request={},
        )

    async with sf() as session, session.begin():
        session.add(attempt())
    with pytest.raises(IntegrityError):
        async with sf() as session, session.begin():
            session.add(attempt())
            await session.flush()
    with pytest.raises(AgentConflict):
        await service.command(actor=BOB, instance_id=instance.id, work_id=first["id"], request=command(first, "reopen", note="Cannot replace unresolved execution"))


@pytest.mark.asyncio
async def test_work_migration_rebuild_and_used_downgrade_guard(work):
    import importlib

    from alembic.operations import Operations
    from alembic.runtime.migration import MigrationContext
    from sqlalchemy import inspect

    service, instance, sf = work
    module = importlib.import_module("deerflow.persistence.migrations.versions.0038_agent_work")

    def rebuild(connection):
        def columns():
            inspector = inspect(connection)
            return {table.name: sorted((c["name"], str(c["type"]), c["nullable"]) for c in inspector.get_columns(table.name)) for table in module._tables}

        before = columns()
        with Operations.context(MigrationContext.configure(connection)):
            importlib.import_module("deerflow.persistence.migrations.versions.0040_work_execution").downgrade()
        for row in (HumanInputReadRow, HumanInputEventRow, HumanInputResponseRow, HumanInputRequestRow):
            row.__table__.drop(connection)
        with Operations.context(MigrationContext.configure(connection)):
            module.upgrade()
            module.downgrade()
            module.upgrade()
            module.upgrade()
            importlib.import_module("deerflow.persistence.migrations.versions.0039_human_input").upgrade()
            importlib.import_module("deerflow.persistence.migrations.versions.0040_work_execution").upgrade()
        assert columns() == before

    async with sf.kw["bind"].begin() as connection:
        await connection.run_sync(rebuild)
    await delegate(service, instance)

    def refuse(connection):
        with Operations.context(MigrationContext.configure(connection)), pytest.raises(RuntimeError, match="Refusing to erase"):
            module.downgrade()

    async with sf.kw["bind"].begin() as connection:
        await connection.run_sync(refuse)


@pytest.mark.asyncio
async def test_http_work_ceiling_visibility_and_no_execution_shortcuts(work, monkeypatch):
    from types import SimpleNamespace

    import httpx
    from test_agent_instances import request_app

    from app.gateway.routers import agent_instances as routes
    from app.gateway.routers.agent_work import router

    service, instance, sf = work
    monkeypatch.setattr(routes, "get_agents_api_config", lambda: SimpleNamespace(enabled=True))
    body = {"operation_id": operation(), "objective": "Review", "success_criteria": "Explain"}
    for kwargs, status in [({"permissions": ("agents:read",)}, 403), ({"permissions": ("agents:write",)}, 403), ({"source": "pat"}, 403), ({"actor": None}, 401), ({}, 201)]:
        app = request_app(service.agents, **kwargs)
        app.include_router(router)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            result = await client.post(f"/api/agent-instances/{instance.id}/work", json=body)
            assert result.status_code == status, result.text
            if status == 201:
                record = result.json()
                for shortcut in ("run", "resume", "outcome"):
                    assert (await client.post(f"/api/agent-instances/{instance.id}/work/{record['id']}/{shortcut}", json={})).status_code == 404
                for forged in ({"action": "complete"}, {"status": "completed"}, {"actor_id": "bob"}):
                    response = await client.post(
                        f"/api/agent-instances/{instance.id}/work/{record['id']}/commands", json={"operation_id": operation(), "expected_revision": 1, "expected_assignment_revision": 1, "action": "cancel", **forged}
                    )
                    assert response.status_code == 422
                assert (await client.get(f"/api/agent-instances/{instance.id}/work")).json()["execution_available"] is True


@pytest.mark.asyncio
async def test_assignment_edits_supersede_exact_blockers_and_keep_history(work):
    service, instance, sf = work
    first = await delegate(service, instance)
    blocker_id = operation()
    async with sf() as session, session.begin():
        row = await session.get(AgentWorkRow, first["id"])
        row.status, row.revision = "blocked", 2
        row.blocker = {"id": blocker_id, "revision": 1, "assignment_revision": 1, "kind": "information", "question": "Which guide?", "sources": []}
        await session.flush()
        session.add(
            AgentWorkEventRow(
                id=operation(),
                instance_id=instance.id,
                work_id=row.id,
                actor_id=instance.principal.subject_id,
                actor_kind="nonhuman",
                operation_id=operation(),
                action="blocked",
                revision=2,
                assignment_revision=1,
                request={},
                result=service._snapshot(row),
            )
        )
    current = await service.get(actor=BOB, instance_id=instance.id, work_id=first["id"])
    changed = await service.command(actor=BOB, instance_id=instance.id, work_id=first["id"], request=command(current, "edit", objective="A different objective"))
    assert changed["status"] == "open" and changed["blocker"] is None and changed["assignment_revision"] == 2
    with pytest.raises(AgentConflict):
        await service.command(actor=BOB, instance_id=instance.id, work_id=first["id"], request=command(changed, "input", blocker_id=blocker_id, blocker_revision=1, note="Old question reply"))
    history = await service.history(actor=BOB, instance_id=instance.id, work_id=first["id"])
    assert history[1]["record"]["blocker"]["question"] == "Which guide?"


@pytest.mark.asyncio
async def test_malformed_outcome_bindings_cannot_be_reviewed_and_history_has_no_new_attempt(work):
    from copy import deepcopy

    service, instance, sf = work
    first = await delegate(service, instance)
    outcome_id = await submit_fixture(sf, first)
    current = await service.get(actor=BOB, instance_id=instance.id, work_id=first["id"])
    assert current["attempt"] is not None
    history = await service.history(actor=BOB, instance_id=instance.id, work_id=first["id"])
    assert history[0]["record"]["attempt"] is None
    async with sf() as session:
        original = deepcopy((await session.get(AgentWorkRow, first["id"])).outcome)
    for invalid in ({"id": operation()}, {"attempt_id": operation()}, {"assignment_revision": 2}, {"statement": "x" * 4097}, {"sources": [{"kind": "private_chat", "thread_id": "private"}]}):
        async with sf() as session, session.begin():
            (await session.get(AgentWorkRow, first["id"])).outcome = {**original, **invalid}
        with pytest.raises(AgentConflict):
            await service.command(actor=BOB, instance_id=instance.id, work_id=first["id"], request=command(first, "accept", outcome_id=outcome_id, evidence_revision=1, basis="outcome_statement", acknowledge_unchecked_sources=True))
    async with sf() as session, session.begin():
        (await session.get(AgentWorkRow, first["id"])).outcome = original


@pytest.mark.asyncio
async def test_decision_locks_persisted_sources_before_instance_and_rechecks_revocation(work, monkeypatch):
    import asyncio

    from deerflow.spaces.contract import Permission

    service, instance, sf = work
    first = await delegate(service, instance)
    blocker_id = operation()
    source = {"kind": "space_file", "space_id": instance.home_id, "path": "guide.txt"}
    async with sf() as session, session.begin():
        row = await session.get(AgentWorkRow, first["id"])
        row.status = "blocked"
        row.blocker = {"id": blocker_id, "revision": 1, "assignment_revision": 1, "kind": "decision", "question": "Accept the selected guide?", "sources": [source]}
    captured, resume = asyncio.Event(), asyncio.Event()
    original = service._lock_sources

    async def paused(session, sources):
        assert [s.model_dump() for s in sources] == [source]
        captured.set()
        await resume.wait()
        await original(session, sources)

    monkeypatch.setattr(service, "_lock_sources", paused)
    action = asyncio.create_task(service.command(actor=BOB, instance_id=instance.id, work_id=first["id"], request=command(first, "decide", blocker_id=blocker_id, blocker_revision=1, note="Use the guide")))
    try:
        # SQLite reserves its writer before resource locks, so revoke through
        # the real registry before starting command admission on that backend.
        await asyncio.wait_for(captured.wait(), timeout=5)
        if sf.kw["bind"].dialect.name == "sqlite":
            resume.set()
            await action
        else:
            home = await service.agents.files.registry.get(actor=ALICE, space_id=instance.home_id)
            await service.agents.files.registry.set_grant(actor=ALICE, space_id=home.id, expected_generation=home.generation, subject=BOB, permissions=Permission(0))
            resume.set()
            with pytest.raises(AgentDenied):
                await action
    finally:
        resume.set()
        await asyncio.gather(action, return_exceptions=True)
