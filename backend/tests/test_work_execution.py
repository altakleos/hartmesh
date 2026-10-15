"""Explicit Work attempts on real SQL; native effects are qualified separately."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
import pytest_asyncio
from _storage_spaces_test_support import ALICE, BOB, operation
from test_agent_instances import instances as instances
from test_agent_work_records import command, delegate
from test_agent_work_records import work as work

from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.agent_instances.conversations import AgentConversations
from deerflow.agent_instances.work_execution import WorkExecution
from deerflow.agent_instances.work_execution_contract import ActivateWork, ReportWork
from deerflow.persistence.agent_instances.model import AgentConversationRow, AgentInstanceGrantRow, AgentProtectedContextRow
from deerflow.persistence.thread_meta.model import ThreadMetaRow


@pytest_asyncio.fixture
async def execution(work):
    service, instance, sf = work
    async with sf.kw["bind"].begin() as c:
        for model in (ThreadMetaRow, AgentConversationRow, AgentProtectedContextRow):
            await c.run_sync(model.__table__.create)
    authority = AgentConversations(service.agents)
    chat = await authority.create(actor=ALICE, instance_id=instance.id, creation_id=operation())
    admitted = await authority.execution(actor=ALICE, thread_id=chat["thread_id"])
    row = await delegate(service, instance)
    return WorkExecution(service), admitted, row, sf


def activation(execution, row, **changes):
    return ActivateWork(operation_id=operation(), expected_revision=row["revision"], expected_assignment_revision=row["assignment_revision"], thread_id=execution.thread_id, **changes)


def run_record(admitted, scope, *, status="success"):
    return SimpleNamespace(
        run_id=operation(),
        thread_id=admitted.thread_id,
        user_id="alice",
        status=status,
        ownership_lost=False,
        idempotency_key="agent-work:" + scope.attempt_id,
        metadata={"agent_work_id": scope.work_id, "agent_work_attempt_id": scope.attempt_id},
    )


@pytest.mark.asyncio
async def test_reservation_is_exact_and_prevents_concurrent_activation(execution):
    service, admitted, row, _ = execution
    body = activation(admitted, row)
    first = await service.reserve(execution=admitted, work_id=row["id"], request=body)
    same = await service.reserve(execution=admitted, work_id=row["id"], request=body)
    assert first.attempt_id == same.attempt_id
    with pytest.raises(AgentConflict):
        await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    with pytest.raises(AgentConflict):
        await service.reserve(execution=admitted, work_id=row["id"], request=body.model_copy(update={"expected_revision": 8}))
    assert (await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"]))["attempt"]["status"] == "starting"


@pytest.mark.asyncio
async def test_new_attempt_capacity_refusal_writes_no_attempt_or_revision(execution):
    from deerflow.agent_instances.contract import ResidentWorkLimit

    service, admitted, row, _sf = execution

    def refuse(_attempt_id):
        raise ResidentWorkLimit(admitted.instance.name)

    with pytest.raises(ResidentWorkLimit):
        await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row), before_admit=refuse)
    current = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    assert current["attempt"] is None and current["revision"] == row["revision"]


@pytest.mark.asyncio
async def test_exact_activation_replay_skips_new_capacity_and_keeps_payload_checks(execution):
    service, admitted, row, _sf = execution
    body = activation(admitted, row)
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=body)

    def refuse(_attempt_id):
        raise AssertionError("A replay does not create another attempt")

    same = await service.reserve(execution=admitted, work_id=row["id"], request=body, before_admit=refuse)
    assert same.attempt_id == scope.attempt_id
    with pytest.raises(AgentConflict):
        await service.reserve(execution=admitted, work_id=row["id"], request=body.model_copy(update={"expected_revision": 8}), before_admit=refuse)


@pytest.mark.asyncio
async def test_cancelled_attempt_commit_retains_the_exact_admitted_identity(execution, monkeypatch):
    import asyncio

    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    service, admitted, row, sf = execution
    entered, release = asyncio.Event(), asyncio.Event()
    admitted_ids = []
    original = service.work._event

    async def event(*args, **kwargs):
        entered.set()
        await release.wait()
        await original(*args, **kwargs)

    monkeypatch.setattr(service.work, "_event", event)
    task = asyncio.create_task(service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row), before_admit=admitted_ids.append))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert len(admitted_ids) == 1
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        async with sf() as session:
            saved = await session.get(AgentWorkAttemptRow, admitted_ids[0])
            assert saved is not None and saved.status == "starting"
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_host_settlement_rechecks_live_attempt_after_work_lock_wait(execution, monkeypatch):
    import asyncio

    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    service, admitted, row, sf = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    entered, release = asyncio.Event(), asyncio.Event()
    live = set()
    original = service.work._row

    async def locked(*args, **kwargs):
        result = await original(*args, **kwargs)
        if kwargs.get("lock"):
            entered.set()
            await release.wait()
        return result

    monkeypatch.setattr(service.work, "_row", locked)
    options = dict(instance_id=admitted.instance.id, host_id="qualified-test-host", reason="start-failed", attachment_id=None, unstarted_after=0, live_run_ids=frozenset(), live_attempts=lambda: frozenset(live))
    task = asyncio.create_task(service.settle_contained(**options))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        live.add(scope.attempt_id)
        release.set()
        assert await task == []
        async with sf() as session:
            assert (await session.get(AgentWorkAttemptRow, scope.attempt_id)).status == "starting"
        live.clear()
        assert await service.settle_contained(**options) == [scope.attempt_id]
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_work_context_marks_history_protected_even_without_memory(execution):
    service, admitted, row, sf = execution
    await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    async with sf() as s, s.begin():
        assert await s.get(AgentProtectedContextRow, admitted.thread_id) is not None
        (await s.get(AgentInstanceGrantRow, (admitted.instance.id, "alice"))).permissions = 1
    from deerflow.agent_instances.contract import AgentPermission

    assert not await admitted.authority.allowed(actor=ALICE, thread_id=admitted.thread_id, permission=AgentPermission.INSPECT)


@pytest.mark.asyncio
async def test_report_is_candidate_until_exact_run_settles(execution):
    service, admitted, row, _ = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    admitted = replace(admitted, work=scope)
    record = run_record(admitted, scope, status="success")
    await scope.bind(admitted, record, admitted.thread_incarnation)
    current = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    await scope.report(admitted, ReportWork(operation_id=operation(), expected_revision=current["revision"], action="outcome", statement="The deployment guide gaps are documented."))
    assert (await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"]))["status"] == "open"
    await scope.finish(admitted, record, settled=True)
    result = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    assert result["status"] == "submitted" and result["review_state"] == "pending"
    assert result["human_input_request_id"]
    await scope.finish(admitted, record, settled=True)
    assert (await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"]))["revision"] == result["revision"]
    with pytest.raises(AgentDenied):
        await scope.report(admitted, ReportWork(operation_id=operation(), expected_revision=result["revision"], action="progress", statement="late"))


@pytest.mark.asyncio
async def test_success_without_report_does_not_complete_work(execution):
    service, admitted, row, _ = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    admitted = replace(admitted, work=scope)
    record = run_record(admitted, scope, status="success")
    await scope.bind(admitted, record, admitted.thread_incarnation)
    await scope.finish(admitted, record, settled=True)
    assert (await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"]))["status"] == "open"


async def _running_stop_attempt(execution):
    service, admitted, row, sf = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    admitted = replace(admitted, work=scope)
    record = run_record(admitted, scope)
    await scope.bind(admitted, record, admitted.thread_incarnation)
    current = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    return service, admitted, current, sf, scope, record


@pytest.mark.asyncio
@pytest.mark.parametrize("is_attempt,internal", [(True, False), (True, True), (False, False)])
async def test_a_work_attempt_run_carries_the_autonomous_interaction_mode(execution, monkeypatch, is_attempt, internal):
    from unittest.mock import AsyncMock

    from test_gateway_services import _make_start_run_persistence_context

    from app.gateway import services
    from app.gateway.authz import AuthContext
    from app.gateway.run_models import RunCreateRequest
    from deerflow.config.app_config import AppConfig, reset_app_config, set_app_config
    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
    from deerflow.runtime.user_context import reset_current_user, set_current_user

    service, admitted, row, sf = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row)) if is_attempt else None
    request, _, _ = _make_start_run_persistence_context()
    request.app.state.thread_store = ThreadMetaRepository(sf, instance_authority=admitted.authority)
    request.app.state.agent_conversations = admitted.authority
    user = SimpleNamespace(id="alice", system_role="user", role="user")
    request.state.user = user
    request.state.auth = AuthContext(user, ["runs:create", "runs:read"])
    request.state.auth_source = "internal" if internal else "session"
    if internal:
        user.system_role = "internal"
        request.headers["X-DeerFlow-Owner-User-Id"] = "alice"
    request.url = "http://test/api/threads/" + admitted.thread_id + "/runs"
    runner = AsyncMock()
    monkeypatch.setattr(services, "run_agent", runner)
    monkeypatch.setattr(services, "resolve_trusted_internal_owner_for_attribution", AsyncMock(return_value=user))
    monkeypatch.setattr(services, "resolve_agent_factory", lambda *_args: lambda **_kwargs: None)
    config = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}})
    set_app_config(config)
    token = set_current_user(user)
    try:
        body = RunCreateRequest(
            input={"messages": [{"role": "user", "content": "Continue"}]}, config={"configurable": {"interaction_mode": "interactive"}, "context": {"interaction_mode": "interactive"}}, context={"interaction_mode": "interactive"}
        )
        record = await services.start_run(body, admitted.thread_id, request, work_attempt=scope)
        await record.task
        runner.assert_awaited_once()
        actual = runner.await_args.kwargs["config"]
        for section in ("configurable", "context"):
            assert actual[section].get("interaction_mode") == ("autonomous" if is_attempt else None)
    finally:
        await request.app.state.run_manager.shutdown()
        reset_current_user(token)
        reset_app_config()


def test_stop_command_names_one_attempt_and_accepts_only_its_fields():
    from pydantic import ValidationError

    record = {"revision": 1, "assignment_revision": 1}
    attempt_id = operation()
    assert command(record, "stop", attempt_id=attempt_id).attempt_id == attempt_id
    assert command(record, "stop", attempt_id=attempt_id, note="Wrong source").note == "Wrong source"
    with pytest.raises(ValidationError, match="Stop names the exact attempt"):
        command(record, "stop")
    for extra in ({"objective": "Other work"}, {"outcome_id": operation()}, {"containment_operation_id": operation()}, {"attempt_id": None}):
        with pytest.raises(ValidationError):
            command(record, "stop", **({"attempt_id": attempt_id} | extra))


@pytest.mark.asyncio
@pytest.mark.parametrize("actor,permissions,allowed", [(ALICE, 3, True), (BOB, 3, False), (BOB, 6, True), (BOB, 7, True), (ALICE, 2, False), (BOB, 4, False), (ALICE, 1, False)])
async def test_stop_is_admitted_to_the_requester_and_to_managers_only(execution, actor, permissions, allowed):
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkRow

    service, admitted, current, sf, scope, _record = await _running_stop_attempt(execution)
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (admitted.instance.id, actor.subject_id))).permissions = permissions
    body = command(current, "stop", attempt_id=scope.attempt_id)
    if allowed:
        result = await service.work.command(actor=actor, instance_id=admitted.instance.id, work_id=current["id"], request=body, can_stop=True)
        assert result["attempt"]["status"] == "stopping"
        assert result["revision"] == current["revision"] + 1
        assert result["assignment_revision"] == current["assignment_revision"] and result["status"] == current["status"]
        assert result["next_action"] == current["next_action"]
    else:
        with pytest.raises(AgentDenied):
            await service.work.command(actor=actor, instance_id=admitted.instance.id, work_id=current["id"], request=body, can_stop=True)
        async with sf() as session:
            assert (await session.get(AgentWorkAttemptRow, scope.attempt_id)).status == "running"
            assert (await session.get(AgentWorkRow, current["id"])).revision == current["revision"]


@pytest.mark.asyncio
async def test_stop_needs_runs_cancel(execution):
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkRow

    service, admitted, current, sf, scope, _record = await _running_stop_attempt(execution)
    with pytest.raises(AgentDenied, match="cancellation capability"):
        await service.work.command(actor=ALICE, instance_id=admitted.instance.id, work_id=current["id"], request=command(current, "stop", attempt_id=scope.attempt_id), can_stop=False)
    async with sf() as session:
        assert (await session.get(AgentWorkAttemptRow, scope.attempt_id)).status == "running"
        assert (await session.get(AgentWorkRow, current["id"])).revision == current["revision"]


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["none", "wrong", "stopping", "uncertain", "succeeded", "failed", "cancelled"])
async def test_stop_of_a_settled_attempt_is_refused(execution, phase):
    from deerflow.agent_instances.contract import Refusal
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkRow

    service, admitted, current, sf = execution
    attempt_id = operation()
    if phase != "none":
        service, admitted, current, sf, scope, _record = await _running_stop_attempt(execution)
        if phase != "wrong":
            attempt_id = scope.attempt_id
            async with sf() as session, session.begin():
                (await session.get(AgentWorkAttemptRow, attempt_id)).status = phase
    with pytest.raises(Refusal) as error:
        await service.work.command(actor=ALICE, instance_id=admitted.instance.id, work_id=current["id"], request=command(current, "stop", attempt_id=attempt_id), can_stop=True)
    assert error.value.reason == "no-running-attempt" and error.value.status == 409
    assert str(error.value) == "There is no running attempt to stop"
    async with sf() as session:
        assert (await session.get(AgentWorkRow, current["id"])).revision == current["revision"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status,expected", [("error", "cancelled"), ("timeout", "cancelled"), ("interrupted", "cancelled"), ("success", "succeeded")])
@pytest.mark.parametrize("settled", [True, False])
async def test_stop_leaves_the_assignment_open_and_settles_cancelled(execution, status, expected, settled):
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    service, admitted, current, sf, scope, record = await _running_stop_attempt(execution)
    async with sf() as session, session.begin():
        (await session.get(AgentWorkAttemptRow, scope.attempt_id)).status = "stopping"
    record.status = status
    assert await scope.finish(admitted, record, settled=settled) == (expected if settled else "uncertain")
    result = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=current["id"])
    assert result["status"] == "open" and result["assignment_revision"] == current["assignment_revision"]
    assert result["objective"] == current["objective"] and result["next_action"] == current["next_action"]
    assert result["outcome"] is None


@pytest.mark.asyncio
async def test_stop_sets_stopping_and_cancels_the_exact_run(execution, monkeypatch):
    from unittest.mock import AsyncMock

    import httpx
    from test_agent_instances import request_app

    from app.gateway.refusals import refusal_handler
    from app.gateway.routers import agent_instances as routes
    from app.gateway.routers.agent_work import router
    from deerflow.agent_instances.contract import Refusal

    service, admitted, current, _sf, scope, record = await _running_stop_attempt(execution)
    manager = SimpleNamespace(get=AsyncMock(return_value=record), cancel=AsyncMock())
    app = request_app(service.agents, permissions=("agents:read", "agents:write", "runs:cancel"))
    app.state.run_manager = manager
    app.add_exception_handler(Refusal, refusal_handler)
    app.include_router(router)
    monkeypatch.setattr(routes, "get_agents_api_config", lambda: SimpleNamespace(enabled=True))
    body = {"operation_id": operation(), "expected_revision": current["revision"], "expected_assignment_revision": current["assignment_revision"], "action": "stop", "attempt_id": scope.attempt_id, "note": "Wrong source"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        path = f"/api/agent-instances/{admitted.instance.id}/work/{current['id']}/commands"
        response = await client.post(path, json=body)
        assert response.status_code == 200, response.text
        assert response.json()["attempt"]["status"] == "stopping"
        assert response.json()["status"] == "open" and response.json()["assignment_revision"] == current["assignment_revision"]
        replay = await client.post(path, json=body)
        assert replay.status_code == 200, replay.text
    assert manager.cancel.await_count == 2
    assert all(call.args == (record.run_id,) for call in manager.cancel.await_args_list)
    events = await service.work.history(actor=ALICE, instance_id=admitted.instance.id, work_id=current["id"])
    stops = [event for event in events if event["action"] == "stop"]
    assert len(stops) == 1 and stops[0]["actor_kind"] == "human" and stops[0]["actor_id"] == "alice"
    from sqlalchemy import select

    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    async with _sf() as session:
        persisted = await session.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.work_id == current["id"], AgentWorkEventRow.action == "stop"))
        assert persisted.request == body


@pytest.mark.asyncio
async def test_stop_http_observes_the_run_cancel_ceiling(execution, monkeypatch):
    from unittest.mock import AsyncMock

    import httpx
    from test_agent_instances import request_app

    from app.gateway.routers import agent_instances as routes
    from app.gateway.routers.agent_work import router
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    service, admitted, current, sf, scope, record = await _running_stop_attempt(execution)
    manager = SimpleNamespace(get=AsyncMock(return_value=record), cancel=AsyncMock())
    app = request_app(service.agents)
    app.state.run_manager = manager
    app.include_router(router)
    monkeypatch.setattr(routes, "get_agents_api_config", lambda: SimpleNamespace(enabled=True))
    body = {"operation_id": operation(), "expected_revision": current["revision"], "expected_assignment_revision": current["assignment_revision"], "action": "stop", "attempt_id": scope.attempt_id}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(f"/api/agent-instances/{admitted.instance.id}/work/{current['id']}/commands", json=body)
    assert response.status_code == 404, response.text
    manager.cancel.assert_not_awaited()
    async with sf() as session:
        assert (await session.get(AgentWorkAttemptRow, scope.attempt_id)).status == "running"


@pytest.mark.asyncio
@pytest.mark.parametrize("surface", ["work", "plugin"])
async def test_stop_receipt_cannot_cancel_a_newer_attempt(execution, monkeypatch, surface):
    from unittest.mock import AsyncMock

    import httpx
    from test_agent_instances import request_app

    from app.gateway.routers import agent_instances as routes
    from app.gateway.routers.agent_work import router

    service, admitted, current, sf, scope, record = await _running_stop_attempt(execution)
    body = command(current, "stop", attempt_id=scope.attempt_id)
    await service.work.command(actor=ALICE, instance_id=admitted.instance.id, work_id=current["id"], request=body, can_stop=True)
    record.status = "interrupted"
    await scope.finish(admitted, record, settled=True)
    current = await service.work.get(actor=BOB, instance_id=admitted.instance.id, work_id=current["id"])
    second_execution = await admitted.authority.execution(actor=BOB, thread_id=admitted.thread_id)
    second_scope = await service.reserve(execution=second_execution, work_id=current["id"], request=activation(second_execution, current))
    second_execution = replace(second_execution, work=second_scope)
    second_record = run_record(second_execution, second_scope)
    second_record.user_id = "bob"
    await second_scope.bind(second_execution, second_record, second_execution.thread_incarnation)
    current = await service.work.get(actor=BOB, instance_id=admitted.instance.id, work_id=current["id"])
    await service.work.command(actor=BOB, instance_id=admitted.instance.id, work_id=current["id"], request=command(current, "stop", attempt_id=second_scope.attempt_id), can_stop=True)
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (admitted.instance.id, "alice"))).permissions = 3
    manager = SimpleNamespace(get=AsyncMock(return_value=second_record), cancel=AsyncMock())
    app = request_app(service.agents, permissions=("agents:read", "agents:write", "runs:cancel"))
    app.state.run_manager = manager
    app.include_router(router)
    payload = body.model_dump(mode="json", exclude_unset=True)
    path = f"/api/agent-instances/{admitted.instance.id}/work/{current['id']}/commands"
    if surface == "plugin":
        from deerflow_extension_api.auth import EXTENSION_PRINCIPAL_RESOLVER_KEY, ExtensionPrincipal
        from deerflow_extension_api.plugins import BackendAction, PluginContribution

        from app.gateway.routers.plugins import router as plugin_router
        from deerflow.extensions.registry import ExtensionRegistry

        async def replay(payload, context):
            return await context.human_input.call("work_command", {"instance_id": admitted.instance.id, "work_id": current["id"], "body": payload["command"]})

        plugin = PluginContribution(namespace="example.stop", title="Stop", enabled=True, api_version=5, human_input_api_version=1, backend=(BackendAction("stop", replay),))
        extensions = ExtensionRegistry()
        with extensions.attributed_to("test:stop"):
            extensions.plugin(plugin)
        app.state.extensions = extensions.build()
        setattr(app.state, EXTENSION_PRINCIPAL_RESOLVER_KEY, lambda _request: ExtensionPrincipal("alice"))
        app.include_router(plugin_router)
        path = "/api/plugins/example.stop/actions/stop"
        payload = {"command": payload}
    monkeypatch.setattr(routes, "get_agents_api_config", lambda: SimpleNamespace(enabled=True))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(path, json=payload)
    assert response.status_code == 200, response.text
    manager.get.assert_not_awaited()
    manager.cancel.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("actor,permissions,can_stop", [(ALICE, 2, True), (BOB, 3, True), (BOB, 7, False)])
async def test_stop_replay_rechecks_current_rights_and_cancellation_ceiling(execution, actor, permissions, can_stop):
    service, admitted, current, sf, scope, _record = await _running_stop_attempt(execution)
    body = command(current, "stop", attempt_id=scope.attempt_id)
    await service.work.command(actor=actor, instance_id=admitted.instance.id, work_id=current["id"], request=body, can_stop=True)
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (admitted.instance.id, actor.subject_id))).permissions = permissions
    with pytest.raises(AgentDenied):
        await service.work.command(actor=actor, instance_id=admitted.instance.id, work_id=current["id"], request=body, can_stop=can_stop)


@pytest.mark.asyncio
@pytest.mark.parametrize("status,expected", [("interrupted", "cancelled"), ("success", "succeeded")])
async def test_stop_receipt_replay_survives_terminal_settlement_without_another_stop_event(execution, status, expected):
    service, admitted, current, _sf, scope, record = await _running_stop_attempt(execution)
    body = command(current, "stop", attempt_id=scope.attempt_id)
    stopped = await service.work.command(actor=ALICE, instance_id=admitted.instance.id, work_id=current["id"], request=body, can_stop=True)
    record.status = status
    assert await scope.finish(admitted, record, settled=True) == expected
    replay = await service.work.command(actor=ALICE, instance_id=admitted.instance.id, work_id=current["id"], request=body, can_stop=True)
    assert replay["revision"] == stopped["revision"]
    events = await service.work.history(actor=ALICE, instance_id=admitted.instance.id, work_id=current["id"])
    assert sum(event["action"] == "stop" for event in events) == 1


@pytest.mark.asyncio
async def test_stop_does_not_replace_a_worker_first_success(execution):
    from deerflow.agent_instances.contract import Refusal

    service, admitted, current, _sf, scope, record = await _running_stop_attempt(execution)
    assert await scope.finish(admitted, record, settled=True) == "succeeded"
    completed = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=current["id"])
    with pytest.raises(Refusal) as error:
        await service.work.command(actor=ALICE, instance_id=admitted.instance.id, work_id=current["id"], request=command(completed, "stop", attempt_id=scope.attempt_id), can_stop=True)
    assert error.value.reason == "no-running-attempt"
    unchanged = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=current["id"])
    assert unchanged["attempt"]["status"] == "succeeded" and unchanged["revision"] == completed["revision"]


@pytest.mark.asyncio
@pytest.mark.parametrize("stop_before_bind", [False, True])
async def test_stop_through_run_cancel_settles_the_attempt_cancelled(execution, monkeypatch, stop_before_bind):
    import asyncio
    from unittest.mock import AsyncMock, patch

    from app.gateway.work_execution import dispatch_work_stop
    from deerflow.agent_instances.runtime import InstanceSandboxProvider
    from deerflow.community.aio_sandbox.aio_sandbox import AioSandbox
    from deerflow.config.app_config import AppConfig
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
    from deerflow.persistence.run.model import RunChangeClockRow, RunRow
    from deerflow.persistence.run.sql import RunRepository
    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.worker import RunContext, run_agent

    service, admitted, row, sf = execution
    async with sf.kw["bind"].begin() as connection:
        for model in (RunRow, RunChangeClockRow):
            await connection.run_sync(model.__table__.create)
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    admitted = replace(admitted, work=scope)
    manager = RunManager(store=RunRepository(sf))
    record = await manager.create_or_reject(admitted.thread_id, "lead_agent", user_id="alice", idempotency_key="agent-work:" + scope.attempt_id, metadata={"agent_work_id": row["id"], "agent_work_attempt_id": scope.attempt_id})
    with patch("deerflow.community.aio_sandbox.aio_sandbox.AioSandboxClient"):
        sandbox = AioSandbox(id="stop-worker-test", base_url="http://localhost:8080")
    provider = InstanceSandboxProvider(admitted, sandbox, app_config=AppConfig(sandbox={"use": "deerflow.sandbox.local.local_sandbox_provider:LocalSandboxProvider"}), skill_revision="test-public-capture")
    monkeypatch.setattr("deerflow.agent_instances.runtime.prepare_environment", AsyncMock(return_value=provider))
    entered = asyncio.Event()

    class Agent:
        async def astream(self, *args, **kwargs):
            entered.set()
            await asyncio.Event().wait()
            yield {"messages": []}

    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())
    if stop_before_bind:
        current = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
        assert current["attempt"]["status"] == "starting" and current["attempt"]["run_id"] is None
        await service.work.command(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], request=command(current, "stop", attempt_id=scope.attempt_id), can_stop=True)
    worker = asyncio.create_task(
        run_agent(bridge, manager, record, ctx=RunContext(checkpointer=None, event_store=None, agent_execution=admitted), agent_factory=lambda **kwargs: Agent(), graph_input={}, config={}, thread_incarnation=admitted.thread_incarnation)
    )
    record.task = worker
    try:
        if not stop_before_bind:
            await asyncio.wait_for(entered.wait(), 10)
            current = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
            await service.work.command(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], request=command(current, "stop", attempt_id=scope.attempt_id), can_stop=True)
            request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(run_manager=manager)))
            await dispatch_work_stop(request, service.work, actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"], expected_attempt_id=scope.attempt_id)
        await asyncio.wait_for(asyncio.gather(worker, return_exceptions=True), 10)
        result = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
        assert result["status"] == "open" and result["assignment_revision"] == current["assignment_revision"]
        assert result["attempt"]["status"] == "cancelled"
        assert record.status.value == ("error" if stop_before_bind else "interrupted")
        assert entered.is_set() is (not stop_before_bind)
        async with sf() as session:
            attempt = await session.get(AgentWorkAttemptRow, scope.attempt_id)
            assert attempt.contained_reason is None
    finally:
        if not worker.done():
            worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        await manager.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("installed", [False, True])
async def test_no_workspace_lifecycle_change_is_left_for_host_only_with_registry(execution, installed):
    from deerflow.persistence.agent_instances.model import AgentInstanceRow
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow

    service, admitted, row, sf = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    admitted = replace(admitted, work=scope)
    record = run_record(admitted, scope)
    await scope.bind(admitted, record, admitted.thread_incarnation)
    service.agents.residents = SimpleNamespace() if installed else None
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceRow, admitted.instance.id)).generation += 1
    status = await scope.finish(admitted, record, settled=True)
    assert status == ("running" if installed else "cancelled")
    async with sf() as session:
        attempt = await session.get(AgentWorkAttemptRow, scope.attempt_id)
        assert (attempt.settled_at is None) == installed


@pytest.mark.asyncio
async def test_cancellation_fences_worker_and_keeps_stop_pending(execution):
    service, admitted, row, _ = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    admitted = replace(admitted, work=scope)
    record = run_record(admitted, scope, status="interrupted")
    await scope.bind(admitted, record, admitted.thread_incarnation)
    current = await service.work.get(actor=BOB, instance_id=admitted.instance.id, work_id=row["id"])
    cancelled = await service.work.command(actor=BOB, instance_id=admitted.instance.id, work_id=row["id"], request=command(current, "cancel"), can_stop=True)
    assert cancelled["status"] == "cancelled" and cancelled["attempt"]["status"] == "stopping"
    with pytest.raises(AgentDenied):
        await admitted.validate()
    with pytest.raises(AgentConflict):
        await service.work.command(actor=BOB, instance_id=admitted.instance.id, work_id=row["id"], request=command(cancelled, "reopen", note="Try again"))
    await scope.finish(admitted, record, settled=True)
    current = await service.work.get(actor=BOB, instance_id=admitted.instance.id, work_id=row["id"])
    assert current["attempt"]["status"] == "cancelled"


@pytest.mark.asyncio
async def test_request_reply_explicit_resume_assessment_and_review(execution):
    from deerflow.agent_instances.human_input import HumanInput
    from deerflow.agent_instances.human_input_contract import Respond

    service, admitted, row, _ = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    first = replace(admitted, work=scope)
    run = run_record(admitted, scope, status="success")
    await scope.bind(first, run, first.thread_incarnation)
    current = (await scope.context(first))["work"]
    blocked = await scope.report(
        first,
        ReportWork(
            operation_id=operation(), expected_revision=current["revision"], action="request_input", statement="Which guide should I assess?", purpose="information", reason="The source is unspecified", expected_response="Identify the guide"
        ),
    )
    human_input = HumanInput(service.work)
    req = await human_input.get(actor=BOB, request_id=blocked["human_input_request_id"])
    assert req["creator_kind"] == "nonhuman" and req["creator_id"] == admitted.instance.principal.subject_id
    # A collaborator may append while the original run is still active.
    response = await human_input.respond(actor=BOB, request_id=req["id"], request=Respond(operation_id=operation(), expected_request_revision=1, expected_assignment_revision=1, text="Use the installation guide."))
    assert response["state"] == "answered"
    await scope.finish(first, run, settled=True)
    current = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    assert current["status"] == "blocked" and current["attempt"]["status"] == "succeeded"
    # The response did not create another attempt; explicit activation does.
    resumed = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, current))
    second = replace(admitted, work=resumed)
    resumed_run = run_record(admitted, resumed, status="success")
    await resumed.bind(second, resumed_run, second.thread_incarnation)
    context = await resumed.context(second)
    basis = {**context["request"], "response_ids": [r["id"] for r in context["responses"]]}
    basis.pop("purpose")
    checked = await resumed.report(second, ReportWork(operation_id=operation(), expected_revision=context["work"]["revision"], action="assess_input", statement="The source is sufficiently identified.", request_basis=basis))
    assert checked["status"] == "open"
    assert (await human_input.get(actor=BOB, request_id=req["id"]))["closed_reason"] == "resolved"
    await resumed.report(second, ReportWork(operation_id=operation(), expected_revision=checked["revision"], action="outcome", statement="The guide gaps have been assessed."))
    await resumed.finish(second, resumed_run, settled=True)
    submitted = await service.work.get(actor=BOB, instance_id=admitted.instance.id, work_id=row["id"])
    review = await human_input.get(actor=BOB, request_id=submitted["human_input_request_id"])
    assert review["purpose"] == "review" and submitted["review_state"] == "pending"
    accepted = await service.work.command(
        actor=BOB,
        instance_id=admitted.instance.id,
        work_id=row["id"],
        request=command(
            submitted,
            "accept",
            outcome_id=submitted["outcome_id"],
            evidence_revision=1,
            basis="outcome_statement",
            acknowledge_unchecked_sources=True,
            request_basis={"id": review["id"], "revision": review["revision"], "request_revision": review["request_revision"], "response_ids": []},
        ),
    )
    assert accepted["status"] == "completed" and accepted["review"]["actor_id"] == "bob"


@pytest.mark.asyncio
async def test_uncertain_cleanup_keeps_candidate_and_blocks_replacement(execution):
    service, admitted, row, _ = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    admitted = replace(admitted, work=scope)
    record = run_record(admitted, scope, status="success")
    await scope.bind(admitted, record, admitted.thread_incarnation)
    current = (await scope.context(admitted))["work"]
    await scope.report(admitted, ReportWork(operation_id=operation(), expected_revision=current["revision"], action="outcome", statement="Candidate only"))
    await scope.finish(admitted, record, settled=False)
    current = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    assert current["status"] == "open" and current["attempt"]["status"] == "uncertain"
    assert current["attempt"]["candidate"]["statement"] == "Candidate only"
    with pytest.raises(AgentConflict):
        await service.reserve(execution=replace(admitted, work=None), work_id=row["id"], request=activation(admitted, current))


@pytest.mark.asyncio
async def test_revoked_audience_prevents_candidate_promotion_but_settles_attempt(execution):
    from deerflow.agent_instances.work_contract import WorkSource
    from deerflow.persistence.spaces.model import SpaceGrantRow

    service, admitted, row, sf = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    admitted = replace(admitted, work=scope)
    run = run_record(admitted, scope, status="success")
    await scope.bind(admitted, run, admitted.thread_incarnation)
    current = (await scope.context(admitted))["work"]
    await scope.report(admitted, ReportWork(operation_id=operation(), expected_revision=current["revision"], action="outcome", statement="A candidate", sources=[WorkSource(space_id=admitted.instance.home_id, path="result.txt")]))
    async with sf() as s, s.begin():
        await s.delete(await s.get(SpaceGrantRow, (admitted.instance.home_id, "human", "bob")))
    await scope.finish(admitted, run, settled=True)
    current = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    assert current["status"] == "open" and current["outcome"] is None
    assert current["attempt"]["status"] == "succeeded"
    assert current["attempt"]["candidate"]["statement"] == "A candidate"


@pytest.mark.asyncio
async def test_factual_question_revision_requires_every_current_reply(execution):
    from deerflow.agent_instances.human_input import HumanInput
    from deerflow.agent_instances.human_input_contract import Respond

    service, admitted, row, _ = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    admitted = replace(admitted, work=scope)
    run = run_record(admitted, scope, status="success")
    await scope.bind(admitted, run, admitted.thread_incarnation)
    current = (await scope.context(admitted))["work"]
    blocked = await scope.report(
        admitted, ReportWork(operation_id=operation(), expected_revision=current["revision"], action="request_input", statement="Which guide?", purpose="information", reason="Need a source", expected_response="Guide name")
    )
    human = HumanInput(service.work)
    await human.respond(actor=BOB, request_id=blocked["human_input_request_id"], request=Respond(operation_id=operation(), expected_request_revision=1, expected_assignment_revision=1, text="The guide."))
    context = await scope.context(admitted)
    basis = {k: v for k, v in context["request"].items() if k != "purpose"}
    basis["response_ids"] = []
    report = ReportWork(
        operation_id=operation(),
        expected_revision=context["work"]["revision"],
        action="revise_input",
        statement="What is the exact guide filename?",
        purpose="information",
        reason="The name is ambiguous",
        expected_response="Exact filename",
        request_basis=basis,
    )
    with pytest.raises(AgentConflict):
        await scope.report(admitted, report)
    report = report.model_copy(update={"request_basis": report.request_basis.model_copy(update={"response_ids": [r["id"] for r in context["responses"]]})})
    revised = await scope.report(admitted, report)
    assert revised["status"] == "blocked" and revised["human_input_request_id"] != blocked["human_input_request_id"]
    assert (await human.get(actor=BOB, request_id=blocked["human_input_request_id"]))["closed_reason"] == "superseded"


@pytest.mark.asyncio
async def test_execution_migration_roundtrip_and_used_guard(execution):
    import importlib

    from alembic.operations import Operations
    from alembic.runtime.migration import MigrationContext
    from sqlalchemy import inspect

    service, admitted, row, sf = execution
    migration = importlib.import_module("deerflow.persistence.migrations.versions.0040_work_execution")

    def rebuild(conn):
        tables = {name for name, _ in migration.columns()}
        before = {name: sorted((c["name"], str(c["type"]), c["nullable"]) for c in inspect(conn).get_columns(name)) for name in tables}
        with Operations.context(MigrationContext.configure(conn)):
            migration.upgrade()
            migration.downgrade()
            migration.upgrade()
            migration.upgrade()
        assert {name: sorted((c["name"], str(c["type"]), c["nullable"]) for c in inspect(conn).get_columns(name)) for name in tables} == before

    async with sf.kw["bind"].begin() as conn:
        await conn.run_sync(rebuild)
    await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))

    def refuse(conn):
        with Operations.context(MigrationContext.configure(conn)), pytest.raises(RuntimeError, match="Refusing to erase"):
            migration.downgrade()

    async with sf.kw["bind"].begin() as conn:
        await conn.run_sync(refuse)


@pytest.mark.asyncio
async def test_cancel_before_worker_bind_still_records_and_settles_exact_run(execution):
    service, admitted, row, _ = execution
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    admitted = replace(admitted, work=scope)
    current = await service.work.get(actor=BOB, instance_id=admitted.instance.id, work_id=row["id"])
    await service.work.command(actor=BOB, instance_id=admitted.instance.id, work_id=row["id"], request=command(current, "cancel"), can_stop=True)
    run = run_record(admitted, scope, status="interrupted")
    with pytest.raises(AgentDenied):
        await scope.bind(admitted, run, admitted.thread_incarnation)
    current = await service.work.get(actor=BOB, instance_id=admitted.instance.id, work_id=row["id"])
    assert current["attempt"]["run_id"] == run.run_id
    await scope.finish(admitted, run, settled=True)
    assert (await service.work.get(actor=BOB, instance_id=admitted.instance.id, work_id=row["id"]))["attempt"]["status"] == "cancelled"


@pytest.mark.asyncio
async def test_restart_observation_binds_prior_run_without_claiming_cleanup(execution):
    service, admitted, row, _ = execution
    body = activation(admitted, row)
    original = await service.reserve(execution=admitted, work_id=row["id"], request=body)
    resumed_service = WorkExecution(service.work)
    scope = await resumed_service.reserve(execution=admitted, work_id=row["id"], request=body)
    run = run_record(admitted, original)
    run.idempotency_reused = True
    await scope.observe_run(admitted, run)
    current = await service.work.get(actor=BOB, instance_id=admitted.instance.id, work_id=row["id"])
    assert current["attempt"]["run_id"] == run.run_id and current["attempt"]["status"] == "uncertain"
    with pytest.raises(AgentConflict):
        await resumed_service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, current))


@pytest.mark.asyncio
async def test_terminal_lock_refresh_observes_concurrent_manager_cancellation(execution, monkeypatch):
    import asyncio

    service, admitted, row, sf = execution
    if sf.kw["bind"].dialect.name != "postgresql":
        pytest.skip("PostgreSQL row-lock race; SQLite serializes writers")
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    admitted = replace(admitted, work=scope)
    run = run_record(admitted, scope)
    await scope.bind(admitted, run, admitted.thread_incarnation)
    current = (await scope.context(admitted))["work"]
    await scope.report(admitted, ReportWork(operation_id=operation(), expected_revision=current["revision"], action="outcome", statement="Provisional report"))
    seen, proceed = asyncio.Event(), asyncio.Event()
    original = service._sources

    async def pause(session, work):
        sources = await original(session, work)
        if not seen.is_set():
            seen.set()
            await proceed.wait()
        return sources

    monkeypatch.setattr(service, "_sources", pause)
    finish = asyncio.create_task(scope.finish(admitted, run, settled=True))
    try:
        await asyncio.wait_for(seen.wait(), 10)
        current = await service.work.get(actor=BOB, instance_id=admitted.instance.id, work_id=row["id"])
        cancelled = await service.work.command(actor=BOB, instance_id=admitted.instance.id, work_id=row["id"], request=command(current, "cancel"), can_stop=True)
    finally:
        proceed.set()
        await asyncio.wait_for(finish, 10)
    current = await service.work.get(actor=BOB, instance_id=admitted.instance.id, work_id=row["id"])
    assert current["status"] == "cancelled" and current["outcome"] is None
    assert current["assignment_revision"] == cancelled["assignment_revision"]
    assert current["revision"] == cancelled["revision"] + 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "lease", "command", "session", "cancel_before_bind", "unknown_provider"])
async def test_real_worker_admission_and_cleanup_control_work_promotion(execution, monkeypatch, failure):
    from unittest.mock import AsyncMock, patch

    from langchain_core.messages import AIMessage

    from deerflow.agent_instances.runtime import InstanceSandboxProvider
    from deerflow.community.aio_sandbox.aio_sandbox import AioSandbox
    from deerflow.config.app_config import AppConfig
    from deerflow.persistence.run.model import RunChangeClockRow, RunRow
    from deerflow.persistence.run.sql import RunRepository
    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.worker import RunContext, run_agent

    service, admitted, row, sf = execution
    async with sf.kw["bind"].begin() as conn:
        for model in (RunRow, RunChangeClockRow):
            await conn.run_sync(model.__table__.create)
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    admitted = replace(admitted, work=scope)
    repository = RunRepository(sf)
    manager = RunManager(store=repository)
    run = await manager.create_or_reject(admitted.thread_id, "lead_agent", user_id="alice", idempotency_key="agent-work:" + scope.attempt_id, metadata={"agent_work_id": row["id"], "agent_work_attempt_id": scope.attempt_id})
    if failure == "cancel_before_bind":
        current = await service.work.get(actor=BOB, instance_id=admitted.instance.id, work_id=row["id"])
        await service.work.command(actor=BOB, instance_id=admitted.instance.id, work_id=row["id"], request=command(current, "cancel"), can_stop=True)
    with patch("deerflow.community.aio_sandbox.aio_sandbox.AioSandboxClient"):
        sandbox = AioSandbox(id="worker-test", base_url="http://localhost:8080")
    provider = InstanceSandboxProvider(admitted, sandbox, app_config=AppConfig(sandbox={"use": "deerflow.sandbox.local.local_sandbox_provider:LocalSandboxProvider"}), skill_revision="test-public-capture")
    if failure == "unknown_provider":
        provider = SimpleNamespace(app_config=provider.app_config, skill_revision=provider.skill_revision, close=lambda: None)
    prepared = AsyncMock(return_value=provider)
    monkeypatch.setattr("deerflow.agent_instances.runtime.prepare_environment", prepared)
    if failure == "lease":
        monkeypatch.setattr("deerflow.sandbox.lease.release_sandbox_execution_lease_async", AsyncMock(side_effect=RuntimeError("cleanup failed")))

    class Agent:
        async def astream(self, *args, **kwargs):
            assert (await scope.context(admitted))["work"]["attempt"]["run_id"] == run.run_id
            if failure == "command":
                import httpx

                sandbox._transport_failure_error(httpx.ReadTimeout("lost command reply"), 30)
            if failure == "session":
                sandbox._mark_session_creation_ambiguous("shell", "lost-session")
            current = (await scope.context(admitted))["work"]
            await scope.report(admitted, ReportWork(operation_id=operation(), expected_revision=current["revision"], action="outcome", statement="Candidate from actual worker"))
            yield {"messages": [AIMessage("Finished assessment.")]}

    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())
    await run_agent(bridge, manager, run, ctx=RunContext(checkpointer=None, event_store=None, agent_execution=admitted), agent_factory=lambda **kwargs: Agent(), graph_input={}, config={}, thread_incarnation=admitted.thread_incarnation)
    current = await service.work.get(actor=BOB, instance_id=admitted.instance.id, work_id=row["id"])
    stored = await repository.get(run.run_id, user_id="alice")
    assert stored["status"] == ("error" if failure == "cancel_before_bind" else "success"), run.error
    if failure is None:
        assert current["status"] == "submitted" and current["attempt"]["status"] == "succeeded"
    elif failure == "cancel_before_bind":
        prepared.assert_not_called()
        assert current["status"] == "cancelled" and current["attempt"]["status"] == "cancelled"
    else:
        assert current["status"] == "open" and current["attempt"]["status"] == "uncertain"
        assert current["outcome"] is None
    assert current["attempt"]["run_id"] == run.run_id
    restarted = RunManager(store=repository)
    receipt = await restarted.create_or_reject(admitted.thread_id, "lead_agent", user_id="alice", idempotency_key="agent-work:" + scope.attempt_id, metadata=run.metadata)
    assert receipt.idempotency_reused and receipt.run_id == run.run_id


@pytest.mark.asyncio
async def test_derived_work_uses_adopted_defaults_and_retains_report_history(execution):
    from deerflow.agent_instances.contract import DefinitionSnapshot
    from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceRow
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    service, admitted, row, sf = execution
    definition = DefinitionSnapshot.capture(
        owner_id="alice",
        config={"name": "analyst", "work_policy": {"enabled": True, "allow_derived": True, "max_derived_per_activation": 1, "default_priority": "high", "review_required": True}},
        soul="Maintain useful technical documentation.",
    )
    async with sf() as session, session.begin():
        session.add(AgentDefinitionRevisionRow(revision=definition.revision, owner_id=definition.owner_id, config=definition.config, soul=definition.soul))
        await session.flush()
        (await session.get(AgentInstanceRow, admitted.instance.id)).definition_revision = definition.revision
        (await session.get(AgentWorkRow, row["id"])).definition_revision = definition.revision
    admitted = await admitted.authority.execution(actor=ALICE, thread_id=admitted.thread_id)
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    admitted = replace(admitted, work=scope)
    run = run_record(admitted, scope)
    await scope.bind(admitted, run, admitted.thread_incarnation)
    current = (await scope.context(admitted))["work"]
    current = await scope.report(admitted, ReportWork(operation_id=operation(), expected_revision=current["revision"], action="derive", statement="Check links", success_criteria="List broken links"))
    with pytest.raises(AgentConflict, match="limit"):
        await scope.report(admitted, ReportWork(operation_id=operation(), expected_revision=current["revision"], action="derive", statement="Another task", success_criteria="Result"))
    rows = await service.work.list(actor=ALICE, instance_id=admitted.instance.id)
    child = next(r for r in rows if r["id"] != row["id"])
    assert child["priority"] == "high" and child["review_required"] and child["attempt"] is None
    current = await scope.report(admitted, ReportWork(operation_id=operation(), expected_revision=current["revision"], action="outcome", statement="A retained candidate"))
    run.status = "error"
    await scope.finish(admitted, run, settled=True)
    current = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    await service.reserve(execution=replace(admitted, work=None), work_id=row["id"], request=activation(admitted, current))
    history = await service.work.history(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
    assert next(e for e in history if e["action"] == "outcome")["report"]["statement"] == "A retained candidate"


@pytest.mark.asyncio
async def test_attempt_recovery_requires_completed_exact_containment(execution):
    from test_storage_spaces_attachments import ContainedProvider

    from deerflow.agent_instances.lifecycle import InstanceLifecycle
    from deerflow.spaces.attachments import ResourceMount, SpaceAttachments

    service, admitted, row, _ = execution
    provider = ContainedProvider()
    attachments = SpaceAttachments(service.agents.files, provider)
    home = await service.agents.files.registry.get(actor=admitted.instance.principal, space_id=admitted.instance.home_id)
    await attachments.attach(actor=admitted.instance.principal, incarnation=operation(), resources=[ResourceMount(home.id, home.generation, "home", writable=True)])
    scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, row))
    current = await service.work.get(actor=BOB, instance_id=admitted.instance.id, work_id=row["id"])
    intent = operation()
    with pytest.raises(AgentConflict):
        await service.work.command(
            actor=BOB, instance_id=admitted.instance.id, work_id=row["id"], request=command(current, "reconcile_attempt", attempt_id=scope.attempt_id, containment_operation_id=intent, note="Resolve the prior attempt")
        )
    stopped = await InstanceLifecycle(service.agents).change(actor=BOB, instance_id=admitted.instance.id, expected_generation=admitted.instance.generation, operation_id=intent, action="suspend")
    assert stopped["complete"]
    result = await service.work.command(
        actor=BOB,
        instance_id=admitted.instance.id,
        work_id=row["id"],
        request=command(current, "reconcile_attempt", attempt_id=scope.attempt_id, containment_operation_id=intent, note="Prior environment contained; inspect effects before resuming"),
    )
    assert result["attempt"]["status"] == "failed" and result["outcome"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("lost", ["before", "after"])
async def test_http_activation_ceiling_and_lost_launch_receipt_reuse_exact_attempt(execution, monkeypatch, lost):
    import httpx
    from fastapi import HTTPException
    from test_agent_instances import request_app

    from app.gateway.routers import agent_instances as routes
    from app.gateway.routers.agent_work import router
    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.store.memory import MemoryRunStore

    service, admitted, row, _ = execution
    manager = RunManager(store=MemoryRunStore())
    calls = []

    async def launcher(body, thread_id, request, *, idempotency_key, require_existing_thread, work_attempt):
        calls.append(idempotency_key)
        assert require_existing_thread and work_attempt.work_id == row["id"]
        if len(calls) == 1 and lost == "before":
            raise HTTPException(503, "Launch acknowledgement unavailable")
        record = await manager.create_or_reject(thread_id, body.assistant_id, user_id="alice", idempotency_key=idempotency_key, metadata={"agent_work_id": row["id"], "agent_work_attempt_id": work_attempt.attempt_id})
        if len(calls) == 1:
            raise HTTPException(503, "Admitted run acknowledgement unavailable")
        return record

    monkeypatch.setattr(routes, "get_agents_api_config", lambda: SimpleNamespace(enabled=True))
    monkeypatch.setattr("app.gateway.services.start_run", launcher)
    body = activation(admitted, row).model_dump(mode="json")
    for permissions, expected in [(("agents:read", "agents:write"), 403), (("agents:read", "agents:write", "runs:create"), 503)]:
        app = request_app(service.agents, permissions=permissions)
        app.state.agent_conversations = admitted.authority
        app.include_router(router)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            path = f"/api/agent-instances/{admitted.instance.id}/work/{row['id']}/activate"
            response = await client.post(path, json=body)
            assert response.status_code == expected, response.text
            if expected == 503:
                response = await client.post(path, json=body)
                assert response.status_code == 200, response.text
                assert response.json()["attempt"]["run_id"]
    assert len(calls) == 2 and calls[0] == calls[1]
