from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from deerflow_extension_api.agent_runs import AgentRunError
from test_agent_conversations import conversations
from test_agent_instances import create
from test_agent_instances import instances as instances
from test_batch_dependencies import repo as repo
from test_subagent_batch_service import _request

from app.subagent_batches.continuation import BatchContinuations
from app.subagent_batches.identity import resolve_batch_principal
from app.subagent_batches.service import batch_owner_allowed
from deerflow.subagents.batch_acceptance import check_batch_acceptance
from deerflow.subagents.batch_service import SubagentBatchService


def agent_spec():
    return {"scope": {"kind": "agent_conversation", "version": 1, "instance_id": "employee", "principal": {"source": "session", "system_role": "user", "token_version": 2, "oauth_provider": None, "oauth_id": None}}}


@pytest.mark.asyncio
@pytest.mark.parametrize("followup", [None, "Compare the results"])
async def test_submission_uses_current_conversation_binding_before_save(followup):
    repo = AsyncMock()
    binding = AsyncMock(return_value="employee")
    host = BatchContinuations(SimpleNamespace(state=SimpleNamespace(agent_conversations=SimpleNamespace(binding=binding))))
    from deerflow.config.subagent_batches_config import SubagentBatchesConfig
    from deerflow.config.subagent_runtime_config import SubagentRuntimeConfig

    service = SubagentBatchService(repository=repo, config=SubagentBatchesConfig(), runtime_config=SubagentRuntimeConfig(), classify_submission=host.admit_submission)
    request = _request(continuation=followup)
    with pytest.raises(AgentRunError, match="instance conversation"):
        await service.submit(request)
    binding.assert_awaited_once_with(request.thread_id)
    repo.create_batch.assert_not_awaited()
    if not followup:
        binding.return_value = None
        await service.submit(request)
        assert repo.create_batch.await_args.kwargs["execution_spec"] == request.execution_spec
        with pytest.raises(ValueError):
            await service.submit(_request(execution_spec=agent_spec()))
        assert repo.create_batch.await_count == 1
        binding.side_effect = RuntimeError("lookup unavailable")
        with pytest.raises(RuntimeError):
            await service.submit(request)
        assert repo.create_batch.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", [None, [], False, {}, {"kind": "future", "version": 1}, {"kind": "agent_conversation", "version": 2}, agent_spec()["scope"]])
async def test_worker_and_file_acceptance_never_use_personal_storage_for_present_scope(scope):
    batch = {"id": "batch", "user_id": "owner", "thread_id": "thread", "execution_spec": {"scope": scope}}
    repo = AsyncMock()
    service = SubagentBatchService.__new__(SubagentBatchService)
    service._repository = repo
    service._owner_access = AsyncMock(return_value=True)
    service._dispatch_continuation = AsyncMock()
    await service._execute_item({"id": "item", "batch": batch}, lease_owner="worker")
    await service._continue_batch(batch, "worker")
    service._owner_access.assert_not_awaited()
    service._dispatch_continuation.assert_not_awaited()
    assert not repo.mock_calls
    assert await BatchContinuations(SimpleNamespace()).dispatch(batch) == {"status": "suppressed"}
    with pytest.raises(PermissionError):
        await check_batch_acceptance(["file:report.txt"], batch=batch, app_config=None, bash_executions=None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("user_changes", "scope_changes", "principal_changes", "extra"),
    [
        ({}, {}, {}, {}),
        ({"system_role": "admin"}, {}, {}, {}),
        ({"oauth_provider": "oidc", "oauth_id": "new"}, {}, {}, {}),
        ({"token_version": 3}, {}, {}, {}),
        ({"disabled_at": datetime(2026, 1, 1, tzinfo=UTC)}, {}, {}, {}),
        ({"needs_setup": True}, {}, {}, {}),
        ({"id": "someone-else"}, {}, {}, {}),
        ({}, {}, {}, {"user_role": None}),
        ({}, {}, {"is_internal": True}, {}),
        ({}, {"version": True}, {}, {}),
        ({}, {}, {"source": "auth_disabled"}, {}),
        ({}, {}, {"token_version": True}, {}),
    ],
    ids=["valid", "promotion", "sign-in", "token", "disabled", "needs-setup", "wrong-owner", "mixed", "internal", "version", "source", "boolean-token"],
)
async def test_bound_identity_matches_current_session_exactly(monkeypatch, user_changes, scope_changes, principal_changes, extra):
    from app.gateway import deps
    from app.subagent_batches import service

    profile = {"id": "owner", "system_role": "user", "token_version": 2, "oauth_provider": None, "oauth_id": None, "needs_setup": False, "disabled_at": None, "released_at": None}
    user = SimpleNamespace(**(profile | user_changes))
    spec = agent_spec()
    spec.update(extra)
    spec["scope"].update(scope_changes)
    spec["scope"]["principal"].update(principal_changes)
    valid = not any((user_changes, scope_changes, principal_changes, extra))
    load = AsyncMock(return_value=user)
    monkeypatch.setattr(deps, "get_local_provider", lambda: SimpleNamespace(get_user=load))
    perms = AsyncMock(return_value={"runs:create"})
    monkeypatch.setattr(service, "resolve_route_permissions", perms)
    monkeypatch.setattr("app.gateway.auth.mode.sign_on_only", lambda: False)
    batch = {"id": "batch", "user_id": "owner", "execution_spec": spec}
    assert await batch_owner_allowed(batch) is valid
    if valid:
        principal = await resolve_batch_principal(batch, load_user=load)
        assert principal.user_id == "owner" and principal.role == "user" and not principal.is_internal
        assert perms.await_args.kwargs["is_internal"] is False
    else:
        with pytest.raises(PermissionError):
            await resolve_batch_principal(batch, load_user=load)


@pytest.mark.asyncio
async def test_ordinary_background_submission_refuses_durable_agent_binding_with_managed_runtime_off(instances):
    from deerflow.agent_instances.conversations import AgentConversations

    agents, conversations_service, _threads, sessions = await conversations(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    record = await conversations_service.create(actor=ALICE, instance_id=agent.id, thread_id=operation())
    bindings = AgentConversations(None, session_factory=sessions)
    host = BatchContinuations(SimpleNamespace(state=SimpleNamespace(agent_conversations=bindings)))
    with pytest.raises(AgentRunError, match="instance conversation"):
        await host.admit_submission(_request(thread_id=record["thread_id"]))
    await host.admit_submission(_request(thread_id=operation()))


@pytest.mark.asyncio
async def test_read_only_cancel_does_not_control_a_followup_run():
    repo = AsyncMock()
    repo.get_batch.return_value = {"read_only": True, "continuation_status": "dispatching"}
    cancel = AsyncMock()
    service = SubagentBatchService.__new__(SubagentBatchService)
    service._cancel_continuation = cancel
    service._repository = repo
    result = await service.cancel_batch(batch_id="batch", user_id="owner", continuation_authority=object())
    assert result["read_only"]
    repo.cancel_batch.assert_not_awaited()
    cancel.assert_not_awaited()


@pytest.mark.asyncio
async def test_real_account_version_revokes_a_bound_session(repo, monkeypatch):
    from app.gateway.auth.models import User
    from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
    from deerflow.persistence.engine import get_session_factory

    accounts = SQLiteUserRepository(get_session_factory())
    user = await accounts.create_user(User(email="reader@example.com", system_role="user"))
    spec = agent_spec()
    spec["scope"]["principal"]["token_version"] = user.token_version
    batch = {"id": "batch", "user_id": str(user.id), "execution_spec": spec}
    monkeypatch.setattr("app.gateway.auth.mode.sign_on_only", lambda: False)
    principal = await resolve_batch_principal(batch, load_user=accounts.get_user_by_id)
    assert principal.user_id == str(user.id) and not principal.is_internal
    await accounts.replace_password(str(user.id), "test-digest", expected_token_version=user.token_version)
    with pytest.raises(PermissionError, match="session"):
        await resolve_batch_principal(batch, load_user=accounts.get_user_by_id)


@pytest.mark.asyncio
async def test_unrecognized_scope_is_refused_without_an_account_store(monkeypatch):
    from app.gateway import deps

    provider = Mock(side_effect=RuntimeError("account store unavailable"))
    monkeypatch.setattr(deps, "get_local_provider", provider)
    assert not await batch_owner_allowed({"user_id": "owner", "execution_spec": {"scope": None}})
    provider.assert_not_called()
