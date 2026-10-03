"""Account refusal fences the durable queue as well as ordinary runs."""

import asyncio
from datetime import UTC, datetime, timedelta
from enum import Enum
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from app.gateway.auth.accounts import BATCH_CANCEL_REASON, AccountsCommand
from app.gateway.auth.models import User
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from deerflow.config.authorization_config import AuthorizationConfig
from deerflow.config.database_config import DatabaseConfig
from deerflow.config.subagent_batches_config import SubagentBatchesConfig
from deerflow.config.subagent_runtime_config import SubagentRuntimeConfig
from deerflow.persistence.engine import close_engine, get_session_factory, init_engine_from_config
from deerflow.persistence.subagent_batches import SubagentBatchRepository
from deerflow.subagents import batch_service

ISSUER = "https://identity.example.test"


class _Status(Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

    @property
    def is_terminal(self):
        return self is not self.RUNNING


@pytest_asyncio.fixture
async def stores(tmp_path, monkeypatch):
    await init_engine_from_config(DatabaseConfig(backend="sqlite", sqlite_dir=str(tmp_path)))
    sf = get_session_factory()
    users = SQLiteUserRepository(sf)
    owner = await users.create_user(User(email="owner@example.com", oauth_provider="oidc", oauth_issuer=ISSUER, oauth_id="owner"))
    colleague = await users.create_user(User(email="colleague@example.com", oauth_provider="oidc", oauth_issuer=ISSUER, oauth_id="colleague"))
    monkeypatch.setattr("app.gateway.deps.get_local_provider", lambda: SimpleNamespace(get_user=users.get_user_by_id))
    monkeypatch.setattr("app.gateway.auth.mode.sign_on_only", lambda: False)
    monkeypatch.setattr("app.gateway.authz._get_route_authorization_config", lambda: AuthorizationConfig())
    try:
        yield users, SubagentBatchRepository(sf), owner, colleague
    finally:
        await close_engine()


async def create_batch(repo, owner, batch_id="batch-1"):
    return await repo.create_batch(
        batch_id=batch_id,
        user_id=str(owner.id),
        thread_id=f"thread-{batch_id}",
        run_id=None,
        tool_call_id=None,
        submission_key=batch_id,
        title="Work",
        subagent_type="general-purpose",
        items=[{"key": str(i), "prompt": "work"} for i in range(3)],
        max_live_items=2,
        max_running_items=1,
        max_attempts=3,
        execution_spec={
            "subagent_config": {"name": "general-purpose", "description": "Work", "system_prompt": "Work carefully"},
            "user_role": "user",
            "oauth_provider": "oidc",
            "oauth_id": "owner",
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["pending", "paused", "leased", "running"])
async def test_disable_cancels_owned_batches_without_claiming_running_work_stopped(stores, state):
    users, repo, owner, colleague = stores
    await create_batch(repo, owner)
    if state == "paused":
        await repo.pause_batch("batch-1", user_id=str(owner.id))
    elif state in {"leased", "running"}:
        [item] = await repo.claim_items(now=datetime.now(UTC), lease_owner="worker", lease_seconds=60, limit=1)
        if state == "running":
            await repo.mark_item_running(item["id"], lease_owner="worker", now=datetime.now(UTC))
    await create_batch(repo, colleague, "colleague-batch")
    command = AccountsCommand(users, tokens=None, schedules=None, batches=repo, wait_seconds=0)
    document = await command.run("disable", issuer=ISSUER, subject="owner")
    assert (await repo.get_batch("batch-1", user_id=str(owner.id)))["counts"]["cancelled"] == 3
    assert (await repo.get_batch("colleague-batch", user_id=str(colleague.id)))["status"] == "queued"
    items = await repo.list_items("batch-1", user_id=str(owner.id))
    assert all(row["error"] == BATCH_CANCEL_REASON for row in items)
    expected_unconfirmed = state in {"leased", "running"}
    assert ("subagent_batches" in document["surfaces_unconfirmed"]) is expected_unconfirmed
    assert document["returncode"] == (2 if expected_unconfirmed else 0)
    # Reading cancelled rows again is not proof that a worker drained.
    again = await command.run("disable", issuer=ISSUER, subject="owner")
    assert ("subagent_batches" in again["surfaces_unconfirmed"]) is expected_unconfirmed
    await command.run("enable", issuer=ISSUER, subject="owner")
    assert (await repo.get_batch("batch-1", user_id=str(owner.id)))["status"] == "cancelled"


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal_at", ["before_claim", "during_assembly", "recovery"])
async def test_current_owner_refusal_prevents_dispatch_even_without_cli_cancellation(stores, monkeypatch, refusal_at):
    from app.subagent_batches.service import batch_owner_allowed

    users, repo, owner, _ = stores
    await create_batch(repo, owner)
    now = datetime.now(UTC)
    if refusal_at == "recovery":
        await repo.claim_items(now=now - timedelta(seconds=61), lease_owner="previous-worker", lease_seconds=60, limit=1)
    if refusal_at != "during_assembly":
        await users.disable_identity(ISSUER, "owner")

    async def assemble(*args, **kwargs):
        await users.disable_identity(ISSUER, "owner")
        return []

    monkeypatch.setattr(batch_service, "run_assembly", assemble)
    monkeypatch.setattr(batch_service, "resolve_subagent_model_name", lambda *a, **kw: "model")
    launches = []
    monkeypatch.setattr(batch_service, "SubagentExecutor", lambda **kw: SimpleNamespace(execute_async=lambda *a, **kw: launches.append(kw)))
    service = batch_service.SubagentBatchService(
        repository=repo,
        config=SubagentBatchesConfig(),
        runtime_config=SubagentRuntimeConfig(),
        app_config=SimpleNamespace(),
        owner_access=batch_owner_allowed,
    )
    await service.run_once(now=now)
    await asyncio.gather(*list(service._executions.values()))
    assert launches == []
    assert (await repo.get_batch("batch-1", user_id=str(owner.id)))["status"] == "cancelled"


@pytest.mark.asyncio
async def test_deployment_command_includes_the_durable_batch_repository(stores, monkeypatch):
    from app.gateway.auth import accounts

    _, repo, owner, _ = stores
    await create_batch(repo, owner)
    monkeypatch.setattr("deerflow.persistence.engine.init_engine_from_config", AsyncMock())
    monkeypatch.setattr("deerflow.persistence.engine.close_engine", AsyncMock())
    monkeypatch.setattr("deerflow.config.get_app_config", lambda: SimpleNamespace(database=SimpleNamespace(backend="sqlite")))
    monkeypatch.setattr(accounts, "deployment_options", lambda _: {})
    document = await accounts._run("disable", issuer=ISSUER, subject="owner", email=None, wait_seconds=0)
    assert document["returncode"] == 0
    assert (await repo.get_batch("batch-1", user_id=str(owner.id)))["counts"]["cancelled"] == 3


@pytest.mark.asyncio
async def test_user_cancellation_before_disable_keeps_execution_unconfirmed(stores):
    users, repo, owner, _ = stores
    await create_batch(repo, owner)
    await repo.claim_items(now=datetime.now(UTC), lease_owner="worker", lease_seconds=60, limit=1)
    await repo.cancel_batch("batch-1", user_id=str(owner.id))
    command = AccountsCommand(users, tokens=None, schedules=None, batches=repo, wait_seconds=0)
    document = await command.run("disable", issuer=ISSUER, subject="owner")
    surface = document["surfaces"]["subagent_batches"]
    assert surface["execution_stop_confirmed"] is False
    assert surface["stopped_after_ms"] is None
    assert surface["not_ended"] == 1
    assert document["returncode"] == 2


@pytest.mark.asyncio
async def test_cancellation_write_failure_cannot_claim_execution_stopped(stores, monkeypatch):
    users, repo, owner, _ = stores
    await create_batch(repo, owner)
    await repo.claim_items(now=datetime.now(UTC), lease_owner="worker", lease_seconds=60, limit=1)
    monkeypatch.setattr(repo, "cancel_batch", AsyncMock(side_effect=OSError("write unavailable")))
    document = await AccountsCommand(users, tokens=None, schedules=None, batches=repo, wait_seconds=0).run("disable", issuer=ISSUER, subject="owner")
    assert document["returncode"] == 2
    assert document["surfaces"]["subagent_batches"]["execution_stop_confirmed"] is False
    assert (await repo.get_batch("batch-1", user_id=str(owner.id)))["counts"]["leased"] == 1


@pytest.mark.asyncio
async def test_cancel_during_final_owner_lookup_is_fenced_by_lease(stores, monkeypatch):
    _, repo, owner, _ = stores
    await create_batch(repo, owner)
    checks = 0

    async def owner_access(batch):
        nonlocal checks
        checks += 1
        if checks == 2:
            await repo.cancel_batch(batch["id"], user_id=batch["user_id"])
        return True

    monkeypatch.setattr(batch_service, "run_assembly", AsyncMock(return_value=[]))
    monkeypatch.setattr(batch_service, "resolve_subagent_model_name", lambda *a, **kw: "model")
    launches = []
    monkeypatch.setattr(batch_service, "SubagentExecutor", lambda **kw: SimpleNamespace(execute_async=lambda *a, **kw: launches.append(kw)))
    service = batch_service.SubagentBatchService(repository=repo, config=SubagentBatchesConfig(), runtime_config=SubagentRuntimeConfig(), app_config=SimpleNamespace(), owner_access=owner_access)
    await service.run_once(now=datetime.now(UTC))
    await asyncio.gather(*list(service._executions.values()))
    assert checks == 2
    assert launches == []


@pytest.mark.asyncio
@pytest.mark.parametrize("lookup_error", [False, True])
@pytest.mark.parametrize("write_error", [False, True])
async def test_owner_refusal_cancels_and_supervises_until_execution_stops(stores, monkeypatch, lookup_error, write_error):
    _, repo, owner, _ = stores
    await create_batch(repo, owner)
    checks = 0
    cancelled = asyncio.Event()
    allow_write = asyncio.Event()
    monkeypatch.setattr(batch_service, "SubagentStatus", _Status)
    result = SimpleNamespace(status=_Status.RUNNING, result=None, error="cancelled", stop_reason=None, token_usage_records=None)

    async def owner_access(batch):
        nonlocal checks
        checks += 1
        if checks <= 2:
            return True
        if lookup_error:
            raise OSError("account store unavailable")
        return False

    monkeypatch.setattr(batch_service, "run_assembly", AsyncMock(return_value=[]))
    monkeypatch.setattr(batch_service, "resolve_subagent_model_name", lambda *a, **kw: "model")
    monkeypatch.setattr(batch_service, "SubagentExecutor", lambda **kw: SimpleNamespace(execute_async=lambda *a, **kw: "execution"))
    monkeypatch.setattr(batch_service, "get_background_task_result", lambda _: result)
    monkeypatch.setattr(batch_service, "request_cancel_background_task", lambda _: cancelled.set())
    monkeypatch.setattr(batch_service, "cleanup_background_task", lambda _: None)
    if write_error:
        original_cancel = repo.cancel_batch

        async def unavailable_until_recovered(*args, **kwargs):
            if not allow_write.is_set():
                raise OSError("batch store unavailable")
            return await original_cancel(*args, **kwargs)

        monkeypatch.setattr(repo, "cancel_batch", unavailable_until_recovered)
    service = batch_service.SubagentBatchService(
        repository=repo,
        config=SubagentBatchesConfig(lease_seconds=10, poll_interval_seconds=0.1),
        runtime_config=SubagentRuntimeConfig(),
        app_config=SimpleNamespace(),
        owner_access=owner_access,
    )
    await service.run_once(now=datetime.now(UTC))
    tasks = list(service._executions.values())
    try:
        await asyncio.wait_for(cancelled.wait(), 6)
        assert service._execution_ids
        assert all(not task.done() for task in tasks), "a cancellation request must not abandon an execution still draining"
    finally:
        result.status = batch_service.SubagentStatus.FAILED
        if write_error:
            await asyncio.sleep(0.25)
            assert all(not task.done() for task in tasks), "refused work must not be requeued when its cancellation write failed"
        allow_write.set()
        await asyncio.wait_for(asyncio.gather(*tasks), 5)
    assert not service._execution_ids
    assert (await repo.get_batch("batch-1", user_id=str(owner.id)))["status"] == "cancelled"
    assert await repo.claim_items(now=datetime.now(UTC), lease_owner="next-worker", lease_seconds=60, limit=3) == []


@pytest.mark.asyncio
async def test_owner_refusal_prevents_post_execution_acceptance_sandbox(stores, monkeypatch):
    _, repo, owner, _ = stores
    await create_batch(repo, owner)
    service = batch_service.SubagentBatchService(repository=repo, config=SubagentBatchesConfig(), runtime_config=SubagentRuntimeConfig(), owner_access=AsyncMock(return_value=False))
    [item] = await repo.claim_items(now=datetime.now(UTC), lease_owner=service._lease_owner, lease_seconds=60, limit=1)
    item["acceptance_criteria"] = ["file:/mnt/user-data/outputs/result.txt exists"]
    checker = AsyncMock()
    monkeypatch.setattr(batch_service, "check_batch_acceptance", checker)
    assert await service._check_acceptance_with_lease(item, SimpleNamespace(), SimpleNamespace()) == (False, None)
    checker.assert_not_called()


@pytest.mark.asyncio
async def test_batch_owner_role_is_not_expanded_after_promotion_and_demotion_refuses(stores, monkeypatch):
    from app.subagent_batches import service as gateway_service

    users, repo, owner, _ = stores
    await create_batch(repo, owner)
    [item] = await repo.claim_items(now=datetime.now(UTC), lease_owner="worker", lease_seconds=60, limit=1)
    batch = item["batch"]
    seen_roles = []

    async def permissions(user, **kwargs):
        seen_roles.append(user.system_role)
        return ["runs:create"]

    monkeypatch.setattr(gateway_service, "resolve_route_permissions", permissions)
    await users.update_user(owner.model_copy(update={"system_role": "admin"}))
    assert await gateway_service.batch_owner_allowed(batch)
    assert seen_roles == ["user"]
    batch["execution_spec"]["user_role"] = "admin"
    await users.limit_role(ISSUER, "owner", "user")
    assert not await gateway_service.batch_owner_allowed(batch)
    assert seen_roles == ["user"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "spec,allowed",
    [
        ({"is_internal": True, "channel_user_id": "channel/user"}, True),
        ({"is_internal": False, "channel_user_id": "channel/user"}, False),
        ({"is_internal": True, "channel_user_id": "other"}, False),
        ({"is_internal": True, "channel_user_id": "channel/user", "user_role": "user"}, False),
        ({"is_internal": True, "channel_user_id": "channel/user", "oauth_id": "deleted-account"}, False),
        ({}, False),
    ],
)
async def test_missing_account_exception_is_only_for_unbound_internal_channels(stores, spec, allowed):
    from app.subagent_batches.service import batch_owner_allowed
    from deerflow.config.paths import make_safe_user_id

    assert await batch_owner_allowed({"user_id": make_safe_user_id("channel/user"), "execution_spec": spec}) is allowed


@pytest.mark.asyncio
@pytest.mark.parametrize("after_assembly", [False, True])
async def test_refusal_before_launch_survives_transient_cancellation_write_failure(stores, monkeypatch, after_assembly):
    _, repo, owner, _ = stores
    await create_batch(repo, owner)
    checks, writes = 0, 0
    original_cancel = repo.cancel_batch

    async def access(batch):
        nonlocal checks
        checks += 1
        return after_assembly and checks == 1

    async def cancel(*args, **kwargs):
        nonlocal writes
        writes += 1
        if writes == 1:
            raise OSError("transient failure")
        return await original_cancel(*args, **kwargs)

    monkeypatch.setattr(repo, "cancel_batch", cancel)
    monkeypatch.setattr(batch_service, "run_assembly", AsyncMock(return_value=[]))
    monkeypatch.setattr(batch_service, "resolve_subagent_model_name", lambda *a, **kw: "model")
    launched = []
    monkeypatch.setattr(batch_service, "SubagentExecutor", lambda **kw: SimpleNamespace(execute_async=lambda *a, **kw: launched.append(kw)))
    service = batch_service.SubagentBatchService(repository=repo, config=SubagentBatchesConfig(poll_interval_seconds=0.1), runtime_config=SubagentRuntimeConfig(), app_config=SimpleNamespace(), owner_access=access)
    await service.run_once(now=datetime.now(UTC))
    await asyncio.wait_for(asyncio.gather(*list(service._executions.values())), 5)
    assert writes == 2
    assert launched == []
    assert (await repo.get_batch("batch-1", user_id=str(owner.id)))["status"] == "cancelled"


@pytest.mark.asyncio
async def test_acceptance_drains_before_retrying_owner_cancellation(stores, monkeypatch):
    _, repo, owner, _ = stores
    await create_batch(repo, owner)
    started, cancelling, release, drained = (asyncio.Event() for _ in range(4))
    checks, writes = 0, 0
    original_cancel = repo.cancel_batch

    async def checker(*args, **kwargs):
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelling.set()
            await release.wait()
            drained.set()
            raise

    async def access(batch):
        nonlocal checks
        checks += 1
        return checks == 1

    async def cancel(*args, **kwargs):
        nonlocal writes
        assert drained.is_set(), "the sandbox checker must stop before database retry waits"
        writes += 1
        if writes == 1:
            raise OSError("transient failure")
        return await original_cancel(*args, **kwargs)

    monkeypatch.setattr(batch_service, "check_batch_acceptance", checker)
    monkeypatch.setattr(repo, "cancel_batch", cancel)
    service = batch_service.SubagentBatchService(repository=repo, config=SubagentBatchesConfig(lease_seconds=10, poll_interval_seconds=0.1), runtime_config=SubagentRuntimeConfig(), owner_access=access)
    [item] = await repo.claim_items(now=datetime.now(UTC), lease_owner=service._lease_owner, lease_seconds=60, limit=1)
    item["acceptance_criteria"] = ["file:/mnt/user-data/outputs/result.txt exists"]
    task = asyncio.create_task(service._check_acceptance_with_lease(item, SimpleNamespace(), SimpleNamespace()))
    try:
        await asyncio.wait_for(started.wait(), 2)
        await asyncio.wait_for(cancelling.wait(), 6)
        assert writes == 0
        assert not task.done()
    finally:
        release.set()
    assert await asyncio.wait_for(task, 5) == (False, None)
    assert writes == 2
    assert (await repo.get_batch("batch-1", user_id=str(owner.id)))["status"] == "cancelled"
