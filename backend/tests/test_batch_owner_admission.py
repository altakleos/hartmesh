"""Native capacity waits revalidate identity on the host's database loop."""

import asyncio
import importlib.util
import json
import sys
from pathlib import Path

import pytest
from support.batch_owner import owner_admin, owner_normal
from test_batch_owner_identity import accounts  # noqa: F401 (fixture)

from deerflow.authz.provider import Principal
from deerflow.config.app_config import AppConfig
from deerflow.config.subagent_batches_config import SubagentBatchesConfig
from deerflow.config.subagent_runtime_config import SubagentRuntimeConfig
from deerflow.subagents.batch_service import SubagentBatchService
from deerflow.subagents.capacity import SubagentExecutionCapacity
from deerflow.subagents.config import SubagentConfig


@pytest.fixture
def executor_module(monkeypatch, tmp_path):
    # conftest installs an executor stub; load the real graph and worker loop.
    path = Path(__file__).parents[1] / "packages/harness/deerflow/subagents/executor.py"
    spec = importlib.util.spec_from_file_location("_batch_owner_executor", path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("BATCH_OWNER_PROBE_DIR", str(tmp_path))
    yield module
    module._shutdown_isolated_subagent_loop()


def _executor(module, resolver, capacity=None, *, user_id="owner", user_role="admin"):
    config = AppConfig.model_validate(
        {
            "models": [{"name": "owner-probe", "use": "support.batch_owner:OwnerModel", "model": "offline"}],
            "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
            "summarization": {"enabled": False},
            "tool_search": {"enabled": False},
            "authorization": {"enabled": True, "provider": {"use": "deerflow.authz.rbac:RbacAuthorizationProvider", "config": {"roles": {"admin": {}, "user": {"tools": {"deny": ["owner_admin"]}}}}}},
        }
    )
    return module.SubagentExecutor(
        config=SubagentConfig(name="general-purpose", description="Owner probe", skills=[]),
        tools=[owner_admin, owner_normal],
        app_config=config,
        parent_model="owner-probe",
        thread_id="batch-owner-test",
        user_id=user_id,
        user_role=user_role,
        oauth_provider="old",
        oauth_id="old",
        authz_attributes={"old_grant": True},
        execution_capacity=capacity,
        principal_resolver=resolver,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["disabled", "different_owner", "fresh_profile", "unchanged"])
async def test_identity_is_rechecked_after_capacity_wait(executor_module, tmp_path, change):
    capacity = SubagentExecutionCapacity(SubagentRuntimeConfig(max_running=1))
    state = "queued"
    calls = []

    async def resolve():
        calls.append(state)
        if state == "disabled":
            raise PermissionError("Owner disabled")
        return Principal(user_id="different" if state == "different_owner" else "owner", role="user" if state == "fresh_profile" else "admin", oauth_provider="oidc", oauth_id="current")

    executor = _executor(executor_module, resolve, capacity)
    task = None
    try:
        async with capacity.slot():
            task = asyncio.create_task(executor._aexecute("OWNER_CHILD:second"))
            async with asyncio.timeout(5):
                while capacity.snapshot().queued != 1:
                    await asyncio.sleep(0)
            assert not calls and not (tmp_path / "reached-second").exists()
            state = change
        result = await asyncio.wait_for(task, 20)
        assert calls == [change]
        assert result.execution_done_event.is_set()
        if change in ("disabled", "different_owner"):
            assert result.status == executor_module.SubagentStatus.FAILED
            assert result.started_at is None
            assert not (tmp_path / "calls.jsonl").exists()
        else:
            assert result.status == executor_module.SubagentStatus.COMPLETED, result.error
            observed = [json.loads(line) for line in (tmp_path / "calls.jsonl").read_text(encoding="utf-8").splitlines()]
            assert {call["tool"] for call in observed} == ({"normal"} if change == "fresh_profile" else {"normal", "admin"})
            assert executor.oauth_provider == "oidc" and executor.oauth_id == "current"
            assert executor.authz_attributes == {}
        assert capacity.snapshot().running == capacity.snapshot().queued == 0
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_native_worker_resolves_on_gateway_loop(executor_module, tmp_path):
    host_loop = asyncio.get_running_loop()
    lookups = []

    async def resolve(batch):
        assert asyncio.get_running_loop() is host_loop
        lookups.append(batch["user_id"])
        return Principal(user_id=batch["user_id"], role="user")

    service = SubagentBatchService(repository=None, config=SubagentBatchesConfig(), runtime_config=SubagentRuntimeConfig(), resolve_principal=resolve)
    resolver = service._bind_principal_resolver({"user_id": "owner"})
    await resolver()  # Pre-discovery check on the host loop.
    executor = _executor(executor_module, resolver)
    pending = executor_module.run_on_isolated_subagent_loop(executor._aexecute("OWNER_CHILD:second"))
    result = await asyncio.wait_for(asyncio.wrap_future(pending), 20)
    assert result.status == executor_module.SubagentStatus.COMPLETED, result.error
    assert lookups == ["owner", "owner"]
    observed = [json.loads(line) for line in (tmp_path / "calls.jsonl").read_text(encoding="utf-8").splitlines()]
    assert {call["tool"] for call in observed} == {"normal"}


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["provider", "subject", "promotion", "unchanged"])
async def test_canonical_sql_identity_is_fenced_after_native_capacity_wait(executor_module, accounts, tmp_path, change):  # noqa: F811
    from datetime import UTC, datetime

    from app.subagent_batches.identity import resolve_batch_principal

    repo, owner, _ = accounts
    saved_role = "user" if change == "promotion" else "admin"
    if change == "promotion":
        await repo.record_sign_in(str(owner.id), system_role="user", oauth_issuer=owner.oauth_issuer, last_sign_in_at=datetime.now(UTC))
    batch = {"user_id": str(owner.id), "execution_spec": {"user_role": saved_role, "oauth_provider": owner.oauth_provider, "oauth_id": owner.oauth_id}}
    host_loop = asyncio.get_running_loop()

    async def load_user(user_id):
        assert asyncio.get_running_loop() is host_loop
        return await repo.get_user_by_id(user_id)

    service = SubagentBatchService(repository=None, config=SubagentBatchesConfig(), runtime_config=SubagentRuntimeConfig(), resolve_principal=lambda candidate: resolve_batch_principal(candidate, load_user=load_user))
    resolver = service._bind_principal_resolver(batch)
    assert (await resolver()).role == saved_role
    capacity = SubagentExecutionCapacity(SubagentRuntimeConfig(max_running=1))
    executor = _executor(executor_module, resolver, capacity, user_id=str(owner.id), user_role=saved_role)
    held_slot = capacity.slot()
    slot_held = False
    task = None
    try:
        # Production capacity is bound to the execution loop; all slot changes
        # stay there while the canonical account loader stays on the host loop.
        await asyncio.wrap_future(executor_module.run_on_isolated_subagent_loop(held_slot.__aenter__()))
        slot_held = True
        pending = executor_module.run_on_isolated_subagent_loop(executor._aexecute("OWNER_CHILD:second"))
        task = asyncio.wrap_future(pending)
        async with asyncio.timeout(5):
            while capacity.snapshot().queued != 1:
                await asyncio.sleep(0)
        if change == "promotion":
            await repo.record_sign_in(str(owner.id), system_role="admin", oauth_issuer=owner.oauth_issuer, last_sign_in_at=datetime.now(UTC))
        elif change in {"provider", "subject"}:
            field = "oauth_provider" if change == "provider" else "oauth_id"
            await repo.update_user(owner.model_copy(update={field: "changed"}))
        await asyncio.wrap_future(executor_module.run_on_isolated_subagent_loop(held_slot.__aexit__(None, None, None)))
        slot_held = False
        result = await asyncio.wait_for(task, 20)
        assert result.execution_done_event.is_set()
        if change in {"provider", "subject"}:
            assert result.status == executor_module.SubagentStatus.FAILED
            assert result.started_at is None
            assert not (tmp_path / "calls.jsonl").exists()
            assert not (tmp_path / "reached-second").exists()
        else:
            assert result.status == executor_module.SubagentStatus.COMPLETED, result.error
            calls = [json.loads(line) for line in (tmp_path / "calls.jsonl").read_text(encoding="utf-8").splitlines()]
            assert {call["tool"] for call in calls} == ({"normal"} if change == "promotion" else {"normal", "admin"})
            assert executor.user_role == saved_role
        assert capacity.snapshot().running == capacity.snapshot().queued == 0
    finally:
        if slot_held:
            await asyncio.wrap_future(executor_module.run_on_isolated_subagent_loop(held_slot.__aexit__(None, None, None)))
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
