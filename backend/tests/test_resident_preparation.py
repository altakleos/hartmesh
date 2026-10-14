"""Resident preparation barriers with a real registry and no native effects."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from _sandbox_provider_fakes import _make_provider
from test_agent_execution_runtime import execution as make_execution

from deerflow.agent_instances import runtime
from deerflow.agent_instances.contract import AgentDenied, ResidentFilesFull, ResidentStartFailed
from deerflow.agent_instances.residents import ResidentRegistry
from deerflow.spaces.attachments import AttachmentPending, ResourceMount


def setup(tmp_path, monkeypatch):
    provider, _backend = _make_provider(tmp_path, monkeypatch)
    adapter = SimpleNamespace(host_id="host", _same_host=Mock())
    attached = SimpleNamespace(id="d" * 32, container_id="e" * 64)
    mounts = SimpleNamespace(
        provider=adapter,
        resume=AsyncMock(return_value=None),
        attach=AsyncMock(return_value=attached),
        inspect_preparation=AsyncMock(return_value=None),
        contain_host=AsyncMock(return_value=False),
        unfenced=AsyncMock(return_value=None),
        container_state=AsyncMock(return_value="unknown"),
    )
    manager = SimpleNamespace(task_runs=Mock(return_value={}))
    registry = ResidentRegistry(attachments=mounts, provider=provider, run_manager=manager, settle=AsyncMock(return_value=[]), owner_of=AsyncMock(return_value=None), replicas=2, idle_timeout=600)
    files = SimpleNamespace(quota=AsyncMock(return_value={"available_bytes": 32 << 20, "available_inodes": 1024}))
    authority = SimpleNamespace(instances=SimpleNamespace(files=files, residents=registry), validate=AsyncMock())
    execution = replace(make_execution(), authority=authority)
    environment = runtime.ExecutionEnvironment(execution)
    monkeypatch.setattr(runtime, "current_environment", lambda: environment)
    monkeypatch.setattr(runtime, "qualified_attachments", AsyncMock(return_value=(provider, mounts)))
    from deerflow.agent_instances import public_skills

    monkeypatch.setattr(public_skills, "capture_public_skills", lambda *_args: SimpleNamespace(revision="capture", files=[]))
    config = SimpleNamespace()
    sandbox = SimpleNamespace(id=attached.id, close=Mock(), requires_container_recycle=False, execution_outcome_uncertain=False)
    client = runtime.InstanceSandboxProvider(execution, sandbox, app_config=config, skill_revision="capture")

    async def prepare_client(*_args, **_kwargs):
        assert not registry.entry(execution.instance).lock.locked()
        assert environment.attachment_id == attached.id
        return client

    monkeypatch.setattr(runtime, "_prepare_client", prepare_client, raising=False)
    return execution, environment, registry, mounts, attached, config, client


async def prepare(parts):
    execution, _environment, _registry, _mounts, _attached, config, _client = parts
    return await runtime.prepare_environment(execution, app_config=config, run_id="run")


@pytest.mark.anyio
async def test_new_preparation_claims_exact_attachment_before_client_and_publishes_handoff(tmp_path, monkeypatch):
    parts = setup(tmp_path, monkeypatch)
    execution, environment, registry, mounts, attached, _config, client = parts
    assert await prepare(parts) is client
    assert environment.provider is client and environment.attachment_id == attached.id
    entry = registry.entry(execution.instance)
    assert entry.attachment_id == attached.id and entry.preparation is None
    assert registry.slots == {execution.instance.home_id}
    mounts.attach.assert_awaited_once()
    assert mounts.attach.call_args.kwargs["resources"] == [ResourceMount(execution.instance.home_id, 1, "home", True)]


@pytest.mark.anyio
async def test_full_home_never_invokes_attach_and_clears_only_preinvocation_claim(tmp_path, monkeypatch):
    parts = setup(tmp_path, monkeypatch)
    execution, _environment, registry, mounts, *_ = parts
    execution.authority.instances.files.quota.return_value["available_bytes"] = 1
    with pytest.raises(ResidentFilesFull):
        await prepare(parts)
    assert registry.entry(execution.instance).preparation is None and not registry.slots
    mounts.attach.assert_not_awaited()
    mounts.inspect_preparation.assert_not_awaited()
    mounts.contain_host.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("host_ok", [False, True])
async def test_drained_failed_attach_without_intent_needs_fresh_host_proof_to_clear_claim(tmp_path, monkeypatch, host_ok):
    parts = setup(tmp_path, monkeypatch)
    execution, _environment, registry, mounts, *_ = parts
    mounts.attach.side_effect = RuntimeError("failed start")
    if not host_ok:
        mounts.provider._same_host.side_effect = RuntimeError("unconfirmed host")
    with pytest.raises(ResidentStartFailed):
        await prepare(parts)
    entry = registry.entry(execution.instance)
    assert (entry.preparation is None) == host_ok
    assert bool(registry.slots) != host_ok
    mounts.unfenced.assert_not_awaited()
    mounts.contain_host.assert_not_awaited()
    assert mounts.inspect_preparation.call_args.kwargs["host_id"] == "host"


@pytest.mark.anyio
async def test_unreadable_attach_result_retains_opaque_claim_and_slot(tmp_path, monkeypatch):
    parts = setup(tmp_path, monkeypatch)
    execution, environment, registry, mounts, *_ = parts
    mounts.attach.side_effect = RuntimeError("commit outcome unknown")
    mounts.inspect_preparation.side_effect = RuntimeError("SQL unavailable")
    with pytest.raises(ResidentStartFailed):
        await prepare(parts)
    assert registry.entry(execution.instance).preparation is not None
    assert registry.slots == {execution.instance.home_id} and environment.attachment_id is None
    mounts.unfenced.assert_not_awaited()
    mounts.contain_host.assert_not_awaited()


@pytest.mark.anyio
async def test_matching_durable_row_retains_actual_cleanup_identity_after_attach_error(tmp_path, monkeypatch):
    parts = setup(tmp_path, monkeypatch)
    execution, environment, registry, mounts, attached, *_ = parts
    mounts.attach.side_effect = RuntimeError("result lost")
    mounts.inspect_preparation.return_value = SimpleNamespace(id=attached.id, phase="pending")
    with pytest.raises(ResidentStartFailed):
        await prepare(parts)
    assert environment.attachment_id == attached.id and environment.recycle_required
    assert registry.entry(execution.instance).attachment_id == attached.id
    assert registry.entry(execution.instance).preparation is None
    mounts.contain_host.assert_awaited_once_with(attached.id, "start-failed")
    mounts.unfenced.assert_not_awaited()


@pytest.mark.anyio
async def test_repeated_cancellation_drains_attach_and_preserves_returned_client_handoff(tmp_path, monkeypatch):
    parts = setup(tmp_path, monkeypatch)
    execution, environment, registry, mounts, attached, _config, client = parts
    entered, release = asyncio.Event(), asyncio.Event()

    async def attach(**_kwargs):
        entered.set()
        await release.wait()
        return attached

    mounts.attach.side_effect = attach
    task = asyncio.create_task(prepare(parts))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and registry.entry(execution.instance).lock.locked()
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert environment.provider is client and environment.attachment_id == attached.id
        assert not registry.entry(execution.instance).lock.locked()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
async def test_authority_revoked_during_capacity_wait_is_rechecked_before_attach(tmp_path, monkeypatch):
    parts = setup(tmp_path, monkeypatch)
    execution, _environment, registry, mounts, *_ = parts
    original = registry.take_slot

    async def take_slot(*args, **kwargs):
        await original(*args, **kwargs)
        execution.authority.validate.side_effect = AgentDenied("revoked")

    monkeypatch.setattr(registry, "take_slot", take_slot)
    with pytest.raises(AgentDenied, match="revoked"):
        await prepare(parts)
    assert registry.entry(execution.instance).preparation is None and not registry.slots
    mounts.attach.assert_not_awaited()


@pytest.mark.anyio
async def test_readiness_failure_requests_exact_recycle_outside_home_lock(tmp_path, monkeypatch):
    parts = setup(tmp_path, monkeypatch)
    execution, environment, registry, mounts, attached, *_ = parts

    async def failed(*_args, **_kwargs):
        assert not registry.entry(execution.instance).lock.locked()
        raise RuntimeError("not ready")

    monkeypatch.setattr(runtime, "_prepare_client", failed)
    with pytest.raises(ResidentStartFailed):
        await prepare(parts)
    assert environment.attachment_id == attached.id and environment.recycle_required
    mounts.contain_host.assert_awaited_once_with(attached.id, "start-failed")


@pytest.mark.anyio
async def test_final_authorization_denial_closes_client_and_never_recycles_workspace(tmp_path, monkeypatch):
    parts = setup(tmp_path, monkeypatch)
    execution, environment, _registry, mounts, _attached, _config, client = parts
    execution.authority.validate.side_effect = [None, None, AgentDenied("late revoked")]
    with pytest.raises(AgentDenied, match="late revoked"):
        await prepare(parts)
    assert environment.provider is client and not environment.recycle_required
    client.sandbox.close.assert_called_once()
    mounts.contain_host.assert_not_awaited()


@pytest.mark.anyio
async def test_resume_unknown_liveness_exhausts_retries_without_containment(tmp_path, monkeypatch):
    parts = setup(tmp_path, monkeypatch)
    execution, _environment, registry, mounts, attached, *_ = parts
    mounts.resume.side_effect = AttachmentPending("verification failed")
    mounts.unfenced.return_value = SimpleNamespace(id=attached.id, phase="active", container_id=attached.container_id)
    registry.claimed(execution.instance, attached.id)
    from deerflow.agent_instances import resident_preparation

    monkeypatch.setattr(resident_preparation, "RESUME_RETRY_SECONDS", 0)
    with pytest.raises(ResidentStartFailed):
        await prepare(parts)
    assert mounts.resume.await_count == 4
    mounts.contain_host.assert_not_awaited()
    mounts.attach.assert_not_awaited()


@pytest.mark.anyio
async def test_resume_scope_mismatch_never_adopts_or_contains_unrelated_attachment(tmp_path, monkeypatch):
    parts = setup(tmp_path, monkeypatch)
    execution, _environment, registry, mounts, *_ = parts
    mounts.resume.side_effect = AttachmentPending("pending row")
    mounts.unfenced.side_effect = AttachmentPending("foreign actor or extra mount")
    with pytest.raises(ResidentStartFailed):
        await prepare(parts)
    assert registry.entry(execution.instance).attachment_id is None
    assert mounts.unfenced.call_args.kwargs == {
        "actor": execution.instance.principal,
        "resources": [ResourceMount(execution.instance.home_id, 1, "home", True)],
        "host_id": "host",
    }
    mounts.contain_host.assert_not_awaited()
    mounts.attach.assert_not_awaited()


@pytest.mark.anyio
async def test_failed_qualification_maps_to_start_failed_before_any_home_lock(tmp_path, monkeypatch):
    from deerflow.spaces.docker import StorageHostUnqualified

    parts = setup(tmp_path, monkeypatch)
    _execution, _environment, registry, mounts, *_ = parts
    monkeypatch.setattr(runtime, "qualified_attachments", AsyncMock(side_effect=StorageHostUnqualified("not qualified")))
    with pytest.raises(ResidentStartFailed):
        await prepare(parts)
    assert not registry.homes
    mounts.resume.assert_not_awaited()


@pytest.mark.anyio
async def test_final_denial_survives_client_close_failure_and_records_cleanup_uncertainty(tmp_path, monkeypatch):
    parts = setup(tmp_path, monkeypatch)
    execution, environment, _registry, mounts, _attached, _config, client = parts
    execution.authority.validate.side_effect = [None, None, AgentDenied("late revoked")]
    client.sandbox.close.side_effect = RuntimeError("close failed")
    with pytest.raises(AgentDenied, match="late revoked"):
        await prepare(parts)
    assert environment.provider is client and environment.recycle_required
    mounts.contain_host.assert_not_awaited()


@pytest.mark.anyio
async def test_two_first_preparations_serialize_attachment_selection_then_share_one_workspace(tmp_path, monkeypatch):
    parts = setup(tmp_path, monkeypatch)
    execution, first_environment, registry, mounts, attached, config, _client = parts
    second_environment = runtime.ExecutionEnvironment(execution)
    monkeypatch.setattr(runtime, "current_environment", lambda: runtime._environment.get())
    entered, release = asyncio.Event(), asyncio.Event()
    mounts.resume.side_effect = [None, attached]

    async def attach(**_kwargs):
        entered.set()
        await release.wait()
        return attached

    async def prepare_client(*_args, **_kwargs):
        assert not registry.entry(execution.instance).lock.locked()
        return SimpleNamespace(sandbox=SimpleNamespace(id=attached.id), close=Mock())

    async def scoped(environment, run_id):
        token = runtime._environment.set(environment)
        try:
            return await runtime.prepare_environment(execution, app_config=config, run_id=run_id)
        finally:
            runtime._environment.reset(token)

    mounts.attach.side_effect = attach
    monkeypatch.setattr(runtime, "_prepare_client", prepare_client)
    first = asyncio.create_task(scoped(first_environment, "first"))
    second = None
    try:
        await asyncio.wait_for(entered.wait(), 2)
        second = asyncio.create_task(scoped(second_environment, "second"))
        await asyncio.sleep(0.02)
        assert mounts.resume.await_count == 1 and not second.done()
        release.set()
        await asyncio.gather(first, second)
        mounts.attach.assert_awaited_once()
        assert first_environment.attachment_id == second_environment.attachment_id == attached.id
        assert first_environment.provider is not second_environment.provider
    finally:
        release.set()
        await asyncio.gather(*(task for task in (first, second) if task is not None), return_exceptions=True)


@pytest.mark.anyio
async def test_cancellation_during_client_readiness_retains_handoff_without_holding_home_lock(tmp_path, monkeypatch):
    parts = setup(tmp_path, monkeypatch)
    execution, environment, registry, _mounts, _attached, _config, client = parts
    entered, release = asyncio.Event(), asyncio.Event()

    async def prepare_client(*_args, **_kwargs):
        assert not registry.entry(execution.instance).lock.locked()
        entered.set()
        await release.wait()
        return client

    monkeypatch.setattr(runtime, "_prepare_client", prepare_client)
    task = asyncio.create_task(prepare(parts))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert environment.provider is client
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
