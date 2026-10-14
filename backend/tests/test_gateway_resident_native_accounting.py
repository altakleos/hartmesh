"""Gateway census proof uses retained native identities, with fake Docker only."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_bootstrap_primary_inventory import setup
from test_native_create_provider import fixture as fixture
from test_native_slot_inventory import CID, SLOT

from deerflow.agent_instances.residents import ResidentRegistry
from deerflow.sandbox.admission import ProviderAdmission
from deerflow.sandbox.exceptions import SandboxRuntimeAdmissionError
from deerflow.spaces.runtime_owner import StorageRuntimeUnavailable


def runtime(fixture):
    from app.gateway.resident_runtime import ResidentRuntime

    local, _bridge, calls = setup(fixture)
    original_run = local._runtime_runner

    def complete_census(command, **kwargs):
        if command[1:3] == ["network", "ls"] or "label=deerflow.role=resident" in command:
            calls.append(command)
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        if command[1] == "ps" and any(selector in command for selector in ("label=deerflow.sandbox_id", "label=deerflow.role=network-proxy", "name=^/deer-flow-netproxy-")):
            calls.append(command)
            return SimpleNamespace(returncode=0, stdout="", stderr="")
        return original_run(command, **kwargs)

    local._runtime_runner = complete_census
    provider, _owner = fixture
    admission = ProviderAdmission()
    provider._bind_runtime_admission(admission)
    files = SimpleNamespace(catalog=SimpleNamespace(volumes={SLOT: SimpleNamespace(mount_path="/pool/slot", image_path="/pool/image")}), registry=SimpleNamespace(_sf=None))
    app = SimpleNamespace(state=SimpleNamespace(storage_spaces=files, agent_instances=SimpleNamespace(_sf=None), run_store=SimpleNamespace(list_inflight=AsyncMock(return_value=[]))))
    residents = ResidentRegistry(
        attachments=SimpleNamespace(on_contained=None, provider=SimpleNamespace(bootstrap_residents=lambda **_kwargs: (), readonly_roots=AsyncMock(return_value=()), _configured_mounts=())),
        provider=provider,
        run_manager=SimpleNamespace(task_runs=lambda _key: {}, _runs={}),
        settle=AsyncMock(return_value=[]),
        owner_of=AsyncMock(return_value=None),
        replicas=3,
        idle_timeout=60,
    )
    host = ResidentRuntime(app, registry=residents, admission=admission, entered=0)
    return host, provider, admission, local, calls


@pytest.mark.anyio
async def test_gateway_native_proof_retains_actual_parked_identity_and_no_sdk(fixture):
    host, provider, admission, _local, calls = runtime(fixture)
    permit = provider._begin_bootstrap_accounting(admission)
    try:
        proof = await host.account_native(permit)
        provider.external_changed(lambda: proof.require_current_locked(provider, permit))
        assert provider._warm_pool["native"][0].container_id == CID
        assert provider._warm_pool["native"][0]._bootstrap_accounting_only
        assert not provider._sandboxes
        assert all(command[1] in {"ps", "inspect", "network"} for command in calls)
    finally:
        provider._end_bootstrap_accounting(permit)


@pytest.mark.anyio
async def test_untracked_resident_cannot_publish_a_false_empty_baseline(fixture):
    from deerflow.spaces.docker import ResidentPrimary

    host, provider, admission, _local, _calls = runtime(fixture)
    host.attachments.provider.bootstrap_residents = lambda **_kwargs: (ResidentPrimary("d" * 64, "hartmesh-space-" + "e" * 32, "2026-10-01T12:00:00Z", "e" * 32, "daemon-A", (SLOT,), (("/pool/slot/data", "/mnt/spaces/home", True),)),)
    permit = provider._begin_bootstrap_accounting(admission)
    try:
        with pytest.raises(StorageRuntimeUnavailable):
            await host.account_native(permit)
        with pytest.raises(SandboxRuntimeAdmissionError):
            admission.require(provider)
    finally:
        provider._end_bootstrap_accounting(permit)


@pytest.mark.anyio
async def test_secondary_without_a_primary_remains_a_native_accounting_blocker(fixture):
    import json

    host, provider, admission, local, _calls = runtime(fixture)
    original = local._runtime_runner
    proxy = "f" * 64
    entry = {
        "Id": proxy,
        "Name": "/orphan-proxy",
        "Created": "2026-10-01T12:00:00Z",
        "Config": {"Labels": {"deerflow.role": "network-proxy", "deerflow.sandbox_id": "orphan", "deerflow.network_mode": "allowlist", "deerflow.network_policy_digest": "8" * 64}},
    }

    def run(command, **kwargs):
        if command[1] == "ps" and "label=deerflow.sandbox_id" in command:
            return SimpleNamespace(returncode=0, stdout=proxy, stderr="")
        if command[1:3] == ["inspect", proxy]:
            return SimpleNamespace(returncode=0, stdout=json.dumps([entry]), stderr="")
        return original(command, **kwargs)

    local._runtime_runner = run
    permit = provider._begin_bootstrap_accounting(admission)
    try:
        with pytest.raises(StorageRuntimeUnavailable, match="unexplained paired"):
            await host.account_native(permit)
        with pytest.raises(SandboxRuntimeAdmissionError):
            admission.require(provider)
    finally:
        provider._end_bootstrap_accounting(permit)


@pytest.mark.anyio
@pytest.mark.parametrize("change", ["identity", "producer", "pending", "catalog"])
async def test_gateway_census_proof_refuses_late_identity_or_producer_change(fixture, change):
    host, provider, admission, _local, _calls = runtime(fixture)
    permit = provider._begin_bootstrap_accounting(admission)
    try:
        proof = await host.account_native(permit)
        if change == "identity":
            provider._warm_pool["native"][0].container_id = "d" * 64
        elif change == "producer":
            provider._runtime_producers = {object()}
        elif change == "catalog":
            host.files.catalog.volumes.clear()
        else:
            labels = {"deerflow.role": "sandbox", "deerflow.owner_user_id": "bob", "deerflow.thread_id": "another", "deerflow.sandbox_id": "other", "hartmesh.chat.storage": "on"}
            provider._native_creation.begin(name="another-native", labels=labels)
        with pytest.raises(StorageRuntimeUnavailable):
            provider.external_changed(lambda: proof.require_current_locked(provider, permit))
    finally:
        provider._runtime_producers = set()
        provider._end_bootstrap_accounting(permit)


@pytest.mark.anyio
async def test_failed_later_run_inventory_closes_admission_and_preserves_native_charge(fixture):
    host, provider, admission, _local, _calls = runtime(fixture)
    permit = provider._begin_bootstrap_accounting(admission)
    try:
        await host.account_native(permit)
        admission.open(provider)
        host.app.state.run_store.list_inflight.side_effect = RuntimeError("Lost complete inventory")
        with pytest.raises(StorageRuntimeUnavailable):
            await host.inventory_runs()
        with pytest.raises(SandboxRuntimeAdmissionError):
            admission.require(provider)
        assert provider._warm_pool["native"][0].container_id == CID
    finally:
        provider._end_bootstrap_accounting(permit)


@pytest.mark.anyio
async def test_gateway_install_reuses_exact_registry_and_preserves_existing_native_charges(fixture):
    from app.gateway.resident_runtime import install_resident_runtime
    from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo

    host, provider, admission, _local, _calls = runtime(fixture)
    attachments = SimpleNamespace(on_contained=None, uncontained={}, provider=SimpleNamespace(host_id="daemon-A"), retained_claims=AsyncMock(return_value=()))
    app = host.app
    app.state.resident_runtime = None
    app.state.chat_storage_host = SimpleNamespace(admission=admission)
    app.state.run_manager = host.registry.run_manager
    app.state.agent_instances.residents = None
    original = SandboxInfo("retained", "", container_id="f" * 64, container_name="retained-native", owner=("alice", "retained"))
    provider._warm_pool["retained"] = (original, 0)
    before = provider.external_slots
    first = install_resident_runtime(app, provider=provider, attachments=attachments, entered=0)
    second = install_resident_runtime(app, provider=provider, attachments=attachments, entered=1)
    assert first is second and app.state.agent_instances.residents is first.registry
    assert first.registry.attachments is attachments and attachments.on_contained == first.registry.contained
    assert provider._warm_pool["retained"][0] is original and provider.external_slots == before
    with pytest.raises(SandboxRuntimeAdmissionError):
        admission.require(provider)
    with pytest.raises(StorageRuntimeUnavailable):
        install_resident_runtime(app, provider=provider, attachments=SimpleNamespace(), entered=2)
