"""Actual owner/evidence proof is offloaded; publication remains memory-only."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_bootstrap_primary_inventory import setup as setup_native_census
from test_gateway_resident_native_accounting import runtime as build_gateway_runtime
from test_native_create_provider import fixture as native_fixture
from test_native_create_provider import install

from deerflow.agent_instances.bootstrap import NativeAccountingProof, ResidentBootstrapCoordinator
from deerflow.agent_instances.residents import ResidentRegistry
from deerflow.sandbox.admission import ProviderAdmission


@pytest.mark.asyncio
async def test_bootstrap_native_owner_checks_and_shutdown_do_not_block_loop(tmp_path):
    fixture = native_fixture.__wrapped__(tmp_path)

    def prepare():
        provider, owner = next(fixture)
        install(provider, owner)
        return provider

    provider = await asyncio.to_thread(prepare)
    try:
        admission = ProviderAdmission()
        provider._bind_runtime_admission(admission)
        registry = ResidentRegistry(
            attachments=SimpleNamespace(retained_claims=AsyncMock(return_value=())),
            provider=provider,
            run_manager=SimpleNamespace(task_runs=lambda _key: {}),
            settle=AsyncMock(return_value=[]),
            owner_of=AsyncMock(return_value=None),
            replicas=3,
            idle_timeout=60,
        )

        async def account(permit):
            return NativeAccountingProof.capture(provider, permit, validate_locked=lambda: None)

        coordinator = ResidentBootstrapCoordinator(
            registry=registry,
            admission=admission,
            entered=asyncio.get_running_loop().time(),
            drain_producers=AsyncMock(),
            account_native=account,
            reconcile=AsyncMock(return_value=True),
            drain_workers=AsyncMock(),
        )
        assert await coordinator.poll(timeout=2) is not None
        admission.require(provider)
        await coordinator.close()
    finally:
        await asyncio.to_thread(fixture.close)


@pytest.mark.asyncio
async def test_gateway_complete_native_census_and_final_drain_do_not_block_loop(tmp_path, monkeypatch):
    fixture = native_fixture.__wrapped__(tmp_path)

    def prepare():
        native = next(fixture)
        return native, setup_native_census(native)

    native, census = await asyncio.to_thread(prepare)
    monkeypatch.setattr("test_gateway_resident_native_accounting.setup", lambda _fixture: census)
    host, provider, admission, _local, calls = build_gateway_runtime(native)
    permit = await asyncio.to_thread(provider._begin_bootstrap_accounting, admission)
    try:
        proof = await host.account_native(permit)
        provider.external_changed(lambda: proof.require_current_locked(provider, permit))
        assert calls and not provider._sandboxes
        await host.drain_workers()
    finally:
        await asyncio.to_thread(provider._end_bootstrap_accounting, permit)
        await asyncio.to_thread(fixture.close)
