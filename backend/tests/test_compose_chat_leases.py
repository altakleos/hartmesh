"""Retained clients require fresh authority, including existing owner bindings."""

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_sandbox_leases import _LeaseProvider

from deerflow.sandbox.lease import SandboxLeaseManager
from deerflow.spaces.backings import BackingUnavailable


def test_deprecated_runtime_getter_also_refreshes_storage_before_return(monkeypatch):
    from deerflow.sandbox import tools

    provider = StorageProvider()
    provider.revoked = True
    runtime = SimpleNamespace(state={"sandbox": {"sandbox_id": "shared"}}, context={"thread_id": "chat", "user_id": "alice"}, config={})
    monkeypatch.setattr(tools, "get_sandbox_provider", lambda: provider)
    monkeypatch.setattr(tools, "resolve_runtime_user_id", lambda runtime: "alice")
    with pytest.raises(BackingUnavailable):
        tools.sandbox_from_runtime(runtime)
    assert provider.validations[0][1]["thread_id"] == "chat"
    assert provider.release_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_missing_active_borrower_settles_pending_final_cleanup(asynchronous):
    from deerflow.sandbox.lease import cleanup_unheld_sandbox, discard_sandbox_lease_manager, get_sandbox_lease_manager

    provider = StorageProvider()
    manager = get_sandbox_lease_manager(provider)
    try:
        manager.retain("borrower", "shared", thread_id="chat", user_id="alice", release_on_last=False)
        assert not cleanup_unheld_sandbox(provider, "shared", lambda: pytest.fail("A held client was destroyed"))
        provider.get = lambda sandbox_id: None
        if asynchronous:
            assert await manager._active_owner_binding_async("borrower", ("alice", "chat"), release_on_last=False) is None
        else:
            assert manager._active_owner_binding("borrower", ("alice", "chat"), release_on_last=False) is None
        assert provider.release_calls == ["shared"]
        assert manager.binding_for("borrower") is None
    finally:
        discard_sandbox_lease_manager(provider)


@pytest.mark.asyncio
async def test_ownerless_release_drains_through_repeated_cancellation():
    from deerflow.sandbox.tools import _rollback_failed_sandbox_lookup_async

    provider = _LeaseProvider()
    entered, finish, returned = (threading.Event() for _ in range(3))

    def release(sandbox_id):
        entered.set()
        assert finish.wait(5)
        returned.set()

    provider.release = release
    task = asyncio.create_task(_rollback_failed_sandbox_lookup_async(provider, "shared", None))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(0.02)
        task.cancel()
        await asyncio.sleep(0.02)
        assert not task.done() and not returned.is_set()
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert returned.is_set()
    finally:
        finish.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("replacement", [False, True])
@pytest.mark.parametrize("cancelled", [False, True])
async def test_tool_without_execution_holder_rechecks_storage(tmp_path, monkeypatch, asynchronous, replacement, cancelled):
    import asyncio

    from deerflow.sandbox import tools

    provider = StorageProvider()
    provider.revoked = True
    if replacement:
        provider.get_scoped = lambda *args, **kwargs: None
    if cancelled:

        def refuse(*args, **kwargs):
            raise asyncio.CancelledError()

        provider.validate_storage_candidate = refuse
    runtime = SimpleNamespace(state={"sandbox": {"sandbox_id": "shared"}}, context={"thread_id": "chat", "user_id": "alice"}, config={})
    monkeypatch.setattr(tools, "get_sandbox_provider", lambda: provider)
    monkeypatch.setattr(tools, "resolve_runtime_user_id", lambda runtime: "alice")
    monkeypatch.setattr(tools, "authorize_sandbox_execution", lambda **kwargs: None)
    monkeypatch.setattr(tools, "authorize_sandbox_execution_async", AsyncMock())
    monkeypatch.setattr(tools, "safe_app_config", lambda: None)
    monkeypatch.setattr(tools, "safe_app_config_async", AsyncMock(return_value=None))
    with pytest.raises(asyncio.CancelledError if cancelled else BackingUnavailable):
        if asynchronous:
            await tools.ensure_sandbox_initialized_async(runtime)
        else:
            tools.ensure_sandbox_initialized(runtime)
    assert len(provider.acquire_calls) == int(replacement)
    assert provider.release_calls == (["shared"] if replacement else [])


class StorageProvider(_LeaseProvider):
    requires_chat_storage_validation = True

    def __init__(self):
        super().__init__()
        self.revoked = False
        self.current = True
        self.validations = []

    def validate_storage_candidate(self, sandbox_id, **scope):
        self.validations.append((sandbox_id, scope))
        if self.revoked:
            raise BackingUnavailable("Current authority was revoked")
        return self.sandbox

    async def validate_storage_candidate_async(self, sandbox_id, **scope):
        return self.validate_storage_candidate(sandbox_id, **scope)

    def storage_candidate_is_current(self, token):
        return self.current and token is self.sandbox


@pytest.mark.parametrize("operation", ["retain", "acquire", "reuse", "existing"])
@pytest.mark.parametrize("failure", ["revocation", "replacement"])
def test_sync_lease_doors_refuse_stale_storage(operation, failure):
    provider = StorageProvider()
    manager = SandboxLeaseManager(provider)
    try:
        if operation == "existing":
            manager.retain("owner", "shared", thread_id="chat", user_id="alice")
        provider.revoked = failure == "revocation"
        provider.current = failure != "replacement"
        with pytest.raises(BackingUnavailable):
            if operation == "retain":
                manager.retain("owner", "shared", thread_id="chat", user_id="alice")
            elif operation == "reuse":
                manager.reuse_or_acquire("owner", "shared", thread_id="chat", user_id="alice")
            else:
                manager.acquire("owner", "chat", user_id="alice")
        if operation != "existing":
            assert manager.binding_for("owner") is None
    finally:
        manager.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["retain", "acquire", "reuse", "existing"])
@pytest.mark.parametrize("failure", ["revocation", "replacement"])
async def test_async_lease_doors_refuse_stale_storage(operation, failure):
    provider = StorageProvider()
    manager = SandboxLeaseManager(provider)
    try:
        if operation == "existing":
            await manager.retain_async("owner", "shared", thread_id="chat", user_id="alice")
        provider.revoked = failure == "revocation"
        provider.current = failure != "replacement"
        with pytest.raises(BackingUnavailable):
            if operation == "retain":
                await manager.retain_async("owner", "shared", thread_id="chat", user_id="alice")
            elif operation == "reuse":
                await manager.reuse_or_acquire_async("owner", "shared", thread_id="chat", user_id="alice")
            else:
                await manager.acquire_async("owner", "chat", user_id="alice")
        if operation != "existing":
            assert manager.binding_for("owner") is None
    finally:
        manager.close()
