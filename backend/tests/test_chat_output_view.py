"""Only the original live host routing can qualify a missing managed view.

Directory-backed fixtures test admission and routing, never native disk limits.
"""

from dataclasses import replace
from datetime import UTC, datetime

import pytest
from test_compose_account_seats import seated_accounts as seated_accounts
from test_compose_user_assignment import account_storage as account_storage

from app.gateway import chat_storage
from app.gateway.auth.models import User
from app.gateway.storage_spaces import assign_user_space
from deerflow.config.paths import Paths, get_paths, paths_scope
from deerflow.persistence.user.model import UserRow
from deerflow.runtime.runs.capture_io import CaptureIOOwner, capture_io_scope
from deerflow.runtime.user_context import reset_current_user, set_current_user
from deerflow.spaces.facade import storage_credential_scope


def request_for(prepared, owner, user_id):
    from deerflow.runtime.runs.output_view import OutputViewRequest

    return OutputViewRequest(owner_user_id=user_id, thread_id="chat-a", run_id="run-a", paths=get_paths(), prepared=prepared, io_owner=owner)


@pytest.mark.asyncio
@pytest.mark.parametrize("credential", [True, False])
async def test_original_host_scope_qualifies_without_acquiring_a_sandbox(account_storage, monkeypatch, credential):
    from deerflow.sandbox import sandbox_provider

    app, repo, _sf, _files, _catalog, _company = account_storage
    user = await repo.create_user(User(email="no-view@example.com"))
    monkeypatch.setattr(sandbox_provider, "get_initialized_sandbox_provider", lambda: None)

    def never_acquire(*args, **kwargs):
        pytest.fail("A model-only output view must not initialize a provider")

    monkeypatch.setattr(sandbox_provider, "get_sandbox_provider", never_acquire)
    token = set_current_user(user)
    owner = CaptureIOOwner()
    try:
        with capture_io_scope(owner), storage_credential_scope(credential):
            async with chat_storage.chat_storage_lifecycle(app, enabled=True):
                qualify = getattr(app.state, "qualify_output_view", None)
                assert callable(qualify), "Gateway must install the explicit host qualifier"
                async with app.state.chat_storage_scope(user_id=str(user.id), thread_id="chat-a") as prepared:
                    request = request_for(prepared, owner, str(user.id))
                    lease = await qualify(request)
                    assert lease is not None
                    view = await lease.confirm(request)
                    assert view.kind == "no_admitted_output_view"
                    assert view.scope.kind == "managed_output_routing"
                    assert prepared.mounts.binds == ()
                    lease.retire()
    finally:
        owner.retire()
        await owner.drain()
        reset_current_user(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["copy", "owner_retired", "owner_retired_without_files", "user_admitted", "credential", "paths", "host", "provider", "capture_owner", "scope_closed"])
async def test_host_qualification_refuses_changed_or_copied_authority(account_storage, monkeypatch, tmp_path, change):
    from deerflow.runtime.runs.output import OutputEvidenceError
    from deerflow.sandbox import sandbox_provider

    app, repo, sf, _files, _catalog, _company = account_storage
    user = await repo.create_user(User(email="changed-view@example.com"))
    if change == "owner_retired_without_files":
        app.state.storage_spaces = None
        monkeypatch.setattr("app.gateway.deps.get_user_repo", lambda: repo)
    monkeypatch.setattr(sandbox_provider, "get_initialized_sandbox_provider", lambda: None)
    token = set_current_user(user)
    owner = CaptureIOOwner()
    try:
        with capture_io_scope(owner), storage_credential_scope(True):
            async with chat_storage.chat_storage_lifecycle(app, enabled=True):
                qualify = getattr(app.state, "qualify_output_view", None)
                assert callable(qualify), "Gateway must install the explicit host qualifier"
                async with app.state.chat_storage_scope(user_id=str(user.id), thread_id="chat-a") as prepared:
                    request = request_for(prepared, owner, str(user.id))
                    if change == "copy":
                        assert await qualify(replace(request, prepared=replace(prepared))) is None
                        return
                    lease = await qualify(request)
                    assert lease is not None
                    if change in {"owner_retired", "owner_retired_without_files"}:
                        async with sf.begin() as session:
                            (await session.get(UserRow, str(user.id))).released_at = datetime.now(UTC)
                    elif change == "user_admitted":
                        await assign_user_space(app, user)
                    elif change == "host":
                        original = app.state.chat_storage_host
                        app.state.chat_storage_host = object()
                        try:
                            with pytest.raises(OutputEvidenceError):
                                await lease.confirm(request)
                        finally:
                            app.state.chat_storage_host = original
                        return
                    elif change == "credential":
                        with storage_credential_scope(False), pytest.raises(OutputEvidenceError):
                            await lease.confirm(request)
                        return
                    elif change == "paths":
                        with paths_scope(Paths(tmp_path / "other")), pytest.raises(OutputEvidenceError):
                            await lease.confirm(request)
                        return
                    elif change == "provider":
                        unknown_provider = object()
                        monkeypatch.setattr(sandbox_provider, "get_initialized_sandbox_provider", lambda: unknown_provider)
                    elif change == "capture_owner":
                        foreign = CaptureIOOwner()
                        try:
                            with capture_io_scope(foreign):
                                assert await qualify(replace(request, io_owner=foreign)) is None
                        finally:
                            foreign.retire()
                            await foreign.drain()
                        return
                    if change != "scope_closed":
                        with pytest.raises(OutputEvidenceError):
                            await lease.confirm(request)
                if change == "scope_closed":
                    with paths_scope(prepared.paths), pytest.raises(OutputEvidenceError):
                        await lease.confirm(request)
    finally:
        owner.retire()
        await owner.drain()
        reset_current_user(token)


@pytest.mark.asyncio
async def test_feature_off_does_not_infer_a_missing_view(tmp_path):
    from types import SimpleNamespace

    app = SimpleNamespace(state=SimpleNamespace(storage_spaces_enabled=False))
    owner = CaptureIOOwner()
    try:
        with capture_io_scope(owner), paths_scope(Paths(tmp_path)):
            async with chat_storage.chat_storage_lifecycle(app, enabled=False):
                qualify = getattr(app.state, "qualify_output_view", None)
                assert callable(qualify), "Explicit format support is independent of storage activation"
                assert await qualify(request_for(None, owner, "alice")) is None
    finally:
        owner.retire()
        await owner.drain()


@pytest.mark.asyncio
async def test_failed_storage_startup_withdraws_output_view_support(monkeypatch):
    from types import SimpleNamespace

    def refuse(_host):
        raise RuntimeError("startup refused")

    app = SimpleNamespace(state=SimpleNamespace())
    monkeypatch.setattr("deerflow.spaces.chat_bridge.install_chat_storage_host", refuse)
    monkeypatch.setattr("deerflow.sandbox.sandbox_provider.get_initialized_sandbox_provider", lambda: None)
    with pytest.raises(RuntimeError, match="startup refused"):
        async with chat_storage.chat_storage_lifecycle(app, enabled=True):
            pytest.fail("Failed startup cannot publish a usable host")
    assert app.state.qualify_output_view is None


@pytest.mark.asyncio
async def test_initial_unsupported_provider_holds_without_importing_native_adapters(account_storage, monkeypatch):
    app, repo, _sf, _files, _catalog, _company = account_storage
    user = await repo.create_user(User(email="unsupported-view@example.com"))
    await exercise_unsupported_provider(app, user, monkeypatch)


async def exercise_unsupported_provider(app, user, monkeypatch):
    import builtins

    provider = object()
    monkeypatch.setattr("deerflow.sandbox.sandbox_provider.get_initialized_sandbox_provider", lambda: provider)
    original_import = builtins.__import__

    def guard(name, *args, **kwargs):
        if name.startswith("deerflow.community.aio_sandbox"):
            pytest.fail("Refusing an unsupported provider must not cold-import native adapters")
        return original_import(name, *args, **kwargs)

    token, owner = set_current_user(user), CaptureIOOwner()
    try:
        with capture_io_scope(owner), storage_credential_scope(True):
            async with chat_storage.chat_storage_lifecycle(app, enabled=True):
                async with app.state.chat_storage_scope(user_id=str(user.id), thread_id="chat-a") as prepared:
                    with monkeypatch.context() as patch:
                        patch.setattr(builtins, "__import__", guard)
                        assert await app.state.qualify_output_view(request_for(prepared, owner, str(user.id))) is None
    finally:
        owner.retire()
        await owner.drain()
        reset_current_user(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("without_files", [False, True])
async def test_owner_retirement_during_final_routing_refresh_is_refused(account_storage, monkeypatch, without_files):
    import asyncio

    from deerflow.runtime.runs.output import OutputEvidenceError

    app, repo, sf, _files, _catalog, _company = account_storage
    user = await repo.create_user(User(email="retiring-view@example.com"))
    if without_files:
        app.state.storage_spaces = None
        monkeypatch.setattr("app.gateway.deps.get_user_repo", lambda: repo)
    monkeypatch.setattr("deerflow.sandbox.sandbox_provider.get_initialized_sandbox_provider", lambda: None)
    token, owner = set_current_user(user), CaptureIOOwner()
    entered, release = asyncio.Event(), asyncio.Event()
    original = chat_storage.prepare_chat_storage

    async def held(*args):
        result = await original(*args)
        entered.set()
        await release.wait()
        return result

    try:
        with capture_io_scope(owner), storage_credential_scope(True):
            async with chat_storage.chat_storage_lifecycle(app, enabled=True):
                async with app.state.chat_storage_scope(user_id=str(user.id), thread_id="chat-a") as prepared:
                    request = request_for(prepared, owner, str(user.id))
                    lease = await app.state.qualify_output_view(request)
                    assert lease is not None
                    monkeypatch.setattr(chat_storage, "prepare_chat_storage", held)
                    confirmation = asyncio.create_task(lease.confirm(request))
                    try:
                        await asyncio.wait_for(entered.wait(), 3)
                        async with sf.begin() as session:
                            (await session.get(UserRow, str(user.id))).released_at = datetime.now(UTC)
                        release.set()
                        with pytest.raises(OutputEvidenceError):
                            await confirmation
                    finally:
                        release.set()
                        await asyncio.gather(confirmation, return_exceptions=True)
    finally:
        owner.retire()
        await owner.drain()
        reset_current_user(token)
