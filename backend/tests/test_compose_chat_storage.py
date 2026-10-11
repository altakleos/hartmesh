"""Chat roots require fresh host identity, grants and backing qualification."""

import asyncio
import threading
from types import SimpleNamespace

import pytest
from sqlalchemy import delete
from test_compose_account_seats import seated_accounts as seated_accounts
from test_compose_user_assignment import account_storage as account_storage
from test_lead_prompt_script_guidance import render as render

from app.gateway import chat_storage
from app.gateway.auth.models import User
from app.gateway.storage_spaces import assign_user_space
from deerflow.config.paths import Paths, get_paths, paths_scope
from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.spaces.model import SpaceGrantRow
from deerflow.runtime.user_context import reset_current_user, set_current_user
from deerflow.spaces.contract import InvalidPrincipal
from deerflow.spaces.facade import storage_credential_scope


@pytest.mark.asyncio
async def test_unavailable_chat_keeps_every_pool_image_in_mount_exclusions(tmp_path, monkeypatch):
    from unittest.mock import AsyncMock

    from deerflow.spaces.contract import PrincipalRef
    from deerflow.spaces.facade import storage_actor_scope

    spec = SimpleNamespace(mount_path=tmp_path / "unused.volume", image_path=tmp_path / "unused.image")
    app = SimpleNamespace(state=SimpleNamespace(storage_spaces_enabled=True, storage_spaces=SimpleNamespace(catalog=SimpleNamespace(volumes={"unused": spec}))))
    monkeypatch.setattr(chat_storage, "storage_state", AsyncMock(return_value=SimpleNamespace(state="unavailable", reason="admission-failed")))
    with storage_actor_scope(PrincipalRef("human", "alice")), storage_credential_scope(True):
        prepared = await chat_storage.prepare_chat_storage(app, "alice", "chat-a")
    assert prepared.mounts.user is None
    assert set(prepared.protected_roots) == {spec.mount_path, spec.image_path}
    assert prepared.paths.protected_roots == prepared.protected_roots


async def prepared_fixture(account_storage):
    app, repo, sf, files, catalog, company = account_storage
    account = await repo.create_user(User(email="chat@example.com"))
    await assign_user_space(app, account)
    return app, account, sf, files, catalog, company


@pytest.mark.asyncio
async def test_pat_conversation_cleanup_is_narrow_and_never_enables_resource_credentials(account_storage, monkeypatch):
    from unittest.mock import AsyncMock

    from starlette.requests import Request

    from app.gateway import chat_cleanup
    from deerflow.spaces.contract import SpaceDenied
    from deerflow.spaces.facade import _current_actor

    app, account, _, _, _, _ = await prepared_fixture(account_storage)
    request = Request({"type": "http", "headers": [], "app": app})
    monkeypatch.setattr("app.gateway.deps.get_current_user_from_request", AsyncMock(return_value=account))
    token = set_current_user(account)
    try:
        with storage_credential_scope(False):
            cleanup = await chat_cleanup.prepare_conversation_cleanup(request, "chat-a")
            assert cleanup._volume is not None
            assert not cleanup.paths.has_personal_files
            with pytest.raises(SpaceDenied):
                _current_actor()
            general = await chat_storage.prepare_chat_storage(app, str(account.id), "chat-a")
            assert general.reason == "credential-unsupported" and general.mounts.user is None
    finally:
        reset_current_user(token)


@pytest.mark.asyncio
async def test_chat_storage_roots_mounts_and_scope_have_no_legacy_file_fallback(account_storage, tmp_path):
    app, account, sf, files, catalog, company = await prepared_fixture(account_storage)
    token = set_current_user(account)
    try:
        prepared = await chat_storage.prepare_chat_storage(app, str(account.id), "chat-a")
        paths = prepared.paths
        with paths_scope(paths):
            assert get_paths() is paths
            root = paths.thread_dir("chat-a", user_id=str(account.id)).parent.parent
            assert paths.thread_dir("chat-b", user_id=str(account.id)) == root / "conversations" / "chat-b"
            assert paths.user_files_dir(str(account.id)) == root / "files"
            assert paths.resolve_virtual_path("chat-a", "/mnt/user-data/outputs/result.txt", user_id=str(account.id), follow_symlinks=False) == root / "conversations/chat-a/user-data/outputs/result.txt"
            assert paths.host_thread_dir("chat-a", user_id=str(account.id)) == str(root / "conversations/chat-a")
        mounts = prepared.mounts
        assert len(mounts.binds) == 6
        destinations = {destination: readonly for _source, destination, readonly in mounts.binds}
        assert destinations == {"/mnt/user-data/workspace": False, "/mnt/user-data/uploads": False, "/mnt/user-data/outputs": False, "/mnt/user-data/files": False, "/mnt/user-data/shared": True, "/mnt/acp-workspace": True}
        assert len(mounts.labels) == 2
        assert all(key.startswith("hartmesh.storage.space.") and value == "1" for key, value in mounts.labels.items())
        assert not any("/projects" in source or "/control" in source for source, _dest, _ro in mounts.binds)
    finally:
        reset_current_user(token)


@pytest.mark.asyncio
async def test_missing_user_space_has_no_company_or_legacy_mounts(account_storage):
    app, repo, _sf, _files, _catalog, _company = account_storage
    account = await repo.create_user(User(email="pending@example.com"))
    token = set_current_user(account)
    try:
        prepared = await chat_storage.prepare_chat_storage(app, str(account.id), "chat-a")
        assert prepared.mounts.binds == ()
        assert prepared.mounts.labels == {}
        assert prepared.reason == "space-not-assigned"
        with pytest.raises(PermissionError):
            prepared.paths.thread_dir("chat-a", user_id=str(account.id))
    finally:
        reset_current_user(token)


@pytest.mark.asyncio
async def test_credential_denial_is_inherited_and_never_replaced_by_matching_identity(account_storage, monkeypatch):
    app, account, _sf, files, catalog, _company = await prepared_fixture(account_storage)
    token = set_current_user(account)
    try:

        def forbidden(*args, **kwargs):
            pytest.fail("A denied credential reached physical storage qualification")

        monkeypatch.setattr(catalog, "verify", forbidden)
        with storage_credential_scope(False):
            task = asyncio.create_task(chat_storage.prepare_chat_storage(app, str(account.id), "chat-a"))
        prepared = await task
        assert prepared.mounts.binds == () and prepared.mounts.labels == {}
        assert prepared.reason == "credential-unsupported"
    finally:
        reset_current_user(token)


@pytest.mark.asyncio
async def test_claimed_user_cannot_replace_the_current_authenticated_owner(account_storage):
    app, account, _sf, _files, _catalog, _company = await prepared_fixture(account_storage)
    token = set_current_user(SimpleNamespace(id="other-owner"))
    try:
        with pytest.raises(InvalidPrincipal):
            await chat_storage.prepare_chat_storage(app, str(account.id), "chat-a")
    finally:
        reset_current_user(token)


@pytest.mark.asyncio
async def test_unsupported_credentials_cannot_replace_authenticated_owner(account_storage):
    app, account, _sf, _files, _catalog, _company = await prepared_fixture(account_storage)
    token = set_current_user(SimpleNamespace(id="other-owner"))
    try:
        with storage_credential_scope(False), pytest.raises(InvalidPrincipal):
            await chat_storage.prepare_chat_storage(app, str(account.id), "chat-a")
    finally:
        reset_current_user(token)


@pytest.mark.asyncio
async def test_unsupported_credentials_still_check_current_account_validity(account_storage, monkeypatch):
    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER

    app, account, _sf, _files, catalog, _company = await prepared_fixture(account_storage)
    await account_storage[1].disable_identity(LOCAL_ACCOUNT_ISSUER, str(account.id))
    monkeypatch.setattr(catalog, "verify", lambda *args, **kwargs: pytest.fail("A disabled identity reached physical qualification"))
    token = set_current_user(account)
    try:
        with storage_credential_scope(False), pytest.raises(InvalidPrincipal):
            await chat_storage.prepare_chat_storage(app, str(account.id), "chat-a")
    finally:
        reset_current_user(token)


@pytest.mark.asyncio
async def test_grant_revocation_during_filesystem_qualification_is_rechecked(account_storage, monkeypatch):
    app, account, sf, files, catalog, _company = await prepared_fixture(account_storage)
    async with sf() as session:
        link = await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(account.id)))
        identifier = link.space_id
    entered, release = threading.Event(), threading.Event()
    verify = catalog.verify

    def held(*args, **kwargs):
        entered.set()
        assert release.wait(10)
        return verify(*args, **kwargs)

    monkeypatch.setattr(catalog, "verify", held)
    token = set_current_user(account)
    task = asyncio.create_task(chat_storage.prepare_chat_storage(app, str(account.id), "chat-a"))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        async with sf() as session, session.begin():
            await session.execute(delete(SpaceGrantRow).where(SpaceGrantRow.space_id == identifier))
        release.set()
        prepared = await task
        assert prepared.mounts.binds == ()
        assert prepared.reason == "space-unverified"
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        reset_current_user(token)


@pytest.mark.asyncio
async def test_no_authenticated_context_never_becomes_default_storage_owner(account_storage):
    app, _account, _sf, _files, _catalog, _company = await prepared_fixture(account_storage)
    token = set_current_user(None)
    try:
        with pytest.raises(InvalidPrincipal):
            await chat_storage.prepare_chat_storage(app, "default", "chat-a")
    finally:
        reset_current_user(token)


@pytest.mark.asyncio
async def test_unverified_company_omits_shared_without_removing_personal_files(account_storage, monkeypatch):
    app, account, _sf, _files, catalog, _company = await prepared_fixture(account_storage)
    from deerflow.spaces.backings import BackingUnavailable

    verify = catalog.verify

    def refuse_company(slot, **kwargs):
        if catalog.volumes[slot].kind == "company":
            raise BackingUnavailable("fixture changed company mount")
        return verify(slot, **kwargs)

    monkeypatch.setattr(catalog, "verify", refuse_company)
    token = set_current_user(account)
    try:
        prepared = await chat_storage.prepare_chat_storage(app, str(account.id), "chat-a")
        assert len(prepared.mounts.binds) == 5
        assert all(destination != "/mnt/user-data/shared" for _source, destination, _readonly in prepared.mounts.binds)
        assert prepared.reason is None
        assert prepared.shared_reason == "space-unverified"
        with pytest.raises(PermissionError):
            prepared.paths.shared_dir()
    finally:
        reset_current_user(token)


@pytest.mark.asyncio
async def test_cancelled_preparation_drains_qualification_before_return(account_storage, monkeypatch):
    app, account, _sf, _files, catalog, _company = await prepared_fixture(account_storage)
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    verify = catalog.verify

    def held(*args, **kwargs):
        entered.set()
        try:
            assert release.wait(10)
            return verify(*args, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(catalog, "verify", held)
    token = set_current_user(account)
    task = asyncio.create_task(chat_storage.prepare_chat_storage(app, str(account.id), "chat-a"))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0.03)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        reset_current_user(token)


@pytest.mark.asyncio
async def test_qualification_never_holds_sql_connections(account_storage, monkeypatch):
    app, account, sf, _files, catalog, _company = await prepared_fixture(account_storage)
    verify = catalog.verify
    calls = []

    def outside_sql(*args, **kwargs):
        assert sf.kw["bind"].sync_engine.pool.checkedout() == 0
        calls.append(args[0])
        return verify(*args, **kwargs)

    monkeypatch.setattr(catalog, "verify", outside_sql)
    token = set_current_user(account)
    try:
        prepared = await chat_storage.prepare_chat_storage(app, str(account.id), "chat-a")
        assert len(calls) == 2 and len(prepared.mounts.binds) == 6
    finally:
        reset_current_user(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["restore", "write"])
async def test_uncertain_personal_operation_never_becomes_writable_mounts(account_storage, action):
    from deerflow.persistence.spaces.files import SpaceFileOperationRow

    app, account, sf, _files, _catalog, _company = await prepared_fixture(account_storage)
    async with sf() as session, session.begin():
        link = await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(account.id)))
        session.add(SpaceFileOperationRow(space_id=link.space_id, operation_id="f" * 32, actor_kind="human", actor_id=str(account.id), generation=1, phase="pending", request={"action": action}))
    token = set_current_user(account)
    try:
        prepared = await chat_storage.prepare_chat_storage(app, str(account.id), "chat-a")
        assert prepared.mounts.binds == () and prepared.reason == "space-unverified"
    finally:
        reset_current_user(token)


@pytest.mark.asyncio
async def test_uncertain_company_restore_omits_shared_but_keeps_personal_mounts(account_storage):
    from deerflow.persistence.spaces.files import SpaceFileOperationRow

    app, account, sf, _files, _catalog, company = await prepared_fixture(account_storage)
    async with sf() as session, session.begin():
        session.add(SpaceFileOperationRow(space_id=company, operation_id="e" * 32, actor_kind="human", actor_id=str(account.id), generation=1, phase="pending", request={"action": "restore"}))
    token = set_current_user(account)
    try:
        prepared = await chat_storage.prepare_chat_storage(app, str(account.id), "chat-a")
        assert len(prepared.mounts.binds) == 5
        assert prepared.reason is None and prepared.shared_reason == "space-unverified"
    finally:
        reset_current_user(token)


@pytest.mark.asyncio
async def test_personal_generation_change_while_company_verifies_never_publishes_old_root(account_storage, monkeypatch):
    from deerflow.persistence.spaces.model import SpaceRow

    app, account, sf, _files, catalog, _company = await prepared_fixture(account_storage)
    entered, release = threading.Event(), threading.Event()
    verify = catalog.verify

    def held_company(slot, **kwargs):
        if catalog.volumes[slot].kind == "company":
            entered.set()
            assert release.wait(10)
        return verify(slot, **kwargs)

    monkeypatch.setattr(catalog, "verify", held_company)
    token = set_current_user(account)
    task = asyncio.create_task(chat_storage.prepare_chat_storage(app, str(account.id), "chat-a"))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        async with sf() as session, session.begin():
            link = await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(account.id)))
            row = await session.get(SpaceRow, link.space_id)
            row.generation += 1
        release.set()
        prepared = await task
        assert prepared.mounts.binds == () and prepared.reason == "space-unverified"
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        reset_current_user(token)


def test_no_files_operational_projection_preserves_distinct_docker_host_root(tmp_path, monkeypatch):
    from pathlib import PureWindowsPath

    from deerflow.spaces.chat_paths import ChatStoragePaths

    monkeypatch.setenv("DEER_FLOW_HOST_BASE_DIR", "D:/docker-state")
    paths = ChatStoragePaths(Paths(tmp_path / "local"), user_id="alice")
    assert paths.thread_data("chat-a", user_id="alice") == {}
    assert paths.thread_skills_view_dir("chat-a", user_id="alice") == tmp_path / "local/chat-runtime/alice/chat-a/skills_view"
    assert paths.host_thread_skills_view_dir("chat-a", user_id="alice") == str(PureWindowsPath("D:/docker-state/chat-runtime/alice/chat-a/skills_view"))
    assert not (tmp_path / "local").exists()


@pytest.mark.parametrize("owner,thread", [("bad/owner", "chat-a"), ("alice", "../chat"), ("alice", "bad/thread")])
@pytest.mark.asyncio
async def test_invalid_scope_is_refused_before_storage_lookup(owner, thread):
    app = SimpleNamespace(state=SimpleNamespace(storage_spaces_enabled=True))
    with pytest.raises(ValueError):
        await chat_storage.prepare_chat_storage(app, owner, thread)


def pinned_chat_volume(tmp_path):
    import uuid

    from deerflow.spaces.backings import FixedVolumeSpec, VerifiedVolume

    root = tmp_path / "slot"
    data, control = root / "data", root / "control"
    data.mkdir(parents=True)
    control.mkdir(mode=0o700)
    (data / "conversations").mkdir()
    spec = FixedVolumeSpec(uuid.uuid4().hex, root, tmp_path / "image", str(uuid.uuid4()), 1 << 30, 1024, kind="user")
    return VerifiedVolume(spec, 1, data.stat().st_dev, data.stat().st_ino, control.stat().st_ino, 1 << 30, 1024, 1 << 30, 1024, 1, 1)


@pytest.mark.parametrize("bytes_free,inodes_free,refused", [((2 << 20) - 1, 2, True), (2 << 20, 1, True), (2 << 20, 2, False)])
def test_upload_capacity_uses_payload_plus_headroom_on_pinned_volume(tmp_path, monkeypatch, bytes_free, inodes_free, refused):
    import os

    from deerflow.spaces.chat_paths import ChatFilesFull, ChatStoragePaths

    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=pinned_chat_volume(tmp_path))
    monkeypatch.setattr(os, "fstatvfs", lambda fd: SimpleNamespace(f_bavail=bytes_free, f_frsize=1, f_favail=inodes_free))
    if refused:
        with pytest.raises(ChatFilesFull):
            paths.check_file_capacity(1 << 20, user_id="alice")
    else:
        paths.check_file_capacity(1 << 20, user_id="alice")
    assert not (tmp_path / "legacy").exists()


def test_policy_skill_views_stay_outside_all_storage_spaces_even_with_user_volume(tmp_path, monkeypatch):
    from deerflow.spaces.chat_paths import ChatStoragePaths

    monkeypatch.setenv("DEER_FLOW_HOST_BASE_DIR", "/host/operational")
    paths = ChatStoragePaths(Paths(tmp_path / "operational"), user_id="alice", user=pinned_chat_volume(tmp_path))
    assert paths.thread_skills_view_dir("chat-a", user_id="alice") == tmp_path / "operational/chat-runtime/alice/chat-a/skills_view"
    assert paths.host_thread_skills_view_dir("chat-a", user_id="alice") == "/host/operational/chat-runtime/alice/chat-a/skills_view"


@pytest.mark.parametrize("free_bytes,free_inodes", [(0, 1000), ((16 << 20) - 1, 1000), (1 << 30, 255)])
def test_new_conversation_refuses_full_pinned_filesystem_before_mkdir(tmp_path, monkeypatch, free_bytes, free_inodes):
    import os

    from deerflow.agent_instances.contract import Refusal
    from deerflow.spaces.chat_paths import ChatStoragePaths

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume)
    monkeypatch.setattr(os, "fstatvfs", lambda _fd: SimpleNamespace(f_bavail=free_bytes, f_frsize=1, f_favail=free_inodes))
    with pytest.raises(Refusal) as refused:
        paths.ensure_thread_dirs("new-chat", user_id="alice")
    assert refused.value.status == 507 and refused.value.reason == "files-full"
    assert not (volume.data_path / "conversations/new-chat").exists()
    assert not (tmp_path / "legacy").exists()


def test_existing_conversation_remains_accessible_when_full(tmp_path, monkeypatch):
    import os

    from deerflow.spaces.chat_paths import ChatStoragePaths

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume)
    paths.ensure_thread_dirs("old-chat", user_id="alice")
    monkeypatch.setattr(os, "fstatvfs", lambda _fd: SimpleNamespace(f_bavail=0, f_frsize=1, f_favail=0))
    paths.check_thread_capacity("old-chat", user_id="alice")
    assert paths.sandbox_outputs_dir("old-chat", user_id="alice").is_dir()


def test_replaced_root_incarnation_refuses_before_creating_thread(tmp_path):
    from deerflow.spaces.chat_paths import ChatStoragePaths
    from deerflow.spaces.filesystem import FilesystemUnavailable

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume)
    volume.data_path.rename(volume.data_path.with_name("old-data"))
    volume.data_path.mkdir()
    with pytest.raises(FilesystemUnavailable):
        paths.ensure_thread_dirs("new-chat", user_id="alice")
    assert list(volume.data_path.iterdir()) == []


def test_available_pinned_filesystem_creates_only_projected_0770_directories(tmp_path):
    from deerflow.spaces.chat_paths import ChatStoragePaths

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume)
    paths.ensure_thread_dirs("new-chat", user_id="alice")
    thread = paths.thread_dir("new-chat", user_id="alice")
    for node in (thread, thread / "user-data", thread / "acp-workspace", *(thread / "user-data" / leaf for leaf in ("workspace", "uploads", "outputs"))):
        assert node.stat().st_mode & 0o777 == 0o770
    assert not (tmp_path / "legacy").exists()


def test_first_thread_route_refuses_full_files_before_metadata_or_checkpoint(tmp_path, monkeypatch):
    import os

    from fastapi.testclient import TestClient
    from test_threads_router import _build_thread_app

    from app.gateway.refusals import refusal_handler
    from deerflow.agent_instances.contract import Refusal
    from deerflow.spaces.chat_paths import ChatStoragePaths

    app, store, checkpointer = _build_thread_app()
    app.add_exception_handler(Refusal, refusal_handler)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=pinned_chat_volume(tmp_path))
    calls = []

    async def prepared(request, thread_id):
        calls.append(thread_id)
        return chat_storage.PreparedChatStorage(paths, paths.mounts(thread_id, user_id="alice"))

    monkeypatch.setattr(chat_storage, "prepare_chat_request", prepared)
    monkeypatch.setattr(os, "fstatvfs", lambda _fd: SimpleNamespace(f_bavail=0, f_frsize=1, f_favail=0))
    with TestClient(app) as client:
        response = client.post("/api/threads", json={"thread_id": "full-chat"})
    assert response.status_code == 507, response.text
    assert response.json()["reason"] == "files-full"
    assert calls == ["full-chat"]
    assert asyncio.run(store.aget(("threads",), "full-chat")) is None
    assert asyncio.run(checkpointer.aget_tuple({"configurable": {"thread_id": "full-chat", "checkpoint_ns": ""}})) is None


def test_existing_thread_route_retry_does_not_require_free_room(monkeypatch):
    from fastapi.testclient import TestClient
    from test_threads_router import _build_thread_app

    app, _store, _checkpointer = _build_thread_app()
    with TestClient(app) as client:
        first = client.post("/api/threads", json={"thread_id": "old-chat"})
        assert first.status_code == 200, first.text

        async def forbidden(*_args, **_kwargs):
            pytest.fail("Idempotent existing thread consulted new-conversation capacity")

        monkeypatch.setattr(chat_storage, "prepare_chat_request", forbidden)
        retry = client.post("/api/threads", json={"thread_id": "old-chat"})
        assert retry.status_code == 200 and retry.json() == first.json()


@pytest.mark.parametrize("lazy", [True, False])
def test_preconstructed_middleware_uses_current_storage_projection(tmp_path, lazy):
    from langgraph.runtime import Runtime

    from deerflow.agents.middlewares.thread_data_middleware import ThreadDataMiddleware
    from deerflow.spaces.chat_paths import ChatStoragePaths

    with paths_scope(Paths(tmp_path / "legacy")):
        middleware = ThreadDataMiddleware(lazy_init=lazy)
    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume)
    with paths_scope(paths):
        result = middleware.before_agent({}, Runtime(context={"thread_id": "chat-a", "user_id": "alice"}))
    assert result["thread_data"]["workspace_path"] == str(volume.data_path / "conversations/chat-a/user-data/workspace")
    assert "shared_path" not in result["thread_data"]
    assert not (tmp_path / "legacy").exists()


@pytest.mark.parametrize("lazy", [True, False])
def test_mountless_middleware_returns_no_file_paths_and_creates_no_legacy_dirs(tmp_path, lazy):
    from langgraph.runtime import Runtime

    from deerflow.agents.middlewares.thread_data_middleware import ThreadDataMiddleware
    from deerflow.spaces.chat_paths import ChatStoragePaths

    with paths_scope(Paths(tmp_path / "legacy")):
        middleware = ThreadDataMiddleware(lazy_init=lazy)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice")
    with paths_scope(paths):
        result = middleware.before_agent({}, Runtime(context={"thread_id": "chat-a", "user_id": "alice"}))
    assert result["thread_data"] == {}
    assert not (tmp_path / "legacy").exists()


@pytest.mark.asyncio
async def test_async_middleware_keeps_capacity_filesystem_work_off_loop(tmp_path, monkeypatch):
    import os

    from langgraph.runtime import Runtime

    from deerflow.agents.middlewares.thread_data_middleware import ThreadDataMiddleware
    from deerflow.spaces.chat_paths import ChatStoragePaths

    loop_thread = threading.get_ident()
    probes = []

    def capacity(_fd):
        assert threading.get_ident() != loop_thread
        probes.append(True)
        return SimpleNamespace(f_bavail=1 << 30, f_frsize=1, f_favail=1024)

    monkeypatch.setattr(os, "fstatvfs", capacity)
    with paths_scope(Paths(tmp_path / "legacy")):
        middleware = ThreadDataMiddleware()
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=pinned_chat_volume(tmp_path))
    with paths_scope(paths):
        result = await middleware.abefore_agent({}, Runtime(context={"thread_id": "chat-a", "user_id": "alice"}))
    assert result["thread_data"] and probes


def test_first_thread_maps_replaced_storage_root_to_unavailable_response(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from test_threads_router import _build_thread_app

    from deerflow.spaces.chat_paths import ChatStoragePaths

    app, _store, _checkpointer = _build_thread_app()
    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume)

    async def replaced(request, thread_id):
        volume.data_path.rename(volume.data_path.with_name("old-data"))
        volume.data_path.mkdir()
        return chat_storage.PreparedChatStorage(paths, paths.mounts(thread_id, user_id="alice"))

    monkeypatch.setattr(chat_storage, "prepare_chat_request", replaced)
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.post("/api/threads", json={"thread_id": "changed-chat"})
    assert response.status_code == 503, response.text
    assert response.json()["detail"]["reason"] == "space-unverified"
    assert list(volume.data_path.iterdir()) == []


@pytest.mark.asyncio
async def test_http_account_disabled_during_qualification_is_identity_refusal(account_storage, monkeypatch):
    import httpx
    from fastapi import FastAPI, Request

    from app.gateway.auth_disabled import AUTH_SOURCE_SESSION
    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER

    storage_app, account, _sf, _files, catalog, _company = await prepared_fixture(account_storage)
    app = FastAPI()
    app.state.__dict__["_state"].update(storage_app.state.__dict__)
    entered, release = threading.Event(), threading.Event()
    verify = catalog.verify

    def held(*args, **kwargs):
        entered.set()
        assert release.wait(10)
        return verify(*args, **kwargs)

    monkeypatch.setattr(catalog, "verify", held)

    @app.post("/prepare")
    async def prepare(request: Request):
        request.state.user = account
        request.state.auth_source = AUTH_SOURCE_SESSION
        await chat_storage.prepare_chat_request(request, "chat-a")
        return {"ready": True}

    token = set_current_user(account)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app, raise_app_exceptions=False), base_url="http://test") as client:
        task = asyncio.create_task(client.post("/prepare"))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            await account_storage[1].disable_identity(LOCAL_ACCOUNT_ISSUER, str(account.id))
            release.set()
            response = await task
            assert response.status_code == 401, response.text
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
            reset_current_user(token)


@pytest.mark.asyncio
async def test_detached_worker_scopes_paths_before_snapshots_and_restores_after_cleanup(tmp_path, monkeypatch):
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    from deerflow.runtime.events.store.memory import MemoryRunEventStore
    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.schemas import RunStatus
    from deerflow.runtime.runs.worker import RunContext, run_agent
    from deerflow.spaces.chat_paths import ChatStoragePaths

    base = Paths(tmp_path / "legacy")
    paths = ChatStoragePaths(base, user_id="alice", user=pinned_chat_volume(tmp_path))
    manager = RunManager()
    record = await manager.create("chat-a")
    observed = []

    @asynccontextmanager
    async def scope(*, user_id, thread_id):
        assert (user_id, thread_id) == ("alice", "chat-a")
        with paths_scope(paths):
            observed.append("scope-open")
            yield chat_storage.PreparedChatStorage(paths, paths.mounts(thread_id, user_id=user_id))
            observed.append("scope-close")

    async def snapshot(*args, **kwargs):
        from deerflow.workspace_changes.types import WorkspaceSnapshot

        assert get_paths() is paths
        observed.append("snapshot")
        return WorkspaceSnapshot()

    class Agent:
        async def astream(self, *args, **kwargs):
            assert get_paths() is paths
            observed.append("stream")
            yield {"messages": []}

    monkeypatch.setattr("deerflow.runtime.runs.worker.capture_output_snapshot", snapshot)
    monkeypatch.setattr("deerflow.runtime.runs.worker.capture_workspace_snapshot", snapshot)
    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())
    token = set_current_user(SimpleNamespace(id="alice"))
    try:
        with paths_scope(base):
            ctx = RunContext(checkpointer=None, event_store=MemoryRunEventStore(), chat_storage_scope=scope)
            await asyncio.create_task(run_agent(bridge, manager, record, ctx=ctx, agent_factory=lambda **kwargs: Agent(), graph_input={}, config={}))
            assert get_paths() is base
        assert record.status == RunStatus.success, record.error
        assert observed[0] == "scope-open" and "snapshot" in observed and "stream" in observed and observed[-1] == "scope-close"
        bridge.publish_end.assert_awaited_once()
    finally:
        reset_current_user(token)


@pytest.mark.asyncio
async def test_worker_storage_refusal_keeps_reason_and_finishes_without_agent(tmp_path):
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock, Mock

    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.schemas import RunStatus
    from deerflow.runtime.runs.worker import RunContext, run_agent
    from deerflow.spaces.chat_paths import ChatFilesFull

    @asynccontextmanager
    async def full(**_args):
        raise ChatFilesFull()
        yield

    manager = RunManager()
    record = await manager.create("full-chat")
    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())
    factory = Mock()
    await run_agent(bridge, manager, record, ctx=RunContext(checkpointer=None, chat_storage_scope=full), agent_factory=factory, graph_input={}, config={})
    assert record.status == RunStatus.error
    factory.assert_not_called()
    errors = [call.args[2] for call in bridge.publish.await_args_list if call.args[1] == "error"]
    assert len(errors) == 1 and errors[0]["reason"] == "files-full"
    bridge.publish_end.assert_awaited_once()


@pytest.mark.parametrize("personal,shared", [(False, False), (True, False), (True, True)])
def test_prompt_describes_only_admitted_files_and_space_failure_guidance(tmp_path, monkeypatch, personal, shared):
    from deerflow.agents.lead_agent import prompt
    from deerflow.spaces.chat_paths import ChatStoragePaths

    monkeypatch.setattr(prompt, "_sandbox_mounts_thread_data", lambda _config: True)
    user = pinned_chat_volume(tmp_path) if personal else None
    company = pinned_chat_volume(tmp_path / "company") if shared else None
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=user, company=company)
    with paths_scope(paths):
        section = prompt._build_user_files_section()
    if not personal:
        assert section == ""
    else:
        assert "/mnt/user-data/files" in section
        assert ("/mnt/user-data/shared" in section) == shared
        assert "do not retry in another folder" in section
        assert str(tmp_path) not in section


def test_mountless_full_prompt_does_not_advertise_uploads_workspace_outputs_or_acp(tmp_path, render):
    from test_lead_prompt_script_guidance import _app_config

    from deerflow.spaces.chat_paths import ChatStoragePaths

    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice")
    with paths_scope(paths):
        text = render(_app_config(acp_agents={"codex": SimpleNamespace(description="Independent work")}), user_id="alice")
    assert "Files are not available" in text
    assert '<working_directory existed="true">' not in text
    assert "Final deliverables must be copied" not in text
    assert "Final deliverables must be in" not in text
    assert "/mnt/acp-workspace" not in text
    assert "User workspace:" not in text
    assert "Current uploads:" not in text


@pytest.mark.asyncio
async def test_mountless_worker_succeeds_without_file_snapshots_or_legacy_data(tmp_path, monkeypatch):
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    from deerflow.runtime.events.store.memory import MemoryRunEventStore
    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.schemas import RunStatus
    from deerflow.runtime.runs.worker import RunContext, run_agent
    from deerflow.spaces.chat_paths import ChatStoragePaths

    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice")

    @asynccontextmanager
    async def scope(**_args):
        with paths_scope(paths):
            yield chat_storage.PreparedChatStorage(paths, paths.mounts("chat-a", user_id="alice"))

    async def forbidden(*_args, **_kwargs):
        pytest.fail("A chat without files scanned a legacy file area")

    monkeypatch.setattr("deerflow.runtime.runs.worker.capture_output_snapshot", forbidden)
    monkeypatch.setattr("deerflow.runtime.runs.worker.capture_workspace_snapshot", forbidden)

    class Agent:
        async def astream(self, *args, **kwargs):
            yield {"messages": []}

    manager = RunManager()
    record = await manager.create("chat-a")
    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())
    await run_agent(bridge, manager, record, ctx=RunContext(checkpointer=None, event_store=MemoryRunEventStore(), chat_storage_scope=scope), agent_factory=lambda **kwargs: Agent(), graph_input={}, config={})
    assert record.status == RunStatus.success, record.error
    assert not (tmp_path / "legacy").exists()
    bridge.publish_end.assert_awaited_once()


@pytest.mark.parametrize("mounted", ["user-data", "user-data/outputs"])
def test_native_thread_creation_refuses_same_device_nested_mount_before_chmod_or_children(tmp_path, monkeypatch, mounted):
    import os
    import stat

    from deerflow.spaces.chat_paths import ChatStoragePaths
    from deerflow.spaces.filesystem import ConfinedFilesystem, UnsafeSpacePath

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume)
    relative = "conversations/chat-a/" + mounted
    foreign = volume.data_path / relative
    foreign.mkdir(parents=True)
    os.chmod(foreign, 0o755)
    original = ConfinedFilesystem._open

    def mount_boundary(filesystem, path, flags, **kwargs):
        if path == relative or path.startswith(relative + "/"):
            raise UnsafeSpacePath("NO_XDEV refuses a nested same-device mount")
        return original(filesystem, path, flags, **kwargs)

    monkeypatch.setattr(ConfinedFilesystem, "_open", mount_boundary)
    with pytest.raises(UnsafeSpacePath):
        paths.ensure_thread_dirs("chat-a", user_id="alice")
    assert stat.S_IMODE(foreign.stat().st_mode) == 0o755
    assert list(foreign.iterdir()) == []


@pytest.mark.parametrize("error", ["ENOSPC", "EDQUOT"])
def test_native_thread_creation_maps_a_racing_directory_quota_failure_to_507(tmp_path, monkeypatch, error):
    import errno

    from deerflow.spaces.chat_paths import ChatFilesFull, ChatStoragePaths

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume)
    original = __import__("os").mkdir

    def full(path, *args, **kwargs):
        if path == "uploads" and kwargs.get("dir_fd") is not None:
            raise OSError(getattr(errno, error), "Space became full after capacity admission")
        return original(path, *args, **kwargs)

    monkeypatch.setattr("deerflow.spaces.chat_paths.os.mkdir", full)
    with pytest.raises(ChatFilesFull):
        paths.ensure_thread_dirs("chat-a", user_id="alice")
    assert not (tmp_path / "legacy").exists()


def test_peeking_a_host_path_scope_never_initializes_legacy_paths(tmp_path, monkeypatch):
    from deerflow.config import paths

    monkeypatch.setattr(paths, "_paths", None)
    token = paths._scoped_paths.set(None)
    try:
        assert paths.peek_scoped_paths() is None
        assert paths._paths is None
        projection = Paths(tmp_path / "legacy")
        with paths_scope(projection):
            assert paths.peek_scoped_paths() is projection
            assert paths._paths is None
        assert paths.peek_scoped_paths() is None
        assert paths._paths is None
    finally:
        paths._scoped_paths.reset(token)
