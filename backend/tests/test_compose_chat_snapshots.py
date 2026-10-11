"""Native file evidence is freshly admitted and descriptor-confined."""

import asyncio

import pytest
from test_compose_chat_storage import pinned_chat_volume

from app.gateway.chat_storage import PreparedChatStorage
from deerflow.config.paths import Paths, paths_scope
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.chat_bridge import ChatStorageHost, install_chat_storage_host, remove_chat_storage_host
from deerflow.spaces.chat_paths import ChatStoragePaths
from deerflow.workspace_changes.recorder import capture_output_snapshot


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "revoked", "redirected-root"])
async def test_output_evidence_never_uses_stale_routing_or_an_unqualified_root(tmp_path, monkeypatch, failure):
    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume)
    paths.ensure_thread_dirs("chat-a", user_id="alice")
    outputs = paths.sandbox_outputs_dir("chat-a", user_id="alice")
    (outputs / "report.txt").write_text("authorized", encoding="utf-8")
    prepared = PreparedChatStorage(paths, paths.mounts("chat-a", user_id="alice"))
    observations = []

    async def prepare(*args):
        observations.append("admitted")
        if failure == "revoked":
            raise BackingUnavailable("User grant is no longer current")
        if failure == "redirected-root" and len(observations) == 1:
            volume.data_path.rename(volume.data_path.with_name("old-data"))
            volume.data_path.mkdir()
            replacement = volume.data_path / "conversations/chat-a/user-data/outputs"
            replacement.mkdir(parents=True)
            (replacement / "report.txt").write_text("unqualified", encoding="utf-8")
        return prepared

    host = ChatStorageHost(asyncio.get_running_loop(), prepare)
    install_chat_storage_host(host)
    try:
        with paths_scope(paths):
            if failure:
                with pytest.raises(Exception):
                    await capture_output_snapshot("chat-a", user_id="alice")
                assert observations, "A native scan must refresh admission before reading"
            else:
                result = await capture_output_snapshot("chat-a", user_id="alice")
                assert "/mnt/user-data/outputs/report.txt" in result.files
                assert observations == ["admitted", "admitted"]
        assert not (tmp_path / "legacy").exists()
    finally:
        remove_chat_storage_host(host)
        await host.close()


@pytest.mark.asyncio
async def test_mountless_evidence_is_empty_without_any_host_file_walk(tmp_path, monkeypatch):
    import deerflow.workspace_changes.recorder as recorder

    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice")

    def forbidden(*args, **kwargs):
        pytest.fail("Mountless snapshot walked a file directory")

    monkeypatch.setattr(recorder, "_prepare_capture", forbidden)
    with paths_scope(paths):
        result = await capture_output_snapshot("chat-a", user_id="alice")
    assert result.files == {} and not result.truncated


@pytest.mark.asyncio
@pytest.mark.parametrize("nested", ["file", "directory"])
async def test_native_evidence_preserves_kernel_mount_boundary_for_every_entry(tmp_path, monkeypatch, nested):
    from deerflow.spaces.filesystem import ConfinedFilesystem, UnsafeSpacePath

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(Paths(tmp_path / "legacy"), user_id="alice", user=volume)
    paths.ensure_thread_dirs("chat-a", user_id="alice")
    relative = "conversations/chat-a/user-data/outputs"
    outputs = volume.data_path / relative
    forbidden = relative + ("/mounted.txt" if nested == "file" else "/nested")
    if nested == "directory":
        (outputs / "nested").mkdir()
        (outputs / "nested/mounted.txt").write_bytes(b"other mount bytes")
    else:
        (outputs / "mounted.txt").write_bytes(b"other mount bytes")
    original_open = ConfinedFilesystem._open
    observations = []

    def deny_nested_mount(filesystem, path, flags, **kwargs):
        if path == forbidden or path.startswith(forbidden + "/"):
            observations.append(path)
            raise UnsafeSpacePath("Kernel NO_XDEV refused a nested mount, even on the same device")
        return original_open(filesystem, path, flags, **kwargs)

    monkeypatch.setattr(ConfinedFilesystem, "_open", deny_nested_mount)
    prepared = PreparedChatStorage(paths, paths.mounts("chat-a", user_id="alice"))

    async def prepare(*args):
        return prepared

    host = ChatStorageHost(asyncio.get_running_loop(), prepare)
    install_chat_storage_host(host)
    try:
        with paths_scope(paths):
            result = await capture_output_snapshot("chat-a", user_id="alice")
        assert result.files == {} and result.truncated
        assert observations == [forbidden]
    finally:
        remove_chat_storage_host(host)
        await host.close()
