"""Direct file refusals and recovery through the durable host service."""

import errno
import hashlib
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from _storage_spaces_test_support import ALICE, assert_host_settlement, home, operation
from deerflow_extension_api.storage import StorageConflict
from test_storage_spaces_maintenance import intent, outcome
from test_storage_spaces_service import space_file_storage as space_file_storage

from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.runtime.user_context import reset_current_user, set_current_user
from deerflow.spaces.facade import HostStorageProvider
from deerflow.spaces.filesystem import ConfinedFilesystem, FilesystemUnavailable
from deerflow.spaces.service import SpaceOperationPending


async def root_for(sf, catalog, space):
    async with sf() as session:
        binding = await session.get(SpaceBackingRow, space.backing_handle)
        return catalog.verified[binding.slot_id].data_path


async def change(files, source, target, action, identifier, path):
    args = dict(actor=ALICE, space_id=target.id, expected_generation=target.generation, operation_id=identifier, path=path)
    if action == "copy":
        return await files.copy(actor=ALICE, source_id=source.id, source_generation=source.generation, destination_id=target.id, destination_generation=target.generation, source_path="source", destination_path=path, operation_id=identifier)
    if action == "write":
        return await files.write(**args, expected_sha256=hashlib.sha256(b"original").hexdigest(), content=b"new")
    if action == "put":
        return await files.put_unique(**args, on_behalf_of="alice", content=b"new")
    if action == "rename":
        return await files.rename(**args, destination="renamed")
    return await getattr(files, action)(**args)


@pytest.mark.asyncio
async def test_permission_refused_before_file_effect_is_terminal(space_file_storage):
    assert os.geteuid() != 0, "Permission regressions require an unprivileged process"
    files, sf, catalog = space_file_storage
    source, target = await home(files), await home(files)
    (await root_for(sf, catalog, source) / "source").write_bytes(b"new")
    folder = await root_for(sf, catalog, target) / "private"
    folder.mkdir()
    item = folder / "original"
    item.write_bytes(b"original")
    folder.chmod(0)
    try:
        for action in ("write", "rename", "remove", "put", "copy"):
            key = operation()
            with pytest.raises(StorageConflict) as caught:
                await change(files, source, target, action, key, "private/original")
            assert getattr(caught.value, "nothing_changed", False), action
            assert await outcome(sf, target, key) == ("failed", None)
        key = operation()
        with pytest.raises(SpaceOperationPending):
            await change(files, source, target, "mkdir", key, "private/new")
        assert await outcome(sf, target, key) == ("pending", None)
    finally:
        folder.chmod(0o700)
    assert list(folder.iterdir()) == [item] and item.read_bytes() == b"original"


@pytest.mark.asyncio
@pytest.mark.parametrize("number", [errno.EACCES, errno.EPERM, errno.EIO])
async def test_lost_directory_flush_after_file_effect_remains_pending(space_file_storage, monkeypatch, number):
    files, sf, catalog = space_file_storage
    source = await home(files)
    (await root_for(sf, catalog, source) / "source").write_bytes(b"new")
    target = await home(files)
    root = await root_for(sf, catalog, target)
    real_fsync = os.fsync
    for action in ("put", "write", "rename", "remove", "copy"):
        folder = root / action
        folder.mkdir()
        item = folder / "item"
        if action in {"write", "rename", "remove"}:
            item.write_bytes(b"original")
        key = operation()
        with monkeypatch.context() as fail:

            def fsync(fd):
                if os.path.samestat(os.fstat(fd), folder.stat()):
                    raise OSError(number, "directory flush refused")
                return real_fsync(fd)

            fail.setattr(os, "fsync", fsync)
            with pytest.raises(SpaceOperationPending):
                await change(files, source, target, action, key, f"{action}/item")
        assert await outcome(sf, target, key) == ("pending", None)
        if action == "rename":
            assert (root / "renamed").read_bytes() == b"original" and not item.exists()
        elif action == "remove":
            assert not item.exists()
        else:
            assert item.read_bytes() == b"new"
        # Copy recovery requires an explicit operator decision. Run it last.
        if action != "copy":
            assert await files.settle_interrupted() == 1


@pytest.mark.asyncio
async def test_unreadable_collision_gets_numbered_copy(space_file_storage):
    assert os.geteuid() != 0
    files, sf, catalog = space_file_storage
    space = await home(files)
    root = await root_for(sf, catalog, space)
    occupied = root / "note"
    occupied.write_bytes(b"private")
    occupied.chmod(0)
    try:
        saved = await files.put_unique(actor=ALICE, space_id=space.id, expected_generation=space.generation, operation_id=operation(), path="note", on_behalf_of="alice", content=b"new")
    finally:
        occupied.chmod(0o600)
    assert saved["path"] == "note (2)" and (root / saved["path"]).read_bytes() == b"new" and occupied.read_bytes() == b"private"


@pytest.mark.asyncio
async def test_interrupted_upload_retains_visible_files_at_each_publication_stage(space_file_storage):
    files, sf, catalog = space_file_storage
    space = await home(files)
    root = await root_for(sf, catalog, space)
    digest = hashlib.sha256(b"new").hexdigest()
    for phase in ("intent", "staged", "published", "numbered"):
        path = root / phase
        key = await intent(sf, space, {"action": "put", "path": phase, "sha256": digest, "size": 3})
        if phase == "staged":
            (root.parent / "control" / ("stage-" + operation())).write_bytes(b"new")
        elif phase == "published":
            path.write_bytes(b"new")
        elif phase == "numbered":
            path.write_bytes(b"old")
            (root / "numbered (2)").write_bytes(b"new")
        assert await files.settle_interrupted() == 1
        state, result = await outcome(sf, space, key)
        if phase == "published":
            assert state == "complete" and result["value"]["path"] == phase and path.read_bytes() == b"new"
        else:
            assert state == "failed"
            assert_host_settlement(result, {"resolution": "host-settled"})
            if phase == "numbered":
                assert path.read_bytes() == b"old" and (root / "numbered (2)").read_bytes() == b"new"
            else:
                assert not path.exists()


@pytest.mark.asyncio
async def test_extension_receives_typed_conflict_before_publication(space_file_storage):
    from deerflow_extension_api.storage import ResourceReference

    from app.gateway.routers.spaces import _http_error

    assert os.geteuid() != 0
    files, sf, catalog = space_file_storage
    source, target = await home(files), await home(files)
    src = await root_for(sf, catalog, source)
    dst = await root_for(sf, catalog, target)
    with (src / "large").open("wb") as large:
        large.truncate((64 << 20) + 1)
    (dst / "blocked").mkdir()
    token = set_current_user(SimpleNamespace(id="alice"))
    try:
        client = await HostStorageProvider(lambda: files).current()
        key = operation()
        with pytest.raises(StorageConflict, match="larger than 64 MiB") as caught:
            await client.copy(source=ResourceReference(source.id, "large"), destination=ResourceReference(target.id, "copy"), source_generation=source.generation, destination_generation=target.generation, operation_id=key)
        assert caught.value.nothing_changed is True
        assert await outcome(sf, target, key) == ("failed", None)
        assert not (dst / "copy").exists()
        key = operation()
        with pytest.raises(StorageConflict, match="loaded revision") as caught:
            await client.write(space_id=source.id, generation=source.generation, operation_id=key, path="large", expected_sha256="f" * 64, content=b"new")
        assert caught.value.nothing_changed is True
        assert await outcome(sf, source, key) == ("failed", None)
        assert (src / "large").stat().st_size == (64 << 20) + 1
        (dst / "blocked").chmod(0)
        key = operation()
        with pytest.raises(StorageConflict) as caught:
            await client.write(space_id=target.id, generation=target.generation, operation_id=key, path="blocked/file", create=True, content=b"new")
        assert caught.value.nothing_changed is True
        error = _http_error(caught.value)
        assert error.status_code == 409 and "nothing was changed" in error.detail
        assert await outcome(sf, target, key) == ("failed", None)
    finally:
        reset_current_user(token)
        (dst / "blocked").chmod(0o700)


@pytest.mark.asyncio
async def test_file_refusal_requires_confirmed_effect_and_commit_boundary(space_file_storage, monkeypatch):
    files, sf, catalog = space_file_storage
    target = await home(files)
    root = await root_for(sf, catalog, target)
    real_replace, real_unlink = os.replace, os.unlink
    original_admit = files.registry.admitted
    for number in (errno.EPERM, errno.EACCES):
        for fault in ("before-publish", "stage-cleanup", "commit", "refusal-commit", "authority"):
            key, count = operation(), 0
            path = fault + str(number)
            (root / path).write_bytes(b"original")

            def replace(*args, **kwargs):
                if fault in {"before-publish", "refusal-commit"}:
                    raise OSError(number, "rename refused")
                return real_replace(*args, **kwargs)

            def unlink(name, *args, **kwargs):
                if fault == "stage-cleanup" and isinstance(name, str) and name.startswith("stage-") and (root / path).read_bytes() == b"new":
                    raise OSError(number, "cleanup refused")
                return real_unlink(name, *args, **kwargs)

            @asynccontextmanager
            async def admitted(**kwargs):
                nonlocal count
                count += 1
                async with original_admit(**kwargs) as value:
                    if count == 2 and fault == "authority":
                        raise PermissionError(number, "authority changed")
                    yield value
                    if (count, fault) in {(2, "commit"), (3, "refusal-commit")}:
                        raise PermissionError(number, "commit unconfirmed")

            with monkeypatch.context() as inject:
                inject.setattr(os, "replace", replace)
                inject.setattr(os, "unlink", unlink)
                inject.setattr(files.registry, "admitted", admitted)
                with pytest.raises(StorageConflict) as caught:
                    await change(files, target, target, "write", key, path)
            if fault == "before-publish":
                assert caught.value.nothing_changed is True
                assert await outcome(sf, target, key) == ("failed", None)
            else:
                assert isinstance(caught.value, SpaceOperationPending)
                assert await outcome(sf, target, key) == ("pending", None)
                assert await files.settle_interrupted() == 1
            assert (root / path).read_bytes() == (b"new" if fault in {"stage-cleanup", "commit"} else b"original")


@pytest.mark.asyncio
async def test_bounded_digest_requires_a_readable_stable_regular_file(space_file_storage, monkeypatch):
    assert os.geteuid() != 0
    files, sf, catalog = space_file_storage
    target = await home(files)
    root = await root_for(sf, catalog, target)
    (root / "private").write_bytes(b"new")
    (root / "link").symlink_to("private")
    with (root / "large").open("wb") as large:
        large.truncate((64 << 20) + 1)
    (root / "private").chmod(0)
    try:
        for action in ("write", "put"):
            for path in ("private", "link", "large"):
                key = await intent(sf, target, dict(action=action, path=path, sha256=hashlib.sha256(b"new").hexdigest()))
                assert await files.settle_interrupted() == 1, path
                phase, result = await outcome(sf, target, key)
                assert phase == "failed"
                assert_host_settlement(result, {"resolution": "host-unconfirmed"})
    finally:
        (root / "private").chmod(0o600)
    original_open = ConfinedFilesystem.open_regular

    def replaced(self, path, **kwargs):
        fd = original_open(self, path, **kwargs)
        if path == "private":
            (root / "replacement").write_bytes(b"other")
            (root / "replacement").replace(root / "private")
        return fd

    key = await intent(sf, target, dict(action="put", path="private", sha256=hashlib.sha256(b"new").hexdigest()))
    monkeypatch.setattr(ConfinedFilesystem, "open_regular", replaced)
    assert await files.settle_interrupted() == 0
    assert await outcome(sf, target, key) == ("pending", None)


@pytest.mark.asyncio
@pytest.mark.parametrize("change_during_read", ["replace", "chmod", "grow"])
async def test_settlement_leaves_a_changing_regular_file_pending(space_file_storage, monkeypatch, change_during_read):
    files, sf, catalog = space_file_storage
    target = await home(files)
    root = await root_for(sf, catalog, target)
    path = root / "note"
    path.write_bytes(b"original")
    original_open, original_read = ConfinedFilesystem.open_regular, os.read
    opened, changed = None, False

    def open_file(filesystem, name, **kwargs):
        nonlocal opened, changed
        if name == path.name:
            if change_during_read == "replace":
                replacement = root / "next"
                replacement.write_bytes(b"next")
                replacement.replace(path)
                changed = True
            elif change_during_read == "chmod":
                path.chmod(0)
                changed = True
        opened = original_open(filesystem, name, **kwargs)
        return opened

    def read_file(fd, count):
        nonlocal changed
        if fd == opened and change_during_read == "grow" and not changed:
            with path.open("ab") as handle:
                handle.truncate((64 << 20) + 1)
            changed = True
        return original_read(fd, count)

    key = await intent(sf, target, dict(action="put", path="note", sha256=hashlib.sha256(b"next").hexdigest()))
    with monkeypatch.context() as fault:
        fault.setattr(ConfinedFilesystem, "open_regular", open_file)
        fault.setattr(os, "read", read_file)
        assert await files.settle_interrupted() == 0
    assert changed
    assert await outcome(sf, target, key) == ("pending", None)
    assert await files.settle_interrupted() == 1
    phase, result = await outcome(sf, target, key)
    assert (phase, result["resolution"]) == (("complete", "host-settled") if change_during_read == "replace" else ("failed", "host-unconfirmed"))


@pytest.mark.asyncio
async def test_inaccessible_names_are_distinct_from_absence_and_unavailable_host(space_file_storage, monkeypatch):
    assert os.geteuid() != 0
    files, sf, catalog = space_file_storage
    space = await home(files)
    root = await root_for(sf, catalog, space)
    private = root / "private"
    private.mkdir()
    (root / "link").symlink_to(private)
    (root / "plain").write_text("regular")
    default = dict(sha256=hashlib.sha256(b"new").hexdigest())
    private.chmod(0)
    try:
        for action in ("put", "write", "rename", "remove", "mkdir"):
            for path in ("private/leaf", "link/leaf", "plain/leaf"):
                key = await intent(sf, space, dict(action=action, path=path, destination="other", **default))
                assert await files.settle_interrupted() == 1, (action, path)
                phase, result = await outcome(sf, space, key)
                assert phase == "failed"
                assert_host_settlement(result, {"resolution": "host-unconfirmed"})
        key = await intent(sf, space, dict(action="rename", path="absent", destination="private/leaf"))
        assert await files.settle_interrupted() == 1
        assert_host_settlement((await outcome(sf, space, key))[1], {"resolution": "host-unconfirmed"})
        key = await intent(sf, space, dict(action="write", path="private/leaf", sha256="invalid"))
        assert await files.settle_interrupted() == 0
        assert await outcome(sf, space, key) == ("pending", None)
    finally:
        private.chmod(0o700)
    key = await intent(sf, space, dict(action="remove", path="absent"))
    original = ConfinedFilesystem._open
    for error in (FilesystemUnavailable("unsupported"), OSError(errno.EIO, "offline")):

        def unavailable(self, path, *args, **kwargs):
            if path == "absent":
                raise error
            return original(self, path, *args, **kwargs)

        with monkeypatch.context() as fault:
            fault.setattr(ConfinedFilesystem, "_open", unavailable)
            assert await files.settle_interrupted() == 0
            assert await outcome(sf, space, key) == ("pending", None)
