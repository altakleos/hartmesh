"""Fresh User-role sources for host file workflows, independent of chat layout."""

import errno
import hashlib
import os
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from app.gateway.chat_files import QualifiedUploadStage, _revision, files_unavailable
from app.gateway.chat_storage import _snapshot_in_session, _verified_role
from app.gateway.native_directory_changes import NativeDirectoryChanges
from app.gateway.storage_spaces import supports_storage_credentials
from deerflow.files.store import DescriptorSource, StoredFile, StoreError, normalize_relative_path
from deerflow.spaces.backings import BackingUnavailable, VerifiedVolume
from deerflow.spaces.chat_paths import ChatFilesFull
from deerflow.spaces.contract import InvalidPrincipal, Permission, PrincipalRef, SpaceDenied
from deerflow.spaces.facade import _bound_principal, _credential_allowed
from deerflow.spaces.filesystem import SpaceFull, UnsafeSpacePath
from deerflow.spaces.service import SpaceFiles
from deerflow.spaces.workflows import mark_effect
from deerflow.uploads.manager import claim_unique_filename, normalize_filename
from deerflow.utils.file_io import await_drained, run_file_io

_feature_input = ContextVar("native_feature_file_input", default=None)


@dataclass(frozen=True)
class NativeFeatureInput:
    sources: object
    source: DescriptorSource | None
    copy_async: object
    stage_async: object = None
    commit_async: object = None
    holding_async: object = None
    rollback_async: object = None


def native_feature_input():
    return _feature_input.get()


@contextmanager
def feature_input_scope(sources, source, copy_async, *, stage_async=None, commit_async=None, holding_async=None, rollback_async=None):
    token = _feature_input.set(NativeFeatureInput(sources, source, copy_async, stage_async, commit_async, holding_async, rollback_async))
    try:
        yield
    finally:
        _feature_input.reset(token)


@asynccontextmanager
async def prepare_feature_source(request, actor, operation, arguments):
    """Validate and hold a source before the destination records an intent."""
    if getattr(request.app.state, "storage_spaces_enabled", False) is not True or operation not in {"keep_thread_file", "publish", "list_project_thread_files", "install_skill", "promote_thread_file_to_shelf"}:
        yield None
        return
    from fastapi import HTTPException

    from app.gateway.agent_conversations import reject_legacy_binding
    from app.gateway.deps import get_thread_store
    from deerflow.config.paths import USER_FILES_VIRTUAL_PREFIX, VIRTUAL_PATH_PREFIX, get_paths
    from deerflow.spaces.chat_gate import conversation_gate
    from deerflow.spaces.chat_paths import ChatStoragePaths
    from deerflow.utils.thread_id import validate_thread_id

    if operation == "list_project_thread_files":
        yield await NativeFileSources.prepare(request, actor), None
        return
    body = arguments["body"]
    if operation == "promote_thread_file_to_shelf":
        if "/" in body.name or "\\" in body.name or body.name in {".", ".."}:
            raise HTTPException(404, "Thread file not found")
        normalized = VIRTUAL_PATH_PREFIX + ("/uploads/" if body.kind == "upload" else "/outputs/") + body.name
    else:
        normalized = "/" + body.path.lstrip("/")
    thread_id = None
    if operation in {"publish", "install_skill"} and normalized.startswith(USER_FILES_VIRTUAL_PREFIX + "/"):
        relative = "files/" + normalize_relative_path(normalized[len(USER_FILES_VIRTUAL_PREFIX) + 1 :])
    elif normalized.startswith((VIRTUAL_PATH_PREFIX + "/uploads/", VIRTUAL_PATH_PREFIX + "/outputs/")) or (operation == "install_skill" and normalized.startswith(VIRTUAL_PATH_PREFIX + "/workspace/")):
        thread_id = arguments.get("thread_id") or getattr(body, "thread_id", None)
        if thread_id is None:
            raise HTTPException(400, "A conversation's file needs its thread_id")
        thread_id = validate_thread_id(thread_id)
        if await get_thread_store(request).get(thread_id, user_id=actor.subject_id) is None:
            raise HTTPException(404, "Thread not found")
        await reject_legacy_binding(request, thread_id)
        relative = f"conversations/{thread_id}/user-data/" + normalize_relative_path(normalized[len(VIRTUAL_PATH_PREFIX) + 1 :])
    else:
        raise HTTPException(400, "Select a file from My Files or conversation uploads and outputs")
    sources = await NativeFileSources.prepare(request, actor)
    async with AsyncExitStack() as stack:
        if thread_id is not None:
            paths = ChatStoragePaths(get_paths(), user_id=actor.subject_id, user=sources.user, user_reference=sources.snapshot[0][:2])
            await stack.enter_async_context(conversation_gate(paths, thread_id, user_id=actor.subject_id))
        source = await stack.enter_async_context(sources.open_file(relative))
        yield sources, source


@dataclass(frozen=True)
class NativeFileSources:
    """A request snapshot that must be checked again by each owning workflow.

    Qualification runs outside SQL. Controller guards borrow the workflow's
    session and perform no filesystem work or nested resource admission.
    """

    files: SpaceFiles
    actor: PrincipalRef
    user: VerifiedVolume
    snapshot: tuple
    permissions: Permission = Permission.READ | Permission.EXPORT

    @property
    def space_id(self):
        return self.snapshot[0][0]

    @property
    def generation(self):
        return self.snapshot[0][1]

    @property
    def requests(self):
        return {self.space_id: (self.permissions, self.generation)}

    @classmethod
    async def prepare(cls, request, actor, *, required=Permission.READ | Permission.EXPORT):
        if getattr(request.app.state, "storage_spaces_enabled", False) is not True:
            return None
        if not supports_storage_credentials(request) or _credential_allowed.get() is not True:
            raise files_unavailable("credential-unsupported")
        return await cls.prepare_host(request.app, actor, required=required)

    @classmethod
    async def prepare_host(cls, app, actor, *, required=Permission.READ | Permission.EXPORT):
        if getattr(app.state, "storage_spaces_enabled", False) is not True:
            raise files_unavailable("admission-failed")
        if _credential_allowed.get() is not True:
            raise files_unavailable("credential-unsupported")
        if actor.kind != "human" or _bound_principal() != actor:
            raise InvalidPrincipal("File sources require their authenticated human owner")
        files = getattr(app.state, "storage_spaces", None)
        if not isinstance(files, SpaceFiles):
            raise files_unavailable("admission-failed")
        try:
            volume, reason, snapshot = await _verified_role(files, actor, "hm.user-space", "human:" + actor.subject_id, required=required)
        except InvalidPrincipal:
            raise
        except (BackingUnavailable, OSError, ValueError):
            raise files_unavailable("space-unverified") from None
        if volume is None:
            raise files_unavailable(reason or "space-not-assigned")
        result = cls(files, actor, volume, snapshot, required)
        async with files.registry.admitted(actor=actor, requests=result.requests) as (session, rows):
            await result.guard(session, rows)
        return result

    async def guard(self, session, rows):
        if _bound_principal() != self.actor or _credential_allowed.get() is not True:
            raise SpaceDenied("File source credentials changed")
        if self.space_id not in rows:
            raise SpaceDenied("The source was not included in the current storage workflow")
        row, grant = rows[self.space_id]
        required = self.permissions
        view = self.files.registry._view(row, grant.permissions)
        if (row.id, row.generation, grant.principal_kind, grant.subject_id) != (self.space_id, self.generation, self.actor.kind, self.actor.subject_id) or view.permissions & required != required:
            raise SpaceDenied("The admitted source no longer matches its owner and permissions")
        try:
            current = await _snapshot_in_session(session, self.files, self.actor, "hm.user-space", "human:" + self.actor.subject_id, required=required)
        except BackingUnavailable:
            raise SpaceDenied("The source role is no longer available") from None
        if current != self.snapshot:
            raise SpaceDenied("The source role changed after qualification")

    async def refresh(self):
        """Refresh a source-only phase without borrowing any caller SQL writer."""
        _volume, _reason, snapshot = await _verified_role(self.files, self.actor, "hm.user-space", "human:" + self.actor.subject_id, required=self.permissions)
        if snapshot != self.snapshot:
            raise SpaceDenied("The source role changed after qualification")
        async with self.files.registry.admitted(actor=self.actor, requests=self.requests) as (session, rows):
            await self.guard(session, rows)

    @asynccontextmanager
    async def open_file(self, relative, *, file_error=None):
        """Own a confined source through the caller's drained workflow."""
        filesystem = descriptor = None

        def acquire():
            nonlocal filesystem, descriptor
            filesystem = self.user.filesystem()
            try:
                descriptor = filesystem.open_regular(relative, follow_symlinks=False)
            except (OSError, UnsafeSpacePath) as exc:
                if file_error is not None:
                    raise file_error(exc) from exc
                raise
            return DescriptorSource(descriptor, name=relative.rsplit("/", 1)[-1])

        def close():
            nonlocal filesystem, descriptor
            try:
                if descriptor is not None:
                    held, descriptor = descriptor, None
                    os.close(held)
            finally:
                if filesystem is not None:
                    held, filesystem = filesystem, None
                    held.close()

        try:
            source = await await_drained(run_file_io(acquire))
            yield source
        finally:
            await await_drained(run_file_io(close))

    async def copy_to(self, source, destination, *, root_relative, name, admission, folder=None, folder_mode=0o777, file_mode=0o666, retain_stage=None):
        """Stage bounded pinned bytes, refresh authority, then publish exclusively.

        The caller supplies its parent workflow admission, including its own
        operation identity. SQL and file workers never wait on each other.
        """
        relative_folder = normalize_relative_path(folder or "", allow_empty=True)
        safe_name = normalize_filename(name)
        if safe_name.startswith("."):
            raise StoreError("A copied file cannot have a hidden name")
        parent = normalize_relative_path(root_relative)
        stage = None
        directories = None

        def stage_copy():
            nonlocal stage, parent, directories
            descriptor = source.open()
            try:
                before = os.fstat(descriptor)
                stage = QualifiedUploadStage(destination, parent, expected_bytes=before.st_size)
                directories = NativeDirectoryChanges(stage.filesystem)
                if relative_folder:
                    filesystem = stage.filesystem
                    for segment in relative_folder.split("/"):
                        directories.ensure(parent, segment, mode=folder_mode, check_capacity=stage.check_capacity)
                        parent += "/" + segment
                    final_parent = filesystem._open(parent, os.O_RDONLY | os.O_DIRECTORY, follow_symlinks=False)
                    os.close(stage.parent_fd)
                    stage.parent_fd, stage.parent_relative = final_parent, parent
                digest = hashlib.sha256()
                remaining = before.st_size
                while remaining:
                    stage.check_capacity(remaining)
                    chunk = os.read(descriptor, min(1 << 20, remaining))
                    if not chunk:
                        raise StoreError("The source changed while copying")
                    stage.write(chunk)
                    digest.update(chunk)
                    remaining -= len(chunk)
                if os.read(descriptor, 1) or _revision(os.fstat(descriptor)) != _revision(before):
                    raise StoreError("The source changed while copying")
                return digest.hexdigest()
            finally:
                os.close(descriptor)

        def reverify():
            current_source = self.files.catalog.verify(self.user.spec.slot_id, previous=self.user)
            self.files._check_incarnation(self.snapshot[1], current_source)
            with current_source.filesystem():
                pass
            current_destination = self.files.catalog.verify(destination.spec.slot_id, previous=destination)
            with current_destination.filesystem() as filesystem:
                for current_fd, held_fd in ((filesystem._data_fd, stage.filesystem._data_fd), (filesystem._control_fd, stage.filesystem._control_fd)):
                    current, held = os.fstat(current_fd), os.fstat(held_fd)
                    if (current.st_dev, current.st_ino) != (held.st_dev, held.st_ino):
                        raise BackingUnavailable("The destination incarnation changed while staging")

        def publish(digest):
            reverify()
            seen = set()
            while True:
                candidate = claim_unique_filename(safe_name, seen)
                try:
                    mark_effect()
                    stage.publish(candidate, file_mode=file_mode)
                    metadata = os.fstat(stage.fd)
                    relative = (relative_folder + "/" if relative_folder else "") + candidate
                    return StoredFile(relative, candidate, metadata.st_size, metadata.st_mtime, digest)
                except FileExistsError:
                    continue

        try:
            try:
                await admission()
                digest = await await_drained(run_file_io(stage_copy))
                await await_drained(run_file_io(reverify))
                await admission()
                return await await_drained(run_file_io(publish, digest))
            except BaseException:
                if directories is not None and stage.published_name is None:
                    await await_drained(run_file_io(directories.rollback))
                raise
        except ChatFilesFull:
            raise SpaceFull("The destination has no room; no file was published") from None
        except OSError as exc:
            if (stage is None or stage.published_name is None) and exc.errno in (errno.ENOSPC, errno.EDQUOT):
                raise SpaceFull("The destination has no room; no file was published") from exc
            raise
        finally:
            retained = retain_stage is not None and stage is not None and stage.published_name is not None
            if directories is not None and not retained:
                await await_drained(run_file_io(directories.close))
            if stage is not None:
                if retained:
                    retain_stage(stage, directories)
                else:
                    await await_drained(run_file_io(stage.close))
