"""Host file effects below freshly admitted conversation storage.

Path projections select a qualified volume; every effect still refreshes host
admission. Descriptor work stays off the owning loop and outside SQL sessions.
"""

import errno
import hashlib
import os
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path

from fastapi import HTTPException

from app.gateway import chat_storage
from app.gateway.routers._file_http import DescriptorFileResponse
from deerflow.spaces.chat_paths import ChatFilesFull
from deerflow.spaces.filesystem import FilesystemUnavailable, UnsafeSpacePath, _path
from deerflow.utils.file_io import await_drained, run_file_io


def files_unavailable(reason):
    return HTTPException(
        403 if reason == "credential-unsupported" else 503,
        {
            "code": "storage-unavailable",
            "reason": reason,
            "message": "Your files are not available right now. Chats run without them.",
        },
    )


def files_full():
    return ChatFilesFull()


async def require_legacy_file_operation(request, thread_id=None):
    """Fail closed until this file door has a qualified native adapter."""
    scope = getattr(request, "scope", None)
    app = scope.get("app") if isinstance(scope, dict) else getattr(request, "app", None)
    if getattr(getattr(app, "state", None), "storage_spaces_enabled", False) is not True:
        return
    if thread_id is not None:
        from deerflow.utils.thread_id import validate_thread_id

        try:
            validate_thread_id(thread_id)
        except ValueError:
            raise HTTPException(400, "Invalid conversation ID") from None
        prepared = await chat_storage.prepare_chat_request(request, thread_id)
        if prepared is None or prepared.mounts.user is None:
            raise files_unavailable(prepared.reason if prepared is not None else "admission-failed")
    raise HTTPException(501, "This file operation is unavailable for managed conversations")


@dataclass(frozen=True)
class ChatFileAccess:
    prepared: chat_storage.PreparedChatStorage

    def source(self, virtual_path):
        try:
            value = _path(virtual_path.lstrip("/"))
        except (ValueError, UnsafeSpacePath):
            raise HTTPException(400, "Use a confined conversation file path") from None
        mounts = self.prepared.mounts
        if value.startswith("mnt/acp-workspace/"):
            return mounts.user, "conversations/" + mounts.thread_id + "/acp-workspace/" + value.removeprefix("mnt/acp-workspace/")
        if not value.startswith("mnt/user-data/"):
            raise HTTPException(400, "Use a confined conversation file path")
        relative = value.removeprefix("mnt/user-data/")
        leaf, separator, tail = relative.partition("/")
        if not separator or not tail:
            raise HTTPException(400, "Select a conversation file")
        if leaf == "files":
            return mounts.user, "files/" + tail
        if leaf == "shared":
            if mounts.company is None:
                raise files_unavailable(self.prepared.shared_reason or "space-not-assigned")
            return mounts.company, "shared/" + tail
        if leaf not in ("workspace", "uploads", "outputs"):
            raise HTTPException(400, "Use a mounted conversation file area")
        return mounts.user, "conversations/" + mounts.thread_id + "/user-data/" + relative

    async def refresh(self, request, *, include_shared=False):
        current = await prepare_chat_files(request, self.prepared.mounts.thread_id)
        signature = self.prepared.paths.routing_signature()
        current_signature = current.prepared.paths.routing_signature() if current is not None else None
        if current_signature is None or current_signature[:2] != signature[:2] or (include_shared and current_signature[2] != signature[2]):
            raise files_unavailable("space-unverified")
        return current

    async def read_with(self, request, virtual_path, reader):
        shared = virtual_path.lstrip("/").startswith("mnt/user-data/shared/")
        current = await self.refresh(request, include_shared=shared)
        volume, relative = current.source(virtual_path)

        def read():
            with volume.filesystem() as filesystem:
                with os.fdopen(filesystem.open_regular(relative, follow_symlinks=False), "rb") as source:
                    return reader(source)

        try:
            result = await await_drained(run_file_io(read))
        except FileNotFoundError:
            raise HTTPException(404, "Artifact not found") from None
        except FilesystemUnavailable:
            raise files_unavailable("space-unverified") from None
        except UnsafeSpacePath:
            raise HTTPException(400, "The file is not accessible within its qualified Space") from None
        await current.refresh(request, include_shared=shared)
        return result

    async def list_uploads(self, request):
        from deerflow.uploads.manager import enrich_file_listing, is_upload_staging_file

        current = await self.refresh(request)
        volume = current.prepared.mounts.user
        thread_id = current.prepared.mounts.thread_id
        relative = f"conversations/{thread_id}/user-data/uploads"

        def listing():
            files = []
            with volume.filesystem() as filesystem:
                try:
                    parent = filesystem._open(relative, os.O_RDONLY | os.O_DIRECTORY, follow_symlinks=False)
                except FileNotFoundError:
                    return {"files": [], "count": 0}
                try:
                    with os.scandir(parent) as entries:
                        for entry in entries:
                            if is_upload_staging_file(entry.name):
                                continue
                            try:
                                metadata = entry.stat(follow_symlinks=False)
                            except FileNotFoundError:
                                continue
                            if not stat.S_ISREG(metadata.st_mode):
                                continue
                            files.append({"filename": entry.name, "size": metadata.st_size, "path": str(volume.data_path / relative / entry.name), "extension": Path(entry.name).suffix, "modified": metadata.st_mtime})
                finally:
                    os.close(parent)
            files.sort(key=lambda item: item["filename"])
            return enrich_file_listing({"files": files, "count": len(files)}, thread_id)

        try:
            result = await await_drained(run_file_io(listing))
        except FilesystemUnavailable:
            raise files_unavailable("space-unverified") from None
        except UnsafeSpacePath:
            raise HTTPException(400, "The uploads directory is not accessible within its qualified Space") from None
        await current.refresh(request)
        return result

    async def delete_upload(self, request, filename):
        try:
            name = _path(filename)
            if "/" in name:
                raise UnsafeSpacePath("Choose one uploaded filename")
        except UnsafeSpacePath:
            raise HTTPException(400, "Invalid uploaded filename") from None
        current = await self.refresh(request)
        volume = current.prepared.mounts.user
        relative = f"conversations/{current.prepared.mounts.thread_id}/user-data/uploads"

        def remove():
            with volume.filesystem() as filesystem:
                parent = filesystem._open(relative, os.O_RDONLY | os.O_DIRECTORY, follow_symlinks=False)
                try:
                    if not stat.S_ISREG(os.stat(name, dir_fd=parent, follow_symlinks=False).st_mode):
                        raise FileNotFoundError(name)
                    os.unlink(name, dir_fd=parent)
                finally:
                    os.close(parent)

        try:
            await await_drained(run_file_io(remove))
        except FileNotFoundError:
            raise HTTPException(404, "Uploaded file not found") from None
        except FilesystemUnavailable:
            raise files_unavailable("space-unverified") from None
        except UnsafeSpacePath:
            raise HTTPException(400, "The uploads directory is not accessible within its qualified Space") from None
        return {"success": True, "message": f"Deleted {name}"}

    async def edit_artifact(self, request, virtual_path, content, expected_sha256, *, maximum_bytes):
        if not virtual_path.lstrip("/").startswith("mnt/user-data/outputs/"):
            raise HTTPException(400, "Edit an outputs artifact")

        def loaded(source):
            before = os.fstat(source.fileno())
            raw = source.read(maximum_bytes + 1)
            if before.st_size > maximum_bytes or len(raw) > maximum_bytes:
                raise HTTPException(413, "Artifact is too large to edit")
            if b"\x00" in raw:
                raise HTTPException(415, "Binary artifacts cannot be edited")
            try:
                raw.decode("utf-8")
            except UnicodeDecodeError:
                raise HTTPException(415, "Only UTF-8 text artifacts can be edited") from None
            if hashlib.sha256(raw).hexdigest() != expected_sha256:
                raise HTTPException(412, "Artifact changed since it was opened")
            after = os.fstat(source.fileno())
            if _revision(before) != _revision(after):
                raise HTTPException(412, "Artifact changed while it was read")
            return before

        before = await self.read_with(request, virtual_path, loaded)
        current = await self.refresh(request)
        volume, relative = current.source(virtual_path)
        parent, _, name = relative.rpartition("/")
        stage = None

        def prepare():
            nonlocal stage
            stage = QualifiedUploadStage(volume, parent, expected_bytes=len(content))
            stage.write(content)

        try:
            await await_drained(run_file_io(prepare))
            await current.refresh(request)
            await await_drained(run_file_io(stage.replace, name, before))
        except ChatFilesFull:
            raise files_full() from None
        except OSError as exc:
            if exc.errno in (errno.ENOSPC, errno.EDQUOT):
                if stage is not None and stage.replaced:
                    raise files_unavailable("space-unverified") from None
                raise files_full() from None
            raise
        except FilesystemUnavailable:
            raise files_unavailable("space-unverified") from None
        except UnsafeSpacePath:
            raise HTTPException(400, "The outputs directory changed before publication") from None
        finally:
            if stage is not None:
                await await_drained(run_file_io(stage.close))
        return hashlib.sha256(content).hexdigest()


def _revision(metadata):
    return metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns, metadata.st_ctime_ns


async def prepare_chat_files(request, thread_id):
    prepared = await chat_storage.prepare_chat_request(request, thread_id)
    if prepared is None:
        return None
    if prepared.mounts.user is None:
        raise files_unavailable(prepared.reason or "space-unverified")
    if prepared.mounts.thread_id != thread_id:
        raise files_unavailable("space-unverified")
    return ChatFileAccess(prepared)


class ChatFileResponse(DescriptorFileResponse):
    """Serve one pinned qualified inode after fresh owner-loop admission."""

    def __init__(self, request, access, virtual_path, *, download=False, hash_max_bytes=None):
        access.source(virtual_path)
        super().__init__(Path(virtual_path), download=download, hash_max_bytes=hash_max_bytes)
        self.headers["cache-control"] = "private, no-store"
        self._request, self._access, self._virtual_path = request, access, virtual_path
        self._filesystem = None
        self._relative = None

    def _open_descriptor(self):
        return self._filesystem.open_regular(self._relative, follow_symlinks=False)

    async def _prepare_owned(self):
        shared = self._virtual_path.lstrip("/").startswith("mnt/user-data/shared/")
        access = await self._access.refresh(self._request, include_shared=shared)
        volume, self._relative = access.source(self._virtual_path)

        def acquire_and_prepare():
            self._filesystem = volume.filesystem()
            self._prepare()

        await await_drained(run_file_io(acquire_and_prepare))
        # A slow open/sniff/hash cannot carry a revoked grant into the first
        # response bytes. The stream thereafter owns its already admitted inode.
        await access.refresh(self._request, include_shared=shared)

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        except FilesystemUnavailable:
            raise files_unavailable("space-unverified") from None
        except UnsafeSpacePath:
            raise HTTPException(400, "The file is not accessible within its qualified Space") from None
        finally:
            if self._filesystem is not None:
                filesystem, self._filesystem = self._filesystem, None
                await await_drained(run_file_io(filesystem.close))


class QualifiedUploadStage:
    """Private, quota-counted staging with publication through pinned parents.

    This supplies containment, not authority. The owner refreshes admission
    before creation, streaming and publication, and drains every worker before
    close. The sandbox never receives the control directory.
    """

    def __init__(self, volume, parent_relative, *, expected_bytes=0, suffix=""):
        self.parent_relative = _path(parent_relative)
        if suffix and (not suffix.startswith(".") or not suffix[1:].isalnum() or len(suffix) > 17):
            raise ValueError("Invalid private upload suffix")
        self.filesystem = None
        self.fd = self.parent_fd = None
        self.stage_name = None
        self.published_name = None
        self.replaced = False
        try:
            self.filesystem = volume.filesystem()
            self.parent_fd = self.filesystem._open(self.parent_relative, os.O_RDONLY | os.O_DIRECTORY, follow_symlinks=False)
            self.check_capacity(expected_bytes)
            name = "chat-upload-" + uuid.uuid4().hex + suffix
            self.fd = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=self.filesystem._control_fd)
            self.stage_name = name
        except BaseException:
            self.close()
            raise

    def check_capacity(self, expected_bytes, *, minimum_inodes=2):
        if type(expected_bytes) is not int or expected_bytes < 0:
            raise ValueError("Upload capacity requires a nonnegative byte count")
        free = os.fstatvfs(self.filesystem._control_fd)
        if free.f_bavail * free.f_frsize < expected_bytes + (1 << 20) or free.f_favail < minimum_inodes:
            raise ChatFilesFull()

    def write(self, content):
        offset = 0
        while offset < len(content):
            written = os.write(self.fd, content[offset:])
            if written <= 0:
                raise OSError("Upload write made no progress")
            offset += written

    def _check_parent(self):
        current = self.filesystem._open(self.parent_relative, os.O_RDONLY | os.O_DIRECTORY, follow_symlinks=False)
        try:
            expected, actual = os.fstat(self.parent_fd), os.fstat(current)
            if (expected.st_dev, expected.st_ino) != (actual.st_dev, actual.st_ino):
                raise FilesystemUnavailable("The uploads directory changed before publication")
        finally:
            os.close(current)

    def publish(self, filename):
        name = _path(filename)
        if "/" in name:
            raise UnsafeSpacePath("Uploads require a single filename")
        self._check_parent()
        os.fsync(self.fd)
        os.fchmod(self.fd, 0o660)
        os.link(self.stage_name, name, src_dir_fd=self.filesystem._control_fd, dst_dir_fd=self.parent_fd, follow_symlinks=False)
        self.published_name = name
        os.fsync(self.parent_fd)

    def replace(self, filename, expected):
        name = _path(filename)
        if "/" in name:
            raise UnsafeSpacePath("Edit one artifact filename")
        self._check_parent()
        os.fsync(self.fd)
        os.fchmod(self.fd, stat.S_IMODE(expected.st_mode) & 0o777)
        current = os.stat(name, dir_fd=self.parent_fd, follow_symlinks=False)
        if not stat.S_ISREG(current.st_mode) or _revision(current) != _revision(expected):
            raise HTTPException(412, "Artifact changed before replacement")
        os.replace(self.stage_name, name, src_dir_fd=self.filesystem._control_fd, dst_dir_fd=self.parent_fd)
        self.stage_name = None
        self.replaced = True
        os.fsync(self.parent_fd)
        os.fsync(self.filesystem._control_fd)

    def rollback_published(self):
        if self.published_name is None:
            return
        name = self.published_name
        # Move first: a public name can change between an inode observation and
        # unlink. Only a private control name can be safely removed by identity.
        private = "chat-rollback-" + uuid.uuid4().hex
        try:
            self.filesystem._rename_no_replace(self.parent_fd, name, self.filesystem._control_fd, private)
        except FileNotFoundError:
            self.published_name = None
            return
        self.published_name = None
        current = os.stat(private, dir_fd=self.filesystem._control_fd, follow_symlinks=False)
        expected = os.fstat(self.fd)
        if (current.st_dev, current.st_ino) == (expected.st_dev, expected.st_ino):
            os.unlink(private, dir_fd=self.filesystem._control_fd)
            return
        try:
            self.filesystem._rename_no_replace(self.filesystem._control_fd, private, self.parent_fd, name)
        except BaseException:
            # Preserve the competing writer's bytes in control if another
            # writer claimed the name during restoration. Never erase them.
            raise FilesystemUnavailable("Upload rollback requires reconciliation; replacement bytes were preserved") from None

    @property
    def private_path(self):
        return Path(f"/proc/self/fd/{self.filesystem._control_fd}/{self.stage_name}")

    def close(self):
        try:
            if self.stage_name is not None and self.filesystem is not None:
                name, self.stage_name = self.stage_name, None
                try:
                    os.unlink(name, dir_fd=self.filesystem._control_fd)
                except FileNotFoundError:
                    pass
        finally:
            try:
                for attribute in ("fd", "parent_fd"):
                    descriptor = getattr(self, attribute)
                    if descriptor is not None:
                        setattr(self, attribute, None)
                        os.close(descriptor)
            finally:
                if self.filesystem is not None:
                    filesystem, self.filesystem = self.filesystem, None
                    filesystem.close()
