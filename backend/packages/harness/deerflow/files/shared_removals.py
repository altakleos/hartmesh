"""Recoverable Shared removals, serialized across processes on the same filesystem.

Bytes move into private staging before their publication is marked removed. The
database decides whether interrupted staging is restored or discarded on the
next access. No staging operation overwrites a later file at the original path.
"""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from uuid import uuid4

from deerflow.files.store import SafeFileAccessUnavailable, StoreError, normalize_relative_path, open_directory_source

_STATE_DIR = ".shared-state"
_MAX_JOURNAL_BYTES = 32768


def _private_directory(parent_fd: int, name: str) -> int:
    try:
        os.mkdir(name, 0o700, dir_fd=parent_fd)
    except FileExistsError:
        pass
    fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
    metadata = os.fstat(fd)
    if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
        os.close(fd)
        raise StoreError("Shared mutation staging must be private to the Gateway")
    return fd


class SharedMutationState:
    """Own the filesystem mutex and all staging descriptors until settled."""

    def __init__(self, root: Path):
        self.root = root
        self.root_fd: int | None = None
        self.state_fd: int | None = None
        self.lock_fd: int | None = None

    def acquire(self) -> None:
        try:
            import fcntl
        except ImportError:
            raise SafeFileAccessUnavailable("Shared mutations require filesystem advisory locking") from None
        self.root_fd = open_directory_source(self.root)
        self.state_fd = _private_directory(self.root_fd, _STATE_DIR)
        os.fsync(self.root_fd)
        self.lock_fd = os.open("mutation.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=self.state_fd)
        metadata = os.fstat(self.lock_fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid() or metadata.st_nlink != 1 or stat.S_IMODE(metadata.st_mode) != 0o600:
            raise StoreError("Shared mutation lock must be a private regular file")
        fcntl.flock(self.lock_fd, fcntl.LOCK_EX)

    def close(self) -> None:
        for attribute in ("lock_fd", "state_fd", "root_fd"):
            fd = getattr(self, attribute)
            if fd is not None:
                setattr(self, attribute, None)
                os.close(fd)

    def pending_names(self) -> list[str]:
        assert self.state_fd is not None
        return sorted(name for name in os.listdir(self.state_fd) if name.startswith("remove-"))

    def open_removal(self, name: str) -> PendingRemoval:
        return PendingRemoval(self, name)

    def stage(self, path: str, publication_id: str | None) -> PendingRemoval:
        assert self.state_fd is not None
        name = f"remove-{uuid4().hex}"
        stage = PendingRemoval(self, name, new={"path": normalize_relative_path(path), "publication_id": publication_id, "removed": False})
        try:
            stage.write_journal()
            os.fsync(self.state_fd)
            parent = open_directory_source(self.root / Path(stage.path).parent)
            try:
                metadata = os.stat(Path(stage.path).name, dir_fd=parent, follow_symlinks=False)
                if not stat.S_ISREG(metadata.st_mode):
                    raise StoreError("Only regular Shared files can be removed")
                os.rename(Path(stage.path).name, "payload", src_dir_fd=parent, dst_dir_fd=stage.fd)
                os.fsync(parent)
                os.fsync(stage.fd)
            finally:
                os.close(parent)
        except BaseException:
            # A rename may have succeeded before a durability probe failed.
            # Keep that journal for recovery instead of destroying its bytes.
            if not stage.has_payload():
                stage.finish()
            stage.close()
            raise
        return stage


class PendingRemoval:
    def __init__(self, state: SharedMutationState, name: str, *, new: dict | None = None):
        self.state, self.name = state, name
        self.fd: int | None = None
        assert state.state_fd is not None
        try:
            if new is not None:
                self.fd = _private_directory(state.state_fd, name)
                self.journal = new
            else:
                self.fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=state.state_fd)
                try:
                    source = os.open("journal.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=self.fd)
                except FileNotFoundError:
                    if self.has_payload():
                        raise StoreError("Shared removal payload has no recovery journal") from None
                    self.journal = None
                    return
                with os.fdopen(source, "rb") as file:
                    if not stat.S_ISREG(os.fstat(file.fileno()).st_mode):
                        raise StoreError("Invalid Shared removal journal")
                    raw = file.read(_MAX_JOURNAL_BYTES + 1)
                if len(raw) > _MAX_JOURNAL_BYTES:
                    raise StoreError("Shared removal journal exceeds its size limit")
                self.journal = json.loads(raw)
            path = self.journal["path"]
            if not isinstance(path, str) or normalize_relative_path(path) != path or not isinstance(self.journal.get("removed"), bool):
                raise StoreError("Invalid Shared removal journal")
            publication_id = self.journal.get("publication_id")
            if publication_id is not None and (not isinstance(publication_id, str) or not publication_id or len(publication_id) > 128):
                raise StoreError("Invalid Shared publication identity")
        except BaseException:
            self.close()
            raise

    @property
    def path(self) -> str:
        return self.journal["path"]

    @property
    def publication_id(self) -> str | None:
        return None if self.journal is None else self.journal["publication_id"]

    def close(self) -> None:
        if self.fd is not None:
            fd, self.fd = self.fd, None
            os.close(fd)

    def write_journal(self) -> None:
        assert self.fd is not None
        fd = os.open("journal.tmp", os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600, dir_fd=self.fd)
        with os.fdopen(fd, "w", encoding="utf-8") as target:
            json.dump(self.journal, target)
            target.flush()
            os.fsync(target.fileno())
        os.replace("journal.tmp", "journal.json", src_dir_fd=self.fd, dst_dir_fd=self.fd)
        os.fsync(self.fd)

    def has_payload(self) -> bool:
        assert self.fd is not None
        try:
            metadata = os.stat("payload", dir_fd=self.fd, follow_symlinks=False)
        except FileNotFoundError:
            return False
        if not stat.S_ISREG(metadata.st_mode):
            raise StoreError("Invalid Shared removal payload")
        return True

    def mark_removed(self) -> None:
        self.journal["removed"] = True
        self.write_journal()

    def restore(self) -> None:
        assert self.fd is not None
        if self.has_payload():
            parent = open_directory_source(self.state.root / Path(self.path).parent)
            try:
                try:
                    os.link("payload", Path(self.path).name, src_dir_fd=self.fd, dst_dir_fd=parent, follow_symlinks=False)
                except FileExistsError:
                    original = os.stat("payload", dir_fd=self.fd, follow_symlinks=False)
                    current = os.stat(Path(self.path).name, dir_fd=parent, follow_symlinks=False)
                    if (original.st_dev, original.st_ino) != (current.st_dev, current.st_ino):
                        raise StoreError("Shared removal recovery cannot overwrite an occupied path") from None
                os.fsync(parent)
            finally:
                os.close(parent)
        self.finish()

    def finish(self) -> None:
        assert self.fd is not None and self.state.state_fd is not None
        for name in ("payload", "journal.json", "journal.tmp"):
            try:
                os.unlink(name, dir_fd=self.fd)
            except FileNotFoundError:
                pass
        os.fsync(self.fd)
        self.close()
        os.rmdir(self.name, dir_fd=self.state.state_fd)
        os.fsync(self.state.state_fd)
