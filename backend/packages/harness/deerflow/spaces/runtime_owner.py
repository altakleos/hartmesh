"""One participating host runtime owns a managed pool through final drainage."""

import os
import stat
import subprocess
import sys
from pathlib import Path
from threading import RLock

from deerflow.files.store import open_directory_source
from deerflow.spaces.docker import StorageHostUnqualified

RUNTIME_LOCK = ".gateway-owner.lock"
_provider_binding_lock = RLock()


class StorageRuntimeUnavailable(StorageHostUnqualified):
    """Ownership qualification may resume in the same live Gateway lifetime."""


def _identity(metadata):
    return metadata.st_dev, metadata.st_ino


class PoolRuntimeOwner:
    """Private retained host ownership, never user or resource authorization.

    The operator creates the stable file. A runtime never repairs or replaces it.
    Its lifecycle owner drains producers, native workers and SQL before close.
    Owned subprocesses inherit a duplicate of the open description: closing a
    killed launcher cannot release a surviving child's exclusion.
    """

    def __init__(self, pool: Path):
        self.pool = Path(pool)
        self._lock = RLock()
        self._pool_fd = self._fd = None
        self._provider = None
        self._closing = self._closed = False

    @property
    def provider(self):
        return self._provider

    def mark_closing(self):
        # Publication checks this again after construction. Do not wait for a
        # constructor's mutex on the owning event loop before signalling closure.
        self._closing = True

    def accepts(self, provider):
        """Memory-only final publication check; never performs filesystem I/O."""
        return not self._closing and self.owns(provider)

    @property
    def closing(self):
        return self._closing or self._closed

    def owns(self, provider):
        """Existing teardown retains its ownership while admission is closing."""
        return not self._closed and self._fd is not None and provider is not None and provider is self._provider

    def drain_provider(self):
        """Join any retained constructor before taking its shutdown snapshot."""
        with self._lock:
            return self._provider

    @staticmethod
    def _private_file(metadata):
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 or metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_size != 0:
            raise StorageRuntimeUnavailable("Managed storage runtime ownership is not a private empty host-owned file")

    @staticmethod
    def _private_pool(metadata):
        if metadata.st_uid not in (0, os.getuid()) or metadata.st_mode & 0o022:
            raise StorageRuntimeUnavailable("Managed storage pool is not host-controlled")

    def _assert_held(self):
        if self._closed or self._fd is None or self._pool_fd is None:
            raise StorageRuntimeUnavailable("Managed storage runtime ownership is unavailable")
        current = open_directory_source(self.pool)
        try:
            parent = os.fstat(current)
            self._private_pool(parent)
            if _identity(parent) != _identity(os.fstat(self._pool_fd)):
                raise StorageRuntimeUnavailable("Managed storage pool identity changed")
        finally:
            os.close(current)
        opened = os.fstat(self._fd)
        named = os.stat(RUNTIME_LOCK, dir_fd=self._pool_fd, follow_symlinks=False)
        self._private_file(opened)
        self._private_file(named)
        if _identity(named) != _identity(opened):
            raise StorageRuntimeUnavailable("Managed storage runtime ownership inode changed")

    def acquire(self):
        with self._lock:
            if self._closing or self._closed:
                raise StorageRuntimeUnavailable("Managed storage runtime is closing")
            if sys.platform != "linux":
                raise StorageRuntimeUnavailable("Managed runtime ownership requires Linux")
            try:
                if self._fd is not None:
                    self._assert_held()
                    return
                pool_fd = open_directory_source(self.pool)
                file_fd = None
                try:
                    self._private_pool(os.fstat(pool_fd))
                    file_fd = os.open(RUNTIME_LOCK, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=pool_fd)
                    self._private_file(os.fstat(file_fd))
                    import fcntl

                    fcntl.flock(file_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self._pool_fd, self._fd = pool_fd, file_fd
                    try:
                        self._assert_held()
                    except BaseException:
                        self._pool_fd = self._fd = None
                        raise
                except BaseException:
                    if file_fd is not None:
                        os.close(file_fd)
                    os.close(pool_fd)
                    raise
            except OSError as exc:
                raise StorageRuntimeUnavailable("Managed storage pool is unavailable or owned by another runtime") from exc

    def resolve_provider(self, getter):
        with self._lock:
            self.acquire()
            if self._provider is None:
                provider = getter()
                if provider is None:
                    raise StorageRuntimeUnavailable("Managed storage has no baseline provider")
                with _provider_binding_lock:
                    bound = getattr(provider, "_pool_runtime_owner", None)
                    host = getattr(provider, "_chat_storage_host", None)
                    if (bound is not None and bound is not self) or (host is not None and getattr(host, "pool_runtime_owner", None) is not self):
                        raise StorageRuntimeUnavailable("Provider belongs to another runtime lifetime")
                    provider._pool_runtime_owner = self
                    self._provider = provider
            if self._closing:
                raise StorageRuntimeUnavailable("Managed storage runtime closed during provider construction")
            self.assert_provider(self._provider)
            return self._provider

    def assert_provider(self, provider, *, teardown=False):
        with self._lock:
            if (self._closing and not teardown) or provider is None or provider is not self._provider:
                raise StorageRuntimeUnavailable("Managed storage requires its exact qualified runtime provider")
            try:
                self._assert_held()
            except OSError as exc:
                raise StorageRuntimeUnavailable("Managed storage runtime ownership cannot be verified") from exc

    def run(self, *args, **kwargs):
        with self._lock:
            self._assert_held()
            descriptor = os.dup(self._fd)
        try:
            inherited = tuple(kwargs.pop("pass_fds", ()))
            return subprocess.run(*args, **kwargs, pass_fds=(*inherited, descriptor))
        finally:
            # Never LOCK_UN: duplicates and surviving children share ownership.
            os.close(descriptor)

    def close(self):
        with self._lock:
            self._closing = self._closed = True
            for name in ("_fd", "_pool_fd"):
                descriptor = getattr(self, name)
                if descriptor is not None:
                    setattr(self, name, None)
                    os.close(descriptor)
