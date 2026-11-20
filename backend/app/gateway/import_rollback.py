"""Owned legacy upload versions retained until an attachment import finishes."""

import errno
import os
import uuid

from app.gateway.chat_files import settle_rollback_entry
from deerflow.files.store import open_regular_source


class ImportRollback:
    def __init__(self, parent_fd):
        self.parent_fd = parent_fd
        self.descriptors = []
        self.published = {}

    def pin(self, source):
        fd = os.dup(source) if isinstance(source, int) else open_regular_source(source)
        self.descriptors.append(fd)
        return fd

    def rollback(self):
        if not self.published:
            return
        # The control directory lives on the destination filesystem and its
        # entries are private to this operation. Never unlink the public name.
        control = ".import-rollback-" + uuid.uuid4().hex
        os.mkdir(control, mode=0o700, dir_fd=self.parent_fd)
        try:
            fd = os.open(control, os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self.parent_fd)
            try:
                for name in reversed(tuple(self.published)):
                    private = uuid.uuid4().hex
                    try:
                        os.rename(name, private, src_dir_fd=self.parent_fd, dst_dir_fd=fd)
                    except FileNotFoundError:
                        del self.published[name]
                        continue
                    expected = self.published.pop(name)
                    settle_rollback_entry(self.parent_fd, name, fd, private, expected)
            finally:
                os.close(fd)
        finally:
            try:
                os.rmdir(control, dir_fd=self.parent_fd)
            except OSError as exc:
                # A competing replacement that could not be restored stays
                # in the private directory for operator reconciliation.
                if exc.errno not in (errno.ENOTEMPTY, errno.EEXIST):
                    raise

    def close(self):
        for fd in self.descriptors:
            os.close(fd)
        self.descriptors.clear()
