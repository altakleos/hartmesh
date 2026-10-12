"""Descriptor-qualified account sources; no Company, Home or orphan scans."""

import asyncio
import os
import stat
import unicodedata

from app.gateway.artifact_archive import reserved_dir_names, unsafe_entry_parts
from deerflow.constants import MCP_INTERNAL_DIRNAME
from deerflow.spaces.filesystem import UnsafeSpacePath


class NativeAccountFiles:
    def __init__(self, sources, spill):
        self.sources = sources
        self.reserved = reserved_dir_names({MCP_INTERNAL_DIRNAME, *([spill] if spill else [])})

    @staticmethod
    def _remember(relative, entry, metadata, plan, seen, *, size=None, sha256=None):
        from app.gateway.account_export import _Found

        if not stat.S_ISREG(metadata.st_mode):
            reason = "link" if stat.S_ISLNK(metadata.st_mode) else "not_a_file"
        elif metadata.st_nlink != 1:
            reason = "hard_link"
        elif size is not None and size != metadata.st_size:
            reason = "changed"
        else:
            key = unicodedata.normalize("NFC", entry).casefold()
            if key not in seen:
                seen.add(key)
                plan.files.append(_Found("", entry, (metadata.st_dev, metadata.st_ino), metadata.st_size, (), sha256, relative))
                return
            reason = "name_collision"
        plan.skipped.append({"path": entry, "reason": reason})

    def _walk(self, filesystem, relative, prefix, plan, seen, cancel):
        pending = [(relative, prefix, None)]
        while pending:
            if cancel.is_set():
                raise asyncio.CancelledError
            directory, archive_prefix, identity = pending.pop()
            try:
                descriptor = filesystem._open(directory, os.O_RDONLY | os.O_DIRECTORY, follow_symlinks=False)
            except FileNotFoundError:
                continue
            except UnsafeSpacePath:
                plan.skipped.append({"path": archive_prefix, "reason": "link"})
                continue
            except OSError:
                plan.skipped.append({"path": archive_prefix, "reason": "unreadable"})
                continue
            try:
                current = os.fstat(descriptor)
                if identity is not None and (current.st_dev, current.st_ino) != identity:
                    plan.skipped.append({"path": archive_prefix, "reason": "changed"})
                    continue
                try:
                    with os.scandir(descriptor) as scan:
                        children = sorted(scan, key=lambda entry: entry.name)
                except OSError:
                    plan.skipped.append({"path": archive_prefix, "reason": "unreadable"})
                    continue
                directories = []
                for child in children:
                    if cancel.is_set():
                        raise asyncio.CancelledError
                    entry = archive_prefix + "/" + child.name
                    path = directory + "/" + child.name
                    try:
                        metadata = child.stat(follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    except OSError:
                        plan.skipped.append({"path": entry, "reason": "unreadable"})
                        continue
                    if stat.S_ISDIR(metadata.st_mode) and child.name.casefold() in self.reserved:
                        continue
                    if unsafe_entry_parts([child.name], self.reserved):
                        plan.skipped.append({"path": entry, "reason": "unsafe_name"})
                        continue
                    if stat.S_ISDIR(metadata.st_mode):
                        directories.append((path, entry, (metadata.st_dev, metadata.st_ino)))
                        continue
                    try:
                        leaf = filesystem._open(path, os.O_PATH, follow_symlinks=False)
                    except FileNotFoundError:
                        plan.skipped.append({"path": entry, "reason": "vanished"})
                        continue
                    except UnsafeSpacePath:
                        plan.skipped.append({"path": entry, "reason": "link"})
                        continue
                    except OSError:
                        plan.skipped.append({"path": entry, "reason": "unreadable"})
                        continue
                    try:
                        actual = os.fstat(leaf)
                    finally:
                        os.close(leaf)
                    if (actual.st_dev, actual.st_ino) != (metadata.st_dev, metadata.st_ino):
                        plan.skipped.append({"path": entry, "reason": "changed"})
                        continue
                    self._remember(path, entry, actual, plan, seen)
                pending.extend(reversed(directories))
            finally:
                os.close(descriptor)

    def plan(self, records, cancel):
        from app.gateway.account_export import USER_DATA_AREAS, _Plan, _unsafe_segment

        plan, seen = _Plan(), set()
        with self.sources.user.filesystem() as filesystem:
            for record in records:
                thread_id = str(record["thread_id"])
                if _unsafe_segment(thread_id):
                    plan.skipped.append({"path": f"conversations/{thread_id}", "reason": "unsafe_name"})
                    continue
                for area in USER_DATA_AREAS:
                    self._walk(filesystem, f"conversations/{thread_id}/user-data/{area}", f"conversations/{thread_id}/files/{area}", plan, seen, cancel)
            self._walk(filesystem, "files", "my-files", plan, seen, cancel)
        return plan

    def project_file(self, relative, entry, plan, seen, *, optional=False, size=None, sha256=None):
        with self.sources.user.filesystem() as filesystem:
            try:
                descriptor = filesystem._open(relative, os.O_PATH, follow_symlinks=False)
            except FileNotFoundError:
                if not optional:
                    plan.skipped.append({"path": entry, "reason": "vanished"})
                return
            except UnsafeSpacePath:
                plan.skipped.append({"path": entry, "reason": "link"})
                return
            except OSError:
                plan.skipped.append({"path": entry, "reason": "unreadable"})
                return
            try:
                metadata = os.fstat(descriptor)
            finally:
                os.close(descriptor)
            self._remember(relative, entry, metadata, plan, seen, size=size, sha256=sha256)
