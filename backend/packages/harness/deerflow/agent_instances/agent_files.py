"""One direct, attributed host write through the durable Space file boundary."""

import hashlib
import re
from dataclasses import dataclass
from typing import BinaryIO, Literal

from deerflow.persistence.spaces.files import SpaceFileOperationRow
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.contract import Permission, SpaceConflict
from deerflow.spaces.filesystem import FileConflict, SpaceFull, UnsafeSpacePath, _path
from deerflow.spaces.provision import HOST_WRITE_MARGIN_BYTES, HOST_WRITE_MIN_INODES
from deerflow.spaces.service import MAX_TRANSFER_BYTES, SpaceOperationPending
from deerflow.utils.file_io import await_drained, run_file_io

from .contract import InstanceIdentity, Refusal, ResidentFilesBusy

AGENT_FILE_REFUSALS = {
    "file-too-large": (413, "The file is larger than 64 MiB, the most one file can be."),
    "files-full": (507, "{name}'s files are full. Delete files or ask for more room."),
    "file-write-failed": (503, "The file could not be saved. Try again."),
    "invalid-name": (400, "That name cannot be used."),
    "reserved-path": (409, "That folder is kept for {name}'s own tools."),
    "name-collision": (409, "Something with that name already exists here."),
    "file-changed": (409, "This file changed after it was opened. Reload it before saving."),
    "directory-not-empty": (409, "Empty this folder before deleting it."),
    "agent-not-active": (409, "{name} is paused or retired, so {name}'s files cannot be changed."),
    "not-found": (404, "That file or folder is no longer there."),
}


class AgentFileRefusal(Refusal):
    def __init__(self, reason: str, *, name: str = "", **extra):
        status, template = AGENT_FILE_REFUSALS[reason]
        super().__init__(template.format(name=name), status=status, reason=reason, **extra)


@dataclass(frozen=True)
class AgentFileResult:
    path: str
    size: int | None
    sha256: str | None
    deduplicated: bool


class AgentFiles:
    def __init__(self, agents):
        self.agents = agents

    @staticmethod
    def _hash(content, size):
        content.seek(0)
        digest, count = hashlib.sha256(), 0
        while True:
            block = content.read(min(1 << 20, size - count + 1))
            if not isinstance(block, bytes):
                raise ValueError("File content must be a seekable binary stream")
            if not block:
                break
            count += len(block)
            if count > size:
                raise ValueError("File size does not match its content")
            digest.update(block)
        content.seek(0)
        if count != size:
            raise ValueError("File size does not match its content")
        return digest.hexdigest()

    @staticmethod
    def _payload(content, size, digest):
        payload = content.read(MAX_TRANSFER_BYTES + 1)
        if not isinstance(payload, bytes) or len(payload) != size or hashlib.sha256(payload).hexdigest() != digest:
            raise ValueError("File content changed before writing")
        return payload

    @staticmethod
    def _result(action, request, value):
        if action == "put":
            return AgentFileResult(**value)
        return AgentFileResult(request["destination"] if action == "rename" else request["path"], request["size"], value if action == "write" else None, False)

    async def apply(
        self,
        *,
        instance: InstanceIdentity,
        requester,
        operation_id: str,
        action: Literal["put", "mkdir", "rename", "remove"],
        path: str,
        destination: str | None = None,
        content: BinaryIO | None = None,
        size: int | None = None,
        expected_sha256: str | None = None,
        admission=None,
    ) -> AgentFileResult:
        if instance.status != "active":
            raise AgentFileRefusal("agent-not-active", name=instance.name)
        if not isinstance(operation_id, str) or not re.fullmatch(r"[0-9a-f]{32}", operation_id):
            raise ValueError("Operation ID must be 32 lower-case hexadecimal characters")
        if action not in ("put", "mkdir", "rename", "remove") or (action == "rename") != (destination is not None):
            raise ValueError("Only a rename requires a destination")
        if requester.kind != "human":
            raise ValueError("A file write requires human requester attribution")
        try:
            for value in (path, destination) if destination is not None else (path,):
                _path(value)
                if any(len(component.encode("utf-8")) > 255 for component in value.split("/")):
                    raise AgentFileRefusal("invalid-name")
                if value.split("/", 1)[0] == ".agent-skill-packages":
                    raise AgentFileRefusal("reserved-path", name=instance.name)
        except (UnsafeSpacePath, UnicodeError):
            raise AgentFileRefusal("invalid-name") from None
        if action == "put":
            if content is None or type(size) is not int or size < 0:
                raise ValueError("An upload requires binary content and its non-negative size")
            if size > MAX_TRANSFER_BYTES:
                raise AgentFileRefusal("file-too-large")
            if expected_sha256 is not None and (not isinstance(expected_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256)):
                raise ValueError("An edit requires a SHA-256 revision")
            digest = await await_drained(run_file_io(self._hash, content, size))
        else:
            if content is not None or size is not None or expected_sha256 is not None:
                raise ValueError("Only a put accepts file content or a revision")
            digest = None
        recorded_action = "write" if action == "put" and expected_sha256 is not None else action
        request = {"action": recorded_action, "path": path, "destination": destination, "sha256": digest, "size": size, "on_behalf_of": requester.subject_id}
        files = self.agents.files
        try:
            async with files.registry.admitted(actor=instance.principal, requests={instance.home_id: (Permission.READ, None)}, operation_id=(instance.home_id, operation_id)) as (session, admitted):
                if admission is not None:
                    await admission(session, admitted)
                row = await session.get(SpaceFileOperationRow, (instance.home_id, operation_id))
                if row is not None:
                    if row.request != request or (row.actor_kind, row.actor_id) != (instance.principal.kind, instance.principal.subject_id):
                        raise SpaceConflict("The operation ID already identifies a different request")
                    if row.phase == "complete":
                        return self._result(recorded_action, request, row.result["value"])
                    if row.phase == "failed":
                        reason = row.result.get("reason") if isinstance(row.result, dict) else None
                        raise AgentFileRefusal(reason if reason in AGENT_FILE_REFUSALS else "file-write-failed", name=instance.name)
                    raise ResidentFilesBusy(instance.name, cause="workflow")
            home = await files.registry.get(actor=instance.principal, space_id=instance.home_id)
            if action in ("put", "mkdir"):
                quota = await files.quota(actor=instance.principal, space_id=instance.home_id)
                if (size or 0) + HOST_WRITE_MARGIN_BYTES > quota["available_bytes"] or quota["available_inodes"] < HOST_WRITE_MIN_INODES:
                    raise AgentFileRefusal("files-full", name=instance.name)
            options = {"actor": instance.principal, "space_id": instance.home_id, "expected_generation": home.generation, "operation_id": operation_id, "path": path, "on_behalf_of": requester.subject_id, "controller_admission": admission}
            if action == "put":
                payload = await await_drained(run_file_io(self._payload, content, size, digest))
                if expected_sha256 is None:
                    value = await files.put_unique(**options, content=payload)
                else:
                    value = await files.write(**options, content=payload, expected_sha256=expected_sha256, create=False)
            elif action == "rename":
                value = await files.rename(**options, destination=destination)
            else:
                value = await getattr(files, action)(**options)
            return self._result(recorded_action, request, value)
        except SpaceFull:
            raise AgentFileRefusal("files-full", name=instance.name) from None
        except FileNotFoundError:
            raise AgentFileRefusal("not-found") from None
        except UnsafeSpacePath:
            raise AgentFileRefusal("invalid-name") from None
        except FileConflict as exc:
            reasons = {"Destination already exists": "name-collision", "File differs from the loaded revision": "file-changed", "File changed before replacement": "file-changed", "Directory is not empty": "directory-not-empty"}
            raise AgentFileRefusal(reasons.get(str(exc), "file-write-failed")) from None
        except SpaceOperationPending as exc:
            raise ResidentFilesBusy(instance.name, cause=exc.cause) from None
        except BackingUnavailable:
            raise AgentFileRefusal("file-write-failed") from None
        except SpaceConflict as exc:
            if str(exc) != "Stale space generation":
                raise
            raise AgentFileRefusal("file-write-failed") from None
