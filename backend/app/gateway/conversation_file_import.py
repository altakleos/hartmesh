"""Authenticated reads of files explicitly presented by a finished conversation."""

import hashlib
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Annotated, Literal

from fastapi import HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.gateway.agent_conversations import inspection_for_read, require_conversation
from app.gateway.authz import require_permission
from app.gateway.chat_storage import _snapshot, _snapshot_in_session
from app.gateway.deps import get_run_event_store, get_run_store, get_shared_publications_repo, get_thread_store
from app.gateway.native_file_sources import NativeFileSources
from app.gateway.routers._file_http import acting_user_id
from app.gateway.routers.spaces import _ERRORS, _actor, _http_error
from app.gateway.storage_features import storage_feature
from deerflow.config.paths import get_paths
from deerflow.features.paths import FeaturePaths
from deerflow.files.store import SafeFileAccessUnavailable, StoreError, normalize_relative_path, open_regular_source
from deerflow.persistence.agent_instances.model import AgentConversationRow
from deerflow.persistence.shared_publications.model import SharedPublicationRow
from deerflow.persistence.thread_meta.model import ThreadMetaRow
from deerflow.runtime.runs.delivery import presented_paths
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.contract import InvalidPrincipal, Permission
from deerflow.spaces.facade import storage_actor_scope
from deerflow.spaces.filesystem import UnsafeSpacePath
from deerflow.utils.file_io import await_drained, run_file_io
from deerflow.utils.thread_id import ThreadId
from deerflow.utils.time import coerce_iso


class ConversationSource(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    kind: Literal["conversation"] = "conversation"
    thread_id: ThreadId
    run_id: str = Field(min_length=1, max_length=256)
    path: str = Field(min_length=1, max_length=4096)


class PresentedFileSelection(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    source_thread_id: ThreadId
    source_run_id: str = Field(min_length=1, max_length=256)
    path: str = Field(min_length=1, max_length=4096)

    def as_source(self) -> ConversationSource:
        return ConversationSource(thread_id=self.source_thread_id, run_id=self.source_run_id, path=self.path)


class SharedPublicationSource(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    kind: Literal["shared_publication"] = "shared_publication"
    publication_id: str = Field(min_length=1, max_length=128)
    sha256: str = Field(pattern="^[a-f0-9]{64}$")


FileSource = Annotated[ConversationSource | SharedPublicationSource, Field(discriminator="kind")]


@dataclass(frozen=True)
class CapturedFile:
    name: str
    content: bytes
    sha256: str
    recheck: Callable[[], Awaitable[None]]
    admit_home_publication: Callable[[AsyncSession], Awaitable[None]]


@dataclass(frozen=True)
class _SourceRole:
    files: object
    actor: object
    namespace: str
    key: str
    observed: tuple

    @classmethod
    async def capture(cls, request, *, shared):
        if getattr(request.app.state, "storage_spaces_enabled", False) is not True:
            return None
        actor = await _actor(request)
        namespace, key = ("hm.company-space", "company") if shared else ("hm.user-space", "human:" + actor.subject_id)
        files = getattr(request.app.state, "storage_spaces", None)
        if files is None:
            raise HTTPException(503, "Source storage is unavailable")
        try:
            observed = await _snapshot(files, actor, namespace, key, required=Permission.READ | Permission.EXPORT)
        except InvalidPrincipal as exc:
            raise _http_error(exc) from None
        except BackingUnavailable:
            raise HTTPException(403, "The source is unavailable for export") from None
        if observed is None:
            raise HTTPException(403, "The source is unavailable for export")
        return cls(files, actor, namespace, key, observed)

    async def recheck(self, session=None):
        try:
            if session is None:
                current = await _snapshot(self.files, self.actor, self.namespace, self.key, required=Permission.READ | Permission.EXPORT)
            else:
                await self.files.registry._actor(self.actor, session=session)
                current = await _snapshot_in_session(session, self.files, self.actor, self.namespace, self.key, required=Permission.READ | Permission.EXPORT, lock=True)
        except InvalidPrincipal as exc:
            raise _http_error(exc) from None
        except BackingUnavailable:
            raise HTTPException(403, "The source is unavailable for export") from None
        if current != self.observed:
            raise HTTPException(403, "The source is unavailable for export")


def _missing():
    return HTTPException(404, "Conversation or file is unavailable")


async def conversation_scope(request, thread_id, *, allow_new=False):
    """Capture the exact owner/incarnation, or a separately admitted agent binding."""
    inspection = await inspection_for_read(request, thread_id)
    if inspection is not None:
        return ("agent", inspection.instance.id, inspection.thread_incarnation, inspection.instance.home_id, inspection.instance.generation, inspection.home_generation)
    record = await get_thread_store(request).get(thread_id, user_id=None)
    if record is None and allow_new:
        return None
    user_id = acting_user_id(request)
    if record is None or record.get("user_id") != user_id:
        raise _missing()
    return ("ordinary", user_id, record.get("incarnation") or record["created_at"])


async def admit_presented_source(request, thread_id, scope):
    """Authorize the source before discovering runs, including an empty page."""
    if scope[0] == "agent":
        raise HTTPException(403, "The agent's files require export access")


async def require_scope(request, thread_id, expected):
    if await conversation_scope(request, thread_id) != expected:
        raise _missing()


def _output_relative(path):
    prefix = "/mnt/user-data/outputs/"
    if not path.startswith(prefix):
        return None
    relative = path.removeprefix(prefix)
    try:
        if normalize_relative_path(relative) != relative:
            return None
    except StoreError:
        return None
    return relative


def _read_opened(file, maximum_bytes):
    original = os.fstat(file.fileno())
    if maximum_bytes is None:
        return original.st_size, original.st_mtime
    if original.st_size > maximum_bytes:
        raise HTTPException(413, "File exceeds the attachment limit")
    content = file.read(maximum_bytes + 1)
    if len(content) > maximum_bytes:
        raise HTTPException(413, "File exceeds the attachment limit")
    after = os.fstat(file.fileno())
    if len(content) != original.st_size or (after.st_mtime_ns, after.st_ctime_ns) != (original.st_mtime_ns, original.st_ctime_ns):
        raise HTTPException(409, "The file changed during the read")
    return content, original.st_mtime


async def read_presented(request, thread_id, path, *, maximum_bytes=None):
    """Read a regular presented output, or just its current size and modification time."""
    relative = _output_relative(path)
    if relative is None:
        raise _missing()
    if await require_conversation(request, thread_id):
        raise HTTPException(403, "The agent's files require export access")
    if getattr(request.app.state, "storage_spaces_enabled", False):
        actor = await _actor(request)
        with storage_actor_scope(actor):
            try:
                sources = await NativeFileSources.prepare(request, actor)
                async with sources.open_file(f"conversations/{thread_id}/user-data/outputs/{relative}") as descriptor:

                    def native_read():
                        with os.fdopen(descriptor.open(), "rb") as file:
                            return _read_opened(file, maximum_bytes)

                    result = await await_drained(run_file_io(native_read))
                await sources.refresh()
                return result
            except FileNotFoundError:
                raise _missing() from None
            except _ERRORS as exc:
                raise _http_error(exc) from None
    root = get_paths().sandbox_outputs_dir(thread_id, user_id=acting_user_id(request))

    def read():
        with os.fdopen(open_regular_source(root / relative), "rb") as file:
            return _read_opened(file, maximum_bytes)

    try:
        return await await_drained(run_file_io(read))
    except SafeFileAccessUnavailable:
        raise HTTPException(501, "Safe file reads are unavailable") from None
    except (FileNotFoundError, PermissionError, StoreError):
        raise _missing() from None


async def receipt_paths(request, thread_id, run_id, scope):
    user_id = None if scope[0] == "agent" else scope[1]
    events = await get_run_event_store(request).list_events(thread_id, run_id, event_types=["run.delivery"], limit=2, user_id=user_id)
    if len(events) != 1 or not isinstance(content := events[0].get("content"), dict):
        return []
    return [path for path in presented_paths(content) if _output_relative(path) is not None]


def _no_longer_shared():
    return HTTPException(409, {"code": "no-longer-shared"})


async def _live_publication(repo, source):
    row = await repo.publication(source.publication_id)
    if row is None or row.get("removed_at") is not None or row["sha256"] != source.sha256:
        raise _no_longer_shared()
    live = await repo.live_publication(row["path"])
    if live is None or live["publication_id"] != source.publication_id:
        raise _no_longer_shared()
    return row


@require_permission("threads", "read")
@storage_feature("hm.shared")
async def _check_shared(request: Request, source: SharedPublicationSource):
    repo = get_shared_publications_repo(request)
    return await _live_publication(repo, source)


@require_permission("threads", "read")
@storage_feature("hm.shared")
async def _shared_capture(request: Request, source: SharedPublicationSource, maximum_bytes: int):
    repo = get_shared_publications_repo(request)
    row = await _live_publication(repo, source)
    role = await _SourceRole.capture(request, shared=True)
    if row["size"] > maximum_bytes:
        raise HTTPException(413, "File exceeds the attachment limit")
    paths = get_paths()

    def capture():
        relative = normalize_relative_path(row["path"])
        if isinstance(paths, FeaturePaths):
            with paths._volume("hm.shared").filesystem() as filesystem:
                with os.fdopen(filesystem.open_regular("shared/" + relative, follow_symlinks=False), "rb") as file:
                    return _read_opened(file, min(maximum_bytes, row["size"]))[0]
        with os.fdopen(open_regular_source(paths.shared_dir() / relative), "rb") as file:
            return _read_opened(file, min(maximum_bytes, row["size"]))[0]

    try:
        content = await await_drained(run_file_io(capture))
    except SafeFileAccessUnavailable:
        raise HTTPException(501, "Safe file reads are unavailable") from None
    except (FileNotFoundError, StoreError, UnsafeSpacePath, HTTPException):
        raise _no_longer_shared() from None
    current = await _live_publication(repo, source)
    if (current["path"], current["size"]) != (row["path"], row["size"]):
        raise _no_longer_shared()
    digest = await run_file_io(lambda: hashlib.sha256(content).hexdigest())
    if len(content) != row["size"] or digest != source.sha256:
        raise _no_longer_shared()

    async def recheck():
        current = await _check_shared(request=request, source=source)
        if (current["path"], current["size"]) != (row["path"], row["size"]):
            raise _no_longer_shared()
        if role is not None:
            await role.recheck()

    async def admit(session):
        current = await session.get(SharedPublicationRow, source.publication_id, populate_existing=True, with_for_update={"read": True})
        if current is None or current.removed_at is not None or (current.path, current.size, current.sha256) != (row["path"], row["size"], digest):
            raise _no_longer_shared()
        latest = await session.scalar(
            select(SharedPublicationRow.publication_id).where(SharedPublicationRow.path == row["path"], SharedPublicationRow.removed_at.is_(None)).order_by(SharedPublicationRow.published_at.desc()).limit(1).with_for_update(read=True)
        )
        if latest != source.publication_id:
            raise _no_longer_shared()
        if role is None:
            raise HTTPException(403, "A Home import requires the company's current Shared grant")
        await role.recheck(session)

    return CapturedFile(PurePosixPath(row["path"]).name, content, digest, recheck, admit)


async def capture_source(request, source: ConversationSource | SharedPublicationSource, *, maximum_bytes: int, home_destination=False, acknowledge_disclosure=False):
    if isinstance(source, SharedPublicationSource):
        return await _shared_capture(request=request, source=source, maximum_bytes=maximum_bytes)
    if not isinstance(source, ConversationSource):
        raise HTTPException(422, "Unknown file source")
    return await _conversation_capture(request=request, source=source, maximum_bytes=maximum_bytes, home_destination=home_destination, acknowledge_disclosure=acknowledge_disclosure)


@require_permission("threads", "read")
@require_permission("runs", "read")
async def _conversation_capture(request: Request, source: ConversationSource, maximum_bytes: int, home_destination: bool, acknowledge_disclosure: bool):
    scope = await conversation_scope(request, source.thread_id)
    if home_destination:
        if scope[0] == "agent":
            raise HTTPException(403, {"code": "not-between-agents"})
        if not acknowledge_disclosure:
            raise HTTPException(409, {"code": "disclosure-needs-confirmation"})
    await admit_presented_source(request, source.thread_id, scope)
    user_id = None if scope[0] == "agent" else scope[1]
    record = await get_run_store(request).get(source.run_id, user_id=user_id)
    if record is None or record.get("thread_id") != source.thread_id or record.get("operation_kind", "run") != "run" or (user_id is not None and record.get("user_id") != user_id):
        raise _missing()
    if record["status"] not in (RunStatus.success, RunStatus.error, RunStatus.timeout, RunStatus.interrupted):
        raise HTTPException(409, "This answer has not finished")
    if source.path not in await receipt_paths(request, source.thread_id, source.run_id, scope):
        raise _missing()
    role = await _SourceRole.capture(request, shared=False) if scope[0] == "ordinary" else None
    payload, _ = await read_presented(request, source.thread_id, source.path, maximum_bytes=maximum_bytes)

    async def recheck():
        await require_scope(request, source.thread_id, scope)
        if role is not None:
            await role.recheck()
        if scope[0] == "agent":
            await read_presented(request, source.thread_id, source.path)

    async def admit(session):
        if scope[0] != "ordinary":
            raise HTTPException(403, {"code": "not-between-agents"})
        row = await session.get(ThreadMetaRow, source.thread_id, populate_existing=True, with_for_update={"read": True})
        if row is None or (row.user_id, row.incarnation or coerce_iso(row.created_at)) != scope[1:] or await session.get(AgentConversationRow, source.thread_id) is not None:
            raise _missing()
        if role is None:
            raise HTTPException(403, "A Home import requires the owner's current export grant")
        await role.recheck(session)

    await recheck()
    return CapturedFile(PurePosixPath(source.path).name, payload, await run_file_io(lambda: hashlib.sha256(payload).hexdigest()), recheck, admit)
