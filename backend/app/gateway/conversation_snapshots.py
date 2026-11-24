"""Read authorized, server-published snapshots and their still-live members."""

import os
from collections import OrderedDict
from dataclasses import replace
from threading import Lock
from typing import Literal

from fastapi import HTTPException, Request
from pydantic import Field, ValidationError
from sqlalchemy import select

from app.gateway.authz import get_auth_context, require_permission
from app.gateway.conversation_file_import import SharedPublicationSource, capture_source
from app.gateway.deps import get_shared_publications_repo, is_admin_user
from app.gateway.routers._file_http import acting_user_id
from app.gateway.routers.agent_instances import _actor, _require_ready, _service
from app.gateway.snapshot_manifest import MANIFEST_LIMIT, MANIFEST_SOURCE, SnapshotAgent, SnapshotManifest, SnapshotMember, SnapshotMessage, SnapshotModel, digest_of_bytes, member_record, parse_manifest
from app.gateway.storage_features import current_feature_service, storage_feature
from app.gateway.transcript import transcript_stem
from deerflow.agent_instances.contract import AgentDenied, AgentPermission, ExecutionUnavailable
from deerflow.config.paths import get_paths
from deerflow.features.paths import FeaturePaths
from deerflow.files import resolve_shared_file
from deerflow.files.folders import read_folder
from deerflow.files.store import SafeFileAccessUnavailable, StoreError, open_regular_source
from deerflow.persistence.shared_publications.model import SharedPublicationRow
from deerflow.spaces.contract import Permission
from deerflow.spaces.filesystem import UnsafeSpacePath
from deerflow.utils.file_io import run_file_io


class SnapshotSummary(SnapshotModel):
    publication_id: str
    title: str
    publisher: str | None
    published_at: str


class SnapshotFolder(SnapshotModel):
    publication_id: str
    folder: str
    entries: list[str]
    truncated: bool


class SharedMember(SnapshotMember):
    available: bool
    publication_id: str | None
    shared_path: str | None


class AssistantAnswerer(SnapshotModel):
    kind: Literal["assistant"] = "assistant"
    reason: Literal["no_access", "paused", "retired", "unavailable"] | None = Field(default=None, exclude_if=lambda v: v is None)


class AgentAnswerer(SnapshotModel):
    kind: Literal["agent"] = "agent"
    id: str
    name: str


Answerer = AssistantAnswerer | AgentAnswerer


async def snapshot_answerer(request: Request, agent: SnapshotAgent | None) -> Answerer:
    if agent is None:
        return AssistantAnswerer()
    auth = get_auth_context(request)
    if not auth or not auth.has_permission("agents", "read"):
        return AssistantAnswerer(reason="no_access")
    try:
        service = await _service(request)
    except HTTPException as exc:
        if exc.status_code in (403, 501):
            return AssistantAnswerer(reason="unavailable")
        raise
    try:
        actor = await _actor(request)
        view = await service.get(actor=actor, instance_id=agent.id, permission=AgentPermission.USE, include_deleted=True)
        if view.status != "active":
            reason = {"suspended": "paused", "archived": "retired"}.get(view.status, "unavailable")
            return AssistantAnswerer(reason=reason)
        if getattr(request.app.state, "agent_conversations", None) is None:
            return AssistantAnswerer(reason="unavailable")
        await _require_ready(request, service, view.id)
    except AgentDenied:
        return AssistantAnswerer(reason="no_access")
    except ExecutionUnavailable:
        return AssistantAnswerer(reason="unavailable")
    except HTTPException as exc:
        if exc.status_code == 403:
            return AssistantAnswerer(reason="no_access")
        raise
    return AgentAnswerer(id=view.id, name=view.name)


class SharedSnapshot(SnapshotSummary):
    with_agent: SnapshotAgent | None = Field(alias="with")
    messages: list[SnapshotMessage]
    transcript: SharedMember
    files: list[SharedMember]
    can_remove: bool
    answerer: Answerer = Field(default_factory=AssistantAnswerer)


def _not_found():
    return HTTPException(404, {"code": "snapshot-unavailable"})


def _open_shared(path):
    paths = get_paths()
    if isinstance(paths, FeaturePaths):
        with paths._volume("hm.shared").filesystem() as filesystem:
            return filesystem.open_regular("shared/" + path, follow_symlinks=False)
    return open_regular_source(resolve_shared_file(path))


def _signature(fd):
    st = os.fstat(fd)
    return st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns


# Only decoded manifests are cached. Each lookup reopens the current path
# under its reader's admission and verifies the inode/revision and live row.
_cache_lock = Lock()
_manifest_cache: OrderedDict[tuple, SnapshotManifest] = OrderedDict()


def _manifest_folder(row):
    if row.get("from_path") != MANIFEST_SOURCE or not row.get("from_thread_id"):
        return None
    parts = row["path"].split("/")
    if len(parts) != 3 or parts[0] != "Conversations" or parts[2] != "conversation.json":
        return None
    suffix = " · " + row["sha256"][:8]
    stem, separator, tail = parts[1].rpartition(suffix)
    if not separator or tail or len(stem) > 80 or stem != transcript_stem(stem):
        return None
    return "/".join(parts[:2])


def _manifest_document(row):
    if not _manifest_folder(row) or not 0 < row["size"] <= MANIFEST_LIMIT:
        return None
    try:
        with os.fdopen(_open_shared(row["path"]), "rb") as f:
            sig = _signature(f.fileno())
            if sig[2] != row["size"]:
                return None
            key = (row["publication_id"], row["sha256"], sig)
            with _cache_lock:
                cached = _manifest_cache.get(key)
            if cached is not None:
                return cached
            content = f.read(row["size"] + 1)
            if len(content) != row["size"] or _signature(f.fileno()) != sig or digest_of_bytes(content) != row["sha256"]:
                return None
    except (FileNotFoundError, NotADirectoryError, StoreError, UnsafeSpacePath):
        return None
    try:
        value = parse_manifest(content)
    except (ValueError, ValidationError, UnicodeError, RecursionError):
        return None
    with _cache_lock:
        if len(_manifest_cache) >= 8:
            _manifest_cache.popitem(last=False)
        _manifest_cache[key] = value
    return value


def _member_bytes_match(row):
    import hashlib

    try:
        with os.fdopen(_open_shared(row["path"]), "rb") as file:
            before = _signature(file.fileno())
            return before[2] == row["size"] and hashlib.file_digest(file, "sha256").hexdigest() == row["sha256"] and _signature(file.fileno()) == before
    except (FileNotFoundError, NotADirectoryError, StoreError, UnsafeSpacePath):
        return False


async def _reader(request, publication_id, *, allow_folder):
    from app.gateway.routers.shared import _publisher_names, _shared_mutation

    repo = get_shared_publications_repo(request)
    row = await repo.publication(publication_id)
    if not row or not (folder := _manifest_folder(row)):
        raise _not_found()
    names = await _publisher_names([row])
    publisher = names.get(row["published_by"])
    if row["removed_at"] is not None:
        raise HTTPException(410, {"code": "snapshot_removed", "publisher": publisher, "published_at": row["published_at"]})
    live = await repo.live_publication(row["path"])
    if not live or live["publication_id"] != publication_id:
        raise _not_found()
    doc = await run_file_io(_manifest_document, row)
    if not doc:
        if allow_folder:
            async with _shared_mutation(repo):
                try:
                    page = await run_file_io(read_folder, None, folder, 50)
                except (FileNotFoundError, SafeFileAccessUnavailable):
                    raise _not_found() from None
                current = await repo.live_publication(row["path"])
                if current and current["publication_id"] == publication_id:
                    names = [file.name for file in page.files] + [path.rsplit("/", 1)[-1] + "/" for path in page.folders]
                    return SnapshotFolder(publication_id=publication_id, folder=folder, entries=sorted(names), truncated=page.truncated)
        raise _not_found()
    prefix = row["path"].rsplit("/", 1)[0] + "/"
    records = await repo.live_publications(paths=[prefix + item.path for item in (doc.transcript, *doc.files)])
    members = []
    for entry in (doc.transcript, *doc.files):
        current = records.get(prefix + entry.path)
        available = member_record(row, entry, current) and await run_file_io(_member_bytes_match, current)
        members.append(SharedMember(**entry.model_dump(), available=available, publication_id=current["publication_id"] if available else None, shared_path=current["path"] if available else None))
    latest = await repo.live_publications(paths=[member.shared_path for member in members if member.available])
    for member in members:
        current = latest.get(member.shared_path)
        if member.available and (not current or current["publication_id"] != member.publication_id):
            member.available, member.publication_id, member.shared_path = False, None, None
    live = await repo.live_publication(row["path"])
    if not live or live["publication_id"] != publication_id:
        raise _not_found()
    auth = get_auth_context(request)
    may_remove = bool(auth and auth.has_permission("threads", "delete")) and (acting_user_id(request) == row["published_by"] or await is_admin_user(request))
    if may_remove and (service := current_feature_service(request, "hm.shared")):
        space = await service.resource()
        may_remove = space.status == "active" and bool(space.permissions & Permission.EXPORT)
    return SharedSnapshot(
        publication_id=publication_id,
        title=doc.title,
        published_at=row["published_at"],
        publisher=publisher,
        with_agent=doc.with_agent,
        messages=doc.messages,
        transcript=members[0],
        files=members[1:],
        can_remove=may_remove,
        answerer=await snapshot_answerer(request, doc.with_agent),
    )


@require_permission("threads", "read")
@storage_feature("hm.shared")
async def read_snapshot(request: Request, publication_id: str, *, allow_folder: bool = False):
    return await _reader(request, publication_id, allow_folder=allow_folder)


@require_permission("threads", "read")
@storage_feature("hm.shared")
async def read_collection(request: Request, limit: int, before: str | None = None):
    from app.gateway.routers.shared import _publisher_names

    repo = get_shared_publications_repo(request)
    cursor = None
    if before is not None:
        cursor = await repo.publication(before)
        if not cursor or cursor["from_path"] != MANIFEST_SOURCE:
            raise HTTPException(422, {"code": "invalid-snapshot-cursor"})
    rows = await repo.live_by_source(MANIFEST_SOURCE, limit=limit + 1, before=cursor)
    names = await _publisher_names(rows)
    items = []
    for row in rows[:limit]:
        if doc := await run_file_io(_manifest_document, row):
            live = await repo.live_publication(row["path"])
            if live and live["publication_id"] == row["publication_id"]:
                items.append(SnapshotSummary(publication_id=row["publication_id"], title=doc.title, published_at=row["published_at"], publisher=names.get(row["published_by"])))
    return {"conversations": items, "truncated": len(rows) > limit, "next_before": rows[limit - 1]["publication_id"] if len(rows) > limit else None}


async def snapshot_memberships(repo, records):
    groups = {}
    marked = {}
    for path, row in records.items():
        parts = path.split("/")
        if len(parts) >= 3 and parts[0] == "Conversations" and row:
            groups.setdefault("/".join(parts[:2]) + "/conversation.json", []).append(row)
    if not groups:
        return marked
    manifests = await repo.live_publications(paths=list(groups))
    for path, subset in groups.items():
        manifest = manifests.get(path)
        if not manifest or not (doc := await run_file_io(_manifest_document, manifest)):
            continue
        prefix = path.rsplit("/", 1)[0] + "/"
        candidates = {prefix + f.path: f for f in (doc.transcript, *doc.files)}
        for record in subset:
            path = record["path"]
            if record["publication_id"] == manifest["publication_id"] or (path in candidates and member_record(manifest, candidates[path], record)):
                marked[path] = manifest["publication_id"]
    return marked


@require_permission("threads", "read")
@storage_feature("hm.shared", export=True)
async def _require_member(request: Request, publication_id: str, member: SharedMember):
    repo = get_shared_publications_repo(request)
    row = await repo.publication(publication_id)
    if row and row["removed_at"] is None:
        current = await repo.live_publication(row["path"])
        if current and current["publication_id"] == publication_id and (doc := await run_file_io(_manifest_document, row)):
            item = next((f for f in doc.files if f.path == member.path and f.sha256 == member.sha256), None)
            if item:
                selected = await repo.live_publication(row["path"].rsplit("/", 1)[0] + "/" + item.path)
                if member_record(row, item, selected) and selected["publication_id"] == member.publication_id:
                    return row["path"]
    raise HTTPException(409, {"code": "snapshot-member-unavailable"})


async def capture_snapshot_member(request: Request, publication_id: str, member: SharedMember, maximum_bytes: int):
    if not member.available or not member.publication_id:
        raise _not_found()
    source = SharedPublicationSource(publication_id=member.publication_id, sha256=member.sha256)
    manifest_path = await _require_member(request=request, publication_id=publication_id, member=member)
    captured = await capture_source(request, source, maximum_bytes=maximum_bytes)

    async def check():
        await captured.recheck()
        await _require_member(request=request, publication_id=publication_id, member=member)

    await check()

    async def admit_home_publication(session):
        # A withdrawal can retire the manifest while its member is still live.
        # Use the same transaction that authorizes the pending Home write.
        latest = await session.scalar(
            select(SharedPublicationRow.publication_id).where(SharedPublicationRow.path == manifest_path, SharedPublicationRow.removed_at.is_(None)).order_by(SharedPublicationRow.published_at.desc()).limit(1).with_for_update(read=True)
        )
        if latest != publication_id:
            raise HTTPException(409, {"code": "snapshot-member-unavailable"})
        await captured.admit_home_publication(session)

    return replace(captured, recheck=check, admit_home_publication=admit_home_publication)
