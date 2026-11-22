"""Bounded browser selection and directory reads under each source's own authority."""

import os
import stat
from dataclasses import asdict
from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field

from app.gateway.authz import get_auth_context, require_permission
from app.gateway.deps import get_shared_publications_repo, is_admin_user
from app.gateway.routers._file_http import acting_user_id
from app.gateway.routers.agent_instances import _service as instance_service
from app.gateway.routers.files import UserFileInfo
from app.gateway.routers.shared import SharedFileInfo, _may_remove, _publisher_names, _shared_mutation
from app.gateway.routers.spaces import MAX_TEXT_BYTES, SpaceFileResponse, StorageRoute, StrictRequest, _actor, text_result
from app.gateway.storage_features import current_feature_service, storage_feature
from deerflow.agent_instances.contract import AgentDenied
from deerflow.config.paths import get_paths
from deerflow.features.paths import FeaturePaths
from deerflow.files import resolve_shared_file, resolve_user_file
from deerflow.files.folders import read_folder
from deerflow.files.manager import UserFile
from deerflow.files.shared import SharedFile
from deerflow.files.shared_attribution import observed_descriptor, visible_records
from deerflow.files.store import SafeFileAccessUnavailable, normalize_relative_path, open_regular_source, selected_revision
from deerflow.spaces.contract import Permission, SpaceDenied
from deerflow.spaces.filesystem import _path
from deerflow.utils.file_io import run_file_io

router = APIRouter(prefix="/api/file-browser/v1", tags=["file-browser"], route_class=StorageRoute)
_HIDDEN_AGENT_NAMES = frozenset({".agent-skill-packages"})


class SelectedEntry(BaseModel):
    path: str
    name: str
    kind: Literal["file", "directory"]
    size: int
    modified: float
    revision: str


def _selection(path, metadata, *, directories=False):
    if stat.S_ISREG(metadata.st_mode):
        kind = "file"
    elif directories and stat.S_ISDIR(metadata.st_mode):
        kind = "directory"
    else:
        raise HTTPException(404, "File or folder is unavailable")
    # A selection describes the entry already observed. It conveys no permission
    # to operate on another file at its old pathname.
    revision = selected_revision(path, metadata)
    return SelectedEntry(path=path, name=path.rsplit("/", 1)[-1], kind=kind, size=metadata.st_size, modified=metadata.st_mtime, revision=revision)


def _collection_entry(path, *, personal, owner):
    relative = normalize_relative_path(path)
    if relative != path:
        raise HTTPException(400, "Use a relative file path")
    paths = get_paths()
    if isinstance(paths, FeaturePaths):
        volume = paths._volume("hm.my-files", owner) if personal else paths._volume("hm.shared")
        with volume.filesystem() as filesystem:
            metadata = filesystem.entry_stat(("files/" if personal else "shared/") + path)
    else:
        source = resolve_user_file(owner, path) if personal else resolve_shared_file(path)
        try:
            fd = open_regular_source(source)
        except SafeFileAccessUnavailable:
            raise HTTPException(501, "File selection requires a host with descriptor-relative reads") from None
        try:
            metadata = os.fstat(fd)
        finally:
            os.close(fd)
    return _selection(path, metadata)


async def _read_collection_folder(owner, path, limit):
    try:
        return await run_file_io(read_folder, owner, path, limit)
    except SafeFileAccessUnavailable:
        raise HTTPException(501, "File browsing requires descriptor-relative reads") from None
    except FileNotFoundError:
        raise HTTPException(404, {"code": "folder-unavailable"}) from None


def _folder_result(response, page, file_rows):
    response.headers["cache-control"] = "private, no-store"
    folders = [{"path": path, "name": path.rsplit("/", 1)[-1], "kind": "directory", "size": 0, "modified": 0} for path in sorted(page.folders)]
    return {"files": [*folders, *sorted(file_rows, key=lambda row: row["path"])], "truncated": page.truncated}


@router.get("/personal/folder")
@require_permission("threads", "read")
@storage_feature("hm.my-files")
async def personal_folder(request: Request, response: Response, path: str = Query("", max_length=4096), limit: int = Query(500, ge=1, le=500)):
    page = await _read_collection_folder(acting_user_id(request), path, limit)
    files = [{"kind": "file", **UserFileInfo.of(UserFile.of(entry)).model_dump()} for entry in page.files]
    return _folder_result(response, page, files)


@router.get("/shared/folder")
@require_permission("threads", "read")
@storage_feature("hm.shared")
async def shared_folder(request: Request, response: Response, path: str = Query("", max_length=4096), limit: int = Query(500, ge=1, le=500)):
    repo = get_shared_publications_repo(request)
    async with _shared_mutation(repo) as state:
        page = await _read_collection_folder(None, path, limit)
        raw = await repo.live_publications(paths=[entry.path for entry in page.files])
        records = await run_file_io(visible_records, state, page.files, raw)
    names = await _publisher_names(list(records.values()))
    service = current_feature_service(request, "hm.shared")
    auth = get_auth_context(request)
    owner = acting_user_id(request)
    admin = await is_admin_user(request)
    may_remove = False
    if service and auth and auth.has_permission("threads", "delete"):
        space = await service.resource()
        may_remove = space.status == "active" and bool(space.permissions & Permission.EXPORT)
    files = []
    for entry in page.files:
        record = records.get(entry.path)
        observed = entry.path not in raw or record is not None
        allowed = may_remove and observed and _may_remove(record, user_id=owner, admin=admin)
        row = SharedFileInfo.of(SharedFile.of(entry), record, can_remove=allowed, publisher=names.get(record["published_by"]) if record else None).model_dump()
        if not observed or not service:
            row.pop("publication_id")
        files.append({"kind": "file", **row})
    from app.gateway.conversation_snapshots import snapshot_memberships

    marks = await snapshot_memberships(repo, records)
    for item in files:
        if item["path"] in marks:
            item["snapshot_publication_id"] = marks[item["path"]]
    return _folder_result(response, page, files)


@router.get("/personal/selection", response_model=SelectedEntry)
@require_permission("threads", "read")
@storage_feature("hm.my-files")
async def select_personal(request: Request, response: Response, path: str = Query(min_length=1, max_length=4096)):
    response.headers["cache-control"] = "private, no-store"
    return await run_file_io(_collection_entry, path, personal=True, owner=acting_user_id(request))


class SharedSelection(SelectedEntry):
    publication_id: str | None = None
    can_remove: bool = False


@router.get("/shared/selection", response_model=SharedSelection, response_model_exclude_unset=True)
@require_permission("threads", "read")
@storage_feature("hm.shared")
async def select_shared(request: Request, response: Response, path: str = Query(min_length=1, max_length=4096)):
    response.headers["cache-control"] = "private, no-store"
    service = current_feature_service(request, "hm.shared")
    if not service:
        selected = await run_file_io(_collection_entry, path, personal=False, owner=None)
        return SharedSelection(**selected.model_dump(), can_remove=False)
    if normalize_relative_path(path) != path:
        raise HTTPException(400, "Use a relative file path")

    raw = await get_shared_publications_repo(request).live_publication(path)

    def observe():
        volume = get_paths()._volume("hm.shared")
        with volume.filesystem() as filesystem:
            fd = filesystem.open_regular("shared/" + path, follow_symlinks=False)
            try:
                metadata = os.fstat(fd)
                if raw is None or raw["size"] != metadata.st_size:
                    return _selection(path, metadata), None
                proof = observed_descriptor(fd, use_cache=True)
                return _selection(path, proof.metadata), raw if proof.matches(raw) else None
            finally:
                os.close(fd)

    selected, record = await run_file_io(observe)
    space = await service.resource()
    auth = get_auth_context(request)
    can_remove = bool(
        (raw is None or record is not None)
        and space.status == "active"
        and space.permissions & Permission.EXPORT
        and auth
        and auth.has_permission("threads", "delete")
        and _may_remove(record, user_id=acting_user_id(request), admin=await is_admin_user(request))
    )
    result = SharedSelection(**selected.model_dump(), can_remove=can_remove)
    # Only an actual absence is encoded as null. A mismatched record provides no identity.
    if raw is None or record is not None:
        result.publication_id = record["publication_id"] if record else None
    return result


class RemoveSelectedShared(StrictRequest):
    path: str = Field(min_length=1, max_length=4096)
    expected_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_publication_id: str | None = Field(min_length=1, max_length=64)
    operation_id: str = Field(pattern=r"^[0-9a-f]{32}$")


@router.post("/shared/remove", status_code=204, response_class=Response)
@require_permission("threads", "delete")
@storage_feature("hm.shared", write=True, selected_remove=True)
async def remove_selected_shared(request: Request, body: RemoveSelectedShared):
    if normalize_relative_path(body.path) != body.path:
        raise HTTPException(400, "Use a relative file path")
    service = current_feature_service(request, "hm.shared")
    await service.execute(
        "remove",
        repo=get_shared_publications_repo(request),
        path=body.path,
        expected_publication_id=body.expected_publication_id or "",
        expected_revision=body.expected_revision,
        admin=await is_admin_user(request),
        stage_admission=request.state.selected_shared_admission,
    )
    return Response(status_code=204)


def _visible_agent_path(path, *, root=False):
    _path(path, allow_root=root)
    if not _HIDDEN_AGENT_NAMES.isdisjoint(path.split("/")):
        raise HTTPException(404, "File or folder is unavailable")


async def _agent_home(request, instance_id):
    agents = await instance_service(request)
    actor = await _actor(request)
    try:
        instance = await agents.get(actor=actor, instance_id=instance_id)
    except AgentDenied:
        raise HTTPException(404, "Agent Home is unavailable") from None
    if not instance.home_id or instance.status not in {"active", "suspended", "archived"}:
        raise HTTPException(404, "Agent Home is unavailable")
    home = await agents.files.registry.get(actor=actor, space_id=instance.home_id, permission=Permission.READ)
    inspect = agents.space_audience_admission(home.id, actor, expected_instance=instance)

    async def admit(session, rows):
        if rows[home.id][0].generation != home.generation:
            raise SpaceDenied("This agent Home changed; refresh the Files tab")
        await inspect(session)

    return agents.files, actor, home.id, admit


@router.get("/agents/{instance_id}/folder")
@require_permission("agents", "read")
async def agent_folder(instance_id: str, request: Request, response: Response, path: str = "", limit: int = Query(200, ge=1, le=500)):
    files, actor, home, admit = await _agent_home(request, instance_id)
    _visible_agent_path(path, root=True)
    entries, truncated = await files.list_directory(
        actor=actor,
        space_id=home,
        path=path,
        limit=limit,
        controller_admission=admit,
        follow_symlinks=False,
        excluded_names=_HIDDEN_AGENT_NAMES,
    )
    response.headers["cache-control"] = "private, no-store"
    return {"files": [asdict(entry) for entry in entries], "truncated": truncated}


@router.get("/agents/{instance_id}/selection", response_model=SelectedEntry)
@require_permission("agents", "read")
async def select_agent_entry(instance_id: str, path: str, request: Request, response: Response):
    files, actor, home, admit = await _agent_home(request, instance_id)
    _visible_agent_path(path)
    metadata = await files.entry_stat(actor=actor, space_id=home, path=path, controller_admission=admit)
    response.headers["cache-control"] = "private, no-store"
    return _selection(path, metadata, directories=True)


@router.get("/agents/{instance_id}/text")
@require_permission("agents", "read")
async def agent_text(instance_id: str, path: str, request: Request, response: Response):
    files, actor, home, admit = await _agent_home(request, instance_id)
    _visible_agent_path(path)
    content = await files.read_regular(actor=actor, space_id=home, path=path, max_bytes=MAX_TEXT_BYTES, controller_admission=admit)
    response.headers["cache-control"] = "private, no-store"
    return text_result(content)


class AgentBrowserFileResponse(SpaceFileResponse):
    def __init__(self, files, actor, home, path, download, admit):
        super().__init__(files, actor, home, path, download)
        self._admit_source = admit

    def _open_descriptor(self):
        return self._filesystem.open_regular(str(self.path), follow_symlinks=False)


@router.get("/agents/{instance_id}/content")
@require_permission("agents", "read")
async def agent_content(instance_id: str, path: str, request: Request, download: bool = False):
    files, actor, home, admit = await _agent_home(request, instance_id)
    _visible_agent_path(path)
    return AgentBrowserFileResponse(files, actor, home, path, download, admit)
