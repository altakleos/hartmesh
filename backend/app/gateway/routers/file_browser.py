"""Bounded browser selection and directory reads under each source's own authority."""

import hashlib
import json
import os
import stat
from dataclasses import asdict
from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel

from app.gateway.authz import require_permission
from app.gateway.routers._file_http import acting_user_id
from app.gateway.routers.agent_instances import _service as instance_service
from app.gateway.routers.spaces import MAX_TEXT_BYTES, SpaceFileResponse, StorageRoute, _actor, text_result
from app.gateway.storage_features import storage_feature
from deerflow.agent_instances.contract import AgentDenied
from deerflow.config.paths import get_paths
from deerflow.features.paths import FeaturePaths
from deerflow.files import resolve_shared_file, resolve_user_file
from deerflow.files.store import SafeFileAccessUnavailable, normalize_relative_path, open_regular_source
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
    identity = [path, metadata.st_dev, metadata.st_ino, metadata.st_mode, metadata.st_size, metadata.st_mtime_ns, metadata.st_ctime_ns]
    revision = hashlib.sha256(json.dumps(identity, separators=(",", ":")).encode()).hexdigest()
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


@router.get("/personal/selection", response_model=SelectedEntry)
@require_permission("threads", "read")
@storage_feature("hm.my-files")
async def select_personal(request: Request, response: Response, path: str = Query(min_length=1, max_length=4096)):
    response.headers["cache-control"] = "private, no-store"
    return await run_file_io(_collection_entry, path, personal=True, owner=acting_user_id(request))


@router.get("/shared/selection", response_model=SelectedEntry)
@require_permission("threads", "read")
@storage_feature("hm.shared")
async def select_shared(request: Request, response: Response, path: str = Query(min_length=1, max_length=4096)):
    response.headers["cache-control"] = "private, no-store"
    return await run_file_io(_collection_entry, path, personal=False, owner=None)


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
