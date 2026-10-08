"""Generic resource files; all authority originates from current host identity."""

from dataclasses import asdict
from pathlib import Path
from typing import Literal
from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field

from app.gateway.auth_disabled import AUTH_SOURCE_INTERNAL, AUTH_SOURCE_PAT
from app.gateway.deps import get_current_user_from_request
from app.gateway.internal_auth import get_trusted_internal_owner_user_id
from app.gateway.routers._file_http import DescriptorFileResponse
from deerflow.spaces.contract import Custody, InvalidPrincipal, MutationMode, Permission, PrincipalRef, SpaceConflict, SpaceDenied, SpaceNotFound
from deerflow.spaces.filesystem import FilesystemUnavailable, _path
from deerflow.spaces.recovery import SpaceRecovery
from deerflow.spaces.service import MAX_TRANSFER_BYTES, SpaceFiles
from deerflow.utils.file_io import await_drained, run_file_io


def _http_error(exc):
    if isinstance(exc, SpaceNotFound | FileNotFoundError):
        return HTTPException(404, "Resource or file is unavailable")
    if isinstance(exc, InvalidPrincipal):
        return HTTPException(401, "A current host identity is required")
    if isinstance(exc, SpaceDenied):
        return HTTPException(403, str(exc))
    if isinstance(exc, SpaceConflict):
        return HTTPException(409, str(exc))
    if isinstance(exc, FilesystemUnavailable):
        return HTTPException(501, str(exc))
    if isinstance(exc, ValueError):
        return HTTPException(400, str(exc))
    return HTTPException(503, "Resource filesystem is unavailable; retain data and retry recovery")


_ERRORS = (SpaceDenied, SpaceConflict, FilesystemUnavailable, ValueError, OSError)


class StorageRoute(APIRoute):
    def get_route_handler(self):
        handler = super().get_route_handler()

        async def admitted(request):
            try:
                return await handler(request)
            except _ERRORS as exc:
                raise _http_error(exc) from None

        return admitted


router = APIRouter(prefix="/api/spaces", tags=["storage-spaces"], route_class=StorageRoute)


def _service(request: Request) -> SpaceFiles:
    service = getattr(request.app.state, "storage_spaces", None)
    if not isinstance(service, SpaceFiles):
        raise HTTPException(501, "This deployment has no qualified Storage Spaces backend")
    return service


async def _actor(request: Request) -> PrincipalRef:
    source = getattr(request.state, "auth_source", None)
    if source == AUTH_SOURCE_PAT:
        raise HTTPException(403, "Personal access tokens do not yet carry storage resource scopes")
    if source == AUTH_SOURCE_INTERNAL:
        owner = get_trusted_internal_owner_user_id(request)
        if not owner:
            raise HTTPException(401, "Storage requires an attributed internal owner")
        return PrincipalRef("human", owner)
    user = await get_current_user_from_request(request)
    if getattr(user, "id", None) is None:
        raise HTTPException(401, "Storage requires authenticated host context")
    return PrincipalRef("human", str(user.id))


class StrictRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class CreateSpace(StrictRequest):
    name: str = Field(min_length=1, max_length=128)
    custody: Literal["personal", "company"] = "personal"


class RenameSpace(StrictRequest):
    name: str = Field(min_length=1, max_length=128)
    generation: int = Field(ge=1)


class GrantRequest(StrictRequest):
    subject_kind: Literal["human", "nonhuman"]
    subject_id: str = Field(min_length=1, max_length=128)
    permissions: int = Field(ge=0, le=31)
    generation: int = Field(ge=1)
    acknowledge_existing_data: bool = False


class FileMutation(StrictRequest):
    operation_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    generation: int = Field(ge=1)
    action: Literal["mkdir", "rename", "remove"]
    path: str = Field(min_length=1, max_length=4096)
    destination: str | None = Field(default=None, max_length=4096)


class CopyRequest(StrictRequest):
    operation_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    source_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    source_generation: int = Field(ge=1)
    destination_generation: int = Field(ge=1)
    source_path: str = Field(min_length=1, max_length=4096)
    destination_path: str = Field(min_length=1, max_length=4096)
    acknowledge_disclosure: bool = False


class LifecycleRequest(StrictRequest):
    operation_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    generation: int = Field(ge=1)
    action: Literal["backup", "restore", "archive", "delete"]
    backup_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")


class RecoveryRequest(StrictRequest):
    operation_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    generation: int = Field(ge=1)
    acknowledge_uncertain_outcome: bool


class RetireAttachmentsRequest(StrictRequest):
    generation: int = Field(ge=1, le=2**31 - 1)
    attachment_ids: list[str] = Field(min_length=1, max_length=32)


@router.get("")
async def list_spaces(request: Request, limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
    actor = await _actor(request)
    return {"spaces": [jsonable_encoder(asdict(space)) for space in await _service(request).registry.list(actor=actor, limit=limit, offset=offset)]}


@router.post("", status_code=201)
async def create_space(body: CreateSpace, request: Request):
    actor = await _actor(request)
    space = await _service(request).create(actor=actor, name=body.name, custody=Custody.personal(actor) if body.custody == "personal" else Custody.company(), mode=MutationMode.NATIVE)
    return jsonable_encoder(asdict(space))


@router.get("/{space_id}")
async def get_space(space_id: str, request: Request):
    actor = await _actor(request)
    service = _service(request)
    space = await service.registry.get(actor=actor, space_id=space_id)
    try:
        quota = await service.quota(actor=actor, space_id=space_id)
    except SpaceConflict:
        return {**jsonable_encoder(asdict(space)), "storage_state": "recovery-pending"}
    return {**jsonable_encoder(asdict(space)), "quota": quota, "storage_state": "available"}


@router.get("/{space_id}/recovery")
async def recovery_status(space_id: str, request: Request):
    return await SpaceRecovery(_service(request)).status(actor=await _actor(request), space_id=space_id)


@router.post("/{space_id}/recovery")
async def accept_observed_state(space_id: str, body: RecoveryRequest, request: Request):
    await SpaceRecovery(_service(request)).accept_current_state(
        actor=await _actor(request), space_id=space_id, expected_generation=body.generation, operation_id=body.operation_id, acknowledge_uncertain_outcome=body.acknowledge_uncertain_outcome
    )
    return {"complete": True, "resolution": "owner-accepted-current-state"}


@router.post("/{space_id}/lifecycle")
async def resource_lifecycle(space_id: str, body: LifecycleRequest, request: Request):
    if (body.action == "restore") != (body.backup_id is not None):
        raise ValueError("Only restore requires a resource backup identity")
    recovery = SpaceRecovery(_service(request))
    arguments = {"actor": await _actor(request), "space_id": space_id, "expected_generation": body.generation, "operation_id": body.operation_id}
    if body.action == "restore":
        arguments["backup_id"] = body.backup_id
    result = await getattr(recovery, body.action)(**arguments)
    return jsonable_encoder(asdict(result))


@router.post("/{space_id}/attachments/retire")
async def retire_attachments(space_id: str, body: RetireAttachmentsRequest, request: Request):
    provider = getattr(_service(request), "attachments", None)
    if provider is None:
        raise HTTPException(501, "The attachment provider must reconnect before containment can be confirmed")
    await provider.retire(actor=await _actor(request), space_id=space_id, expected_generation=body.generation, attachment_ids=body.attachment_ids)
    return {"complete": True}


@router.patch("/{space_id}")
async def rename_space(space_id: str, body: RenameSpace, request: Request):
    space = await _service(request).registry.rename(actor=await _actor(request), space_id=space_id, name=body.name, expected_generation=body.generation)
    return jsonable_encoder(asdict(space))


@router.put("/{space_id}/grants")
async def set_grant(space_id: str, body: GrantRequest, request: Request):
    space = await _service(request).registry.set_grant(
        actor=await _actor(request),
        space_id=space_id,
        subject=PrincipalRef(body.subject_kind, body.subject_id),
        permissions=Permission(body.permissions),
        expected_generation=body.generation,
        acknowledge_existing_data=body.acknowledge_existing_data,
    )
    return jsonable_encoder(asdict(space))


@router.get("/{space_id}/files")
async def list_directory(space_id: str, request: Request, path: str = "", limit: int = Query(200, ge=1, le=500)):
    entries, truncated = await _service(request).list_directory(actor=await _actor(request), space_id=space_id, path=path, limit=limit)
    return {"files": [{**asdict(entry), "url": f"/api/spaces/{space_id}/content?" + urlencode({"path": entry.path})} for entry in entries], "truncated": truncated}


@router.get("/{space_id}/text")
async def read_text(space_id: str, request: Request, path: str):
    import hashlib

    content = await _service(request).read(actor=await _actor(request), space_id=space_id, path=path, max_bytes=1024 * 1024)
    if b"\0" in content:
        raise HTTPException(415, "Binary files can be downloaded, not edited as text")
    try:
        text = content.decode("utf-8")
    except UnicodeError:
        raise HTTPException(415, "The text editor supports UTF-8 files") from None
    return {"text": text, "sha256": hashlib.sha256(content).hexdigest(), "concurrency": "exclusive-host-window"}


class SpaceFileResponse(DescriptorFileResponse):
    """Ranges and MIME reuse the opened resource inode under live admission."""

    def __init__(self, service, actor, space_id, path, download):
        _path(path)
        super().__init__(Path(path), download=download)
        self.headers["cache-control"] = "private, no-store"
        self._service = service
        self._actor = actor
        self._space_id = space_id
        self._filesystem = None

    def _open_descriptor(self):
        return self._filesystem.open_regular(str(self.path))

    async def _prepare_owned(self):
        permission = Permission.READ | (Permission.EXPORT if self._download else Permission(0))
        async with self._service.registry.admitted(actor=self._actor, requests={self._space_id: (permission, None)}) as (session, rows):
            volume = await self._service._volume(session, rows[self._space_id][0])

            def acquire_and_prepare():
                self._filesystem = volume.filesystem()
                self._prepare()

            await await_drained(run_file_io(acquire_and_prepare))
        # Already-admitted reads own that inode. Atomic host replacement cannot
        # alter it; SQL authority locks must not follow a slow client stream.
        # This is not a coherent snapshot of a concurrently native-written DB.

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        except _ERRORS as exc:
            raise _http_error(exc) from None
        finally:
            if self._filesystem is not None:
                await await_drained(run_file_io(self._filesystem.close))
                self._filesystem = None


@router.get("/{space_id}/content")
async def get_content(space_id: str, request: Request, path: str, download: bool = False):
    return SpaceFileResponse(_service(request), await _actor(request), space_id, path, download)


@router.put("/{space_id}/content")
async def write_content(space_id: str, request: Request, path: str, generation: int = Query(ge=1), operation_id: str = Query(pattern=r"^[0-9a-f]{32}$"), expected_sha256: str | None = None, create: bool = False):
    service, actor = _service(request), await _actor(request)
    await service.registry.get(actor=actor, space_id=space_id, permission=Permission.WRITE, expected_generation=generation)
    content = bytearray()
    async for chunk in request.stream():
        if len(content) + len(chunk) > MAX_TRANSFER_BYTES:
            raise HTTPException(413, "File import/edit is limited to64MiB per request")
        content.extend(chunk)
    result = await service.write(actor=actor, space_id=space_id, expected_generation=generation, operation_id=operation_id, path=path, content=bytes(content), expected_sha256=expected_sha256, create=create)
    return {"sha256": result}


@router.post("/{space_id}/files")
async def mutate_file(space_id: str, body: FileMutation, request: Request):
    service, actor = _service(request), await _actor(request)
    kwargs = dict(actor=actor, space_id=space_id, expected_generation=body.generation, operation_id=body.operation_id, path=body.path)
    if body.action == "rename":
        if body.destination is None:
            raise HTTPException(400, "Rename requires a destination")
        await service.rename(**kwargs, destination=body.destination)
    elif body.destination is not None:
        raise HTTPException(400, "Only rename accepts a destination")
    elif body.action == "mkdir":
        await service.mkdir(**kwargs)
    else:
        await service.remove(**kwargs)
    return {"complete": True}


@router.post("/{space_id}/import")
async def copy_file(space_id: str, body: CopyRequest, request: Request):
    result = await _service(request).copy(
        actor=await _actor(request),
        source_id=body.source_id,
        destination_id=space_id,
        source_generation=body.source_generation,
        destination_generation=body.destination_generation,
        source_path=body.source_path,
        destination_path=body.destination_path,
        operation_id=body.operation_id,
        acknowledge_disclosure=body.acknowledge_disclosure,
    )
    return {"sha256": result}
