"""Direct, attributed host writes for composer uploads and agent Home files."""

import tempfile
import unicodedata
from contextlib import asynccontextmanager
from dataclasses import asdict
from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.routing import APIRoute
from pydantic import Field, model_validator
from starlette.datastructures import UploadFile
from starlette.formparsers import MultiPartException, MultiPartParser

from app.gateway.agent_conversations import execution_for_run
from app.gateway.authz import require_permission
from app.gateway.routers.agent_instances import _service
from app.gateway.routers.spaces import StrictRequest, _actor
from app.gateway.storage_spaces import require_execution
from deerflow.agent_instances.agent_files import AgentFileRefusal
from deerflow.agent_instances.contract import AgentConflict, AgentDenied, InstanceIdentity
from deerflow.persistence.agent_instances.model import AgentInstanceRow
from deerflow.spaces.contract import InvalidPrincipal, Permission, SpaceConflict, SpaceDenied, SpaceNotFound
from deerflow.spaces.filesystem import FilesystemUnavailable
from deerflow.spaces.service import MAX_TRANSFER_BYTES
from deerflow.utils.file_io import await_drained, run_file_io
from deerflow.utils.thread_id import ThreadId


class AgentHomeRoute(APIRoute):
    def get_route_handler(self):
        handler = super().get_route_handler()

        async def admitted(request):
            try:
                return await handler(request)
            except AgentDenied:
                raise HTTPException(404, "Agent instance is unavailable") from None
            except SpaceNotFound:
                raise HTTPException(404, "Resource or file is unavailable") from None
            except InvalidPrincipal:
                raise HTTPException(401, "A current host identity is required") from None
            except SpaceDenied:
                raise HTTPException(403, "You do not have permission to change these files.") from None
            except (AgentConflict, SpaceConflict):
                raise HTTPException(409, "This file request conflicts with the current state.") from None
            except FilesystemUnavailable:
                raise HTTPException(501, "File access is unavailable on this host.") from None
            except ValueError:
                raise HTTPException(400, "The file request is invalid.") from None
            except OSError:
                raise AgentFileRefusal("file-write-failed") from None

        return admitted


router = APIRouter(prefix="/api/agent-instances", tags=["agent-instances"], route_class=AgentHomeRoute)


def _filename(value):
    if not isinstance(value, str):
        raise AgentFileRefusal("invalid-name")
    name = value.replace("\\", "/").rsplit("/", 1)[-1]
    try:
        length = len(name.encode("utf-8", errors="strict"))
    except UnicodeError:
        raise AgentFileRefusal("invalid-name") from None
    if not 1 <= length <= 255 or name in (".", "..") or any(unicodedata.category(char) == "Cc" for char in name):
        raise AgentFileRefusal("invalid-name")
    return name


class _DrainedUploadFile(UploadFile):
    async def write(self, data):
        if self.size is not None:
            self.size += len(data)
        await await_drained(run_file_io(self.file.write, data))

    async def seek(self, offset):
        await await_drained(run_file_io(self.file.seek, offset))


class _BoundedMultipart(MultiPartParser):
    """Limit each streamed file before spooling; the route owns all closure.

    Starlette's max_part_size limits text fields only. Its raw spool list is
    transferred here so error and cancellation cleanup can drain off-loop too.
    """

    spool_max_size = 1 << 20

    def __init__(self, request):
        super().__init__(request.headers, request.stream(), max_files=1, max_fields=0)
        self.owned_files, self.file_bytes, self.header_bytes = [], 0, 0

    def _header_limit(self, start, end):
        self.header_bytes += end - start
        if self.header_bytes > 16 * 1024:
            raise AgentFileRefusal("invalid-name")

    def on_header_field(self, data, start, end):
        self._header_limit(start, end)
        super().on_header_field(data, start, end)

    def on_header_value(self, data, start, end):
        self._header_limit(start, end)
        super().on_header_value(data, start, end)

    def on_headers_finished(self):
        super().on_headers_finished()
        upload = self._current_part.file
        if upload is not None:
            self.owned_files.append(upload.file)
            self._files_to_close_on_error.remove(upload.file)
            self._current_part.file = _DrainedUploadFile(file=upload.file, size=upload.size, filename=upload.filename, headers=upload.headers)
            _filename(upload.filename)
        if self._current_part.field_name != "file":
            raise AgentFileRefusal("invalid-name")

    def on_part_data(self, data, start, end):
        if self._current_part.file is not None:
            self.file_bytes += end - start
            if self.file_bytes > MAX_TRANSFER_BYTES:
                raise AgentFileRefusal("file-too-large")
        super().on_part_data(data, start, end)


def _close(files):
    for file in files:
        file.close()


@asynccontextmanager
async def _body(request, *, multipart=False):
    parser, spool = None, None
    try:
        if multipart:
            parser = _BoundedMultipart(request)
            try:
                form = await parser.parse()
            except (MultiPartException, ValueError, KeyError):
                raise AgentFileRefusal("invalid-name") from None
            upload = form.get("file")
            if len(form.multi_items()) != 1 or upload is None or not hasattr(upload, "file"):
                raise AgentFileRefusal("invalid-name")
            yield upload.file, parser.file_bytes, _filename(upload.filename)
        else:
            spool = tempfile.SpooledTemporaryFile(max_size=1 << 20, mode="w+b")
            size = 0
            async for chunk in request.stream():
                size += len(chunk)
                if size > MAX_TRANSFER_BYTES:
                    raise AgentFileRefusal("file-too-large")
                await await_drained(run_file_io(spool.write, chunk))
            yield spool, size, None
    except PermissionError:
        raise
    except OSError:
        raise AgentFileRefusal("file-write-failed") from None
    finally:
        owned = parser.owned_files if parser is not None else [spool] if spool is not None else []
        await await_drained(run_file_io(_close, owned))


async def _conversation(request, instance_id, thread_id):
    execution = await execution_for_run(request, thread_id)
    if execution is None or execution.instance.id != instance_id:
        raise HTTPException(404, "Agent conversation is unavailable")
    return execution


async def _home(request, service, instance_id):
    actor = await _actor(request)
    async with service._sf() as session:
        row = await session.get(AgentInstanceRow, instance_id)
        instance = InstanceIdentity.from_row(row) if row is not None else None
    if instance is None or instance.home_id is None:
        raise HTTPException(404, "Resource or file is unavailable")
    home = await service.files.registry.get(actor=actor, space_id=instance.home_id, permission=Permission.WRITE)
    return instance, actor, home.generation


def _human_admission(service, instance, actor, generation, operation_id):
    async def admit(session, _rows):
        await service.files.registry.admit_in_session(session, actor=actor, requests={instance.home_id: (Permission.WRITE, generation)}, operation_id=(instance.home_id, operation_id))

    return admit


def _conversation_admission(execution, operation_id):
    async def admit(session, _rows):
        await execution.authority.instances.files.registry.admit_in_session(
            session, actor=execution.instance.principal, requests={execution.instance.home_id: (Permission.READ, execution.home_generation)}, operation_id=(execution.instance.home_id, operation_id)
        )
        await execution.authority.context_admission_in_session(session, execution)

    return admit


@router.post("/{instance_id}/conversations/{thread_id}/files", status_code=201)
@require_permission("threads", "write")
@require_permission("agents", "read")
async def conversation_file(instance_id: str, thread_id: ThreadId, request: Request, operation_id: str = Query(pattern=r"^[0-9a-f]{32}$")):
    await require_execution(request.app, instance_id)
    service = await _service(request)
    await _conversation(request, instance_id, thread_id)
    async with _body(request, multipart=True) as (content, size, name):
        execution = await _conversation(request, instance_id, thread_id)
        result = await service.agent_files.apply(
            instance=execution.instance, requester=execution.requester, operation_id=operation_id, action="put", path="uploads/" + name, content=content, size=size, admission=_conversation_admission(execution, operation_id)
        )
        return {**asdict(result), "sandbox_path": "/mnt/user-data/" + result.path}


@router.put("/{instance_id}/home/content")
@require_permission("agents", "write")
@require_permission("agents", "read")
async def home_content(instance_id: str, request: Request, path: str, operation_id: str = Query(pattern=r"^[0-9a-f]{32}$"), expected_sha256: str | None = Query(default=None, pattern=r"^[0-9a-f]{64}$")):
    await require_execution(request.app, instance_id)
    service = await _service(request)
    await _home(request, service, instance_id)
    async with _body(request) as (content, size, _name):
        instance, actor, generation = await _home(request, service, instance_id)
        return asdict(
            await service.agent_files.apply(
                instance=instance,
                requester=actor,
                operation_id=operation_id,
                action="put",
                path=path,
                content=content,
                size=size,
                expected_sha256=expected_sha256,
                admission=_human_admission(service, instance, actor, generation, operation_id),
            )
        )


class FileAction(StrictRequest):
    operation_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    action: Literal["mkdir", "rename", "remove"]
    path: str
    destination: str | None = None

    @model_validator(mode="after")
    def destination_for_rename(self):
        if (self.action == "rename" and self.destination is None) or (self.action != "rename" and "destination" in self.model_fields_set):
            raise ValueError("Only a rename requires a destination")
        return self


@router.post("/{instance_id}/home/files")
@require_permission("agents", "write")
@require_permission("agents", "read")
async def home_files(instance_id: str, body: FileAction, request: Request):
    await require_execution(request.app, instance_id)
    service = await _service(request)
    instance, actor, generation = await _home(request, service, instance_id)
    result = await service.agent_files.apply(instance=instance, requester=actor, admission=_human_admission(service, instance, actor, generation, body.operation_id), **body.model_dump())
    return {"complete": True, "path": result.path}
