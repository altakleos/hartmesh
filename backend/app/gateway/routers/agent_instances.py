"""Persistent instance management; request metadata never authenticates agents."""

from dataclasses import asdict
from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.routing import APIRoute
from pydantic import Field

from app.gateway.authz import require_permission
from app.gateway.routers.spaces import StrictRequest, _actor, _http_error
from deerflow.agent_instances.contract import AgentConflict, AgentDenied, DefinitionSnapshot
from deerflow.agent_instances.service import AgentInstances
from deerflow.agents.memory.manager import MemoryConflictError, MemoryCorruptionError, _get_host_memory_manager
from deerflow.config.agents_api_config import get_agents_api_config
from deerflow.persistence.agents import get_agent_store
from deerflow.spaces.contract import PrincipalRef, SpaceConflict, SpaceDenied
from deerflow.spaces.filesystem import FilesystemUnavailable
from deerflow.utils.file_io import await_drained, run_file_io


class InstanceRoute(APIRoute):
    def get_route_handler(self):
        handler = super().get_route_handler()

        async def admitted(request):
            try:
                return await handler(request)
            except AgentDenied:
                raise HTTPException(404, "Agent instance is unavailable") from None
            except AgentConflict as exc:
                raise HTTPException(409, str(exc)) from None
            except MemoryConflictError:
                raise HTTPException(409, "Instance memory changed; reload before retrying") from None
            except MemoryCorruptionError:
                raise HTTPException(500, "Instance memory data is invalid") from None
            except FileNotFoundError:
                raise HTTPException(404, "Agent definition is unavailable") from None
            except (SpaceDenied, SpaceConflict, FilesystemUnavailable, ValueError, OSError) as exc:
                raise _http_error(exc) from None

        return admitted


router = APIRouter(prefix="/api/agent-instances", tags=["agent-instances"], route_class=InstanceRoute)


async def _service(request: Request) -> AgentInstances:
    if not (await run_file_io(get_agents_api_config)).enabled:
        raise HTTPException(403, "Agent management API is disabled")
    service = getattr(request.app.state, "agent_instances", None)
    if not isinstance(service, AgentInstances):
        raise HTTPException(501, "Persistent instances require qualified Storage Spaces and a durable database")
    return service


class CreateInstance(StrictRequest):
    creation_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    name: str = Field(min_length=1, max_length=128)
    definition_name: str = Field(pattern=r"^[a-z0-9-]{1,128}$")
    custody: Literal["personal", "company"] = "personal"
    supervisor_id: str | None = Field(default=None, min_length=1, max_length=128)


class RenameInstance(StrictRequest):
    generation: int = Field(ge=1, le=2**31 - 1)
    name: str = Field(min_length=1, max_length=128)


class CreateConversation(StrictRequest):
    creation_id: str = Field(pattern=r"^[0-9a-f]{32}$")


async def _load_definition(request, actor, name):
    # Explicit owner selection stays inside trusted host code. The browser
    # cannot send another owner's ID, a snapshot, revision or capability grant.
    def load():
        store = get_agent_store()
        config = store.get(name, user_id=actor.subject_id)
        soul = store.get_soul(name, user_id=actor.subject_id) or ""
        document = config.model_dump(mode="json")
        document["name"] = name
        return DefinitionSnapshot.capture(owner_id=actor.subject_id, config=document, soul=soul)

    return await run_file_io(load)


@router.get("")
@require_permission("agents", "read")
async def list_instances(request: Request, limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0)):
    service = await _service(request)
    instances = await service.list(actor=await _actor(request), limit=limit, offset=offset)
    return {"instances": [jsonable_encoder(asdict(instance)) for instance in instances]}


@router.post("", status_code=201)
@require_permission("agents", "write")
@require_permission("agents", "read")
async def create_instance(body: CreateInstance, request: Request):
    service = await _service(request)
    actor = await _actor(request)
    supervisor = PrincipalRef("human", body.supervisor_id) if body.supervisor_id is not None else actor
    existing = await service.creation(actor=actor, creation_id=body.creation_id)
    expected = {"name": body.name, "custody": body.custody, "supervisor_id": supervisor.subject_id, "definition_name": body.definition_name}
    if existing is not None:
        intent, definition = existing
        if intent != expected:
            raise AgentConflict("Creation identity belongs to another agent request")
    else:
        definition = await _load_definition(request, actor, body.definition_name)
    try:
        instance = await service.create(actor=actor, creation_id=body.creation_id, name=body.name, custody=body.custody, supervisor=supervisor, definition=definition)
    except AgentDenied as exc:
        raise HTTPException(403, str(exc)) from None
    return jsonable_encoder(asdict(instance))


@router.get("/{instance_id}")
@require_permission("agents", "read")
async def get_instance(instance_id: str, request: Request):
    instance = await (await _service(request)).get(actor=await _actor(request), instance_id=instance_id)
    return jsonable_encoder(asdict(instance))


@router.get("/{instance_id}/definition")
@require_permission("agents", "read")
async def adopted_definition(instance_id: str, request: Request):
    snapshot = await (await _service(request)).definition(actor=await _actor(request), instance_id=instance_id)
    return {"revision": snapshot.revision, "config": snapshot.config, "soul": snapshot.soul}


@router.patch("/{instance_id}")
@require_permission("agents", "write")
async def rename_instance(instance_id: str, body: RenameInstance, request: Request):
    instance = await (await _service(request)).rename(actor=await _actor(request), instance_id=instance_id, expected_generation=body.generation, name=body.name)
    return jsonable_encoder(asdict(instance))


@router.post("/{instance_id}/conversations", status_code=201)
@require_permission("threads", "write")
@require_permission("agents", "read")
async def create_conversation(instance_id: str, body: CreateConversation, request: Request):
    await _service(request)
    authority = getattr(request.app.state, "agent_conversations", None)
    if authority is None:
        raise HTTPException(501, "Agent conversation runtime is unavailable")
    return await authority.create(actor=await _actor(request), instance_id=instance_id, creation_id=body.creation_id)


class CreateMemoryFact(StrictRequest):
    content: str = Field(min_length=1, max_length=65536)
    category: str = Field(default="context", min_length=1, max_length=128)
    confidence: float = Field(default=0.7, ge=0, le=1)


class UpdateMemoryFact(StrictRequest):
    content: str | None = Field(default=None, min_length=1, max_length=65536)
    category: str | None = Field(default=None, min_length=1, max_length=128)
    confidence: float | None = Field(default=None, ge=0, le=1)


class ImportMemory(StrictRequest):
    document: dict


async def _memory_manager(request, instance_id, *, write=False):
    from deerflow.agent_instances.contract import AgentPermission
    from deerflow.agent_instances.memory import instance_memory_service
    from deerflow.agents.memory.backends.deermem.deer_mem import DeerMem
    from deerflow.agents.memory.manager import _resolve_manager_class
    from deerflow.config.memory_config import get_memory_config

    agents = await _service(request)
    actor = await _actor(request)
    await agents.get(actor=actor, instance_id=instance_id, permission=AgentPermission.INSPECT | (AgentPermission.MANAGE if write else AgentPermission(0)))
    config = await run_file_io(get_memory_config)
    supported = await run_file_io(_resolve_manager_class, config.manager_class)
    if supported is not DeerMem or config.backend_config.get("storage_class", "") not in ("", "file", "markdown") or config.backend_config.get("retrieval_adapter", "fts5") not in ("", "fts5"):
        raise HTTPException(501, "The configured memory backend has no qualified instance scope")
    base = await run_file_io(_get_host_memory_manager)
    if type(base) is not DeerMem or base._config.storage_class not in ("", "file", "markdown") or base._config.retrieval_adapter not in ("", "fts5"):
        raise HTTPException(501, "The configured memory backend has no qualified instance scope")
    storage = await instance_memory_service(agents).bind(actor=actor, instance_id=instance_id, management_write=write)
    return base.with_storage(storage), storage.scope.agent_name, actor.subject_id


async def _memory_call(request, instance_id, operation, *, write=False, **kwargs):
    manager, agent_name, user_id = await _memory_manager(request, instance_id, write=write)
    try:
        work = run_file_io(getattr(manager, operation), agent_name=agent_name, user_id=user_id, **kwargs)
        return await await_drained(work) if write else await work
    except KeyError:
        raise HTTPException(404, "Instance memory fact is unavailable") from None


@router.get("/{instance_id}/memory")
@router.get("/{instance_id}/memory/export")
@require_permission("agents", "read")
@require_permission("memory", "read")
async def get_instance_memory(instance_id: str, request: Request):
    return await _memory_call(request, instance_id, "get_memory")


@router.delete("/{instance_id}/memory")
@require_permission("agents", "write")
@require_permission("memory", "write")
@require_permission("memory", "read")
async def clear_instance_memory(instance_id: str, request: Request):
    return await _memory_call(request, instance_id, "clear_memory", write=True)


@router.post("/{instance_id}/memory/import")
@require_permission("agents", "write")
@require_permission("memory", "write")
@require_permission("memory", "read")
async def import_instance_memory(instance_id: str, body: ImportMemory, request: Request):
    return await _memory_call(request, instance_id, "import_memory", write=True, memory_data=body.document)


@router.post("/{instance_id}/memory/facts", status_code=201)
@require_permission("agents", "write")
@require_permission("memory", "write")
@require_permission("memory", "read")
async def create_instance_memory_fact(instance_id: str, body: CreateMemoryFact, request: Request):
    document, fact_id = await _memory_call(request, instance_id, "create_fact", write=True, **body.model_dump())
    return {"memory": document, "fact_id": fact_id}


@router.patch("/{instance_id}/memory/facts/{fact_id}")
@require_permission("agents", "write")
@require_permission("memory", "write")
@require_permission("memory", "read")
async def update_instance_memory_fact(instance_id: str, fact_id: str, body: UpdateMemoryFact, request: Request):
    return await _memory_call(request, instance_id, "update_fact", write=True, fact_id=fact_id, **body.model_dump(exclude_unset=True))


@router.delete("/{instance_id}/memory/facts/{fact_id}")
@require_permission("agents", "write")
@require_permission("memory", "write")
@require_permission("memory", "read")
async def delete_instance_memory_fact(instance_id: str, fact_id: str, request: Request):
    return await _memory_call(request, instance_id, "delete_fact", write=True, fact_id=fact_id)
