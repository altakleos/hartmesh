"""Persistent instance management; request metadata never authenticates agents."""

from dataclasses import asdict
from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.routing import APIRoute
from pydantic import Field, model_validator

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
from deerflow.utils.thread_id import ThreadId


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
            except NotImplementedError as exc:
                raise HTTPException(501, str(exc)) from None
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


class CreateCompanyCopy(StrictRequest):
    creation_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    generation: int = Field(ge=1, le=2**31 - 1)
    name: str = Field(min_length=1, max_length=128)
    supervisor_id: str | None = Field(default=None, min_length=1, max_length=128)


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
async def list_instances(request: Request, limit: int = Query(100, ge=1, le=500), offset: int = Query(0, ge=0), include_deleted: bool = False):
    service = await _service(request)
    instances = await service.list(actor=await _actor(request), limit=limit, offset=offset, include_deleted=include_deleted)
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
async def get_instance(instance_id: str, request: Request, include_deleted: bool = False):
    from deerflow.agent_instances.contract import AgentPermission

    service = await _service(request)
    actor = await _actor(request)
    # Use recipients need the identity/control projection, never adopted content.
    try:
        instance = await service.get(actor=actor, instance_id=instance_id, permission=AgentPermission.INSPECT, include_deleted=include_deleted)
    except AgentDenied:
        try:
            instance = await service.get(actor=actor, instance_id=instance_id, permission=AgentPermission.USE, include_deleted=include_deleted)
        except AgentDenied:
            instance = await service.get(actor=actor, instance_id=instance_id, permission=AgentPermission.MANAGE, include_deleted=include_deleted)
    return jsonable_encoder(asdict(instance))


@router.post("/{instance_id}/company-copy", status_code=201)
@require_permission("agents", "write")
@require_permission("agents", "read")
async def create_company_copy(instance_id: str, body: CreateCompanyCopy, request: Request):
    from sqlalchemy import select

    from deerflow.agent_instances.contract import AgentPermission
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow

    service = await _service(request)
    actor = await _actor(request)
    supervisor = PrincipalRef("human", body.supervisor_id) if body.supervisor_id is not None else actor
    existing = await service.creation(actor=actor, creation_id=body.creation_id)
    if existing is not None:
        intent, snapshot = existing
        expected = {"name": body.name, "custody": "company", "supervisor_id": supervisor.subject_id, "definition_name": snapshot.config["name"], "copy_from": instance_id, "copy_generation": body.generation}
        if intent != expected:
            raise AgentConflict("Company-copy identity belongs to another request")
    else:
        async with service._sf() as session, session.begin():
            await service._reserve_writer(session)
            row = (await session.execute(select(AgentInstanceRow).where(AgentInstanceRow.id == instance_id).with_for_update())).scalar_one_or_none()
            grant = await session.get(AgentInstanceGrantRow, (instance_id, actor.subject_id))
            if row is None or grant is None:
                raise AgentDenied("Personal source is unavailable")
            view = service._view(row, grant.permissions)
            if (
                view.custody != "personal"
                or view.owner_id != actor.subject_id
                or view.status in {"deleted", "provisioning"}
                or view.permissions & (AgentPermission.INSPECT | AgentPermission.MANAGE) != AgentPermission.INSPECT | AgentPermission.MANAGE
            ):
                raise AgentDenied("Company copying requires the current personal custodian with Inspect and Manage")
            await service._human(actor)
            if view.generation != body.generation:
                raise AgentConflict("Personal source changed; review it before copying")
            captured = await service._stored_definition(session, view.definition_revision)
            # Explicit business-data copy under the consenting custodian's namespace.
            # This grants no old owner identity, credentials or storage membership.
            snapshot = DefinitionSnapshot.capture(owner_id=actor.subject_id, config=captured.config, soul=captured.soul)
    instance = await service.create(actor=actor, creation_id=body.creation_id, name=body.name, custody="company", supervisor=supervisor, definition=snapshot, copy_from=instance_id, copy_generation=body.generation)
    return jsonable_encoder(asdict(instance))


@router.get("/{instance_id}/definition")
@require_permission("agents", "read")
async def adopted_definition(instance_id: str, request: Request):
    snapshot = await (await _service(request)).definition(actor=await _actor(request), instance_id=instance_id)
    return {"revision": snapshot.revision, "config": snapshot.config, "soul": snapshot.soul}


@router.get("/{instance_id}/grants")
@require_permission("agents", "read")
async def instance_grants(instance_id: str, request: Request):
    from sqlalchemy import select

    from deerflow.agent_instances.contract import AgentPermission
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    service = await _service(request)
    await service.get(actor=await _actor(request), instance_id=instance_id, permission=AgentPermission.MANAGE, include_deleted=True)
    async with service._sf() as session:
        grants = (await session.execute(select(AgentInstanceGrantRow).where(AgentInstanceGrantRow.instance_id == instance_id).order_by(AgentInstanceGrantRow.user_id))).scalars().all()
        return {"grants": [{"user_id": grant.user_id, "permissions": grant.permissions} for grant in grants]}


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


@router.get("/conversations/{thread_id}/instance")
@require_permission("agents", "read")
@require_permission("threads", "read")
async def conversation_instance(thread_id: ThreadId, request: Request):
    from deerflow.agent_instances.contract import AgentPermission

    await _service(request)
    authority = getattr(request.app.state, "agent_conversations", None)
    actor = await _actor(request)
    instance_id = await authority.binding(thread_id) if authority is not None else None
    if instance_id is None:
        return {"instance": None}
    if not await authority.allowed(actor=actor, thread_id=thread_id, permission=AgentPermission.INSPECT):
        raise AgentDenied("Conversation is unavailable")
    return {"instance": await get_instance(instance_id, request)}


class LifecycleChange(StrictRequest):
    operation_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    generation: int = Field(ge=1, lt=2**31 - 1)
    action: Literal["suspend", "archive", "delete", "restore", "adopt", "supervise", "grant"]
    definition_name: str | None = Field(default=None, pattern=r"^[a-z0-9-]{1,128}$")
    supervisor_id: str | None = Field(default=None, min_length=1, max_length=128)
    member_id: str | None = Field(default=None, min_length=1, max_length=128)
    permissions: int | None = Field(default=None, ge=0, le=7)

    @model_validator(mode="after")
    def fields_match_action(self):
        if (self.action == "adopt") != (self.definition_name is not None) or (self.action == "supervise") != (self.supervisor_id is not None):
            raise ValueError("Lifecycle action fields do not match")
        if self.action == "grant":
            if self.member_id is None or self.permissions is None:
                raise ValueError("Grant action requires member and permissions")
        elif self.member_id is not None or self.permissions is not None:
            raise ValueError("Grant fields require a grant action")
        return self


@router.get("/{instance_id}/lifecycle")
@require_permission("agents", "read")
async def lifecycle_status(instance_id: str, request: Request):
    from deerflow.agent_instances.lifecycle import InstanceLifecycle

    return {"operations": await InstanceLifecycle(await _service(request)).status(actor=await _actor(request), instance_id=instance_id)}


@router.post("/{instance_id}/lifecycle")
@require_permission("agents", "write")
@require_permission("agents", "read")
async def change_lifecycle(instance_id: str, body: LifecycleChange, request: Request, response: Response):
    from deerflow.agent_instances.contract import AgentPermission
    from deerflow.agent_instances.lifecycle import InstanceLifecycle
    from deerflow.persistence.agent_instances.model import AgentLifecycleRow

    service = await _service(request)
    actor = await _actor(request)
    view = await service.get(actor=actor, instance_id=instance_id, permission=AgentPermission.MANAGE, include_deleted=True)
    definition = None
    if body.definition_name is not None:
        async with service._sf() as session:
            intent = await session.get(AgentLifecycleRow, (instance_id, body.operation_id))
            if intent is not None and (intent.actor_id == actor.subject_id or view.custody == "company") and intent.request.get("definition_revision"):
                definition = await service._stored_definition(session, intent.request["definition_revision"])
                if definition.config["name"].lower() != body.definition_name:
                    raise AgentConflict("Lifecycle definition retry changed")
        definition = definition or await _load_definition(request, actor, body.definition_name)
    result = await InstanceLifecycle(service).change(
        actor=actor,
        instance_id=instance_id,
        expected_generation=body.generation,
        operation_id=body.operation_id,
        action=body.action,
        definition=definition,
        supervisor=PrincipalRef("human", body.supervisor_id) if body.supervisor_id is not None else None,
        member=PrincipalRef("human", body.member_id) if body.member_id is not None else None,
        permissions=body.permissions,
    )
    response.status_code = 200 if result["complete"] else 202
    return {**result, "instance": jsonable_encoder(asdict(result["instance"]))}


class AbandonLifecycle(StrictRequest):
    operation_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    generation: int = Field(ge=1, le=2**31 - 1)


@router.post("/{instance_id}/lifecycle/abandon")
@require_permission("agents", "write")
@require_permission("agents", "read")
async def abandon_lifecycle(instance_id: str, body: AbandonLifecycle, request: Request):
    from deerflow.agent_instances.lifecycle import InstanceLifecycle

    result = await InstanceLifecycle(await _service(request)).abandon(actor=await _actor(request), instance_id=instance_id, expected_generation=body.generation, operation_id=body.operation_id)
    return {**result, "instance": jsonable_encoder(asdict(result["instance"]))}


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
