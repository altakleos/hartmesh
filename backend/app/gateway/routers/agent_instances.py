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
from deerflow.config.agents_api_config import get_agents_api_config
from deerflow.persistence.agents import get_agent_store
from deerflow.spaces.contract import PrincipalRef, SpaceConflict, SpaceDenied
from deerflow.spaces.filesystem import FilesystemUnavailable
from deerflow.utils.file_io import run_file_io


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
