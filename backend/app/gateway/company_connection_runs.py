"""Host admission and public turn attribution for company connections."""

from collections.abc import Mapping
from uuid import uuid4

from fastapi import HTTPException, Request
from fastapi.routing import APIRoute
from langchain_core.messages import HumanMessage

from app.gateway.auth_disabled import AUTH_SOURCE_AUTH_DISABLED, AUTH_SOURCE_INTERNAL, AUTH_SOURCE_SESSION
from deerflow.agent_instances.conversations import ConnectionOrigin
from deerflow.runtime.run_origin import DEERFLOW_ORIGIN_KEY, admit_origin

PACKAGE_MESSAGE_KEY = "deerflow_package_origin"
PACKAGE_TURN_KEY = "deerflow_package_turn"
_INTERACTIVE_ROUTES = {
    ("app.gateway.routers.thread_runs", "create_run"),
    ("app.gateway.routers.thread_runs", "stream_run"),
    ("app.gateway.routers.thread_runs", "wait_run"),
    ("app.gateway.routers.runs", "stateless_stream"),
    ("app.gateway.routers.runs", "stateless_wait"),
}


def interactive_proposal_turn(request, body):
    """Attribute only a fresh, directly submitted human web message."""
    route = request.scope.get("route") if isinstance(request, Request) else None
    if not isinstance(route, APIRoute) or (route.endpoint.__module__, route.name) not in _INTERACTIVE_ROUTES or getattr(request.state, "auth_source", None) != AUTH_SOURCE_SESSION:
        return False
    if any(getattr(body, field, None) is not None for field in ("command", "checkpoint", "checkpoint_id")):
        return False
    messages = (body.input or {}).get("messages")
    if not isinstance(messages, list) or not messages:
        return False
    if not all(isinstance(m, dict) and m.get("role", m.get("type")) in ("human", "user") for m in messages):
        return False
    attributes = messages[-1].get("additional_kwargs", {})
    return isinstance(attributes, dict) and not attributes.get("hide_from_ui") and getattr(request.state, "admitted_queued_human", False) is not True


def proposal_account_admission(user):
    """Recheck the admitted web principal using the borrowed employee transaction."""
    from app.gateway.deps import get_user_repo
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.config.agents_api_config import get_agents_api_config
    from deerflow.utils.file_io import run_file_io

    def identity(current):
        return tuple(getattr(current, key, None) for key in ("id", "token_version", "system_role", "role_limit", "oauth_provider", "oauth_id", "oauth_issuer", "needs_setup", "disabled_at", "released_at"))

    original = identity(user)
    repository = get_user_repo()

    async def admit(session):
        current = await repository.get_user_by_id(str(original[0]), session=session) if repository is not None else None
        if current is None or identity(current) != original or not (await run_file_io(get_agents_api_config)).enabled:
            raise AgentDenied("The proposal requester's authority changed")

    return admit


def package_name_for_turn(request, origin):
    host = admit_origin(getattr(request.state, "run_origin", None))
    return host.get("namespace") if host and host == origin and host["kind"] == "extension" else None


def admitted_connection_origin(request, metadata, origin, *, assignment, config, extensions, telegram=False):
    """An unrecognized host launch receives no connection authority."""
    if assignment:
        return ConnectionOrigin("assignment")
    if telegram and origin == {"kind": "im_channel", "provider": "telegram"}:
        return ConnectionOrigin("message")
    if origin is None:
        declared = getattr(request.state, "run_origin", None)
        if getattr(request.state, "auth_source", None) == AUTH_SOURCE_INTERNAL and isinstance(metadata, Mapping):
            declared = metadata.get(DEERFLOW_ORIGIN_KEY, declared)
        if declared is not None:
            return None
        auth_source = getattr(request.state, "auth_source", None)
        if telegram or (getattr(request.state, "admitted_queued_human", False) is True and auth_source in {AUTH_SOURCE_SESSION, AUTH_SOURCE_AUTH_DISABLED}):
            return ConnectionOrigin("message")
        route = request.scope.get("route") if isinstance(request, Request) else None
        source = getattr(request.state, "auth_source", None)
        if isinstance(route, APIRoute) and (route.endpoint.__module__, route.name) in _INTERACTIVE_ROUTES and source in {AUTH_SOURCE_SESSION, AUTH_SOURCE_AUTH_DISABLED}:
            return ConnectionOrigin("message")
        return None
    if origin["kind"] == "subagent_batch" and getattr(request.state, "durable_batch_followup", False) is True:
        return ConnectionOrigin("batch_followup")
    if origin["kind"] != "extension":
        return None
    package = package_name_for_turn(request, origin)
    allowed = getattr(getattr(config, "agent_connections", None), "packages", None)
    if not package or not isinstance(allowed, list) or not all(isinstance(name, str) for name in allowed):
        return None
    installed = {plugin.namespace for _, plugin in getattr(extensions, "plugins", ())}
    if package not in installed or package not in allowed:
        return None
    return ConnectionOrigin("package", package)


def stamp_package_turn(graph_input, metadata, *, package: str, user_id: str, run_id: str) -> None:
    """Pair displayed human rows with this run's separately stored host attribution."""
    if not isinstance(graph_input, dict) or not isinstance(graph_input.get("messages"), list):
        return
    ids = []
    stamped = []
    for message in graph_input["messages"]:
        if isinstance(message, HumanMessage) and not message.additional_kwargs.get("hide_from_ui"):
            # A package supplies text, but never the identity of the host-
            # attributed row. Reusing a prior human ID would replace that turn.
            message_id = str(uuid4())
            ids.append(message_id)
            candidate = {"package": package, "user_id": user_id, "run_id": run_id}
            message = message.model_copy(update={"id": message_id, "additional_kwargs": {**message.additional_kwargs, PACKAGE_MESSAGE_KEY: candidate}})
        stamped.append(message)
    if ids:
        graph_input["messages"] = stamped
        metadata[PACKAGE_TURN_KEY] = {"run_id": run_id, "message_ids": ids}


async def package_input_source(request, thread_id, target_metadata):
    """Only the same named package may put text into an employee run it started."""
    caller = admit_origin(getattr(getattr(request, "state", None), "run_origin", None))
    if caller is None or caller["kind"] != "extension":
        return None
    from app.gateway.agent_conversations import require_conversation
    from deerflow.agent_instances.contract import AgentPermission

    if not await require_conversation(request, thread_id, permission=AgentPermission.USE):
        return None
    target = admit_origin((target_metadata or {}).get(DEERFLOW_ORIGIN_KEY))
    if not caller.get("namespace") or not target or target.get("kind") != "extension" or target.get("namespace") != caller["namespace"]:
        raise HTTPException(403, "A package may send input only to an employee run it started")
    return caller["namespace"]
