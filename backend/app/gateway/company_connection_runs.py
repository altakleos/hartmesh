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


def admitted_web_chat_request(request):
    """Recognize a person at the web entry point or dispatch of their stored queued input."""
    state = request.state
    if getattr(state, "auth_source", None) not in {AUTH_SOURCE_SESSION, AUTH_SOURCE_AUTH_DISABLED} or getattr(state, "run_origin", None) is not None:
        return False
    if getattr(state, "admitted_queued_human", False) is True:
        return True
    route = request.scope.get("route") if isinstance(request, Request) else None
    return isinstance(route, APIRoute) and (route.endpoint.__module__, route.name) in _INTERACTIVE_ROUTES


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
        if telegram or admitted_web_chat_request(request):
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
