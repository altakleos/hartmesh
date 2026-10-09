"""Lifetime-bound human plugin actions over the canonical Work services."""

from collections.abc import Mapping

from deerflow_extension_api.auth import resolve_principal
from deerflow_extension_api.human_input import HumanInputActions
from fastapi import HTTPException

from app.gateway.authz import authorize_plugin_action_for_request, get_auth_context, resolve_route_permissions_for_request
from app.gateway.deps import get_current_user_from_request
from app.gateway.routers.agent_instances import _service
from app.gateway.routers.spaces import _actor
from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.human_input_contract import CreateRequest, RequestCommand, Respond
from deerflow.agent_instances.work import AgentWork
from deerflow.agent_instances.work_contract import WorkCommand
from deerflow.extensions.plugin_tools import plugin_settings
from deerflow.utils.file_io import run_file_io


class HostHumanInputActions(HumanInputActions):
    def __init__(self, request, source, plugin, action, principal):
        self._request, self._source, self._plugin, self._action = request, source, plugin, action
        self._user_id = principal.user_id
        self._active = True

    def retire(self):
        self._active = False

    async def call(self, operation, payload):
        if operation not in {"list", "get", "responses", "history", "create", "respond", "command", "work_command"} or not isinstance(payload, Mapping):
            raise ValueError("Unsupported human input operation")
        request, plugin = self._request, self._plugin
        if not self._active or not any(source == self._source and installed is plugin for source, installed in request.app.state.extensions.plugins):
            raise PermissionError("Human action handle is retired")
        principal = resolve_principal(request)
        if principal is None or principal.user_id != self._user_id:
            raise PermissionError("Human action identity changed")
        settings = await run_file_io(plugin_settings, self._source, plugin)
        if settings["enabled"] is not True:
            raise PermissionError("Plugin is disabled")
        await authorize_plugin_action_for_request(request, namespace=plugin.namespace, action_name=self._action.name)
        if self._action.purpose == "management":
            from app.gateway.app import _resolve_extension_plugin_management_async

            if not await _resolve_extension_plugin_management_async(request, plugin.namespace, "write"):
                raise PermissionError("Management action is unavailable")
        permissions = await resolve_route_permissions_for_request(request, await get_current_user_from_request(request))
        context = get_auth_context(request)
        if context is not None:
            permissions = [permission for permission in permissions if permission in context.permissions]
        write = operation in {"create", "respond", "command", "work_command"}
        if "agents:read" not in permissions or (write and "agents:write" not in permissions):
            raise PermissionError("Human Work action is not allowed")
        actor = await _actor(request)
        if actor.subject_id != self._user_id or actor.kind != "human" or not self._active:
            raise PermissionError("Human action identity is unavailable")
        work = AgentWork(await _service(request))
        service = HumanInput(work)
        args = dict(payload)
        # Explicit argument allowlists exclude actor, source attribution, authority
        # and arbitrary service dispatch. DTO validation supplies finite limits.
        allowed = {
            "list": {"view", "instance_id", "work_id", "limit", "offset"},
            "get": {"request_id"},
            "responses": {"request_id", "limit", "offset"},
            "history": {"request_id", "limit", "offset"},
            "create": {"instance_id", "work_id", "body"},
            "respond": {"request_id", "body"},
            "command": {"request_id", "body"},
            "work_command": {"instance_id", "work_id", "body"},
        }[operation]
        if set(args) - allowed:
            raise ValueError("Unexpected human action arguments")
        model = {"create": CreateRequest, "respond": Respond, "command": RequestCommand, "work_command": WorkCommand}.get(operation)
        if model:
            args["request"] = model.model_validate(args.pop("body", None))
        if operation in {"list", "get"}:
            args["can_write"] = "agents:write" in permissions
        try:
            return await (work.command if operation == "work_command" else getattr(service, operation))(actor=actor, **args)
        except TypeError:
            raise HTTPException(422, "Missing or invalid human action arguments") from None
