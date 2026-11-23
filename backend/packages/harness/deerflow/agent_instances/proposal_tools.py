"""Issue the installed submission form only inside a live employee lead tool call."""

import json
from collections import Counter
from contextlib import asynccontextmanager
from types import MappingProxyType

from deerflow_extension_api.plugins import ToolContext
from deerflow_extension_api.storage import StorageActor

from deerflow.agent_instances.contract import AgentDenied
from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY, AgentExecution
from deerflow.agent_instances.proposals import FIELD, ConfigurationProposals
from deerflow.agent_instances.runtime import current_environment
from deerflow.agent_instances.work_tools import on_owner_loop
from deerflow.agents.middlewares.model_response import last_ai_message
from deerflow.extensions import get_agent_build_extensions, get_loaded_extensions
from deerflow.extensions.plugin_tools import _build_tool, plugin_settings, plugin_tool_name
from deerflow.runtime.runs.manager import RunRecord
from deerflow.runtime.runs.schemas import RunStatus


def installed_forms(loaded):
    return [(source, plugin) for source, plugin in getattr(loaded, "plugins", ()) if plugin.proposal_api_version == 1 and plugin.proposal_tool is not None and plugin_settings(source, plugin)["enabled"] is True]


def proposal_tools(groups):
    loaded = get_agent_build_extensions()
    return [_build_tool(source, plugin, plugin.proposal_tool, loaded) for source, plugin in installed_forms(loaded) if groups is None or plugin.proposal_tool.group in groups]


def _arguments(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def observe_proposal_calls(environment, response, names, issuance):
    message = last_ai_message(response)
    observed = {}
    # LangGraph assigns the same task ID on node retry and a new one on the next model step.
    if message is not None and isinstance(issuance, str) and issuance:
        counts = Counter(call.get("id") for call in message.tool_calls)
        for call in message.tool_calls:
            name, call_id = call.get("name"), call.get("id")
            if name in names and isinstance(call_id, str) and counts[call_id] == 1:
                observed[(name, call_id)] = (issuance, _arguments(call["args"]))
    environment.proposal_calls = observed


@asynccontextmanager
async def bound_submission(runtime, source, plugin, declaration, settings, payload):
    context = runtime.context or {}
    execution = context.get(AGENT_EXECUTION_CONTEXT_KEY)
    environment = current_environment()
    name = plugin_tool_name(plugin.namespace, declaration.name)
    call_id = runtime.tool_call_id
    if not isinstance(execution, AgentExecution) or not execution.proposal_submission or context.get("is_subagent") or environment is None or environment.execution is not execution:
        raise AgentDenied("Configuration submission requires an interactive employee lead")
    record = environment.run_record
    issuance = environment.proposal_calls.get((name, call_id))
    if not isinstance(record, RunRecord) or record.thread_id != execution.thread_id or record.user_id != execution.requester.subject_id or not issuance or issuance[1] != _arguments(payload):
        raise AgentDenied("Configuration submission requires the current issuing model call")
    active = True

    def check_live():
        if (
            not active
            or current_environment() is not environment
            or environment.proposal_calls.get((name, call_id)) != issuance
            or record.status != RunStatus.running
            or record.finalizing
            or record.abort_event.is_set()
            or record.ownership_lost
        ):
            raise AgentDenied("Configuration submission is unavailable for this run")
        if not any(installed_source == source and installed is plugin for installed_source, installed in get_loaded_extensions().plugins):
            raise AgentDenied("The proposal form is no longer installed")

    async def submit(field, proposed_value):
        check_live()
        if field != FIELD or type(proposed_value) is not bool or proposed_value is not payload["enabled"]:
            raise ValueError("Unsupported proposal field or value")
        await execution.validate(environment)
        service = ConfigurationProposals(execution.authority.instances)
        return await on_owner_loop(execution, service.submit(execution, run_id=record.run_id, issuer_id=issuance[0], call_id=call_id, proposed_value=proposed_value, check_live=check_live))

    check_live()
    try:
        yield ToolContext(None, MappingProxyType(settings), execution.thread_id, actor=StorageActor("nonhuman", execution.instance.principal.subject_id), submit_proposal=submit)
    finally:
        active = False
