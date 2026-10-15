"""Agent workspace decisions use admitted authority and ordinary provider doubles."""

import asyncio
import threading
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from langchain_core.messages import HumanMessage, ToolMessage
from langgraph.runtime import Runtime
from langgraph.types import Command
from test_agent_execution_runtime import execution
from test_sandbox_middleware import _make_tool_call_request, _NetworkPolicyProvider

from deerflow.agent_instances import runtime
from deerflow.agent_instances.contract import AgentInstance, AgentPermission, Refusal
from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
from deerflow.sandbox.middleware import SandboxMiddleware
from deerflow.sandbox.sandbox_provider import reset_sandbox_provider, set_sandbox_provider


def agent_context(*, manage=True, work=False, custody="company", **extra):
    bound = execution()
    identity = vars(bound.instance) | {"custody": custody, "owner_id": "creator" if custody == "personal" else None}
    bound = replace(bound, instance=AgentInstance(**identity, permissions=AgentPermission(7 if manage else 3)), work=SimpleNamespace() if work else None)
    return {AGENT_EXECUTION_CONTEXT_KEY: bound, **extra}


@pytest.mark.parametrize("custody", ["personal", "company"])
def test_network_card_allowed_has_three_answers(custody):
    assert runtime.network_card_allowed(None) is None
    assert runtime.network_card_allowed({AGENT_EXECUTION_CONTEXT_KEY: {"forged": True}}) is None
    assert runtime.network_card_allowed(agent_context(custody=custody)) is True
    assert runtime.network_card_allowed(agent_context(custody=custody, manage=False)) is False
    assert runtime.network_card_allowed(agent_context(custody=custody, work=True)) is False


def test_requester_can_manage_reads_the_admitted_execution_and_does_no_io():
    assert runtime.requester_can_manage(None) is None
    assert runtime.requester_can_manage({}) is None
    assert runtime.requester_can_manage(agent_context()) is True
    assert runtime.requester_can_manage(agent_context(manage=False)) is False


def test_instance_provider_reports_the_baseline_network_mode_and_delegates_decisions():
    baseline = Mock()
    baseline.sandbox_network_mode.return_value = "allowlist"
    baseline.sandbox_network_temporary_grant_ttl.return_value = 120
    baseline.decide_network_policy_request.return_value = False
    provider = runtime.InstanceSandboxProvider(execution(), SimpleNamespace(id="attachment"), baseline=baseline)
    assert provider.sandbox_network_mode() == "allowlist"
    assert provider.sandbox_network_temporary_grant_ttl() == 120
    assert provider.decide_network_policy_request("attachment", "request", "deny") is False
    baseline.decide_network_policy_request.assert_called_once_with("attachment", "request", "deny")


@pytest.mark.parametrize("as_command", [False, True])
def test_network_denial_artifact_denies_one_event_and_marks_the_last_tool_message(as_command):
    provider = _NetworkPolicyProvider()
    provider.events = [{"request_id": "oldest", "host": "example.org", "port": 443}]
    original = ToolMessage(content="blocked", tool_call_id="call", artifact={"existing": "kept"})
    earlier = ToolMessage(content="earlier", tool_call_id="earlier")
    result = Command(update={"messages": [earlier, original], "other": "kept"}, goto="continue") if as_command else original
    marked = runtime.network_denial_artifact(provider, "attachment", result)
    message = marked.update["messages"][-1] if as_command else marked
    assert message is not original and original.artifact == {"existing": "kept"}
    assert message.artifact == {"existing": "kept", "sandbox_network_denied": {"host": "example.org", "port": 443}}
    assert provider.decisions == [("attachment", "oldest", "deny")]
    if as_command:
        assert marked.goto == result.goto and marked.update["other"] == "kept"
        assert marked.update["messages"][0] is earlier and result.update["messages"][-1] is original
    assert runtime.network_denial_artifact(provider, "attachment", original) is original


@pytest.mark.parametrize("event", [{}, {"request_id": 3, "host": "example.org", "port": 443}, {"request_id": "a", "host": "example.org", "port": "443"}])
def test_malformed_network_denial_is_not_marked(event):
    provider = _NetworkPolicyProvider()
    provider.events = [event]
    result = ToolMessage(content="blocked", tool_call_id="call")
    assert runtime.network_denial_artifact(provider, "attachment", result) is result
    assert provider.decisions == []


@pytest.mark.parametrize(
    "context,decision,refused",
    [
        (None, "allow_sandbox", False),
        (agent_context(), "allow_temporary", False),
        (agent_context(), "allow_sandbox", True),
        (agent_context(manage=False), "allow_temporary", True),
        (agent_context(work=True), "allow_temporary", True),
        (agent_context(manage=False), "deny", False),
    ],
)
def test_manage_is_checked_again_when_the_answer_is_applied(context, decision, refused):
    if refused:
        with pytest.raises(Refusal) as error:
            runtime.check_network_decision(context, decision)
        assert error.value.reason == "managers-only" and "Analyst" in str(error.value)
    else:
        runtime.check_network_decision(context, decision)


def blocked_tool(context, *, async_path=False):
    provider = _NetworkPolicyProvider()
    provider.events = [{"request_id": "oldest", "host": "example.org", "port": 443}]
    request = _make_tool_call_request({"sandbox": {"sandbox_id": "existing"}})
    request.runtime.context.update(context)
    original = ToolMessage(content="blocked", tool_call_id="call-1", name="bash")

    async def handler(_request):
        return original

    set_sandbox_provider(provider)
    try:
        result = asyncio.run(SandboxMiddleware().awrap_tool_call(request, handler)) if async_path else SandboxMiddleware().wrap_tool_call(request, lambda _request: original)
    finally:
        reset_sandbox_provider()
    return provider, original, result


@pytest.mark.parametrize("async_path", [False, True])
def test_network_card_for_an_agent_offers_deny_and_temporary_only(async_path):
    _, _, result = blocked_tool(agent_context(), async_path=async_path)
    payload = result.update["messages"][0].artifact["human_input"]
    assert [item["id"] for item in payload["options"]] == ["deny", "allow_temporary"]


@pytest.mark.parametrize("context", [{}, agent_context()])
def test_every_network_request_carries_details_with_host_port_and_ttl(context):
    _, _, result = blocked_tool(context)
    payload = result.update["messages"][0].artifact["human_input"]
    assert payload["details"] == {"host": "example.org", "port": 443, "ttl_seconds": 300}
    if not context:
        assert [item["id"] for item in payload["options"]] == ["deny", "allow_temporary", "allow_sandbox"]


@pytest.mark.parametrize("async_path", [False, True])
@pytest.mark.parametrize("context", [agent_context(manage=False), agent_context(interaction_mode="autonomous"), agent_context(work=True, interaction_mode="interactive"), agent_context(is_subagent=True)])
def test_denied_agent_connection_marks_its_own_result_without_ending_the_turn(context, async_path):
    provider, original, result = blocked_tool(context, async_path=async_path)
    assert isinstance(result, ToolMessage) and result.content == original.content
    assert result.artifact == {"sandbox_network_denied": {"host": "example.org", "port": 443}}
    assert original.artifact is None
    assert provider.decisions == [("existing", "oldest", "deny")]
    assert provider.deny_pending_calls == []


def test_an_ordinary_chat_never_carries_sandbox_network_denied():
    provider, original, result = blocked_tool({"interaction_mode": "autonomous"})
    assert result is original and result.artifact is None
    assert provider.deny_pending_calls == ["existing"]


def test_a_denial_dropped_in_a_subagents_call_marks_the_subagents_own_message_and_nothing_stored():
    from deerflow.subagents.step_events import build_subagent_step

    _, original, result = blocked_tool(agent_context(is_subagent=True))
    assert result is not original and result.artifact["sandbox_network_denied"]["host"] == "example.org"
    stored = build_subagent_step(result.model_dump(), task_id="task", message_index=0)
    assert stored["kind"] == "tool" and "artifact" not in stored
    assert "sandbox_network_denied" not in str(stored)


def test_network_denial_ack_failure_logs_only_a_fixed_diagnostic(caplog):
    provider = _NetworkPolicyProvider()
    provider.events = [{"request_id": "private-request", "host": "private-host.example", "port": 443}]
    provider.decide_network_policy_request = Mock(return_value=False)
    result = runtime.network_denial_artifact(provider, "private-sandbox", ToolMessage(content="blocked", tool_call_id="call"))
    assert result.artifact["sandbox_network_denied"]["host"] == "private-host.example"
    assert "acknowledgement failed" in caplog.text
    assert not any(value in caplog.text for value in ("private-request", "private-host.example", "private-sandbox"))


@pytest.mark.asyncio
async def test_async_agent_denial_runs_off_loop_and_drains_repeated_cancellation():
    provider = _NetworkPolicyProvider()
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    main_thread = threading.get_ident()

    def consume(_sandbox_id):
        assert threading.get_ident() != main_thread
        entered.set()
        assert release.wait(5)
        return [{"request_id": "oldest", "host": "example.org", "port": 443}]

    def decide(*_args):
        finished.set()
        return True

    provider.consume_network_policy_events = consume
    provider.decide_network_policy_request = decide
    request = _make_tool_call_request({"sandbox": {"sandbox_id": "existing"}})
    request.runtime.context.update(agent_context(work=True))

    async def handler(_request):
        return ToolMessage(content="blocked", tool_call_id="call-1")

    set_sandbox_provider(provider)
    task = asyncio.create_task(SandboxMiddleware().awrap_tool_call(request, handler))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not finished.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        reset_sandbox_provider()


def test_apply_network_response_refuses_before_the_provider_is_asked():
    provider = _NetworkPolicyProvider()
    response = HumanMessage(
        content="allow",
        additional_kwargs={"human_input_response": {"version": 1, "kind": "human_input_response", "source": "sandbox_network", "request_id": "oldest", "response_kind": "option", "option_id": "allow_temporary", "value": "allow"}},
    )
    state = {"sandbox": {"sandbox_id": "existing"}, "messages": [response]}
    set_sandbox_provider(provider)
    try:
        with pytest.raises(Refusal, match="Only a person who can manage"):
            SandboxMiddleware()._apply_network_policy_response(state, Runtime(context=agent_context(manage=False)))
    finally:
        reset_sandbox_provider()
    assert provider.decisions == []
