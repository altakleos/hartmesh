"""Egress and scope facts are recorded for both sandbox session kinds."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from langchain.agents.middleware.types import ToolCallRequest
from langchain.tools import ToolRuntime
from langchain_core.messages import HumanMessage, ToolMessage
from langgraph.types import Command

from deerflow.sandbox import diagnostics as diagnostics_module
from deerflow.sandbox.diagnostics import discard_sandbox_diagnostics, sandbox_diagnostics
from deerflow.sandbox.middleware import SandboxMiddleware
from deerflow.sandbox.sandbox import Sandbox
from deerflow.sandbox.sandbox_provider import SandboxProvider, reset_sandbox_provider, set_sandbox_provider
from deerflow.sandbox.session import SandboxSessionKind
from deerflow.sandbox.tools import _execute_bash_command

_EVENT = {"request_id": "req-1", "host": "example.com", "port": 443, "method": "CONNECT"}


class _NetworkProvider(SandboxProvider):
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []
        self.decisions: list[tuple[str, str, str]] = []
        self.deny_pending_calls: list[str] = []

    def acquire(self, thread_id=None, *, user_id=None):
        return "box-1"

    def get(self, sandbox_id):
        return None

    def release(self, sandbox_id):
        return None

    def sandbox_network_mode(self):
        return "allowlist"

    def sandbox_network_temporary_grant_ttl(self):
        return 600

    def consume_network_policy_events(self, sandbox_id):
        events, self.events = self.events, []
        return events

    def deny_pending_network_policy_events(self, sandbox_id):
        self.deny_pending_calls.append(sandbox_id)
        self.events = []
        return True

    def decide_network_policy_request(self, sandbox_id, request_id, decision):
        self.decisions.append((sandbox_id, request_id, decision))
        return True


def _request(state: dict, context: dict) -> ToolCallRequest:
    runtime = ToolRuntime(state=state, context=context, config={"configurable": {}}, stream_writer=lambda _: None, tools=[], tool_call_id="call-1", store=None)
    return ToolCallRequest(tool_call={"id": "call-1", "name": "bash", "args": {}}, tool=None, state=state, runtime=runtime)


def _context(run_id: str, **extra) -> dict:
    return {"thread_id": "thread-1", "run_id": run_id, "user_id": "user-1", **extra}


@pytest.fixture
def provider():
    provider = _NetworkProvider()
    set_sandbox_provider(provider)
    try:
        yield provider
    finally:
        reset_sandbox_provider()


def _diagnostics(run_id: str):
    return [obs for _, obs in sandbox_diagnostics(run_id).since(0)]


def test_blocked_egress_is_recorded_thread_scoped_for_an_ordinary_session(provider) -> None:
    discard_sandbox_diagnostics("run-blocked")
    provider.events = [dict(_EVENT)]
    state = {"sandbox": {"sandbox_id": "box-1"}}
    original = ToolMessage(content="proxy denied", tool_call_id="call-1", name="bash")

    result = SandboxMiddleware().wrap_tool_call(_request(state, _context("run-blocked")), lambda _r: original)

    assert isinstance(result, Command)
    recorded = _diagnostics("run-blocked")
    assert [obs.kind for obs in recorded] == ["egress.blocked"]
    assert recorded[0].session_kind is SandboxSessionKind.ORDINARY
    assert recorded[0].thread_id == "thread-1"
    assert recorded[0].sandbox_ref == "box-1"
    assert recorded[0].execution_evidence_digest is None
    assert recorded[0].facts == {"host": "example.com", "method": "CONNECT", "port": 443, "request_ref": "req-1"}
    # The card the model sees is the same object the receipt layer will digest.
    card = result.update["messages"][0]
    assert card.artifact["human_input"]["request_id"] == "req-1"
    discard_sandbox_diagnostics("run-blocked")


def test_applied_decision_is_recorded_with_its_grant(provider) -> None:
    discard_sandbox_diagnostics("run-decided")
    response = HumanMessage(
        content="Allow network access for 10 minutes",
        additional_kwargs={
            "human_input_response": {
                "version": 1,
                "kind": "human_input_response",
                "source": "sandbox_network",
                "request_id": "req-1",
                "response_kind": "option",
                "option_id": "allow_temporary",
                "value": "Allow network access for 10 minutes",
            },
        },
    )
    state = {"sandbox": {"sandbox_id": "box-1"}, "messages": [response]}
    runtime = SimpleNamespace(context=_context("run-decided"))

    SandboxMiddleware()._apply_network_policy_response(state, runtime)

    assert provider.decisions == [("box-1", "req-1", "allow_temporary")]
    recorded = _diagnostics("run-decided")
    assert [obs.kind for obs in recorded] == ["egress.decided"]
    assert recorded[0].facts == {"decision": "allow_temporary", "request_ref": "req-1", "ttl_seconds": 600}
    discard_sandbox_diagnostics("run-decided")


@pytest.mark.parametrize(("context_extra", "reason"), [({"non_interactive": True}, "non_interactive"), ({"is_subagent": True}, "subagent")])
def test_unasked_denial_is_recorded_once_per_sandbox(provider, context_extra, reason) -> None:
    run_id = f"run-denied-{reason}"
    discard_sandbox_diagnostics(run_id)
    state = {"sandbox": {"sandbox_id": "box-1"}}
    original = ToolMessage(content="proxy denied", tool_call_id="call-1", name="bash")
    middleware = SandboxMiddleware()

    for _ in range(3):
        provider.events = [dict(_EVENT)]
        assert middleware.wrap_tool_call(_request(state, _context(run_id, **context_extra)), lambda _r: original) is original

    assert provider.deny_pending_calls == ["box-1", "box-1", "box-1"]
    recorded = _diagnostics(run_id)
    assert [obs.kind for obs in recorded] == ["egress.denied"]
    assert recorded[0].facts == {"drained": True, "reason": reason}
    discard_sandbox_diagnostics(run_id)


@pytest.mark.anyio
async def test_accepted_session_is_denied_unasked_and_recorded_run_bound(provider, monkeypatch: pytest.MonkeyPatch) -> None:
    discard_sandbox_diagnostics("run-accepted")
    declaration = SimpleNamespace(kind=SandboxSessionKind.ACCEPTED, public_ref="accepted-execution-" + "a" * 64)
    bridge = SimpleNamespace(execution_evidence_digest="b" * 64, attempt_ref="attempt-1", batch_child_attempt_ref=None)
    monkeypatch.setattr("deerflow.sandbox.middleware.current_sandbox_session", lambda: declaration)
    monkeypatch.setattr(diagnostics_module, "current_sandbox_session", lambda: declaration)
    monkeypatch.setattr(diagnostics_module, "current_accepted_sandbox_bridge", lambda: bridge)
    provider.events = [dict(_EVENT)]
    state = {"sandbox": {"sandbox_id": declaration.public_ref}}
    original = ToolMessage(content="proxy denied", tool_call_id="call-1", name="bash")

    async def handler(_request):
        return original

    result = await SandboxMiddleware().awrap_tool_call(_request(state, _context("run-accepted")), handler)

    assert result is original
    assert provider.deny_pending_calls == [declaration.public_ref]
    recorded = _diagnostics("run-accepted")
    assert [obs.kind for obs in recorded] == ["egress.denied"]
    assert recorded[0].session_kind is SandboxSessionKind.ACCEPTED
    assert recorded[0].facts == {"drained": True, "reason": "accepted_session"}
    assert recorded[0].execution_evidence_digest == "b" * 64
    assert recorded[0].attempt_ref == "attempt-1"
    discard_sandbox_diagnostics("run-accepted")


class _ScopedSandbox(Sandbox):
    def __init__(self) -> None:
        super().__init__("box-1")
        self.scoped: list[tuple[str, str]] = []

    def execute_command(self, command, env=None, timeout=None):
        return "unscoped"

    def execute_command_in_scope(self, command, env=None, timeout=None, *, scope_id=None):
        self.scoped.append((command, scope_id))
        return "scoped"

    def read_file(self, path, start_line=None, end_line=None):
        return ""

    def download_file(self, path):
        return b""

    def list_dir(self, path, max_depth=2):
        return []

    def write_file(self, path, content, append=False):
        return None

    def glob(self, path, pattern, *, include_dirs=False, max_results=200):
        return [], False

    def grep(self, path, pattern, *, glob=None, literal=False, case_sensitive=False, max_results=100):
        return [], False

    def update_file(self, path, content):
        return None


def test_scoped_command_records_the_scope_once() -> None:
    discard_sandbox_diagnostics("run-scope")
    sandbox = _ScopedSandbox()
    runtime = SimpleNamespace(context=_context("run-scope", sandbox_id="box-1", sandbox_command_scope_id="owner-7"))

    assert _execute_bash_command(sandbox, "echo 1", runtime=runtime, env=None) == "scoped"
    assert _execute_bash_command(sandbox, "echo 2", runtime=runtime, env=None) == "scoped"

    assert sandbox.scoped == [("echo 1", "owner-7"), ("echo 2", "owner-7")]
    recorded = _diagnostics("run-scope")
    assert [obs.kind for obs in recorded] == ["scope.opened"]
    assert recorded[0].facts == {"scope_ref": "owner-7"}
    unscoped = SimpleNamespace(context=_context("run-scope", sandbox_id="box-1"))
    assert _execute_bash_command(sandbox, "echo 3", runtime=unscoped, env=None) == "unscoped"
    assert len(_diagnostics("run-scope")) == 1
    discard_sandbox_diagnostics("run-scope")


@pytest.mark.anyio
async def test_worker_publishes_both_kinds_of_diagnostics_as_run_events() -> None:
    from unittest.mock import AsyncMock, MagicMock

    from deerflow.runtime.events.store.memory import MemoryRunEventStore
    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.store.memory import MemoryRunStore
    from deerflow.runtime.runs.worker import RunContext, run_agent
    from deerflow.runtime.tenant_identity import TenantIdentityV1
    from deerflow.sandbox.diagnostics import record_sandbox_diagnostic

    tenant = TenantIdentityV1.from_canonical_id("local").to_persisted_reference()
    store = MemoryRunStore()
    event_store = MemoryRunEventStore(run_store=store, tenant=tenant)
    run_manager = RunManager(store=store, event_store=event_store, tenant=tenant)
    record = await run_manager.create("thread-diagnostics")
    provider = MagicMock()
    provider.get.return_value = None

    class RecordingAgent:
        async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
            del graph_input, stream_mode, subgraphs
            context = config["configurable"]["__pregel_runtime"].context
            context["sandbox_id"] = "box-ordinary"
            for n in range(3):
                assert record_sandbox_diagnostic(context, "egress.blocked", facts={"host": "example.com", "port": 443, "request_ref": f"req-{n}"}) is not None
            yield {"messages": []}

    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())
    set_sandbox_provider(provider)
    try:
        await run_agent(
            bridge,
            run_manager,
            record,
            ctx=RunContext(checkpointer=None, event_store=event_store, tenant=tenant),
            agent_factory=lambda **_kwargs: RecordingAgent(),
            graph_input={},
            config={},
        )
    finally:
        reset_sandbox_provider()

    events = await event_store.list_events(record.thread_id, record.run_id, event_types=["sandbox.diagnostic.v1"])
    assert [event["content"]["facts"]["request_ref"] for event in events] == ["req-0", "req-1", "req-2"]
    assert {event["content"]["session_kind"] for event in events} == {"ordinary"}
    assert {event["content"]["thread_id"] for event in events} == {record.thread_id}
    assert [event["metadata"]["sequence"] for event in events] == [0, 1, 2]
    assert {event["metadata"]["dropped"] for event in events} == {0}
    # The run's stream is forgotten after its final publication.
    assert len(sandbox_diagnostics(record.run_id)) == 0
    discard_sandbox_diagnostics(record.run_id)
