"""An already-built company tool rechecks the grant and current remote before every call."""

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import ToolMessage
from langchain_core.utils.function_calling import convert_to_openai_tool
from support.mcp_endpoint_stubs import EndpointStub
from test_agent_execution_runtime import execution

from deerflow.agent_instances import connections
from deerflow.agent_instances.conversations import ConnectionOrigin
from deerflow.runtime.journal import RunJournal


@pytest.mark.asyncio
async def test_redirected_and_revoked_company_connection(tmp_path, monkeypatch):
    from deerflow.agent_instances import connection_tools as runtime
    from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY

    file = tmp_path / "extensions.json"
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(file))
    monkeypatch.delenv("DEER_FLOW_TENANT_WRITTEN_ENDPOINTS", raising=False)
    bound = replace(execution(), connection_origin=ConnectionOrigin("interactive"))
    monkeypatch.setattr(type(bound), "validate", AsyncMock())
    monkeypatch.setattr(runtime, "connections_enabled", lambda: True)
    events = []

    class Journal(RunJournal):
        async def record_connection_call(self, changes):
            events.append(changes)

    ctx = {AGENT_EXECUTION_CONTEXT_KEY: bound, "__run_journal": object.__new__(Journal)}
    monkeypatch.setattr(runtime, "get_runtime", lambda: SimpleNamespace(context=ctx))
    with EndpointStub() as first, EndpointStub() as second:
        file.write_text(json.dumps({"mcpServers": {"records": {"type": "http", "url": first.url("/mcp"), "headers": {"Authorization": "Bearer sample-first"}}}}))
        [choice] = connections.list_connections(bound.instance.id, manager=True)
        connections.change_grant(bound.instance.id, choice["id"], "manager", give=True)
        tools, names = await runtime.issue_connection_tools(bound)
        assert names == [("records", True)]
        [tool] = tools
        assert first.url("/mcp") not in str(tool.model_dump())
        assert "Bearer sample-first" not in str(tool.model_dump())
        assert convert_to_openai_tool(tool)["function"]["parameters"]["properties"] == {"text": {"type": "string"}}

        async def call(call_id):
            return await tool.ainvoke({"type": "tool_call", "name": tool.name, "id": call_id, "args": {"text": "hello"}}, {})

        assert isinstance(await call("first"), ToolMessage)
        raw = json.loads(file.read_text())
        raw["mcpServers"]["records"].update(url=second.url("/mcp"), headers={"Authorization": "Bearer sample-second"})
        file.write_text(json.dumps(raw))
        assert isinstance(await call("second"), ToolMessage)
        connections.change_grant(bound.instance.id, choice["id"], "manager", give=False)
        with pytest.raises(runtime.ConnectionRefused) as refused:
            await call("third")
        assert refused.value.reason == "not-given"
        assert [(event["call_id"], event["outcome"]) for event in events] == [("first", "sending"), ("first", "made"), ("second", "sending"), ("second", "made"), ("third", "refused")]
        for service, token in ((first, "Bearer sample-first"), (second, "Bearer sample-second")):
            assert [req.headers.get("authorization") for req in service.requests if b'"tools/call"' in req.body] == [token]
            assert sum(b'"tools/call"' in req.body for req in service.requests) == 1


@pytest.mark.parametrize("marked", [False, True])
def test_compiled_graph_contacts_only_a_granted_connection(tmp_path, monkeypatch, marked):
    from test_instance_middleware_tools import build_graph

    from deerflow.agent_instances import connection_tools as runtime
    from deerflow.config.extensions_config import reset_extensions_config

    file = tmp_path / "extensions.json"
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(file))
    monkeypatch.delenv("DEER_FLOW_TENANT_WRITTEN_ENDPOINTS", raising=False)
    monkeypatch.setattr(runtime, "connections_enabled", lambda: True)
    bound = replace(execution(), connection_origin=ConnectionOrigin("interactive") if marked else None)
    reset_extensions_config()
    try:
        with EndpointStub() as selected, EndpointStub() as other:
            file.write_text(json.dumps({"mcpServers": {"records": {"type": "http", "url": selected.url("/mcp")}, "other": {"type": "http", "url": other.url("/mcp")}}}))
            row = next(row for row in connections.list_connections(bound.instance.id, manager=True) if row["name"] == "records")
            connections.change_grant(bound.instance.id, row["id"], "manager", give=True)
            graph, chain = build_graph(monkeypatch, bound=bound)
            tools = graph.get_graph().nodes["tools"].data.tools_by_name
            assert ("records_echo" in tools) is marked
            assert ("records_echo" in chain[0].allowed_tool_names) is marked
            assert bool(selected.requests) is marked
            assert other.requests == []
    finally:
        reset_extensions_config()


@pytest.mark.asyncio
async def test_change_while_sending_receipt_is_persisted(tmp_path, monkeypatch):
    from deerflow.agent_instances import connection_tools as runtime
    from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY

    file = tmp_path / "extensions.json"
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(file))
    monkeypatch.delenv("DEER_FLOW_TENANT_WRITTEN_ENDPOINTS", raising=False)
    monkeypatch.setattr(runtime, "connections_enabled", lambda: True)
    bound = replace(execution(), connection_origin=ConnectionOrigin("interactive"))
    monkeypatch.setattr(type(bound), "validate", AsyncMock())
    outcomes = []

    class Journal(RunJournal):
        async def record_connection_call(self, event):
            outcomes.append(event["outcome"])
            if event["outcome"] == "sending":
                connections.change_grant(bound.instance.id, choice["id"], "manager", give=False)

    ctx = {AGENT_EXECUTION_CONTEXT_KEY: bound, "__run_journal": object.__new__(Journal)}
    monkeypatch.setattr(runtime, "get_runtime", lambda: SimpleNamespace(context=ctx))
    with EndpointStub() as stub:
        file.write_text(json.dumps({"mcpServers": {"records": {"type": "http", "url": stub.url("/mcp")}}}))
        [choice] = connections.list_connections(bound.instance.id, manager=True)
        connections.change_grant(bound.instance.id, choice["id"], "manager", give=True)
        [tool], _ = await runtime.issue_connection_tools(bound)
        with pytest.raises(runtime.ConnectionRefused, match="took away"):
            await tool.ainvoke({"type": "tool_call", "name": tool.name, "id": "stopped", "args": {"text": "hello"}})
        assert outcomes == ["sending", "refused"]
        assert all(b'"tools/call"' not in req.body for req in stub.requests)


def test_final_inventory_rejects_an_unissued_tool(monkeypatch):
    from test_instance_middleware_tools import build_graph, format_heading

    from deerflow.agent_instances import connection_tools as runtime
    from deerflow.tools.mcp_metadata import tag_mcp_tool

    bound = replace(execution(), connection_origin=ConnectionOrigin("interactive"))
    wrong = tag_mcp_tool(format_heading.model_copy(deep=True), server_name="unselected", transport="http")
    monkeypatch.setattr(runtime, "build_connection_inventory", lambda _: ([wrong], [("unselected", True)]))
    with pytest.raises(RuntimeError, match="company connection"):
        build_graph(monkeypatch, bound=bound)


@pytest.mark.asyncio
async def test_unavailable_feature_omits_connection_guidance(monkeypatch):
    from deerflow.agent_instances import connection_tools as runtime

    monkeypatch.setattr(runtime, "connections_enabled", lambda: False)
    assert await runtime.issue_connection_tools(execution()) == ([], None)


@pytest.mark.parametrize("deferred", [False, True])
def test_compiled_graph_retains_host_tool_and_defers_only_company_tool(tmp_path, monkeypatch, caplog, deferred):
    from test_instance_middleware_tools import HeadingMiddleware, build_graph
    from test_subagent_declared_tool_policy import _app_config

    from deerflow.agent_instances import connection_tools as runtime
    from deerflow.agents.lead_agent import agent
    from deerflow.config.extensions_config import reset_extensions_config
    from deerflow.sandbox.tools import read_file_tool
    from deerflow.skills.catalog import SkillCatalog
    from deerflow.skills.describe import SkillSearchSetup, build_describe_skill_tool

    class Catalog(EndpointStub):
        @staticmethod
        def _answer(message):
            reply = EndpointStub._answer(message)
            if isinstance(message, dict) and message.get("method") == "tools/list":
                schema = reply["result"]["tools"][0]
                reply["result"]["tools"] = [dict(schema, name=name) for name in ("echo", "read_file", "describe_skill", "format_heading", "show_web_page", "tool_search")]
            return reply

    file = tmp_path / "extensions.json"
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(file))
    monkeypatch.delenv("DEER_FLOW_TENANT_WRITTEN_ENDPOINTS", raising=False)
    monkeypatch.setattr(runtime, "connections_enabled", lambda: True)
    bound = replace(execution(), connection_origin=ConnectionOrigin("interactive"))
    cfg = _app_config()
    cfg.tool_search.enabled = deferred
    monkeypatch.setattr("deerflow.tools.get_available_tools", lambda **kwargs: [read_file_tool])
    describe = build_describe_skill_tool(SkillCatalog(()))
    monkeypatch.setattr("deerflow.skills.describe.build_skill_search_setup", lambda *args, **kwargs: SkillSearchSetup(describe, frozenset()))
    host_stack = agent.build_middlewares
    monkeypatch.setattr(agent, "build_middlewares", lambda *args, **kwargs: [*host_stack(*args, **kwargs), HeadingMiddleware()])
    reset_extensions_config()
    try:
        with Catalog() as stub:
            file.write_text(json.dumps({"mcpServers": {"notes": {"type": "http", "url": stub.url("/mcp"), "tool_name_prefix": False}}}))
            [row] = connections.list_connections(bound.instance.id, manager=True)
            connections.change_grant(bound.instance.id, row["id"], "manager", give=True)
            with caplog.at_level("INFO", logger=runtime.__name__):
                graph, middlewares = build_graph(monkeypatch, bound=bound, app_config=cfg)
            tools = graph.get_graph().nodes["tools"].data.tools_by_name
            assert isinstance(tools["echo"], runtime.InstanceConnectionTool)
            for host_name in ("show_web_page", "read_file", "describe_skill", "format_heading"):
                assert not isinstance(tools[host_name], runtime.InstanceConnectionTool)
                assert "conflicting instance connection tool " + host_name in caplog.text
            assert ("tool_search" in tools) is deferred
            assert not isinstance(tools.get("tool_search"), runtime.InstanceConnectionTool)
            assert any("echo" in getattr(layer, "_deferred", ()) for layer in middlewares) is deferred
    finally:
        reset_extensions_config()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        ("revoke", "not-given"),
        ("delete", "removed"),
        ("disable", "switched-off"),
        ("tenant-off", "not-available"),
        ("bad-file", "not-available"),
        ("per-person", "per-person-credentials"),
        ("per-request", "per-request-credentials"),
        ("agent", "agent-credential"),
        ("helper", "not-available"),
        ("missing-journal", "audit-unavailable"),
        ("authority-unavailable", "not-available"),
    ],
)
async def test_live_tool_rejects_changed_authority_without_another_request(tmp_path, monkeypatch, mutation, reason):
    from deerflow.agent_instances import connection_tools as runtime
    from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY

    file = tmp_path / "extensions.json"
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(file))
    monkeypatch.delenv("DEER_FLOW_TENANT_WRITTEN_ENDPOINTS", raising=False)
    monkeypatch.setattr(runtime, "connections_enabled", lambda: True)
    bound = replace(execution(), connection_origin=ConnectionOrigin("interactive"))
    monkeypatch.setattr(type(bound), "validate", AsyncMock())
    events = []

    class Journal(RunJournal):
        async def record_connection_call(self, changes):
            events.append(changes)

    ctx = {AGENT_EXECUTION_CONTEXT_KEY: bound, "__run_journal": object.__new__(Journal)}
    monkeypatch.setattr(runtime, "get_runtime", lambda: SimpleNamespace(context=ctx))
    with EndpointStub() as stub:
        file.write_text(json.dumps({"mcpServers": {"notes": {"type": "http", "url": stub.url("/mcp")}}}))
        [row] = connections.list_connections(bound.instance.id, manager=True)
        connections.change_grant(bound.instance.id, row["id"], "manager", give=True)
        [tool], _ = await runtime.issue_connection_tools(bound)
        before = stub.calls()
        raw = json.loads(file.read_text())
        config = raw["mcpServers"]["notes"]
        if mutation == "revoke":
            connections.change_grant(bound.instance.id, row["id"], "manager", give=False)
        elif mutation == "tenant-off":
            monkeypatch.setattr(runtime, "connections_enabled", lambda: False)
        elif mutation == "bad-file":
            file.write_text("{")
        elif mutation == "helper":
            ctx["is_subagent"] = True
        elif mutation == "missing-journal":
            ctx.pop("__run_journal")
        elif mutation == "authority-unavailable":
            monkeypatch.setattr(type(bound), "validate", AsyncMock(side_effect=RuntimeError("private authority diagnostic")))
        else:
            if mutation == "delete":
                del raw["mcpServers"]["notes"]
            elif mutation == "disable":
                config["enabled"] = False
            elif mutation == "agent":
                config["agent_auth"] = True
            elif mutation == "per-request":
                config["headers_from_context"] = {"enabled": True, "headers": {"X-Owner": "owner"}}
            else:
                config["user_auth"] = {"enabled": True}
            file.write_text(json.dumps(raw))
        with pytest.raises(runtime.ConnectionRefused) as refused:
            await tool.ainvoke({"type": "tool_call", "name": tool.name, "id": "held", "args": {"text": "hello"}})
        assert refused.value.reason == reason
        assert stub.calls() == before
        if mutation not in {"helper", "missing-journal"}:
            assert [(event["outcome"], event["reason"]) for event in events] == [("refused", reason)]


@pytest.mark.asyncio
@pytest.mark.parametrize("problem", ["timeout", "slow-close", "auth", "error", "receipt"])
async def test_failed_call_uses_fixed_guidance_and_one_attempt(tmp_path, monkeypatch, problem):
    import asyncio

    import httpx
    from langchain_core.tools import StructuredTool

    from deerflow.agent_instances import connection_tools as runtime
    from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
    from deerflow.mcp import cache

    file = tmp_path / "extensions.json"
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(file))
    monkeypatch.setattr(runtime, "connections_enabled", lambda: True)
    bound = replace(execution(), connection_origin=ConnectionOrigin("package", "calendar"))
    monkeypatch.setattr(type(bound), "validate", AsyncMock())
    entries, attempts = [], []
    release, closing = asyncio.Event(), asyncio.Event()

    class Journal(RunJournal):
        async def record_connection_call(self, changes):
            if problem == "receipt":
                raise OSError("store not writable")
            entries.append(changes)

    ctx = {AGENT_EXECUTION_CONTEXT_KEY: bound, "__run_journal": object.__new__(Journal)}
    monkeypatch.setattr(runtime, "get_runtime", lambda: SimpleNamespace(context=ctx))
    private_value = "example-private-path"
    file.write_text(json.dumps({"mcpServers": {"notes": {"type": "http", "url": "https://example.test/" + private_value, "tool_call_timeout": 0.015}}}))
    [row] = connections.list_connections(bound.instance.id, manager=True)
    connections.change_grant(bound.instance.id, row["id"], "manager", give=True)

    async def answer(text: str):
        attempts.append(text)
        if problem in {"timeout", "slow-close"}:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                if problem == "slow-close":
                    closing.set()
                    await release.wait()
                raise
        if problem == "auth":
            request = httpx.Request("POST", "https://example.test/" + private_value)
            raise httpx.HTTPStatusError(private_value, request=request, response=httpx.Response(401, request=request))
        raise ValueError(private_value)

    actual = StructuredTool.from_function(coroutine=answer, name="notes_echo", description="Echo a note.")

    async def lookup(name, *, expected_definition):
        return cache.CompanyServerTools(tools=(actual,), definition=expected_definition)

    monkeypatch.setattr(cache, "get_company_server_tools", lookup)
    [tool], _ = await runtime.issue_connection_tools(bound)
    request = {"type": "tool_call", "name": tool.name, "id": "one-call", "args": {"text": "hello"}}
    action = asyncio.create_task(tool.ainvoke(request))
    try:
        if problem == "slow-close":
            await asyncio.wait_for(closing.wait(), 3)
            done, _ = await asyncio.wait({action}, timeout=0.3)
            assert done, "the model remained blocked on the remote transport shutdown"
        with pytest.raises(runtime.ConnectionRefused) as refused:
            await action
    finally:
        release.set()
        await asyncio.gather(action, return_exceptions=True)
    reason = {"timeout": "timed-out", "slow-close": "timed-out", "auth": "refused", "error": "error", "receipt": "audit-unavailable"}[problem]
    assert refused.value.reason == reason
    assert private_value not in str(refused.value)
    if problem == "receipt":
        assert not attempts and not entries
    else:
        assert attempts == ["hello"]
        assert [(e["outcome"], e["reason"], e["package"], e["call_id"]) for e in entries] == [("sending", None, "calendar", "one-call"), ("failed", reason, "calendar", "one-call")]
    assert private_value not in str(entries)


def test_an_inventory_from_a_prior_grant_cannot_be_bound_again(tmp_path, monkeypatch):
    from deerflow.agent_instances import connection_tools as runtime
    from deerflow.tools.mcp_metadata import tag_mcp_tool

    bound = replace(execution(), connection_origin=ConnectionOrigin("interactive"))
    monkeypatch.setattr(runtime, "connections_enabled", lambda: True)
    file = tmp_path / "extensions.json"
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(file))
    file.write_text(json.dumps({"mcpServers": {"notes": {"type": "http", "url": "https://example.test"}}}))
    [row] = connections.list_connections(bound.instance.id, manager=True)
    connections.change_grant(bound.instance.id, row["id"], "manager", give=True)
    [granted] = connections.granted_connections(bound.instance.id)
    tool = tag_mcp_tool(runtime.InstanceConnectionTool(name="notes_echo", description="Echo.", args_schema={}, instance_id=bound.instance.id, connection_id=granted.binding, connection_name=granted.name, remote_name="echo"))
    runtime.verify_connection_inventory(bound, [], [SimpleNamespace(tools=[tool])])
    with pytest.raises(RuntimeError, match="company connection"):
        runtime.verify_connection_inventory(bound, [tool], [], helper=True)
    connections.change_grant(bound.instance.id, row["id"], "manager", give=False)
    with pytest.raises(RuntimeError, match="company connection"):
        runtime.verify_connection_inventory(bound, [], [SimpleNamespace(tools=[tool])])


@pytest.mark.asyncio
@pytest.mark.parametrize(("interrupt", "synchronous"), [(False, False), (True, False), (False, True)])
async def test_remote_observes_committed_sending_event_before_it_answers(tmp_path, monkeypatch, interrupt, synchronous):
    import asyncio
    import sqlite3
    import threading

    from langchain_core.messages import AIMessage
    from langgraph.graph import END, START, MessagesState, StateGraph
    from langgraph.prebuilt import ToolNode
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from deerflow.agent_instances import connection_tools as runtime
    from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
    from deerflow.persistence.models.run_event import RunEventRow, RunEventThreadSeqRow
    from deerflow.runtime.events.store.db import DbRunEventStore

    database = tmp_path / "journal.db"
    file = tmp_path / "extensions.json"
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(file))
    monkeypatch.delenv("DEER_FLOW_TENANT_WRITTEN_ENDPOINTS", raising=False)
    monkeypatch.setattr(runtime, "connections_enabled", lambda: True)
    bound = replace(execution(), connection_origin=ConnectionOrigin("assignment"), owner_loop=asyncio.get_running_loop())
    monkeypatch.setattr(type(bound), "validate", AsyncMock())
    received, release = threading.Event(), threading.Event()
    observed = []

    class Observer(EndpointStub):
        def _answer(self, message):
            if message.get("method") == "tools/call":
                with sqlite3.connect(database) as sql:
                    rows = sql.execute("SELECT content FROM run_events WHERE thread_id=? AND run_id=? AND event_type=?", ("chat", "run", "middleware:connection_call")).fetchall()
                observed.extend(json.loads(row[0])["changes"]["outcome"] for row in rows)
                received.set()
                if interrupt:
                    release.wait(10)
            return super()._answer(message)

    engine = create_async_engine(f"sqlite+aiosqlite:///{database}")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(lambda sync: RunEventRow.metadata.create_all(sync, tables=[RunEventRow.__table__, RunEventThreadSeqRow.__table__]))
        store = DbRunEventStore(async_sessionmaker(engine, expire_on_commit=False))
        journal = RunJournal(thread_id="chat", run_id="run", user_id="requester", event_store=store)
        ctx = {AGENT_EXECUTION_CONTEXT_KEY: bound, "__run_journal": journal}
        with Observer() as stub:
            file.write_text(json.dumps({"mcpServers": {"notes": {"type": "http", "url": stub.url("/mcp")}}}))
            [row] = connections.list_connections(bound.instance.id, manager=True)
            connections.change_grant(bound.instance.id, row["id"], "manager", give=True)
            [tool], _ = await runtime.issue_connection_tools(bound)
            workflow = StateGraph(MessagesState, context_schema=dict)
            workflow.add_node("tools", ToolNode([tool], handle_tool_errors=False))
            workflow.add_edge(START, "tools")
            workflow.add_edge("tools", END)
            request = AIMessage(content="", tool_calls=[{"name": tool.name, "id": "action", "args": {"text": "hello"}}])
            graph = workflow.compile()
            body = {"messages": [request]}
            call = asyncio.create_task(asyncio.to_thread(graph.invoke, body, context=ctx) if synchronous else graph.ainvoke(body, context=ctx))
            try:
                assert await asyncio.to_thread(received.wait, 5)
                assert observed == ["sending"]
                if interrupt:
                    call.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await call
                else:
                    assert isinstance((await call)["messages"][-1], ToolMessage)
            finally:
                release.set()
                if not call.done():
                    call.cancel()
                await asyncio.gather(call, return_exceptions=True)
        events = await store.list_events("chat", "run", user_id="requester")
        outcomes = [event["content"]["changes"]["outcome"] for event in events]
        assert outcomes == (["sending"] if interrupt else ["sending", "made"])
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_same_definition_relocated_during_receipt_refuses_the_old_tool(tmp_path, monkeypatch):
    from deerflow.agent_instances import connection_tools as runtime
    from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY

    file = tmp_path / "extensions.json"
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(file))
    monkeypatch.delenv("DEER_FLOW_TENANT_WRITTEN_ENDPOINTS", raising=False)
    monkeypatch.setattr(runtime, "connections_enabled", lambda: True)
    bound = replace(execution(), connection_origin=ConnectionOrigin("interactive"))
    monkeypatch.setattr(type(bound), "validate", AsyncMock())
    outcomes = []

    class Journal(RunJournal):
        async def record_connection_call(self, changes):
            outcomes.append(changes["outcome"])
            if changes["outcome"] == "sending":
                registry = json.loads(file.read_text())
                registry["mcpServers"]["renamed"] = registry["mcpServers"].pop("original")
                file.write_text(json.dumps(registry))

    monkeypatch.setattr(runtime, "get_runtime", lambda: SimpleNamespace(context={AGENT_EXECUTION_CONTEXT_KEY: bound, "__run_journal": object.__new__(Journal)}))
    with EndpointStub() as stub:
        file.write_text(json.dumps({"mcpServers": {"original": {"type": "http", "url": stub.url("/mcp")}}}))
        [row] = connections.list_connections(bound.instance.id, manager=True)
        connections.change_grant(bound.instance.id, row["id"], "manager", give=True)
        [tool], _ = await runtime.issue_connection_tools(bound)
        with pytest.raises(runtime.ConnectionRefused) as refused:
            await tool.ainvoke({"type": "tool_call", "name": tool.name, "id": "rename", "args": {"text": "hello"}})
        assert refused.value.reason == "changed"
        assert outcomes == ["sending", "refused"]
        assert all(b'"tools/call"' not in req.body for req in stub.requests)
