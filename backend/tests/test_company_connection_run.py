"""Host-origin admission, protected package attribution and durable connection events."""

import asyncio
from types import SimpleNamespace

import pytest
from langchain_core.messages import HumanMessage
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from deerflow.runtime.journal import RunJournal


@pytest.mark.parametrize(
    ("host", "work", "listed", "installed", "kind"),
    [
        (None, False, [], [], None),
        (None, True, [], [], "assignment"),
        ({"kind": "extension", "namespace": "calendar"}, False, ["calendar"], ["calendar"], "package"),
        ({"kind": "extension", "namespace": "calendar"}, False, [], ["calendar"], None),
        ({"kind": "extension", "namespace": "calendar"}, False, ["calendar", "missing"], ["calendar"], "package"),
        ({"kind": "extension", "namespace": "calendar"}, False, "calendar", ["calendar"], None),
        ({"kind": "extension"}, False, ["calendar"], ["calendar"], None),
        ({"kind": "schedule"}, False, ["calendar"], ["calendar"], None),
        ({"kind": "unrecognized"}, False, [], [], None),
        ({"kind": "im_channel", "provider": "slack"}, False, [], [], None),
    ],
)
def test_admitted_origin_uses_host_and_installed_package_list(host, work, listed, installed, kind):
    from app.gateway.company_connection_runs import admitted_connection_origin
    from app.gateway.run_origin import resolve_request_origin

    request = SimpleNamespace(state=SimpleNamespace(run_origin=host))
    config = SimpleNamespace(agent_connections=SimpleNamespace(packages=listed))
    extensions = SimpleNamespace(plugins=[(None, SimpleNamespace(namespace=name)) for name in installed])
    origin = admitted_connection_origin(request, {}, resolve_request_origin(request, {}), assignment=work, config=config, extensions=extensions)
    assert (origin.kind if origin else None) == kind
    if kind == "package":
        assert origin.package == "calendar"


def test_package_label_and_attestation_bind_only_visible_human_input():
    from app.gateway.company_connection_runs import stamp_package_turn
    from app.gateway.services import normalize_input, strip_server_owned_state_metadata

    sample = {"role": "user", "id": "sample", "content": "Schedule the review", "additional_kwargs": {"deerflow_package_origin": {"package": "old field"}}}
    assert "deerflow_package_origin" not in normalize_input({"messages": [sample]})["messages"][0].additional_kwargs
    assert "deerflow_package_origin" not in strip_server_owned_state_metadata({"messages": [sample]})["messages"][0].additional_kwargs
    messages = [HumanMessage(content="Schedule the review", id="sample"), HumanMessage(content="internal", additional_kwargs={"hide_from_ui": True})]
    graph = {"messages": messages}
    metadata = {}
    stamp_package_turn(graph, metadata, package="calendar", user_id="alice", run_id="run-a")
    candidate = graph["messages"][0].additional_kwargs["deerflow_package_origin"]
    assert candidate == {"package": "calendar", "user_id": "alice", "run_id": "run-a"}
    row_id = graph["messages"][0].id
    assert row_id and row_id != "sample"
    assert metadata["deerflow_package_turn"] == {"run_id": "run-a", "message_ids": [row_id]}
    assert "deerflow_package_origin" not in graph["messages"][1].additional_kwargs
    assert "deerflow_package_origin" not in messages[0].additional_kwargs


@pytest.mark.asyncio
async def test_connection_event_commits_from_worker_loop_before_return(tmp_path, caplog):
    from deerflow.persistence.models.run_event import RunEventRow, RunEventThreadSeqRow
    from deerflow.runtime.events.store.db import DbRunEventStore

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'journal.sqlite'}")
    try:
        async with engine.begin() as conn:
            for table in (RunEventRow.__table__, RunEventThreadSeqRow.__table__):
                await conn.run_sync(table.create)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        store = DbRunEventStore(sessions)
        journal = RunJournal(thread_id="thread", run_id="run", event_store=store, user_id="alice")
        changes = {"call_id": "call", "connection_id": "c", "connection_name": "calendar", "instance_id": "worker", "origin": "message", "outcome": "sending", "reason": None}
        caplog.set_level("INFO", logger="deerflow.runtime.journal")
        await asyncio.to_thread(lambda: asyncio.run(journal.record_connection_call(changes)))
        other = DbRunEventStore(sessions)
        rows = await other.list_events("thread", "run", user_id="alice")
        assert [(row["event_type"], row["content"]["changes"]) for row in rows] == [("middleware:connection_call", changes)]
        await journal.record_connection_call({**changes, "outcome": "made"})
        assert len(await other.list_events("thread", "run", user_id="alice")) == 2
        records = [record for record in caplog.records if record.getMessage().startswith("Company connection call recorded")]
        assert [(record.run_id, record.thread_id, record.user_id, record.connection_id, record.connection_name, record.instance_id, record.call_id, record.origin, record.outcome, record.reason) for record in records] == [
            ("run", "thread", "alice", "c", "calendar", "worker", "call", "message", outcome, None) for outcome in ("sending", "made")
        ]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_non_durable_journal_never_acknowledges_a_send():
    from deerflow.runtime.events.store.memory import MemoryRunEventStore

    journal = RunJournal(thread_id="thread", run_id="run", event_store=MemoryRunEventStore())
    with pytest.raises(RuntimeError):
        await journal.record_connection_call({"call_id": "c", "outcome": "sending"})


@pytest.mark.asyncio
@pytest.mark.parametrize("target", [None, {"kind": "extension", "namespace": "cal"}, {"kind": "extension", "namespace": "other"}])
async def test_package_input_requires_its_own_employee_run(monkeypatch, target):
    from app.gateway import agent_conversations, company_connection_runs

    async def bound(*args, **kwargs):
        return True

    monkeypatch.setattr(agent_conversations, "require_conversation", bound)
    caller = {"kind": "extension", "namespace": "cal"}
    request = SimpleNamespace(state=SimpleNamespace(run_origin=caller))
    if target == caller:
        assert await company_connection_runs.package_input_source(request, "thread", {"deerflow_origin": target}) == "cal"
    else:
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as error:
            await company_connection_runs.package_input_source(request, "thread", {"deerflow_origin": target})
        assert error.value.status_code == 403


@pytest.mark.parametrize("name", ["create_run", "stream_run", "wait_run", "stateless_stream", "stateless_wait"])
def test_public_interactive_run_requires_the_authenticated_gateway_route(name):
    from fastapi import Request

    from app.gateway.company_connection_runs import admitted_connection_origin
    from app.gateway.routers import runs, thread_runs

    router = runs.router if name.startswith("stateless") else thread_runs.router
    route = next(item for item in router.routes if item.name == name)
    request = Request({"type": "http", "route": route, "state": {"auth_source": "session"}, "method": "POST", "path": "/", "headers": []})
    assert admitted_connection_origin(request, {}, None, assignment=False, config=None, extensions=None).kind == "message"
    request.state.auth_source = "pat"
    assert admitted_connection_origin(request, {}, None, assignment=False, config=None, extensions=None) is None
    request.state.auth_source = "internal"
    assert admitted_connection_origin(request, {}, None, assignment=False, config=None, extensions=None) is None
    assert admitted_connection_origin(request, {}, None, assignment=False, config=None, extensions=None, telegram=True).kind == "message"


def test_only_host_named_package_origin_grants_authority():
    from app.gateway.company_connection_runs import admitted_connection_origin

    origin = {"kind": "extension", "namespace": "calendar"}
    request = SimpleNamespace(state=SimpleNamespace(auth_source="internal"))
    cfg = SimpleNamespace(agent_connections=SimpleNamespace(packages=["calendar"]))
    extensions = SimpleNamespace(plugins=[(None, SimpleNamespace(namespace="calendar"))])
    assert admitted_connection_origin(request, {"deerflow_origin": origin}, origin, assignment=False, config=cfg, extensions=extensions) is None


def test_all_known_host_origin_kinds_have_an_explicit_admission_decision():
    from app.gateway.company_connection_runs import admitted_connection_origin
    from deerflow.runtime.run_origin import ORIGIN_KINDS

    decisions = {"extension": None, "github": None, "im_channel": None, "mcp_notification": None, "schedule": None, "subagent_batch": "batch_followup"}
    assert decisions.keys() == ORIGIN_KINDS
    for key, expected in decisions.items():
        origin = {"kind": key}
        request = SimpleNamespace(state=SimpleNamespace(run_origin=origin, durable_batch_followup=key == "subagent_batch"))
        actual = admitted_connection_origin(request, {}, origin, assignment=False, config=None, extensions=None)
        assert (actual.kind if actual else None) == expected


def test_host_run_launch_sites_have_reviewed_origins():
    import ast
    from pathlib import Path

    gateway = Path(__file__).resolve().parents[1] / "app" / "gateway"
    expected = {
        ("extension_agent_runs.py", "_start", "create_run"),
        ("extension_agent_runs.py", "start_batch_followup", "start_run"),
        ("run_inputs.py", "dispatch_one_input", "create_run"),
        ("work_execution.py", "perform", "start_run"),
        ("services.py", "launch_scheduled_thread_run", "start_run"),
        ("services.py", "launch_mcp_task_notification_run", "start_run"),
        ("routers/thread_runs.py", "create_run", "start_run"),
        ("routers/thread_runs.py", "stream_run", "start_run"),
        ("routers/thread_runs.py", "wait_run", "start_run"),
        ("routers/runs.py", "stateless_stream", "start_run"),
        ("routers/runs.py", "stateless_wait", "start_run"),
    }
    observed = []
    for path in (gateway / "..").resolve().rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        if "start_run" not in source and "create_run" not in source:
            continue
        tree = ast.parse(source)
        aliases = {"start_run": "start_run", "create_run": "create_run"}
        for imported in ast.walk(tree):
            if isinstance(imported, ast.ImportFrom) and imported.module in {"app.gateway.services", "app.gateway.routers.thread_runs"}:
                aliases.update({entry.asname or entry.name: entry.name for entry in imported.names if entry.name in {"start_run", "create_run"}})
        for node in ast.walk(tree):
            if not isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
                continue
            todo = list(node.body)
            while todo:
                child = todo.pop()
                if isinstance(child, (ast.AsyncFunctionDef, ast.FunctionDef, ast.ClassDef)):
                    continue
                if isinstance(child, ast.Call):
                    called = child.func
                    if isinstance(called, ast.Name):
                        name = aliases.get(called.id)
                    else:
                        name = called.attr if isinstance(called, ast.Attribute) and isinstance(called.value, ast.Name) and called.value.id in {"services", "thread_runs"} else None
                    if name in {"start_run", "create_run"}:
                        observed.append((str(path.relative_to(gateway.parent)).removeprefix("gateway/"), node.name, name))
                todo.extend(ast.iter_child_nodes(child))
    assert sorted(observed) == sorted(expected)


def test_telegram_requires_qualified_inbound_receipt_and_matching_provider():
    from app.gateway.company_connection_runs import admitted_connection_origin

    request = SimpleNamespace(state=SimpleNamespace(auth_source="internal"))
    telegram = {"kind": "im_channel", "provider": "telegram"}
    assert admitted_connection_origin(request, {}, telegram, assignment=False, config=None, extensions=None, telegram=True).kind == "message"
    assert admitted_connection_origin(request, {}, telegram, assignment=False, config=None, extensions=None) is None
    assert admitted_connection_origin(request, {}, {"kind": "im_channel", "provider": "slack"}, assignment=False, config=None, extensions=None, telegram=True) is None


@pytest.mark.parametrize(("source", "allowed"), [("session", True), ("auth_disabled", True), ("internal", False), ("pat", False), (None, False)])
def test_queued_human_requires_authenticated_current_session(source, allowed):
    from app.gateway.company_connection_runs import admitted_connection_origin

    request = SimpleNamespace(state=SimpleNamespace(auth_source=source, admitted_queued_human=True))
    origin = admitted_connection_origin(request, {}, None, assignment=False, config=None, extensions=None)
    assert (origin.kind if origin else None) == ("message" if allowed else None)
