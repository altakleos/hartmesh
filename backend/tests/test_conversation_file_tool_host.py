"""An explicit reference can authorize ordinary chat attachment tools on its owning run."""

import asyncio
import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from _router_auth_helpers import _make_stub_user
from fastapi import Request
from langchain_core.messages import ToolMessage
from test_conversation_file_import import OUTPUT, site  # noqa: F401

from deerflow.config.tool_output_config import ToolOutputConfig


@pytest.fixture()
def host(site, monkeypatch):  # noqa: F811
    from app.gateway import conversation_file_tools as host
    from app.gateway.authz import AuthContext

    permissions = {"threads:write", "threads:read", "runs:read"}
    user = _make_stub_user().model_copy(update={"id": UUID(site.owner), "token_version": 7})
    mode = SimpleNamespace(user=user, permissions=permissions)

    async def live():
        return mode.user

    async def live_user(uid):
        assert uid == site.owner
        return await live()

    async def roles(*args, **kwargs):
        return list(mode.permissions)

    monkeypatch.setattr(host, "get_local_provider", lambda: SimpleNamespace(get_user=live_user))
    monkeypatch.setattr(host, "resolve_route_permissions", roles)
    monkeypatch.setattr(host, "account_refusal", lambda user: None)
    site.record["created_at"] = "2026-01-02T00:00:00Z"
    site.app.state.run_store.list_by_thread = AsyncMock(return_value=[site.record])
    app = site.app
    req = Request({"type": "http", "app": app, "state": {}, "method": "POST", "headers": [], "path": "/", "query_string": b""})
    req.state.user = user
    req.state.auth_source = "session"
    req.state.auth = AuthContext(user, list(permissions))
    site.rows["earlier"]["display_name"] = "Earlier report"
    opts = dict(
        request=req,
        source_ids=("earlier",),
        reader=lambda: None,
        destination=site.rows["later"],
        run=SimpleNamespace(user_id=site.owner, thread_id="later", abort_event=asyncio.Event(), status="running"),
        config=SimpleNamespace(uploads=SimpleNamespace(auto_convert_documents=False), tool_output=ToolOutputConfig()),
    )

    def prepare():
        return site.client.portal.call(lambda: host.bind_conversation_files(**opts))

    def call(fn, **kwargs):
        return site.client.portal.call(lambda: fn(**kwargs))

    return SimpleNamespace(site=site, mode=mode, req=req, opts=opts, prepare=prepare, call=call)


def test_bound_capability_finds_then_copies_only_a_presented_file(host):
    access = host.prepare()
    page = host.call(access.list_files, thread_id="earlier")
    assert [(f["run_id"], f["path"], f["available"]) for f in page["files"]] == [("answer", OUTPUT, True), ("answer", "/mnt/user-data/outputs/gone.txt", False)]
    assert not page["has_more"] and page["next_offset"] is None
    args = dict(thread_id="earlier", run_id="answer", path=OUTPUT)
    receipt = host.call(access.import_file, **args)
    assert receipt.filename == "notes.txt" and receipt.source_title == "Earlier report"
    assert receipt.virtual_path == "/mnt/user-data/uploads/notes.txt" and receipt.sha256 == hashlib.sha256(b"meeting notes").hexdigest()
    assert host.call(access.import_file, **args) == receipt
    assert (host.site.destination / "notes.txt").read_bytes() == b"meeting notes"
    assert host.call(access.import_file, **{**args, "path": "/mnt/user-data/outputs/draft.txt"}) is None
    host.site.app.state.run_event_store.list_events.reset_mock()
    assert host.call(access.list_files, thread_id="unlisted") is None
    assert host.call(access.import_file, **{**args, "thread_id": "unlisted"}) is None
    host.site.app.state.run_event_store.list_events.assert_not_awaited()


@pytest.mark.parametrize("cut", ["owner", "source incarnation", "destination incarnation", "route permission", "token", "aborted", "terminal"])
def test_referenced_copy_refuses_changed_grants_before_io(host, cut):
    access = host.prepare()
    if cut == "owner":
        host.site.rows["earlier"]["user_id"] = "other"
    elif cut == "source incarnation":
        host.site.rows["earlier"]["incarnation"] = "replacement"
    elif cut == "destination incarnation":
        host.site.rows["later"]["incarnation"] = "replacement"
    elif cut == "route permission":
        host.mode.permissions.remove("threads:write")
    elif cut == "token":
        host.mode.user = host.req.state.user.model_copy(update={"token_version": 8})
    elif cut == "terminal":
        host.opts["run"].status = "success"
    else:
        host.opts["run"].abort_event.set()
    assert host.call(access.list_files, thread_id="earlier") is None
    assert host.call(access.import_file, thread_id="earlier", run_id="answer", path=OUTPUT) is None
    assert not list(host.site.destination.iterdir())
    host.site.app.state.run_event_store.list_events.assert_not_awaited()


def test_receipt_cannot_be_injected_by_a_client():
    from app.gateway.services import normalize_input, strip_server_owned_state_metadata

    tool = ToolMessage(content="saved", tool_call_id="x", name="import_conversation_file", additional_kwargs={"imported_files": [{"filename": "fake.txt"}]})
    for value in (tool, tool.model_dump()):
        for normalize in (normalize_input, strip_server_owned_state_metadata):
            [safe] = normalize({"messages": [value]})["messages"]
            assert "imported_files" not in safe.additional_kwargs


def test_bounded_discovery_preserves_every_presented_name_within_a_single_run(host):
    events = host.site.app.state.run_event_store
    paths = [f"/mnt/user-data/outputs/file-{i}.txt" for i in range(17)]
    events.list_events.return_value = [{"content": {"presented_files": paths}}]
    created = "2026-01-02T00:00:00Z"
    host.site.record["created_at"] = created
    manager = host.site.app.state.run_store
    record = manager.list_by_thread.return_value[0]

    async def list_before(thread_id, *, before_run_id, **kw):
        return [] if before_run_id is not None else [record]

    manager.list_by_thread.side_effect = list_before
    access = host.prepare()
    opts = {}
    found = []
    for n in (8, 8, 1):
        page = host.call(access.list_files, thread_id="earlier", **opts)
        assert len(page["files"]) == n
        found.extend(f["path"] for f in page["files"])
        opts = dict(before_created_at=page["next_before_created_at"], before_run_id=page["next_before_run_id"], offset=page["next_offset"])
    assert not page["has_more"] and found == paths


@pytest.mark.parametrize("cut", ["role loss", "abort", "incarnation"])
def test_change_while_opening_source_cannot_leave_an_attachment(host, monkeypatch, cut):
    from app.gateway import conversation_file_tools as service

    original = service.capture_source

    async def capture(*args, **kw):
        value = await original(*args, **kw)
        if cut == "role loss":
            host.mode.permissions.remove("runs:read")
        elif cut == "abort":
            host.opts["run"].abort_event.set()
        else:
            host.site.rows["later"]["incarnation"] = "another"
        return value

    monkeypatch.setattr(service, "capture_source", capture)
    access = host.prepare()
    assert host.call(access.import_file, thread_id="earlier", run_id="answer", path=OUTPUT) is None
    assert not list(host.site.destination.iterdir())


@pytest.mark.parametrize("cut", ["pat", "internal", "no-write", "mismatched-user", "misbound-destination", "other-run"])
def test_noninteractive_or_unqualified_destination_keeps_reader_only(host, cut):
    if cut in ("pat", "internal"):
        host.req.state.auth_source = cut
    elif cut == "no-write":
        host.req.state.auth.permissions.remove("threads:write")
    elif cut == "mismatched-user":
        host.req.state.user = _make_stub_user()
    elif cut == "misbound-destination":
        host.opts["destination"] = {**host.opts["destination"], "user_id": "elsewhere"}
    else:
        host.opts["run"].thread_id = "unrelated"
    assert host.prepare() is host.opts["reader"]


def test_withdrawn_presentation_during_read_never_publishes(host, monkeypatch):
    from app.gateway import conversation_file_tools as module

    original = module.capture_source

    async def capture(*args, **kw):
        file = await original(*args, **kw)
        host.site.app.state.run_event_store.list_events.return_value = []
        return file

    monkeypatch.setattr(module, "capture_source", capture)
    access = host.prepare()
    assert host.call(access.import_file, thread_id="earlier", run_id="answer", path=OUTPUT) is None
    assert not list(host.site.destination.iterdir())


def test_discovery_respects_the_configured_inline_budget_without_truncating_names(host):
    import json

    host.opts["config"].tool_output = ToolOutputConfig(tool_overrides={"list_conversation_files": 350})
    host.site.app.state.run_event_store.list_events.return_value = [{"content": {"presented_files": ["/mnt/user-data/outputs/" + "<" * 40]}}]
    access = host.prepare()
    result = host.call(access.list_files, thread_id="earlier")
    assert result["status"] == "output_budget_too_small"
    assert result["files"] == [] and len(json.dumps(result)) <= 350
    host.site.app.state.run_event_store.list_events.return_value = [{"content": {"presented_files": [OUTPUT]}}]
    result = host.call(access.list_files, thread_id="earlier")
    assert result["files"][0]["path"] == OUTPUT and not result["has_more"]


def test_callback_from_another_event_loop_cannot_write_or_leave_work_behind(host):
    access = host.prepare()
    assert asyncio.run(access.import_file(thread_id="earlier", run_id="answer", path=OUTPUT)) is None
    assert not list(host.site.destination.iterdir())


@pytest.mark.asyncio
async def test_start_run_binds_the_explicit_files_to_the_ordinary_lead_only(monkeypatch):
    from starlette.datastructures import URL
    from test_gateway_services import _make_start_run_persistence_context

    from app.gateway import conversation_file_tools, services
    from app.gateway.authz import AuthContext
    from app.gateway.routers.thread_runs import RunCreateRequest
    from deerflow.config.app_config import AppConfig, reset_app_config, set_app_config
    from deerflow.runtime.user_context import reset_current_user, set_current_user
    from deerflow.tools.conversation_import import ConversationFileAccess

    request, _, threads = _make_start_run_persistence_context()
    request.url = URL("https://deerflow.example/api/threads/destination/runs")
    user = _make_stub_user()
    user_id = str(user.id)
    request.state.user = user
    permissions = ["threads:read", "threads:write", "runs:create", "runs:read"]
    request.state.auth = AuthContext(user, permissions)
    request.state.auth_source = "session"
    await threads.create("source", user_id=user_id)
    app_config = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "tools": [{"name": "read_conversation", "group": "conversation", "use": "deerflow.tools.conversation:read_conversation"}]})
    set_app_config(app_config)
    monkeypatch.setattr(conversation_file_tools, "get_local_provider", lambda: SimpleNamespace(get_user=AsyncMock(return_value=user)))
    monkeypatch.setattr(conversation_file_tools, "resolve_route_permissions", AsyncMock(return_value=permissions))
    monkeypatch.setattr(conversation_file_tools, "account_refusal", lambda _: None)
    monkeypatch.setattr(services, "resolve_agent_factory", lambda *_: object())
    runner = AsyncMock()
    monkeypatch.setattr(services, "run_agent", runner)
    token = set_current_user(user)
    try:
        for destination, references, credential, expected in [("personal", ["source"], "session", True), ("unreferenced", [], "session", False), ("token", ["source"], "pat", False)]:
            request.state.auth_source = credential
            body = RunCreateRequest(input={"messages": [{"role": "user", "content": "Bring the report."}]}, conversation_references=references)
            record = await services.start_run(body, destination, request)
            await record.task
            assert isinstance(runner.await_args.kwargs["ctx"].conversation_reader, ConversationFileAccess) is expected
            assert "__conversation_reader" not in str(record.kwargs)
    finally:
        reset_current_user(token)
        reset_app_config()


def test_source_withdrawal_after_a_committed_copy_preserves_bytes_and_withholds_receipt(host, monkeypatch):
    from app.gateway import conversation_file_tools as module

    original = module.import_into_chat

    async def committed(**kwargs):
        answer = await original(**kwargs)
        host.site.app.state.run_event_store.list_events.return_value = []
        return answer

    monkeypatch.setattr(module, "import_into_chat", committed)
    access = host.prepare()
    assert host.call(access.import_file, thread_id="earlier", run_id="answer", path=OUTPUT) is None
    assert (host.site.destination / "notes.txt").read_bytes() == b"meeting notes"


def test_capture_retired_during_source_read_refuses_before_publishing(host, monkeypatch):
    from app.gateway import conversation_file_tools as module
    from deerflow.runtime.runs.capture_io import CaptureIOOwner, capture_io_scope, current_capture_io_owner

    original = module.capture_source

    async def capture(*args, **kwargs):
        file = await original(*args, **kwargs)
        current_capture_io_owner().retire()
        return file

    monkeypatch.setattr(module, "capture_source", capture)

    async def run():
        owner = CaptureIOOwner()
        with capture_io_scope(owner):
            access = await module.bind_conversation_files(**host.opts)
            assert await access.import_file(thread_id="earlier", run_id="answer", path=OUTPUT) is None
        await owner.drain()

    host.site.client.portal.call(run)
    assert not list(host.site.destination.iterdir())
