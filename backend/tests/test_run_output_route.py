"""Actual SQL output projection; host-seeded seals do not prove worker selection."""

from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest
import pytest_asyncio
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _run_output_capture_test_support import put_messages
from langchain_core.messages import AIMessage, HumanMessage
from sqlalchemy import delete, update
from test_thread_runs_internal_scope import _ScopeAuthMiddleware

from app.gateway.auth.models import User
from app.gateway.auth_disabled import AUTH_SOURCE_SESSION
from app.gateway.routers import run_output
from deerflow.config import app_config
from deerflow.runtime.runs.output import TextOutputReceipt, canonical_text_digest
from deerflow.runtime.runs.schemas import RunStatus

OWNER = "00000000-0000-0000-0000-00000000000a"
PUBLICATION = "a" * 32
HEADER = "X-DeerFlow-Require-Channel-Execution"
ACK = "X-DeerFlow-Channel-Execution-Version"
TEXT = "Original café\n exact "


@pytest_asyncio.fixture
async def route_environment(capture_environment, monkeypatch):
    from fastapi import FastAPI

    config = app_config.AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}})
    for key in ("_app_config", "_app_config_path", "_app_config_mtime", "_app_config_signature", "_app_config_is_custom"):
        monkeypatch.setattr(app_config, key, getattr(app_config, key))
    app_config.set_app_config(config)
    e = capture_environment
    app = FastAPI()
    user = User(id=UUID(OWNER), email="output@example.com", system_role="user")
    app.add_middleware(_ScopeAuthMiddleware, user=user, auth_source=AUTH_SOURCE_SESSION)
    app.state.thread_store = e.thread_store
    app.state.run_manager = e.manager
    app.state.run_event_store = e.event_store
    app.include_router(run_output.router, prefix="/api/threads")
    await e.thread_store.create("thread", user_id=OWNER)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway.test", trust_env=False) as client:
        yield SimpleNamespace(**vars(e), app=app, client=client, user=user)


async def seed(e, *, sealed=True, capture=True):
    kwargs = {"channel_execution": {"version": 1, "publication_id": PUBLICATION, "owner_user_id": OWNER, "thread_id": "thread", "assistant_id": "lead_agent", "agent_name": None}}
    if capture:
        kwargs["output_capture"] = {"version": 1}
    record = await e.manager.create_or_reject("thread", "lead_agent", user_id=OWNER, kwargs=kwargs)
    await e.manager.try_start(record.run_id)
    human = HumanMessage(id="human", content="input", additional_kwargs={"channel_turn_id": PUBLICATION})
    ai = AIMessage(id="actual-ai", content=TEXT)
    rows = await put_messages(e, [human.model_dump(), ai.model_dump()], owner=OWNER, run_id=record.run_id)
    receipt = TextOutputReceipt(
        version=1, kind="text", owner_user_id=OWNER, thread_id="thread", run_id=record.run_id, input_message_id="human", assistant_message_id="actual-ai", event_seq=rows[-1]["seq"], text_sha256=canonical_text_digest(TEXT)
    )
    if sealed:
        result = await e.manager.finalize_output(record.run_id, RunStatus.success, output_receipt=receipt)
        assert result.output_receipt_confirmed
    return record, receipt


async def get(e, run_id, *, headers=None):
    return await e.client.get(f"/api/threads/thread/runs/{run_id}/output", headers={HEADER: "1"} if headers is None else headers)


@pytest.mark.asyncio
async def test_actual_stored_seal_and_source_are_complete(route_environment):
    e = route_environment
    record, receipt = await seed(e)
    response = await get(e, record.run_id)
    assert response.status_code == 200 and response.headers.get(ACK) == "1"
    body = response.json()
    assert body["state"] == "complete" and body["source"] == {"text": TEXT}
    assert body["output_evidence"] == receipt.model_dump(mode="json")


@pytest.mark.asyncio
async def test_active_row_is_pending_even_if_cached_success_is_staged(route_environment):
    record, _ = await seed(route_environment, sealed=False)
    record.status = RunStatus.success
    record.finalizing = True
    response = await get(route_environment, record.run_id)
    assert response.status_code == 200
    assert response.json()["state"] == "pending" and response.json()["source"] is None


@pytest.mark.asyncio
async def test_supported_actual_miss_has_ack(route_environment):
    response = await get(route_environment, uuid4().hex)
    assert response.status_code == 404 and response.headers.get(ACK) == "1"


@pytest.mark.asyncio
@pytest.mark.parametrize("headers", [{}, {HEADER: "2"}, [(HEADER, "1"), (HEADER, "1")]])
async def test_invalid_or_absent_version_never_acknowledges(route_environment, headers):
    response = await get(route_environment, "missing", headers=headers)
    assert response.status_code == 422 and ACK not in response.headers


@pytest.mark.asyncio
async def test_a_failed_read_leaves_its_cause_and_not_its_text(route_environment, monkeypatch, caplog):
    async def fail(**_kwargs):
        raise KeyError("private text")

    monkeypatch.setattr("app.gateway.routers.run_output._read_owned", fail)
    response = await get(route_environment, "missing")
    assert response.status_code == 503 and ACK not in response.headers
    assert "Run output read failed run_id=missing cause=KeyError:test_run_output_route.py:" in caplog.text and "private text" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("authority", ["run", "event"])
async def test_capability_is_required_before_missing_ack(route_environment, authority, caplog):
    from deerflow.runtime.events.store.memory import MemoryRunEventStore
    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.store.memory import MemoryRunStore

    e = route_environment
    if authority == "run":
        e.app.state.run_manager = RunManager(MemoryRunStore())
    else:
        e.app.state.run_event_store = MemoryRunEventStore()
    response = await get(e, "missing")
    assert response.status_code == 503 and ACK not in response.headers
    assert "Run output read failed run_id=missing cause=" in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing-ai", "missing-human", "changed-ai", "malformed-json", "wrong-marker"])
async def test_exact_source_failures_never_adopt_another_answer(route_environment, change):
    import json

    from deerflow.persistence.models.run_event import RunEventRow

    e = route_environment
    record, receipt = await seed(e)
    async with e.sf.begin() as session:
        if change.startswith("missing"):
            await session.execute(delete(RunEventRow).where(RunEventRow.seq == (receipt.event_seq if change == "missing-ai" else 1)))
        elif change == "malformed-json":
            await session.execute(update(RunEventRow).where(RunEventRow.seq == receipt.event_seq).values(content="{broken"))
        else:
            value = AIMessage(id="actual-ai", content="different").model_dump() if change == "changed-ai" else HumanMessage(id="human", content="input", additional_kwargs={"channel_turn_id": "b" * 32}).model_dump()
            await session.execute(update(RunEventRow).where(RunEventRow.seq == (receipt.event_seq if change == "changed-ai" else 1)).values(content=json.dumps(value)))
    response = await get(e, record.run_id)
    if change == "malformed-json":
        assert response.status_code == 503 and ACK not in response.headers
    else:
        assert response.status_code == 200
        assert response.json()["state"] == "unavailable" and response.json()["source"] is None
        assert response.json()["reason"] == ("source_missing" if change.startswith("missing") else "source_changed")


@pytest.mark.asyncio
async def test_legacy_unsealed_terminal_row_is_uncertainty(route_environment):
    e = route_environment
    record, _ = await seed(e, sealed=False, capture=False)
    await e.manager.set_status(record.run_id, RunStatus.success)
    response = await get(e, record.run_id)
    assert response.status_code == 503 and ACK not in response.headers


@pytest.mark.asyncio
async def test_actual_worker_seal_is_consumed_by_the_real_sql_route(route_environment, monkeypatch):
    # Session authentication stand-in; real SQL owner/run/events and real graph/
    # model callbacks select and seal the output. No AI/event/receipt is seeded.
    import test_run_output_capture_worker as worker_test

    e = route_environment
    monkeypatch.setattr(worker_test, "RAW_OWNER", OWNER)
    env = await worker_test._environment(e, monkeypatch)
    env.human.additional_kwargs["channel_turn_id"] = PUBLICATION
    env.record = await env.manager.create_or_reject(
        "thread",
        "lead_agent",
        user_id=OWNER,
        kwargs={"output_capture": {"version": 1}, "channel_execution": {"version": 1, "publication_id": PUBLICATION, "owner_user_id": OWNER, "thread_id": "thread", "assistant_id": "lead_agent", "agent_name": None}},
    )
    e.app.state.run_manager = env.manager
    e.app.state.run_event_store = env.events
    assert env.human.id is None
    await worker_test._execute(env)
    response = await get(e, env.record.run_id)
    assert response.status_code == 200 and response.headers.get(ACK) == "1"
    body = response.json()
    assert body["state"] == "complete"
    assert body["source"] == {"text": "  Exact answer\nα"}
    assert body["output_evidence"]["input_message_id"] == env.human.id
    assert body["output_evidence"]["assistant_message_id"] == "genuine-model-id"
    assert len(env.calls) == 1


@pytest.mark.asyncio
async def test_backend_http_exception_cannot_forge_supported_missing(route_environment, monkeypatch):
    from fastapi import HTTPException

    async def failed(*args, **kwargs):
        raise HTTPException(404, "private store content", headers={ACK: "1"})

    monkeypatch.setattr(route_environment.manager, "get_output_snapshot", failed)
    response = await get(route_environment, "missing")
    assert response.status_code == 503 and ACK not in response.headers
    assert "private store content" not in response.text


@pytest.mark.asyncio
async def test_complete_route_read_and_scope_restore_are_drained_across_cancel(route_environment, monkeypatch):
    import asyncio

    from fastapi import Request

    from app.gateway.authz import AuthContext
    from deerflow.runtime.user_context import get_current_user

    e = route_environment
    record, _ = await seed(e)
    entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = e.event_store.list_messages_by_run

    async def held(*args, **kwargs):
        assert str(get_current_user().id) == OWNER
        entered.set()
        await release.wait()
        try:
            return await original(*args, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(e.event_store, "list_messages_by_run", held)
    previous = get_current_user()
    request = Request({"type": "http", "method": "GET", "path": "/", "headers": [(HEADER.lower().encode(), b"1")], "app": e.app})
    request.state.user = User(id=UUID(OWNER), email="output@example.com", system_role="user")
    request.state.auth_source = AUTH_SOURCE_SESSION
    request.state.auth = AuthContext(user=request.state.user, permissions=["runs:read"])
    operation = asyncio.create_task(run_output.get_run_output("thread", record.run_id, request))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        operation.cancel()
        operation.cancel()
        await asyncio.sleep(0)
        assert not operation.done() and not finished.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert finished.is_set()
        assert get_current_user() is previous
    finally:
        release.set()
        await asyncio.gather(operation, return_exceptions=True)


@pytest.mark.asyncio
async def test_thread_owner_changed_during_source_read_is_not_a_positive(route_environment, monkeypatch):
    from deerflow.persistence.thread_meta.model import ThreadMetaRow

    e = route_environment
    record, _ = await seed(e)
    original = e.event_store.list_messages_by_run

    async def changed(*args, **kwargs):
        rows = await original(*args, **kwargs)
        async with e.sf.begin() as session:
            await session.execute(update(ThreadMetaRow).where(ThreadMetaRow.thread_id == "thread").values(user_id="foreign"))
        return rows

    monkeypatch.setattr(e.event_store, "list_messages_by_run", changed)
    response = await get(e, record.run_id)
    assert response.status_code == 404 and ACK not in response.headers


@pytest.mark.asyncio
async def test_actual_retained_facade_owner_reads_original_sql_route(route_environment):
    from test_channel_execution_client import Admission

    from app.channels.execution import ChannelExecutionScope
    from app.channels.execution_client import ChannelExecutionFacade
    from app.telegram_bots.gateway_client import GatewayClientOwner

    e = route_environment
    record, receipt = await seed(e)
    # ASGI session identity is a labelled auth stand-in; wire, facade, retained
    # HTTP owner, router, run store and event source are their actual implementations.
    owner = GatewayClientOwner(gateway_url="http://gateway.test", langgraph_url="http://gateway.test/api", transport=httpx.ASGITransport(app=e.app))
    scope = ChannelExecutionScope(PUBLICATION, OWNER, "connection", "chat", "thread", owner.endpoint_id, "lead_agent", None)
    client = ChannelExecutionFacade(owner, scope=scope, admission=Admission())
    try:
        owner.close_run_admission()
        result = await client.get_run_output(record.run_id)
        assert result.state == "complete" and result.source.text == TEXT
        assert result.output_evidence.event_seq == receipt.event_seq
    finally:
        await owner.stop()


@pytest.mark.asyncio
async def test_account_disabled_during_source_sql_refuses_the_final_response(route_environment, monkeypatch):
    from datetime import UTC, datetime

    e = route_environment
    record, _ = await seed(e)
    original = e.event_store.list_messages_by_run

    async def disabled(*args, **kwargs):
        rows = await original(*args, **kwargs)
        e.user.disabled_at = datetime.now(UTC)
        return rows

    monkeypatch.setattr(e.event_store, "list_messages_by_run", disabled)
    response = await get(e, record.run_id)
    assert response.status_code == 403 and ACK not in response.headers


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reason,status",
    [("unsupported_input", "success"), ("unsupported_output", "success"), ("human_input_required", "success"), ("ambiguous_source", "success"), ("missing_message_identity", "success"), ("failed", "error"), ("cancelled", "interrupted")],
)
async def test_closed_unavailable_seal_never_invents_a_source(route_environment, reason, status):
    from deerflow.runtime.runs.output import UnavailableOutputReceipt

    e = route_environment
    record, _ = await seed(e, sealed=False)
    receipt = UnavailableOutputReceipt(version=1, kind="unavailable", owner_user_id=OWNER, thread_id="thread", run_id=record.run_id, input_message_id=None, reason=reason)
    result = await e.manager.finalize_output(record.run_id, RunStatus(status), output_receipt=receipt)
    assert result.output_receipt_confirmed
    response = await get(e, record.run_id)
    assert response.status_code == 200 and response.headers.get(ACK) == "1"
    assert response.json()["state"] == "unavailable" and response.json()["reason"] == reason
    assert response.json()["source"] is None


@pytest.mark.asyncio
async def test_later_web_run_and_other_thread_do_not_replace_saved_source(route_environment):
    e = route_environment
    record, receipt = await seed(e)
    later = await e.manager.create_or_reject("thread", "lead_agent", user_id=OWNER)
    await e.manager.try_start(later.run_id)
    await put_messages(e, [HumanMessage(id="later-human", content="later").model_dump(), AIMessage(id="later-ai", content="later answer").model_dump()], owner=OWNER, run_id=later.run_id)
    await e.manager.set_status(later.run_id, RunStatus.success)
    await e.thread_store.create("new-thread", user_id=OWNER)
    response = await get(e, record.run_id)
    assert response.status_code == 200 and response.json()["source"] == {"text": TEXT}
    assert response.json()["output_evidence"] == receipt.model_dump(mode="json")


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["owner", "scope-failure"])
async def test_supported_missing_run_refreshes_authority_before_ack(route_environment, monkeypatch, change):
    from deerflow.persistence.thread_meta.model import ThreadMetaRow

    e = route_environment
    original = e.manager.get_output_snapshot

    async def missing(*args, **kwargs):
        result = await original(*args, **kwargs)
        assert result is None
        if change == "owner":
            async with e.sf.begin() as session:
                await session.execute(update(ThreadMetaRow).where(ThreadMetaRow.thread_id == "thread").values(user_id="foreign"))
        else:

            async def failed(*args, **kwargs):
                raise RuntimeError("private scope lookup details")

            monkeypatch.setattr(e.thread_store, "get", failed)
        return result

    monkeypatch.setattr(e.manager, "get_output_snapshot", missing)
    response = await get(e, "actual-missing-run")
    assert response.status_code == (404 if change == "owner" else 503)
    assert ACK not in response.headers
    assert "private scope lookup details" not in response.text
