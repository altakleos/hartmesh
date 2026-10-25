"""Capture admission over actual SQL; worker doubles prove admission, not output."""

import asyncio
import hashlib
import json
import threading
import traceback
from copy import deepcopy
from types import SimpleNamespace
from uuid import UUID

import httpx
import pytest
import pytest_asyncio
from _router_auth_helpers import make_authed_test_app
from _run_output_store_test_support import output_store as output_store
from fastapi import HTTPException
from sqlalchemy import text, update
from test_gateway_services import _make_start_run_request

from app.gateway import agent_conversations, chat_storage, services
from app.gateway.auth.models import User
from app.gateway.auth_disabled import AUTH_SOURCE_SESSION
from app.gateway.run_models import RunCreateRequest
from deerflow.config import app_config as app_config_module
from deerflow.config.app_config import AppConfig, set_app_config
from deerflow.persistence.models.run_event import RunEventRow
from deerflow.persistence.projects import ProjectRepository
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.thread_meta import ThreadMetaRepository, ThreadMetaRow
from deerflow.runtime import DisconnectMode, MemoryStreamBridge, RunManager, RunStatus
from deerflow.runtime.events.store.db import DbRunEventStore
from deerflow.runtime.events.store.jsonl import JsonlRunEventStore
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.output import OUTPUT_CAPTURE_DISCONNECT_KEY, OutputCaptureIntent, UnavailableOutputReceipt
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.runtime.user_context import reset_current_user, set_current_user

OWNER_ID = "00000000-0000-0000-0000-00000000000a"


@pytest_asyncio.fixture
async def admission(output_store, monkeypatch):
    e = output_store
    async with e.engine.begin() as connection:
        if e.dialect == "sqlite":
            await connection.execute(text("BEGIN"))
        await connection.run_sync(lambda sync: RunEventRow.metadata.create_all(sync, tables=[RunEventRow.__table__, ThreadMetaRow.__table__]))
    config = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}})
    for name in ("_app_config", "_app_config_path", "_app_config_mtime", "_app_config_signature", "_app_config_is_custom"):
        monkeypatch.setattr(app_config_module, name, getattr(app_config_module, name))
    set_app_config(config)
    owner = User(id=UUID(OWNER_ID), email="capture-admission@example.com", system_role="user")
    token = set_current_user(owner)
    manager = RunManager(e.repository)
    threads = ThreadMetaRepository(e.sf)
    thread_id = "capture-admission"
    await threads.create(thread_id, assistant_id="lead_agent", user_id=OWNER_ID)
    request = _make_start_run_request(manager, thread_store=threads, auth_source=AUTH_SOURCE_SESSION)
    request.state.user = owner
    request.app.state.project_repo = ProjectRepository(e.sf)
    request.app.state.stream_bridge = bridge = MemoryStreamBridge()
    request.app.state.run_event_store = DbRunEventStore(e.sf)
    workers, inputs, preparations, managers = set(), [], [], [manager]
    prepare = agent_conversations.execution_for_run

    async def observed_preparation(*args, **kwargs):
        preparations.append(True)
        return await prepare(*args, **kwargs)

    async def worker(_bridge, actual_manager, record, *, ctx, graph_input, config, **kwargs):
        workers.add(asyncio.current_task())
        inputs.append({"record": record, "ctx": ctx, "graph_input": graph_input, "config": config})
        await actual_manager.try_start(record.run_id)
        await actual_manager.set_status(record.run_id, RunStatus.success)
        await bridge.publish_end(record.run_id)

    monkeypatch.setattr(agent_conversations, "execution_for_run", observed_preparation)
    monkeypatch.setattr(services, "run_agent", worker)
    try:
        yield SimpleNamespace(**vars(e), manager=manager, managers=managers, threads=threads, thread_id=thread_id, request=request, owner=owner, config=config, workers=workers, inputs=inputs, preparations=preparations, bridge=bridge)
    finally:
        for actual_manager in managers:
            await actual_manager.shutdown(timeout=2)
        await asyncio.gather(*workers, return_exceptions=True)
        await bridge.close()
        reset_current_user(token)


def ordinary(**changes):
    return RunCreateRequest.model_validate({"assistant_id": "lead_agent", "input": {"messages": [{"role": "human", "content": "Synthetic question"}]}, **changes})


def future_internal_capture(**changes):
    # A deliberate trusted internal stand-in before the wire field exists.
    # Its baseline failure is service behavior, not accepted HTTP negotiation.
    return ordinary(**changes).model_copy(update={"output_capture": OutputCaptureIntent(version=1)})


async def launched(e, body, *, key="capture-key"):
    record = await services.start_run(body, e.thread_id, e.request, idempotency_key=key)
    if record.task is not None:
        await record.task
    return record


def captured(**changes):
    return ordinary(output_capture={"version": 1}, **changes)


def install_manager(e, store, *, worker_id=None):
    manager = RunManager(store=store, **({"worker_id": worker_id} if worker_id else {}))
    e.managers.append(manager)
    e.request.app.state.run_manager = manager
    return manager


@pytest.mark.asyncio
async def test_capture_refuses_actual_memory_source_before_preparation(admission):
    e = admission
    e.request.app.state.run_event_store = MemoryRunEventStore()
    with pytest.raises(HTTPException) as failure:
        await launched(e, future_internal_capture())
    assert failure.value.status_code == 503
    assert e.preparations == [] and e.inputs == []
    assert await e.repository.list_by_thread(e.thread_id, user_id=OWNER_ID) == []


@pytest.mark.asyncio
async def test_ordinary_retry_cannot_reuse_an_existing_capture_intent(admission):
    e = admission
    body = ordinary()
    saved = await e.manager.create_or_reject(e.thread_id, "lead_agent", user_id=OWNER_ID, kwargs={"input": body.input, "output_capture": {"version": 1}}, idempotency_key="capture-key")
    await e.manager.set_status(saved.run_id, RunStatus.success)
    before = await e.repository.get(saved.run_id, user_id=OWNER_ID)
    with pytest.raises(HTTPException) as failure:
        await launched(e, body)
    assert failure.value.status_code == 409
    assert e.inputs == []
    after = await e.repository.get(saved.run_id, user_id=OWNER_ID)
    assert after["kwargs"] == before["kwargs"] and after["change_seq"] == before["change_seq"]


@pytest.mark.asyncio
@pytest.mark.parametrize("input_value", [None, {"messages": []}, {"messages": [{"role": "human", "content": "Synthetic question"}]}])
async def test_supported_capture_is_generic_and_retains_the_actual_context_source(admission, monkeypatch, input_value):
    e = admission

    async def no_channel_human(*args, **kwargs):
        raise AssertionError("Generic output capture cannot require channel human admission")

    monkeypatch.setattr(services, "live_channel_human", no_channel_human)
    source = e.request.app.state.run_event_store
    body = captured(input=input_value)
    before = body.model_dump(mode="json")
    record = await launched(e, body)
    row = await e.repository.get(record.run_id, user_id=OWNER_ID)
    assert e.inputs[0]["ctx"].event_store is source
    assert row["kwargs"]["output_capture"] == {"version": 1}
    assert "output_evidence" not in row["kwargs"] and "channel_execution" not in row["kwargs"]
    assert body.model_dump(mode="json") == before
    assert len(e.inputs) == 1


def unsupported_profile(e, profile, monkeypatch):
    if profile == "memory-source":
        e.request.app.state.run_event_store = MemoryRunEventStore()
    elif profile == "missing-source":
        e.request.app.state.run_event_store = None
    elif profile == "jsonl-source":
        e.request.app.state.run_event_store = JsonlRunEventStore()
    elif profile == "db-fallback":
        from deerflow.config.run_events_config import RunEventsConfig
        from deerflow.persistence import engine
        from deerflow.runtime.events.store import make_run_event_store

        monkeypatch.setattr(engine, "get_session_factory", lambda: None)
        source = make_run_event_store(RunEventsConfig(backend="db"))
        assert isinstance(source, MemoryRunEventStore)
        e.request.app.state.run_event_store = source
    elif profile == "memory-runs":
        install_manager(e, MemoryRunStore())
    elif profile == "truthy-source":
        e.request.app.state.run_event_store = SimpleNamespace(supports_durable_output_evidence=1)
    else:
        raise AssertionError(profile)


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", ["memory-source", "missing-source", "jsonl-source", "db-fallback", "memory-runs", "truthy-source"])
@pytest.mark.parametrize("endpoint", ["thread-create", "thread-stream", "thread-wait", "stateless-stream", "stateless-wait"])
async def test_actual_creating_post_refuses_unsupported_authorities_before_preparation(admission, monkeypatch, profile, endpoint):
    """Real ASGI/run SQL; auth is a labelled router fixture, not credential proof."""
    from app.gateway.routers import runs, thread_runs

    e = admission
    unsupported_profile(e, profile, monkeypatch)
    effects = []

    async def no_effect(*args, **kwargs):
        effects.append(True)
        raise AssertionError("Unsupported capture reached execution preparation")

    monkeypatch.setattr(services, "_load_scope_agent_config", no_effect)
    monkeypatch.setattr(services, "ensure_checkpoint_history_seeded", no_effect)
    monkeypatch.setattr(chat_storage, "prepare_chat_request", no_effect)
    e.request.app.state.storage_spaces_enabled = True
    app = make_authed_test_app(user_factory=lambda: e.owner, bind_current_user=True, signed_in=True)
    for name, value in vars(e.request.app.state).items():
        setattr(app.state, name, value)
    app.include_router(thread_runs.router)
    app.include_router(runs.router)
    path = {
        "thread-create": f"/api/threads/{e.thread_id}/runs",
        "thread-stream": f"/api/threads/{e.thread_id}/runs/stream",
        "thread-wait": f"/api/threads/{e.thread_id}/runs/wait",
        "stateless-stream": "/api/runs/stream",
        "stateless-wait": "/api/runs/wait",
    }[endpoint]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as client:
        response = await client.post(path, json=captured().model_dump(mode="json"))
    assert response.status_code == 503, response.text
    assert response.json() == {"detail": "Run output evidence is unavailable"}
    assert "content-location" not in response.headers
    assert effects == [] and e.preparations == [] and e.inputs == []
    assert await e.repository.list_by_thread(e.thread_id, user_id=OWNER_ID) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit_null", [False, True])
async def test_ordinary_memory_admission_and_inert_caller_proof_fields_remain_compatible(admission, explicit_null):
    e = admission
    store = MemoryRunStore()
    install_manager(e, store)
    e.request.app.state.run_event_store = MemoryRunEventStore()
    supplied = {"version": 1, "kind": "text", "assistant_message_id": "forged-source"}
    values = {"output_capture": None} if explicit_null else {}
    body = ordinary(
        metadata={"output_capture": {"version": 1}, "output_evidence": supplied},
        config={"metadata": {"output_capture": {"version": 1}, "output_evidence": supplied}},
        context={"output_capture": {"version": 1}, "output_evidence": supplied},
        **values,
    )
    record = await launched(e, body)
    assert "output_capture" not in record.kwargs and "output_evidence" not in record.kwargs
    assert len(e.inputs) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("peer", [False, True])
@pytest.mark.parametrize("representation", ["hash", "legacy-canonical", "legacy-raw"])
@pytest.mark.parametrize("intent", ["add", "remove", "same", "stored-null", "stored-version", "stored-extra"])
async def test_capture_identity_precedes_all_input_compatibility_branches(admission, peer, representation, intent):
    e = admission
    request_capture = intent != "remove"
    body = captured() if request_capture else ordinary()
    normalized = services.normalize_input(body.input)
    canonical = services._canonical_run_record_input(body.input, normalized)
    kwargs = {"input": body.input if representation == "legacy-raw" else canonical}
    if representation == "hash":
        kwargs["knowledge_default_request_hash"] = hashlib.sha256(json.dumps(canonical, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    stored = {"add": None, "remove": {"version": 1}, "same": {"version": 1}, "stored-null": None, "stored-version": {"version": 2}, "stored-extra": {"version": 1, "extra": True}}[intent]
    if intent != "add":
        kwargs["output_capture"] = stored
    malformed = intent in {"stored-null", "stored-version", "stored-extra"}
    initial = {**kwargs, "output_capture": {"version": 1}} if malformed else kwargs
    record = await e.manager.create_or_reject(e.thread_id, "lead_agent", user_id=OWNER_ID, kwargs=initial, idempotency_key="capture-key")
    await e.manager.set_status(record.run_id, RunStatus.success)
    if malformed:
        # New admissions correctly reject these shapes. Represent an already
        # existing malformed row/cache without weakening that production gate.
        async with e.sf() as session, session.begin():
            await session.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(kwargs_json=kwargs))
        record.kwargs = deepcopy(kwargs)
    before = await e.repository.get(record.run_id, user_id=OWNER_ID)
    if peer:
        install_manager(e, e.repository, worker_id="capture-peer")
    if intent == "same":
        reused = await launched(e, body)
        assert reused.run_id == record.run_id
    else:
        with pytest.raises(HTTPException) as failure:
            await launched(e, body)
        assert failure.value.status_code == 409
    after = await e.repository.get(record.run_id, user_id=OWNER_ID)
    assert after["kwargs"] == before["kwargs"] and after["change_seq"] == before["change_seq"]
    assert e.inputs == []


@pytest.mark.asyncio
async def test_captured_idempotent_reuse_keeps_original_disconnect_semantics(admission):
    e = admission
    first = await launched(e, captured(on_disconnect="continue"))
    with pytest.raises(HTTPException) as failure:
        await launched(e, captured(on_disconnect="cancel"))
    assert failure.value.status_code == 409
    assert len(e.inputs) == 1
    assert first.on_disconnect.value == "continue"


@pytest.mark.asyncio
async def test_peer_capture_continue_retry_preserves_saved_policy(admission):
    e = admission
    body = captured(on_disconnect="continue")
    first = await launched(e, body)
    install_manager(e, e.repository, worker_id="capture-peer")
    reused = await launched(e, body)
    assert reused.run_id == first.run_id and reused.on_disconnect.value == "continue"
    assert len(e.inputs) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("peer", [False, True])
async def test_capture_retry_cannot_invent_missing_durable_disconnect_policy(admission, peer):
    e = admission
    body = captured()
    first = await launched(e, body)
    kwargs = deepcopy(first.kwargs)
    kwargs.pop(OUTPUT_CAPTURE_DISCONNECT_KEY)
    async with e.sf() as session, session.begin():
        await session.execute(update(RunRow).where(RunRow.run_id == first.run_id).values(kwargs_json=kwargs))
    first.kwargs = kwargs
    if peer:
        install_manager(e, e.repository, worker_id="capture-peer")
    with pytest.raises(HTTPException) as failure:
        await launched(e, body)
    assert failure.value.status_code == 409
    assert len(e.inputs) == 1


@pytest.mark.asyncio
async def test_actual_manager_stamps_disconnect_and_discards_caller_policy(admission):
    e = admission
    record = await e.manager.create_or_reject(e.thread_id, "lead_agent", user_id=OWNER_ID, on_disconnect=DisconnectMode.continue_, kwargs={"output_capture": {"version": 1}, OUTPUT_CAPTURE_DISCONNECT_KEY: "cancel"})
    row = await e.repository.get(record.run_id, user_id=OWNER_ID)
    assert row["kwargs"][OUTPUT_CAPTURE_DISCONNECT_KEY] == "continue"
    ordinary_record = await e.manager.create_or_reject("ordinary-capture-policy", user_id=OWNER_ID, kwargs={OUTPUT_CAPTURE_DISCONNECT_KEY: "continue"})
    assert OUTPUT_CAPTURE_DISCONNECT_KEY not in (await e.repository.get(ordinary_record.run_id, user_id=OWNER_ID))["kwargs"]


@pytest.mark.asyncio
async def test_admission_reuse_preserves_an_existing_immutable_unavailable_seal(admission):
    """Host-seeded store/admission boundary, not worker/model provenance."""
    e = admission
    body = captured(on_disconnect="continue")
    canonical = services._canonical_run_record_input(body.input, services.normalize_input(body.input))
    record = await e.manager.create_or_reject(e.thread_id, "lead_agent", user_id=OWNER_ID, on_disconnect=DisconnectMode.continue_, kwargs={"input": canonical, "output_capture": {"version": 1}}, idempotency_key="capture-key")
    await e.manager.try_start(record.run_id)
    receipt = UnavailableOutputReceipt(version=1, kind="unavailable", owner_user_id=OWNER_ID, thread_id=e.thread_id, run_id=record.run_id, input_message_id=None, reason="failed")
    sealed = await e.manager.finalize_output(record.run_id, RunStatus.error, output_receipt=receipt)
    assert sealed.output_receipt_confirmed
    before = await e.repository.get(record.run_id, user_id=OWNER_ID)
    install_manager(e, e.repository, worker_id="capture-peer")
    reused = await launched(e, body)
    after = await e.repository.get(record.run_id, user_id=OWNER_ID)
    assert reused.run_id == record.run_id
    assert after["kwargs"] == before["kwargs"] and after["change_seq"] == before["change_seq"]
    assert e.inputs == []


@pytest.mark.asyncio
@pytest.mark.parametrize("same_capture", [False, True])
async def test_peer_found_after_capacity_refusal_still_compares_capture(admission, monkeypatch, same_capture):
    from deerflow.spaces.chat_paths import ChatFilesFull

    e = admission
    body = captured(on_disconnect="continue")
    peer = RunManager(e.repository, worker_id="capacity-peer")
    e.managers.append(peer)
    e.request.app.state.storage_spaces_enabled = True
    admitted = []

    async def prepared(*args):
        return object()

    async def overlapping_refusal(_prepared, thread_id):
        kwargs = {"input": body.input, **({"output_capture": {"version": 1}} if same_capture else {})}
        record = await peer.create_or_reject(thread_id, "lead_agent", user_id=OWNER_ID, on_disconnect=DisconnectMode.continue_, kwargs=kwargs, idempotency_key="capture-key")
        await peer.set_status(record.run_id, RunStatus.success)
        admitted.append(record)
        raise ChatFilesFull()

    monkeypatch.setattr(chat_storage, "prepare_chat_request", prepared)
    monkeypatch.setattr(chat_storage, "check_chat_thread_capacity", overlapping_refusal)
    if same_capture:
        record = await launched(e, body)
        assert record.run_id == admitted[0].run_id and record.on_disconnect == DisconnectMode.continue_
    else:
        with pytest.raises(HTTPException) as failure:
            await launched(e, body)
        assert failure.value.status_code == 409
    assert len(admitted) == 1 and e.inputs == []


@pytest.mark.asyncio
async def test_recorded_capture_retry_bypasses_storage_preparation(admission, monkeypatch):
    e = admission
    body = captured(on_disconnect="continue")
    first = await launched(e, body)
    install_manager(e, e.repository, worker_id="capture-peer")
    e.request.app.state.storage_spaces_enabled = True

    async def no_preparation(*args):
        raise AssertionError("An indexed recorded retry must bypass fresh storage preparation")

    monkeypatch.setattr(chat_storage, "prepare_chat_request", no_preparation)
    reused = await launched(e, body)
    assert reused.run_id == first.run_id and len(e.inputs) == 1


@pytest.mark.asyncio
async def test_capture_snapshot_precedes_the_first_preparation_await(admission, monkeypatch):
    from langchain_core.messages import HumanMessage

    e = admission
    entered, release = asyncio.Event(), asyncio.Event()
    source = e.request.app.state.run_event_store
    prepare = agent_conversations.execution_for_run

    async def held_preparation(*args, **kwargs):
        entered.set()
        await release.wait()
        return await prepare(*args, **kwargs)

    monkeypatch.setattr(agent_conversations, "execution_for_run", held_preparation)
    message = HumanMessage(content=[{"type": "text", "text": "Original"}], additional_kwargs={"files": [{"path": "uploads/original"}]})
    body = captured(input={"messages": [message]}, context={"thinking_enabled": True}, config={"metadata": {"retained": ["original"]}})
    operation = asyncio.create_task(launched(e, body))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        message.content[0]["text"] = "Changed"
        message.additional_kwargs["files"][0]["path"] = "uploads/changed"
        body.context["thinking_enabled"] = False
        body.config["metadata"]["retained"].append("changed")
        object.__setattr__(body.output_capture, "version", 2)
        e.request.app.state.run_event_store = MemoryRunEventStore()
        install_manager(e, MemoryRunStore())
        release.set()
        record = await asyncio.wait_for(operation, 5)
    finally:
        release.set()
        await asyncio.gather(operation, return_exceptions=True)
    actual = e.inputs[0]
    assert actual["ctx"].event_store is source
    assert actual["graph_input"]["messages"][0].content == [{"type": "text", "text": "Original"}]
    assert actual["graph_input"]["messages"][0].additional_kwargs["files"] == [{"path": "uploads/original"}]
    assert actual["config"]["context"]["thinking_enabled"] is True
    row = await e.repository.get(record.run_id, user_id=OWNER_ID)
    assert row["kwargs"]["output_capture"] == {"version": 1}
    assert row["kwargs"]["config"]["metadata"]["retained"] == ["original"]


@pytest.mark.asyncio
async def test_repeated_cancellation_retains_started_capture_copy(admission, monkeypatch):
    e = admission
    entered, finished = asyncio.Event(), threading.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()

    def held_copy(value):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        try:
            return deepcopy(value)
        finally:
            finished.set()

    monkeypatch.setattr(services, "deepcopy", held_copy)
    operation = asyncio.create_task(launched(e, captured()))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        operation.cancel()
        operation.cancel()
        done, _ = await asyncio.wait({operation}, timeout=0.05)
        assert not done and not finished.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert finished.is_set() and e.inputs == [] and e.preparations == []
        assert await e.repository.list_by_thread(e.thread_id, user_id=OWNER_ID) == []
    finally:
        release.set()
        await asyncio.gather(operation, return_exceptions=True)


@pytest.mark.asyncio
async def test_capture_copy_failure_has_a_fixed_error_before_preparation(admission, monkeypatch):
    e = admission

    def failed_copy(value):
        raise RuntimeError("private-capture-copy-payload")

    monkeypatch.setattr(services, "deepcopy", failed_copy)
    with pytest.raises(HTTPException) as failure:
        await launched(e, captured())
    assert failure.value.status_code == 503
    assert failure.value.detail == "Run output evidence is unavailable"
    assert "private-capture-copy-payload" not in "".join(traceback.format_exception(failure.value))
    assert e.inputs == [] and e.preparations == []


@pytest.mark.asyncio
async def test_capture_intent_removed_during_copy_cannot_downgrade_to_ordinary(admission, monkeypatch):
    e = admission
    entered, release = asyncio.Event(), threading.Event()
    loop = asyncio.get_running_loop()

    def held_copy(value):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        return deepcopy(value)

    monkeypatch.setattr(services, "deepcopy", held_copy)
    body = captured()
    operation = asyncio.create_task(launched(e, body))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        body.output_capture = None
        release.set()
        with pytest.raises(HTTPException) as failure:
            await operation
        assert failure.value.status_code == 422
        assert e.inputs == [] and e.preparations == []
    finally:
        release.set()
        await asyncio.gather(operation, return_exceptions=True)


@pytest.mark.asyncio
async def test_channel_copy_added_capture_uses_the_detached_intent_for_preflight(admission, monkeypatch):
    from _channel_execution_test_support import ControlledProvider

    from app.gateway import channel_execution_reads, deps
    from app.gateway.auth_disabled import AUTH_SOURCE_INTERNAL
    from app.gateway.internal_auth import INTERNAL_OWNER_USER_ID_HEADER_NAME, get_internal_user
    from deerflow.agent_instances.conversations import AgentConversations
    from deerflow.persistence.agent_instances.model import AgentConversationRow, AgentDefinitionRevisionRow, AgentInstanceRow
    from deerflow.persistence.spaces.model import SpaceRow

    e = admission
    async with e.engine.begin() as connection:
        if e.dialect == "sqlite":
            await connection.execute(text("BEGIN"))
        tables = [AgentDefinitionRevisionRow.__table__, SpaceRow.__table__, AgentInstanceRow.__table__, AgentConversationRow.__table__]
        await connection.run_sync(lambda sync: AgentConversationRow.metadata.create_all(sync, tables=tables))
    provider = ControlledProvider(e.owner)
    for module in (deps, services, channel_execution_reads):
        monkeypatch.setattr(module, "get_local_provider", lambda: provider)
    e.request.state.user = get_internal_user(owner_user_id=OWNER_ID)
    e.request.state.auth_source = AUTH_SOURCE_INTERNAL
    e.request.headers = httpx.Headers({INTERNAL_OWNER_USER_ID_HEADER_NAME: OWNER_ID})
    e.request.app.state.agent_conversations = AgentConversations(None, session_factory=e.sf)
    e.request.app.state.run_event_store = MemoryRunEventStore()
    entered, release = asyncio.Event(), threading.Event()
    loop = asyncio.get_running_loop()

    def held_copy(value):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        return deepcopy(value)

    monkeypatch.setattr(services, "deepcopy", held_copy)
    body = ordinary(channel_execution={"version": 1, "publication_id": "a" * 32}, on_disconnect="continue")
    assert body.output_capture is None
    operation = asyncio.create_task(launched(e, body))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        body.output_capture = OutputCaptureIntent(version=1)
        release.set()
        with pytest.raises(HTTPException) as failure:
            await operation
        assert failure.value.status_code == 503
        assert failure.value.detail == "Run output evidence is unavailable"
        assert e.inputs == [] and e.preparations == []
        assert await e.repository.list_by_thread(e.thread_id, user_id=OWNER_ID) == []
    finally:
        release.set()
        await asyncio.gather(operation, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [{}, {"version": True}, {"version": 3}, {"version": 1, "extra": True}])
async def test_internal_model_copy_cannot_bypass_capture_validation(admission, value):
    e = admission
    body = ordinary().model_copy(update={"output_capture": value})
    with pytest.raises(HTTPException) as failure:
        await launched(e, body)
    assert failure.value.status_code == 422
    assert e.inputs == [] and e.preparations == []
