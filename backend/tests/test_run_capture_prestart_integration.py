"""Actual creating service and worker keep failed startup distinct from cancellation."""

import asyncio
from uuid import UUID

import httpx
import pytest
from _channel_execution_test_support import TABLES, ControlledProvider
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from fastapi import FastAPI
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.store.memory import InMemoryStore
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.channels.execution import ChannelExecutionScope
from app.channels.execution_client import ChannelExecutionFacade
from app.gateway import channel_execution_reads, deps, services
from app.gateway.auth.models import User
from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.csrf_middleware import CSRFMiddleware
from app.gateway.routers import run_output, thread_runs
from app.telegram_bots.gateway_client import GatewayClientOwner
from deerflow.agent_instances.conversations import AgentConversations
from deerflow.config import app_config
from deerflow.persistence.run.model import RunRow
from deerflow.runtime import MemoryStreamBridge
from deerflow.runtime.runs.worker import run_agent

OWNER = "00000000-0000-0000-0000-00000000000a"
PUBLICATION = "bc" * 16
THREAD = "startup-failure-output"


class Admission:
    async def authorize(self, operation):
        assert operation in {"run_create", "run_read", "source_read"}

    def current(self, operation):
        return operation in {"run_create", "run_read", "source_read"}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["metadata", "attachment", "commit_response"])
async def test_actual_service_startup_failure_has_failed_output_without_model_execution(capture_environment, monkeypatch, failure):
    e = capture_environment
    assert services.run_agent is run_agent
    async with e.engine.begin() as connection:
        if e.dialect == "sqlite":
            await connection.execute(text("BEGIN"))
        await connection.run_sync(lambda sync: TABLES[0].metadata.create_all(sync, tables=TABLES))
    config = app_config.AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}})
    for name in ("_app_config", "_app_config_path", "_app_config_mtime", "_app_config_signature", "_app_config_is_custom"):
        monkeypatch.setattr(app_config, name, getattr(app_config, name))
    app_config.set_app_config(config)
    monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "0")
    provider = ControlledProvider(User(id=UUID(OWNER), email="startup@example.com", system_role="user"))
    for module in (deps, services, channel_execution_reads):
        monkeypatch.setattr(module, "get_local_provider", lambda: provider)
    await e.thread_store.create(THREAD, assistant_id="lead_agent", user_id=OWNER)

    factory_calls = []

    def factory(**kwargs):
        factory_calls.append(kwargs)
        raise AssertionError("A failed startup reached agent construction")

    monkeypatch.setattr(services, "resolve_agent_factory", lambda _assistant: factory)
    fault_calls = []
    if failure == "metadata":
        original_metadata = services._ensure_thread_metadata

        async def fail_metadata(*args, **kwargs):
            # Finish the actual owner-scoped SQL lookup, then fail the attached
            # worker's prestart step. Admission/preparation reads stay real.
            await original_metadata(*args, **kwargs)
            fault_calls.append("metadata")
            raise LookupError("Controlled metadata startup failure")

        monkeypatch.setattr(services, "_ensure_thread_metadata", fail_metadata)
    elif failure == "attachment":

        class AttachmentFailure:
            def __getattr__(self, name):
                return getattr(asyncio, name)

            def create_task(self, coroutine, **kwargs):
                if getattr(getattr(coroutine, "cr_code", None), "co_name", None) == "run_after_metadata":
                    fault_calls.append("attachment")
                    raise RuntimeError("Controlled task attachment failure")
                return asyncio.create_task(coroutine, **kwargs)

        # Patch the service's task-construction boundary, preserving all SQL,
        # journal and host-owned asyncio tasks in the other modules.
        monkeypatch.setattr(services, "asyncio", AttachmentFailure())
    else:
        original_commit = AsyncSession.commit

        async def lose_committed_admission_response(session):
            creating = session.bind is e.engine and any(isinstance(row, RunRow) and row.operation_kind == "run" for row in session.new)
            await original_commit(session)
            if creating and not fault_calls:
                # The actual INSERT committed. The manager still has to retain
                # its constructed identity when this response is lost.
                fault_calls.append("commit_response")
                raise RuntimeError("Controlled committed admission response loss")

        monkeypatch.setattr(AsyncSession, "commit", lose_committed_admission_response)

    bridge = MemoryStreamBridge()
    app = FastAPI()
    app.add_middleware(CSRFMiddleware)
    app.add_middleware(AuthMiddleware)
    app.state.stream_bridge = bridge
    app.state.run_manager = e.manager
    app.state.checkpointer = InMemorySaver()
    app.state.store = InMemoryStore()
    app.state.run_event_store = e.event_store
    app.state.run_events_config = None
    app.state.thread_store = e.thread_store
    app.state.run_store = e.repository
    app.state.agent_conversations = AgentConversations(None, session_factory=e.sf)
    app.include_router(thread_runs.router)
    app.include_router(run_output.router, prefix="/api/threads")
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    owner = GatewayClientOwner(gateway_url="http://gateway.test", langgraph_url="http://gateway.test/api", transport=transport)
    admission = Admission()
    scope = ChannelExecutionScope(PUBLICATION, OWNER, None, "chat", THREAD, owner.endpoint_id, "lead_agent", None)
    facade = ChannelExecutionFacade(owner, scope=scope, admission=admission, capture_output=True)
    body = dict(
        assistant_id="lead_agent",
        input={"messages": [{"role": "human", "content": "Startup never executes this"}]},
        output_capture={"version": 1},
        channel_execution={"version": 1, "publication_id": PUBLICATION},
        on_disconnect="continue",
        on_completion=None,
        multitask_strategy="reject",
    )
    record = None
    try:
        response = await owner.request_json(
            operation="run_create",
            owner_user_id=OWNER,
            method="POST",
            path=f"threads/{THREAD}/runs",
            body=body,
            headers={"Idempotency-Key": PUBLICATION},
            before_dispatch=lambda: admission.authorize("run_create"),
            after_response=lambda: admission.authorize("run_read"),
        )
        assert response.status_code == (200 if failure == "metadata" else 503), response.body
        rows = await e.repository.list_by_thread(THREAD, user_id=OWNER)
        assert len(rows) == 1
        run_id = rows[0]["run_id"]
        record = await e.manager.get(run_id, user_id=OWNER)
        assert record is not None and not record.store_only
        if record.task is not None:
            await asyncio.wait_for(asyncio.shield(record.task), 10)
        stored = await e.repository.get(run_id, user_id=OWNER)
        assert factory_calls == [] and fault_calls == [failure]
        assert stored["status"] == "error"
        expected = dict(version=1, kind="unavailable", owner_user_id=OWNER, thread_id=THREAD, run_id=run_id, input_message_id=None, reason="failed")
        assert stored["kwargs"].get("output_evidence") == expected
        assert not record.ownership_lost
        if failure == "commit_response":
            assert record.error == "Run admission failed before execution."
        else:
            assert record.error and "Controlled" in record.error
        projection = await facade.get_run_output(run_id)
        assert projection.state == "unavailable" and projection.reason == "failed"
        assert projection.run_status == "error" and projection.source is None
        assert projection.output_evidence.model_dump(mode="json") == expected
        assert len(await e.repository.list_by_thread(THREAD, user_id=OWNER)) == 1
        assert await e.event_store.list_messages_by_run(THREAD, run_id, user_id=OWNER) == []
    finally:
        await e.manager.shutdown(timeout=2)
        if record is not None and record.task is not None:
            if not record.task.done():
                record.task.cancel()
            await asyncio.gather(record.task, return_exceptions=True)
        await owner.stop()
        await bridge.close()
