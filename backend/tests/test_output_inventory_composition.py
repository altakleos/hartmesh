"""Actual first-party assembly -> worker/scans/SQL -> owned strict ASGI inventory.

No profile, scanner, event, receipt, worker or source read is substituted. The
offline model replaces provider I/O; host Paths confines all files to task disk.
"""

import asyncio
import json
from uuid import UUID

import httpx
import pytest
from _channel_execution_test_support import TABLES, ControlledProvider
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from fastapi import FastAPI
from langchain_core.messages import AIMessage
from langgraph.checkpoint.memory import InMemorySaver
from sqlalchemy import text
from test_gateway_services import _make_start_run_request
from test_output_inventory_profile import assemble_offline

from app.channels.execution import ChannelExecutionScope
from app.channels.execution_client import ChannelExecutionFacade
from app.gateway import channel_execution_reads, deps, services
from app.gateway.auth.models import User
from app.gateway.auth_middleware import AuthMiddleware
from app.gateway.csrf_middleware import CSRFMiddleware
from app.gateway.routers import run_output, thread_runs
from app.gateway.stream_cleanup import StreamCleanupRegistry
from app.telegram_bots.gateway_client import GatewayClientOwner
from deerflow.agent_instances.conversations import AgentConversations
from deerflow.agents.lead_agent import agent
from deerflow.config import app_config
from deerflow.config.paths import Paths, paths_scope
from deerflow.runtime import MemoryStreamBridge
from deerflow.runtime.capture_resources import OwnedCheckpointSaver, OwnedStore
from deerflow.runtime.runs.output import canonical_text_digest
from deerflow.runtime.runs.output_inventory import presentation_fragment_digest
from deerflow.runtime.runs.worker import run_agent
from deerflow.utils.messages import restore_original_human_message, strip_injected_user_message_id_suffix

OWNER = "00000000-0000-0000-0000-00000000000a"
PUBLICATION = "ba" * 16
THREAD = "composition-output"
QUESTION = "  Original HTTP question\n"
ANSWER = "  Exact offline answer\nα"


class Admission:
    async def authorize(self, operation):
        assert operation in {"run_create", "run_read", "source_read"}

    def current(self, operation):
        return operation in {"run_create", "run_read", "source_read"}


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["normal", "runtime_ai_mutation", "same_ai_id", "old_path_reoffer", "clarification", "old_rich_history", "unsupported_factory", "early_interrupt", "early_rollback", "late_interrupt", "late_rollback"])
async def test_actual_firstparty_worker_inventory_and_real_cancellation_through_owned_http(capture_environment, monkeypatch, tmp_path, case):
    e = capture_environment
    paths = await asyncio.to_thread(Paths, tmp_path / "data")
    with paths_scope(paths):
        # Pin the no-double promise before any request/fixture work.
        assert services.run_agent is run_agent
        original_start = services.start_run
        async with e.engine.begin() as connection:
            if e.dialect == "sqlite":
                await connection.execute(text("BEGIN"))
            await connection.run_sync(lambda sync: TABLES[0].metadata.create_all(sync, tables=TABLES))
        from deerflow.tools.builtins.clarification_tool import ask_clarification_tool
        from deerflow.tools.builtins.present_file_tool import present_file_tool

        tools = [present_file_tool] if case == "old_path_reoffer" else [ask_clarification_tool] if case == "clarification" else []
        assembly, _built, model = await asyncio.to_thread(assemble_offline, monkeypatch, tool_objects=tools)
        assert assembly.descriptor is None and assembly.inventory_profile is not None
        model.responses = [AIMessage(ANSWER, **({"id": "inherited-ai-id"} if case == "same_ai_id" else {}))]
        if case in {"old_path_reoffer", "clarification"}:
            call = dict(id="actual-current-call", name=tools[0].name, args={"filepaths": ["/mnt/user-data/outputs/existing.txt"]} if case == "old_path_reoffer" else {"question": "Supply the requested information"})
            model.responses = [AIMessage(content="", tool_calls=[call]), AIMessage(ANSWER)]
        config = agent.get_app_config()
        await asyncio.to_thread(paths.ensure_thread_dirs, THREAD, user_id=OWNER)
        if case == "old_path_reoffer":
            await asyncio.to_thread(lambda: (paths.sandbox_outputs_dir(THREAD, user_id=OWNER) / "existing.txt").write_text("Unchanged historical file", encoding="utf-8"))
        for name in ("_app_config", "_app_config_path", "_app_config_mtime", "_app_config_signature", "_app_config_is_custom"):
            monkeypatch.setattr(app_config, name, getattr(app_config, name))
        app_config.set_app_config(config)
        monkeypatch.setenv("DEER_FLOW_AUTH_DISABLED", "0")
        user = User(id=UUID(OWNER), email="composition@example.com", system_role="user")
        provider = ControlledProvider(user)
        for module in (deps, services, channel_execution_reads):
            monkeypatch.setattr(module, "get_local_provider", lambda: provider)
        await e.thread_store.create(THREAD, assistant_id="lead_agent", user_id=OWNER)
        assert await AgentConversations(None, session_factory=e.sf).binding(THREAD) is None

        calls, humans, factories, requests, responses = [], [], [], [], []
        actual_generate = type(model)._generate

        def generate(self, messages, *args, **kwargs):
            calls.append(True)
            humans.extend(message for message in messages if message.type == "human" and message.additional_kwargs.get("channel_turn_id") == PUBLICATION)
            return actual_generate(self, messages, *args, **kwargs)

        monkeypatch.setattr(type(model), "_generate", generate)
        if case in {"same_ai_id", "runtime_ai_mutation"}:
            actual_agenerate = type(model)._agenerate

            async def produce_file(self, *args, **kwargs):
                await asyncio.to_thread(lambda: (paths.sandbox_outputs_dir(THREAD, user_id=OWNER) / "current.txt").write_text("Current produced file", encoding="utf-8"))
                return await actual_agenerate(self, *args, **kwargs)

            monkeypatch.setattr(type(model), "_agenerate", produce_file)

        def factory(**kwargs):
            if "callbacks" in kwargs["config"]:
                factories.append(tuple(type(callback).__name__ for callback in kwargs["config"]["callbacks"]))
            return assembly.graph if case == "unsupported_factory" else assembly

        def resolve(assistant_id):
            assert assistant_id == "lead_agent"
            return factory

        monkeypatch.setattr(services, "resolve_agent_factory", resolve)
        controls = []
        if case.startswith("early_"):
            from deerflow.runtime.runs import worker

            actual_scan = worker._produced_output_paths
            actual_stage = e.manager.set_status_if_not_cancelled
            scanned = []

            async def after_scan(*args, **kwargs):
                result = await actual_scan(*args, **kwargs)
                assert kwargs["inventory_capture"].final_scan is not None
                scanned.append(True)
                return result

            async def before_status(run_id, status, **kwargs):
                assert str(status) == "success" and scanned == [True]
                # This await occurs AFTER the worker has cached delivery_content.
                # A separate real control task now cancels the actual worker.
                control = asyncio.create_task(e.manager.cancel(run_id, action=case.removeprefix("early_")))
                controls.append(control)
                await asyncio.shield(control)
                return await actual_stage(run_id, status, **kwargs)

            monkeypatch.setattr(worker, "_produced_output_paths", after_scan)
            monkeypatch.setattr(e.manager, "set_status_if_not_cancelled", before_status)
        elif case.startswith("late_"):
            from deerflow.runtime.runs import worker

            actual_append = worker.persist_output_inventory

            async def after_inventory(*args, **kwargs):
                result = await actual_append(*args, **kwargs)
                await e.repository.request_cancel(kwargs["output_receipt"].run_id, action=case.removeprefix("late_"))
                return result

            monkeypatch.setattr(worker, "persist_output_inventory", after_inventory)
        bridge = MemoryStreamBridge()
        prototype = _make_start_run_request(e.manager, thread_store=e.thread_store)
        app = FastAPI()
        app.add_middleware(CSRFMiddleware)
        app.add_middleware(AuthMiddleware)
        for key, value in vars(prototype.app.state).items():
            setattr(app.state, key, value)
        app.state.stream_bridge = bridge
        app.state.stream_cleanups = StreamCleanupRegistry(bridge)
        app.state.checkpointer = OwnedCheckpointSaver(InMemorySaver(), profile="memory", loop=asyncio.get_running_loop())
        if case in {"same_ai_id", "old_path_reoffer", "old_rich_history"}:
            from langchain_core.messages import HumanMessage

            assembly.graph.checkpointer = app.state.checkpointer
            old = AIMessage(content=[{"type": "image_url", "image_url": {"url": "https://unit.invalid/old.png"}}] if case == "old_rich_history" else "previous answer", id="inherited-ai-id")
            state = {"messages": [HumanMessage("previous question", id="previous-human"), old]}
            if case == "old_path_reoffer":
                state["artifacts"] = ["/mnt/user-data/outputs/existing.txt"]
            await assembly.graph.aupdate_state({"configurable": {"thread_id": THREAD}}, state, as_node="model")
        app.state.store = OwnedStore(prototype.app.state.store, profile="memory", loop=asyncio.get_running_loop())
        app.state.run_event_store = e.event_store
        app.state.run_store = e.repository
        app.state.agent_conversations = AgentConversations(None, session_factory=e.sf)
        app.include_router(thread_runs.router)
        app.include_router(run_output.router, prefix="/api/threads")

        class RecordingASGI(httpx.ASGITransport):
            async def handle_async_request(self, request):
                # Never store authority headers/cookies in diagnostic artifacts.
                requests.append((request.method, request.url.path, json.loads(request.content) if request.content else None))
                response = await super().handle_async_request(request)
                responses.append((request.method, request.url.path, response.status_code, response.headers.get("X-DeerFlow-Channel-Execution-Version")))
                return response

        owner = GatewayClientOwner(gateway_url="http://gateway.test", langgraph_url="http://gateway.test/api", transport=RecordingASGI(app=app))
        admission = Admission()
        scope = ChannelExecutionScope(PUBLICATION, OWNER, None, "chat", THREAD, owner.endpoint_id, "lead_agent", None)
        facade = ChannelExecutionFacade(owner, scope=scope, admission=admission, capture_output=True, capture_inventory=True)
        body = dict(
            assistant_id="lead_agent",
            input={"messages": [{"role": "human", "content": QUESTION}]},
            output_capture={"version": 2},
            channel_execution={"version": 1, "publication_id": PUBLICATION},
            on_disconnect="continue",
            on_completion=None,
            multitask_strategy="reject",
        )
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
            assert response.status_code == 200, response.body
            created = response.body
            run_id = created["run_id"]
            expected_admission = dict(version=1, publication_id=PUBLICATION, owner_user_id=OWNER, thread_id=THREAD, assistant_id="lead_agent", agent_name=None)
            assert created["kwargs"]["channel_execution"] == expected_admission
            assert created["kwargs"]["output_capture"] == {"version": 2}
            record = await e.manager.get(run_id, user_id=OWNER)
            assert record.task is not None
            await asyncio.wait_for(asyncio.shield(record.task), 10)
            stored = await e.repository.get(run_id, user_id=OWNER)
            expected_status = "interrupted" if case.endswith("interrupt") else "error" if case.endswith("rollback") or case == "unsupported_factory" else "success"
            assert stored["status"] == expected_status
            assert services.start_run is original_start and services.run_agent is run_agent
            expected_calls = 0 if case == "unsupported_factory" else 2 if case == "old_path_reoffer" else 1
            assert len(calls) == len(humans) == expected_calls
            assert len(factories) == 1
            assert "RunJournal" in factories[0]
            receipt = stored["kwargs"]["output_evidence"]
            if humans:
                human = humans[-1]
                assert human.id and restore_original_human_message(human).content == QUESTION
                assert human.additional_kwargs["channel_turn_id"] == PUBLICATION
                assert receipt["input_message_id"] == strip_injected_user_message_id_suffix(human.id)
            source_rows = await e.event_store.list_messages_by_run(THREAD, run_id, user_id=OWNER)
            model_rows = [row for row in source_rows if row["event_type"] == "llm.ai.response"]
            assert len(model_rows) == expected_calls
            sealed = model_rows[-1] if model_rows else None
            if case not in {"unsupported_factory", "clarification"}:
                assert sealed["content"]["content"] == ANSWER
            projection = await facade.get_run_output(run_id)
            assert projection.version == 2
            inventory_rows = await e.event_store.list_events(THREAD, run_id, event_types=["run.output_inventory"], limit=2, user_id=OWNER)
            assert len(inventory_rows) == 1, "Actual cleanup never appends another inventory"
            if case == "unsupported_factory":
                assert receipt["kind"] == "unavailable" and receipt["reason"] == "failed"
                assert projection.inventory.state == "matched"
                assert projection.inventory.document.coverage == "unsupported"
                assert projection.inventory.document.reason == "unsupported_profile"
                assert projection.inventory.document.root_epoch is None
            elif case == "old_path_reoffer":
                assert receipt["kind"] == "text"
                assert projection.inventory.state == "matched" and projection.inventory.document.coverage == "complete"
                (item,) = projection.inventory.document.items
                assert item.path == "/mnt/user-data/outputs/existing.txt"
                assert set(item.surfaces) == {"artifact_reference", "presentation_reference"}
                assert projection.inventory.document.scan.produced_paths == ()
                assert any(witness.kind == "callback" for witness in item.witnesses)
            elif case == "clarification":
                assert receipt["kind"] == "unavailable" and receipt["reason"] == "human_input_required"
                assert projection.inventory.state == "matched" and projection.inventory.document.coverage == "complete"
                (item,) = projection.inventory.document.items
                (tool_row,) = [row for row in source_rows if row["event_type"] == "llm.tool.result"]
                assert item.value_sha256 == presentation_fragment_digest(tool_row["content"]["artifact"])
                assert item.witness.kind == "root_snapshot", "Actual short-circuit has no raw tool callback"
            elif case == "same_ai_id":
                assert receipt["kind"] == "unavailable" and receipt["reason"] in {"ambiguous_source", "unsupported_output"}
                assert projection.inventory.state == "matched" and projection.inventory.document.coverage == "complete"
                (item,) = projection.inventory.document.items
                assert item.path == "/mnt/user-data/outputs/current.txt"
                assert "output_change" in item.surfaces
                assert "presentation_reference" in item.surfaces, "Actual current runtime emission overrides inherited message-ID exclusion"
                assert any(witness.kind == "root_snapshot" for witness in item.witnesses)
            elif case == "runtime_ai_mutation":
                assert receipt["kind"] == "text"
                assert projection.inventory.state == "matched" and projection.inventory.document.coverage == "complete"
                (item,) = projection.inventory.document.items
                assert item.path == "/mnt/user-data/outputs/current.txt"
                assert "presentation_reference" in item.surfaces and "output_change" in item.surfaces
                assert "presented_files" not in sealed["content"].get("additional_kwargs", {}), "The canonical model event precedes the actual same-ID host mutation"
                assert any(witness.kind == "root_snapshot" and witness.message_id == receipt["assistant_message_id"] for witness in item.witnesses)
            elif case in {"normal", "old_rich_history"}:
                assert receipt["kind"] == "text" and receipt["text_sha256"] == canonical_text_digest(ANSWER)
                assert sealed["content"]["id"] == receipt["assistant_message_id"]
                assert projection.output.state == "complete" and projection.output.source.text == ANSWER
                assert projection.inventory.state == "matched"
                assert projection.inventory.document.coverage == "complete"
                assert projection.inventory.document.items == ()
                assert projection.inventory.document.scan.before_root == projection.inventory.document.scan.after_root == "opened"
            else:
                assert receipt["kind"] == "unavailable" and receipt["reason"] == "cancelled"
                assert projection.output.state == "unavailable" and projection.output.reason == "cancelled"
                if case.startswith("early_"):
                    assert projection.inventory.state == "matched"
                    assert projection.inventory.document.coverage == "incomplete"
                    assert projection.inventory.document.scan.state == "incomplete", "A scan from before actual cleanup is stale"
                else:
                    assert projection.inventory.state == "unavailable"
                    assert projection.inventory.document is None
                    assert projection.inventory.reason == "cancellation_after_inventory_not_enumerated"
            assert projection.inventory.evidence.model_dump(mode="json") == stored["kwargs"]["inventory_evidence"]
            assert projection.output.channel_execution.model_dump(mode="json") == expected_admission
            assert projection.output.output_evidence.model_dump(mode="json") == receipt
            assert sum(method == "POST" for method, _path, _body in requests) == 1
            assert responses[-1][-1] == "1"
            assert len(await e.repository.list_by_thread(THREAD, user_id=OWNER)) == 1

        finally:
            await e.manager.shutdown(timeout=2)
            await asyncio.gather(*controls, return_exceptions=True)
            await owner.stop()
            await app.state.stream_cleanups.close()
            await bridge.close()
