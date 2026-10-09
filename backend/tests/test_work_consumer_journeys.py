"""Deterministic consumers on production Work/tools/worker and reopened SQL.

The native SDK and graph are injected. This is not actual-model or quota proof.
"""

import asyncio
import json
import os
import socket
import subprocess
import sys
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from langchain_core.messages import AIMessage
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from test_agent_instances import instances as instances
from test_agent_work_records import command
from test_agent_work_records import work as work
from test_work_execution import activation
from test_work_execution import execution as execution

from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY, AgentConversations
from deerflow.agent_instances.directory import InstanceDirectory
from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.runtime import InstanceSandboxProvider
from deerflow.agent_instances.service import AgentInstances
from deerflow.agent_instances.work import AgentWork
from deerflow.agent_instances.work_execution import WorkExecution
from deerflow.agent_instances.work_execution_contract import ReportWork
from deerflow.agent_instances.work_tools import read_work_context, report_work
from deerflow.community.aio_sandbox.aio_sandbox import AioSandbox
from deerflow.config.app_config import AppConfig
from deerflow.persistence.run.model import RunChangeClockRow, RunRow
from deerflow.persistence.run.sql import RunRepository
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.worker import RunContext, run_agent
from deerflow.spaces.principals import HostPrincipalResolver
from deerflow.spaces.registry import SpaceRegistry
from deerflow.spaces.service import SpaceFiles

ROOT = Path(__file__).resolve().parents[2]
CASES = {
    "reporting": ("Prepare period review", "Identify missing reconciliation input", "Which category should this scoped review exclude?", "Warranty"),
    "operations": ("Improve the onboarding procedure", "Surface conflicting source instructions", "Which procedure revision should be used?", "Use revision B of the supplied procedure."),
    "vendor": ("Compare project quotations", "Expose missing quote delivery information", "Supply the missing delivery date", "Delivery is expected on 2026-11-10."),
    "technical": ("Review a technical installation guide", "Document supported operating assumptions", "Which supported platform should be covered?", "Cover the supplied Linux installation steps."),
}


def produce(case, home, answer=None):
    """Execute the shipped consumer; retain its ordinary output bytes."""
    out = home / "outputs" / case / ("final" if answer else "preliminary")
    if case == "reporting":
        script = ROOT / "skills/public/business-report/scripts/report.py"
        source = ROOT / "backend/tests/skills/business_report/fixtures/example_services_export_small.csv"
        args = ["build", str(source), "--period", "2026-08", "--out", str(out)]
        if answer:
            args += ["--exclude", "category=" + answer]
    else:
        consumer = "supplier-comparison" if case == "vendor" else "procedure-summary"
        script = ROOT / "examples/skills" / consumer / "scripts/build.py"
        source = ROOT / "examples/skills/inputs" / ("quotes.json" if case == "vendor" else "procedure.json")
        if case == "technical":
            source = home / "technical.json"
            source.write_text(
                json.dumps({"title": "Installation guide", "scope": "Supplied Linux setup", "steps": ["Check the supported release", "Verify service readiness"], "caveats": ["Other platforms have not been verified"]}), encoding="utf-8"
            )
        if answer:
            content = json.loads(source.read_text(encoding="utf-8"))
            if case == "vendor":
                content["suppliers"][0]["delivery"] = answer
            elif case == "operations":
                content.setdefault("caveats", []).append("Confirmed source basis: " + answer)
            else:
                content["scope"] = answer
            source = home / f"{case}-confirmed.json"
            source.write_text(json.dumps(content), encoding="utf-8")
        args = ["--input", str(source), "--output", str(out)]
    result = subprocess.run([sys.executable, "-B", str(script), *args], capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    documents = list(out.rglob("*.view.json"))
    assert documents
    return documents[0], documents[0].read_bytes()


async def reopen(service, sf):
    """Close the old engine and rebuild the service objects, retaining SQL/files."""
    old = sf.kw["bind"]
    connect_args = {}
    if old.dialect.name == "postgresql":
        async with old.connect() as connection:
            schema = await connection.scalar(text("select current_schema()"))
        connect_args = {"server_settings": {"search_path": schema}, "ssl": False}
    url = old.url
    human = service.work.agents.files.registry._resolver._human
    catalog = service.work.agents.files.catalog
    await old.dispose()
    engine = create_async_engine(url, connect_args=connect_args)
    new_sf = async_sessionmaker(engine, expire_on_commit=False)
    directory = InstanceDirectory(new_sf, human=human)
    files = SpaceFiles(SpaceRegistry(new_sf, HostPrincipalResolver(human=human, nonhuman=directory.lookup)), catalog)
    return WorkExecution(AgentWork(AgentInstances(files, directory))), new_sf


@asynccontextmanager
async def gateway_process(fixture, tmp_path):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    address = f"http://127.0.0.1:{listener.getsockname()[1]}"
    process = None
    log = (tmp_path / "gateway-process.log").open("ab")
    try:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-B",
            str(Path(__file__).with_name("_work_consumer_gateway.py")),
            "--fixture",
            str(fixture),
            "--fd",
            str(listener.fileno()),
            pass_fds=(listener.fileno(),),
            stdout=log,
            stderr=log,
            env={**os.environ, "PYTHONPATH": str(ROOT / "backend"), "DEER_FLOW_HOME": str(tmp_path / "gateway-home")},
        )
        async with httpx.AsyncClient(base_url=address, timeout=2) as client:
            for _ in range(100):
                if process.returncode is not None:
                    raise AssertionError((tmp_path / "gateway-process.log").read_text(encoding="utf-8"))
                try:
                    response = await client.get("/api/human-input")
                    if response.status_code == 200:
                        break
                except httpx.TransportError:
                    pass
                await asyncio.sleep(0.1)
            else:
                raise AssertionError("Disposable Gateway did not become ready")
            yield client
    finally:
        if process is not None and process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), 10)
            except TimeoutError:
                process.kill()
                await process.wait()
        listener.close()
        log.close()


async def process_fixture(service, sf, tmp_path):
    engine = sf.kw["bind"]
    connect_args = {}
    if engine.dialect.name == "postgresql":
        async with engine.connect() as connection:
            schema = await connection.scalar(text("select current_schema()"))
        connect_args = {"server_settings": {"search_path": schema}, "ssl": False}
    catalog = service.work.agents.files.catalog
    fixture = tmp_path / "synthetic-gateway.json"
    fixture.write_text(
        json.dumps(
            {
                "url": engine.url.render_as_string(hide_password=False),
                "connect_args": connect_args,
                "volumes": [{"spec": vars(volume.spec), "data": str(volume.data_path), "control": str(volume.control_path)} for volume in catalog.verified.values()],
            }
        ),
        encoding="utf-8",
    )
    return fixture


@pytest.mark.asyncio
@pytest.mark.parametrize("case", CASES)
async def test_consumer_work_survives_new_conversation_and_reopened_services(execution, tmp_path, monkeypatch, case):
    service, admitted, row, sf = execution
    if case == "reporting":
        for library in ("docx", "pypdf", "jinja2", "matplotlib"):
            pytest.importorskip(library)
    objective, criteria, question, answer = CASES[case]
    row = await service.work.command(actor=BOB, instance_id=admitted.instance.id, work_id=row["id"], request=command(row, "edit", objective=objective, success_criteria=criteria))
    async with sf.kw["bind"].begin() as conn:
        for model in (RunRow, RunChangeClockRow):
            await conn.run_sync(model.__table__.create)
    from sqlalchemy import select

    from deerflow.persistence.spaces.files import SpaceBackingRow

    async with sf() as session:
        backing = (await session.execute(select(SpaceBackingRow).where(SpaceBackingRow.space_id == admitted.instance.home_id))).scalar_one()
    home = service.work.agents.files.catalog.verify(backing.slot_id).data_path
    produced = []

    async def attempt(resume=False):
        nonlocal admitted
        current = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
        scope = await service.reserve(execution=admitted, work_id=row["id"], request=activation(admitted, current))
        active = replace(admitted, work=scope)
        manager = RunManager(store=RunRepository(sf))
        run = await manager.create_or_reject(active.thread_id, "lead_agent", user_id="alice", idempotency_key="agent-work:" + scope.attempt_id, metadata={"agent_work_id": row["id"], "agent_work_attempt_id": scope.attempt_id})
        with patch("deerflow.community.aio_sandbox.aio_sandbox.AioSandboxClient"):
            sandbox = AioSandbox(id="qualification", base_url="http://localhost:8080")
        provider = InstanceSandboxProvider(active, sandbox, app_config=AppConfig(sandbox={"use": "deerflow.sandbox.local.local_sandbox_provider:LocalSandboxProvider"}), skill_revision="synthetic-test")
        monkeypatch.setattr("deerflow.agent_instances.runtime.prepare_environment", AsyncMock(return_value=provider))
        runtime = SimpleNamespace(context={AGENT_EXECUTION_CONTEXT_KEY: active})

        async def report(action, statement, **extras):
            context = json.loads(await read_work_context.coroutine(runtime))
            return json.loads(await report_work.coroutine(ReportWork(operation_id=operation(), expected_revision=context["work"]["revision"], action=action, statement=statement, **extras), runtime))

        class DeterministicConsumer:
            async def astream(self, *args, **kwargs):
                context = json.loads(await read_work_context.coroutine(runtime))
                assert context["work"]["objective"] == objective
                if not resume:
                    produced.append(await asyncio.to_thread(produce, case, home))
                    await report("progress", "Prepared the supplied material; verification remains incomplete", next_action="Obtain the missing basis")
                    await report("request_input", question, purpose="information", reason="Required to assess the delegated outcome", expected_response="Supply the missing factual basis")
                else:
                    assert context["responses"][0]["text"] == answer
                    basis = {**context["request"], "response_ids": [r["id"] for r in context["responses"]]}
                    basis.pop("purpose")
                    await report("assess_input", "Supplied facts identify the basis to use", request_basis=basis)
                    assert produced[0][0].read_bytes() == produced[0][1]
                    final = await asyncio.to_thread(produce, case, home, context["responses"][0]["text"])
                    assert final[1] != produced[0][1]
                    if case == "reporting":

                        def count(view_path):
                            source = next(view_path.parent.glob("*.report.json"))
                            return json.loads(source.read_text(encoding="utf-8"))["kpis"][1]["value"]

                        assert count(final[0]) < count(produced[0][0])
                    else:
                        assert answer in final[1].decode("utf-8")
                    produced.append(final)
                    await report("suggest", "Verify the remaining source limitations in a separate assignment")
                    await report(
                        "outcome", "Prepared the requested material with the supplied basis; remaining limitations are explicit", sources=[{"space_id": admitted.instance.home_id, "path": produced[-1][0].relative_to(home).as_posix()}]
                    )
                yield {"messages": [AIMessage("Recorded current progress.")]}

        bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())
        await run_agent(
            bridge, manager, run, ctx=RunContext(checkpointer=None, event_store=None, agent_execution=active), agent_factory=lambda **_: DeterministicConsumer(), graph_input={}, config={}, thread_incarnation=active.thread_incarnation
        )
        assert run.status == "success", run.error
        return await service.work.get(actor=BOB, instance_id=active.instance.id, work_id=row["id"])

    first = await attempt()
    assert first["status"] == "blocked" and first["attempt"]["status"] == "succeeded"
    old_thread = admitted.thread_id
    service, sf = await reopen(service, sf)
    try:
        inbox = HumanInput(service.work)
        attention = await inbox.list(actor=BOB)
        assert attention["counts"]["pending"] == 1
        request = await inbox.get(actor=BOB, request_id=first["human_input_request_id"])
        fixture = await process_fixture(service, sf, tmp_path)
        async with gateway_process(fixture, tmp_path) as client:
            pending = await client.get("/api/human-input")
            assert pending.json()["counts"]["pending"] == 1
            response = await client.post(
                f"/api/human-input/{request['id']}/responses", json={"operation_id": operation(), "expected_request_revision": request["request_revision"], "expected_assignment_revision": request["assignment_revision"], "text": answer}
            )
            assert response.status_code == 200, response.text
            assert response.json()["state"] == "answered"
        # A new OS process with new services reads the exact durable response.
        async with gateway_process(fixture, tmp_path) as client:
            retained = await client.get(f"/api/human-input/{request['id']}/responses")
            assert retained.status_code == 200, retained.text
            assert [r["text"] for r in retained.json()["responses"]] == [answer]
            state = await client.get(f"/api/agent-instances/{admitted.instance.id}/work/{row['id']}")
            assert state.status_code == 200, state.text
            assert state.json()["attempt"]["id"] == first["attempt"]["id"]
        unchanged = await service.work.get(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
        assert unchanged["attempt"]["id"] == first["attempt"]["id"] and unchanged["status"] == "blocked"
        authority = AgentConversations(service.work.agents)
        chat = await authority.create(actor=ALICE, instance_id=admitted.instance.id, creation_id=operation())
        admitted = await authority.execution(actor=ALICE, thread_id=chat["thread_id"])
        assert admitted.thread_id != old_thread
        completed = await attempt(resume=True)
        assert completed["status"] == "submitted" and completed["review_state"] == "pending"
        review = await inbox.get(actor=BOB, request_id=completed["human_input_request_id"])
        accepted = await service.work.command(
            actor=BOB,
            instance_id=admitted.instance.id,
            work_id=row["id"],
            request=command(
                completed,
                "accept",
                outcome_id=completed["outcome_id"],
                evidence_revision=1,
                basis="outcome_statement",
                acknowledge_unchecked_sources=True,
                request_basis={"id": review["id"], "revision": review["revision"], "request_revision": review["request_revision"], "response_ids": []},
            ),
        )
        assert accepted["status"] == "completed" and accepted["review_state"] == "accepted"
        history = await service.work.history(actor=ALICE, instance_id=admitted.instance.id, work_id=row["id"])
        assert any(e["actor_kind"] == "nonhuman" and e["action"] == "progress" for e in history)
        assert any(e["actor_kind"] == "human" and e["actor_id"] == "bob" and e["action"] == "accept" for e in history)
        # Inspect of Work does not grant current read access to its output evidence.
        from deerflow.spaces.contract import Permission

        resource = await service.work.agents.files.registry.get(actor=ALICE, space_id=admitted.instance.home_id)
        await service.work.agents.files.registry.set_grant(actor=ALICE, space_id=resource.id, subject=BOB, permissions=Permission(0), expected_generation=resource.generation)
        redacted = await service.work.get(actor=BOB, instance_id=admitted.instance.id, work_id=row["id"])
        assert redacted["status"] == "completed"
        assert redacted["outcome"]["sources"] == [{"kind": "unavailable", "current_contents": "not_checked"}]

    finally:
        await sf.kw["bind"].dispose()
