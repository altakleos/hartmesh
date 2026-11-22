"""Continuing a company snapshot uses the sender's existing agent admission."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest
from _storage_spaces_test_support import ALICE, BOB
from langgraph.checkpoint.memory import InMemorySaver
from sqlalchemy import select, text
from test_agent_conversation_http import app_for
from test_agent_conversations import conversations
from test_agent_instances import create
from test_agent_instances import instances as instances
from test_threads_router import _RawStateAccessor

from app.gateway.deps import get_config
from app.gateway.routers import continued_conversations as continued
from app.gateway.snapshot_manifest import SnapshotAgent
from deerflow.agent_instances.contract import AgentPermission
from deerflow.agent_instances.memory import execution_memory_enabled
from deerflow.config.memory_config import MemoryConfig
from deerflow.persistence.agent_instances.model import AgentConversationRow, AgentInstanceGrantRow, AgentInstanceRow, AgentProtectedContextRow
from deerflow.persistence.base import Base
from deerflow.persistence.models.run_event import RunEventRow, RunEventThreadSeqRow
from deerflow.persistence.run.model import CompletedRunSnapshotRow, RunChangeClockRow, RunProducerPolicyRow, RunRow
from deerflow.persistence.run.sql import RunRepository
from deerflow.runtime.events.store.db import DbRunEventStore
from deerflow.runtime.runs.manager import RunManager


@pytest.mark.asyncio
async def test_use_only_continue_seeds_its_own_conversation_under_strict_fencing(instances, monkeypatch, tmp_path):
    import asyncio

    import yaml
    from langgraph.store.memory import InMemoryStore

    from app.gateway import deps
    from deerflow.agent_instances import runtime
    from deerflow.config.app_config import AppConfig, pop_current_app_config, push_current_app_config
    from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    async with sf() as session, session.begin():
        grant = await session.get(AgentInstanceGrantRow, (agent.id, ALICE.subject_id))
        grant.permissions = int(AgentPermission.USE)
    async with sf.kw["bind"].begin() as conn:
        await conn.run_sync(Base.metadata.create_all, tables=[m.__table__ for m in (RunRow, RunChangeClockRow, RunEventRow, RunEventThreadSeqRow, CompletedRunSnapshotRow, RunProducerPolicyRow)])
        await conn.execute(text("INSERT INTO run_producer_policy(id,strict,cutover_id) VALUES (1,true,'continued-agent')"))
    app = app_for(agents, authority, threads, monkeypatch, actor=ALICE)
    app.include_router(continued.router)
    settings = {
        "models": [{"name": "model", "use": "langchain_openai:ChatOpenAI", "model": "fixture", "api_key": "test"}],
        "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
        "skills": {"path": str(tmp_path / "skills")},
        "title": {"enabled": False},
        "memory": {"enabled": False},
        "run_events": {"backend": "db", "producer_fencing": "strict", "producer_cutover_id": "continued-agent"},
    }
    config = AppConfig.model_validate(settings)
    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.safe_dump(settings), encoding="utf-8")
    monkeypatch.setenv("DEER_FLOW_CONFIG_PATH", str(config_file))
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / "home"))
    monkeypatch.setattr(deps, "get_config", lambda: config)
    admitted = asyncio.get_running_loop().create_future()

    async def observe_workspace_admission(execution, *, app_config, run_id):
        admitted.set_result((execution, run_id))
        raise RuntimeError("Test ends at native workspace allocation")

    monkeypatch.setattr(runtime, "prepare_environment", observe_workspace_admission)
    app.state.store = InMemoryStore()
    app.state.stream_bridge = MemoryStreamBridge()
    app.state.run_events_config = config.run_events
    app.state.checkpointer = InMemorySaver()
    app.state.run_event_store = DbRunEventStore(sf, producer_fencing="strict", producer_cutover_id="continued-agent")
    app.state.run_store = RunRepository(sf)
    app.state.run_manager = RunManager(store=app.state.run_store, event_store=app.state.run_event_store, worker_id="continue-agent-test")
    app.dependency_overrides[get_config] = lambda: SimpleNamespace()
    monkeypatch.setattr(continued, "build_checkpoint_state_mutation_accessor", lambda request, *, thread_id, as_node: (_RawStateAccessor(app.state.checkpointer), {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}))
    monkeypatch.setattr(continued, "acting_user_id", lambda request: ALICE.subject_id)
    agent_ref = SnapshotAgent(kind="employee", id=agent.id, name="Earlier name")

    async def snapshot(request, publication_id):
        from app.gateway.conversation_snapshots import snapshot_answerer

        return SimpleNamespace(
            title="Shared notes",
            with_agent=agent_ref,
            messages=[SimpleNamespace(type="human", content="Earlier question"), SimpleNamespace(type="ai", content="Earlier answer")],
            files=[],
            answerer=await snapshot_answerer(request, agent_ref),
        )

    monkeypatch.setattr(continued, "read_snapshot", snapshot)
    url = "/api/shared-conversations/verified-publication/continue"
    request_id = str(uuid4())
    body = {"request_id": request_id, "answerer": {"kind": "agent", "id": agent.id}}
    push_current_app_config(config)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
            mismatch = await client.post(url, json={**body, "answerer": {"kind": "assistant"}})
            assert mismatch.status_code == 409 and mismatch.json()["detail"]["answerer"] == {"kind": "agent", "id": agent.id, "name": agent.name}
            async with sf() as session:
                assert (await session.scalars(select(AgentConversationRow))).all() == []
            write_events = app.state.run_event_store.put_batch

            async def interrupted_seed(rows):
                await write_events(rows[:1])
                raise OSError("Fixture stopped after the first durable seed event")

            with monkeypatch.context() as patch:
                patch.setattr(app.state.run_event_store, "put_batch", interrupted_seed)
                interrupted = await client.post(url, json=body)
                assert interrupted.status_code == 503
            reply = await client.post(url, json=body)
            assert reply.status_code == 200, reply.text
            result = reply.json()
            assert result["answerer"] == {"kind": "agent", "id": agent.id, "name": agent.name}
            target = result["thread_id"]
            assert (await client.post(url, json=body)).json() == result
            async with sf() as session:
                binding = (await session.scalars(select(AgentConversationRow))).one()
                assert binding.thread_id == target and binding.instance_id == agent.id and binding.requester_id == ALICE.subject_id
                assert await session.get(AgentProtectedContextRow, target) is None
            actor_execution = await authority.execution(actor=ALICE, thread_id=target)
            assert not actor_execution.memory_audience_allowed
            events = await app.state.run_event_store.list_messages(target, user_id=None)
            assert [e["content"]["content"] for e in events] == ["Earlier question", "Earlier answer"]
            assert all(e["metadata"]["shared_history"] == "verified-publication" for e in events)
            assert (await client.get(f"/api/threads/{target}/shared-history")).json()["publication_id"] == "verified-publication"

            async def first_admission(target):
                nonlocal admitted
                admitted = asyncio.get_running_loop().create_future()
                sent = await client.post(
                    f"/api/threads/{target}/runs", json={"assistant_id": "lead_agent", "input": {"messages": [{"role": "user", "id": "new-question", "content": "Please continue."}]}, "context": {"thinking_enabled": False}}
                )
                assert sent.status_code == 200, sent.text
                execution, run_id = await asyncio.wait_for(admitted, timeout=10)
                assert run_id == sent.json()["run_id"]
                assert execution.instance.id == agent.id and execution.requester == ALICE and execution.thread_id == target
                return execution

            use_only = await first_admission(target)
            assert not use_only.memory_audience_allowed and not execution_memory_enabled(use_only, MemoryConfig())
            async with sf.begin() as session:
                (await session.get(AgentInstanceGrantRow, (agent.id, ALICE.subject_id))).permissions = int(AgentPermission.USE | AgentPermission.INSPECT)
            inspected = await client.post(url, json={**body, "request_id": str(uuid4())})
            assert inspected.status_code == 200, inspected.text
            inspected_target = inspected.json()["thread_id"]
            assert inspected_target != target
            full_view = await first_admission(inspected_target)
            assert full_view.memory_audience_allowed and execution_memory_enabled(full_view, MemoryConfig())
            async with sf.begin() as session:
                (await session.get(AgentInstanceGrantRow, (agent.id, ALICE.subject_id))).permissions = int(AgentPermission.USE)

            other_id = str(uuid4())
            proposed = continued._stable_id("continue-agent-conversation", ALICE.subject_id, "verified-publication", other_id, agent.id)
            earlier = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=UUID(proposed).hex)
            refused = await client.post(url, json={**body, "request_id": other_id})
            assert refused.status_code == 409 and refused.json()["detail"]["code"] == "conversation_changed"
            assert earlier["thread_id"] != proposed
            assert await app.state.run_event_store.list_messages(earlier["thread_id"], user_id=None) == []
            with monkeypatch.context() as patch:
                probe = AsyncMock(return_value=SimpleNamespace(execution="unavailable", reason="operator-hold"))
                patch.setattr(agents, "execution_probe", probe)
                unavailable = await client.post(url, json={**body, "request_id": str(uuid4())})
                assert unavailable.status_code == 409
                assert unavailable.json()["detail"]["answerer"] == {"kind": "assistant", "reason": "unavailable"}
            with monkeypatch.context() as patch:
                patch.setattr("app.gateway.routers.agent_instances.get_agents_api_config", lambda: SimpleNamespace(enabled=False))
                disabled = await client.post(url, json={**body, "request_id": str(uuid4())})
                assert disabled.status_code == 409
                assert disabled.json()["detail"]["answerer"] == {"kind": "assistant", "reason": "unavailable"}
            for status, reason in (("suspended", "paused"), ("archived", "retired")):
                async with sf() as session, session.begin():
                    (await session.get(AgentInstanceRow, agent.id)).status = status
                stale = await client.post(url, json={**body, "request_id": str(uuid4())})
                assert stale.status_code == 409 and stale.json()["detail"]["answerer"] == {"kind": "assistant", "reason": reason}
            async with sf() as session, session.begin():
                await session.delete(await session.get(AgentInstanceGrantRow, (agent.id, ALICE.subject_id)))
            mismatch = await client.post(url, json={**body, "request_id": str(uuid4())})
            assert mismatch.status_code == 409 and mismatch.json()["detail"] == {"code": "answerer_changed", "answerer": {"kind": "assistant", "reason": "no_access"}}
            async with sf() as session:
                assert len((await session.scalars(select(AgentConversationRow))).all()) == 3
    finally:
        await app.state.run_manager.shutdown()
        pop_current_app_config()


@pytest.mark.asyncio
async def test_company_snapshot_import_uses_the_live_manifest_and_original_home_receipt(instances, monkeypatch):
    import json
    from contextlib import AsyncExitStack

    from deerflow_extension_api import ExtensionRuntimeDeps
    from test_agent_file_http import file_app
    from test_agent_files import backing_root
    from test_compose_native_file_sources import native_request

    from app.gateway.routers import agent_home, shared_conversations
    from app.gateway.snapshot_manifest import MANIFEST_SOURCE, TRANSCRIPT_SOURCE, SnapshotMember, digest_of_bytes, freeze_manifest
    from deerflow.extensions.loader import load_extensions
    from deerflow.features.plugins import with_default_features
    from deerflow.features.resources import StorageFeatureLinkRow
    from deerflow.persistence.shared_publications.model import SharedPublicationRow
    from deerflow.persistence.shared_publications.sql import SharedPublicationRepository
    from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
    from deerflow.spaces.contract import Custody, MutationMode, Permission
    from deerflow.spaces.facade import HostStorageProvider

    app, agents, _, authority, sf = await file_app(instances, monkeypatch)
    files, catalog = instances[1], instances[3]
    agent = await create(agents, custody="company", supervisor=BOB)
    async with sf() as session, session.begin():
        grant = await session.get(AgentInstanceGrantRow, (agent.id, ALICE.subject_id))
        grant.permissions = int(AgentPermission.USE)
    async with sf.kw["bind"].begin() as conn:
        await conn.run_sync(Base.metadata.create_all, tables=[StorageFeatureLinkRow.__table__, SharedPublicationRow.__table__])
    installed, diagnostics = load_extensions(with_default_features([]))
    assert not diagnostics
    app.state.extensions = installed
    repo = app.state.shared_publications_repo = SharedPublicationRepository(sf)
    app.state.checkpointer = InMemorySaver()
    app.include_router(continued.router)
    app.include_router(shared_conversations.router)
    app.dependency_overrides[get_config] = lambda: SimpleNamespace()
    monkeypatch.setattr(agent_home, "get_auth_config", lambda: SimpleNamespace(jwt_secret="synthetic fixture signing key"))
    monkeypatch.setattr(continued, "acting_user_id", lambda request: ALICE.subject_id)
    monkeypatch.setattr(continued, "build_checkpoint_state_mutation_accessor", lambda request, *, thread_id, as_node: (_RawStateAccessor(app.state.checkpointer), {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}))
    async with AsyncExitStack() as stack:
        for _, service in installed.services:
            await service.start(ExtensionRuntimeDeps(app_store=installed.app_store, session_factory=sf, storage=HostStorageProvider(lambda: files)))
            stack.push_async_callback(service.stop)
        company = await files.registry.host_create(name="Company", custody=Custody.company(), mode=MutationMode.NATIVE, feature=None, kind="company")
        await files.registry.host_grant(company, ALICE, Permission.READ | Permission.EXPORT)
        native_request(app)
        async with sf() as session:
            slot = await session.scalar(select(SpaceBackingRow.slot_id).where(SpaceBackingRow.space_id == company))
        shared = catalog.verified[slot].data_path / "shared"
        home = await backing_root(instances, agent)
        (home / "uploads").mkdir(exist_ok=True)
        (home / "uploads/notes.txt").write_text("Existing employee note")
        data = b"Words released to the company"
        member = SnapshotMember(path="files/notes.txt", size=len(data), sha256=digest_of_bytes(data))
        frozen = freeze_manifest("Shared note", [{"type": "human", "content": "Read this"}], [member])
        doc = frozen.manifest.model_dump(by_alias=True, exclude_none=True)
        doc["with"] = {"kind": "employee", "id": agent.id, "name": "Old name"}
        content = json.dumps(doc).encode()
        folder = "Conversations/Shared note · " + digest_of_bytes(content)[:8]
        (shared / folder / "files").mkdir(parents=True)
        for leaf, raw, origin in [(member.path, data, "/mnt/user-data/outputs/notes.txt"), ("transcript.md", frozen.transcript, TRANSCRIPT_SOURCE), ("conversation.json", content, MANIFEST_SOURCE)]:
            (shared / folder / leaf).write_bytes(raw)
            row = await repo.record_publication(path=folder + "/" + leaf, size=len(raw), sha256=digest_of_bytes(raw), published_by=BOB.subject_id, from_thread_id="private-original", from_path=origin)
            if leaf == member.path:
                member_id = row["publication_id"]
        publication = row["publication_id"]
        url = f"/api/shared-conversations/{publication}"
        client = await stack.enter_async_context(httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}))
        body = {"request_id": str(uuid4()), "answerer": {"kind": "agent", "id": agent.id}}
        async with sf.begin() as session:
            await session.delete(await session.get(AgentInstanceGrantRow, (agent.id, ALICE.subject_id)))
        ungranted = await client.get(url)
        assert ungranted.status_code == 200
        assert ungranted.json()["with"]["id"] == agent.id
        assert ungranted.json()["answerer"] == {"kind": "assistant", "reason": "no_access"}
        wrong_target = await client.post(url + "/continue", json=body)
        assert wrong_target.status_code == 409
        assert wrong_target.json()["detail"] == {"code": "answerer_changed", "answerer": {"kind": "assistant", "reason": "no_access"}}
        async with sf() as session:
            assert (await session.scalars(select(AgentConversationRow))).all() == []
        unrelated_id = str(uuid4())
        private_target = continued._stable_id("continue-agent-conversation", ALICE.subject_id, publication, unrelated_id, agent.id)
        await authority.create(actor=BOB, instance_id=agent.id, thread_id=private_target)
        unrelated = await client.post(url + "/continue", json={**body, "request_id": unrelated_id})
        assert unrelated.status_code == wrong_target.status_code
        assert unrelated.json() == wrong_target.json()
        async with sf.begin() as session:
            session.add(AgentInstanceGrantRow(instance_id=agent.id, user_id=ALICE.subject_id, permissions=int(AgentPermission.USE)))
        view = await client.get(url)
        assert view.status_code == 200, view.text
        assert view.json()["answerer"] == {"kind": "agent", "id": agent.id, "name": agent.name}
        quota = files.quota
        monkeypatch.setattr(files, "quota", AsyncMock(return_value={"available_bytes": 1, "available_inodes": 100}))
        full = await client.post(url + "/continue", json=body)
        assert full.status_code == 200, full.text
        assert full.json()["files"] == [{"path": member.path, "status": "not_copied", "reason": "storage_full"}]
        monkeypatch.setattr(files, "quota", quota)
        first = await client.post(url + "/continue", json=body)
        assert first.status_code == 200, first.text
        result = first.json()
        assert result["thread_id"] == full.json()["thread_id"]
        assert (await client.post(url + "/continue", json=body)).json() == result
        attachment = result["files"][0]["attachment"]
        assert attachment["filename"] != "notes.txt" and (home / "uploads" / attachment["filename"]).read_bytes() == data
        assert attachment["source"] == {"kind": "shared_publication", "publication_id": member_id, "sha256": member.sha256}
        async with sf() as session:
            ops = (await session.scalars(select(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id == agent.home_id))).all()
            assert len([op for op in ops if op.request.get("source") == {"kind": "shared_publication", "publication_id": member_id}]) == 1
        apply = agents.agent_files.apply

        async def pause_after_home_write(**kwargs):
            receipt = await apply(**kwargs)
            async with sf.begin() as session:
                (await session.get(AgentInstanceRow, agent.id)).status = "suspended"
            return receipt

        # A later conversation gets its own collision-safe copy. Change the
        # previously available identical bytes to force the physical write.
        (home / "uploads" / attachment["filename"]).write_bytes(b"An intervening Home edit")
        late_body = {**body, "request_id": str(uuid4())}
        with monkeypatch.context() as patch:
            patch.setattr(agents.agent_files, "apply", pause_after_home_write)
            changed = await client.post(url + "/continue", json=late_body)
        assert changed.status_code == 409 and changed.json()["detail"] == {"code": "continuation_interrupted"}
        after_write = {path.name: path.read_bytes() for path in (home / "uploads").iterdir()}
        assert len(after_write) == 3 and list(after_write.values()).count(data) == 1
        retry = await client.post(url + "/continue", json=late_body)
        assert retry.status_code == 409 and retry.json()["detail"] == {"code": "continuation_interrupted"}
        assert {path.name: path.read_bytes() for path in (home / "uploads").iterdir()} == after_write
        async with sf.begin() as session:
            (await session.get(AgentInstanceRow, agent.id)).status = "active"
        resumed = await client.post(url + "/continue", json=late_body)
        assert resumed.status_code == 200, resumed.text
        assert resumed.json()["files"][0]["attachment"]["filename"] in after_write

        # Removing the private chat does not retire its agent Home files or
        # the binding that prevents recreating the original destination.
        old_target = resumed.json()["thread_id"]
        await app.state.thread_store.delete(old_target, user_id=BOB.subject_id)
        assert await app.state.thread_store.get(old_target, user_id=None) is None
        assert await authority.binding(old_target) == agent.id
        async with sf.begin() as session:
            (await session.get(AgentInstanceRow, agent.id)).status = "suspended"
        after_delete = await client.post(url + "/continue", json=late_body)
        assert after_delete.status_code == 409 and after_delete.json()["detail"] == {"code": "continuation_interrupted"}
        assert {path.name: path.read_bytes() for path in (home / "uploads").iterdir()} == after_write
        async with sf.begin() as session:
            (await session.get(AgentInstanceRow, agent.id)).status = "active"

        async def retire_before_admission(**kwargs):
            await repo.record_removal(publication_id=publication, removed_by=BOB.subject_id)
            return await apply(**kwargs)

        monkeypatch.setattr(agents.agent_files, "apply", retire_before_admission)
        refused = await client.post(url + "/continue", json={**body, "request_id": str(uuid4())})
        assert refused.status_code == 200, refused.text
        assert refused.json()["files"] == [{"path": member.path, "status": "not_copied", "reason": "no_longer_shared"}]
        assert {path.name: path.read_bytes() for path in (home / "uploads").iterdir()} == after_write
