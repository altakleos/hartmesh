"""Work result files use canonical outcome sources and current human grants."""

from dataclasses import replace
from uuid import uuid4

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from test_telegram_instance_file_admission import prepare_file_home
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages
from test_work_execution import activation, run_record

from deerflow.agent_instances.work_contract import DelegateWork, WorkSource
from deerflow.agent_instances.work_execution import WorkExecution
from deerflow.agent_instances.work_execution_contract import ReportWork
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.spaces.model import SpaceGrantRow
from deerflow.spaces.contract import Permission, SpaceDenied


@pytest_asyncio.fixture
async def work_exports(work_delivery, tmp_path, monkeypatch, request):
    e = await prepare_file_home(work_delivery, tmp_path, monkeypatch)
    e.http_app.state.storage_spaces = e.files
    e.instance = await e.file_service.get(actor=e.actor, instance_id=e.link.agent_ref)
    e.home = await e.files.registry.get(actor=e.instance.principal, space_id=e.home.id)
    await e.files.mkdir(actor=e.instance.principal, space_id=e.home.id, expected_generation=e.home.generation, operation_id=uuid4().hex, path="outputs")
    await e.files.write(actor=e.instance.principal, space_id=e.home.id, expected_generation=e.home.generation, operation_id=uuid4().hex, path="outputs/result.txt", content=b"verified result", create=True)
    async with e.db.sessions.begin() as writer:
        writer.add(SpaceGrantRow(space_id=e.home.id, principal_kind="human", subject_id=e.actor.subject_id, permissions=int(Permission.READ | Permission.EXPORT)))
    e.source_space = e.home
    if getattr(request, "param", None) == "external":
        from deerflow.spaces.contract import Custody, MutationMode

        e.source_space = await e.files.create(actor=e.actor, name="Shared evidence", custody=Custody.personal(e.actor), mode=MutationMode.NATIVE)
        await e.files.mkdir(actor=e.actor, space_id=e.source_space.id, expected_generation=e.source_space.generation, operation_id=uuid4().hex, path="outputs")
        await e.files.write(actor=e.actor, space_id=e.source_space.id, expected_generation=e.source_space.generation, operation_id=uuid4().hex, path="outputs/result.txt", content=b"external result", create=True)
    record = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=uuid4().hex, objective="Report the findings", success_criteria="Provide evidence"))
    conversations = e.http_app.state.agent_conversations
    chat = await conversations.create(actor=e.actor, instance_id=e.instance.id, creation_id=uuid4().hex)
    execution = await conversations.execution(actor=e.actor, thread_id=chat["thread_id"])
    scope = await WorkExecution(e.human.work).reserve(execution=execution, work_id=record["id"], request=activation(execution, record))
    execution = replace(execution, work=scope)
    run = run_record(execution, scope)
    run.user_id = e.actor.subject_id
    await scope.bind(execution, run, execution.thread_incarnation)
    current = (await scope.context(execution))["work"]
    await scope.report(
        execution, ReportWork(operation_id=uuid4().hex, expected_revision=current["revision"], action="outcome", statement="The findings are verified.", sources=[WorkSource(space_id=e.source_space.id, path="outputs/result.txt")])
    )
    await scope.finish(execution, run, settled=True)
    e.result = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=record["id"])
    e.export_calls = []
    from deerflow.spaces.filesystem import ConfinedFilesystem

    e.native_reads = []
    read = ConfinedFilesystem.read_bytes

    def reading(filesystem, path, **kwargs):
        e.native_reads.append(path)
        return read(filesystem, path, **kwargs)

    monkeypatch.setattr(ConfinedFilesystem, "read_bytes", reading)
    exported = e.files.export_file

    async def export(**kwargs):
        e.export_calls.append((kwargs["actor"], kwargs["space_id"], kwargs["path"], kwargs["max_bytes"]))
        return await exported(**kwargs)

    monkeypatch.setattr(e.files, "export_file", export)
    return e


async def prepare(e):
    from app.telegram_bots.work_file_exports import prepare_work_file_export

    return await prepare_work_file_export(e.reader, e.link, work_id=e.result["id"], outcome_id=e.result["outcome_id"], evidence_revision=e.result["outcome"]["evidence_revision"], source_index=0, current=lambda: True)


@pytest.mark.asyncio
async def test_canonical_result_file_exports_as_recipient_without_workspace(work_exports):
    e = work_exports
    access = await prepare(e)
    assert await access.read(max_bytes=50) == b"verified result"
    assert e.export_calls == [(e.actor, e.home.id, "outputs/result.txt", 50)]
    assert e.native_reads == ["outputs/result.txt"]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["outcome", "evidence", "source", "assignment", "instance_grant", "space_grant"])
async def test_changed_result_authority_prevents_native_read(work_exports, change):
    from app.telegram_bots.producer import ProductSourceRetired
    from deerflow.agent_instances.contract import AgentDenied

    e = work_exports
    access = await prepare(e)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(AgentWorkRow, e.result["id"])
        if change == "outcome":
            row.outcome_id = uuid4().hex
        elif change == "evidence":
            row.outcome = {**row.outcome, "evidence_revision": 2}
        elif change == "source":
            row.outcome = {**row.outcome, "sources": [{"kind": "space_file", "space_id": e.home.id, "path": "outputs/other.txt"}]}
        elif change == "assignment":
            row.assignment_revision += 1
            row.outcome_assignment_revision = row.assignment_revision
            row.outcome = {**row.outcome, "assignment_revision": row.assignment_revision}
        elif change == "instance_grant":
            (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.actor.subject_id))).permissions = 1
        else:
            (await writer.get(SpaceGrantRow, (e.home.id, "human", e.actor.subject_id))).permissions = int(Permission.READ)
    with pytest.raises((ProductSourceRetired, AgentDenied, SpaceDenied)):
        await access.read(max_bytes=50)
    assert not e.native_reads


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["provider", "volume", "after_read"])
async def test_result_proof_is_current_at_native_io_boundaries(work_exports, monkeypatch, boundary):
    import app.gateway.authz as authz
    import deerflow.spaces.service as service
    from app.telegram_bots.producer import ProductSourceRetired

    e = work_exports
    access = await prepare(e)
    volume, resolve, run_io = e.files._volume, authz.resolve_route_permissions, service.run_file_io
    state, changed = {}, []

    async def alter():
        writer = state["writer"]
        row = await writer.get(AgentWorkRow, e.result["id"])
        row.outcome = {**row.outcome, "evidence_revision": 2}
        await writer.flush()
        changed.append(True)

    async def validated(writer, row):
        result = await volume(writer, row)
        state["writer"] = writer
        if boundary == "volume":
            await alter()
        return result

    async def provider(*args, **kwargs):
        held = await resolve(*args, **kwargs)
        if boundary == "provider" and state and not changed:
            await alter()
        return held

    async def finished(*args, **kwargs):
        result = await run_io(*args, **kwargs)
        if boundary == "after_read" and state and not changed:
            await alter()
        return result

    monkeypatch.setattr(e.files, "_volume", validated)
    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    monkeypatch.setattr(service, "run_file_io", finished)
    with pytest.raises(ProductSourceRetired):
        await access.read(max_bytes=50)
    assert changed
    assert e.native_reads == (["outputs/result.txt"] if boundary == "after_read" else [])


@pytest.mark.asyncio
async def test_result_export_obeys_byte_limit_and_protected_dispatch(work_exports):
    from app.telegram_bots.producer import DeliveryClaimRetired
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow
    from deerflow.spaces.filesystem import FileTooLarge

    e = work_exports
    access = await prepare(e)
    with pytest.raises(FileTooLarge):
        await access.read(max_bytes=3)
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="protected", updated_by=e.actor.subject_id, updated_at=e.sender.outbox.clock()))
    with pytest.raises(DeliveryClaimRetired):
        await access.authorize(protect_content=False)
    await access.authorize(protect_content=True)


@pytest.mark.asyncio
async def test_real_delivery_claim_shares_the_final_file_snapshot(work_exports, monkeypatch):
    import app.gateway.authz as authz
    from app.telegram_bots.producer import DeliveryClaimRetired
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = work_exports
    access = await prepare(e)
    resolve, active, changed, refused = authz.resolve_route_permissions, {}, [], []

    async def provider(*args, **kwargs):
        result = await resolve(*args, **kwargs)
        if active and not changed:
            row = await active["writer"].get(TelegramOutboxRow, e.identity)
            row.claim_id = "new-owner"
            await active["writer"].flush()
            changed.append(True)
        return result

    async def deliver(**kwargs):
        capture = {key: value for key, value in kwargs.items() if key != "link_id"}
        saved = await e.sender.outbox.capture_sending_part(link_id=e.link.id, token_revision=e.link.token_revision, **capture)

        class Claim:
            def extend(self, query):
                self.column = len(query.column_descriptions)
                return query.add_columns(TelegramOutboxRow).join(TelegramOutboxRow, TelegramOutboxRow.id == saved.item.id)

            def check(self, rows):
                row = rows[0][self.column]
                current = e.sender.outbox._sending(rows[0][0], row, link_id=e.link.id, token_revision=e.link.token_revision, **{key: value for key, value in capture.items() if key != "outbox_id"})
                if current != saved:
                    raise DeliveryClaimRetired()

        async def lock(writer):
            await writer.get(TelegramOutboxRow, e.identity, with_for_update=True, populate_existing=True)
            active["writer"] = writer

        try:
            await access.read(max_bytes=50, check_delivery=lock, projection=Claim())
        except DeliveryClaimRetired:
            refused.append(True)
            raise
        finally:
            active.clear()
        raise AssertionError("Retired file delivery read bytes")

    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    e.sender.deliver_part = deliver
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert changed and refused
    assert not e.native_reads


@pytest.mark.asyncio
async def test_cancelled_export_owns_and_drains_the_native_read(work_exports, monkeypatch):
    import asyncio
    import threading

    from deerflow.spaces.filesystem import ConfinedFilesystem

    e = work_exports
    access = await prepare(e)
    loop, entered, release = asyncio.get_running_loop(), asyncio.Event(), threading.Event()
    read = ConfinedFilesystem.read_bytes

    def held(filesystem, path, **kwargs):
        loop.call_soon_threadsafe(entered.set)
        if not release.wait(10):
            raise AssertionError("Native read was not released")
        return read(filesystem, path, **kwargs)

    monkeypatch.setattr(ConfinedFilesystem, "read_bytes", held)
    task = asyncio.create_task(access.read(max_bytes=50))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert await access.read(max_bytes=50) == b"verified result"


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["read", "authorize"])
@pytest.mark.parametrize("retire", ["lifetime", "agent_service", "file_service"])
async def test_retirement_during_real_transaction_exit_drops_export(work_exports, monkeypatch, operation, retire):
    from contextlib import asynccontextmanager

    from app.telegram_bots.producer import ProductSourceRetired

    e = work_exports
    live = [True]
    access = replace(await prepare(e), current=lambda: live[0])
    admitted = e.files.registry.admitted

    @asynccontextmanager
    async def retiring(**kwargs):
        async with admitted(**kwargs) as facts:
            yield facts
        if retire == "lifetime":
            live[0] = False
        else:
            monkeypatch.setattr(e.http_app.state, "agent_instances" if retire == "agent_service" else "storage_spaces", object())

    monkeypatch.setattr(e.files.registry, "admitted", retiring)
    with pytest.raises(ProductSourceRetired):
        if operation == "read":
            await access.read(max_bytes=50)
        else:
            await access.authorize()
    assert live[0] is (retire != "lifetime")
    assert e.native_reads == (["outputs/result.txt"] if operation == "read" else [])


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["external"], indirect=True)
async def test_result_source_outside_agent_home_never_substitutes_home_bytes(work_exports):
    e = work_exports
    access = await prepare(e)
    assert await access.read(max_bytes=50) == b"external result"
    assert e.export_calls == [(e.actor, e.source_space.id, "outputs/result.txt", 50)]
    assert e.source_space.id != e.instance.home_id
