"""Actual instance publication consumes Home receipts instead of legacy uploads."""

from types import SimpleNamespace

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select, update
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_puts import runner
from test_telegram_instance_publication import publication as publication

from app.telegram_bots.publication import PublicationRefused
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

pytestmark = pytest.mark.asyncio


async def prepared(e, monkeypatch, *, install=True):
    service, origin, file, args, *rest = await runner(e, monkeypatch, text="Read this document")
    saved = await service.put(e.link, origin, file, **args)

    async def admit(view, *, work):
        assert view.id == e.link.agent_ref and work is False
        # Publication now admits chat execution; the earlier save was tested
        # with a resident object that refuses every access.

    e.file_service.residents = SimpleNamespace(admit=admit)

    from app.telegram_bots.supervisor import TelegramSupervisor

    services = e.channel._services
    services.file_puts, services.authority = service, e.live.authority

    async def receipts(link, *, current=None):
        return await TelegramSupervisor.instance_file_receipts(services, link, current=current)

    if install:
        e.coordinator.instance_file_receipts = receipts
    return service, origin, file, saved


async def test_saved_file_reaches_actual_bus_in_web_composer_shape(file_admission, monkeypatch):
    e = file_admission
    service, origin, file, saved = await prepared(e, monkeypatch)
    record = await e.coordinator.dispatch(e.link, [origin], thread_id=e.thread_id)
    sent = e.bus.inbound_queue.get_nowait()
    assert sent.files == []
    assert sent.text == "Read this document" and sent.execution_context.scope.instance_id == e.link.agent_ref
    assert sent.metadata["human_message_kwargs"]["files"] == [{"filename": "input.txt", "size": 4, "path": "/mnt/user-data/" + saved.result["path"], "status": "uploaded"}]
    assert record.dispatch_started


async def test_instance_files_never_fall_back_to_legacy_reader_without_receipt_service(file_admission, monkeypatch):
    e = file_admission
    service, origin, file, saved = await prepared(e, monkeypatch, install=False)
    with pytest.raises(PublicationRefused):
        await e.coordinator.dispatch(e.link, [origin], thread_id=e.thread_id)
    assert e.bus.inbound_queue.empty()


async def test_home_change_after_composer_read_refuses_publication_mark(file_admission, monkeypatch):
    e = file_admission
    service, origin, file, saved = await prepared(e, monkeypatch)
    original = e.inbox.mark_dispatch_started

    async def changed(record, *, guard):
        async with e.db.sessions.begin() as writer:
            await writer.execute(update(SpaceRow).where(SpaceRow.id == e.home.id).values(generation=SpaceRow.generation + 1))
        return await original(record, guard=guard)

    monkeypatch.setattr(e.inbox, "mark_dispatch_started", changed)
    with pytest.raises(PublicationRefused):
        await e.coordinator.dispatch(e.link, [origin], thread_id=e.thread_id)
    assert e.bus.inbound_queue.empty()
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id == origin.message_id))
        assert not row.route_basis["publications"][-1]["publication_execution"].get("dispatch_started", False)


def manager_endpoint(e, monkeypatch, tmp_path):
    import json

    from fastapi import Request
    from starlette.responses import Response

    from app.channels.manager import ChannelManager
    from app.channels.store import JsonChannelStore

    bodies = []

    @e.http_app.post("/api/threads/{thread_id}/runs/stream")
    async def receive(thread_id: str, request: Request):
        # Labeled execution protocol boundary: actual manager, facade, SDK,
        # owner auth, SQL grants and Home receipts stand before this endpoint.
        assert thread_id == e.thread_id and request.state.user.id == e.link.owner_user_id
        bodies.append(await request.json())
        frame = json.dumps({"messages": [{"type": "ai", "content": "Recorded"}]})
        return Response("event: values\ndata: " + frame + "\n\n", media_type="text/event-stream", headers={"X-DeerFlow-Channel-Execution-Version": "1"})

    manager = ChannelManager(e.bus, JsonChannelStore(str(tmp_path / "channels.json")))

    def forbidden(*args, **kwargs):
        raise AssertionError("An instance attachment cannot enter the legacy client or uploader")

    from app.channels import manager as module

    monkeypatch.setattr(manager, "_get_client", forbidden)
    monkeypatch.setattr(module, "_ingest_inbound_files", forbidden)
    return manager, bodies


async def test_actual_manager_sends_composer_files_in_human_message_without_upload(file_admission, monkeypatch, tmp_path):
    e = file_admission
    service, origin, file, saved = await prepared(e, monkeypatch)
    record = await e.coordinator.dispatch(e.link, [origin], thread_id=e.thread_id)
    sent = e.bus.inbound_queue.get_nowait()
    manager, bodies = manager_endpoint(e, monkeypatch, tmp_path)
    await manager._handle_chat(sent)
    assert len(bodies) == 1
    (human,) = bodies[0]["input"]["messages"]
    assert human["additional_kwargs"]["files"] == sent.metadata["human_message_kwargs"]["files"]
    assert human["additional_kwargs"]["channel_turn_id"] == record.publication_id
    assert human["content"] == "Read this document"


@pytest.mark.parametrize("change", ["home", "use", "lifetime"])
async def test_final_dispatch_writer_rechecks_composed_receipt(file_admission, monkeypatch, change):
    from app.telegram_bots import file_puts
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = file_admission
    service, origin, file, saved = await prepared(e, monkeypatch)
    original = file_puts._FileReceiptGuard.check
    calls = []

    async def changed(guard, writer):
        if type(guard.base).__module__ == "app.telegram_bots.publication":
            calls.append(True)
            if len(calls) == 2:
                if change == "home":
                    await writer.execute(update(SpaceRow).where(SpaceRow.id == e.home.id).values(generation=SpaceRow.generation + 1))
                elif change == "use":
                    await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
                else:
                    e.coordinator.begin_shutdown()
        return await original(guard, writer)

    monkeypatch.setattr(file_puts._FileReceiptGuard, "check", changed)
    with pytest.raises(PublicationRefused):
        await e.coordinator.dispatch(e.link, [origin], thread_id=e.thread_id)
    assert len(calls) == 2 and e.bus.inbound_queue.empty()
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id == origin.message_id))
        assert not row.route_basis["publications"][-1]["publication_execution"].get("dispatch_started", False)


@pytest.mark.parametrize("boundary", ["queued", "http"])
@pytest.mark.parametrize("change", ["home_generation", "home_id", "instance_generation"])
async def test_queued_instance_file_cannot_rebind_path_before_run_post(file_admission, monkeypatch, tmp_path, change, boundary):
    from app.channels.execution import ChannelExecutionProtocolError
    from deerflow.persistence.agent_instances.model import AgentInstanceRow

    e = file_admission
    service, origin, file, saved = await prepared(e, monkeypatch)
    record = await e.coordinator.dispatch(e.link, [origin], thread_id=e.thread_id)
    sent = e.bus.inbound_queue.get_nowait()
    changes = []

    async def change_target():
        changes.append(True)
        async with e.db.sessions.begin() as writer:
            if change == "home_generation":
                await writer.execute(update(SpaceRow).where(SpaceRow.id == e.home.id).values(generation=SpaceRow.generation + 1))
            elif change == "home_id":
                replacement = await writer.scalar(select(SpaceRow.id).where(SpaceRow.id != e.home.id))
                assert replacement
                await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == e.link.agent_ref).values(home_id=replacement))
            else:
                await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == e.link.agent_ref).values(generation=AgentInstanceRow.generation + 1))

    if boundary == "queued":
        await change_target()
    else:
        original_stream = e.http_client.stream_sse

        def stream(**kwargs):
            original_fence = kwargs["before_dispatch"]

            async def check():
                await change_target()
                await original_fence()

            return original_stream(**{**kwargs, "before_dispatch": check})

        monkeypatch.setattr(e.http_client, "stream_sse", stream)
    manager, bodies = manager_endpoint(e, monkeypatch, tmp_path)
    with pytest.raises(ChannelExecutionProtocolError):
        await manager._handle_chat(sent)
    assert changes == [True] and not bodies
    assert not [value for value in e.observed if value[0] == "POST" and "/runs/" in value[1]]
    async with e.db.sessions() as reader:
        row = await reader.scalar(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id == origin.message_id))
        assert row.route_basis["publications"][-1]["publication_execution"]["dispatch_started"] is True
    assert record.dispatch_started
