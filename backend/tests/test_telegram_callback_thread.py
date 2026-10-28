"""Real SQL/thread SDK requests for callback-owned input, never fake messages."""

import json

import httpx
import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import delete, select
from test_telegram_callback_publication import retry_input_environment as retry_input_environment
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment

from app.channels.message_bus import InboundMessage
from app.telegram_bots.threads import ThreadCoordinator
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

pytestmark = pytest.mark.asyncio


async def test_default_thread_preparation_retains_exact_old_json_keys(coordinator_environment):
    e = coordinator_environment
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    prep = await e.db.links.reserve_thread_preparation(e.link, e.origins[0], purpose="ensure", endpoint_id=e.http.endpoint_id, check_live_owner=owner, check_attempt_current=lambda: True)
    assert set(prep.origin.route_basis["thread_preparation"]) == {"purpose", "creation_id", "endpoint_id", "scope", "expected_mapping_id", "expected_thread_id", "selected_thread_id", "mapping_id"}


@pytest.mark.parametrize("retry_input_environment", ["unprepared"], indirect=True)
async def test_callback_thread_creation_survives_original_gc_and_reuses_its_bookmark(retry_input_environment):
    e = retry_input_environment
    await e.stage()
    async with e.db.sessions.begin() as session:
        await session.execute(delete(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id))
    remote, posts = {}, []

    async def respond(request):
        if request.method == "POST":
            body = json.loads(request.content)
            posts.append(body["thread_id"])
            remote.setdefault(body["thread_id"], {"thread_id": body["thread_id"], "owner_user_id": e.link.owner_user_id, "agent_instance": None, "metadata": body["metadata"]})
            return httpx.Response(200, json=remote[body["thread_id"]])
        return httpx.Response(200, json=remote[request.url.path.rsplit("/", 1)[-1]])

    e.responder = respond
    controller = ThreadCoordinator(links=e.db.links, authority=e.live.authority, client=e.http, channel=lambda _id: e.current[0], send_allowed=lambda: e.creating[0], outbox=e.outbox, product_name="HartMesh")
    try:
        first = await controller.ensure_callback_thread(e.link, callback_id=e.callback_id)
        second = await controller.ensure_callback_thread(e.link, callback_id=e.callback_id)
        assert first == second and posts == [first]
        stage = await e.inbox.read_callback_input(e.link, e.callback_id, guard=e.guard_factory())
        assert stage.thread_preparation["owner_callback_id"] == e.callback_id
        assert stage.thread_preparation["creation_id"] == first
        assert stage.thread_preparation["selected_thread_id"] == first
        assert await e.db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id), None) == first
        async with e.db.sessions() as session:
            assert list(await session.scalars(select(TelegramInboundMarkRow))) == []
    finally:
        await controller.stop()


async def test_real_topic_thread_mapping_does_not_replace_general(coordinator_environment):
    from datetime import UTC, datetime

    from telegram import Chat, Message, Update, User

    e = coordinator_environment
    incoming = Update(4991, message=Message(991, datetime.now(UTC), Chat(e.link.chat_id, "private"), from_user=User(e.link.telegram_user_id, "Topic human", False), text="Topic input", message_thread_id=77))
    row, _ = await e.inbox.accept(e.link, incoming, sanitized_input={"type": "text", "text": "Topic input"})
    row = await e.inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
    await e.db.connections.set_thread_id(connection_id=e.link.connection_id, owner_user_id=e.link.owner_user_id, provider="telegram", external_conversation_id=str(e.link.chat_id), thread_id="general-thread")
    remote = {}

    async def respond(request):
        if request.method == "POST":
            body = json.loads(request.content)
            remote[body["thread_id"]] = {"thread_id": body["thread_id"], "owner_user_id": e.link.owner_user_id, "agent_instance": None, "metadata": body["metadata"]}
            return httpx.Response(200, json=remote[body["thread_id"]])
        return httpx.Response(200, json=remote[request.url.path.rsplit("/", 1)[-1]])

    e.responder = respond
    controller = ThreadCoordinator(links=e.db.links, authority=e.live.authority, client=e.http, channel=lambda _id: e.current[0], send_allowed=lambda: e.creating[0], outbox=e.outbox, product_name="HartMesh")
    inbound = InboundMessage("telegram", str(e.link.chat_id), str(e.link.telegram_user_id), "Topic input", connection_id=e.link.connection_id, owner_user_id=e.link.owner_user_id, workspace_id=str(e.link.bot_id), topic_id="77")
    try:
        thread_id = await controller.ensure_thread(e.link, inbound, origin=row)
        assert await e.db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id), "77") == thread_id
        assert await e.db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id), None) == "general-thread"
    finally:
        await controller.stop()


@pytest.mark.asyncio
async def test_grouped_custom_deletion_has_no_single_origin_side_effect(db, monkeypatch):
    from sqlalchemy import select
    from test_telegram_bot_handlers import message
    from test_telegram_threads import close, environment, inbound

    from deerflow.config import agents_config
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow

    e = await environment(db, monkeypatch, lead=False)
    try:
        row, _ = await e.inbox.accept(e.link, message(e, 8991, content="original safe text"), sanitized_input={"type": "text", "text": "original safe text"})
        row = await e.inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
        owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
        guard = db.links.publication_guard(e.link, check_live_owner=owner, check_attempt_current=lambda: e.active[0])
        grouped = await e.inbox.group_prepublication((row,), guard=guard)
        async with db.sessions() as session:
            before = set(await session.scalars(select(TelegramOutboxRow.id)))

        def missing(*args, **kwargs):
            raise FileNotFoundError("synthetic missing target")

        monkeypatch.setattr(agents_config, "load_agent_config", missing)
        with pytest.raises(ValueError) as error:
            await e.threads.ensure_thread(e.link, inbound(e), origin=grouped[0])
        assert getattr(error.value, "kind", None) == "custom_agent_deleted"
        async with db.sessions() as session:
            actual = await session.get(TelegramInboundMarkRow, (e.link.id, row.message_id, row.chat_id))
            assert not actual.route_complete and actual.outbox_id is None
            assert set(await session.scalars(select(TelegramOutboxRow.id))) == before
        assert not e.http_calls
    finally:
        await close(e)


@pytest.mark.asyncio
async def test_actual_create_507_is_typed_without_reading_error_text(db, monkeypatch):
    from test_telegram_threads import close, environment, inbound, origin

    async def full(e, request):
        if request.method == "POST":
            return httpx.Response(507, json={"detail": "opaque synthetic body"})

    e = await environment(db, monkeypatch, responder=full)
    try:
        with pytest.raises(Exception) as error:
            await e.threads.ensure_thread(e.link, inbound(e), origin=await origin(e))
        assert getattr(error.value, "kind", None) == "files_full"
        assert len(e.http_calls) == 1 and e.http_calls[0].method == "POST"
    finally:
        await close(e)


async def _retire_on_preparation_flush(e, monkeypatch, *, callback=False, reserve=False):
    import asyncio

    from sqlalchemy.ext.asyncio import AsyncSession

    from deerflow.persistence.channel_connections.model import ChannelConversationRow
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    selected = [True]
    owner = await e.live.authority.prepare_live_owner(e.link.owner_user_id)
    stage = await e.stage() if callback else None
    source = stage.origins[0] if stage else e.origins[0]
    extra = {"owner_callback_id": e.callback_id} if callback else {}
    kwargs = dict(purpose="ensure", endpoint_id=e.http.endpoint_id, check_live_owner=owner, check_attempt_current=lambda: selected[0], **extra)
    prep = None if reserve else await e.db.links.reserve_thread_preparation(e.link, source, **kwargs)
    entered, release = asyncio.Event(), asyncio.Event()
    actual = AsyncSession.flush

    async def held(session, *args, **kwargs):
        wait = False
        for row in session.dirty:
            if callback and isinstance(row, TelegramCallbackRow) and row.id == e.callback_id:
                saved = row.payload.get("publication_owner", {}).get("thread_preparation")
            elif not callback and isinstance(row, TelegramInboundMarkRow) and row.link_id == source.link_id and row.message_id == source.message_id:
                saved = row.route_basis.get("thread_preparation")
            else:
                continue
            wait = isinstance(saved, dict) and (reserve or saved.get("selected_thread_id") is not None)
            if wait:
                break
        result = await actual(session, *args, **kwargs)
        if wait and not entered.is_set():
            entered.set()
            await release.wait()
        return result

    monkeypatch.setattr(AsyncSession, "flush", held)
    if reserve:
        task = asyncio.create_task(e.db.links.reserve_thread_preparation(e.link, source, **kwargs))
    else:
        task = asyncio.create_task(e.db.links.commit_thread_mapping(e.link, prep, thread_id=prep.creation_id, endpoint_id=e.http.endpoint_id, check_live_owner=owner, check_attempt_current=lambda: selected[0]))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        selected[0] = False
        release.set()
        if reserve:
            with pytest.raises(ValueError):
                await task
        else:
            assert await task is False
        async with e.db.sessions() as session:
            mappings = list(await session.scalars(select(ChannelConversationRow).where(ChannelConversationRow.connection_id == e.link.connection_id)))
            assert not mappings
            if callback:
                row = await session.get(TelegramCallbackRow, e.callback_id)
                saved = row.payload["publication_owner"]["thread_preparation"]
                assert saved["selected_thread_id"] is None
            else:
                row = await session.get(TelegramInboundMarkRow, (source.link_id, source.message_id, source.chat_id))
                saved = row.route_basis.get("thread_preparation")
                assert saved is None if reserve else saved["selected_thread_id"] is None
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("reserve", [False, True])
async def test_normal_preparation_final_flush_retires_original_attempt(coordinator_environment, monkeypatch, reserve):
    await _retire_on_preparation_flush(coordinator_environment, monkeypatch, reserve=reserve)


@pytest.mark.parametrize("retry_input_environment", ["unprepared"], indirect=True)
async def test_callback_mapping_final_flush_retires_original_attempt(retry_input_environment, monkeypatch):
    await _retire_on_preparation_flush(retry_input_environment, monkeypatch, callback=True)


@pytest.mark.parametrize("retry_input_environment", ["unprepared"], indirect=True)
@pytest.mark.parametrize("bookmark", ["preparation", "source_publication"])
async def test_chained_callback_retry_keeps_actual_owner_bookmark_across_new_mapping(retry_input_environment, bookmark):
    from types import MappingProxyType

    from test_telegram_publication_observation import run_row

    from app.channels.execution_client import ValidatedRun
    from app.gateway.channel_execution import receipt_from_run_kwargs
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = retry_input_environment
    await e.stage()
    remote, posts = {}, []

    async def respond(request):
        if request.method == "POST":
            body = json.loads(request.content)
            posts.append(body["thread_id"])
            remote[body["thread_id"]] = {"thread_id": body["thread_id"], "owner_user_id": e.link.owner_user_id, "agent_instance": None, "metadata": body["metadata"]}
            return httpx.Response(200, json=remote[body["thread_id"]])
        return httpx.Response(200, json=remote[request.url.path.rsplit("/", 1)[-1]])

    e.responder = respond
    controller = ThreadCoordinator(links=e.db.links, authority=e.live.authority, client=e.http, channel=lambda _id: e.current[0], send_allowed=lambda: e.creating[0], outbox=e.outbox, product_name="HartMesh")
    controls = [[{"label": "label.try_again", "kind": "retry_message", "target_kind": "thread", "target_id": e.thread_id, "payload": {}}]]

    async def adopt(parent, message_id):
        async with e.db.sessions() as session:
            row = await session.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == parent))
            identity, group_id = row.id, row.group_id
        assert await e.callbacks.adopt(e.link, group_id, message_id, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=parent, presentation_version=0, check_current=lambda: True)
        return await e.inbox.stage_callback_input(e.link, callback_id=identity, update_id=message_id + 2000, chat_id=e.link.chat_id, message_id=message_id, message_thread_id=None, callbacks=e.callbacks, guard=e.guard_factory())

    try:
        original_thread = await controller.ensure_callback_thread(e.link, callback_id=e.callback_id)
        saved = await e.inbox.read_callback_input(e.link, e.callback_id, guard=e.guard_factory())
        creation_id = saved.thread_preparation["creation_id"]
        input_owner = e.callback_id
        if bookmark == "source_publication":
            # SQL/source protocol fixture: no model-creating POST is claimed.
            record = await e.inbox.reserve_callback_publication(e.link, input_owner, thread_id=original_thread, endpoint_id=e.http.endpoint_id, guard=e.guard_factory())
            await e.inbox.mark_dispatch_started(record, guard=e.guard_factory())
            record = await e.inbox.bind_publication_run(record, run_id="original-callback-run", guard=e.guard_factory())
            run = run_row(record, run_id=record.run_id)
            run.update(status="error", metadata={"channel_turn_id": record.publication_id})
            parent = await e.outbox.plan_publication_outcome(
                record, run=ValidatedRun(MappingProxyType(run), receipt_from_run_kwargs(run["kwargs"])), source_ref={"key": "error.generic", "values": {}}, keyboard=controls, callbacks=e.callbacks, guard=e.guard_factory()
            )
            interim = await adopt(parent, 1902)
            assert interim.record is None and interim.thread_id == original_thread
            input_owner = interim.callback_id
        parent = await e.outbox.plan_input_outcome(
            e.link, source_id="owner-bookmark:" + input_owner, source_ref={"key": "error.generic", "values": {}}, keyboard=controls, callbacks=e.callbacks, guard=e.guard_factory(), input_owner_callback_id=input_owner
        )
        async with e.db.sessions.begin() as session:
            await session.execute(delete(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id))
        remote["new-mapping-thread"] = {"thread_id": "new-mapping-thread", "owner_user_id": e.link.owner_user_id, "agent_instance": None, "metadata": {"channel_agent_name": e.link.agent_ref}}
        await e.db.connections.set_thread_id(connection_id=e.link.connection_id, owner_user_id=e.link.owner_user_id, provider="telegram", external_conversation_id=str(e.link.chat_id), thread_id="new-mapping-thread")
        next_stage = await adopt(parent, 1903)
        if bookmark == "preparation":
            assert next_stage.thread_preparation is not None
            assert next_stage.thread_preparation["creation_id"] == creation_id
            assert next_stage.thread_preparation["owner_callback_id"] == next_stage.callback_id
        else:
            assert next_stage.thread_id == original_thread
        assert await controller.ensure_callback_thread(e.link, callback_id=next_stage.callback_id) == original_thread
        assert posts == [original_thread]
        assert await e.db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id), None) == "new-mapping-thread"
        assert next_stage.origins == saved.origins
    finally:
        await controller.stop()
