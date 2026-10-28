"""Owner-scoped preparation uses real SQL and the public SDK over recorded HTTP."""

import asyncio
import json
import threading
from dataclasses import replace
from types import SimpleNamespace

import httpx
import pytest
from _telegram_repository_test_support import code_of, rows
from _telegram_repository_test_support import db as db
from sqlalchemy import update
from test_telegram_bot_handlers import ingress, message
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.channels.message_bus import InboundMessage
from app.telegram_bots import pages
from app.telegram_bots.gateway_client import GatewayClientOwner
from app.telegram_bots.inbox import InboxRepository
from deerflow.persistence.channel_connections.model import ChannelConnectionRow
from deerflow.persistence.telegram_bots.model import TelegramAccountRow, TelegramInboundMarkRow, TelegramOutboxRow

pytestmark = pytest.mark.asyncio


async def environment(db, monkeypatch, *, lead=True, responder=None, off_loop=False):
    from app.telegram_bots.threads import ThreadCoordinator

    e = await ingress(db, monkeypatch=monkeypatch, lead=lead, off_loop=off_loop)
    # pages imports this accessor by value. Keep real page/config validation
    # bound to this fixture even when collection imported pages before ingress.
    monkeypatch.setattr(pages, "get_app_config", lambda: e.live.config)
    e.http_calls, e.remote = [], {}
    e.current = [e.channel]

    async def respond(request):
        e.http_calls.append(request)
        if responder is not None:
            answer = await responder(e, request)
            if answer is not None:
                return answer
        if request.method == "POST":
            body = json.loads(request.content)
            e.remote.setdefault(body["thread_id"], {"thread_id": body["thread_id"], "owner_user_id": request.headers["X-DeerFlow-Owner-User-Id"], "metadata": body["metadata"], "agent_instance": None})
            return httpx.Response(200, json=e.remote[body["thread_id"]])
        return httpx.Response(200, json=e.remote[request.url.path.rsplit("/", 1)[-1]])

    e.http_client = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/base/api", transport=httpx.MockTransport(respond))
    e.threads = ThreadCoordinator(links=db.links, authority=e.live.authority, client=e.http_client, channel=lambda identity: e.current[0], send_allowed=lambda: e.active[0], outbox=e.services.outbox, product_name="HartMesh")
    e.inbox = InboxRepository(db.sessions)
    return e


async def origin(e, *, identity=901):
    update = message(e, identity, content="Synthetic future text")
    row, _ = await e.inbox.accept(e.channel.link, update, sanitized_input={"text": "Synthetic future text"})
    return await e.inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"kind": e.link.agent_kind, "target": e.link.agent_ref}, operation_ids=[])


def inbound(e):
    return InboundMessage(
        channel_name="telegram",
        chat_id=str(e.link.chat_id),
        user_id=str(e.link.telegram_user_id),
        text="Synthetic future text",
        connection_id=e.link.connection_id,
        owner_user_id=e.link.owner_user_id,
        workspace_id=str(e.link.bot_id),
        metadata={"preferred_thread_id": "forged-preference"},
    )


async def close(e):
    await e.threads.stop()
    await e.http_client.stop()
    await e.channel.stop()


async def test_first_preparation_creates_as_owner_then_authoritatively_reads_and_reuses(db, monkeypatch):
    e = await environment(db, monkeypatch)
    try:
        row = await origin(e)
        identity = await e.threads.ensure_thread(e.channel.link, inbound(e), origin=row)
        assert identity != "forged-preference"
        assert [request.method for request in e.http_calls] == ["POST", "GET"]
        assert e.http_calls[-1].headers["X-DeerFlow-Require-Thread-Binding"] == "true"
        assert e.remote[identity]["metadata"] == {"channel_agent_name": "lead_agent", "channel_source": {"type": "im_channel", "provider": "telegram", "chat_id": str(e.link.chat_id), "connection_id": e.link.connection_id}}
        assert await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id), None) == identity
        assert await e.threads.ensure_thread(e.channel.link, inbound(e), origin=row) == identity
        assert sum(request.method == "POST" for request in e.http_calls) == 1
    finally:
        await close(e)


@pytest.mark.parametrize("proof", ["missing-owner", "foreign-owner", "instance", "unavailable"])
async def test_post_create_requires_actual_owner_and_authoritative_no_instance_proof(db, monkeypatch, proof):
    async def respond(e, request):
        if request.method == "GET":
            if proof == "unavailable":
                return httpx.Response(503, json={"detail": "Thread binding authority unavailable"})
            result = dict(e.remote[request.url.path.rsplit("/", 1)[-1]])
            if proof == "missing-owner":
                result.pop("owner_user_id")
            elif proof == "foreign-owner":
                result["owner_user_id"] = "another-owner"
            else:
                result["agent_instance"] = {"id": "some-instance"}
            return httpx.Response(200, json=result)

    e = await environment(db, monkeypatch, responder=respond)
    try:
        with pytest.raises((ValueError, httpx.HTTPStatusError)):
            await e.threads.ensure_thread(e.channel.link, inbound(e), origin=await origin(e))
        assert await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id), None) is None
    finally:
        await close(e)


async def test_lost_create_response_reuses_saved_uuid_without_another_remote_thread(db, monkeypatch):
    failures = [True]

    async def respond(e, request):
        if request.method == "POST" and failures[0]:
            failures[0] = False
            body = json.loads(request.content)
            e.remote[body["thread_id"]] = {"thread_id": body["thread_id"], "owner_user_id": e.link.owner_user_id, "metadata": body["metadata"], "agent_instance": None}
            raise httpx.ReadError("synthetic lost response")

    e = await environment(db, monkeypatch, responder=respond)
    try:
        row = await origin(e)
        with pytest.raises(httpx.ReadError):
            await e.threads.ensure_thread(e.channel.link, inbound(e), origin=row)
        identity = await e.threads.ensure_thread(e.channel.link, inbound(e), origin=row)
        assert set(e.remote) == {identity}
        assert [json.loads(request.content)["thread_id"] for request in e.http_calls if request.method == "POST"] == [identity, identity]
    finally:
        await close(e)


async def test_old_token_response_is_denied_fresh_token_reuses_original_creation_identity(db, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    first = [True]

    async def respond(e, request):
        if request.method == "POST" and first[0]:
            first[0] = False
            entered.set()
            await release.wait()

    e = await environment(db, monkeypatch, responder=respond)
    task = None
    try:
        row = await origin(e)
        old = e.channel.link
        task = asyncio.create_task(e.threads.ensure_thread(old, inbound(e), origin=row))
        await asyncio.wait_for(entered.wait(), 3)
        replacement = await db.links.store_token(old.id, "synthetic-new-wrapped-token", actor_id=old.owner_user_id, expected_revision=old.token_revision, token_changed=True)
        e.current[0] = SimpleNamespace(link=replacement, application=SimpleNamespace(running=True))
        release.set()
        with pytest.raises(ValueError, match="thread-attempt-changed"):
            await task
        assert await db.connections.get_thread_id(old.connection_id, str(old.chat_id), None) is None
        identity = await e.threads.ensure_thread(replacement, inbound(e), origin=row)
        assert len(e.remote) == 1 and identity in e.remote
        assert len({json.loads(request.content)["thread_id"] for request in e.http_calls if request.method == "POST"}) == 1
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await close(e)


async def test_shutdown_during_http_drains_but_never_installs_a_mapping(db, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()

    async def respond(e, request):
        if request.method == "POST":
            entered.set()
            await release.wait()

    e = await environment(db, monkeypatch, responder=respond)
    task, cleanup = None, None
    try:
        task = asyncio.create_task(e.threads.ensure_thread(e.channel.link, inbound(e), origin=await origin(e)))
        await asyncio.wait_for(entered.wait(), 3)
        e.threads.begin_shutdown()
        cleanup = asyncio.create_task(e.threads.stop())
        await asyncio.sleep(0)
        assert not cleanup.done() and not e.http_client.closed
        release.set()
        with pytest.raises(ValueError, match="thread-attempt-changed"):
            await task
        await cleanup
        assert not e.http_client.closed
        await e.http_client.stop()
        assert e.http_client.closed
        assert await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id), None) is None
    finally:
        release.set()
        await asyncio.gather(*(item for item in (task, cleanup) if item is not None), return_exceptions=True)
        await close(e)


@pytest.mark.parametrize("selection", ["same", "other", "malformed"])
async def test_existing_custom_mapping_requires_exact_canonical_target(db, monkeypatch, selection):
    from deerflow.config import agents_config

    e = await environment(db, monkeypatch, lead=False)
    monkeypatch.setattr(agents_config, "load_agent_config", lambda name, *, user_id: agents_config.AgentConfig(name=name))
    saved = "previous-thread"
    selected = e.link.agent_ref if selection == "same" else "another-custom"
    metadata = {"channel_agent_name": selected, "agent_name": selected} if selection != "malformed" else {"channel_agent_name": selected}
    e.remote[saved] = {"thread_id": saved, "owner_user_id": e.link.owner_user_id, "agent_instance": None, "metadata": metadata}
    await db.connections.set_thread_id(connection_id=e.link.connection_id, owner_user_id=e.link.owner_user_id, provider="telegram", external_conversation_id=str(e.link.chat_id), thread_id=saved)
    try:
        row = await origin(e)
        if selection == "malformed":
            with pytest.raises(ValueError, match="invalid-thread-target"):
                await e.threads.ensure_thread(e.channel.link, inbound(e), origin=row)
            assert [request.method for request in e.http_calls] == ["GET"]
        else:
            identity = await e.threads.ensure_thread(e.channel.link, inbound(e), origin=row)
            assert (identity == saved) == (selection == "same")
            assert [request.method for request in e.http_calls] == (["GET"] if selection == "same" else ["GET", "POST", "GET"])
    finally:
        await close(e)


@pytest.mark.parametrize("health", ["conflict", "policy_off", "token_rejected", "secret_unreadable", "unreachable", "stopped", "agent_unavailable"])
async def test_non_ok_health_never_performs_private_thread_io(db, monkeypatch, health):
    e = await environment(db, monkeypatch)
    try:
        row = await origin(e)
        await db.links.set_health(e.link.id, health, holds=True, expected_token_revision=e.link.token_revision)
        with pytest.raises(ValueError, match="thread-attempt-changed"):
            await e.threads.ensure_thread(e.channel.link, inbound(e), origin=row)
        assert not e.http_calls
    finally:
        await close(e)


async def test_ensure_and_new_share_one_lock_and_receipt_cannot_rewind_new_mapping(db, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    held = [False]

    async def respond(e, request):
        if request.method == "POST" and not held[0]:
            held[0] = True
            entered.set()
            await release.wait()

    e = await environment(db, monkeypatch, responder=respond)
    tasks = []
    try:
        first, second = await origin(e), await origin(e, identity=902)
        tasks.append(asyncio.create_task(e.threads.ensure_thread(e.channel.link, inbound(e), origin=first)))
        await asyncio.wait_for(entered.wait(), 3)
        tasks.append(asyncio.create_task(e.threads.new_conversation(e.channel.link, origin=second)))
        await asyncio.sleep(0)
        assert len(e.http_calls) == 1
        release.set()
        older, newer = await asyncio.gather(*tasks)
        assert older != newer
        assert await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id), None) == newer
        assert await e.threads.ensure_thread(e.channel.link, inbound(e), origin=first) == older
        assert await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id), None) == newer
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        await close(e)


async def test_legitimate_generic_writer_beats_delayed_preparation_without_overwrite(db, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()

    async def respond(e, request):
        if request.method == "POST":
            entered.set()
            await release.wait()

    e = await environment(db, monkeypatch, responder=respond)
    task = None
    try:
        row = await origin(e)
        task = asyncio.create_task(e.threads.ensure_thread(e.channel.link, inbound(e), origin=row))
        await asyncio.wait_for(entered.wait(), 3)
        await db.connections.set_thread_id(connection_id=e.link.connection_id, owner_user_id=e.link.owner_user_id, provider="telegram", external_conversation_id=str(e.link.chat_id), thread_id="newer-legitimate-mapping")
        release.set()
        with pytest.raises(ValueError, match="thread-preparation-stale"):
            await task
        assert [request.method for request in e.http_calls] == ["POST"]
        assert await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id), None) == "newer-legitimate-mapping"
        before = len(e.http_calls)
        with pytest.raises(ValueError):
            await e.threads.ensure_thread(e.channel.link, inbound(e), origin=row)
        assert len(e.http_calls) == before
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await close(e)


@pytest.mark.parametrize("public_base_url", [None, "https://product.example/prefix"])
async def test_deleted_custom_plans_only_a_fixed_repair_and_sends_through_actual_sdk(db, monkeypatch, tmp_path, public_base_url):
    from test_telegram_bot_handlers import command_sender, sent

    from deerflow.config import agents_config

    def missing(name, *, user_id):
        raise FileNotFoundError("synthetic missing owned config")

    monkeypatch.setenv("DEER_FLOW_CONFIG_PATH", str(tmp_path / "deliberately-absent-application.yaml"))
    e = await environment(db, monkeypatch, lead=False)
    e.live.config.model_extra["telegram_bots"] = {"enabled": True, "public_base_url": public_base_url}
    monkeypatch.setattr(agents_config, "load_agent_config", missing)
    sender = None
    try:
        with pytest.raises(ValueError, match="custom-agent-deleted"):
            await e.threads.ensure_thread(e.channel.link, inbound(e), origin=await origin(e))
        assert not e.http_calls
        assert (await db.links.get(e.link.id)).health == "agent_unavailable"
        plans = [row for row in await rows(db, TelegramOutboxRow) if row.payload.get("delivery_kind") == "status_notice"]
        assert len(plans) == 1 and plans[0].parts[0]["source_ref"]["key"] == "custom_agent.deleted"
        expected_url = public_base_url + "/workspace?settings=channels&telegram=custom_agent." + e.link.agent_ref if public_base_url is not None else None
        if expected_url is None:
            assert "url_button" not in plans[0].parts[0]
        else:
            assert plans[0].parts[0]["url_button"] == {"page": "web.telegram", "url": expected_url}
        assert await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id)) is None
        e.services.eligibility = e.live.authority.delivery
        e.request.enqueue("sendMessage", result=sent(e))
        sender = await command_sender(db, e)
        await sender.tick()
        await sender.drain()
        assert e.request.calls[-1].endpoint == "sendMessage"
        assert "no longer exists" in e.request.calls[-1].parameters["text"]
        markup = e.request.calls[-1].parameters.get("reply_markup")
        if expected_url is None:
            assert markup is None
        else:
            assert markup["inline_keyboard"] == [[{"text": "Open Telegram settings", "url": expected_url}]]
    finally:
        if sender is not None:
            await sender.stop()
        await close(e)


@pytest.mark.parametrize("change", ["adapter", "shutdown", "account", "connection"])
async def test_superseded_attempt_cannot_dispatch_after_sdk_request_encoding(db, monkeypatch, change):
    from langgraph_sdk._async import http as sdk_http

    entered, release = threading.Event(), threading.Event()
    original = sdk_http.orjson.dumps

    def encode(value, *args):
        entered.set()
        release.wait(10)
        return original(value, *args)

    e = await environment(db, monkeypatch)
    task = None
    try:
        monkeypatch.setattr(sdk_http.orjson, "dumps", encode)
        task = asyncio.create_task(e.threads.ensure_thread(e.channel.link, inbound(e), origin=await origin(e)))
        assert await asyncio.to_thread(entered.wait, 3)
        if change == "shutdown":
            e.threads.begin_shutdown()
        elif change == "adapter":
            e.current[0] = SimpleNamespace(link=e.channel.link, application=SimpleNamespace(running=True))
        elif change == "account":
            async with db.sessions.begin() as writer:
                await writer.execute(update(TelegramAccountRow).where(TelegramAccountRow.telegram_user_id == e.link.telegram_user_id).values(owner_user_id="different-owner"))
        else:
            async with db.sessions.begin() as writer:
                await writer.execute(update(ChannelConnectionRow).where(ChannelConnectionRow.id == e.link.connection_id).values(status="revoked"))
        release.set()
        with pytest.raises(ValueError):
            await task
        assert not e.http_calls
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await close(e)


async def test_withdrawn_read_ceiling_after_remote_get_prevents_mapping_commit(db, monkeypatch):
    from app.telegram_bots import threads as module

    entered, release = asyncio.Event(), asyncio.Event()
    denied = [False]
    actual = module.require

    async def require(owner, *needed, **kwargs):
        if denied[0] and "threads:read" in needed:
            from app.telegram_bots.authority import CeilingRefused

            raise CeilingRefused
        return await actual(owner, *needed, **kwargs)

    async def respond(e, request):
        if request.method == "GET":
            entered.set()
            await release.wait()

    e = await environment(db, monkeypatch, responder=respond)
    task = None
    try:
        monkeypatch.setattr(module, "require", require)
        task = asyncio.create_task(e.threads.ensure_thread(e.channel.link, inbound(e), origin=await origin(e)))
        await asyncio.wait_for(entered.wait(), 3)
        denied[0] = True
        release.set()
        from app.telegram_bots.authority import CeilingRefused

        with pytest.raises(CeilingRefused):
            await task
        assert await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id)) is None
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await close(e)


@pytest.mark.parametrize("phase", ["config", "health", "notice"])
async def test_deleted_custom_attempt_is_fenced_at_each_awaited_boundary(db, monkeypatch, phase):
    from deerflow.config import agents_config

    entered, release = asyncio.Event(), asyncio.Event()
    thread_entered, thread_release = threading.Event(), threading.Event()

    def missing(name, *, user_id):
        if phase == "config":
            thread_entered.set()
            thread_release.wait(10)
        raise FileNotFoundError("synthetic absent target")

    e = await environment(db, monkeypatch, lead=False)
    monkeypatch.setattr(agents_config, "load_agent_config", missing)
    if phase == "health":
        actual = db.links.set_health

        async def health(*args, **kwargs):
            entered.set()
            await release.wait()
            return await actual(*args, **kwargs)

        monkeypatch.setattr(db.links, "set_health", health)
    elif phase == "notice":
        actual = e.services.outbox.plan_status_notice

        async def notice(*args, **kwargs):
            entered.set()
            await release.wait()
            return await actual(*args, **kwargs)

        monkeypatch.setattr(e.services.outbox, "plan_status_notice", notice)
    task = None
    try:
        task = asyncio.create_task(e.threads.ensure_thread(e.channel.link, inbound(e), origin=await origin(e)))
        if phase == "config":
            assert await asyncio.to_thread(thread_entered.wait, 3)
        else:
            await asyncio.wait_for(entered.wait(), 3)
        e.current[0] = SimpleNamespace(link=e.channel.link, application=SimpleNamespace(running=True))
        release.set()
        thread_release.set()
        with pytest.raises(ValueError):
            await task
        assert not e.http_calls
        assert (await db.links.get(e.link.id)).health == ("agent_unavailable" if phase == "notice" else "ok")
        assert not [row for row in await rows(db, TelegramOutboxRow) if row.payload.get("delivery_kind") == "status_notice"]
    finally:
        release.set()
        thread_release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await close(e)


async def test_deleted_config_health_writer_cannot_change_a_new_sql_binding(db, monkeypatch):
    from deerflow.config import agents_config

    entered, release = asyncio.Event(), asyncio.Event()

    def missing(name, *, user_id):
        raise FileNotFoundError("synthetic absent target")

    e = await environment(db, monkeypatch, lead=False)
    monkeypatch.setattr(agents_config, "load_agent_config", missing)
    actual = db.links.set_health

    async def health(*args, **kwargs):
        entered.set()
        await release.wait()
        return await actual(*args, **kwargs)

    monkeypatch.setattr(db.links, "set_health", health)
    task = None
    try:
        task = asyncio.create_task(e.threads.ensure_thread(e.channel.link, inbound(e), origin=await origin(e)))
        await asyncio.wait_for(entered.wait(), 3)
        await db.links.unbind(e.link, actor_id=e.link.owner_user_id)
        unbound = await db.links.get(e.link.id)
        code = code_of(await db.links.mint_code(unbound))
        rebound = await db.links.bind(
            unbound,
            telegram_user_id=e.link.telegram_user_id,
            chat_id=e.link.chat_id,
            first_name="Fixture",
            username=None,
            code=code,
            start_identity={"update_id": 9991, "message_id": 9991, "chat_id": e.link.chat_id},
            hello_parts=[{"source_ref": {"key": "hello.custom_agent", "values": {"agent": e.link.agent_ref, "product": "HartMesh", "email": "fixture@example.test"}}}, {"source_ref": {"key": "hello.privacy", "values": {}}}],
        )
        assert rebound.binding_generation != e.link.binding_generation
        # Keep the original adapter/snapshot registered: only the writer's SQL
        # captured-link fence can discover this external binding replacement.
        release.set()
        with pytest.raises(ValueError, match="thread-attempt-changed"):
            await task
        assert (await db.links.get(e.link.id)).health == "ok"
        assert not [row for row in await rows(db, TelegramOutboxRow) if row.payload.get("delivery_kind") == "status_notice"]
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await close(e)


@pytest.mark.parametrize("kind", ["unrouted", "sealed", "forged"])
async def test_invalid_origin_cannot_trigger_deleted_config_health_or_notice(db, monkeypatch, kind):
    from deerflow.config import agents_config

    reads = []

    def missing(name, *, user_id):
        reads.append(name)
        raise FileNotFoundError("synthetic absent target")

    e = await environment(db, monkeypatch, lead=False)
    monkeypatch.setattr(agents_config, "load_agent_config", missing)
    try:
        if kind == "unrouted":
            row, _ = await e.inbox.accept(e.link, message(e, 901, content="Synthetic future text"), sanitized_input={"text": "Synthetic future text"})
        else:
            row = await origin(e)
            if kind == "sealed":
                await e.inbox.finish_route(row)
            else:
                row = replace(row, message_id=987654)
        with pytest.raises(ValueError):
            await e.threads.ensure_thread(e.link, inbound(e), origin=row)
        assert not reads and not e.http_calls
        assert (await db.links.get(e.link.id)).health == "ok"
        assert not [row for row in await rows(db, TelegramOutboxRow) if row.payload.get("delivery_kind") == "status_notice"]
    finally:
        await close(e)


@pytest.mark.parametrize("outcome", ["deleted", "unreadable"])
async def test_custom_target_changed_during_http_prevents_mapping(db, monkeypatch, outcome):
    from deerflow.config import agents_config

    entered, release = asyncio.Event(), asyncio.Event()
    available = [True]

    def config(name, *, user_id):
        if available[0]:
            return agents_config.AgentConfig(name=name)
        if outcome == "deleted":
            raise FileNotFoundError("synthetic absent target")
        raise ValueError("synthetic unreadable target")

    async def respond(e, request):
        if request.method == "POST":
            entered.set()
            await release.wait()

    e = await environment(db, monkeypatch, lead=False, responder=respond)
    monkeypatch.setattr(agents_config, "load_agent_config", config)
    task = None
    try:
        task = asyncio.create_task(e.threads.ensure_thread(e.link, inbound(e), origin=await origin(e)))
        await asyncio.wait_for(entered.wait(), 3)
        available[0] = False
        release.set()
        with pytest.raises(ValueError):
            await task
        assert await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id)) is None
        assert [request.method for request in e.http_calls] == ["POST"]
        statuses = [row for row in await rows(db, TelegramOutboxRow) if row.payload.get("delivery_kind") == "status_notice"]
        assert bool(statuses) == (outcome == "deleted")
        assert (await db.links.get(e.link.id)).health == ("agent_unavailable" if outcome == "deleted" else "ok")
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await close(e)


async def test_fresh_explicit_retry_recovers_repair_after_health_committed_and_notice_failed(db, monkeypatch):
    from deerflow.config import agents_config

    def missing(name, *, user_id):
        raise FileNotFoundError("synthetic absent target")

    e = await environment(db, monkeypatch, lead=False)
    monkeypatch.setattr(agents_config, "load_agent_config", missing)
    actual, calls = e.services.outbox.plan_status_notice, []

    async def plan(*args, **kwargs):
        calls.append(True)
        if len(calls) == 1:
            raise RuntimeError("synthetic unavailable notice writer")
        return await actual(*args, **kwargs)

    monkeypatch.setattr(e.services.outbox, "plan_status_notice", plan)
    try:
        row = await origin(e)
        with pytest.raises(RuntimeError, match="synthetic unavailable notice writer"):
            await e.threads.ensure_thread(e.link, inbound(e), origin=row)
        assert (await db.links.get(e.link.id)).health == "agent_unavailable"
        before = next(saved for saved in await rows(db, TelegramInboundMarkRow) if saved.message_id == row.message_id).route_basis["thread_preparation"]["creation_id"]
        with pytest.raises(ValueError, match="custom-agent-deleted"):
            await e.threads.ensure_thread(e.link, inbound(e), origin=row)
        plans = [saved for saved in await rows(db, TelegramOutboxRow) if saved.payload.get("delivery_kind") == "status_notice"]
        assert len(plans) == 1
        after = next(saved for saved in await rows(db, TelegramInboundMarkRow) if saved.message_id == row.message_id).route_basis["thread_preparation"]["creation_id"]
        assert before == after and not e.http_calls
        with pytest.raises(ValueError):
            await e.threads.ensure_thread(e.link, inbound(e), origin=row)
        assert len([saved for saved in await rows(db, TelegramOutboxRow) if saved.payload.get("delivery_kind") == "status_notice"]) == 1
    finally:
        await close(e)


@pytest.mark.parametrize("change", ["ceiling", "account", "connection"])
async def test_completed_receipt_rechecks_read_permission_after_its_get_response(db, monkeypatch, change):
    from app.telegram_bots import threads as module
    from app.telegram_bots.authority import CeilingRefused

    entered, release = asyncio.Event(), asyncio.Event()
    held, denied = [False], [False]
    actual = module.require

    async def require(owner, *needed, **kwargs):
        if denied[0] and "threads:read" in needed:
            raise CeilingRefused
        return await actual(owner, *needed, **kwargs)

    async def respond(e, request):
        if held[0] and request.method == "GET":
            entered.set()
            await release.wait()

    e = await environment(db, monkeypatch, responder=respond)
    task = None
    try:
        row = await origin(e)
        identity = await e.threads.ensure_thread(e.link, inbound(e), origin=row)
        held[0] = True
        monkeypatch.setattr(module, "require", require)
        task = asyncio.create_task(e.threads.ensure_thread(e.link, inbound(e), origin=row))
        await asyncio.wait_for(entered.wait(), 3)
        if change == "ceiling":
            denied[0] = True
        elif change == "account":
            async with db.sessions.begin() as writer:
                await writer.execute(update(TelegramAccountRow).where(TelegramAccountRow.telegram_user_id == e.link.telegram_user_id).values(owner_user_id="different-owner"))
        else:
            async with db.sessions.begin() as writer:
                await writer.execute(update(ChannelConnectionRow).where(ChannelConnectionRow.id == e.link.connection_id).values(status="revoked"))
        release.set()
        with pytest.raises(CeilingRefused if change == "ceiling" else ValueError):
            await task
        assert await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id)) == identity
        assert sum(request.method == "POST" for request in e.http_calls) == 1
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await close(e)


async def test_cancelled_scope_lock_waiter_drains_without_waiting_for_other_http(db, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()

    async def respond(e, request):
        if request.method == "POST":
            entered.set()
            await release.wait()

    e = await environment(db, monkeypatch, responder=respond)
    first, waiter = None, None
    try:
        first_origin, next_origin = await origin(e), await origin(e, identity=902)
        first = asyncio.create_task(e.threads.ensure_thread(e.link, inbound(e), origin=first_origin))
        await asyncio.wait_for(entered.wait(), 3)
        waiter = asyncio.create_task(e.threads.new_conversation(e.link, origin=next_origin))
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(waiter), 3)
        assert not first.done() and len(e.http_calls) == 1
        release.set()
        await first
    finally:
        release.set()
        await asyncio.gather(*(task for task in (first, waiter) if task is not None), return_exceptions=True)
        await close(e)


async def test_cancelled_http_waiter_retires_attempt_but_drains_and_retains_replay_identity(db, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()

    async def respond(e, request):
        if request.method == "POST":
            entered.set()
            await release.wait()

    e = await environment(db, monkeypatch, responder=respond)
    task = None
    try:
        row = await origin(e)
        task = asyncio.create_task(e.threads.ensure_thread(e.link, inbound(e), origin=row))
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        assert not task.done() and not e.http_client.closed
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await db.connections.get_thread_id(e.link.connection_id, str(e.link.chat_id)) is None
        identity = await e.threads.ensure_thread(e.link, inbound(e), origin=row)
        assert set(e.remote) == {identity}
        assert len({json.loads(request.content)["thread_id"] for request in e.http_calls if request.method == "POST"}) == 1
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await close(e)


async def test_last_dispatch_reserve_rechecks_health_after_its_awaited_preparation(db, monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    e = await environment(db, monkeypatch)
    actual = db.links.reserve_thread_preparation

    async def reserve(*args, **kwargs):
        if kwargs.get("require_healthy"):
            entered.set()
            await release.wait()
        return await actual(*args, **kwargs)

    monkeypatch.setattr(db.links, "reserve_thread_preparation", reserve)
    task = None
    try:
        task = asyncio.create_task(e.threads.ensure_thread(e.link, inbound(e), origin=await origin(e)))
        # Cold SQL/authority preparation is fixture arrival, not a product
        # deadline. The health change below still happens at the exact barrier.
        await asyncio.wait_for(entered.wait(), 15)
        await db.links.set_health(e.link.id, "secret_unreadable", holds=True, expected_token_revision=e.link.token_revision)
        release.set()
        with pytest.raises(ValueError):
            await task
        assert not e.http_calls
    finally:
        release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await close(e)
