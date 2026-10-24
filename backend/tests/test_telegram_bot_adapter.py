"""Managed transport uses the actual pinned SDK over socket-free requests."""

import asyncio
import secrets
from dataclasses import replace
from types import SimpleNamespace

import pytest
from _telegram_bot_test_support import ManualClock, RecordingRequest, make_update
from _telegram_repository_test_support import add_link, bind_link
from _telegram_repository_test_support import db as db
from sqlalchemy import event, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from telegram.error import InvalidToken, NetworkError
from telegram.ext import Application

from app.channels.telegram import TelegramChannel
from app.telegram_bots.adapter import BoundedRateLimiter, SendContext, TelegramBotChannel
from app.telegram_bots.producer import DeliveryEligibility, Outbox, OutboxSender
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramOutboxRow

pytestmark = pytest.mark.asyncio


async def build(db, *, connected=True, clock=None, normal_request=None, updates_request=None, protect_notices=False, link=None, construct_off_loop=False):
    link = link if link is not None else await add_link(db)
    if connected:
        link = await bind_link(db, link)
    normal, updates, created = normal_request or RecordingRequest(), updates_request or RecordingRequest(), []
    tls = object()

    def request_factory(**kwargs):
        created.append(kwargs)
        return normal if kwargs["connection_pool_size"] == 8 else updates

    clock = clock or ManualClock()

    async def protect_notice(context):
        return protect_notices

    async def eligibility(link, item, part):
        return DeliveryEligibility.allow()

    services = SimpleNamespace(
        send_allowed=lambda: True, config=db.config, links=db.links, outbox=Outbox(db.sessions, clock=clock.now), settings=db.settings, tls_context=tls, request_factory=request_factory, protect_notice=protect_notice, eligibility=eligibility
    )
    token = str(link.bot_id) + ":" + secrets.token_urlsafe(30)

    def construct():
        return TelegramBotChannel(link=link, token=token, supervisor=services, monotonic=clock.monotonic, sleep=clock.sleep, now=clock.now)

    channel = await asyncio.to_thread(construct) if construct_off_loop else construct()
    services.channel = lambda link_id: channel if link_id == link.id else None
    return SimpleNamespace(channel=channel, link=link, request=normal, updates=updates, services=services, clock=clock, created=created, tls=tls)


def fail(request, endpoint="getMe", *, code=401, description="Unauthorized", release=None):
    return request.enqueue(endpoint, status=code, body={"ok": False, "error_code": code, "description": description}, release=release)


async def start(fixture):
    fixture.request.enqueue_get_me(bot_id=fixture.link.bot_id, can_manage_bots=True, has_topics_enabled=True, allows_users_to_create_topics=False)
    await fixture.channel.start()


async def test_constructor_uses_actual_application_and_explicit_sdk_defaults(db):
    fixture = await build(db)
    channel = fixture.channel
    assert isinstance(channel, TelegramChannel) and isinstance(channel.application, Application)
    assert channel.application.updater is None and channel.application.job_queue is None
    assert isinstance(channel.bot.rate_limiter, BoundedRateLimiter)
    assert channel.bot.defaults.link_preview_options.is_disabled
    assert channel.bot.defaults.allow_sending_without_reply is True
    assert channel.bot.defaults.parse_mode is None
    assert channel.transport_revision == fixture.link.token_revision
    with pytest.raises(AttributeError):
        channel.transport_revision = fixture.link.token_revision + 1
    assert fixture.created == [{"connection_pool_size": 8, "httpx_kwargs": {"verify": fixture.tls}}, {"connection_pool_size": 1, "httpx_kwargs": {"verify": fixture.tls}}]
    assert fixture.request.calls == fixture.updates.calls == []
    await channel.stop()


@pytest.mark.parametrize("field,value", [("binding_generation", True), ("token_revision", True), ("chat_id", True), ("purpose", "caller-metadata"), ("link_id", "")])
async def test_server_send_context_rejects_ambiguous_scope_types(field, value):
    fields = {"link_id": "a" * 32, "chat_id": 7_000_000_001, "binding_generation": 1, "token_revision": 1, "purpose": "bound_message"}
    fields[field] = value
    with pytest.raises(ValueError, match="send-context"):
        SendContext(**fields)


async def test_same_token_binding_refresh_keeps_transport_revision_immutable(db):
    fixture = await build(db)
    await start(fixture)
    await db.links.unbind(fixture.link, actor_id=fixture.link.owner_user_id)
    current = await fixture.channel.refresh_link()
    assert current.binding_generation != fixture.link.binding_generation
    assert fixture.channel.transport_revision == fixture.link.token_revision
    await db.links.store_token(fixture.link.id, "wrapped-new-synthetic", actor_id=fixture.link.owner_user_id, expected_revision=fixture.link.token_revision, token_changed=True)
    assert await fixture.channel.refresh_link() is None
    assert fixture.channel.transport_revision == fixture.link.token_revision
    await fixture.channel.stop()


async def test_start_and_stop_own_no_updater_and_persist_current_bot_facts(db):
    fixture = await build(db)
    await start(fixture)
    assert fixture.channel.application.running
    saved = await db.links.get(fixture.link.id)
    assert saved.can_manage_bots and saved.topics_enabled
    assert await fixture.channel.handle_update(make_update(99)) is False
    assert [call.endpoint for call in fixture.request.calls] == ["getMe"]
    assert fixture.updates.calls == []
    await fixture.channel.stop()
    await fixture.channel.stop()
    assert not fixture.channel.application.running
    assert fixture.request.closed and fixture.updates.closed


async def test_failed_initialize_closes_both_clients_and_can_retry_full_start(db):
    fixture = await build(db)
    fail(fixture.request, code=502, description="Synthetic server failure")
    with pytest.raises(NetworkError):
        await fixture.channel.start()
    assert not fixture.channel.application.running
    assert fixture.request.closed and fixture.updates.closed
    await start(fixture)
    assert fixture.channel.application.running
    assert [call.endpoint for call in fixture.request.calls] == ["getMe", "getMe"]
    await fixture.channel.stop()


@pytest.mark.parametrize("phase", ["before-start", "after-stop"])
async def test_raw_and_disposable_transports_require_started_application_but_plans_survive(db, phase):
    fixture = await build(db)
    if phase == "after-stop":
        await start(fixture)
        await fixture.channel.stop()
    identity, sender = await delivery(db, fixture)
    calls = len(fixture.request.calls)
    context = SendContext(fixture.link.id, fixture.link.chat_id, fixture.link.binding_generation, fixture.link.token_revision, "bound_message")
    try:
        assert await fixture.channel.send_notice("hello.privacy", context=context) is None
        await fixture.channel.answer_press("synthetic-query")
        await sender.tick()
        await sender.drain()
        row = await saved(db, identity)
        assert row.state == "pending" and row.attempts == 0
        assert len(fixture.request.calls) == calls
        await start(fixture)
        fixture.clock.advance(1)
        message(fixture.request)
        await sender.tick()
        await sender.drain()
        assert (await saved(db, identity)).state == "sent"
    finally:
        await fixture.channel.stop()


@pytest.mark.parametrize("interruption", ["cancel", "failure"])
async def test_early_request_initialization_closes_exact_clients_before_sdk_flag(db, interruption):
    entered, release = asyncio.Event(), asyncio.Event()

    class InitializingRequest(RecordingRequest):
        async def initialize(self):
            await super().initialize()
            entered.set()
            if interruption == "failure":
                raise NetworkError("Synthetic initialization failure")
            await release.wait()

    fixture = await build(db, normal_request=InitializingRequest())
    pending = asyncio.create_task(fixture.channel.start())
    try:
        await entered.wait()
        if interruption == "cancel":
            pending.cancel()
        result = (await asyncio.gather(pending, return_exceptions=True))[0]
        assert isinstance(result, asyncio.CancelledError if interruption == "cancel" else NetworkError)
        assert fixture.request.initialized and fixture.updates.initialized
        assert fixture.request.closed and fixture.updates.closed
        assert fixture.request.calls == []
    finally:
        release.set()
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        await fixture.channel.stop()


async def test_initialize_wrapped_first_401_is_counted_and_second_is_spaced(db):
    fixture = await build(db)
    fail(fixture.request)
    fail(fixture.request)
    pending = asyncio.create_task(fixture.channel.start())
    try:
        await fixture.clock.wait_for_sleep()
        assert [call.endpoint for call in fixture.request.calls] == ["getMe"]
        fixture.clock.advance(29)
        await asyncio.sleep(0)
        assert not pending.done()
        fixture.clock.advance(1)
        with pytest.raises(NetworkError):
            await pending
        assert (await db.links.get(fixture.link.id)).health == "token_rejected"
        assert len(fixture.request.calls) == 2
        assert fixture.request.closed and fixture.updates.closed
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        await fixture.channel.stop()


async def test_verifier_is_coalesced_and_cancelling_one_waiter_keeps_the_check(db):
    fixture = await build(db)
    await start(fixture)
    fail(fixture.request)
    fail(fixture.request)
    first = asyncio.create_task(fixture.channel.verify_token(InvalidToken("Unauthorized")))
    second = None
    try:
        await fixture.clock.wait_for_sleep()
        second = asyncio.create_task(fixture.channel.wait_token_verification())
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        assert not second.done()
        fixture.clock.advance(30)
        result = await second
        assert result.status == "rejected"
        assert len(fixture.request.calls) == 3
    finally:
        first.cancel()
        if second:
            second.cancel()
        await asyncio.gather(first, *([second] if second else []), return_exceptions=True)
        await fixture.channel.stop()


async def test_getme_404_never_rejects_a_token(db):
    fixture = await build(db)
    await start(fixture)
    fail(fixture.request, code=404, description="Not Found")
    result = await fixture.channel.verify_token(InvalidToken("Unauthorized"))
    assert result.status == "valid" and result.user is None
    assert (await db.links.get(fixture.link.id)).health == "ok"
    assert fixture.clock.sleeps == []
    await fixture.channel.stop()


async def test_delayed_404_verification_result_is_retired_after_token_rotation(db):
    fixture = await build(db)
    await start(fixture)
    release = asyncio.Event()
    attempt = fail(fixture.request, code=404, description="Not Found", release=release)
    pending = asyncio.create_task(fixture.channel.verify_token(InvalidToken("Unauthorized")))
    try:
        await attempt.started.wait()
        await db.links.store_token(fixture.link.id, "wrapped-new-synthetic", actor_id=fixture.link.owner_user_id, expected_revision=fixture.link.token_revision, token_changed=True)
        release.set()
        assert (await pending).status == "retired"
        assert fixture.clock.sleeps == []
    finally:
        release.set()
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        await fixture.channel.stop()


async def test_rotated_token_retires_pending_verification_without_second_call(db):
    fixture = await build(db)
    await start(fixture)
    fail(fixture.request)
    pending = asyncio.create_task(fixture.channel.verify_token(InvalidToken("Unauthorized")))
    try:
        await fixture.clock.wait_for_sleep()
        await db.links.store_token(fixture.link.id, "wrapped-new-synthetic", actor_id=fixture.link.owner_user_id, expected_revision=fixture.link.token_revision, token_changed=True)
        fixture.clock.advance(30)
        assert (await pending).status == "retired"
        assert len(fixture.request.calls) == 2
        assert (await db.links.get(fixture.link.id)).health == "ok"
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        await fixture.channel.stop()


async def test_stop_cancels_and_drains_the_channel_owned_token_check(db):
    fixture = await build(db)
    await start(fixture)
    fail(fixture.request)
    pending = asyncio.create_task(fixture.channel.verify_token(InvalidToken("Unauthorized")))
    try:
        await fixture.clock.wait_for_sleep()
        await fixture.channel.stop()
        assert isinstance((await asyncio.gather(pending, return_exceptions=True))[0], asyncio.CancelledError)
        assert fixture.request.closed and fixture.updates.closed and not fixture.channel.application.running
        assert len(fixture.request.calls) == 2
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        await fixture.channel.stop()


async def test_cancelled_initialization_checker_is_owned_until_stop_closes_clients(db):
    fixture = await build(db)
    fail(fixture.request)
    starting = asyncio.create_task(fixture.channel.start())
    waiting = None
    try:
        await fixture.clock.wait_for_sleep()
        waiting = asyncio.create_task(fixture.channel.wait_token_verification())
        starting.cancel()
        assert isinstance((await asyncio.gather(starting, return_exceptions=True))[0], asyncio.CancelledError)
        assert not waiting.done()
        await fixture.channel.stop()
        assert isinstance((await asyncio.gather(waiting, return_exceptions=True))[0], asyncio.CancelledError)
        assert fixture.request.closed and fixture.updates.closed and not fixture.channel.application.running
        assert len(fixture.request.calls) == 1
    finally:
        starting.cancel()
        if waiting:
            waiting.cancel()
        await asyncio.gather(starting, *([waiting] if waiting else []), return_exceptions=True)
        await fixture.channel.stop()


async def test_getme_identity_must_match_captured_bot_before_application_starts(db):
    fixture = await build(db)
    fixture.request.enqueue_get_me(bot_id=fixture.link.bot_id + 1, can_manage_bots=True)
    with pytest.raises(NetworkError, match="Bot identity mismatch"):
        await fixture.channel.start()
    assert not fixture.channel.application.running and fixture.request.closed and fixture.updates.closed
    assert not (await db.links.get(fixture.link.id)).can_manage_bots
    await fixture.channel.stop()


@pytest.mark.parametrize("purpose", ["bound_message", "stranger", "profile", "group", "callback", "optional"])
async def test_forbidden_only_blocks_current_bound_message_context(db, purpose):
    fixture = await build(db)
    await start(fixture)
    fail(fixture.request, "sendMessage", code=403, description="Forbidden: bot was blocked by the user")
    context = SendContext(fixture.link.id, fixture.link.chat_id, fixture.link.binding_generation, fixture.link.token_revision, purpose)
    assert await fixture.channel.send_notice("hello.privacy", context=context) is None
    current = await db.links.get(fixture.link.id)
    assert (current.state == "blocked") is (purpose == "bound_message")
    await fixture.channel.stop()


@pytest.mark.parametrize("change", ["generation", "token"])
async def test_delayed_old_forbidden_cannot_block_a_successor(db, change):
    fixture = await build(db)
    await start(fixture)
    release = asyncio.Event()
    attempt = fail(fixture.request, "sendMessage", code=403, description="Forbidden: bot was blocked by the user", release=release)
    context = SendContext(fixture.link.id, fixture.link.chat_id, fixture.link.binding_generation, fixture.link.token_revision, "bound_message")
    pending = asyncio.create_task(fixture.channel.send_notice("hello.privacy", context=context))
    try:
        await attempt.started.wait()
        if change == "generation":
            await db.links.unbind(fixture.link, actor_id=fixture.link.owner_user_id)
        else:
            await db.links.store_token(fixture.link.id, "wrapped-new-synthetic", actor_id=fixture.link.owner_user_id, expected_revision=fixture.link.token_revision, token_changed=True)
        release.set()
        assert await pending is None
        assert (await db.links.get(fixture.link.id)).state != "blocked"
    finally:
        release.set()
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        await fixture.channel.stop()


async def test_notice_refuses_stale_generation_without_retargeting_destination(db):
    fixture = await build(db)
    await start(fixture)
    context = SendContext(fixture.link.id, fixture.link.chat_id + 123, fixture.link.binding_generation - 1, fixture.link.token_revision, "stranger")
    fixture.request.enqueue("sendMessage", result={"message_id": 501, "date": 1_893_456_000, "chat": {"id": context.chat_id, "type": "private"}, "text": "Notice"})
    fixture.channel.link = replace(fixture.link, chat_id=fixture.link.chat_id + 456)
    assert await fixture.channel.send_notice("hello.privacy", context=context) is None
    assert [call.endpoint for call in fixture.request.calls] == ["getMe"]
    await fixture.channel.stop()


async def test_notice_requires_explicit_destination_before_render_or_transport(db):
    fixture = await build(db)
    context = SendContext(fixture.link.id, None, fixture.link.binding_generation, fixture.link.token_revision, "optional")
    with pytest.raises(ValueError, match="destination"):
        await fixture.channel.send_notice("missing.product.key", context=context)
    assert fixture.request.calls == []
    await fixture.channel.stop()


async def test_notice_uses_server_owned_protection_fact_and_captured_destination(db):
    fixture = await build(db, protect_notices=True)
    await start(fixture)
    context = SendContext(fixture.link.id, fixture.link.chat_id + 123, fixture.link.binding_generation, fixture.link.token_revision, "stranger")
    message(fixture.request, chat=context.chat_id)
    assert await fixture.channel.send_notice("hello.privacy", context=context, protect_content=False) == 501
    assert fixture.request.calls[-1].parameters["chat_id"] == context.chat_id
    assert fixture.request.calls[-1].parameters.get("protect_content") is True
    await fixture.channel.stop()


async def delivery(db, fixture, *, edit=None, topic=None, protected=False, reply=None, row_silent=False):
    async with db.sessions.begin() as session:
        for row in await session.scalars(select(TelegramOutboxRow)):
            row.state = "sent"  # isolate this plan from the existing welcome
    if row_silent:
        await fixture.services.outbox.add(fixture.link, dedupe_key="notice:transport-test", class_="reply", silent=True, payload={"parts": [{"kind": "text", "source_ref": {"key": "hello.privacy", "values": {}}, "silent": False}]})
        async with db.sessions() as session:
            identity = await session.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.dedupe_key == "notice:transport-test"))
    elif edit is None:
        identity = await fixture.channel.send_text("hello.privacy", source_id="transport-test", thread=topic, reply_to=reply)
    else:
        identity = await fixture.channel.edit_text(edit, "hello.privacy", source_id="transport-test")

    async def permitted(link, item, part):
        return DeliveryEligibility.allow(protect_content=protected)

    async def deliver(*, link_id, **kwargs):
        assert link_id == fixture.link.id
        return await fixture.channel.send_part(**kwargs)

    return identity, OutboxSender(fixture.services.outbox, run_id="transport-process", deliver_part=deliver, eligibility=permitted, jitter=lambda: 0)


async def saved(db, identity):
    async with db.sessions() as session:
        return await session.get(TelegramOutboxRow, identity)


def message(request, endpoint="sendMessage", *, identity=501, chat=7_000_000_001):
    return request.enqueue(endpoint, result={"message_id": identity, "date": 1_893_456_000, "chat": {"id": chat, "type": "private"}, "text": "Saved product text"})


async def test_actual_claimed_sdk_send_records_receipt_and_required_protection(db):
    fixture = await build(db)
    await start(fixture)
    identity, sender = await delivery(db, fixture, protected=True, reply=77)
    before = await saved(db, identity)
    message(fixture.request)
    assert await sender.tick() == 1
    await sender.drain()
    current = await saved(db, identity)
    assert current.state == "sent" and current.parts[0]["message_ids"] == [501]
    assert current.parts[0]["part_id"] == before.parts[0]["part_id"]
    actual = fixture.request.calls[-1]
    assert actual.endpoint == "sendMessage" and actual.parameters["protect_content"] is True
    assert actual.parameters["parse_mode"] == "HTML" and actual.parameters["link_preview_options"] == {"is_disabled": True}
    assert actual.parameters["reply_parameters"] == {"message_id": 77, "allow_sending_without_reply": True}
    assert await fixture.channel.send_text("hello.privacy", source_id="transport-test", reply_to=77) == identity
    assert len(fixture.request.calls) == 2  # initialization and the single claimed send
    await fixture.channel.stop()


async def test_explicit_saved_row_silence_cannot_be_unmuted_by_a_part(db):
    fixture = await build(db)
    await start(fixture)
    identity, sender = await delivery(db, fixture, row_silent=True)
    message(fixture.request)
    try:
        await sender.tick()
        await sender.drain()
        assert (await saved(db, identity)).state == "sent"
        assert fixture.request.calls[-1].parameters["disable_notification"] is True
    finally:
        await fixture.channel.stop()


@pytest.mark.parametrize("mode", ["message", "unmodified-edit", "notice"])
async def test_auxiliary_status_failure_preserves_known_sdk_acknowledgement(db, mode, caplog):
    fixture = await build(db)
    await start(fixture)
    identity = sender = None
    if mode != "notice":
        identity, sender = await delivery(db, fixture, edit=700 if mode == "unmodified-edit" else None)
    if mode == "unmodified-edit":
        fail(fixture.request, "editMessageText", code=400, description="Bad Request: message is not modified")
    else:
        message(fixture.request)
    fixture.clock.advance(60)
    armed = True

    def mark_status(session, flush_context, instances):
        if session.get_bind() is db.engine.sync_engine and any(isinstance(row, TelegramBotLinkRow) for row in session.dirty):
            session.info["telegram_adapter_status_fault"] = True

    def fail_status(session):
        nonlocal armed
        if armed and session.info.pop("telegram_adapter_status_fault", False):
            armed = False
            raise OperationalError("status-only", {}, RuntimeError("private status marker"))

    event.listen(Session, "before_flush", mark_status)
    event.listen(Session, "before_commit", fail_status)
    try:
        if mode == "notice":
            context = SendContext(fixture.link.id, fixture.link.chat_id, fixture.link.binding_generation, fixture.link.token_revision, "bound_message")
            assert await fixture.channel.send_notice("hello.privacy", context=context) == 501
        else:
            await sender.tick()
            await sender.drain()
            row = await saved(db, identity)
            assert row.state == "sent" and row.parts[0]["message_ids"] == [700 if mode == "unmodified-edit" else 501]
        assert not armed
        assert len([call for call in fixture.request.calls if call.endpoint in {"sendMessage", "editMessageText"}]) == 1
        assert not any("private status marker" in record.message for record in caplog.records)
    finally:
        event.remove(Session, "before_flush", mark_status)
        event.remove(Session, "before_commit", fail_status)
        await fixture.channel.stop()


async def test_durable_long_server_wait_stays_pending_without_an_attempt(db):
    fixture = await build(db)
    await start(fixture)
    identity, sender = await delivery(db, fixture)
    fixture.request.enqueue("sendMessage", status=429, body={"ok": False, "error_code": 429, "description": "Synthetic rate limit", "parameters": {"retry_after": 86_400}})
    await sender.tick()
    await sender.drain()
    row = await saved(db, identity)
    assert row.state == "pending" and row.attempts == 0
    assert row.not_before.replace(tzinfo=fixture.clock.now().tzinfo) > fixture.clock.now()
    assert fixture.clock.sleeps == [] and len(fixture.request.calls) == 2
    await fixture.channel.stop()


async def test_verifier_server_wait_releases_delivery_and_gates_only_verification(db, caplog):
    fixture = await build(db)
    await start(fixture)
    identity, sender = await delivery(db, fixture)
    original = await saved(db, identity)
    fail(fixture.request, "sendMessage")
    fixture.request.enqueue("getMe", status=429, body={"ok": False, "error_code": 429, "description": "Synthetic rate limit", "parameters": {"retry_after": 120}})
    gates = []
    try:
        await sender.tick()
        await asyncio.wait_for(sender.drain(), timeout=3)
        row = await saved(db, identity)
        assert row.state == "pending" and row.attempts == 0
        assert (row.not_before.replace(tzinfo=fixture.clock.now().tzinfo) - fixture.clock.now()).total_seconds() >= 120
        assert fixture.clock.sleeps == [] and fixture.channel.bot.rate_limiter._short_hold_until == 0
        fixture.request.enqueue("getWebhookInfo", result={"url": "", "has_custom_certificate": False, "pending_update_count": 0})
        assert (await fixture.channel.bot.get_webhook_info()).pending_update_count == 0
        gates = [asyncio.create_task(fixture.channel.wait_token_verification()) for _ in range(2)]
        await fixture.clock.wait_for_sleep()
        fixture.clock.advance(119)
        await asyncio.sleep(0)
        result = await fixture.channel.verify_token(InvalidToken("Unauthorized"))
        assert result.status == "unverified" and result.retry_after_seconds == 1
        assert len([call for call in fixture.request.calls if call.endpoint == "getMe"]) == 2
        fixture.request.enqueue_get_me(bot_id=fixture.link.bot_id)
        fixture.clock.advance(1)
        assert all(result.status == "valid" for result in await asyncio.gather(*gates))
        assert len([call for call in fixture.request.calls if call.endpoint == "getMe"]) == 3
        await sender.tick()
        await sender.drain()
        failed = await saved(db, identity)
        assert failed.state == "failed" and failed.failed_reason == "invalid-payload"
        assert "invalid_token_verification_pending" not in failed.parts[0]
        assert failed.parts[0]["part_id"] == original.parts[0]["part_id"] and failed.parts[0]["source_ref"] == original.parts[0]["source_ref"]
        assert len([call for call in fixture.request.calls if call.endpoint == "sendMessage"]) == 1
        calls = [record.message for record in caplog.records if record.name == "app.telegram_bots.supervisor"]
        assert len([record for record in calls if f"outbox_id={identity}" in record]) == 1
        assert any("retry_after" in record and "method=getMe" in record and "outbox_id=" not in record for record in calls)
        await fixture.services.outbox.retry(fixture.link, identity)
        fixture.clock.advance(1)
        message(fixture.request)
        await sender.tick()
        await sender.drain()
        assert (await saved(db, identity)).state == "sent"
        assert len([call for call in fixture.request.calls if call.endpoint == "sendMessage"]) == 2
    finally:
        for gate in gates:
            gate.cancel()
        await asyncio.gather(*gates, return_exceptions=True)
        await fixture.channel.stop()


@pytest.mark.parametrize("verification", ["valid", "404"])
async def test_reconstructed_adapter_classifies_original_invalid_send_before_raw_retry(db, verification):
    fixture = await build(db)
    await start(fixture)
    identity, sender = await delivery(db, fixture, protected=True)
    original = await saved(db, identity)
    token = fixture.channel.bot.token
    fail(fixture.request, "sendMessage")
    fixture.request.enqueue("getMe", status=429, body={"ok": False, "error_code": 429, "description": "Synthetic rate limit", "parameters": {"retry_after": 120}})
    await sender.tick()
    await sender.drain()
    await fixture.channel.stop()
    fixture.clock.advance(120)
    request, updates = RecordingRequest(), RecordingRequest()
    request.enqueue_get_me(bot_id=fixture.link.bot_id)
    if verification == "valid":
        request.enqueue_get_me(bot_id=fixture.link.bot_id)
    else:
        fail(request, code=404, description="Not Found")
    services = SimpleNamespace(**{**vars(fixture.services), "request_factory": lambda **kwargs: request if kwargs["connection_pool_size"] == 8 else updates})
    restored = TelegramBotChannel(link=await db.links.get(fixture.link.id), token=token, supervisor=services, monotonic=fixture.clock.monotonic, sleep=fixture.clock.sleep, now=fixture.clock.now)
    services.channel = lambda link_id: restored if link_id == fixture.link.id else None

    async def allowed(link, row, part):
        return DeliveryEligibility.allow(protect_content=True)

    async def raw(*, link_id, **scope):
        return await restored.send_part(**scope)

    resumed = OutboxSender(services.outbox, run_id="restored-process", deliver_part=raw, eligibility=allowed)
    try:
        await restored.start()
        await resumed.tick()
        await resumed.drain()
        current = await saved(db, identity)
        assert current.state == "failed" and current.failed_reason == "invalid-payload"
        assert current.parts[0]["part_id"] == original.parts[0]["part_id"] and current.parts[0]["source_ref"] == original.parts[0]["source_ref"]
        assert current.parts[0]["protect_content"] is True
        assert "invalid_token_verification_pending" not in current.parts[0]
        assert len([call for call in fixture.request.calls + request.calls if call.endpoint == "sendMessage"]) == 1
    finally:
        await restored.stop()


async def test_network_unverified_original_send_keeps_classification_before_retry(db):
    fixture = await build(db)
    await start(fixture)
    identity, sender = await delivery(db, fixture)
    fail(fixture.request, "sendMessage")
    fail(fixture.request, code=502, description="Synthetic server failure")
    try:
        await sender.tick()
        await sender.drain()
        pending = await saved(db, identity)
        assert pending.state == "pending" and pending.attempts == 1
        assert pending.parts[0]["invalid_token_verification_pending"]["token_revision"] == fixture.link.token_revision
        fixture.clock.advance(5)
        fixture.request.enqueue_get_me(bot_id=fixture.link.bot_id)
        await sender.tick()
        await sender.drain()
        assert (await saved(db, identity)).state == "failed"
        assert len([call for call in fixture.request.calls if call.endpoint == "sendMessage"]) == 1
    finally:
        await fixture.channel.stop()


async def test_verifier_429_keeps_first_401_evidence_without_another_short_path(db):
    fixture = await build(db)
    await start(fixture)
    fail(fixture.request)
    fixture.request.enqueue("getMe", status=429, body={"ok": False, "error_code": 429, "description": "Synthetic rate limit", "parameters": {"retry_after": 120}})
    checking = asyncio.create_task(fixture.channel.verify_token(InvalidToken("Unauthorized")))
    gate = None
    try:
        await fixture.clock.wait_for_sleep()
        fixture.clock.advance(30)
        result = await checking
        assert result.status == "unverified" and result.retry_after_seconds == 120
        gate = asyncio.create_task(fixture.channel.wait_token_verification())
        await fixture.clock.wait_for_sleep(2)
        fail(fixture.request)
        fixture.clock.advance(120)
        assert (await gate).status == "rejected"
        assert fixture.clock.sleeps == [30, 120]
        assert (await db.links.get(fixture.link.id)).health == "token_rejected"
    finally:
        checking.cancel()
        if gate:
            gate.cancel()
        await asyncio.gather(checking, *([gate] if gate else []), return_exceptions=True)
        await fixture.channel.stop()


async def test_stopping_verifier_delay_drains_gate_and_restart_keeps_server_deadline(db):
    fixture = await build(db)
    await start(fixture)
    fixture.request.enqueue("getMe", status=429, body={"ok": False, "error_code": 429, "description": "Synthetic rate limit", "parameters": {"retry_after": 120}})
    result = await fixture.channel.verify_token(InvalidToken("Unauthorized"))
    gate = restarting = None
    try:
        assert result.retry_after_seconds == 120
        gate = asyncio.create_task(fixture.channel.wait_token_verification())
        await fixture.clock.wait_for_sleep()
        await fixture.channel.stop()
        assert isinstance((await asyncio.gather(gate, return_exceptions=True))[0], asyncio.CancelledError)
        fixture.request.enqueue_get_me(bot_id=fixture.link.bot_id)
        restarting = asyncio.create_task(fixture.channel.start())
        await fixture.clock.wait_for_sleep(2)
        assert len([call for call in fixture.request.calls if call.endpoint == "getMe"]) == 2
        fixture.clock.advance(120)
        await restarting
        assert len([call for call in fixture.request.calls if call.endpoint == "getMe"]) == 3
    finally:
        if gate:
            gate.cancel()
        if restarting:
            restarting.cancel()
        await asyncio.gather(*([gate] if gate else []), *([restarting] if restarting else []), return_exceptions=True)
        await fixture.channel.stop()


async def test_verifier_retains_observed_server_deadline_when_following_sql_guard_fails(db, monkeypatch):
    fixture = await build(db)
    await start(fixture)
    attempt = fixture.request.enqueue("getMe", status=429, body={"ok": False, "error_code": 429, "description": "Synthetic rate limit", "parameters": {"retry_after": 120}})
    original = db.links.update_if_current
    armed, gate = True, None

    async def broken(*args, **kwargs):
        nonlocal armed
        if armed and attempt.started.is_set():
            armed = False
            raise OperationalError("guard-only", {}, RuntimeError("private status marker"))
        return await original(*args, **kwargs)

    monkeypatch.setattr(db.links, "update_if_current", broken)
    try:
        with pytest.raises(OperationalError):
            await fixture.channel.verify_token(InvalidToken("Unauthorized"))
        assert not armed
        gate = asyncio.create_task(fixture.channel.wait_token_verification())
        await fixture.clock.wait_for_sleep()
        fixture.clock.advance(119)
        await asyncio.sleep(0)
        assert not gate.done()
        assert len([call for call in fixture.request.calls if call.endpoint == "getMe"]) == 2
        fixture.request.enqueue_get_me(bot_id=fixture.link.bot_id)
        fixture.clock.advance(1)
        assert (await gate).status == "valid"
    finally:
        if gate:
            gate.cancel()
            await asyncio.gather(gate, return_exceptions=True)
        await fixture.channel.stop()


async def test_parse_failure_persists_same_plain_part_before_a_later_sdk_attempt(db, caplog):
    fixture = await build(db)
    await start(fixture)
    identity, sender = await delivery(db, fixture, protected=True)
    original = await saved(db, identity)
    fail(fixture.request, "sendMessage", code=400, description="Bad Request: can't parse entities")
    await sender.tick()
    await sender.drain()
    fallback = await saved(db, identity)
    assert fallback.state == "pending" and fallback.parts[0]["plain_fallback"] is True
    assert fallback.parts[0]["part_id"] == original.parts[0]["part_id"] and fallback.parts[0]["source_ref"] == original.parts[0]["source_ref"]
    assert len(fixture.request.calls) == 2
    failures = [record.message for record in caplog.records if record.name == "app.telegram_bots.supervisor" and f"outbox_id={identity}" in record.message]
    assert len(failures) == 1 and "desc=parse-entities" in failures[0] and "failed_reason" not in failures[0]
    fixture.clock.advance(1)
    message(fixture.request)
    await sender.tick()
    await sender.drain()
    assert (await saved(db, identity)).state == "sent"
    assert "parse_mode" not in fixture.request.calls[-1].parameters
    assert fixture.request.calls[-1].parameters["protect_content"] is True
    await fixture.channel.stop()


async def test_repeated_plain_parse_failure_is_permanent_and_never_recurses(db):
    fixture = await build(db)
    await start(fixture)
    identity, sender = await delivery(db, fixture)
    for _ in range(2):
        fail(fixture.request, "sendMessage", code=400, description="Bad Request: can't parse entities")
        await sender.tick()
        await sender.drain()
        fixture.clock.advance(1)
    assert (await saved(db, identity)).state == "failed"
    assert len([call for call in fixture.request.calls if call.endpoint == "sendMessage"]) == 2
    await fixture.channel.stop()


async def test_unmodified_edit_acknowledges_its_saved_target_without_requeue(db):
    fixture = await build(db)
    await start(fixture)
    identity, sender = await delivery(db, fixture, edit=700)
    fail(fixture.request, "editMessageText", code=400, description="Bad Request: message is not modified")
    await sender.tick()
    await sender.drain()
    row = await saved(db, identity)
    assert row.state == "sent" and row.parts[0]["message_ids"] == [700]
    assert fixture.request.calls[-1].parameters["message_id"] == 700
    await fixture.channel.stop()


async def test_protected_keyboard_cleanup_edits_existing_markup_without_replacement_text(db):
    fixture = await build(db)
    await start(fixture)
    async with db.sessions.begin() as session:
        for row in await session.scalars(select(TelegramOutboxRow)):
            row.state = "sent"
    await fixture.services.outbox.add(
        fixture.link,
        dedupe_key="notice:keyboard-cleanup",
        class_="reply",
        payload={"kind": "keyboard_cleanup", "parts": [{"kind": "edit", "source_ref": {"key": "delivery.failed", "values": {}}, "target_message_id": 700, "remove_keyboard": True}]},
    )
    async with db.sessions() as session:
        identity = await session.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.dedupe_key == "notice:keyboard-cleanup"))

    async def allowed(link, row, part):
        return DeliveryEligibility.allow(protect_content=True)

    async def send(*, link_id, **scope):
        return await fixture.channel.send_part(**scope)

    sender = OutboxSender(fixture.services.outbox, run_id="cleanup-process", deliver_part=send, eligibility=allowed)
    message(fixture.request, "editMessageReplyMarkup", identity=700)
    try:
        await sender.tick()
        await sender.drain()
        row = await saved(db, identity)
        assert row.state == "sent" and row.payload["presentation_version"] == 0
        assert row.parts[0]["message_ids"] == [700]
        assert [call.endpoint for call in fixture.request.calls] == ["getMe", "editMessageReplyMarkup"]
        assert fixture.request.calls[-1].parameters["message_id"] == 700
        assert fixture.request.calls[-1].parameters.get("reply_markup") is None
    finally:
        await fixture.channel.stop()


async def test_unavailable_file_then_missing_topic_keeps_one_visible_warning_in_general(db):
    fixture = await build(db)
    await start(fixture)
    async with db.sessions.begin() as session:
        for row in await session.scalars(select(TelegramOutboxRow)):
            row.state = "sent"
    await fixture.services.outbox.add(
        fixture.link,
        dedupe_key="notice:file-topic",
        class_="reply",
        message_thread_id=77,
        payload={"parts": [{"kind": "document", "source_ref": {"space_id": "authorized", "path": "report.pdf"}, "silent": True, "reply_to_message_id": 77}, {"kind": "text", "source_ref": {"key": "hello.privacy", "values": {}}}]},
    )
    async with db.sessions() as session:
        identity = await session.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.dedupe_key == "notice:file-topic"))
    original = await saved(db, identity)
    protection = True

    async def send(*, link_id, **scope):
        return await fixture.channel.send_part(**scope)

    async def protected(link, row, part):
        return DeliveryEligibility.allow(protect_content=protection)

    sender = OutboxSender(fixture.services.outbox, run_id="file-topic-process", deliver_part=send, eligibility=protected)
    try:
        await sender.tick()
        await sender.drain()
        unavailable = await saved(db, identity)
        assert unavailable.parts[0]["state"] == "unavailable"
        protection = False  # the saved requirement must survive relaxed policy
        fixture.clock.advance(1)
        fail(fixture.request, "sendMessage", code=400, description="Bad Request: message thread not found")
        await sender.tick()
        await sender.drain()
        moved = await saved(db, identity)
        version = moved.payload["presentation_version"]
        assert version == 1 and moved.message_thread_id is None
        current_parts = [part for part in moved.parts if part["presentation_version"] == version]
        assert [part["kind"] for part in current_parts] == ["text", "text"]
        assert current_parts[0]["silent"] is True and current_parts[0]["protect_content"] is True and current_parts[0]["reply_to_message_id"] == 77
        historical = next(part for part in moved.parts if part["part_id"] == original.parts[0]["part_id"])
        assert historical["state"] == "unavailable" and historical["source_ref"] == original.parts[0]["source_ref"]
        assert historical["unavailable_reason"] == unavailable.parts[0]["unavailable_reason"]
        for number in (501, 502):
            fixture.clock.advance(1)
            message(fixture.request, identity=number)
            await sender.tick()
            await sender.drain()
        assert (await saved(db, identity)).state == "sent"
        general = [call for call in fixture.request.calls if call.endpoint == "sendMessage" and "message_thread_id" not in call.parameters]
        assert len(general) == 2
        assert sum("report.pdf" in call.parameters["text"] for call in general) == 1
        warning = next(call for call in general if "report.pdf" in call.parameters["text"])
        assert warning.parameters["disable_notification"] is True and warning.parameters["protect_content"] is True
        assert warning.parameters["reply_parameters"] == {"message_id": 77, "allow_sending_without_reply": True}
        assert not any(call.endpoint in {"sendDocument", "sendPhoto", "sendMediaGroup"} for call in fixture.request.calls)
    finally:
        await fixture.channel.stop()


async def test_malformed_verification_continuation_is_failed_before_another_sdk_call(db):
    fixture = await build(db)
    await start(fixture)
    identity, sender = await delivery(db, fixture)
    async with db.sessions.begin() as session:
        row = await session.get(TelegramOutboxRow, identity)
        row.parts = [{**row.parts[0], "invalid_token_verification_pending": {"token_revision": fixture.link.token_revision, "failure_logged": True}}]
    fail(fixture.request, code=502, description="Synthetic server failure")
    try:
        await sender.tick()
        await sender.drain()
        assert (await saved(db, identity)).state == "failed"
        assert [call.endpoint for call in fixture.request.calls] == ["getMe"]
    finally:
        await fixture.channel.stop()


@pytest.mark.parametrize("missing", ["edit", "topic"])
async def test_missing_anchor_or_topic_relocates_frozen_product_before_resending(db, missing):
    fixture = await build(db)
    await start(fixture)
    identity, sender = await delivery(db, fixture, edit=700 if missing == "edit" else None, topic=77 if missing == "topic" else None)
    original = await saved(db, identity)
    fail(fixture.request, "editMessageText" if missing == "edit" else "sendMessage", code=400, description="Bad Request: message to edit not found" if missing == "edit" else "Bad Request: message thread not found")
    await sender.tick()
    await sender.drain()
    replaced = await saved(db, identity)
    assert replaced.state == "pending" and replaced.payload["presentation_version"] == 1
    assert replaced.parts[-1]["source_ref"] == original.parts[0]["source_ref"]
    fixture.clock.advance(1)
    message(fixture.request)
    await sender.tick()
    await sender.drain()
    assert (await saved(db, identity)).state == "sent"
    assert fixture.request.calls[-1].endpoint == "sendMessage" and "message_thread_id" not in fixture.request.calls[-1].parameters
    await fixture.channel.stop()


async def test_recapture_after_awaited_rendering_rejects_a_rotated_claim(db, monkeypatch):
    fixture = await build(db)
    await start(fixture)
    identity, sender = await delivery(db, fixture)
    entered, release = asyncio.Event(), asyncio.Event()
    original_get = db.settings.get

    async def paused(owner):
        entered.set()
        await release.wait()
        return await original_get(owner)

    monkeypatch.setattr(db.settings, "get", paused)
    try:
        await sender.tick()
        await entered.wait()
        await db.links.store_token(fixture.link.id, "wrapped-new-synthetic", actor_id=fixture.link.owner_user_id, expected_revision=fixture.link.token_revision, token_changed=True)
    finally:
        release.set()
        await sender.drain()
        await fixture.channel.stop()
    row = await saved(db, identity)
    assert row.state == "pending" and row.attempts == 0
    assert [call.endpoint for call in fixture.request.calls] == ["getMe"]


async def test_invalid_fixed_product_source_is_visible_failure_not_endless_retry(db):
    fixture = await build(db)
    await start(fixture)
    identity, sender = await delivery(db, fixture)
    async with db.sessions.begin() as session:
        row = await session.get(TelegramOutboxRow, identity)
        row.parts = [{**row.parts[0], "source_ref": {"key": "missing.product.key", "values": {}}}]
    await sender.tick()
    await sender.drain()
    assert (await saved(db, identity)).state == "failed"
    assert [call.endpoint for call in fixture.request.calls] == ["getMe"]
    await fixture.channel.stop()


async def test_callback_401_joins_shared_verifier_without_exceeding_ack_deadline(db, caplog):
    fixture = await build(db)
    await start(fixture)
    fail(fixture.request, "answerCallbackQuery")
    fail(fixture.request)
    fail(fixture.request)
    pending = asyncio.create_task(fixture.channel.answer_press("synthetic-query"))
    try:
        await fixture.clock.wait_for_sleep()
        await asyncio.wait_for(pending, timeout=3)
        assert len([record for record in caplog.records if record.name == "app.telegram_bots.supervisor" and "method=answerCallbackQuery" in record.message]) == 1
        assert (await db.links.get(fixture.link.id)).health == "ok"
        waiting = asyncio.create_task(fixture.channel.wait_token_verification())
        fixture.clock.advance(30)
        assert (await waiting).status == "rejected"
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        await fixture.channel.stop()


async def test_shared_success_uses_fresh_stored_basis_and_recovery_hint(db):
    fixture = await build(db)
    await start(fixture)
    fixture.clock.advance(10)
    assert await fixture.channel.record_poll_success(recovered=True)
    assert (await db.links.get(fixture.link.id)).last_ok_at == fixture.clock.now()
    fixture.clock.advance(40)
    external = fixture.clock.now()
    await db.links.update_if_current(fixture.link.id, token_revision=fixture.link.token_revision, last_ok_at=external)
    fixture.clock.advance(20)
    assert await fixture.channel.record_poll_success()
    assert (await db.links.get(fixture.link.id)).last_ok_at == external
    fixture.clock.advance(40)
    assert await fixture.channel.record_poll_success()
    assert (await db.links.get(fixture.link.id)).last_ok_at == fixture.clock.now()
    await fixture.channel.stop()


@pytest.mark.parametrize("kind", ["bad-request", "401-valid", "404-404"])
async def test_sdk_permanent_failure_has_one_closed_durable_log_record(db, caplog, kind):
    fixture = await build(db)
    await start(fixture)
    identity, sender = await delivery(db, fixture)
    if kind == "bad-request":
        fail(fixture.request, "sendMessage", code=400, description="Bad Request: synthetic private payload marker")
    else:
        fail(fixture.request, "sendMessage", code=401 if kind == "401-valid" else 404, description="Unauthorized" if kind == "401-valid" else "Not Found")
        if kind == "401-valid":
            fixture.request.enqueue_get_me(bot_id=fixture.link.bot_id)
        else:
            fail(fixture.request, code=404, description="Not Found")
    try:
        await sender.tick()
        await sender.drain()
        row = await saved(db, identity)
        assert row.state == "failed" and row.failed_reason == "invalid-payload"
        failures = [record for record in caplog.records if record.name == "app.telegram_bots.supervisor" and f"outbox_id={identity}" in record.message]
        assert len(failures) == 1
        logged = failures[0].message
        assert logged.startswith(f"telegram_bots send_failed link={fixture.link.id} bot={fixture.link.bot_id} method=sendMessage ")
        assert "failed_reason=invalid-payload" in logged and "attempts=1" in logged
        assert "claim_owner=transport-process" in logged and f"part_id={row.parts[0]['part_id']}" in logged
        assert "synthetic private payload marker" not in logged and "Unauthorized" not in logged and "Not Found" not in logged
    finally:
        await fixture.channel.stop()


async def test_direct_429s_use_retry_after_records_without_failure_fields(db, caplog):
    fixture = await build(db)
    await start(fixture)
    context = SendContext(fixture.link.id, fixture.link.chat_id, fixture.link.binding_generation, fixture.link.token_revision, "bound_message")
    for endpoint in ("sendMessage", "answerCallbackQuery"):
        fixture.request.enqueue(endpoint, status=429, body={"ok": False, "error_code": 429, "description": "Synthetic rate limit", "parameters": {"retry_after": 120}})
    try:
        assert await fixture.channel.send_notice("hello.privacy", context=context) is None
        await fixture.channel.answer_press("synthetic-query")
        records = [record.message for record in caplog.records if record.name == "app.telegram_bots.supervisor"]
        assert len(records) == 2
        assert all(message.startswith("telegram_bots retry_after ") for message in records)
        assert all("retry_in=120.0 attempts=1" in message and not any(key in message for key in (" error=", " cause=", " http=", " desc=", " failed_reason=")) for message in records)
    finally:
        await fixture.channel.stop()


async def test_shared_success_serializes_concurrent_timestamp_decisions(db, monkeypatch):
    fixture = await build(db)
    await start(fixture)
    fixture.clock.advance(60)
    entered, release, second_started = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = db.links.update_if_current
    writes = []

    async def paused(*args, **kwargs):
        if "last_ok_at" in kwargs:
            writes.append(kwargs["last_ok_at"])
            if len(writes) == 1:
                entered.set()
                await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(db.links, "update_if_current", paused)
    first = asyncio.create_task(fixture.channel.record_poll_success())

    async def other():
        second_started.set()
        return await fixture.channel.record_poll_success()

    second = None
    try:
        await entered.wait()
        second = asyncio.create_task(other())
        await second_started.wait()
        release.set()
        assert all(await asyncio.gather(first, second))
        assert writes == [fixture.clock.now()]
    finally:
        release.set()
        first.cancel()
        if second:
            second.cancel()
        await asyncio.gather(first, *([second] if second else []), return_exceptions=True)
        await fixture.channel.stop()


@pytest.mark.parametrize("failure_kind", ["network", "timeout"])
async def test_first_success_after_raw_transport_failure_updates_shared_timestamp(db, failure_kind):
    from telegram.error import TimedOut

    class TimeoutRequest(RecordingRequest):
        once = True

        async def do_request(self, url, method, request_data=None, **kwargs):
            if self.once and url.endswith("/sendMessage"):
                self.once = False
                raise TimedOut()
            return await super().do_request(url, method, request_data, **kwargs)

    fixture = await build(db, normal_request=TimeoutRequest() if failure_kind == "timeout" else None)
    await start(fixture)
    identity, sender = await delivery(db, fixture)
    if failure_kind == "network":
        fail(fixture.request, "sendMessage", code=502, description="Synthetic server failure")
    await sender.tick()
    await sender.drain()
    fixture.clock.advance(5)
    message(fixture.request)
    await sender.tick()
    await sender.drain()
    assert (await saved(db, identity)).state == "sent"
    assert (await db.links.get(fixture.link.id)).last_ok_at == fixture.clock.now()
    await fixture.channel.stop()
