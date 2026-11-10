"""Instance failures use exact host run reasons and durable publication custody."""

from dataclasses import replace
from html import unescape
from types import MappingProxyType

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import update
from test_telegram_instance_publication import publication as publication
from test_telegram_manager_sentences import scoped_run
from test_telegram_publication_observation import run_row
from test_telegram_publication_primary import _primary

from app.channels.execution_client import ValidatedRun
from app.gateway.channel_execution import receipt_from_run_kwargs
from app.telegram_bots.callbacks import CallbackRepository
from app.telegram_bots.errors import classify_run
from app.telegram_bots.producer import _item, validate_publication_outcome
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow


@pytest.mark.parametrize(
    "reason,key,retry",
    [
        ("busy", "busy.queued", True),
        ("starting-elsewhere", "busy.starting_elsewhere", True),
        ("run-limit", "busy.two_conversations", True),
        ("restarting", "refused.restarting", False),
        ("storage-busy", "refused.files_busy", False),
        ("start-failed", "start.ws_failed", False),
        ("files-full", "refused.files_full", False),
    ],
)
@pytest.mark.parametrize("status", ["error", "success", "interrupted"])
def test_only_exact_instance_error_reason_selects_capacity_or_manual_retry(reason, key, retry, status):
    scope, run = scoped_run(reason=reason, status=status)
    scope = replace(scope, instance_id="c" * 32)
    failure = classify_run(run, scope)
    if status == "success":
        assert failure is None
    else:
        assert (failure.text_key, failure.retry_later) == ((key, retry) if status == "error" else ("error.generic", False))


@pytest.mark.asyncio
@pytest.mark.parametrize("reason,key", [("restarting", "refused.restarting"), ("storage-busy", "refused.files_busy"), ("start-failed", "start.ws_failed"), ("files-full", "refused.files_full"), ("run-limit", "busy.gave_up")])
async def test_instance_run_outcome_roundtrips_actual_sql_and_keeps_original_binding(publication, reason, key):
    e = publication
    record = await e.coordinator.dispatch(e.link, [e.origin], thread_id=e.thread_id)
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http_client.endpoint_id)
    record = await e.inbox.bind_publication_run(record, run_id="instance-error-run", guard=attempt.read_guard)
    row = run_row(record, run_id=record.run_id)
    row.update(status="error", stop_reason=reason, metadata={"channel_turn_id": record.publication_id})
    run = ValidatedRun(MappingProxyType(row), receipt_from_run_kwargs(row["kwargs"]))
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http_client.endpoint_id)
    box = e.channel._services.outbox
    callbacks = CallbackRepository(e.db.sessions)
    button = {"page": "web.files", "url": "https://app.example/files"} if key == "refused.files_full" else None
    identity = await box.plan_publication_outcome(record, run=run, source_ref={"key": key, "values": {"name": "Fixture employee"}}, keyboard=[], callbacks=callbacks, guard=attempt.read_guard, url_button=button)
    proof = validate_publication_outcome(_item(await _primary(e.db, identity)))
    assert proof.record.scope.agent_ref == e.link.agent_ref and proof.source_ref["key"] == key
    assert proof.run_id == "instance-error-run"


@pytest.mark.asyncio
@pytest.mark.parametrize("permissions,manager", [(1, False), (3, False), (7, True)])
async def test_full_files_presentation_and_link_follow_current_manager_grant(publication, permissions, manager):
    from types import SimpleNamespace

    from app.telegram_bots.publication import failure_button, fixed_source
    from app.telegram_bots.render import render_plain

    e = publication
    e.live.config.model_extra["telegram_bots"] = {"enabled": True, "public_base_url": "https://app.example"}
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceGrantRow).values(permissions=permissions))
    host = SimpleNamespace(authority=e.live.authority, product_name="HartMesh")
    source = await fixed_source(host, e.link, "refused.files_full")
    text = render_plain(source["key"], "en", **source["values"])
    assert "Delete files on the web" in text if manager else "Ask " in text and " to delete files" in text
    assert bool(await failure_button(host, e.link, "refused.files_full")) is manager


@pytest.mark.asyncio
async def test_instance_creation_refusal_cannot_omit_its_binding(publication):
    from app.channels.execution import ChannelExecutionBusyError
    from app.channels.outcomes import capture_creation_refusal

    e = publication
    record = await e.coordinator.dispatch(e.link, [e.origin], thread_id=e.thread_id)
    incoming = e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    refusal = capture_creation_refusal(incoming.execution_context, ChannelExecutionBusyError())
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http_client.endpoint_id)
    identity = await e.channel._services.outbox.plan_publication_outcome(record, creation_refusal=refusal, source_ref={"key": "busy.gave_up", "values": {}}, keyboard=[], callbacks=CallbackRepository(e.db.sessions), guard=attempt.read_guard)
    item = _item(await _primary(e.db, identity))
    assert validate_publication_outcome(item).kind == "creation_refusal"
    item.payload["turn_outcome_basis"]["proof"]["scope"].pop("instance_id")
    with pytest.raises(ValueError):
        validate_publication_outcome(item)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "recovery,url_rejected,withdraw,remaining",
    [
        (False, False, None, 7),
        (True, False, None, 7),
        (False, True, None, 7),
        (True, True, None, 7),
        (False, False, "before", 3),
        (False, False, "limiter", 3),
        (False, False, "before", 5),
        (False, False, "limiter", 5),
    ],
)
async def test_manager_full_files_sdk_keeps_files_and_retry_controls(publication, recovery, url_rejected, withdraw, remaining):
    import asyncio

    from test_telegram_bot_adapter import fail, message
    from test_telegram_threads import inbound

    from app.telegram_bots.producer import OutboxSender
    from app.telegram_bots.turns import ChatTurns, _Turn

    e = publication
    await e.db.links.end_pending(e.link.id, skip_reason="superseded")
    e.live.config.model_extra["telegram_bots"] = {"enabled": True, "public_base_url": "https://app.example"}
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceGrantRow).values(permissions=7))
    record = await e.coordinator.dispatch(e.link, [e.origin], thread_id=e.thread_id)
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http_client.endpoint_id)
    record = await e.inbox.bind_publication_run(record, run_id="files-full-run", guard=attempt.read_guard)
    row = run_row(record, run_id=record.run_id)
    row.update(status="error", stop_reason="files-full", metadata={"channel_turn_id": record.publication_id})
    run = ValidatedRun(MappingProxyType(row), receipt_from_run_kwargs(row["kwargs"]))
    services = e.channel._services
    services.authority = e.live.authority
    services.eligibility = e.live.authority.delivery
    services.callbacks = CallbackRepository(e.db.sessions)
    services.prepare_live_owner = e.live.authority.prepare_live_owner

    async def values(link):
        return {"name": (await e.live.authority.instance_view(link)).name}

    services.prepare_ingress_values = values
    if recovery:
        e.coordinator.host = services
        result = await e.coordinator._close_run(record, e.link, run, attempt.read_guard, answered=False)
        identity = result.outbox_id
    else:
        turns = ChatTurns(services)
        turn = _Turn(e.channel, e.link, inbound(e), (e.origin,), record=record)
        turns._turns[(e.link.id, None)] = turn
        identity = await turns._publication_outcome(turn, "refused.files_full", run=run)
    request = e.channel.bot.request
    if url_rejected:
        fail(request, endpoint="sendMessage", code=400, description="Button URL invalid")
    message(request)

    async def revoke(delay=0):
        async with e.db.sessions.begin() as writer:
            await writer.execute(update(AgentInstanceGrantRow).values(permissions=remaining))
        e.channel.bot.rate_limiter._short_hold_until = 0

    if withdraw == "before":
        await revoke()
    elif withdraw == "limiter":
        e.channel.bot.rate_limiter._sleep = revoke
        e.channel.bot.rate_limiter._short_hold_until = e.channel.bot.rate_limiter._time() + 1

    async def raw(*, link_id, **kwargs):
        return await e.channel.send_part(**kwargs)

    sender = OutboxSender(services.outbox, run_id="files-reply-fixture", deliver_part=raw, eligibility=e.live.authority.delivery, jitter=lambda: 0)
    try:
        for _ in range(2 if url_rejected else 1):
            await sender.tick()
            await asyncio.wait_for(sender.drain(), 3)
            services.outbox.clock.__self__.advance(1)
        calls = [call for call in request.calls if call.endpoint == "sendMessage"]
        if withdraw:
            assert calls == []
            assert (await _primary(e.db, identity)).state != "sent"
            return
        first = calls[0].parameters["reply_markup"]["inline_keyboard"]
        assert len(first) == 2 and first[0][0]["text"] == "Open Files" and "url" in first[0][0]
        assert first[1][0]["text"] == "Try again" and first[1][0]["callback_data"].startswith("h1:")
        if url_rejected:
            final = calls[-1].parameters
            assert final["reply_markup"]["inline_keyboard"] == [first[1]]
            assert unescape(final["text"]).endswith(first[0][0]["url"])
        assert (await _primary(e.db, identity)).state == "sent"
    finally:
        await sender.stop()
