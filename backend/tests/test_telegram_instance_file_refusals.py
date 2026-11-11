"""File admission failures close the original input without a download or run."""

import asyncio

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select, update
from telegram import Update
from test_telegram_bot_adapter import message as sent_message
from test_telegram_bot_handlers import message
from test_telegram_instance_file_admission import file_admission as file_admission
from test_telegram_instance_file_input import input_environment
from test_telegram_instance_lifecycle import lifecycle as lifecycle
from test_telegram_instance_publication import publication as publication
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.gateway.storage_spaces import ExecutionState, require_execution
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow, TelegramOutboxRow

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("status,permissions,key", [("suspended", 7, "file.not_active"), ("archived", 7, "file.not_active"), ("suspended", 2, "access.agent_off")])
async def test_inactive_file_gate_preserves_use_precedence_and_sends_file_wording(lifecycle, status, permissions, key):
    from app.telegram_bots.render import render

    e = lifecycle
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == e.link.agent_ref).values(status=status))
        await writer.execute(update(AgentInstanceGrantRow).where(AgentInstanceGrantRow.instance_id == e.link.agent_ref).values(permissions=permissions))
    payload = message(e, 2301, content="Read this", caption=True).to_dict()
    payload["message"]["document"] = {"file_id": "document", "file_unique_id": "unique", "file_name": "input.txt", "file_size": 4}
    sent_message(e.runtime.normal)
    assert await e.channel.handle_update(Update.de_json(payload, e.channel.bot))
    await asyncio.gather(*tuple(e.channel._ingress_tasks))
    calls = [call for call in e.runtime.normal.calls if call.endpoint == "sendMessage"]
    assert len(calls) == 1
    if key == "file.not_active":
        assert calls[0].parameters["text"] == render(key, "en", name="Fixture employee")
        assert "reply_markup" not in calls[0].parameters
    else:
        assert "You no longer have access" in calls[0].parameters["text"]
    assert not any(call.endpoint == "getFile" for call in e.runtime.normal.calls)
    assert e.runtime.service.bus.inbound_queue.empty()


@pytest.mark.parametrize("caption", ["", "Read the file"])
async def test_readiness_refusal_closes_entire_input_without_journal_or_download(file_admission, monkeypatch, caption):
    e = file_admission
    turns, origin, file, request, reads, started = await input_environment(e, monkeypatch, text=caption)

    async def held(instance_id):
        assert instance_id == e.link.agent_ref
        return ExecutionState(True, "unavailable", "operator-hold")

    e.file_service.execution_probe = held
    monkeypatch.setattr("app.gateway.storage_spaces.require_execution", require_execution)
    await turns._admit_origins(e.channel, e.link, (origin,))
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramActRow)))
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, origin.message_id, e.link.chat_id))
        assert mark.route_complete and mark.thread_id is None
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("notice:chat-input:%"))))
        assert len(rows) == 1 and rows[0].parts[0]["source_ref"]["key"] == "unavailable.paused"
    assert not reads and not started and e.bus.inbound_queue.empty()


async def test_pause_after_inbox_admission_closes_caption_and_uses_file_notice(file_admission, monkeypatch):
    e = file_admission
    turns, origin, file, request, reads, started = await input_environment(e, monkeypatch, text="Read the file")
    notices = []

    async def notice(link, *, file_input=False):
        notices.append((link.id, file_input))

    e.channel._services.notice_instance_unavailable = notice
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == e.link.agent_ref).values(status="suspended"))
    await turns._admit_origins(e.channel, e.link, (origin,))
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, origin.message_id, e.link.chat_id))
        assert mark.route_complete and mark.thread_id is None
        assert not list(await reader.scalars(select(TelegramActRow)))
    assert notices == [(e.link.id, True)] and not reads and not started


@pytest.mark.parametrize("change", ["grant", "status", "policy"])
async def test_readiness_outcome_rechecks_authority_in_its_final_writer(file_admission, monkeypatch, change):
    from datetime import UTC, datetime

    from app.telegram_bots.file_refusals import _RefusalGuard
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    e = file_admission
    turns, origin, file, request, reads, started = await input_environment(e, monkeypatch, text="Read this")

    async def held(instance_id):
        return ExecutionState(True, "unavailable", "operator-hold")

    e.file_service.execution_probe = held
    monkeypatch.setattr("app.gateway.storage_spaces.require_execution", require_execution)
    original, changed = _RefusalGuard.check, []

    async def change_before_commit(guard, writer):
        if await writer.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.dedupe_key.like("notice:chat-input:%"))) and not changed:
            changed.append(True)
            if change == "grant":
                await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
            elif change == "status":
                await writer.execute(update(AgentInstanceRow).values(status="suspended"))
            else:
                writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="off", updated_by=e.link.owner_user_id, updated_at=datetime.now(UTC)))
                await writer.flush()
        return await original(guard, writer)

    monkeypatch.setattr(_RefusalGuard, "check", change_before_commit)
    with pytest.raises(AgentDenied):
        await turns._admit_origins(e.channel, e.link, (origin,))
    assert changed == [True] and not reads and not started
    async with e.db.sessions() as reader:
        assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key.like("notice:chat-input:%"))))
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, origin.message_id, e.link.chat_id))
        assert not mark.route_complete


async def test_file_notice_checks_recovery_at_the_actual_sdk_limiter(lifecycle):
    e = lifecycle
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceRow).values(status="suspended"))
    limiter = e.channel.bot.rate_limiter

    async def recovered(delay):
        async with e.db.sessions.begin() as writer:
            await writer.execute(update(AgentInstanceRow).values(status="active"))
        limiter._short_hold_until = 0

    limiter._sleep = recovered
    limiter._short_hold_until = limiter._time() + 1
    sent_message(e.runtime.normal)
    await asyncio.wait_for(e.supervisor.notice_instance_unavailable(e.link, file_input=True), 3)
    assert not [call for call in e.runtime.normal.calls if call.endpoint == "sendMessage"]


@pytest.mark.parametrize("change", ["disabled", "released", "inert", "binding"])
async def test_account_or_custody_withdrawn_at_final_refusal_snapshot_refuses(file_admission, change):
    from datetime import UTC, datetime

    from sqlalchemy import event, insert

    from app.telegram_bots.authority import AccessOff
    from app.telegram_bots.file_refusals import _RefusalGuard
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow
    from deerflow.persistence.user.access import DisabledIdentityRow
    from deerflow.persistence.user.model import UserRow

    e = file_admission
    e.channel._services.authority = e.live.authority
    e.channel._services.prepare_live_owner = e.live.authority.prepare_live_owner
    if change == "inert":
        async with e.db.sessions.begin() as writer:
            await writer.execute(update(UserRow).where(UserRow.id == e.link.owner_user_id).values(oauth_provider="fixture", oauth_id=e.link.owner_user_id, oauth_issuer="https://issuer.example"))
        e.live.config.auth.local.enabled = False
    guard = _RefusalGuard(e.channel._services, e.channel, e.link, lambda: e.active[0], repair=True)
    await guard.prepare()
    changed = []

    def withdraw(connection, cursor, statement, parameters, context, many):
        if "FROM agent_instances JOIN telegram_bot_links" in statement and not changed:
            changed.append(True)
            now = datetime.now(UTC)
            if change == "disabled":
                connection.execute(insert(DisabledIdentityRow).values(issuer=LOCAL_ACCOUNT_ISSUER, subject=e.link.owner_user_id, disabled_at=now))
            elif change == "inert":
                connection.execute(update(UserRow).where(UserRow.id == e.link.owner_user_id).values(oauth_provider=None, oauth_id=None, oauth_issuer=None))
            elif change == "binding":
                connection.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id).values(binding_generation=TelegramBotLinkRow.binding_generation + 1))
            else:
                connection.execute(update(UserRow).where(UserRow.id == e.link.owner_user_id).values(released_at=now))

    event.listen(e.db.engine.sync_engine, "before_cursor_execute", withdraw)
    try:
        async with e.db.sessions.begin() as writer:
            with pytest.raises((AccessOff, AgentDenied)):
                await guard.check(writer)
        assert changed == [True]
    finally:
        event.remove(e.db.engine.sync_engine, "before_cursor_execute", withdraw)
