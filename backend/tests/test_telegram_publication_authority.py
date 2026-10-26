"""Actual SQL custody for the host's original creating/read attempt views."""

import asyncio
import traceback
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
import pytest_asyncio
from _telegram_ingress_test_support import install_live_owner
from _telegram_repository_test_support import code_of, welcome
from _telegram_repository_test_support import db as db
from telegram import Chat, Message, Update, User

from app.channels.execution import ChannelExecutionProtocolError
from app.telegram_bots import authority as live_authority
from app.telegram_bots.authority import AccessOff, CeilingRefused
from app.telegram_bots.inbox import InboxRepository
from app.telegram_bots.links import prepare_publication_guard
from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow
from deerflow.persistence.user.access import DisabledIdentityRow

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def attempt_environment(db, monkeypatch):
    live = await install_live_owner(db, monkeypatch)
    await db.settings.ensure(live.id, timezone="UTC", locale="en-US")
    link = await db.links.add(
        owner_user_id=live.id,
        agent_kind="lead_agent",
        agent_ref="lead_agent",
        bot_id=9_000_000_081,
        bot_username="attempt_fixture_bot",
        encrypted_token="wrapped-attempt-fixture",
        actor_id=live.id,
    )
    code = code_of(await db.links.mint_code(link))
    link = await db.links.bind(
        await db.links.get(link.id),
        telegram_user_id=7_000_000_081,
        chat_id=7_000_000_081,
        first_name="Fixture",
        username="attempt_person",
        code=code,
        start_identity={"update_id": 201, "message_id": 101, "chat_id": 7_000_000_081},
        hello_parts=welcome("hello.chat"),
    )
    channel = SimpleNamespace(link=link, transport_revision=link.token_revision, application=SimpleNamespace(running=True))
    current, creating, reading = [channel], [True], [True]
    client = SimpleNamespace(endpoint_id="e" * 64)
    from app.telegram_bots.publication_authority import PublicationAttemptAuthority

    host = PublicationAttemptAuthority(
        links=db.links,
        authority=live.authority,
        client=client,
        channel=lambda identity: current[0] if identity == link.id else None,
        create_allowed=lambda: creating[0],
        read_allowed=lambda: reading[0],
    )
    return SimpleNamespace(db=db, live=live, link=link, channel=channel, current=current, creating=creating, reading=reading, client=client, host=host)


async def capture(e):
    return await e.host.capture(e.link, thread_id="original-thread", endpoint_id=e.client.endpoint_id)


async def test_phase_one_closes_creating_but_retains_actual_borrowed_read_guard(attempt_environment):
    e = attempt_environment
    attempt = await capture(e)
    await attempt.admission.authorize("run_create")
    e.host.close_creation()
    e.creating[0] = False
    e.channel.application.running = False
    assert attempt.admission.current("run_create") is False
    await attempt.admission.authorize("source_read")
    async with e.db.sessions.begin() as writer:
        assert (await attempt.read_guard.check(writer)).id == e.link.id
    async with e.db.sessions.begin() as writer:
        with pytest.raises(ValueError):
            await attempt.create_guard.check(writer)
    e.host.close_reads()
    assert attempt.admission.current("source_read") is False
    with pytest.raises(ChannelExecutionProtocolError):
        await attempt.admission.authorize("source_read")


@pytest.mark.parametrize("change", ["adapter", "token", "endpoint", "read_admission"])
async def test_retirement_during_await_cannot_authorize_original_source(attempt_environment, monkeypatch, change):
    e = attempt_environment
    attempt = await capture(e)
    entered, release = asyncio.Event(), asyncio.Event()
    original = live_authority.require

    async def paused(owner, *actions, **kwargs):
        if actions:
            entered.set()
            await release.wait()
        return await original(owner, *actions, **kwargs)

    monkeypatch.setattr(live_authority, "require", paused)
    operation = asyncio.create_task(attempt.admission.authorize("source_read"))
    await entered.wait()
    if change == "adapter":
        e.current[0] = SimpleNamespace(link=e.link, transport_revision=e.link.token_revision, application=SimpleNamespace(running=True))
    elif change == "token":
        e.channel.link = replace(e.link, token_revision=e.link.token_revision + 1)
    elif change == "endpoint":
        e.client.endpoint_id = "f" * 64
    else:
        e.reading[0] = False
    release.set()
    with pytest.raises(ChannelExecutionProtocolError):
        await operation
    assert attempt.admission.current("source_read") is False


async def test_live_account_withdrawal_refuses_read_and_writer_after_capture(attempt_environment):
    e = attempt_environment
    attempt = await capture(e)
    async with e.db.sessions.begin() as writer:
        writer.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=e.live.id, disabled_at=datetime.now(UTC)))
    with pytest.raises(ChannelExecutionProtocolError):
        await attempt.admission.authorize("source_read")
    async with e.db.sessions.begin() as writer:
        with pytest.raises((AccessOff, ValueError)):
            await attempt.read_guard.check(writer)


@pytest.mark.parametrize(
    "operation,needed",
    [
        ("thread_read", {"threads:read"}),
        ("run_create", {"threads:read", "threads:write", "runs:create"}),
        ("run_read", {"threads:read", "runs:read"}),
        ("source_read", {"threads:read", "runs:read"}),
    ],
)
async def test_managed_operation_requires_its_explicit_product_ceilings(attempt_environment, monkeypatch, operation, needed):
    e = attempt_environment
    attempt = await capture(e)
    actual = []
    original = live_authority.require

    async def record(owner, *actions, **kwargs):
        actual.append((owner, set(actions)))
        return await original(owner, *actions, **kwargs)

    monkeypatch.setattr(live_authority, "require", record)
    await attempt.admission.authorize(operation)
    assert (e.live.id, needed) in actual


async def test_truthy_nonboolean_host_admission_and_unknown_operation_refuse(attempt_environment):
    e = attempt_environment
    attempt = await capture(e)
    e.creating[0] = 1
    assert attempt.admission.current("run_create") is False
    e.reading[0] = 1
    assert attempt.admission.current("source_read") is False
    assert attempt.admission.current("unknown") is False
    with pytest.raises(ChannelExecutionProtocolError):
        await attempt.admission.authorize("unknown")


async def test_canceled_authorization_drains_admitted_check_and_retires_its_attempt(attempt_environment, monkeypatch):
    e = attempt_environment
    attempt = await capture(e)
    entered, release = asyncio.Event(), asyncio.Event()
    original = live_authority.require

    async def retained(owner, *actions, **kwargs):
        if actions:
            entered.set()
            await release.wait()
        return await original(owner, *actions, **kwargs)

    monkeypatch.setattr(live_authority, "require", retained)
    operation = asyncio.create_task(attempt.admission.authorize("source_read"))
    await entered.wait()
    operation.cancel()
    await asyncio.sleep(0)
    assert not operation.done()
    operation.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await operation
    assert attempt.admission.current("source_read") is False


@pytest.mark.parametrize("withdraw", ["ceiling", "target"])
async def test_owned_reservation_refreshes_authority_after_earlier_preflight(attempt_environment, monkeypatch, withdraw):
    e = attempt_environment
    inbox = InboxRepository(e.db.sessions)
    incoming = Update(401, message=Message(301, datetime.now(UTC), Chat(e.link.chat_id, "private"), from_user=User(e.link.telegram_user_id, "Fixture", False), text="saved input"))
    row, _ = await inbox.accept(e.link, incoming, sanitized_input={"text": "saved input"})
    row = await inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"kind": "lead_agent"}, operation_ids=[])
    attempt = await capture(e)
    await attempt.admission.authorize("run_create")

    async def denied(*args, **kwargs):
        raise CeilingRefused

    async def unavailable(*args, **kwargs):
        return "agent_unavailable"

    if withdraw == "ceiling":
        monkeypatch.setattr(live_authority, "require", denied)
    else:
        monkeypatch.setattr(e.live.authority, "target", unavailable)
    with pytest.raises((ChannelExecutionProtocolError, ValueError)):
        await inbox.reserve_publication(e.link, [row], thread_id="original-thread", endpoint_id=e.client.endpoint_id, guard=attempt.create_guard)
    async with e.db.sessions() as reader:
        saved = await reader.get(TelegramInboundMarkRow, (e.link.id, row.message_id, row.chat_id))
        assert saved.turn_id is None and "publications" not in saved.route_basis


async def test_original_thread_and_endpoint_bookmarks_are_frozen(attempt_environment):
    e = attempt_environment
    attempt = await capture(e)
    assert attempt.thread_id == "original-thread" and attempt.endpoint_id == e.client.endpoint_id
    with pytest.raises(FrozenInstanceError):
        attempt.thread_id = "different-thread"
    with pytest.raises(FrozenInstanceError):
        attempt.endpoint_id = "f" * 64


@pytest.mark.parametrize("malformed", ["none", "boolean", "synchronous", "truthy-result"])
async def test_present_malformed_preparation_refuses_instead_of_using_legacy_fallback(malformed):
    async def truthy():
        return True

    values = {"none": None, "boolean": True, "synchronous": lambda: None, "truthy-result": truthy}
    with pytest.raises(ValueError, match="^publication-guard-unavailable$"):
        await prepare_publication_guard(SimpleNamespace(prepare=values[malformed]))


async def test_absent_preparation_preserves_legacy_and_valid_hook_is_awaited_once():
    assert await prepare_publication_guard(SimpleNamespace()) is None
    calls = []

    async def prepare():
        calls.append(True)

    assert await prepare_publication_guard(SimpleNamespace(prepare=prepare)) is None
    assert calls == [True]


async def test_preparation_cancellation_retains_the_admitted_async_collaborator():
    entered, release = asyncio.Event(), asyncio.Event()
    finished = []

    async def prepare():
        entered.set()
        await release.wait()
        finished.append(True)

    operation = asyncio.create_task(prepare_publication_guard(SimpleNamespace(prepare=prepare)))
    await entered.wait()
    operation.cancel()
    await asyncio.sleep(0)
    assert not operation.done()
    operation.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await operation
    assert finished == [True]


async def test_retired_admission_suppresses_a_private_callers_exception_context(attempt_environment):
    e = attempt_environment
    attempt = await capture(e)
    e.host.close_reads()
    try:
        raise ValueError("private-caller-diagnostic")
    except ValueError:
        try:
            await attempt.admission.authorize("source_read")
        except ChannelExecutionProtocolError as error:
            rendered = "".join(traceback.format_exception(error))
        else:
            pytest.fail("Retired authority was admitted")
    assert "private-caller-diagnostic" not in rendered
