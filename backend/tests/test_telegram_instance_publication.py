"""Instance publication over actual Telegram SQL, Gateway auth and binding reads."""

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from sqlalchemy import update
from test_telegram_bot_handlers import message
from test_telegram_instance import instance_environment
from test_telegram_threads import close, inbound

from app.channels.message_bus import MessageBus
from app.telegram_bots.publication import PublicationCoordinator, PublicationRefused
from app.telegram_bots.publication_authority import PublicationAttemptAuthority
from deerflow.persistence.agent_instances.model import AgentConversationRow, AgentInstanceGrantRow

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def publication(db, monkeypatch):
    e = await instance_environment(db, monkeypatch)
    e.db = db
    incoming = message(e, 907, content="Original employee question")
    row, _ = await e.inbox.accept(e.link, incoming, sanitized_input={"type": "text", "text": incoming.message.text})
    e.origin = await e.inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
    e.thread_id = await e.threads.ensure_thread(e.link, inbound(e), origin=e.origin)
    e.bus = MessageBus(inbound_queue_maxsize=4)
    e.host = PublicationAttemptAuthority(links=db.links, authority=e.live.authority, client=e.http_client, channel=lambda identity: e.current[0], create_allowed=lambda: e.active[0], read_allowed=lambda: e.active[0])
    e.coordinator = PublicationCoordinator(inbox=e.inbox, outbox=e.channel._services.outbox, bus=e.bus, http_owner=e.http_client, capture_attempt=e.host.capture)
    try:
        yield e
    finally:
        await e.coordinator.stop()
        e.bus.discard_pending_inbound()
        await close(e)


async def test_instance_publication_keeps_authoritative_binding_and_owner_timezone(publication):
    e = publication
    e.admissions.clear()
    record = await e.coordinator.dispatch(e.link, [e.origin], thread_id=e.thread_id)
    assert record.scope.agent_kind == "instance" and record.scope.agent_ref == e.link.agent_ref
    sent = e.bus.inbound_queue.get_nowait()
    assert sent.owner_user_id == e.link.owner_user_id and sent.text == "Original employee question"
    assert sent.execution_context.scope.instance_id == e.link.agent_ref
    assert sent.execution_context.scope.agent_name is None
    assert sent.metadata["human_message_kwargs"]["person_timezone"] == (await e.db.settings.get(e.link.owner_user_id)).timezone
    assert e.admissions and all(value == ("execution", e.link.agent_ref) or value == ("resident", e.link.agent_ref, False) for value in e.admissions)


@pytest.mark.parametrize("change", ["use", "binding", "authority"])
async def test_instance_publication_refuses_changed_authority_without_bus_handoff(publication, change):
    e = publication
    if change == "use":
        async with e.db.sessions.begin() as session:
            await session.execute(update(AgentInstanceGrantRow).values(permissions=2))
    elif change == "binding":
        async with e.db.sessions.begin() as session:
            await session.delete(await session.get(AgentConversationRow, e.thread_id))
            from deerflow.persistence.thread_meta.model import ThreadMetaRow

            await session.execute(update(ThreadMetaRow).where(ThreadMetaRow.thread_id == e.thread_id).values(metadata_json={"agent_instance_id": e.link.agent_ref}))
    else:
        e.http_app.state.agent_conversations = None
    with pytest.raises(PublicationRefused):
        await e.coordinator.dispatch(e.link, [e.origin], thread_id=e.thread_id)
    assert e.bus.inbound_queue.empty()


async def test_retained_instance_reads_require_current_use_without_execution_readiness(publication, monkeypatch):
    from app.channels.execution import ChannelExecutionProtocolError
    from app.gateway import storage_spaces

    e = publication
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http_client.endpoint_id)
    await attempt.admission.authorize("run_create")
    e.host.close_creation()

    async def unavailable(*args, **kwargs):
        raise AssertionError("Retained reads cannot require a workspace")

    monkeypatch.setattr(storage_spaces, "require_execution", unavailable)
    await attempt.admission.authorize("source_read")
    async with e.db.sessions.begin() as writer:
        assert (await attempt.read_guard.check(writer)).id == e.link.id
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
    with pytest.raises(ChannelExecutionProtocolError):
        await attempt.admission.authorize("source_read")


@pytest.mark.parametrize("change", ["use", "off", "discard", "binding", "protected"])
async def test_instance_writer_rechecks_grant_policy_binding_and_protected_provenance(publication, change):
    from app.channels.execution import ChannelExecutionProtocolError
    from app.telegram_bots.policy import InstancePolicy
    from deerflow.persistence.agent_instances.model import AgentProtectedContextRow

    e = publication
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http_client.endpoint_id)
    await attempt.create_guard.prepare()
    if change in {"off", "discard"}:
        store = InstancePolicy(e.db.sessions)

        async def checked(session):
            assert session.in_transaction()

        await store.set(e.link.agent_ref, "off", updated_by=e.link.owner_user_id, check_current=checked)
        if change == "discard":
            await store.set(e.link.agent_ref, "allowed", updated_by=e.link.owner_user_id, check_current=checked)
    else:
        async with e.db.sessions.begin() as writer:
            if change == "use":
                await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
            elif change == "binding":
                await writer.delete(await writer.get(AgentConversationRow, e.thread_id))
            else:
                writer.add(AgentProtectedContextRow(thread_id=e.thread_id))
    async with e.db.sessions.begin() as writer:
        with pytest.raises(ChannelExecutionProtocolError):
            await attempt.create_guard.check(writer)


async def test_execution_refusal_never_reaches_the_bus(publication, monkeypatch):
    from app.gateway import storage_spaces
    from deerflow.agent_instances.contract import ExecutionUnavailable

    e = publication

    async def unavailable(*args):
        raise ExecutionUnavailable("operator-hold")

    monkeypatch.setattr(storage_spaces, "require_execution", unavailable)
    with pytest.raises(PublicationRefused):
        await e.coordinator.dispatch(e.link, [e.origin], thread_id=e.thread_id)
    assert e.bus.inbound_queue.empty()


@pytest.mark.parametrize("operation", ["read", "writer"])
async def test_instance_authority_snapshot_drains_repeated_cancellation(publication, operation):
    import asyncio

    from sqlalchemy import event
    from sqlalchemy.util.concurrency import await_only

    e = publication
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http_client.endpoint_id)
    await attempt.read_guard.prepare()
    entered, release = asyncio.Event(), asyncio.Event()
    completed = []

    def pause(connection, cursor, statement, parameters, context, many):
        if "FROM telegram_bot_links JOIN agent_instances" in statement:
            entered.set()
            await_only(release.wait())
            completed.append(True)

    async def act():
        if operation == "read":
            await attempt.admission.authorize("source_read")
        else:
            async with e.db.sessions.begin() as writer:
                await attempt.read_guard.check(writer)

    event.listen(e.db.engine.sync_engine, "before_cursor_execute", pause)
    task = asyncio.create_task(act())
    try:
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not completed
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert completed == [True]
        assert e.db.engine.sync_engine.pool.checkedout() == 0
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        event.remove(e.db.engine.sync_engine, "before_cursor_execute", pause)


@pytest.mark.parametrize("status", ["suspended", "archived"])
async def test_inactive_instance_cannot_publish_model_content_through_retained_read(publication, status):
    from app.channels.execution import ChannelExecutionProtocolError
    from deerflow.persistence.agent_instances.model import AgentInstanceRow

    e = publication
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http_client.endpoint_id)
    await attempt.admission.authorize("source_read")
    async with e.db.sessions.begin() as writer:
        await writer.execute(update(AgentInstanceRow).where(AgentInstanceRow.id == e.link.agent_ref).values(status=status))
    with pytest.raises(ChannelExecutionProtocolError):
        await attempt.admission.authorize("source_read")


@pytest.mark.parametrize("change", ["disabled", "released", "inert"])
async def test_account_withdrawn_at_final_instance_writer_snapshot_refuses(publication, change):
    from datetime import UTC, datetime

    from sqlalchemy import event, insert

    from app.channels.execution import ChannelExecutionProtocolError
    from app.telegram_bots.authority import AccessOff
    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.user.access import DisabledIdentityRow
    from deerflow.persistence.user.model import UserRow

    e = publication
    if change == "inert":
        async with e.db.sessions.begin() as writer:
            await writer.execute(update(UserRow).where(UserRow.id == e.link.owner_user_id).values(oauth_provider="fixture", oauth_id=e.link.owner_user_id, oauth_issuer="https://issuer.example"))
        e.live.config.auth.local.enabled = False
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http_client.endpoint_id)
    await attempt.create_guard.prepare()
    changed = []

    def withdraw(connection, cursor, statement, parameters, context, many):
        if "FROM telegram_bot_links JOIN agent_instances" in statement and not changed:
            changed.append(True)
            now = datetime.now(UTC)
            if change == "disabled":
                connection.execute(insert(DisabledIdentityRow).values(issuer=LOCAL_ACCOUNT_ISSUER, subject=e.link.owner_user_id, disabled_at=now))
            elif change == "inert":
                connection.execute(update(UserRow).where(UserRow.id == e.link.owner_user_id).values(oauth_provider=None, oauth_id=None, oauth_issuer=None))
            else:
                connection.execute(update(UserRow).where(UserRow.id == e.link.owner_user_id).values(released_at=now))

    event.listen(e.db.engine.sync_engine, "before_cursor_execute", withdraw)
    try:
        async with e.db.sessions.begin() as writer:
            with pytest.raises((AccessOff, ChannelExecutionProtocolError)):
                await attempt.create_guard.check(writer)
        assert changed == [True]
    finally:
        event.remove(e.db.engine.sync_engine, "before_cursor_execute", withdraw)


@pytest.mark.parametrize(
    "provider,issuer,fact_issuer,refused",
    [
        (False, None, "https://other.example", False),
        (True, "https://issuer.example/", "https://issuer.example", True),
        (True, "https://issuer.example", "https://other.example", False),
        (True, None, "https://other.example", True),
        (True, None, "local", False),
    ],
)
async def test_final_snapshot_matches_auth_identity_semantics(publication, provider, issuer, fact_issuer, refused):
    from datetime import UTC, datetime

    from sqlalchemy import event, insert

    from app.telegram_bots.authority import AccessOff
    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.user.access import DisabledIdentityRow
    from deerflow.persistence.user.model import UserRow

    e = publication
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http_client.endpoint_id)
    changed = []

    def change_identity(connection, cursor, statement, parameters, context, many):
        if "FROM telegram_bot_links JOIN agent_instances" in statement and not changed:
            changed.append(True)
            if provider:
                connection.execute(update(UserRow).where(UserRow.id == e.link.owner_user_id).values(oauth_provider="fixture", oauth_id=e.link.owner_user_id, oauth_issuer=issuer))
            connection.execute(insert(DisabledIdentityRow).values(issuer=LOCAL_ACCOUNT_ISSUER if fact_issuer == "local" else fact_issuer, subject=e.link.owner_user_id, disabled_at=datetime.now(UTC)))

    event.listen(e.db.engine.sync_engine, "before_cursor_execute", change_identity)
    try:
        async with e.db.sessions.begin() as writer:
            if refused:
                with pytest.raises(AccessOff):
                    await attempt.read_guard.check(writer)
            else:
                assert (await attempt.read_guard.check(writer)).id == e.link.id
        assert changed == [True]
    finally:
        event.remove(e.db.engine.sync_engine, "before_cursor_execute", change_identity)
