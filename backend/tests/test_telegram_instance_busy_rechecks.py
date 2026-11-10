"""A creating refusal is rechecked through the actual conversation authority."""

from dataclasses import replace
from types import SimpleNamespace

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from sqlalchemy import update
from test_telegram_instance_admission import admission as admission
from test_telegram_instance_publication import publication as publication
from test_telegram_publication_primary import _primary

from app.channels.execution import ChannelExecutionBusyError, ChannelExecutionProtocolError
from app.channels.outcomes import capture_creation_refusal
from app.telegram_bots.producer import _item, validate_publication_outcome
from app.telegram_bots.publication import channel_execution_scope
from deerflow.agent_instances.contract import DefinitionSnapshot, Refusal
from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceRow
from deerflow.persistence.spaces.model import SpaceGrantRow
from deerflow.spaces.contract import PrincipalRef
from deerflow.spaces.principals import HostPrincipalResolver
from deerflow.spaces.registry import SpaceRegistry

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def busy(admission):
    e = admission
    agents = e.http_app.state.agent_instances
    async with e.db.engine.begin() as connection:
        await connection.run_sync(lambda sync: SpaceGrantRow.__table__.create(sync, checkfirst=True))
    definition = DefinitionSnapshot.capture(owner_id=e.link.owner_user_id, config={"name": "fixture-assistant"}, soul="")
    async with e.db.sessions.begin() as writer:
        writer.add(AgentDefinitionRevisionRow(revision=definition.revision, owner_id=definition.owner_id, config=definition.config, soul=definition.soul))
        await writer.flush()
        instance = await writer.get(AgentInstanceRow, e.link.agent_ref)
        instance.definition_revision = definition.revision
        writer.add(SpaceGrantRow(space_id=instance.home_id, principal_kind="nonhuman", subject_id=instance.principal_id, permissions=3))
    agents.files.registry = SpaceRegistry(e.db.sessions, HostPrincipalResolver(human=agents.directory.human, nonhuman=agents.directory.lookup, nonhuman_in_session=agents.directory.lookup_in_session))
    # This exercises actual SQL execution binding and storage audience admission;
    # there is no workspace acquisition or model dispatch in the recheck.
    execution = await e.http_app.state.agent_conversations.execution(actor=PrincipalRef("human", e.link.owner_user_id), thread_id=e.thread_id, require_ready=True)
    assert execution.instance.id == e.link.agent_ref
    assert await e.turns._prepare(e.turn)
    incoming = e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    e.refusal = capture_creation_refusal(incoming.execution_context, ChannelExecutionBusyError())
    e.turn.refusal = e.refusal
    e.scope = channel_execution_scope(e.turn.record)
    return e


@pytest.mark.parametrize(
    "reason,key,retry",
    [(None, "busy.thread", True), ("execution-unavailable", "unavailable.paused", False), ("restarting", "refused.restarting", False), ("storage-busy", "refused.files_busy", False), ("run-limit", "busy.two_conversations", True)],
)
async def test_recheck_uses_actual_execution_then_resident_admission(busy, reason, key, retry):
    from app.telegram_bots.instance_refusals import recheck_creation_refusal, validate_rechecked_refusal

    e = busy
    calls = []

    async def readiness(identity):
        calls.append(("execution", identity))
        return SimpleNamespace(execution="unavailable" if reason == "execution-unavailable" else "available", reason="operator-hold")

    async def resident(view, *, work):
        calls.append(("resident", view.id, work))
        if reason:
            raise Refusal("Host wording is not presentation", reason=reason, cause="lifecycle")

    e.http_app.state.agent_instances.execution_probe = readiness
    e.http_app.state.agent_instances.residents.admit = resident
    proof = await recheck_creation_refusal(e.channel._services, e.link, e.turn.record, e.refusal)
    failure = validate_rechecked_refusal(proof, e.scope)
    assert (failure.text_key, failure.retry_later) == (key, retry)
    assert calls == [("execution", e.link.agent_ref)] + ([] if reason == "execution-unavailable" else [("resident", e.link.agent_ref, False)])
    assert e.bus.inbound_queue.empty()
    with pytest.raises(ChannelExecutionProtocolError):
        validate_rechecked_refusal(proof, replace(e.scope, publication_id="a" * 32))


@pytest.mark.parametrize("reason", ["start-failed", "starting-elsewhere", "files-full", "work-limit"])
async def test_run_only_refusals_do_not_become_creating_evidence(busy, reason):
    from app.telegram_bots.instance_refusals import recheck_creation_refusal

    async def resident(view, *, work):
        raise Refusal("Not a send refusal", reason=reason)

    busy.http_app.state.agent_instances.residents.admit = resident
    with pytest.raises(Refusal):
        await recheck_creation_refusal(busy.channel._services, busy.link, busy.turn.record, busy.refusal)


@pytest.mark.parametrize("reason,key", [("storage-busy", "refused.files_busy"), ("restarting", "refused.restarting"), ("run-limit", "busy.gave_up"), ("execution-unavailable", "unavailable.moved")])
async def test_rechecked_manual_outcome_is_saved_with_original_publication(busy, reason, key):
    from app.telegram_bots.instance_refusals import recheck_creation_refusal

    e = busy

    async def resident(view, *, work):
        raise Refusal("Private diagnostic wording", reason=reason, cause="lifecycle", execution_reason="host-replaced")

    e.http_app.state.agent_instances.residents.admit = resident
    proof = await recheck_creation_refusal(e.channel._services, e.link, e.turn.record, e.refusal)
    identity = await e.turns._publication_outcome(e.turn, key, refusal=proof)
    item = _item(await _primary(e.db, identity))
    result = validate_publication_outcome(item)
    assert result.kind == "instance_creation_refusal" and result.record.publication_id == e.turn.record.publication_id
    assert "Private diagnostic wording" not in str(item.payload)
    item.payload["turn_outcome_basis"]["proof"]["scope"]["instance_id"] = "f" * 32
    with pytest.raises(ValueError):
        validate_publication_outcome(item)


@pytest.mark.parametrize("change", ["use", "binding", "service", "original"])
async def test_recheck_refuses_stale_authority_before_readiness(busy, change):
    from app.telegram_bots.instance_refusals import recheck_creation_refusal
    from deerflow.persistence.agent_instances.model import AgentConversationRow, AgentInstanceGrantRow

    e = busy
    called = []

    async def readiness(identity):
        called.append(identity)
        raise AssertionError("Invalid authority must not read readiness")

    e.http_app.state.agent_instances.execution_probe = readiness
    if change in {"use", "binding"}:
        async with e.db.sessions.begin() as writer:
            if change == "use":
                await writer.execute(update(AgentInstanceGrantRow).values(permissions=2))
            else:
                await writer.delete(await writer.get(AgentConversationRow, e.thread_id))
    elif change == "service":
        e.http_app.state.agent_conversations = None
    else:
        e.refusal = object()
    with pytest.raises(ChannelExecutionProtocolError):
        await recheck_creation_refusal(e.channel._services, e.link, e.turn.record, e.refusal)
    assert not called


async def test_recheck_drains_actual_readiness_after_repeated_cancellation(busy):
    import asyncio

    from app.telegram_bots.instance_refusals import recheck_creation_refusal

    e = busy
    entered, release = asyncio.Event(), asyncio.Event()
    completed = []

    async def readiness(identity):
        entered.set()
        await release.wait()
        completed.append(True)
        return SimpleNamespace(execution="available", reason=None)

    e.http_app.state.agent_instances.execution_probe = readiness
    task = asyncio.create_task(recheck_creation_refusal(e.channel._services, e.link, e.turn.record, e.refusal))
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
        assert completed == [True] and e.bus.inbound_queue.empty()
        assert e.db.engine.sync_engine.pool.checkedout() == 0
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_chat_driver_uses_rechecked_manual_failure_without_retrying_publication(busy, monkeypatch):
    import asyncio

    from sqlalchemy import select

    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = busy
    calls = []

    async def resident(view, *, work):
        calls.append((view.id, work))
        raise Refusal("Manager wording is never a source", reason="restarting")

    async def already_prepared(turn):
        return True

    async def no_preview(turn):
        pass

    async def no_run(*args):
        return None

    async def watch(record, link, *, turn_done, **kwargs):
        await turn_done.wait()

    e.http_app.state.agent_instances.residents.admit = resident
    monkeypatch.setattr(e.turns, "_prepare", already_prepared)
    monkeypatch.setattr(e.turns, "_view", no_preview)
    monkeypatch.setattr(e.turns.observer, "read_run", no_run)
    monkeypatch.setattr(e.turns.observer, "watch", watch)
    await asyncio.wait_for(e.turns._drive(e.turn), 5)
    assert e.turn.done.is_set() and e.bus.inbound_queue.empty()
    assert calls == [(e.link.agent_ref, False)]
    async with e.db.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "reply:" + e.turn.record.turn_id)))
        assert len(rows) == 1
        outcome = validate_publication_outcome(_item(rows[0]))
        assert outcome.source_ref["key"] == "refused.restarting" and outcome.kind == "instance_creation_refusal"
