"""Actual Work effects retain authority after the final provider await."""

from contextvars import ContextVar
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_work_actions import actions as actions

from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputRequestRow
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramInstancePolicyRow


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["recipient_grant", "recipient_disabled", "manager", "binding", "policy", "cancelled", "presentation"])
async def test_route_effect_rolls_back_changes_during_final_provider_await(actions, monkeypatch, change):
    from app.gateway import authz
    from app.gateway.auth.models import User
    from deerflow.agent_instances.human_input import HumanInput
    from deerflow.authz.provider import AuthzDecision
    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.user.access import DisabledIdentityRow

    e = actions
    recipient = await e.live.users.create_user(User(email="effect-recipient@example.com"))
    recipient_id, operation_id = str(recipient.id), uuid4().hex
    async with e.db.sessions.begin() as writer:
        writer.add(AgentInstanceGrantRow(instance_id=e.instance.id, user_id=recipient_id, permissions=3))
    current_writer = ContextVar("effect_test_writer", default=None)
    original = HumanInput._project
    changed = []

    async def project(self, session, *args, **kwargs):
        result = await original(self, session, *args, **kwargs)
        # Activate the race only after this exact domain event exists in its
        # uncommitted service writer, never during journal/source preparation.
        event = await session.scalar(select(HumanInputEventRow).where(HumanInputEventRow.operation_id == operation_id, HumanInputEventRow.action == "route"))
        if event is not None:
            current_writer.set(session)
        return result

    class Provider:
        async def aauthorize(self, request):
            writer = current_writer.get()
            if writer is not None and not changed:
                assert writer.in_transaction() and writer.is_active
                changed.append(True)
                now = datetime.now(UTC)
                if change in {"recipient_grant", "manager"}:
                    identity = recipient_id if change == "recipient_grant" else e.actor.subject_id
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, identity))).permissions = 1 if change == "recipient_grant" else 3
                elif change == "recipient_disabled":
                    writer.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=recipient_id, disabled_at=now))
                elif change == "binding":
                    (await writer.get(TelegramBotLinkRow, e.link.id)).binding_generation += 1
                elif change == "policy":
                    policy = await writer.get(TelegramInstancePolicyRow, e.instance.id)
                    if policy is None:
                        writer.add(TelegramInstancePolicyRow(instance_id=e.instance.id, policy="off", updated_by=e.actor.subject_id, updated_at=now))
                    else:
                        policy.policy = "off"
                elif change == "presentation":
                    # Simulate a retained historical transfer reaching a pending
                    # row: the final effect guard must reject the receipt even
                    # though its old refused phase differs from pending.
                    (await writer.get(TelegramActRow, operation_id)).continuation = {"version": 2, "work_outcome_transfer": {"phase": ["refused", "storage-busy", 0], "outbox_id": uuid4().hex}}
                else:
                    (await writer.get(TelegramActRow, operation_id)).cancel_requested_at = now
                await writer.flush()
            return AuthzDecision(allow=True)

    provider = Provider()

    async def prepared(_config):
        return provider

    e.live.config.authorization.enabled = True
    monkeypatch.setattr(authz, "_aget_cached_route_provider", prepared)
    monkeypatch.setattr(HumanInput, "_project", project)
    result = await e.acts.route(e.link, operation_id=operation_id, request_id=e.request["id"], request_revision=1, recipient_id=recipient_id)
    assert changed
    assert result.state == "refused", result
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(HumanInputEventRow).where(HumanInputEventRow.operation_id == operation_id)) == 0
        request = await reader.get(HumanInputRequestRow, e.request["id"])
        assert request.recipient_id == e.actor.subject_id and request.request_revision == 1
        assert (await reader.get(AgentInstanceGrantRow, (e.instance.id, recipient_id))).permissions == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["binding", "operation"])
async def test_final_provider_wait_retains_binding_and_operation_locks(actions, monkeypatch, target):
    import asyncio

    from sqlalchemy import text, update
    from sqlalchemy.exc import DBAPIError

    from app.gateway import authz
    from deerflow.agent_instances.human_input import HumanInput
    from deerflow.authz.provider import AuthzDecision

    e, operation_id = actions, uuid4().hex
    inside_effect = ContextVar("effect_lock_test", default=False)
    entered, release = asyncio.Event(), asyncio.Event()
    original = HumanInput._project

    async def project(self, session, *args, **kwargs):
        result = await original(self, session, *args, **kwargs)
        if await session.scalar(select(HumanInputEventRow.id).where(HumanInputEventRow.operation_id == operation_id)):
            inside_effect.set(True)
        return result

    class Provider:
        async def aauthorize(self, request):
            if inside_effect.get():
                entered.set()
                await release.wait()
            return AuthzDecision(allow=True)

    async def prepared(_config):
        return Provider()

    e.live.config.authorization.enabled = True
    monkeypatch.setattr(authz, "_aget_cached_route_provider", prepared)
    monkeypatch.setattr(HumanInput, "_project", project)
    pending = asyncio.create_task(e.acts.route(e.link, operation_id=operation_id, request_id=e.request["id"], request_revision=1, recipient_id=e.actor.subject_id))
    arrived = asyncio.create_task(entered.wait())
    try:
        await asyncio.wait({pending, arrived}, return_when=asyncio.FIRST_COMPLETED)
        assert entered.is_set(), pending.result()
        with pytest.raises(DBAPIError) as error:
            async with e.db.sessions() as other, other.begin():
                await other.execute(text("PRAGMA busy_timeout=0" if e.db.dialect == "sqlite" else "SET LOCAL lock_timeout='1ms'"))
                statement = (
                    update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == e.link.id).values(binding_generation=e.link.binding_generation + 1)
                    if target == "binding"
                    else update(TelegramActRow).where(TelegramActRow.id == operation_id).values(cancel_requested_at=datetime.now(UTC))
                )
                await other.execute(statement)
        assert getattr(error.value.orig, "sqlite_errorcode", None) == 5 if e.db.dialect == "sqlite" else getattr(error.value.orig, "sqlstate", None) == "55P03"
    finally:
        release.set()
        arrived.cancel()
        result = (await asyncio.gather(pending, arrived, return_exceptions=True))[0]
    assert result.state == "done", result
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(HumanInputEventRow).where(HumanInputEventRow.operation_id == operation_id)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("recovery", ["check", "reconcile_receipt"])
async def test_receipt_recovery_ignores_withdrawn_fresh_route_authority(actions, monkeypatch, recovery):
    from app.gateway.auth.models import User
    from app.telegram_bots import work_effect_authority
    from deerflow.agent_instances.human_input import HumanInput
    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.user.access import DisabledIdentityRow

    e, operation_id = actions, uuid4().hex
    user = await e.live.users.create_user(User(email="receipt-recipient@example.com"))
    recipient_id = str(user.id)
    async with e.db.sessions.begin() as writer:
        writer.add(AgentInstanceGrantRow(instance_id=e.instance.id, user_id=recipient_id, permissions=3))
    completed = await e.acts.route(e.link, operation_id=operation_id, request_id=e.request["id"], request_revision=1, recipient_id=recipient_id)
    assert completed.state == "done"
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramActRow, operation_id)
        row.state, row.result = "uncertain", None
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.actor.subject_id))).permissions = 3
        (await writer.get(AgentInstanceGrantRow, (e.instance.id, recipient_id))).permissions = 1
        writer.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=recipient_id, disabled_at=datetime.now(UTC)))
        (await writer.get(TelegramBotLinkRow, e.link.id)).acts_paused_since = datetime.now(UTC)

    async def forbidden(*args, **kwargs):
        raise AssertionError("An exact receipt must not prepare or execute a fresh effect")

    monkeypatch.setattr(work_effect_authority, "prepare_effect_admission", forbidden)
    monkeypatch.setattr(HumanInput, "command", forbidden)
    recovered = await getattr(e.acts, recovery)(operation_id)
    assert recovered.state == "done" and recovered.body == completed.body and recovered.attempts == completed.attempts
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(HumanInputEventRow).where(HumanInputEventRow.operation_id == operation_id)) == 1
        assert (await reader.get(HumanInputRequestRow, e.request["id"])).request_revision == 2
