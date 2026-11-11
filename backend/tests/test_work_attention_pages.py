"""Stable complete service scans keep old actionable Work visible."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from _telegram_work_test_support import work_environment
from sqlalchemy import select
from test_telegram_threads import close

from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkRow


@pytest_asyncio.fixture
async def work_pages(db, monkeypatch):
    e = await work_environment(db, monkeypatch)
    try:
        yield e
    finally:
        await close(e)


async def seed_records(e, count):
    async with e.db.sessions.begin() as session:
        source = await session.get(AgentWorkRow, e.work["id"])
        values = {col.name: getattr(source, col.name) for col in AgentWorkRow.__table__.columns}
        now = datetime.now(UTC)
        for index in range(count):
            values_for_row = {**values, "id": uuid4().hex, "status": "cancelled", "review_state": "none", "blocker": None, "created_at": now - timedelta(minutes=count - index), "updated_at": now}
            session.add(AgentWorkRow(**values_for_row))
        source.created_at = now - timedelta(days=1)
    return now


@pytest.mark.asyncio
async def test_work_pages_cover_all_records_with_stable_ties_and_frozen_horizon(work_pages):
    e = work_pages
    horizon = await seed_records(e, 125)
    page = await e.human.work.list_page(actor=e.actor, instance_id=e.instance.id, limit=10, as_of=horizon)
    ids = [item["id"] for item in page["items"]]
    assert ids[0] == e.work["id"]
    # A new record belongs to the next fresh scan, not this continuation.
    async with e.db.sessions.begin() as session:
        source = await session.get(AgentWorkRow, ids[-1])
        values = {col.name: getattr(source, col.name) for col in AgentWorkRow.__table__.columns}
        later = uuid4().hex
        session.add(AgentWorkRow(**{**values, "id": later, "created_at": horizon + timedelta(seconds=1)}))
    while page["next_cursor"]:
        page = await e.human.work.list_page(actor=e.actor, instance_id=e.instance.id, limit=17, cursor=page["next_cursor"], as_of=horizon)
        ids.extend(item["id"] for item in page["items"])
    assert len(ids) == len(set(ids)) == 126
    assert later not in ids


@pytest.mark.asyncio
async def test_cursor_cannot_change_instance_actor_or_horizon_and_revocation_is_current(work_pages):
    e = work_pages
    horizon = await seed_records(e, 3)
    page = await e.human.work.list_page(actor=e.actor, instance_id=e.instance.id, limit=1, as_of=horizon)
    for overrides in ({"instance_id": uuid4().hex}, {"as_of": horizon + timedelta(seconds=1)}):
        with pytest.raises(ValueError):
            await e.human.work.list_page(**{**dict(actor=e.actor, instance_id=e.instance.id, cursor=page["next_cursor"], as_of=horizon), **overrides})
    async with e.db.sessions.begin() as session:
        grant = await session.get(AgentInstanceGrantRow, (e.instance.id, e.actor.subject_id))
        grant.permissions = 1
    with pytest.raises(AgentDenied):
        await e.human.work.list_page(actor=e.actor, instance_id=e.instance.id, cursor=page["next_cursor"], as_of=horizon)


@pytest.mark.asyncio
async def test_mutable_status_never_changes_scan_position(work_pages):
    e = work_pages
    horizon = await seed_records(e, 5)
    page = await e.human.work.list_page(actor=e.actor, instance_id=e.instance.id, limit=2, as_of=horizon)
    seen = {item["id"] for item in page["items"]}
    async with e.db.sessions.begin() as session:
        later = await session.scalar(select(AgentWorkRow).where(AgentWorkRow.id.not_in(seen)).order_by(AgentWorkRow.created_at, AgentWorkRow.id))
        later.status = "open"
        later.updated_at = horizon + timedelta(hours=1)
        identity = later.id
    rest = await e.human.work.list_page(actor=e.actor, instance_id=e.instance.id, cursor=page["next_cursor"], as_of=horizon)
    assert identity in {item["id"] for item in rest["items"]}
    assert not seen.intersection(item["id"] for item in rest["items"])


async def seed_requests(e, *, hidden=110, pending=65):
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow

    async with e.db.sessions.begin() as session:
        work = await session.get(AgentWorkRow, e.work["id"])
        request = await session.get(HumanInputRequestRow, e.request["id"])
        work_values = {col.name: getattr(work, col.name) for col in AgentWorkRow.__table__.columns}
        request_values = {col.name: getattr(request, col.name) for col in HumanInputRequestRow.__table__.columns}
        now = datetime.now(UTC)
        request.created_at = now - timedelta(days=3)
        for index in range(hidden + pending):
            work_id, request_id = uuid4().hex, uuid4().hex
            moment = now - timedelta(days=2) + timedelta(seconds=index)
            session.add(AgentWorkRow(**{**work_values, "id": work_id, "created_at": moment}))
            await session.flush()
            # Closed records are inspectable history, but not pending Attention.
            session.add(HumanInputRequestRow(**{**request_values, "id": request_id, "work_id": work_id, "created_at": moment, "state": "closed" if index < hidden else "pending", "closed_reason": "withdrawn" if index < hidden else None}))
    return now


@pytest.mark.asyncio
async def test_request_filter_scans_past_a_full_batch_and_follows_more_than_fifty_pending(work_pages):
    e = work_pages
    horizon = await seed_requests(e)
    page = await e.human.list_page(actor=e.actor, view="pending", instance_id=e.instance.id, limit=25, as_of=horizon)
    seen = [item["id"] for item in page["items"]]
    assert seen[0] == e.request["id"]
    while page["next_cursor"]:
        page = await e.human.list_page(actor=e.actor, view="pending", instance_id=e.instance.id, limit=25, cursor=page["next_cursor"], as_of=horizon)
        seen.extend(item["id"] for item in page["items"])
    assert len(seen) == len(set(seen)) == 66


@pytest.mark.asyncio
async def test_complete_attention_ranks_old_actionable_records_before_new_closed_history(work_pages):
    from app.telegram_bots.attention import AttentionReader

    e = work_pages
    horizon = await seed_records(e, 125)
    collected = await AttentionReader(work=e.human.work, authority=e.live.authority, links=e.db.links).collect(e.link, as_of=horizon)
    assert len(collected["work"]) == 126
    assert collected["work"][0]["id"] == e.work["id"]
    assert collected["pending"][0]["id"] == e.request["id"]


@pytest.mark.asyncio
async def test_attention_borrows_account_reads_without_a_nested_pool_checkout(work_pages, monkeypatch):
    from sqlalchemy import event

    from app.gateway.auth.mode import account_refusal
    from app.telegram_bots.attention import AttentionReader
    from deerflow.spaces.contract import ResolvedPrincipal

    e = work_pages
    original = e.human.agents.directory.human

    class Human:
        async def __call__(self, actor):
            return await original(actor)

        async def in_session(self, actor, session):
            user = await e.live.users.get_user_by_id(actor.subject_id, session=session)
            return ResolvedPrincipal(actor) if user is not None and account_refusal(user, sign_on_only_mode=False) is None else None

    monkeypatch.setattr(e.human.agents.directory, "human", Human())
    active, maximum = [0], [0]

    def checkout(*args):
        active[0] += 1
        maximum[0] = max(maximum[0], active[0])

    def checkin(*args):
        active[0] -= 1

    event.listen(e.db.engine.sync_engine, "checkout", checkout)
    event.listen(e.db.engine.sync_engine, "checkin", checkin)
    try:
        result = await AttentionReader(work=e.human.work, authority=e.live.authority, links=e.db.links).collect(e.link, as_of=datetime.now(UTC))
        assert result["pending"]
        assert maximum[0] == 1 and active[0] == 0
    finally:
        event.remove(e.db.engine.sync_engine, "checkout", checkout)
        event.remove(e.db.engine.sync_engine, "checkin", checkin)


@pytest.mark.asyncio
async def test_equal_creation_times_use_id_ties_without_duplicates(work_pages):
    from sqlalchemy import update

    e = work_pages
    horizon = await seed_records(e, 25)
    async with e.db.sessions.begin() as session:
        await session.execute(update(AgentWorkRow).values(created_at=horizon))
        expected = list(await session.scalars(select(AgentWorkRow.id).order_by(AgentWorkRow.id)))
    cursor, seen = None, []
    while True:
        page = await e.human.work.list_page(actor=e.actor, instance_id=e.instance.id, limit=4, cursor=cursor, as_of=horizon)
        seen.extend(item["id"] for item in page["items"])
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert seen == expected


@pytest.mark.asyncio
async def test_attention_rejects_configuration_change_at_final_policy_read(work_pages):
    from sqlalchemy import event

    from app.telegram_bots.attention import AttentionReader
    from app.telegram_bots.authority import AuthorityUnavailable

    e = work_pages
    changed = []

    def change_mode(connection, cursor, statement, parameters, context, executemany):
        if not changed and statement.lstrip().upper().startswith("SELECT") and "JOIN telegram_instance_policy" in statement:
            e.live.config.auth.local.enabled = False
            changed.append(True)

    event.listen(e.db.engine.sync_engine, "after_cursor_execute", change_mode)
    try:
        with pytest.raises(AuthorityUnavailable):
            await AttentionReader(work=e.human.work, authority=e.live.authority, links=e.db.links)._current(e.link)
        assert changed
    finally:
        event.remove(e.db.engine.sync_engine, "after_cursor_execute", change_mode)


@pytest.mark.asyncio
@pytest.mark.parametrize("withdraw", ["account", "manage", "binding"])
async def test_attention_final_snapshot_rejects_changes_during_provider_evaluation(work_pages, monkeypatch, withdraw):
    from app.gateway import authz
    from app.telegram_bots.attention import AttentionReader
    from app.telegram_bots.authority import AccessOff, AuthorityUnavailable
    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow
    from deerflow.persistence.user.model import UserRow

    e = work_pages
    original = authz.resolve_route_permissions
    calls = []

    async def resolve(*args, **kwargs):
        held = await original(*args, **kwargs)
        calls.append(True)
        # Withdraw during the final collector provider evaluation, after
        # its decision but before the joined final authority snapshot.
        if len(calls) == 2:
            async with e.db.sessions.begin() as writer:
                if withdraw == "account":
                    (await writer.get(UserRow, e.actor.subject_id)).released_at = datetime.now(UTC)
                elif withdraw == "manage":
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.actor.subject_id))).permissions = 3
                else:
                    (await writer.get(TelegramBotLinkRow, e.link.id)).binding_generation += 1
        return held

    monkeypatch.setattr(authz, "resolve_route_permissions", resolve)
    with pytest.raises((AccessOff, AuthorityUnavailable, AgentDenied)):
        await AttentionReader(work=e.human.work, authority=e.live.authority, links=e.db.links).collect(e.link, as_of=datetime.now(UTC))
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_read_only_attention_retains_content_without_action_permissions(work_pages, monkeypatch):
    from app.gateway import authz
    from app.telegram_bots.attention import AttentionReader

    e = work_pages

    async def read_only(*args, **kwargs):
        return ["agents:read"]

    monkeypatch.setattr(authz, "resolve_route_permissions", read_only)
    result = await AttentionReader(work=e.human.work, authority=e.live.authority, links=e.db.links).collect(e.link, as_of=datetime.now(UTC))
    assert result["work"] and result["requests"]
    assert not result["pending"] and not result["routing"]
    assert not result["requests"][0]["can_respond"] and not result["requests"][0]["can_manage"]


@pytest.mark.asyncio
async def test_request_cursor_is_bound_to_actor_view_and_write_projection(work_pages):
    from deerflow.spaces.contract import PrincipalRef

    e = work_pages
    horizon = await seed_requests(e, hidden=1, pending=2)
    basis = dict(actor=e.actor, instance_id=e.instance.id, view="all", can_write=True, limit=1, as_of=horizon)
    page = await e.human.list_page(**basis)
    assert page["next_cursor"]
    for changed in ({"actor": PrincipalRef("human", str(uuid4()))}, {"view": "pending"}, {"can_write": False}):
        with pytest.raises(ValueError):
            await e.human.list_page(**{**basis, **changed, "cursor": page["next_cursor"]})
    for limit in (True, 0, 101):
        with pytest.raises(ValueError):
            await e.human.list_page(**{**basis, "limit": limit})


@pytest.mark.asyncio
async def test_attention_uses_one_captured_auth_mode_for_account_classification(work_pages, monkeypatch):
    from app.telegram_bots.attention import AttentionReader
    from app.telegram_bots.authority import AccessOff
    from deerflow.config import app_config

    e = work_pages
    strict = e.live.config.model_copy(deep=True)
    strict.auth.local.enabled = False
    permissive = e.live.config.model_copy(deep=True)
    permissive.auth.local.enabled = True
    reads = []

    def changing_config():
        reads.append(True)
        return strict if len(reads) == 1 else permissive

    monkeypatch.setattr(app_config, "get_app_config", changing_config)
    monkeypatch.setattr(app_config, "peek_loaded_app_config", lambda: strict)
    with pytest.raises(AccessOff):
        await AttentionReader(work=e.human.work, authority=e.live.authority, links=e.db.links)._current(e.link)
    assert reads
