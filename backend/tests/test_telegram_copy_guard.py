"""Known-host checks and conditional conflict latches use real SQL."""

import asyncio
import threading
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from _telegram_repository_test_support import add_link
from _telegram_repository_test_support import db as db
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.orm import Session

from app.telegram_bots import deployment
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramDeploymentRow

pytestmark = pytest.mark.asyncio
NOW = datetime(2030, 1, 1, tzinfo=UTC)


def guard(db, host="engine-a", *, sessions=None, clock=lambda: NOW):
    return deployment.CopyGuard(sessions or db.sessions, host_reader=lambda: host, clock=clock)


async def saved(db):
    async with db.sessions() as session:
        return await session.get(TelegramDeploymentRow, 1)


async def seed(db, *, host="engine-a", state="running", adopted=None):
    async with db.sessions.begin() as session:
        session.add(TelegramDeploymentRow(id=1, host_id=host, polling_state=state, adopted_at=adopted, updated_at=NOW))


async def test_peek_missing_row_is_read_only_and_captures_one_host_observation(db):
    reads = []

    def read():
        reads.append("host")
        return "engine-a"

    instance = deployment.CopyGuard(db.sessions, host_reader=read)
    assert await instance.peek() == "running"
    assert instance.this_host_id == "engine-a" and reads == ["host"]
    assert await saved(db) is None


async def test_peek_borrows_active_same_engine_snapshot_without_committing(db):
    instance = guard(db)
    async with db.sessions() as session:
        with pytest.raises(ValueError):
            await instance.peek(session=session)
        async with session.begin():
            session.add(TelegramDeploymentRow(id=1, host_id="older-engine", polling_state="polled_elsewhere", updated_at=NOW))
            await session.flush()
            assert await instance.peek(session=session) == "host_changed"
            assert session.in_transaction()
            await session.rollback()
    assert await saved(db) is None


async def test_borrowed_peek_does_not_autoflush_the_callers_pending_writes(db):
    statements = []

    def observe(connection, cursor, statement, parameters, context, many):
        statements.append(statement)

    async with db.sessions() as session, session.begin():
        session.add(TelegramDeploymentRow(id=1, host_id="not-persisted", polling_state="host_changed", updated_at=NOW))
        event.listen(db.engine.sync_engine, "before_cursor_execute", observe)
        try:
            assert await guard(db).peek(session=session) == "running"
        finally:
            event.remove(db.engine.sync_engine, "before_cursor_execute", observe)
        assert not any(statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for statement in statements)
        await session.rollback()
    assert await saved(db) is None


async def test_cancelled_host_observation_is_off_loop_and_drains_before_any_writer(db):
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    loop_thread = threading.get_ident()
    worker_threads = []

    def read():
        worker_threads.append(threading.get_ident())
        entered.set()
        assert release.wait(5)
        finished.set()
        return "engine-a"

    instance = deployment.CopyGuard(db.sessions, host_reader=read)
    task = asyncio.create_task(instance.at_start())
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not finished.is_set()
        assert await saved(db) is None
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set() and worker_threads[0] != loop_thread
        assert await saved(db) is None
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_host_observation_holds_no_capacity_writer_lock(db):
    entered, release = threading.Event(), threading.Event()

    def read():
        entered.set()
        assert release.wait(5)
        return "engine-a"

    instance = deployment.CopyGuard(db.sessions, host_reader=read)
    starting = asyncio.create_task(instance.at_start())
    adding = None
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        adding = asyncio.create_task(add_link(db))
        added = await asyncio.wait_for(asyncio.shield(adding), 3)
        assert added.owner_user_id == "owner-a" and not starting.done()
        release.set()
        assert await starting == "running"
        assert (await saved(db)).host_id == "engine-a"
    finally:
        release.set()
        await asyncio.gather(*(task for task in (starting, adding) if task is not None), return_exceptions=True)


@pytest.mark.parametrize(
    "stored_host,current_host,state,expected",
    [
        ("engine-a", "engine-b", "running", "host_changed"),
        ("engine-a", "engine-b", "polled_elsewhere", "host_changed"),
        ("engine-a", "engine-a", "polled_elsewhere", "polled_elsewhere"),
        ("engine-a", None, "running", "running"),
        ("engine-a", None, "host_changed", "host_changed"),
        (None, "engine-b", "running", "running"),
    ],
)
async def test_peek_precedence_does_not_write_any_row(db, stored_host, current_host, state, expected):
    await seed(db, host=stored_host, state=state)
    before = await saved(db)
    assert await guard(db, current_host).peek() == expected
    after = await saved(db)
    assert (after.host_id, after.polling_state, after.updated_at) == (before.host_id, before.polling_state, before.updated_at)


@pytest.mark.parametrize("host", ["engine-a", None])
async def test_at_start_creates_singleton_without_adopting_a_missing_host(db, host):
    instance = guard(db, host)
    assert await instance.at_start() == "running"
    row = await saved(db)
    assert row.host_id == host and row.polling_state == "running" and row.adopted_at is None


async def test_first_readable_host_is_captured_without_clearing_existing_latch(db):
    await seed(db, host=None, state="polled_elsewhere")
    assert await guard(db).at_start() == "polled_elsewhere"
    row = await saved(db)
    assert row.host_id == "engine-a" and row.polling_state == "polled_elsewhere"


async def test_changed_known_host_stays_refused_until_explicit_adoption(db):
    await seed(db, host="old-engine")
    instance = guard(db, "new-engine")
    assert await instance.at_start() == "host_changed"
    row = await saved(db)
    assert row.host_id == "old-engine" and row.polling_state == "host_changed"
    assert await instance.adopt() == {"from": "old-engine", "host_id": "new-engine", "verdict": "adopted"}
    assert await instance.at_start() == "running"


async def test_unreadable_host_does_not_replace_a_known_host(db):
    await seed(db)
    assert await guard(db, None).at_start() == "running"
    assert (await saved(db)).host_id == "engine-a"


async def test_concurrent_first_start_uses_one_shared_singleton_writer(db):
    other = async_sessionmaker(db.engine, expire_on_commit=False)
    result = await asyncio.gather(guard(db).at_start(), guard(db, sessions=other).at_start())
    assert result == ["running", "running"]
    async with db.sessions() as session:
        rows = list(await session.scalars(select(TelegramDeploymentRow)))
    assert len(rows) == 1 and rows[0].host_id == "engine-a"


async def test_concurrent_different_hosts_cannot_both_publish_a_first_running_state(db):
    result = await asyncio.gather(guard(db, "engine-a").at_start(), guard(db, "engine-b").at_start())
    assert sorted(result) == ["host_changed", "running"]
    row = await saved(db)
    assert row.host_id in {"engine-a", "engine-b"} and row.polling_state == "host_changed"


async def test_adopt_is_atomic_and_idempotent_with_exact_previous_host(db):
    instance = guard(db)
    assert await instance.adopt() == {"from": None, "host_id": "engine-a", "verdict": "adopted"}
    before = await saved(db)
    assert await guard(db, clock=lambda: NOW + timedelta(hours=1)).adopt() == {"from": "engine-a", "host_id": "engine-a", "verdict": "already_adopted"}
    after = await saved(db)
    assert after.adopted_at == before.adopted_at and after.updated_at == before.updated_at


@pytest.mark.parametrize("host,state,reason", [(None, "running", "host-unreadable"), ("engine-a", "polled_elsewhere", "polled-elsewhere")])
async def test_adopt_refusal_preserves_every_deployment_field(db, host, state, reason):
    await seed(db, state=state)
    before = await saved(db)
    with pytest.raises(deployment.GuardRefused) as caught:
        await guard(db, host).adopt()
    assert caught.value.reason == reason and str(caught.value) == reason
    after = await saved(db)
    assert (after.host_id, after.polling_state, after.adopted_at, after.updated_at) == (before.host_id, before.polling_state, before.adopted_at, before.updated_at)


async def test_adopt_different_host_may_clear_the_previous_hosts_latch(db):
    await seed(db, host="old-engine", state="polled_elsewhere")
    assert await guard(db, "replacement-engine").adopt() == {"from": "old-engine", "host_id": "replacement-engine", "verdict": "adopted"}


@pytest.mark.parametrize("state,verdict", [(None, "already_running"), ("running", "already_running"), ("polled_elsewhere", "resumed")])
async def test_resume_uses_stored_state_without_reading_the_daemon(db, state, verdict):
    if state is not None:
        await seed(db, host="old-engine", state=state, adopted=NOW)

    def no_host_read():
        raise AssertionError("resume must not look up the daemon")

    instance = deployment.CopyGuard(db.sessions, host_reader=no_host_read)
    assert await instance.resume() == {"verdict": verdict}
    row = await saved(db)
    if state is None:
        assert row is None
    else:
        assert row.host_id == "old-engine" and row.adopted_at.replace(tzinfo=UTC) == NOW
        assert row.polling_state == "running"


async def test_resume_refuses_only_a_persisted_host_changed_state(db):
    await seed(db, host="old-engine", state="host_changed")
    with pytest.raises(deployment.GuardRefused) as caught:
        await guard(db, "different-host").resume()
    assert caught.value.reason == "host-changed"
    assert (await saved(db)).polling_state == "host_changed"


async def test_resume_before_start_does_not_repair_an_unadopted_host_mismatch(db):
    await seed(db, host="old-engine", state="polled_elsewhere")
    instance = guard(db, "new-engine")
    assert await instance.resume() == {"verdict": "resumed"}
    assert await instance.peek() == "host_changed"
    assert await instance.at_start() == "host_changed"
    assert (await saved(db)).host_id == "old-engine"


async def test_latch_and_operator_verbs_preserve_link_lifecycle_and_pause(db):
    link = await add_link(db)
    async with db.sessions.begin() as session:
        row = await session.get(TelegramBotLinkRow, link.id)
        row.acts_paused_since = NOW
        row.notices = {"conflict": NOW.isoformat()}
    before = await db.links.get(link.id)
    instance = guard(db)
    await instance.at_start()
    await instance.stop_for_broad_conflict()
    assert (await saved(db)).polling_state == "polled_elsewhere"
    await instance.resume()
    await guard(db, "replacement-engine").adopt()
    assert await db.links.get(link.id) == before


@pytest.mark.parametrize("operation", ["at_start", "adopt", "stop_for_broad_conflict"])
async def test_guard_commit_failure_rolls_back_singleton_creation(db, operation):
    def fail(session):
        if session.get_bind() is db.engine.sync_engine:
            raise RuntimeError("synthetic guard commit failure")

    event.listen(Session, "before_commit", fail)
    try:
        with pytest.raises(RuntimeError, match="synthetic guard commit failure"):
            await getattr(guard(db), operation)()
    finally:
        event.remove(Session, "before_commit", fail)
    assert await saved(db) is None


def link(identity, owner="owner-a", health="ok"):
    return SimpleNamespace(id=identity, owner_user_id=owner, health=health)


def record(*, polls=0, successes=0, conflicts=0, webhook=False):
    return SimpleNamespace(polls=polls, successes=successes, conflicts=conflicts, webhook=webhook)


@pytest.mark.parametrize(
    "owners,counts,expected",
    [
        (["a"], [(1, 0, 1)], False),
        (["a", "a"], [(1, 0, 1), (1, 0, 1)], True),
        (["a", "b"], [(1, 0, 1), (1, 0, 1)], True),
        (["a", "a", "b"], [(1, 0, 1), (1, 0, 1), (1, 1, 0)], False),
        (["a", "b", "c"], [(1, 0, 1), (1, 0, 1), (1, 1, 0)], True),
        (["a", "b", "c", "d"], [(1, 0, 1), (1, 0, 1), (1, 1, 0), (1, 1, 0)], False),
        (["a", "b"], [(2, 0, 1), (1, 0, 1)], False),
        (["a", "b"], [(2, 1, 1), (1, 0, 1)], False),
    ],
)
async def test_broad_conflict_uses_literal_majority_owners_and_all_409_history(db, owners, counts, expected):
    links = [link(str(i), owner) for i, owner in enumerate(owners)]
    results = {str(i): record(polls=p, successes=s, conflicts=c) for i, (p, s, c) in enumerate(counts)}
    assert guard(db).broad_conflict(links, results) is expected


@pytest.mark.parametrize("excluded", ["original-conflict", "webhook"])
async def test_set_aside_links_stay_in_the_total_but_not_the_409_candidates(db, excluded):
    links = [link("a", "a"), link("b", "b"), link("c", "c"), link("d", "d")]
    if excluded == "original-conflict":
        links[0].health = "conflict"
    results = {value.id: record(polls=1, conflicts=1, webhook=excluded == "webhook" and value.id == "a") for value in links}
    assert guard(db).broad_conflict(links, results) is True
    results["d"] = record(polls=1, successes=1)
    assert guard(db).broad_conflict(links, results) is False


async def test_preexisting_conflict_plus_success_counts_once_in_the_early_floor(db):
    links = [link("a", "a", "conflict"), link("b", "b"), link("c", "c")]
    results = {"a": record(polls=1, successes=1), "b": record(polls=1, conflicts=1), "c": record(polls=1, conflicts=1)}
    assert guard(db).can_still_be_broad_conflict(links, results) is True
    assert guard(db).broad_conflict(links, results) is True


async def test_early_predicate_retains_unpolled_candidates_and_releases_after_success(db):
    links = [link("a"), link("b"), link("c")]
    results = {"a": record(polls=1, successes=1)}
    assert guard(db).can_still_be_broad_conflict(links, results) is True
    results["b"] = record(polls=1, successes=1)
    assert guard(db).can_still_be_broad_conflict(links, results) is False
    assert guard(db).can_still_be_broad_conflict([], {}) is False
    assert guard(db).can_still_be_broad_conflict([link("one")], {}) is False


async def test_both_successful_deployments_can_proceed_without_an_ownership_election(db):
    links = [link("a", "a"), link("b", "b")]
    first = {value.id: record(polls=1, successes=1) for value in links}
    second = {value.id: record(polls=1, successes=1) for value in links}
    for observations in (first, second):
        assert guard(db).can_still_be_broad_conflict(links, observations) is False
        assert guard(db).broad_conflict(links, observations) is False


async def test_broad_conflict_log_counts_share_the_set_aside_and_owner_classification(db):
    links = [link("a", "a", "conflict"), link("b", "b"), link("c", "c"), link("unpolled", "d")]
    observations = {"a": record(polls=1, successes=1), "b": record(polls=1, conflicts=1), "c": record(polls=1, conflicts=1)}
    instance = guard(db)
    assert instance.broad_conflict_counts(links, observations) == {"polled": 3, "counted": 2, "people": 2}
    assert instance.broad_conflict(links, observations) is True
    assert instance.broad_conflict_counts([*links, links[1]], observations) == {"polled": 3, "counted": 2, "people": 2}
    observations["c"].webhook = True
    assert instance.broad_conflict_counts(links, observations) == {"polled": 3, "counted": 1, "people": 1}
    assert instance.broad_conflict(links, observations) is False
