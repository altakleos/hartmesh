"""Startup recovery runs outside any request, so it must not need a user.

A tenant VM was backed up whole -- disk image -- while one run was executing a
long bash tool call, and the image was restored to a new VM. The Gateway's
first start failed:

    RunRepository.get called with user_id=AUTO but no user context is set

``reconcile_orphaned_inflight_runs`` re-read the row it had just claimed, and
that read defaulted to the ``AUTO`` sentinel, which resolves the caller's user
from a contextvar the auth middleware sets per request. Startup has no request
and therefore no user, so the read raised and took the whole lifespan with it.

The damage is larger than one failed start. The claim commits *before* that
read, so the row is already terminal when the exception unwinds; the recovered
records never reach the caller, and the terminal stream marker and the thread
projection that the caller publishes from them never happen. The next start
finds a row that is no longer in flight, so the run is never announced to
anyone at all. A restore is the moment a deployer most needs the service to
come up the first time and to say truthfully what it did with work that was in
flight.

Every test here carries the ``no_auto_user`` marker. ``conftest`` injects a
user into the contextvar for every test by default, which is exactly why the
existing suite could not see this: under that fixture ``AUTO`` always
resolves, and a startup path that depends on a request-scoped user looks
indistinguishable from one that does not. Production startup has no request,
so reproducing it means opting out.
"""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from deerflow.persistence.base import Base
from deerflow.persistence.run.sql import RunRepository
from deerflow.runtime import RunStatus
from deerflow.runtime.runs.manager import (
    ORPHAN_RECOVERY_STOP_REASON,
    STARTUP_ORPHAN_RECOVERY_ERROR,
    RunManager,
)


async def _store(tmp_path, name: str):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / name}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    return RunRepository(async_sessionmaker(engine, expire_on_commit=False)), engine


async def _seed_inflight_run(store, run_id: str = "run-mid-tool-call") -> None:
    """A run left `running` by a backup taken mid-execution, lease long expired."""
    await store.put(
        run_id,
        thread_id="thread-1",
        user_id="owner-1",
        status="running",
        created_at="2020-01-01T00:00:00+00:00",
    )


@pytest.mark.no_auto_user
@pytest.mark.anyio
async def test_recovery_terminalizes_an_inflight_run_with_no_user_in_context(tmp_path) -> None:
    """The first start succeeds, and says what it did with the in-flight run."""
    store, engine = await _store(tmp_path, "restore.db")
    try:
        await _seed_inflight_run(store)
        manager = RunManager(store=store)

        recovered = await manager.reconcile_orphaned_inflight_runs(
            error=STARTUP_ORPHAN_RECOVERY_ERROR,
            stop_reason=ORPHAN_RECOVERY_STOP_REASON,
        )

        # Returned, not merely written: the caller publishes the terminal
        # stream marker and the thread projection from this list, and a
        # start that raises here leaves a terminal row nobody is told about.
        assert [record.run_id for record in recovered] == ["run-mid-tool-call"]
        assert recovered[0].status is RunStatus.error
        assert recovered[0].stop_reason == ORPHAN_RECOVERY_STOP_REASON

        row = await store.get("run-mid-tool-call", user_id=None)
        assert row["status"] == "error"
        assert row["stop_reason"] == ORPHAN_RECOVERY_STOP_REASON
    finally:
        await engine.dispose()


@pytest.mark.no_auto_user
@pytest.mark.anyio
async def test_recovery_never_resumes_the_run_it_found(tmp_path) -> None:
    """Terminalized, never resumed: no graph starts and no tool call is retried.

    The measured run was mid-bash-call when the backup was taken. A recovery
    that re-entered the graph would run that call a second time, against a
    restored disk, with no one watching.
    """
    store, engine = await _store(tmp_path, "no-resume.db")
    try:
        await _seed_inflight_run(store)

        manager = RunManager(store=store)

        recovered = await manager.reconcile_orphaned_inflight_runs(
            error=STARTUP_ORPHAN_RECOVERY_ERROR,
            stop_reason=ORPHAN_RECOVERY_STOP_REASON,
        )

        assert [record.status for record in recovered] == [RunStatus.error]
        # A run only ever executes through worker attachment, which sets
        # ``task``. Recovery attaches nothing, so the graph is never entered
        # and the bash call the backup caught mid-flight is not issued again.
        assert all(record.task is None for record in recovered), "no execution task is attached to a recovered run"

        # And it is not put back in flight for the next pass to pick up.
        assert await manager.reconcile_orphaned_inflight_runs(error=STARTUP_ORPHAN_RECOVERY_ERROR, stop_reason=ORPHAN_RECOVERY_STOP_REASON) == []
        assert (await store.get("run-mid-tool-call", user_id=None))["status"] == "error"
    finally:
        await engine.dispose()


@pytest.mark.no_auto_user
@pytest.mark.anyio
async def test_no_repository_call_on_the_startup_path_asks_who_the_user_is(tmp_path, monkeypatch) -> None:
    """The general rule, not the one call that happened to be found.

    ``AUTO`` means "read the user from the request contextvar". On a path
    that has no request, every use of it is a start that fails, so the way to
    keep this fixed is to assert none of them is reached rather than to list
    the ones known today.
    """
    import importlib

    from deerflow.runtime.user_context import _AutoSentinel

    # Every persistence module that resolves a user, not only the one the
    # reported traceback happened to name.
    watched = [importlib.import_module(f"deerflow.persistence.{module}") for module in ("run.sql", "thread_meta.sql", "thread_meta.memory", "feedback.sql")]

    store, engine = await _store(tmp_path, "auto-audit.db")
    try:
        await _seed_inflight_run(store)

        asked: list[str] = []
        for module in watched:
            real_resolver = module.resolve_user_id

            def recording_resolver(value, *, method_name="repository method", _real=real_resolver):
                if isinstance(value, _AutoSentinel):
                    asked.append(method_name)
                return _real(value, method_name=method_name)

            monkeypatch.setattr(module, "resolve_user_id", recording_resolver)

        manager = RunManager(store=store)
        await manager.reconcile_orphaned_inflight_runs(
            error=STARTUP_ORPHAN_RECOVERY_ERROR,
            stop_reason=ORPHAN_RECOVERY_STOP_REASON,
        )
        # The same work the background timer does, on a tree with nothing
        # left to recover: a path that only asks on the empty pass still
        # fails a Gateway, just later than startup.
        await manager.reconcile_orphaned_inflight_runs(
            error=STARTUP_ORPHAN_RECOVERY_ERROR,
            stop_reason=ORPHAN_RECOVERY_STOP_REASON,
        )

        assert asked == [], f"startup recovery reached user-scoped repository methods with AUTO: {sorted(set(asked))}"
    finally:
        await engine.dispose()


@pytest.mark.no_auto_user
@pytest.mark.anyio
async def test_recovery_says_what_it_did_to_each_run_it_found(tmp_path, caplog) -> None:
    """A count is not an account.

    Recovery logged only "Recovered N orphaned inflight run(s) as error". A
    deployer reading the first log after a restore needs to know *which* runs
    were ended and what they were doing, because those are the pieces of work
    that silently stopped -- one line per run, with the status it came from
    and the status it went to.
    """
    import logging

    store, engine = await _store(tmp_path, "accounted.db")
    try:
        await _seed_inflight_run(store)

        with caplog.at_level(logging.INFO, logger="deerflow.runtime.runs.manager"):
            await RunManager(store=store).reconcile_orphaned_inflight_runs(
                error=STARTUP_ORPHAN_RECOVERY_ERROR,
                stop_reason=ORPHAN_RECOVERY_STOP_REASON,
            )

        accounted = [record for record in caplog.records if record.levelno == logging.INFO and "run-mid-tool-call" in record.getMessage()]
        assert accounted, f"no INFO line accounts for the recovered run; saw {[r.getMessage() for r in caplog.records]}"
        message = accounted[0].getMessage()
        assert "running" in message, f"the status the run came from is missing: {message}"
        assert "error" in message, f"the status the run went to is missing: {message}"
        assert ORPHAN_RECOVERY_STOP_REASON in message, f"the reason is missing: {message}"
    finally:
        await engine.dispose()
