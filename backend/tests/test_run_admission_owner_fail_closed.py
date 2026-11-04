"""A run admission without an owner fails closed instead of storing an ownerless row.

Upstream #7619 reads an explicit ``user_id=None`` at
``create_thread_operation_atomic`` as an ownerless row. For a run, HartMesh
keeps its earlier rule: ``None`` is the request user, and with no user in
context the admission raises. The run manager passes ``None`` whenever no owner
was given and no user is in context, so without this rule such an admission
would land a run row that no owner-scoped read can see. A non-run reservation
keeps upstream's reading: seeding a legacy ownerless thread's history needs an
ownerless reservation, which executes nothing.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from deerflow.persistence.run import RunRepository
from deerflow.runtime.user_context import reset_current_user, set_current_user


async def _repo(tmp_path):
    from deerflow.persistence.engine import get_session_factory, init_engine

    await init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path / 'owner.db'}", sqlite_dir=str(tmp_path))
    return RunRepository(get_session_factory())


def _admit(repo, run_id, *, operation_kind="run", **owner):
    lease = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()
    return repo.create_thread_operation_atomic(
        run_id, thread_id=f"thread-{run_id}", owner_worker_id="worker-1", lease_expires_at=lease, operation_kind=operation_kind, multitask_strategy="reject", created_at=datetime.now(UTC).isoformat(), **owner
    )


@pytest.mark.anyio
@pytest.mark.no_auto_user
@pytest.mark.parametrize("owner", [{}, {"user_id": None}], ids=["omitted", "explicit-none"])
async def test_an_admission_with_no_owner_and_no_user_raises(tmp_path, owner):
    from deerflow.persistence.engine import close_engine

    repo = await _repo(tmp_path)
    try:
        with pytest.raises(RuntimeError, match="no user context"):
            await _admit(repo, "ownerless", **owner)
        assert await repo.get("ownerless", user_id=None) is None
    finally:
        await close_engine()


@pytest.mark.anyio
@pytest.mark.no_auto_user
async def test_a_none_owner_is_the_request_user(tmp_path):
    from deerflow.persistence.engine import close_engine

    repo = await _repo(tmp_path)
    token = set_current_user(SimpleNamespace(id="request-user"))
    try:
        await _admit(repo, "owned", user_id=None)
        row = await repo.get("owned", user_id=None)
        assert row is not None and row["user_id"] == "request-user"
    finally:
        reset_current_user(token)
        await close_engine()


@pytest.mark.anyio
@pytest.mark.no_auto_user
async def test_a_non_run_reservation_may_be_ownerless(tmp_path):
    from deerflow.persistence.engine import close_engine

    repo = await _repo(tmp_path)
    try:
        await _admit(repo, "seed", operation_kind="checkpoint_write", user_id=None)
        row = await repo.get("seed", user_id=None)
        assert row is not None and row["user_id"] is None and row["operation_kind"] == "checkpoint_write"
    finally:
        await close_engine()
