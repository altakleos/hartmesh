"""The ordinary worker leaves scoped and unrecognised batch specifications alone."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, update
from test_batch_dependencies import create, finish
from test_batch_dependencies import repo as _repo_fixture

from deerflow.persistence.extension_leases.model import ExtensionExecutionLeaseRow
from deerflow.persistence.subagent_batches.model import SubagentBatchItemRow, SubagentBatchRow
from deerflow.subagents.batch_runtime import BATCH_CONTINUATION_NAMESPACE

repo = _repo_fixture
pytestmark = pytest.mark.asyncio
NOW = datetime.now(UTC)
KNOWN = {"version": 1, "kind": "agent_conversation", "instance_id": "agent-1", "principal": {"source": "session", "token_version": 0, "system_role": "user", "oauth_provider": None, "oauth_id": None}}


async def seed(repo, key, spec):
    row = await create(repo, [{"key": "a", "prompt": "A"}, {"key": "b", "prompt": "B"}], batch_id=key, submission_key=key, execution_spec={}, continuation={"prompt": "Summarize", "future": [1, 2]})
    await change(repo, key, execution_spec=spec)
    return row


async def change(repo, key, **values):
    async with repo._sf.begin() as session:
        await session.execute(update(SubagentBatchRow).where(SubagentBatchRow.id == key).values(**values))


async def snapshot(repo, key):
    async with repo._sf() as session:
        b = (await session.execute(select(SubagentBatchRow.__table__).where(SubagentBatchRow.id == key))).mappings().one()
        items = (await session.execute(select(SubagentBatchItemRow.__table__).where(SubagentBatchItemRow.batch_id == key).order_by(SubagentBatchItemRow.position))).mappings().all()
        lease = await session.get(ExtensionExecutionLeaseRow, (BATCH_CONTINUATION_NAMESPACE, key))
        return b, items, (lease.holder_id, lease.epoch, lease.expires_at) if lease else None


async def test_scopes_yield_item_and_followup_slots_without_mutating_unknown_work(repo):
    specs = [{"scope": s} for s in (None, False, 1, "", [], {}, KNOWN, KNOWN | {"version": 2}, KNOWN | {"version": True}, KNOWN | {"kind": "future"})] + [None, []]
    for i, spec in enumerate(specs):
        await seed(repo, f"scoped-{i}", spec)
    await seed(repo, "ordinary", {})
    rows = await repo.claim_items(now=NOW, lease_owner="worker", lease_seconds=4, limit=1)
    assert [r["batch"]["id"] for r in rows] == ["ordinary"]
    for i in range(len(specs)):
        await change(repo, f"scoped-{i}", status="completed", updated_at=NOW - timedelta(days=1))
    await change(repo, "ordinary", status="completed")
    # More older unsupported follow-ups than fit in a single scan page.
    claims = await repo.claim_continuations(now=NOW, lease_owner="worker", lease_seconds=10, limit=2)
    assert [r["id"] for r in claims] == ["ordinary"]
    assert [r["id"] for r in await repo.list_continuation_batches_for_run("run")] == ["ordinary"]
    before = await snapshot(repo, "scoped-0")
    await repo.suppress_thread_continuations("thread")
    assert await snapshot(repo, "scoped-0") == before
    await repo.suppress_thread_continuations("thread", teardown=True)
    assert (await repo.get_batch("scoped-0", user_id="owner"))["continuation_status"] == "suppressed"
    assert (await repo.get_batch("scoped-0", user_id="owner"))["read_only"] is True
    assert "read_only" not in await repo.get_batch("ordinary", user_id="owner")


async def test_held_worker_and_controls_cannot_change_a_scoped_batch(repo):
    await seed(repo, "changed", {})
    item, other = await repo.claim_items(now=NOW, lease_owner="worker", lease_seconds=1, limit=2)
    assert await finish(repo, other, succeeded=False)
    await change(repo, "changed", execution_spec={"scope": None}, status="paused")
    before = await snapshot(repo, "changed")
    assert not await repo.mark_item_running(item["id"], lease_owner="worker", now=NOW)
    assert not (await repo.renew_item_lease(item["id"], lease_owner="worker", lease_seconds=4, now=NOW))["valid"]
    assert not await finish(repo, item)
    assert not await repo.requeue_item_after_admission_failure(item["id"], lease_owner="worker", error=None, now=NOW)
    assert not await repo.claim_items(now=NOW + timedelta(hours=1), lease_owner="later", lease_seconds=20, limit=1)
    assert not await repo.resume_batch("changed", user_id="owner")
    assert not await repo.retry_item("changed", other["id"], user_id="owner")
    assert not await repo.cancel_batch("changed", user_id="owner")
    assert await snapshot(repo, "changed") == before
    assert await repo.cancel_batch("changed", user_id="owner", teardown=True)
    assert (await repo.get_batch("changed", user_id="owner"))["status"] == "cancelled"


async def test_unavailable_scope_settlement_is_atomic_idempotent_and_rechecks_each_candidate(repo):
    await seed(repo, "a", {})
    item, pending = await repo.claim_items(now=NOW, lease_owner="worker", lease_seconds=4, limit=2)
    assert await finish(repo, item)
    await change(repo, "a", execution_spec={"scope": KNOWN})
    for key in ("b", "c", "d", "e"):
        await seed(repo, key, {})
        await change(repo, key, status="completed")
    # Acquire a real dispatch lease before changing the future row shape.
    await repo.claim_continuations(now=NOW, lease_owner="prior-worker", lease_seconds=60, limit=4)
    for key in ("b", "c", "d", "e"):
        await change(repo, key, execution_spec={"scope": KNOWN})
    assert await repo.list_unavailable_bound_batches(after=None, limit=2) == ["a", "b"]
    assert await repo.list_unavailable_bound_batches(after="b", limit=2) == ["c", "d"]
    await change(repo, "c", execution_spec={"scope": KNOWN | {"version": 2}})
    await change(repo, "d", continuation_status="dispatched")
    await change(repo, "e", execution_spec={"scope": KNOWN, "user_role": "admin"})
    for key in ("c", "d", "e"):
        before = await snapshot(repo, key)
        assert not await repo.cancel_unavailable_bound_batch(key)
        assert await snapshot(repo, key) == before
    await change(repo, "c", execution_spec={"scope": KNOWN}, continuation_json=["future"])
    malformed = await snapshot(repo, "c")
    with pytest.raises(ValueError, match="continuation document"):
        await repo.cancel_unavailable_bound_batch("c")
    assert await snapshot(repo, "c") == malformed
    before_a = await snapshot(repo, "a")
    assert sorted(await asyncio.gather(repo.cancel_unavailable_bound_batch("a"), repo.cancel_unavailable_bound_batch("a"))) == [False, True]
    after_a = await snapshot(repo, "a")
    assert after_a[0]["status"] == after_a[0]["continuation_status"] == "cancelled"
    assert after_a[0]["continuation_json"] == before_a[0]["continuation_json"] | {"outcome": {"version": 1, "reason": "unavailable"}}
    assert after_a[1][0] == before_a[1][0]
    assert after_a[1][1]["id"] == pending["id"] and after_a[1][1]["lease_owner"] is None
    assert after_a[1][1]["error"] == "Cancelled: this job cannot continue here. Nothing more will run for it."
    assert not await repo.cancel_unavailable_bound_batch("a") and await snapshot(repo, "a") == after_a
    b = await snapshot(repo, "b")
    assert not await repo.finish_continuation("b", lease_owner="prior-worker", status="dispatched", run_id="late")
    assert await repo.cancel_unavailable_bound_batch("b")
    settled = await snapshot(repo, "b")
    assert settled[0]["status"] == settled[0]["continuation_status"] == "cancelled"
    assert settled[2] == (b[2][0], b[2][1] + 1, 0)
    assert not await repo.finish_continuation("b", lease_owner="prior-worker", status="dispatched", run_id="late")


async def test_locked_startup_reconciliation_yields_and_connection_policy_is_restored(repo):
    from contextlib import AsyncExitStack

    from sqlalchemy import text
    from sqlalchemy.exc import DBAPIError

    await seed(repo, "locked", {"scope": KNOWN})
    async with repo._sf() as holder:
        dialect = holder.get_bind().dialect.name
        policy_sql = text("PRAGMA busy_timeout" if dialect == "sqlite" else "SHOW lock_timeout")
        original_policy = await holder.scalar(policy_sql)
        await holder.execute(update(SubagentBatchRow).where(SubagentBatchRow.id == "locked").values(title="Uncommitted"))
        with pytest.raises(DBAPIError):
            await asyncio.wait_for(repo.cancel_unavailable_bound_batch("locked"), timeout=2)
        await holder.rollback()
    assert await repo.cancel_unavailable_bound_batch("locked")
    # Simultaneous checkouts include the pooled connection used by the startup writer.
    async with AsyncExitStack() as stack:
        for _ in range(3):
            session = await stack.enter_async_context(repo._sf())
            assert await session.scalar(policy_sql) == original_policy


async def test_enabled_recovery_cancels_only_jobs_without_a_carry_record(repo):
    from app.subagent_batches.recovery import recover_unavailable_batches

    for key, carry in (("legacy", None), ("supported", {"version": 1, "replacements": 0}), ("future", {"version": 4})):
        await seed(repo, key, {})
        doc = {"carry": carry} if carry else {}
        await change(repo, key, execution_spec={"scope": KNOWN}, continuation_json=doc)
    await recover_unavailable_batches(repo, agent_enabled=True)
    assert (await repo.get_batch("legacy", user_id="owner"))["status"] == "cancelled"
    for key in ("supported", "future"):
        assert not await repo.cancel_unavailable_bound_batch(key, legacy_only=True)
        assert (await repo.get_batch(key, user_id="owner"))["status"] == "queued"
