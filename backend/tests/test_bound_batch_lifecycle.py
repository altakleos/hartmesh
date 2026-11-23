"""Real SQL admission, recovery and run-bound execution transactions on both stores."""

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import update
from test_batch_dependencies import create
from test_batch_dependencies import repo as _repo
from test_batch_scope_repository import KNOWN

from deerflow.persistence.agent_instances.model import AgentConversationRow, AgentDefinitionRevisionRow, AgentInstanceRow
from deerflow.persistence.models.run_event import RunEventRow
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.run.sql import RunRepository
from deerflow.persistence.subagent_batches.bound import advance_thread_generation
from deerflow.persistence.subagent_batches.model import SubagentBatchRow
from deerflow.persistence.thread_meta.model import ThreadMetaRow
from deerflow.persistence.user.model import UserRow
from deerflow.runtime.runs.manager import ConflictError

repo = _repo
pytestmark = pytest.mark.asyncio
CTX = dict(thread_id="thread", user_id="owner")
ITEMS = [{"key": "first", "prompt": "A"}, {"key": "second", "prompt": "B", "depends_on": ["first"]}]


async def seed_source(repo):
    async with repo._sf.begin() as s:
        s.add(UserRow(id="owner", email="owner@example.test"))
        s.add(AgentDefinitionRevisionRow(revision="v", owner_id="owner", config={}, soul=""))
        await s.flush()
        s.add(
            AgentInstanceRow(
                id="agent-1",
                principal_id="agent:agent-1",
                name="Employee",
                custody="company",
                owner_id=None,
                creator_id="owner",
                supervisor_id="owner",
                definition_revision="v",
                status="provisioning",
                generation=1,
                creation_id="create",
                creation_request={},
            )
        )
        await s.flush()
        s.add(AgentConversationRow(thread_id="thread", instance_id="agent-1", requester_id="owner", creation_id="thread"))
        s.add(ThreadMetaRow(thread_id="thread", user_id="owner", incarnation="inc"))
        s.add(RunRow(run_id="run", thread_id="thread", user_id="owner", status="running", metadata_json={"agent_instance_id": "agent-1"}))


async def submit(repo, key="batch", items=None):
    g = {"source": "session", "token_version": 0, "permissions": ["runs:create"], "principal": {"system_role": "user", "oauth_provider": None, "oauth_id": None}}
    return await create(repo, ITEMS if items is None else items, batch_id=key, submission_key=key, execution_spec={"scope": KNOWN, "thread_incarnation": "inc"}, continuation={"prompt": "Summarize", "thread_incarnation": "inc", "grant": g})


async def mark(repo, id, status, reason=None, lease=None):
    async with repo._sf.begin() as s:
        await s.execute(update(RunRow).where(RunRow.run_id == id).values(status=status, stop_reason=reason, lease_expires_at=lease))


async def record_reply(repo, *, run="first-run", caller="lead_agent", **changes):
    body = {"id": "answer", "type": "ai", "content": "Checks are complete.", **changes}
    async with repo._sf.begin() as session:
        session.add(
            RunEventRow(thread_id="thread", user_id="owner", run_id=run, event_type="llm.ai.response", category="message", event_metadata={"caller": caller, "content_is_dict": True, "content_is_json": True}, content=json.dumps(body), seq=1)
        )


async def claim(repo, now=None, owner="worker"):
    return await repo.claim_bound_dispatch("batch", lease_owner=owner, lease_seconds=120, now=now or datetime.now(UTC))


async def admit(repo, dispatch, name):
    from deerflow.persistence.subagent_batches.bound import BoundBatchExecutionFence

    f = BoundBatchExecutionFence(**dispatch["execution_fence"], scope=KNOWN, thread_incarnation="inc", thread_generation=dispatch["thread_generation"])
    return await RunRepository(repo._sf, on_thread_admission=advance_thread_generation).create_thread_operation_atomic(
        name,
        **CTX,
        owner_worker_id="host",
        lease_expires_at=(datetime.now(UTC) + timedelta(seconds=30)).isoformat(),
        execution_fence=f,
        metadata={"deerflow_origin": {"kind": "subagent_batch"}, "agent_instance_id": "agent-1"},
        idempotency_key=dispatch["idempotency_key"],
    )


async def start(repo):
    await seed_source(repo)
    await submit(repo)
    assert not await claim(repo)
    await mark(repo, "run", "success")
    dispatch = await claim(repo)
    await admit(repo, dispatch, "first-run")
    return dispatch


async def items(repo, run_id):
    return await repo.claim_bound_items("batch", **CTX, run_id=run_id, lease_owner="execution", limit=3, now=datetime.now(UTC))


async def finish(repo, id, run_id):
    return await repo.finish_bound_item(
        "batch",
        id,
        **CTX,
        run_id=run_id,
        lease_owner="execution",
        succeeded=True,
        result="checked",
        result_preview="checked",
        result_truncated=False,
        error=None,
        stop_reason=None,
        token_usage=None,
        model_name="offline",
        completed_at=datetime.now(UTC),
    )


async def test_concurrent_submission_is_per_conversation_and_replay_stable(repo):
    await seed_source(repo)
    a, b = await asyncio.gather(submit(repo), submit(repo))
    assert a["id"] == b["id"]
    with pytest.raises(ValueError, match="open"):
        await submit(repo, "second")
    assert await repo.list_bound_jobs(after=None, limit=4) == ["batch"]


async def test_terminal_source_without_success_cancels_checks_together(repo):
    await seed_source(repo)
    await submit(repo)
    await mark(repo, "run", "error")
    assert not await claim(repo)
    row = await repo.get_batch("batch", user_id="owner")
    assert row["continuation_status"] == row["status"] == "cancelled" and row["counts"]["cancelled"] == 2


async def test_replacement_rejects_old_run_keeps_results_and_never_repeats_final(repo):
    await start(repo)
    (one,) = await items(repo, "first-run")
    assert await finish(repo, one["id"], "first-run")
    (two,) = await items(repo, "first-run")
    await mark(repo, "first-run", "interrupted", "gateway_stopped", datetime.now(UTC))
    replacement = await claim(repo)
    assert replacement["idempotency_key"].endswith(":replacement")
    assert not await finish(repo, two["id"], "first-run")
    await admit(repo, replacement, "second-run")
    (two_again,) = await items(repo, "second-run")
    assert two_again["id"] == two["id"] and await finish(repo, two["id"], "second-run")
    assert await repo.begin_bound_final("batch", **CTX, run_id="second-run", now=datetime.now(UTC))
    assert not await repo.begin_bound_final("batch", **CTX, run_id="second-run", now=datetime.now(UTC))
    await mark(repo, "second-run", "error", lease=datetime.now(UTC))
    assert not await claim(repo)
    row = await repo.get_batch("batch", user_id="owner")
    assert row["status"] == "completed" and row["continuation_status"] == "cancelled" and row["counts"]["succeeded"] == 2
    async with repo._sf() as s:
        assert (await s.get(SubagentBatchRow, "batch")).continuation_json["outcome"]["reason"] == "interrupted"


async def test_cancel_before_commit_invalidates_held_dispatch(repo):
    from deerflow.runtime.runs.manager import ConflictError

    await seed_source(repo)
    await submit(repo)
    await mark(repo, "run", "success")
    held = await claim(repo)
    assert not await repo.cancel_bound_batch("batch", user_id="other")
    await repo.cancel_bound_batch("batch", user_id="owner")
    with pytest.raises(ConflictError):
        await admit(repo, held, "late")
    async with repo._sf() as s:
        assert not await s.get(RunRow, "late")


async def test_admission_rechecks_session_and_source_incarnation(repo):
    await seed_source(repo)
    await submit(repo)
    await mark(repo, "run", "success")
    dispatch = await claim(repo)
    async with repo._sf.begin() as session:
        await session.execute(update(UserRow).where(UserRow.id == "owner").values(token_version=1))
    with pytest.raises(ConflictError):
        await admit(repo, dispatch, "changed-account")
    async with repo._sf.begin() as session:
        await session.execute(update(UserRow).where(UserRow.id == "owner").values(token_version=0))
        batch = await session.get(SubagentBatchRow, "batch")
        batch.execution_spec = {**batch.execution_spec, "thread_incarnation": "earlier"}
    with pytest.raises(ConflictError):
        await admit(repo, dispatch, "changed-thread")
    async with repo._sf() as session:
        assert (await session.get(SubagentBatchRow, "batch")).continuation_run_id is None


async def test_ordinary_submission_key_cannot_recover_bound_job(repo):
    await seed_source(repo)
    await submit(repo)
    with pytest.raises(ValueError, match="another source"):
        await create(repo, ITEMS, batch_id="unrelated", submission_key="batch", thread_id="other", execution_spec={})


@pytest.mark.parametrize("minutes", [59, 61])
async def test_replacement_uses_actual_lease_and_rechecks_expiry_while_waiting(repo, minutes):
    await start(repo)
    now = datetime.now(UTC)
    await mark(repo, "first-run", "interrupted", "orphan_recovered", now - timedelta(minutes=minutes))
    replacement = await claim(repo, now)
    if minutes == 59:
        assert replacement["idempotency_key"].endswith(":replacement")
        assert await repo.finish_bound_dispatch("batch", execution_fence=replacement["execution_fence"], status="waiting", reason="busy")
        assert not await claim(repo, now + timedelta(minutes=2))
    else:
        assert not replacement
    async with repo._sf() as session:
        batch = await session.get(SubagentBatchRow, "batch")
        assert batch.continuation_json["outcome"]["reason"] == "interrupted"
        assert batch.continuation_status == "cancelled"


async def test_second_lost_run_cannot_replace_and_no_final_before_checks_end(repo):
    await start(repo)
    assert not await repo.begin_bound_final("batch", **CTX, run_id="first-run", now=datetime.now(UTC))
    await mark(repo, "first-run", "error", lease=None)
    replacement = await claim(repo)
    await admit(repo, replacement, "second-run")
    await mark(repo, "second-run", "error", lease=datetime.now(UTC))
    assert not await claim(repo)
    assert (await repo.get_batch("batch", user_id="owner"))["counts"]["cancelled"] == 2


async def test_concurrent_distinct_proposals_allow_one_and_stale_dispatch_cannot_report(repo):
    await seed_source(repo)
    results = await asyncio.gather(submit(repo, "a"), submit(repo, "b"), return_exceptions=True)
    [winner] = [row["id"] for row in results if isinstance(row, dict)]
    assert sum(isinstance(result, ValueError) for result in results) == 1
    await mark(repo, "run", "success")
    now = datetime.now(UTC)
    expired = await repo.claim_bound_dispatch(winner, lease_owner="old", lease_seconds=1, now=now - timedelta(seconds=10))
    active = await repo.claim_bound_dispatch(winner, lease_owner="new", lease_seconds=120, now=now)
    assert active["execution_fence"]["epoch"] > expired["execution_fence"]["epoch"]
    assert not await repo.finish_bound_dispatch(winner, execution_fence=expired["execution_fence"], status="ended")
    assert await repo.finish_bound_dispatch(winner, execution_fence=active["execution_fence"], status="waiting", reason="busy")


async def test_work_deadline_and_requester_stop_are_terminal(repo):
    await start(repo)
    state = await repo.get_bound_run("batch", **CTX, run_id="first-run", work_seconds=2)
    assert state["unfinished"] == 2
    (check,) = await items(repo, "first-run")
    assert check["batch"]["subagent_type"] == state["subagent_type"] == "general-purpose"
    assert not await repo.timeout_bound_items("batch", **CTX, run_id="first-run", now=state["items_deadline"] - timedelta(seconds=1))
    assert await repo.timeout_bound_items("batch", **CTX, run_id="first-run", now=state["items_deadline"])
    assert not await finish(repo, check["id"], "first-run")
    assert (await repo.get_bound_run("batch", **CTX, run_id="first-run"))["counts"]["failed"] == 2
    async with repo._sf.begin() as session:
        await session.execute(update(RunRow).where(RunRow.run_id == "first-run").values(status="interrupted", cancel_requested_at=datetime.now(UTC)))
    assert not await claim(repo)
    assert (await repo.get_batch("batch", user_id="owner"))["continuation_status"] == "cancelled"


@pytest.mark.parametrize("final_state", ["lost", "done", "live"])
async def test_disabled_startup_closes_only_lost_or_completed_final(repo, final_state):
    from app.subagent_batches.recovery import recover_unavailable_batches

    await start(repo)
    for _ in ITEMS:
        (check,) = await items(repo, "first-run")
        assert await finish(repo, check["id"], "first-run")
    assert await repo.begin_bound_final("batch", **CTX, run_id="first-run", now=datetime.now(UTC))
    if final_state == "lost":
        await mark(repo, "first-run", "interrupted", "orphan_recovered", datetime.now(UTC) - timedelta(hours=2))
    elif final_state == "done":
        await record_reply(repo)
        await mark(repo, "first-run", "success")
    await recover_unavailable_batches(repo)
    records = await repo.list_items("batch", user_id="owner", include_result=True)
    assert [record["result"] for record in records] == ["checked", "checked"]
    async with repo._sf() as session:
        batch = await session.get(SubagentBatchRow, "batch")
        assert batch.status == "completed"
        if final_state == "live":
            assert batch.continuation_status == "dispatched" and "outcome" not in batch.continuation_json
        else:
            assert batch.continuation_json["outcome"]["reason"] == ("interrupted" if final_state == "lost" else "completed")
            assert batch.continuation_status == ("cancelled" if final_state == "lost" else "dispatched")


async def test_only_native_fence_can_admit_and_all_finished_without_final_is_not_replaced(repo):
    from deerflow_extension_api.agent_runs import ExecutionFence

    await seed_source(repo)
    await submit(repo)
    await mark(repo, "run", "success")
    dispatch = await claim(repo)
    with pytest.raises(ConflictError):
        await RunRepository(repo._sf, on_thread_admission=advance_thread_generation).create_thread_operation_atomic(
            "ordinary-request",
            **CTX,
            owner_worker_id="host",
            lease_expires_at=None,
            execution_fence=ExecutionFence(**dispatch["execution_fence"]),
            idempotency_key=dispatch["idempotency_key"],
            metadata={"deerflow_origin": {"kind": "subagent_batch"}, "agent_instance_id": "agent-1"},
        )
    await admit(repo, dispatch, "first-run")
    for _ in ITEMS:
        (check,) = await items(repo, "first-run")
        assert await finish(repo, check["id"], "first-run")
    await mark(repo, "first-run", "error", lease=datetime.now(UTC))
    assert not await claim(repo)
    assert not await repo.get_bound_run("batch", **CTX, run_id="first-run")
    async with repo._sf() as session:
        batch = await session.get(SubagentBatchRow, "batch")
        assert batch.continuation_json["outcome"] == {"version": 1, "reason": "interrupted", "finished": 2, "total": 2}
        assert not await session.get(RunRow, "ordinary-request")


@pytest.mark.parametrize(
    ("witness", "run_status", "reason"),
    [
        ("empty", "success", "interrupted"),
        ("tool", "success", "interrupted"),
        ("hidden", "success", "interrupted"),
        ("other-run", "success", "interrupted"),
        ("helper", "success", "interrupted"),
        ("visible", "interrupted", "completed"),
    ],
)
async def test_reconciliation_requires_the_actual_final_reply(repo, witness, run_status, reason):
    await start(repo)
    for _ in ITEMS:
        (check,) = await items(repo, "first-run")
        assert await finish(repo, check["id"], "first-run")
    assert await repo.begin_bound_final("batch", **CTX, run_id="first-run", now=datetime.now(UTC))
    if witness == "tool":
        await record_reply(repo, tool_calls=[{"id": "call", "name": "lookup", "args": {}}])
    elif witness == "hidden":
        await record_reply(repo, additional_kwargs={"hide_from_ui": True})
    elif witness == "other-run":
        await record_reply(repo, run="other")
    elif witness == "helper":
        await record_reply(repo, caller="subagent:general")
    elif witness == "visible":
        await record_reply(repo)
    await mark(repo, "first-run", run_status, "gateway_stopped" if run_status == "interrupted" else None)
    assert not await claim(repo)
    async with repo._sf() as session:
        batch = await session.get(SubagentBatchRow, "batch")
        assert batch.continuation_json["outcome"]["reason"] == reason and batch.status == "completed"


async def test_generation_refuses_racing_other_owner_even_after_completed_run_is_deleted(repo):
    await seed_source(repo)
    await submit(repo)
    await mark(repo, "run", "success")
    held = await claim(repo)
    runs = RunRepository(repo._sf, on_thread_admission=advance_thread_generation)
    peer = dict(thread_id="thread", user_id="manager", owner_worker_id="other-host", lease_expires_at=(datetime.now(UTC) + timedelta(minutes=3)).isoformat())
    await runs.create_thread_operation_atomic("competing", **peer)
    assert not await claim(repo)
    assert await runs.update_status("competing", "success", owner_worker_id="other-host")
    await runs.delete("competing", user_id="manager")
    with pytest.raises(ConflictError):
        await admit(repo, held, "stale")
    assert not await repo.finish_bound_dispatch("batch", execution_fence=held["execution_fence"], status="ended", reason="late decision")
    fresh = await claim(repo)
    with pytest.raises(ConflictError):
        await admit(repo, {**fresh, "thread_generation": ""}, "missing-generation")
    assert held["thread_generation"] != fresh["thread_generation"]
    await runs.create_thread_operation_atomic("unrelated", **{**peer, "thread_id": "another-thread"})
    await admit(repo, fresh, "first-run")


async def test_live_generation_and_replayed_foreign_admissions(repo):
    from deerflow.persistence.run.sql import RunIdempotencyConflict

    await seed_source(repo)
    await submit(repo)
    await mark(repo, "run", "success")
    runs = RunRepository(repo._sf, on_thread_admission=advance_thread_generation)
    peer = dict(thread_id="thread", user_id="manager", owner_worker_id="other-host", lease_expires_at=(datetime.now(UTC) + timedelta(minutes=3)).isoformat(), idempotency_key="request")
    await runs.create_thread_operation_atomic("competing", **peer)
    assert not await claim(repo)
    assert await runs.update_status("competing", "success", owner_worker_id="other-host")
    held = await claim(repo)
    with pytest.raises(RunIdempotencyConflict):
        await runs.create_thread_operation_atomic("replay", **peer)
    await admit(repo, held, "first-run")
