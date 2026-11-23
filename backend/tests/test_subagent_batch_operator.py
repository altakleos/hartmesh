"""CLI reads actual persisted work and distinguishes cancellation from worker acknowledgement."""

from datetime import UTC, datetime

import pytest
from test_batch_scope_repository import KNOWN, change, seed
from test_batch_scope_repository import repo as repo_fixture

from app.subagent_batches.operator import BatchOperator
from deerflow.persistence.run.model import RunRow
from deerflow.subagents.batch_runtime import batch_continuation_key

repo = repo_fixture
pytestmark = pytest.mark.asyncio


async def test_operator_pages_skip_unknown_and_do_not_export_results_or_account_secrets(repo):
    for key, spec in (("a", {}), ("b", {"scope": KNOWN | {"version": 99}}), ("c", {"scope": KNOWN})):
        await seed(repo, key, spec)
    await change(repo, "c", continuation_json={"carry": {"version": 1, "replacements": 0}})
    await seed(repo, "done", {})
    await change(repo, "done", status="completed", continuation_status="dispatched")
    operator = BatchOperator(repo._sf)
    first = await operator.list(after=None, limit=1)
    assert first["jobs"] == [{"batch_id": "a", "thread_id": "thread", "kind": "personal", "status": "queued", "continuation_status": "pending"}]
    second = await operator.list(after=first["next_after"], limit=5)
    assert [r["batch_id"] for r in second["jobs"]] == ["c"]
    assert second["next_after"] is None
    with pytest.raises(ValueError, match="unavailable"):
        await operator.cancel("b", wait_seconds=0)
    assert (await repo.get_batch("b", user_id="owner"))["status"] == "queued"


async def test_operator_cancels_personal_and_reports_a_running_final_as_unconfirmed_until_it_stops(repo):
    await seed(repo, "one", {})
    run_id = "final"
    await change(repo, "one", status="completed", continuation_status="dispatched", continuation_run_id=run_id)
    async with repo._sf.begin() as session:
        session.add(RunRow(run_id=run_id, user_id="owner", thread_id="thread", origin_kind="subagent_batch", idempotency_key=batch_continuation_key("one"), status="running", created_at=datetime.now(UTC)))
    operator = BatchOperator(repo._sf)
    assert [j["batch_id"] for j in (await operator.list(after=None, limit=2))["jobs"]] == ["one"]
    result = await operator.cancel("one", wait_seconds=0)
    assert result["run_stop"] == "unconfirmed"
    async with repo._sf.begin() as session:
        run = await session.get(RunRow, run_id)
        assert run.cancel_action == "interrupt"
        run.status = "interrupted"
    assert (await operator.cancel("one", wait_seconds=0))["run_stop"] == "confirmed"


async def test_operator_can_stop_a_genuine_bound_final_with_retained_results(repo):
    from sqlalchemy import update
    from test_bound_batch_lifecycle import start

    from deerflow.persistence.subagent_batches.model import SubagentBatchItemRow

    await start(repo)
    async with repo._sf.begin() as session:
        await session.execute(update(SubagentBatchItemRow).where(SubagentBatchItemRow.batch_id == "batch", SubagentBatchItemRow.position == 0).values(status="succeeded", result="retained", completed_at=datetime.now(UTC)))
    operator = BatchOperator(repo._sf)
    assert [(row["batch_id"], row["kind"]) for row in (await operator.list(after=None, limit=20))["jobs"]] == [("batch", "agent")]
    result = await operator.cancel("batch", wait_seconds=0)
    assert result["cancelled"] and result["run_stop"] == "unconfirmed"
    items = await repo.list_items("batch", user_id="owner", include_result=True)
    assert [(row["status"], row["result"]) for row in items] == [("succeeded", "retained"), ("cancelled", None)]
    current = await repo.get_batch("batch", user_id="owner")
    assert current["follow_up"] == {"state": "ended", "finished": 1, "total": 2, "reason": "cancelled"}
    assert current["controls"] == []
