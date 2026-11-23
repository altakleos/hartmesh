"""Run-bound items finish before the first employee model call."""

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from app.subagent_batches.execution import BoundBatchItems


class Repository:
    def __init__(self, items=4):
        self.pending = list(range(items))
        self.results = []
        self.final = 0
        self.deadline = datetime.now(UTC) + timedelta(minutes=15)
        self.current = True
        self.timed_out = 0

    async def get_bound_run(self, *args, **kwargs):
        if self.current:
            return {"unfinished": len(self.pending), "items_deadline": self.deadline, "max_running_items": 2}

    async def claim_bound_items(self, *args, limit, **kwargs):
        ids, self.pending = self.pending[:limit], self.pending[limit:]
        return [{"id": str(i), "item_key": str(i), "prompt": "Read the sample", "batch": {"subagent_type": "general-purpose"}} for i in ids]

    async def finish_bound_item(self, batch_id, item_id, *, lease_owner, **kwargs):
        if not self.current:
            return False
        self.results.append((item_id, lease_owner, kwargs))
        return True

    async def timeout_bound_items(self, *args, **kwargs):
        self.pending.clear()
        self.timed_out += 1
        return self.current

    async def begin_bound_final(self, *args, **kwargs):
        if self.final or not self.current:
            return False
        self.final += 1
        return True


def bound(repo):
    return BoundBatchItems(repo, batch_id="batch", run_id="run", thread_id="thread", user_id="user", instance_id="employee")


@pytest.mark.anyio
async def test_first_model_waits_for_durable_results_and_one_final_transition():
    repo = Repository()
    job = bound(repo)
    running = peak = 0

    async def check(item):
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0)
        running -= 1
        return {"succeeded": True, "result": item["prompt"]}

    await job.run_items(check, stopped=lambda: False)
    await job.run_items(check, stopped=lambda: False)
    assert peak == 2 and repo.final == 1
    assert [r[0] for r in repo.results] == ["0", "1", "2", "3"]
    with pytest.raises(RuntimeError):
        await bound(repo).run_items(check, stopped=lambda: False)
    assert repo.final == 1


@pytest.mark.anyio
async def test_withdrawal_during_item_withholds_final_turn():
    repo = Repository(1)

    async def withdraw(_item):
        repo.current = False
        return {"succeeded": True}

    with pytest.raises(RuntimeError):
        await bound(repo).run_items(withdraw, stopped=lambda: False)
    assert not repo.results and not repo.final


@pytest.mark.anyio
async def test_timed_out_work_is_cancelled_and_stored_before_final():
    repo = Repository(2)
    repo.deadline = datetime.now(UTC) + timedelta(seconds=0.05)
    entered = asyncio.Event()
    left = asyncio.Event()

    async def wait(_item):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            left.set()

    await bound(repo).run_items(wait, stopped=lambda: False)
    assert entered.is_set() and left.is_set()
    assert repo.timed_out == 1 and repo.final == 1 and not repo.results


def test_host_job_binding_requires_both_halves_of_the_authority():
    from dataclasses import replace

    from test_agent_execution_runtime import execution

    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.agent_instances.conversations import ConnectionOrigin
    from deerflow.agent_instances.runtime import validate_bound_job

    employee = replace(execution(), connection_origin=ConnectionOrigin("batch_followup"))
    repo = Repository()
    job = BoundBatchItems(repo, "batch", "run", employee.thread_id, employee.requester.subject_id, employee.instance.id)
    origin = {"deerflow_origin": {"kind": "subagent_batch"}}
    rec = SimpleNamespace(kwargs={"agent_job_id": "batch"}, metadata=origin, run_id=job.run_id, thread_id=job.thread_id, user_id=job.user_id)
    validate_bound_job(rec, employee, job)
    for adapter in (None, replace(job, batch_id="other"), replace(job, user_id="another"), replace(job, thread_id="old"), replace(job, instance_id="different"), replace(job, run_id="retired")):
        with pytest.raises(AgentDenied):
            validate_bound_job(rec, employee, adapter)
    with pytest.raises(AgentDenied):
        validate_bound_job(rec, None, None)
    rec.kwargs.clear()
    with pytest.raises(AgentDenied):
        validate_bound_job(rec, employee, job)
    validate_bound_job(rec, None, None)  # ordinary continuation
    with pytest.raises(AgentDenied):
        validate_bound_job(rec, replace(employee, connection_origin=None), job)
    rec.metadata = {}
    validate_bound_job(rec, employee, None)  # normal employee chat
    with pytest.raises(AgentDenied):
        validate_bound_job(rec, employee, job)


def test_bound_job_uses_the_production_helper_graph():
    import os
    import subprocess
    import sys
    from pathlib import Path

    backend = Path(__file__).resolve().parents[1]
    sources = [backend, backend / "tests", backend / "packages/harness", backend / "packages/extension-api"]
    subprocess.run(
        [sys.executable, "-m", "pytest", "--noconftest", "-q", "tests/support/bound_batch_helper_graph.py"],
        cwd=backend,
        env={**os.environ, "PYTHONPATH": os.pathsep.join(map(str, sources))},
        timeout=120,
        check=True,
    )


@pytest.mark.anyio
async def test_cutoff_is_durable_while_a_helper_drains():
    repo = Repository(3)
    repo.deadline = datetime.now(UTC) + timedelta(seconds=0.08)
    job = bound(repo)
    draining = asyncio.Event()
    release = asyncio.Event()

    async def late(_item):
        try:
            await asyncio.Event().wait()
        finally:
            draining.set()
            await release.wait()

    task = asyncio.create_task(job.run_items(late, stopped=lambda: False))
    try:
        await asyncio.wait_for(draining.wait(), 3)
        assert repo.timed_out == 1 and not job.items_open() and not repo.final
    finally:
        release.set()
        await task
    assert repo.final == 1 and not repo.results
