"""A generic delegate admission shares the domain's original transaction."""

from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_work_actions import actions as actions

from deerflow.agent_instances.contract import AgentDenied
from deerflow.agent_instances.work_contract import DelegateWork
from deerflow.persistence.agent_instances.work import AgentWorkEventRow, AgentWorkRow


@pytest.mark.asyncio
async def test_delegate_admission_encloses_mutation_and_post_projection(actions):
    e = actions
    seen = []

    async def admit(writer):
        assert writer.in_transaction() and writer.is_active
        seen.append(await writer.scalar(select(func.count()).select_from(AgentWorkRow)))

    body = DelegateWork(operation_id=uuid4().hex, objective="Review the current guide", success_criteria="Explain verified omissions")
    result = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=body, admission=admit)
    assert result["objective"] == body.objective and seen == [1, 2]


@pytest.mark.asyncio
async def test_final_delegate_admission_failure_rolls_back_work_and_receipt(actions):
    e = actions
    calls = []

    async def admit(writer):
        calls.append(True)
        if len(calls) == 2:
            raise AgentDenied("Captured caller no longer admits this effect")

    identity = uuid4().hex
    with pytest.raises(AgentDenied):
        await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=DelegateWork(operation_id=identity, objective="Review the guide", success_criteria="Explain omissions"), admission=admit)
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1
        assert await reader.scalar(select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id == identity)) is None


@pytest.mark.asyncio
async def test_exact_delegate_receipt_remains_read_only(actions):
    e = actions
    body = DelegateWork(operation_id=uuid4().hex, objective="Review the guide", success_criteria="Explain omissions")
    result = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=body)

    async def refuse_mutation(_writer):
        raise AssertionError("A committed exact receipt is not another mutation")

    replay = await e.human.work.delegate(actor=e.actor, instance_id=e.instance.id, request=body, admission=refuse_mutation)
    assert replay["id"] == result["id"]
