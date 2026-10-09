"""Current-attempt tools, never human management or ambient Work authority."""

import asyncio
import json

from langchain.tools import ToolRuntime, tool

from deerflow.agent_instances.contract import AgentDenied
from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY, AgentExecution
from deerflow.agent_instances.work_execution import WorkAttempt
from deerflow.agent_instances.work_execution_contract import ReportWork


async def on_owner_loop(execution, operation):
    loop = execution.owner_loop
    if loop is None or not loop.is_running():
        operation.close()
        raise AgentDenied("The host Work authority is unavailable")
    if asyncio.get_running_loop() is loop:
        return await operation
    return await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(operation, loop))


def _execution(runtime):
    execution = (runtime.context or {}).get(AGENT_EXECUTION_CONTEXT_KEY)
    if not isinstance(execution, AgentExecution) or not isinstance(execution.work, WorkAttempt):
        raise AgentDenied("No admitted Work attempt")
    return execution


@tool
async def read_work_context(runtime: ToolRuntime, response_offset: int = 0) -> str:
    """Read the current assigned Work, exact revisions, blocker and attributed replies.

    Work is shared with authorized collaborators. Source references grant no read
    access or mounts. Other Work needs separate explicit human activation. Follow
    next_response_offset until null before assessing the exact complete response set.
    """
    execution = _execution(runtime)
    await execution.validate()
    return json.dumps(await on_owner_loop(execution, execution.work.context(execution, response_offset=response_offset)), ensure_ascii=False)


@tool
async def report_work(report: ReportWork, runtime: ToolRuntime) -> str:
    """Record progress, a blocker, factual assessment, suggested follow-up or outcome.

    Read current Work first and use its exact row revision. Keep one operation_id
    (32-character UUID hex) and identical body for an uncertain retry. request_input
    asks the eligible human supervisor; no email or automatic resumption occurs.
    assess_input is only for sufficient factual replies and must name the exact
    request revision and every assessed response ID. For inadequate factual input,
    revise_input replaces the exact question and retains a blocker. Never treat
    prose as a human decision or acceptance. outcome is a
    completion candidate: it is promoted only after this run settles successfully,
    and required review remains a separate human action. derive is policy-bounded;
    the new Work awaits its own explicit human activation. No report alters priority,
    review requirements, mandate or human authority.
    """
    execution = _execution(runtime)
    await execution.validate()
    result = await on_owner_loop(execution, execution.work.report(execution, report))
    return json.dumps(result, ensure_ascii=False)
