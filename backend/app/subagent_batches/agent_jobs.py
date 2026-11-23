"""Session-bound jobs use a fresh employee conversation admission for every run."""

import json

from deerflow_extension_api import AgentRunError

from deerflow.agent_instances.chat_tools import successful_assignment_proposal
from deerflow.agent_instances.contract import Refusal
from deerflow.agents.human_input import extract_unanswered_human_input_request
from deerflow.mcp_scope import THREAD_INCARNATION_CONTEXT_KEY
from deerflow.runtime.run_origin import origin_kind_of
from deerflow.runtime.runs.output import OUTPUT_CAPTURE_KEY
from deerflow.subagents.batch_scope import known_agent_batch_scope
from deerflow.utils.messages import is_real_user_message


async def classify_agent_submission(app, request, instance_id, *, max_items):
    if not request.continuation or not request.continuation.strip():
        raise AgentRunError(422, "For a job in an agent conversation, say what to do with the results")
    if len(request.items) > max_items:
        raise AgentRunError(422, f"An agent conversation job allows at most {max_items} checks")
    from deerflow.agent_instances.conversations import AgentExecution

    execution = request.agent_execution
    if (
        not isinstance(execution, AgentExecution)
        or execution.batch_mode != "submit"
        or execution.work is not None
        or execution.instance.id != instance_id
        or execution.requester.subject_id != request.user_id
        or execution.thread_id != request.thread_id
    ):
        raise AgentRunError(403, "Only the requesting web turn may schedule a job in this conversation")
    await execution.validate()
    host = app.state.agent_runs_host
    grant = host.capture_batch_authority(request.continuation_authority, user_id=request.user_id)
    if grant["source"] != "session" or grant["durable_followup"]:
        raise AgentRunError(403, "Start a job from a signed-in web conversation")
    # Refresh the account and permission ceiling before any data is stored.
    await host.restore_batch_authority(user_id=request.user_id, grant=grant)
    record = await app.state.run_manager.get(request.run_id, user_id=request.user_id) if request.run_id else None
    if record is None or record.thread_id != request.thread_id or record.user_id != request.user_id or record.status not in ("pending", "running"):
        raise AgentRunError(409, "The requesting turn is no longer active")
    metadata, kwargs = record.metadata or {}, record.kwargs or {}
    if (
        metadata.get("agent_instance_id") != instance_id
        or origin_kind_of(metadata) is not None
        or any(metadata.get(k) is not None for k in ("agent_work_id", "agent_work_attempt_id", "scheduled_task_id"))
        or any(kwargs.get(k) is not None for k in (OUTPUT_CAPTURE_KEY, "channel_execution"))
    ):
        raise AgentRunError(422, "Start a job in a person's web conversation with this agent")
    captured = request.continuation_context or {}
    if (captured.get("goal") or {}).get("status") == "active":
        raise AgentRunError(409, "End the active goal before requesting an agent job")
    thread = await app.state.thread_store.get(request.thread_id, user_id=request.user_id)
    incarnation = request.execution_spec.get(THREAD_INCARNATION_CONTEXT_KEY)
    if not incarnation or incarnation != execution.thread_incarnation or not thread or thread.get("incarnation") != incarnation:
        raise AgentRunError(404, "The requesting conversation is unavailable")
    principal = {"source": "session", "token_version": grant["token_version"], **grant["principal"]}
    scope = {"version": 1, "kind": "agent_conversation", "instance_id": instance_id, "principal": principal}
    if known_agent_batch_scope({"scope": scope}) is None:
        raise AgentRunError(403, "The requesting session cannot start an agent job")
    return scope


def require_job_execution(request, execution, *, origin, thread_id, idempotency_key):
    """Bind the native run to the host's stored session and current agent execution."""
    from fastapi import HTTPException

    from deerflow.persistence.subagent_batches.bound import BoundBatchExecutionFence
    from deerflow.subagents.batch_runtime import BATCH_CONTINUATION_NAMESPACE, batch_continuation_key

    fence = getattr(request.state, "extension_execution_fence", None)
    if execution is None and not isinstance(fence, BoundBatchExecutionFence):
        return None
    if not isinstance(fence, BoundBatchExecutionFence) or execution is None or origin != {"kind": "subagent_batch"} or fence.namespace != BATCH_CONTINUATION_NAMESPACE:
        raise HTTPException(422, "Automatic batch follow-ups require a matching agent conversation job")
    scope = known_agent_batch_scope({"scope": fence.scope})
    user = request.state.user
    key = batch_continuation_key(fence.resource_id)
    if (
        scope is None
        or execution.instance.id != scope.instance_id
        or execution.thread_id != thread_id
        or execution.requester.subject_id != str(user.id)
        or execution.thread_incarnation != fence.thread_incarnation
        or user.token_version != scope.token_version
        or user.system_role != scope.system_role
        or user.oauth_provider != scope.oauth_provider
        or user.oauth_id != scope.oauth_id
        or getattr(request.state, "auth_source", None) != "session"
        or idempotency_key not in (key, key + ":replacement")
    ):
        raise HTTPException(403, "The agent job can no longer use this conversation")
    return fence


def waiting_for_response(messages):
    """Recognize open cards in the same wire representation returned by AgentRuns."""
    if extract_unanswered_human_input_request(messages) is not None:
        return True
    from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

    # Assignment proposals identify the actual issuing tool call and its human
    # turn. Rehydrate just that turn when the checkpoint ends in tool output.
    if not messages or messages[-1].get("type") != "tool":
        return False
    classes = {"human": HumanMessage, "ai": AIMessage, "tool": ToolMessage}
    turn = []
    for data in reversed(messages):
        cls = classes.get(data.get("type"))
        if cls is None:
            continue
        message = cls.model_validate(data)
        turn.append(message)
        if is_real_user_message(message):
            break
    return successful_assignment_proposal(list(reversed(turn))) is not None


async def access_loss_reason(runs, conversations, thread_id, instance_id):
    """Show lifecycle details only to someone who can still use and read the thread."""
    from app.gateway.routers.spaces import _actor
    from deerflow.agent_instances.contract import AgentDenied, AgentPermission

    try:
        async with runs._request("threads:read", thread_id) as request:
            if await conversations.binding(thread_id) != instance_id:
                return "access-lost"
            actor = await _actor(request)
            for permission in (AgentPermission.USE, AgentPermission.INSPECT):
                if not await conversations.allowed(actor=actor, thread_id=thread_id, permission=permission):
                    return "access-lost"
            view = await conversations.instances.get(actor=actor, instance_id=instance_id, permission=AgentPermission.USE)
            return {"suspended": "agent-paused", "archived": "agent-archived"}.get(view.status, "access-lost")
    except (AgentDenied, AgentRunError):
        return "access-lost"


async def dispatch_agent_job(app, batch):
    """Only claimants can admit a run. Unknown admission outcomes retry the same key."""
    from deerflow.persistence.subagent_batches.bound import BoundBatchExecutionFence

    scope = known_agent_batch_scope(batch["execution_spec"])
    continuation = batch["continuation"]
    grant = continuation["grant"]
    if (
        scope is None
        or grant.get("source") != "session"
        or grant.get("token_version") != scope.token_version
        or grant.get("principal") != {"system_role": scope.system_role, "oauth_provider": scope.oauth_provider, "oauth_id": scope.oauth_id}
    ):
        return "ended", "account-changed"
    try:
        runs = await app.state.agent_runs_host.restore_batch_authority(user_id=batch["user_id"], grant=grant)
    except AgentRunError as error:
        if error.status_code == 403:
            return "ended", "account-changed"
        raise
    thread_id = batch["thread_id"]
    if await app.state.agent_conversations.binding(thread_id) != scope.instance_id:
        return "ended", "access-lost"
    try:
        thread = await app.state.thread_store.get(thread_id, user_id=batch["user_id"])
        if not thread or thread.get("incarnation") != continuation["thread_incarnation"]:
            return "ended", "access-lost"
        state = await runs.get_state(thread_id=thread_id)
        values = state.get("values") or {}
        messages = values.get("messages") or []
        if waiting_for_response(messages):
            return "waiting", "waiting-for-answer"
        if (values.get("goal") or {}).get("status") == "active":
            return "ended", "goal-active"
        fence = BoundBatchExecutionFence(**batch["execution_fence"], scope=batch["execution_spec"]["scope"], thread_incarnation=continuation["thread_incarnation"], thread_generation=batch["thread_generation"])
        content = "Use the stored results of the requested checks for the agreed response. Preserve failures and missing information.\n" + json.dumps({"batch_id": batch["id"], "follow_up": continuation["prompt"]}, ensure_ascii=False)
        metadata = {"hide_from_ui": True, "untrusted_input": True}
        if continuation.get("knowledge_scope") is not None:
            from deerflow.knowledge_scope import KNOWLEDGE_SCOPE_KEY

            metadata[KNOWLEDGE_SCOPE_KEY] = continuation["knowledge_scope"]
        await runs.start_batch_followup(
            thread_id=thread_id,
            batch_id=batch["id"],
            input={"messages": [{"role": "user", "content": content, "additional_kwargs": metadata}]},
            context=continuation.get("context"),
            config=continuation.get("config"),
            execution_fence=fence,
            idempotency_key=batch["idempotency_key"],
        )
        return "dispatched", None
    except Refusal as error:
        if error.reason == "files-full":
            return "ended", "files-full"
        return "waiting", error.reason or "busy"
    except AgentRunError as error:
        if error.status_code == 404:
            return "ended", await access_loss_reason(runs, app.state.agent_conversations, thread_id, scope.instance_id)
        if error.status_code == 403:
            return "ended", "access-lost"
        if error.status_code in (409, 412, 423, 429, 503):
            return "waiting", getattr(error, "reason", None) or "busy"
        if error.status_code in (400, 422):
            return "ended", "cannot-run"
        raise
