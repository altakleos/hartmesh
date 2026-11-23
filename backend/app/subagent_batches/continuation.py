"""Native batch follow-ups through the existing revocable AgentRuns host."""

import copy
import json
import logging

from deerflow_extension_api.agent_runs import AgentRunError, ExecutionFence

from deerflow.agents.human_input import extract_unanswered_human_input_request
from deerflow.mcp_scope import THREAD_INCARNATION_CONTEXT_KEY
from deerflow.runtime.goal import goal_waits_for_external_result, read_thread_goal
from deerflow.runtime.runs.schemas import GOAL_RUN_HARD_STOP_REASONS
from deerflow.subagents.batch_scope import is_ordinary_batch_spec

logger = logging.getLogger(__name__)
_TRUSTED = object()


async def _require_ordinary_source(app, record):
    from deerflow.runtime.run_origin import origin_kind_of
    from deerflow.runtime.runs.output import OUTPUT_CAPTURE_KEY

    metadata, kwargs = record.metadata or {}, record.kwargs or {}
    if (
        any(metadata.get(key) is not None for key in ("agent_work_id", "agent_work_attempt_id", "agent_instance_id", "scheduled_task_id"))
        or any(kwargs.get(key) is not None for key in (OUTPUT_CAPTURE_KEY, "channel_execution"))
        or origin_kind_of(metadata) not in (None, "extension", "subagent_batch")
    ):
        raise AgentRunError(422, "Automatic batch follow-ups require an ordinary chat Run")
    authority = getattr(app.state, "agent_conversations", None)
    if authority is not None and await authority.binding(record.thread_id) is not None:
        raise AgentRunError(422, "Automatic batch follow-ups are unavailable for instance conversations")


class BatchContinuations:
    def __init__(self, app, *, agent_enabled=False, agent_max_items=50, agent_work_seconds=900):
        self.app = app
        self.agent_enabled = agent_enabled
        self.agent_max_items = agent_max_items
        self.agent_work_seconds = agent_work_seconds
        self._last_agent_job = None

    async def admit_submission(self, request):
        # The Gateway installs a SQL-backed binding reader even when managed
        # storage is off, so a previously bound conversation stays protected.
        authority = getattr(self.app.state, "agent_conversations", None)
        if authority is None:
            raise AgentRunError(503, "The conversation binding cannot be verified")
        instance_id = await authority.binding(request.thread_id)
        if instance_id is None:
            return None
        if not self.agent_enabled:
            raise AgentRunError(422, "Background batches are unavailable for instance conversations")
        from app.subagent_batches.agent_jobs import classify_agent_submission

        return await classify_agent_submission(self.app, request, instance_id, max_items=self.agent_max_items)

    async def prepare(self, request):
        from app.gateway.services import _CONTEXT_CONFIGURABLE_KEYS, _MODEL_SELECTION_CONTEXT_KEYS

        host = self.app.state.agent_runs_host
        grant = host.capture_batch_authority(request.continuation_authority, user_id=request.user_id)
        await host.restore_batch_authority(user_id=request.user_id, grant=grant)
        source = await request.continuation_authority.get(thread_id=request.thread_id, run_id=request.run_id)
        if source.status not in ("pending", "running"):
            raise AgentRunError(409, "The originating Run has already stopped")
        record = await self.app.state.run_manager.get(request.run_id, user_id=request.user_id)
        if record is None or record.user_id != request.user_id or record.thread_id != request.thread_id:
            raise AgentRunError(403, "Batch follow-up must belong to its originating Run")
        from deerflow.subagents.batch_scope import known_agent_batch_scope

        bound = known_agent_batch_scope(request.execution_spec)
        if bound is None:
            await _require_ordinary_source(self.app, record)
        elif not self.agent_enabled or grant.get("source") != "session" or record.metadata.get("agent_instance_id") != bound.instance_id or await self.app.state.agent_conversations.binding(request.thread_id) != bound.instance_id:
            raise AgentRunError(403, "Agent conversation admission changed")
        captured = request.continuation_context or {}
        if (grant.get("durable_followup") is True or (record.metadata or {}).get("deerflow_origin", {}).get("kind") == "subagent_batch") and (captured.get("goal") or {}).get("status") != "active":
            raise AgentRunError(409, "A batch follow-up cannot register another automatic follow-up")
        context = {key: value for key, value in captured.get("context", {}).items() if key in _CONTEXT_CONFIGURABLE_KEYS | _MODEL_SELECTION_CONTEXT_KEYS and key not in ("agent_name", "is_bootstrap")}
        if isinstance(captured.get("context", {}).get("locale"), str):
            context["locale"] = captured["context"]["locale"]
        scope = request.execution_spec.get("knowledge_scope")
        return {
            "prompt": request.continuation,
            "grant": grant,
            "thread_incarnation": request.execution_spec.get(THREAD_INCARNATION_CONTEXT_KEY),
            "goal": copy.deepcopy(captured.get("goal")),
            "context": copy.deepcopy(context),
            "config": copy.deepcopy(captured.get("config", {})),
            "knowledge_scope": copy.deepcopy(scope),
        }

    async def claim_agent_jobs(self, *, now, lease_owner, lease_seconds, start_wait_seconds, exclude_batch_ids):
        repo = self.app.state.subagent_batch_repo
        try:
            candidates = await repo.list_bound_jobs(after=self._last_agent_job, limit=16)
        except Exception as error:
            # Leased personal follow-ups and the ordinary item poll must still run.
            logger.warning("Could not list agent jobs (%s)", type(error).__name__)
            return []
        self._last_agent_job = candidates[-1] if len(candidates) == 16 else None
        claims = []
        for batch_id in candidates:
            if batch_id in exclude_batch_ids:
                continue
            try:
                claim = await repo.claim_bound_dispatch(batch_id, now=now, lease_owner=lease_owner, lease_seconds=lease_seconds, start_wait_seconds=start_wait_seconds)
            except Exception as error:
                logger.warning("Could not reconcile agent job %s (%s)", batch_id, type(error).__name__)
                continue
            if claim is not None:
                claims.append(claim)
        return claims

    async def dispatch(self, batch):
        from deerflow.subagents.batch_scope import known_agent_batch_scope

        if self.agent_enabled and known_agent_batch_scope(batch.get("execution_spec")) is not None:
            from app.subagent_batches.agent_jobs import dispatch_agent_job

            status, reason = await dispatch_agent_job(self.app, batch)
            if status != "dispatched":
                await self.app.state.subagent_batch_repo.finish_bound_dispatch(batch["id"], execution_fence=batch["execution_fence"], status=status, reason=reason)
            return None
        if not is_ordinary_batch_spec(batch.get("execution_spec")):
            return {"status": "suppressed"}
        continuation = batch["continuation"]
        try:
            runs = await self.app.state.agent_runs_host.restore_batch_authority(user_id=batch["user_id"], grant=continuation["grant"])
            record = await self.app.state.run_manager.get(batch["run_id"], user_id=batch["user_id"])
            if record is None or record.thread_id != batch["thread_id"]:
                return {"status": "suppressed"}
            await _require_ordinary_source(self.app, record)
            existing_id = await self.app.state.subagent_batch_repo.continuation_run_id(batch["id"], user_id=batch["user_id"])
            if existing_id is not None:
                existing = await runs.get(thread_id=batch["thread_id"], run_id=existing_id)
                return {"status": "dispatched", "run_id": existing.run_id}
            source = await runs.get(thread_id=batch["thread_id"], run_id=batch["run_id"])
            if source.status in ("pending", "running"):
                return None
            if source.status != "success" or source.stop_reason in GOAL_RUN_HARD_STOP_REASONS:
                return {"status": "suppressed"}
            thread = await self.app.state.thread_store.get(batch["thread_id"], user_id=batch["user_id"])
            if thread is None or thread.get("incarnation") != continuation.get("thread_incarnation"):
                return {"status": "suppressed"}
            state = await runs.get_state(thread_id=batch["thread_id"])
            values = state.get("values", {})
            if extract_unanswered_human_input_request(values.get("messages")) is not None:
                return {"status": "waiting"}
            goal = values.get("goal")
            goal_identity = {key: goal.get(key) for key in ("created_at", "status", "objective")} if isinstance(goal, dict) else None
            expected_goal = continuation.get("goal")
            if goal_identity != expected_goal:
                return {"status": "suppressed"}
            goal_wakeup = None
            if expected_goal is not None:
                latest = await self.app.state.subagent_batch_repo.latest_run_state(batch["thread_id"], user_id=batch["user_id"])
                if latest and latest["status"] in ("pending", "running"):
                    return None
                if latest and (latest["status"] != "success" or latest.get("stop_reason") in GOAL_RUN_HARD_STOP_REASONS):
                    return {"status": "suppressed"}
                if not goal_waits_for_external_result(goal, expected_goal):
                    return None if not (goal.get("last_evaluation") or {}).get("stand_down_reason") else {"status": "suppressed"}
                goal_wakeup = expected_goal
            public = await self.app.state.subagent_batch_repo.get_batch(batch["id"], user_id=batch["user_id"])
            if public is None or public.get("continuation_status") == "cancelled" or public["status"] == "cancelled":
                return {"status": "suppressed"}
            if public["status"] not in ("completed", "failed"):
                return None
            # Stable input is essential after an unknown admission outcome.
            # Counts/results stay in the existing owner-scoped result reader.
            content = "The requested background batch has finished. Continue the agreed follow-up using its stored results; preserve any failures or missing information.\n" + json.dumps(
                {"batch_id": batch["id"], "follow_up": continuation["prompt"]}, ensure_ascii=False
            )
            from deerflow.knowledge_scope import KNOWLEDGE_SCOPE_KEY

            metadata = {"hide_from_ui": True, "untrusted_input": True}
            if continuation.get("knowledge_scope") is not None:
                metadata[KNOWLEDGE_SCOPE_KEY] = continuation["knowledge_scope"]
            result = await runs.start_batch_followup(
                thread_id=batch["thread_id"],
                batch_id=batch["id"],
                input={"messages": [{"role": "user", "content": content, "additional_kwargs": metadata}]},
                context=continuation.get("context"),
                config=continuation.get("config"),
                execution_fence=ExecutionFence(**batch["execution_fence"]),
                goal_wakeup=goal_wakeup,
            )
            return {"status": "dispatched", "run_id": result.run_id}
        except AgentRunError as exc:
            if exc.status_code == 409:
                return None
            if exc.status_code in (400, 401, 403, 404, 422):
                return {"status": "suppressed"}
            raise

    async def cancel(self, batch_id, user_id, *, thread_id=None, authority=_TRUSTED, authorize_only=False, bound_run_id=None):
        if authority is not _TRUSTED:
            self.app.state.agent_runs_host.capture_batch_authority(authority, user_id=user_id)
            async with authority._request("runs:cancel", thread_id) as request:
                from app.gateway.agent_conversations import require_conversation
                from deerflow.agent_instances.contract import AgentPermission

                if await require_conversation(request, thread_id, permission=AgentPermission.USE):
                    await require_conversation(request, thread_id, permission=AgentPermission.INSPECT)
                if authorize_only:
                    return
        elif authorize_only:
            return
        run_id = bound_run_id or await self.app.state.subagent_batch_repo.continuation_run_id(batch_id, user_id=user_id)
        if run_id is None:
            return
        record = await self.app.state.run_manager.get(run_id, user_id=user_id)
        if record is not None:
            if bound_run_id is not None:
                from deerflow.runtime.run_origin import origin_kind_of

                if record.thread_id != thread_id or record.user_id != user_id or origin_kind_of(record.metadata) != "subagent_batch" or (record.kwargs or {}).get("agent_job_id") != batch_id:
                    return
            await self.app.state.run_manager.cancel(run_id)

    async def source_finished(self, record):
        if record.stop_reason == "goal_wakeup_superseded":
            return
        if record.status == "success" and record.stop_reason not in GOAL_RUN_HARD_STOP_REASONS:
            return
        for batch in await self.app.state.subagent_batch_repo.list_continuation_batches_for_run(record.run_id):
            try:
                await self.app.state.subagent_batch_service.cancel_batch(batch_id=batch["id"], user_id=batch["user_id"])
            except Exception:
                logger.warning("Batch %s cancellation failed for run %s (non-fatal)", batch["id"], record.run_id, exc_info=True)
        goal = await read_thread_goal(self.app.state.checkpointer, record.thread_id)
        if goal and goal.get("status") == "active" and goal.get("created_at"):
            await self.app.state.subagent_batch_repo.suppress_thread_continuations(record.thread_id, goal_created_at=goal["created_at"])
