from __future__ import annotations

import asyncio
import logging
import socket
import uuid
from datetime import UTC, datetime
from typing import Any

from deerflow_extension_api import TrustedRunContextV1

from deerflow.config.app_config import AppConfig
from deerflow.config.subagent_batches_config import SubagentBatchesConfig
from deerflow.config.subagent_runtime_config import SubagentRuntimeConfig
from deerflow.runtime.accepted_invocation import (
    ResolvedAgentMaterialV1,
    canonical_digest,
)
from deerflow.runtime.agent_revision import app_config_execution_digest
from deerflow.runtime.skill_projection import (
    SkillProjectionConsumerToken,
    get_skill_projection_coordinator,
)
from deerflow.runtime.subagent_snapshot import (
    SubagentCatalogError,
    resolved_tool_contract_digest,
)
from deerflow.runtime.tool_evidence import (
    NullDurableToolReceiptSink,
    get_active_tool_receipt,
)
from deerflow.sandbox.sandbox_provider import release_accepted_skill_consumer
from deerflow.subagents.batch_acceptance import (
    AcceptedBatchItemV1,
    AcceptedBatchV1,
    BatchAdmissionConflict,
    BatchAdmissionError,
    BatchItemRequestV1,
    ParentBoundBatchExecutionV1,
    ParentBoundBatchRequest,
)
from deerflow.subagents.capacity import SubagentExecutionCapacity
from deerflow.subagents.config import resolve_subagent_model_name
from deerflow.subagents.executor import (
    SubagentExecutor,
    SubagentStatus,
    cleanup_background_task,
    get_background_task_result,
    request_cancel_background_task,
)
from deerflow.subagents.status_contract import SUBAGENT_STOP_REASON_VALUES

logger = logging.getLogger(__name__)
_TERMINAL_BATCH_STATUSES = frozenset({"completed", "failed", "cancelled"})


def _usage(records: list[dict[str, Any]] | None) -> dict[str, int] | None:
    if not records:
        return None
    return {
        "input_tokens": sum(int(row.get("input_tokens") or 0) for row in records),
        "output_tokens": sum(int(row.get("output_tokens") or 0) for row in records),
        "total_tokens": sum(int(row.get("total_tokens") or 0) for row in records),
    }


def _safe_stop_reason(value: object) -> str | None:
    """Keep only the public, bounded subagent stop-reason vocabulary."""

    return value if value in SUBAGENT_STOP_REASON_VALUES else None


class SubagentBatchService:
    """Lease, execute, and recover durable native-subagent batch items."""

    def __init__(
        self,
        *,
        repository,
        config: SubagentBatchesConfig,
        runtime_config: SubagentRuntimeConfig,
        app_config: AppConfig | None = None,
        execution_capacity: SubagentExecutionCapacity | None = None,
        extensions: Any | None = None,
        authorization_provider: Any | None = None,
        capability_manifest_digest: str | None = None,
    ) -> None:
        self._repository = repository
        self._config = config
        self._runtime_config = runtime_config
        self._app_config = app_config
        self._execution_capacity = execution_capacity
        self._extensions = extensions
        self._authorization_provider = authorization_provider
        self._capability_manifest_digest = capability_manifest_digest
        self._lease_owner = f"{socket.gethostname()}:{uuid.uuid4().hex}"
        self._stop = asyncio.Event()
        self._poller: asyncio.Task[None] | None = None
        self._executions: dict[str, asyncio.Task[None]] = {}
        self._execution_ids: dict[str, str] = {}
        self._item_batches: dict[str, str] = {}
        self._accepted_material: dict[str, ResolvedAgentMaterialV1] = {}
        self._runtime_adapters: dict[str, dict[str, Any]] = {}
        self._batch_owners: dict[str, str] = {}
        # Installing process material must precede the transaction that makes
        # items claimable. Serialize that handoff with terminal pruning and
        # duplicate submissions so cleanup cannot create a material-less row
        # or release another submitter's retained lease.
        self._material_lock = asyncio.Lock()

    async def start(self) -> None:
        if self._poller is not None:
            return
        self._stop.clear()
        self._poller = asyncio.create_task(self._run(), name="subagent-batch-poller")

    async def stop(self) -> None:
        self._stop.set()
        poller = self._poller
        self._poller = None
        if poller is not None:
            poller.cancel()
            await asyncio.gather(poller, return_exceptions=True)
        execution_ids = list(self._execution_ids.values())
        for execution_id in execution_ids:
            request_cancel_background_task(execution_id)
        tasks = list(self._executions.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._executions.clear()
        self._execution_ids.clear()
        self._item_batches.clear()
        async with self._material_lock:
            for batch_id in list(self._accepted_material):
                await self._release_batch_material(batch_id)
            self._accepted_material.clear()
            self._runtime_adapters.clear()
            self._batch_owners.clear()

    async def _release_batch_material(self, batch_id: str) -> None:
        material = self._accepted_material.get(batch_id)
        adapters = self._runtime_adapters.get(batch_id, {})
        projection_token = adapters.get("skill_projection_token")
        if isinstance(projection_token, SkillProjectionConsumerToken):
            try:
                await asyncio.to_thread(
                    release_accepted_skill_consumer,
                    projection_token,
                )
            except Exception:
                logger.exception(
                    "Failed to release durable subagent batch skill projection (batch_id=%s)",
                    batch_id,
                )
                return
            # Releasing a consumer is not guaranteed to be repeatable. Clear
            # the process-local handle before releasing the separate material
            # lease so a later cleanup retry cannot double-release it.
            adapters["skill_projection_token"] = None
        if material is not None:
            try:
                await asyncio.to_thread(material.release_process_material)
            except Exception:
                logger.exception(
                    "Failed to release durable subagent batch material (batch_id=%s)",
                    batch_id,
                )
                return
            if self._accepted_material.get(batch_id) is material:
                self._accepted_material.pop(batch_id, None)
        self._runtime_adapters.pop(batch_id, None)
        self._batch_owners.pop(batch_id, None)

    async def _prune_terminal_material(self) -> None:
        getter = getattr(self._repository, "get_batch", None)
        if not callable(getter):
            return
        async with self._material_lock:
            active_batch_ids = set(self._item_batches.values())
            for batch_id in list(self._accepted_material):
                if batch_id in active_batch_ids:
                    continue
                user_id = self._batch_owners.get(batch_id)
                if user_id is None:
                    continue
                try:
                    batch = await getter(batch_id, user_id=user_id)
                except Exception:
                    logger.exception(
                        "Failed to inspect durable subagent batch material (batch_id=%s)",
                        batch_id,
                    )
                    continue
                if batch is None or batch.get("status") in _TERMINAL_BATCH_STATUSES:
                    await self._release_batch_material(batch_id)

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.run_once(now=datetime.now(UTC))
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Subagent batch scheduler pass failed")
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=self._config.poll_interval_seconds,
                )
            except TimeoutError:
                pass

    async def run_once(self, *, now: datetime) -> None:
        await self._prune_terminal_material()
        available = max(0, self._runtime_config.max_running - len(self._executions))
        if available <= 0:
            return
        items = await self._repository.claim_items(
            now=now,
            lease_owner=self._lease_owner,
            lease_seconds=self._config.lease_seconds,
            limit=available,
        )
        for item in items:
            item_id = item["id"]
            if item_id in self._executions:
                continue
            self._item_batches[item_id] = item["batch"]["id"]
            task = asyncio.create_task(
                self._execute_item(item),
                name=f"subagent-batch-item-{item_id}",
            )
            self._executions[item_id] = task
            task.add_done_callback(
                lambda _task, current_id=item_id: self._executions.pop(
                    current_id,
                    None,
                )
            )

    async def accept(self, request: ParentBoundBatchRequest) -> dict[str, Any]:
        """Persist one subordinate acceptance from trusted parent objects."""

        if not isinstance(request, ParentBoundBatchRequest):
            raise BatchAdmissionError("parent_not_accepted")
        total = len(request.items)
        if total < 1 or total > self._config.max_items_per_batch:
            raise BatchAdmissionError("batch_item_count_invalid")
        limits = request.limits
        if (
            limits.max_live_items > self._config.max_live_items_per_batch
            or limits.max_running_items > self._config.max_running_items_per_batch
            or limits.max_attempts > self._config.max_attempts
            or limits.max_attempt_records_per_item > self._config.max_attempt_records_per_item
            or limits.max_result_chars > self._config.max_result_chars
            or limits.max_total_runtime_seconds > self._config.max_total_runtime_seconds
        ):
            raise BatchAdmissionError("batch_limits_invalid")
        if request.resolved_parent_material.runtime_defaults.get("subagent_enabled") is not True:
            raise BatchAdmissionError("subagent_not_accepted")
        # A durable started receipt can outlive the synchronous tool frame in
        # storage. Require the host's live tool-scope capability too, so a
        # captured receipt cannot admit a batch after that attempt returned.
        if get_active_tool_receipt() is not request.parent_tool_receipt:
            raise BatchAdmissionError("tool_attempt_not_active")
        if isinstance(
            request.parent_tool_receipt_sink,
            NullDurableToolReceiptSink,
        ):
            raise BatchAdmissionError("tool_attempt_not_active")
        try:
            # Re-appending the already-reserved start is idempotent, while
            # the durable receipt store rechecks the parent owner/lease fence
            # before returning the existing event.
            await request.parent_tool_receipt_sink.record_started(request.parent_tool_receipt)
        except Exception as exc:
            raise BatchAdmissionError("tool_attempt_not_active") from exc

        try:
            batch_id = (
                "sb_"
                + canonical_digest(
                    {
                        "version": 1,
                        "domain": "parent_bound_subagent_batch_id",
                        "tenant_digest": request.tenant.digest,
                        "parent_tool_receipt_id": (request.parent_tool_receipt.receipt_id),
                        "submission_key": request.submission_key,
                    }
                )[:48]
            )
            accepted = AcceptedBatchV1.from_parent_request(
                request,
                batch_id=batch_id,
            )
            if accepted.evidence_size_bytes > self._config.max_evidence_bytes:
                raise BatchAdmissionError("batch_acceptance_too_large")
            execution = ParentBoundBatchExecutionV1.from_parent_request(
                request,
                accepted=accepted,
            )
            accepted_app_config = request.resolved_parent_material.app_config if request.resolved_parent_material.app_config is not None else request.app_config if request.app_config is not None else self._app_config
            if accepted_app_config is not None:
                self._validate_app_execution_digest(
                    execution,
                    accepted_app_config,
                )
        except BatchAdmissionError:
            raise
        except Exception as exc:
            raise BatchAdmissionError("execution_material_unavailable") from exc

        async with self._material_lock:
            return await self._persist_accepted_batch(
                request=request,
                batch_id=batch_id,
                accepted=accepted,
                execution=execution,
                accepted_app_config=accepted_app_config,
            )

    async def _persist_accepted_batch(
        self,
        *,
        request: ParentBoundBatchRequest,
        batch_id: str,
        accepted: AcceptedBatchV1,
        execution: ParentBoundBatchExecutionV1,
        accepted_app_config: object | None,
    ) -> dict[str, Any]:
        """Install retained material and commit acceptance as one local fence."""

        installed = False
        if batch_id in self._accepted_material:
            try:
                await asyncio.to_thread(
                    request.resolved_parent_material.verify_process_material,
                )
            except Exception as exc:
                raise BatchAdmissionError("execution_material_unavailable") from exc
        else:
            retained_token: SkillProjectionConsumerToken | None = None

            def retain_verified_material() -> ResolvedAgentMaterialV1:
                request.resolved_parent_material.verify_process_material()
                return request.resolved_parent_material.retain_process_material()

            try:
                retained = await asyncio.to_thread(retain_verified_material)
                if request.skill_projection_token is not None:
                    if not isinstance(
                        request.skill_projection_token,
                        SkillProjectionConsumerToken,
                    ):
                        raise BatchAdmissionError("execution_material_unavailable")
                    retained_token = await asyncio.to_thread(
                        get_skill_projection_coordinator().retain,
                        request.skill_projection_token,
                        consumer_id=f"subagent-batch:{batch_id}",
                    )
            except Exception as exc:
                if "retained" in locals():
                    await asyncio.to_thread(retained.release_process_material)
                if isinstance(exc, BatchAdmissionError):
                    raise
                raise BatchAdmissionError("execution_material_unavailable") from exc
            self._accepted_material[batch_id] = retained
            self._batch_owners[batch_id] = request.user_id
            self._runtime_adapters[batch_id] = {
                "app_config": accepted_app_config,
                "extensions": (request.extensions if request.extensions is not None else self._extensions),
                "authorization_provider": (request.authorization_provider if request.authorization_provider is not None else self._authorization_provider),
                "invocation_constraints": request.invocation_constraints,
                "skill_projection_token": retained_token,
                "accepted_parent": request.accepted_parent,
            }
            installed = True

        try:
            result = await self._repository.accept_batch(
                accepted=accepted,
                execution=execution,
                item_requests=request.items,
                user_id=request.user_id,
                submission_key=request.submission_key,
                title=request.title,
                subagent_type=request.subagent_name,
            )
        except BatchAdmissionConflict:
            if installed:
                await self._release_batch_material(batch_id)
            raise
        except BatchAdmissionError:
            if installed:
                await self._release_batch_material(batch_id)
            raise
        if result.get("status") in _TERMINAL_BATCH_STATUSES and batch_id not in self._item_batches.values():
            await self._release_batch_material(batch_id)
        return result

    async def load_execution(
        self,
        batch_id: str,
    ) -> ParentBoundBatchExecutionV1:
        loaded = await self._repository.load_execution(batch_id)
        execution = loaded.get("execution")
        if not isinstance(execution, ParentBoundBatchExecutionV1):
            raise BatchAdmissionError("execution_material_unavailable")
        return execution

    async def submit(self, request: object) -> dict[str, Any]:
        """Reject the removed free-form admission seam."""

        del request
        raise BatchAdmissionError("parent_not_accepted")

    async def get_batch(
        self,
        *,
        batch_id: str,
        user_id: str,
    ) -> dict[str, Any] | None:
        return await self._repository.get_batch(batch_id, user_id=user_id)

    async def cancel_batch(
        self,
        *,
        batch_id: str,
        user_id: str,
    ) -> dict[str, Any] | None:
        batch = await self._repository.cancel_batch(batch_id, user_id=user_id)
        if batch is None:
            return None
        for item_id, execution_id in list(self._execution_ids.items()):
            if self._item_batches.get(item_id) == batch_id:
                request_cancel_background_task(execution_id)
        if batch_id not in self._item_batches.values():
            await self._prune_terminal_material()
        # Normal ids are not prefixed; the renew loop observes the durable
        # cancellation within lease_seconds/3. Keeping cancellation durable is
        # what lets another worker own the HTTP control request safely.
        return batch

    async def _execution_material(
        self,
        *,
        batch_id: str,
        acceptance: AcceptedBatchV1,
        execution: ParentBoundBatchExecutionV1,
    ) -> ResolvedAgentMaterialV1:
        material = self._accepted_material.get(batch_id)
        if material is not None:
            try:
                await asyncio.to_thread(material.verify_process_material)
            except Exception as exc:
                raise BatchAdmissionError("execution_material_unavailable") from exc
            if material.subagent_catalog.digest != acceptance.subagent_catalog_digest or material.subagent_catalog.get(execution.selected_subagent_name) != execution.selected_definition:
                raise BatchAdmissionError("execution_material_unavailable")
            return material

        # Empty-skill accepted material is fully reconstructable from the
        # protected execution record.  Snapshot-backed skills deliberately
        # require their retained immutable filesystem lease; there is no live
        # skill-registry fallback after a restart.
        if execution.skill_snapshot_id is not None or acceptance.skill_material_digests:
            raise BatchAdmissionError("execution_material_unavailable")
        try:
            material = ResolvedAgentMaterialV1(
                agent_id=execution.agent_id,
                storage_source=execution.storage_source,
                storage_version=execution.storage_version,
                agent_config=None,
                soul=b"",
                model_profile=dict(execution.model_profile),
                tool_groups=execution.tool_groups,
                tools=execution.parent_tool_names,
                skills=(),
                runtime_defaults=dict(execution.runtime_defaults),
                subagent_catalog=execution.catalog,
                skill_scopes=execution.skill_scopes,
                app_config=self._app_config,
                user_id=execution.user_id,
            )
            await asyncio.to_thread(material.verify_process_material)
        except Exception as exc:
            raise BatchAdmissionError("execution_material_unavailable") from exc
        self._accepted_material[batch_id] = material
        self._batch_owners[batch_id] = execution.user_id
        return material

    def _validated_extensions(
        self,
        acceptance: AcceptedBatchV1,
        adapters: dict[str, Any],
    ) -> Any | None:
        if acceptance.capability_manifest_digest != self._capability_manifest_digest:
            raise BatchAdmissionError("provider_not_qualified")
        extensions = adapters["extensions"] if "extensions" in adapters else self._extensions
        if extensions is None:
            if acceptance.extension_generation != 0 or acceptance.extension_artifact_manifest_digest is not None:
                raise BatchAdmissionError("provider_not_qualified")
            return None
        if getattr(extensions, "generation", None) != acceptance.extension_generation:
            raise BatchAdmissionError("provider_not_qualified")
        if getattr(extensions, "artifact_manifest_digest", None) != acceptance.extension_artifact_manifest_digest or getattr(extensions, "extension_configuration_digest", None) != acceptance.extension_configuration_digest:
            raise BatchAdmissionError("provider_not_qualified")
        return extensions

    @staticmethod
    def _validate_app_execution_digest(
        execution: ParentBoundBatchExecutionV1,
        app_config: object,
    ) -> None:
        if not isinstance(app_config, AppConfig):
            return
        accepted_digest = execution.model_profile.get("app_execution_digest")
        try:
            current_digest = app_config_execution_digest(app_config)
        except Exception as exc:
            raise BatchAdmissionError("provider_not_qualified") from exc
        if accepted_digest != current_digest:
            raise BatchAdmissionError("provider_not_qualified")

    def _validated_tools(
        self,
        acceptance: AcceptedBatchV1,
        discovered_tools: list[Any],
    ) -> list[Any]:
        discovered_by_name = {str(getattr(tool, "name", "")): tool for tool in discovered_tools}
        if len(discovered_by_name) != len(discovered_tools):
            raise BatchAdmissionError("provider_not_qualified")
        try:
            selected = [discovered_by_name[name] for name in acceptance.allowed_tool_names]
        except KeyError as exc:
            raise BatchAdmissionError("provider_not_qualified") from exc
        if tuple(resolved_tool_contract_digest(tool) for tool in selected) != acceptance.allowed_tool_contract_digests:
            raise BatchAdmissionError("provider_not_qualified")
        return selected

    async def _execute_item(self, item: dict[str, Any]) -> None:
        item_id = item["id"]
        execution_id: str | None = None
        batch_id: str | None = None
        user_id: str | None = None
        service_loop = asyncio.get_running_loop()
        try:
            batch = item["batch"]
            batch_id = batch["id"]
            user_id = batch["user_id"]
            self._item_batches[item_id] = batch_id
            acceptance = batch.get("acceptance")
            execution = batch.get("execution")
            if not isinstance(acceptance, AcceptedBatchV1) or not isinstance(
                execution,
                ParentBoundBatchExecutionV1,
            ):
                raise BatchAdmissionError("legacy_batch_unbound")
            try:
                immutable_item = AcceptedBatchItemV1.from_request(
                    BatchItemRequestV1(
                        key=item["item_key"],
                        prompt=item["prompt"],
                    ),
                    batch_id=acceptance.batch_id,
                    ordinal=item["position"],
                )
            except (BatchAdmissionError, KeyError, TypeError) as exc:
                raise BatchAdmissionError("execution_material_unavailable") from exc
            if (
                acceptance.batch_id != batch["id"]
                or execution.acceptance_digest != acceptance.acceptance_digest
                or immutable_item.ordinal >= acceptance.item_count
                or immutable_item.item_id != item_id
                or immutable_item.request_digest != item.get("request_digest")
            ):
                raise BatchAdmissionError("execution_material_unavailable")
            try:
                execution.verify_against_acceptance(acceptance)
            except BatchAdmissionError as exc:
                raise BatchAdmissionError("execution_material_unavailable") from exc
            config = execution.selected_definition.to_subagent_config()
            adapters = self._runtime_adapters.get(batch["id"], {})
            if batch["id"] not in self._runtime_adapters and self._authorization_provider is not None:
                raise BatchAdmissionError("execution_material_unavailable")
            app_config = adapters["app_config"] if "app_config" in adapters else self._app_config
            if app_config is None:
                raise BatchAdmissionError("execution_material_unavailable")
            self._validate_app_execution_digest(execution, app_config)
            parent_model_value = execution.model_profile.get("name")
            parent_model = parent_model_value if isinstance(parent_model_value, str) else None
            try:
                await asyncio.to_thread(
                    execution.selected_definition.verify_execution_settings,
                    app_config,
                    parent_model_name=parent_model,
                )
            except SubagentCatalogError as exc:
                raise BatchAdmissionError("provider_not_qualified") from exc
            material = await self._execution_material(
                batch_id=batch["id"],
                acceptance=acceptance,
                execution=execution,
            )
            from deerflow.tools import get_available_tools

            effective_model = resolve_subagent_model_name(
                config,
                parent_model,
                app_config=app_config,
            )
            discovered_tools = await asyncio.to_thread(
                get_available_tools,
                groups=list(execution.tool_groups),
                model_name=effective_model,
                subagent_enabled=False,
                include_upload_tool=False,
                app_config=app_config,
            )
            tools = self._validated_tools(acceptance, discovered_tools)
            accepted_parent = adapters.get("accepted_parent")
            principal = execution.parent_principal
            trusted_context = None
            if accepted_parent is not None:
                trusted_context = accepted_parent.trusted_context
                if trusted_context is not None:
                    trusted_context = trusted_context.bind_run(acceptance.parent_run_id)
            elif execution.trusted_context is not None:
                trusted_context = TrustedRunContextV1.from_persisted_json(dict(execution.trusted_context)).bind_run(acceptance.parent_run_id)
            persisted_constraints = execution.constraint_projection
            execution.validate_constraint_freshness()
            supplied_constraints = adapters.get("invocation_constraints")
            if supplied_constraints is not None and supplied_constraints != persisted_constraints:
                raise BatchAdmissionError("execution_material_unavailable")

            async def persist_execution_started() -> None:
                async def mark_on_service_loop() -> None:
                    marked_running = await self._repository.mark_item_running(
                        item_id,
                        attempt_id=item.get("attempt_id"),
                        lease_epoch=item.get("lease_epoch"),
                        lease_owner=self._lease_owner,
                        now=None,
                    )
                    if not marked_running:
                        raise BatchAdmissionError("lease_lost")

                if asyncio.get_running_loop() is service_loop:
                    await mark_on_service_loop()
                    return
                future = asyncio.run_coroutine_threadsafe(
                    mark_on_service_loop(),
                    service_loop,
                )
                await asyncio.wrap_future(future)

            executor = SubagentExecutor(
                config=config,
                tools=tools,
                app_config=app_config,
                parent_model=parent_model,
                thread_id=batch["thread_id"],
                user_id=batch["user_id"],
                user_role=_optional_string(principal.get("role")),
                oauth_provider=_optional_string(principal.get("oauth_provider")),
                oauth_id=_optional_string(principal.get("oauth_id")),
                run_id=batch.get("run_id"),
                channel_user_id=_optional_string(principal.get("channel_user_id")),
                is_internal=principal.get("is_internal") is True,
                trusted_run_context=trusted_context,
                extensions=self._validated_extensions(
                    acceptance,
                    adapters,
                ),
                authorization_provider=(adapters["authorization_provider"] if "authorization_provider" in adapters else self._authorization_provider),
                invocation_constraints=persisted_constraints,
                resolved_agent_material=material,
                skill_projection_token=adapters.get("skill_projection_token"),
                accepted_extension_generation=(acceptance.extension_generation),
                accepted_extension_manifest_digest=(acceptance.capability_manifest_digest),
                accepted_extension_artifact_manifest_digest=(acceptance.extension_artifact_manifest_digest),
                accepted_extension_configuration_digest=(acceptance.extension_configuration_digest),
                execution_capacity=self._execution_capacity,
                execution_admitted_callback=persist_execution_started,
            )
            prompt = f"Durable batch item key: {item['item_key']}\nThis item may be retried after a worker crash. Keep side effects idempotent and use the item key as the idempotency identity.\n\n{item['prompt']}"
            execution_id = executor.execute_async(prompt, task_id=item_id)
            self._execution_ids[item_id] = execution_id
            renew_every = max(1.0, self._config.lease_seconds / 3)
            status_poll_every = min(
                self._config.poll_interval_seconds,
                renew_every,
            )
            loop = asyncio.get_running_loop()
            next_renew_at = loop.time() + renew_every
            while True:
                result = get_background_task_result(execution_id)
                if result is None:
                    raise RuntimeError("Native subagent execution disappeared")
                if result.status.is_terminal:
                    break
                now_monotonic = loop.time()
                if now_monotonic >= next_renew_at:
                    lease = await self._repository.renew_item_lease(
                        item_id,
                        attempt_id=item.get("attempt_id"),
                        lease_epoch=item.get("lease_epoch"),
                        lease_owner=self._lease_owner,
                        lease_seconds=self._config.lease_seconds,
                        now=None,
                    )
                    next_renew_at = loop.time() + renew_every
                    if not lease["valid"]:
                        request_cancel_background_task(execution_id)
                try:
                    until_renew = max(0.0, next_renew_at - loop.time())
                    await asyncio.wait_for(
                        self._stop.wait(),
                        timeout=min(status_poll_every, until_renew),
                    )
                    if self._stop.is_set():
                        raise asyncio.CancelledError
                except TimeoutError:
                    pass

            raw_result = result.result or ""
            if getattr(result, "admission_failure", False):
                await self._repository.requeue_item_after_admission_failure(
                    item_id,
                    attempt_id=item.get("attempt_id"),
                    lease_epoch=item.get("lease_epoch"),
                    lease_owner=self._lease_owner,
                    error="queue_rejected",
                    now=None,
                )
                return
            result_limit = acceptance.limits.max_result_chars
            truncated = len(raw_result) > result_limit
            stored_result = raw_result[:result_limit] if raw_result and not truncated else None
            preview = raw_result[: self._config.result_preview_max_chars] if raw_result else None
            succeeded = result.status is SubagentStatus.COMPLETED and not truncated
            terminal_code = "result_too_large" if truncated else "cancelled" if getattr(result.status, "value", None) == "cancelled" else None
            await self._repository.finalize_item(
                item_id,
                attempt_id=item.get("attempt_id"),
                lease_epoch=item.get("lease_epoch"),
                lease_owner=self._lease_owner,
                succeeded=succeeded,
                result=stored_result,
                result_preview=None if truncated else preview,
                result_truncated=truncated,
                error=None if succeeded else terminal_code or "execution_failed",
                stop_reason=_safe_stop_reason(result.stop_reason),
                token_usage=_usage(result.token_usage_records),
                model_name=effective_model,
                completed_at=None,
                terminal_code=terminal_code,
            )
        except asyncio.CancelledError:
            if execution_id is not None:
                request_cancel_background_task(execution_id)
            # Do not finalize on process shutdown. The durable lease expires and
            # another worker reclaims the same stable item key.
            raise
        except Exception as exc:
            reason_code = exc.code if isinstance(exc, BatchAdmissionError) else "execution_failed"
            logger.error(
                "Durable subagent batch item failed (item_id=%s reason_code=%s)",
                item_id,
                reason_code,
            )
            await self._repository.finalize_item(
                item_id,
                attempt_id=item.get("attempt_id"),
                lease_epoch=item.get("lease_epoch"),
                lease_owner=self._lease_owner,
                succeeded=False,
                result=None,
                result_preview=None,
                result_truncated=False,
                error=reason_code,
                stop_reason=None,
                token_usage=None,
                model_name=None,
                completed_at=None,
                terminal_code=(
                    exc.code
                    if isinstance(exc, BatchAdmissionError)
                    and exc.code
                    in {
                        "policy_stopped",
                        "provider_not_qualified",
                        "execution_material_unavailable",
                    }
                    else None
                ),
            )
        finally:
            self._execution_ids.pop(item_id, None)
            self._item_batches.pop(item_id, None)
            if execution_id is not None:
                cleanup_background_task(execution_id)
            if batch_id is not None and user_id is not None:
                await self._prune_terminal_material()


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) else None
