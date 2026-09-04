"""Gateway repository adapter for portable terminal-run evidence snapshots."""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from deerflow.constants import RETRIEVAL_OBSERVATION_EVENT_TYPE
from deerflow.retrieval import (
    RetrievalEvidenceError,
    RetrievalObservationV1,
    validate_retrieval_pair,
)
from deerflow.runtime.accepted_invocation import AcceptedInvocation
from deerflow.runtime.assembly_evidence import (
    ASSEMBLY_EVIDENCE_VERSION,
    AssemblyEvidenceError,
    AssemblyEvidenceV1,
    assembly_evidence_digest,
)
from deerflow.runtime.events.catalog import RUN_TERMINAL_EVENT
from deerflow.runtime.failure_evidence import (
    RuntimeFailureV1,
    TerminalSummaryV1,
)
from deerflow.runtime.run_evidence import (
    MAX_EVIDENCE_REFERENCES,
    EvidenceSectionV1,
    EvidenceSnapshotRequest,
    EvidenceSnapshotSourceV1,
    RunEvidenceBundleError,
    RunEvidenceSnapshotService,
    RunEvidenceSnapshotV1,
    canonical_json_bytes,
)
from deerflow.runtime.runs.lifecycle_query import build_tool_receipt_page
from deerflow.runtime.subagent_snapshot import SubagentCatalogError
from deerflow.runtime.tool_evidence import (
    TOOL_RECEIPT_OUTCOME_EVENT,
    TOOL_RECEIPT_STARTED_EVENT,
    ToolEvidenceError,
    parse_tool_receipt_event,
)
from deerflow.sandbox.accepted_material import (
    AcceptedExecutionEvidenceV2,
    decode_accepted_execution_evidence,
)

_EVENT_PAGE_SIZE = 2000
_MAX_EVENTS = 100_000
_EXTERNAL_PAGE_SIZE = 100
_LEAF_DOMAIN = b"hartmesh.run-evidence-bundle.safe-leaf.v1\x00"
_FENCE_DOMAIN = b"hartmesh.run-evidence-bundle.snapshot-fence.v1\x00"
_MCP_TERMINAL = frozenset({"completed", "failed", "cancelled"})
_BATCH_TERMINAL = frozenset({"completed", "failed", "cancelled"})


def _error(code: str) -> None:
    raise RunEvidenceBundleError(code)


def _safe_leaf(kind: str, value: object) -> str:
    return hashlib.sha256(_LEAF_DOMAIN + kind.encode("ascii") + b"\x00" + canonical_json_bytes(value)).hexdigest()


def _snapshot_fence(value: object) -> str:
    return hashlib.sha256(_FENCE_DOMAIN + canonical_json_bytes(value)).hexdigest()


def _as_datetime(value: object) -> datetime:
    if not isinstance(value, str) or len(value.encode("utf-8")) > 64:
        _error("evidence_cross_link_invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RunEvidenceBundleError("evidence_cross_link_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _error("evidence_cross_link_invalid")
    return parsed.astimezone(UTC)


def _validated_terminal_event(
    event: Mapping[str, object],
    *,
    status: str,
    row_stop_reason: object,
) -> tuple[TerminalSummaryV1, str]:
    if event.get("event_type") != RUN_TERMINAL_EVENT.event_type or event.get("category") != RUN_TERMINAL_EVENT.category:
        _error("evidence_cross_link_invalid")
    body = event.get("content")
    if not isinstance(body, Mapping) or set(body) != {
        "version",
        "status",
        "stop_reason",
        "failure",
    }:
        _error("evidence_cross_link_invalid")
    raw_failure = body.get("failure")
    failure = None
    if raw_failure is not None:
        if not isinstance(raw_failure, Mapping) or set(raw_failure) != {
            "version",
            "code",
            "error_class",
            "correlation_id",
        }:
            _error("evidence_cross_link_invalid")
        try:
            failure = RuntimeFailureV1(
                version=raw_failure["version"],  # type: ignore[arg-type]
                code=raw_failure["code"],  # type: ignore[arg-type]
                error_class=raw_failure["error_class"],  # type: ignore[arg-type]
                correlation_id=raw_failure["correlation_id"],  # type: ignore[arg-type]
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RunEvidenceBundleError("evidence_cross_link_invalid") from exc
        if failure.to_event_body() != dict(raw_failure):
            _error("evidence_cross_link_invalid")
    try:
        summary = TerminalSummaryV1(
            version=body["version"],  # type: ignore[arg-type]
            status=body["status"],  # type: ignore[arg-type]
            stop_reason=body["stop_reason"],  # type: ignore[arg-type]
            failure=failure,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RunEvidenceBundleError("evidence_cross_link_invalid") from exc
    if summary.to_event_body() != dict(body) or summary.status != status:
        _error("evidence_cross_link_invalid")
    if row_stop_reason is not None and row_stop_reason != summary.stop_reason:
        _error("evidence_cross_link_invalid")
    seq = event.get("seq")
    created_at = event.get("created_at")
    if type(seq) is not int or seq < 1 or not isinstance(created_at, str):
        _error("evidence_cross_link_invalid")
    digest = _safe_leaf(
        "terminal_event",
        {
            "seq": seq,
            "created_at": created_at,
            "content": summary.to_event_body(),
        },
    )
    return summary, digest


def _presented_artifacts(events: Sequence[Mapping[str, object]]) -> tuple[str, ...]:
    deliveries = [event for event in events if event.get("event_type") == "run.delivery"]
    if len(deliveries) != 1:
        _error("evidence_incomplete")
    content = deliveries[0].get("content")
    if not isinstance(content, Mapping):
        _error("evidence_cross_link_invalid")
    presented_count = content.get("presented")
    all_paths = content.get("paths")
    by_tool = content.get("by_tool")
    if type(presented_count) is not int or presented_count < 0 or not isinstance(all_paths, list) or len(all_paths) != presented_count or any(not isinstance(path, str) for path in all_paths) or not isinstance(by_tool, Mapping):
        _error("evidence_cross_link_invalid")
    raw_presented = by_tool.get("present_files", [])
    if not isinstance(raw_presented, list) or any(not isinstance(path, str) for path in raw_presented):
        _error("evidence_cross_link_invalid")
    paths = tuple(dict.fromkeys(raw_presented))
    if any(path not in all_paths for path in paths):
        _error("evidence_cross_link_invalid")
    return paths


def _lifecycle_counts(events: Sequence[Mapping[str, object]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for event in events:
        event_type = event.get("event_type")
        category = event.get("category")
        if event_type == RETRIEVAL_OBSERVATION_EVENT_TYPE:
            counts["retrieval"] += 1
        elif event_type in {TOOL_RECEIPT_STARTED_EVENT, TOOL_RECEIPT_OUTCOME_EVENT}:
            counts["tools"] += 1
        elif event_type == "run.delivery" or category in {"outputs", "workspace"}:
            counts["artifacts"] += 1
        elif isinstance(event_type, str) and (event_type.startswith("run.") or event_type in {"run.start", "run.end"}):
            counts["lifecycle"] += 1
        else:
            counts["other"] += 1
    return dict(counts)


def _receipt_references(
    events: Sequence[Mapping[str, object]],
    *,
    request: EvidenceSnapshotRequest,
    accepted: AcceptedInvocation,
    assembly: AssemblyEvidenceV1,
) -> tuple[tuple[str, ...], dict[str, Mapping[str, object]]]:
    receipt_events = [dict(event) for event in events if event.get("event_type") in {TOOL_RECEIPT_STARTED_EVENT, TOOL_RECEIPT_OUTCOME_EVENT}]
    references: list[str] = []
    outcomes: dict[str, Mapping[str, object]] = {}
    cursor = None
    while True:
        page = build_tool_receipt_page(
            receipt_events,
            run_id=request.run_id,
            thread_id=request.thread_id,
            cursor=cursor,
            limit=100,
            legacy_unavailable=False,
        )
        if page.evidence_status != "available" or page.invalid_event_count:
            _error("evidence_cross_link_invalid")
        for item in page.items:
            if item.status == "indeterminate":
                _error("evidence_incomplete")
            if (
                item.agent_revision_digest != accepted.agent_revision.digest
                or item.assembly_fingerprint != assembly.fingerprint
                or accepted.agent_revision.subagent_catalog is None
                or item.subagent_catalog_digest != accepted.agent_revision.subagent_catalog.digest
                or item.capability_manifest_digest != accepted.extension_manifest_digest
                or item.artifact_manifest_digest != accepted.extension_artifact_manifest_digest
                or item.extension_configuration_digest != accepted.extension_configuration_digest
            ):
                _error("evidence_cross_link_invalid")
            references.append(_safe_leaf("tool_receipt", item.to_dict()))
        if len(references) > MAX_EVIDENCE_REFERENCES:
            _error("bundle_limit_exceeded")
        if page.next_cursor is None:
            break
        if page.next_cursor == cursor:
            _error("evidence_cross_link_invalid")
        cursor = page.next_cursor

    for event in receipt_events:
        try:
            receipt = parse_tool_receipt_event(event).receipt
        except ToolEvidenceError as exc:
            raise RunEvidenceBundleError("evidence_cross_link_invalid") from exc
        if receipt.phase != "started":
            if receipt.receipt_id in outcomes:
                _error("evidence_cross_link_invalid")
            outcomes[receipt.receipt_id] = receipt.to_event_body()
    return tuple(references), outcomes


def _retrieval_references(
    events: Sequence[Mapping[str, object]],
    *,
    request: EvidenceSnapshotRequest,
    receipt_outcomes: Mapping[str, Mapping[str, object]],
) -> tuple[str, ...]:
    references: list[str] = []
    receipt_ids: set[str] = set()
    for event in events:
        if event.get("event_type") != RETRIEVAL_OBSERVATION_EVENT_TYPE:
            continue
        body = event.get("content")
        if not isinstance(body, Mapping):
            _error("evidence_cross_link_invalid")
        try:
            observation = RetrievalObservationV1.from_event_body(body)
            receipt_body = receipt_outcomes.get(observation.receipt_id)
            if receipt_body is None:
                _error("evidence_cross_link_invalid")
            validate_retrieval_pair(receipt_body, body)
        except RetrievalEvidenceError as exc:
            raise RunEvidenceBundleError("evidence_cross_link_invalid") from exc
        if observation.draft.run_id != request.run_id or observation.draft.tenant_ref != request.tenant.public_ref or observation.draft.tenant_digest != request.tenant.digest or observation.receipt_id in receipt_ids:
            _error("evidence_cross_link_invalid")
        receipt_ids.add(observation.receipt_id)
        references.append(observation.observation_digest)
    if len(references) > MAX_EVIDENCE_REFERENCES:
        _error("bundle_limit_exceeded")
    return tuple(references)


class GatewayRunEvidenceSnapshotReader:
    """Read current Gateway stores behind a terminal row/event high-water fence."""

    def __init__(
        self,
        *,
        run_store: Any,
        event_store: Any,
        mcp_task_repo: Any | None = None,
        subagent_batch_repo: Any | None = None,
    ) -> None:
        self._run_store = run_store
        self._event_store = event_store
        self._mcp_task_repo = mcp_task_repo
        self._subagent_batch_repo = subagent_batch_repo
        self._fences: dict[int, dict[str, object]] = {}

    async def _events(self, request: EvidenceSnapshotRequest) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        after_seq = None
        while True:
            page = await self._event_store.list_events(
                request.thread_id,
                request.run_id,
                limit=_EVENT_PAGE_SIZE,
                after_seq=after_seq,
                user_id=request.owner_id,
            )
            if not isinstance(page, list):
                _error("evidence_cross_link_invalid")
            if not page:
                break
            if any(not isinstance(event, Mapping) or event.get("thread_id") != request.thread_id or event.get("run_id") != request.run_id for event in page):
                _error("evidence_cross_link_invalid")
            seqs = [event.get("seq") for event in page]
            if any(type(seq) is not int or seq < 1 for seq in seqs):
                _error("evidence_cross_link_invalid")
            if seqs != sorted(seqs) or len(set(seqs)) != len(seqs):
                _error("evidence_cross_link_invalid")
            if after_seq is not None and seqs[0] <= after_seq:
                _error("evidence_cross_link_invalid")
            events.extend(page)
            if len(events) > _MAX_EVENTS:
                _error("bundle_limit_exceeded")
            if len(page) < _EVENT_PAGE_SIZE:
                break
            after_seq = seqs[-1]
        if not events:
            _error("evidence_incomplete")
        return events

    async def _mcp_items(
        self,
        request: EvidenceSnapshotRequest,
        receipt_outcomes: Mapping[str, Mapping[str, object]],
    ) -> tuple[str, ...]:
        if self._mcp_task_repo is None:
            return ()
        references: list[str] = []
        cursor = None
        while True:
            page = await self._mcp_task_repo.list_by_parent_run(
                request.run_id,
                user_id=request.owner_id,
                limit=_EXTERNAL_PAGE_SIZE,
                cursor=cursor,
                tenant_digest=request.tenant.digest,
            )
            if not isinstance(page, Mapping) or page.get("pruning_status") != "not_pruned":
                _error("evidence_pruned")
            items = page.get("items")
            if not isinstance(items, list):
                _error("evidence_cross_link_invalid")
            for item in items:
                if not isinstance(item, Mapping):
                    _error("evidence_cross_link_invalid")
                receipt_id = item.get("receipt_id")
                if item.get("status") not in _MCP_TERMINAL or not isinstance(receipt_id, str) or receipt_id not in receipt_outcomes:
                    _error("evidence_incomplete")
                projection = {
                    "lineage_digest": item.get("lineage_digest"),
                    "receipt_id": receipt_id,
                    "status": item.get("status"),
                    "safe_terminal_code": item.get("safe_terminal_code"),
                    "completed_at": item.get("completed_at"),
                }
                references.append(_safe_leaf("mcp_task", projection))
            if len(references) > MAX_EVIDENCE_REFERENCES:
                _error("bundle_limit_exceeded")
            next_cursor = page.get("next_cursor")
            if next_cursor is None:
                break
            if not isinstance(next_cursor, str) or next_cursor == cursor:
                _error("evidence_cross_link_invalid")
            cursor = next_cursor
        return tuple(references)

    async def _batch_items(
        self,
        request: EvidenceSnapshotRequest,
        *,
        accepted: AcceptedInvocation,
        assembly: AssemblyEvidenceV1,
        receipt_outcomes: Mapping[str, Mapping[str, object]],
    ) -> tuple[str, ...]:
        if self._subagent_batch_repo is None:
            return ()
        references: list[str] = []
        cursor = None
        while True:
            page = await self._subagent_batch_repo.list_lifecycle_by_parent_run(
                request.run_id,
                user_id=request.owner_id,
                limit=_EXTERNAL_PAGE_SIZE,
                cursor=cursor,
                tenant_digest=request.tenant.digest,
            )
            if not isinstance(page, Mapping) or page.get("pruning_status") != "not_pruned":
                _error("evidence_pruned")
            items = page.get("items")
            if not isinstance(items, list):
                _error("evidence_cross_link_invalid")
            for item in items:
                if not isinstance(item, Mapping) or item.get("status") not in _BATCH_TERMINAL:
                    _error("evidence_incomplete")
                batch_id = item.get("batch_id")
                if not isinstance(batch_id, str):
                    _error("evidence_cross_link_invalid")
                batch = await self._subagent_batch_repo.get_batch(
                    batch_id,
                    user_id=request.owner_id,
                )
                evidence = batch.get("evidence") if isinstance(batch, Mapping) else None
                parent_receipt = evidence.get("parent_tool_receipt_id") if isinstance(evidence, Mapping) else None
                catalog = accepted.agent_revision.subagent_catalog
                if (
                    not isinstance(evidence, Mapping)
                    or batch.get("compatibility_state") != "accepted_v1"
                    or evidence.get("tenant_ref") != request.tenant.public_ref
                    or evidence.get("tenant_digest") != request.tenant.digest
                    or evidence.get("parent_run_id") != request.run_id
                    or evidence.get("parent_invocation_digest") != accepted.runtime_identity_digest
                    or evidence.get("parent_assembly_fingerprint") != assembly.fingerprint
                    or catalog is None
                    or evidence.get("subagent_catalog_digest") != catalog.digest
                    or not isinstance(parent_receipt, str)
                    or parent_receipt not in receipt_outcomes
                ):
                    _error("evidence_cross_link_invalid")
                observations = item.get("observations")
                if not isinstance(observations, list):
                    _error("evidence_cross_link_invalid")
                observation_refs = [
                    _safe_leaf(
                        "batch_observation",
                        {
                            "event": observation.get("event"),
                            "transition": observation.get("transition"),
                            "state": observation.get("state"),
                            "terminal_code": observation.get("terminal_code"),
                            "consumed": observation.get("consumed"),
                            "evidence_digest": observation.get("evidence_digest"),
                            "occurred_at": observation.get("occurred_at"),
                        },
                    )
                    for observation in observations
                    if isinstance(observation, Mapping)
                ]
                if len(observation_refs) != len(observations):
                    _error("evidence_cross_link_invalid")
                references.append(
                    _safe_leaf(
                        "subagent_batch",
                        {
                            "acceptance_digest": item.get("acceptance_digest"),
                            "parent_tool_receipt_id": parent_receipt,
                            "status": item.get("status"),
                            "terminal_code": item.get("terminal_code"),
                            "total_items": item.get("total_items"),
                            "observation_references": sorted(observation_refs),
                        },
                    )
                )
            if len(references) > MAX_EVIDENCE_REFERENCES:
                _error("bundle_limit_exceeded")
            next_cursor = page.get("next_cursor")
            if next_cursor is None:
                break
            if not isinstance(next_cursor, str) or next_cursor == cursor:
                _error("evidence_cross_link_invalid")
            cursor = next_cursor
        return tuple(references)

    async def read(self, request: EvidenceSnapshotRequest) -> EvidenceSnapshotSourceV1:
        row = await self._run_store.get(request.run_id, user_id=request.owner_id)
        if not isinstance(row, Mapping) or row.get("thread_id") != request.thread_id or row.get("operation_kind", "run") != "run":
            _error("evidence_incomplete")
        status = row.get("status")
        if status in {"pending", "running"}:
            _error("run_not_terminal")
        if not isinstance(status, str):
            _error("evidence_cross_link_invalid")
        try:
            accepted = AcceptedInvocation.from_persisted(row)
        except (SubagentCatalogError, TypeError, ValueError) as exc:
            raise RunEvidenceBundleError("evidence_legacy_unbound") from exc
        if accepted is None or accepted.tenant != request.tenant:
            _error("evidence_legacy_unbound")
        raw_assembly = row.get("assembly_evidence_json")
        persisted_assembly_digest = row.get("assembly_evidence_digest")
        try:
            if not isinstance(raw_assembly, Mapping):
                _error("evidence_legacy_unbound")
            assembly = AssemblyEvidenceV1.from_persisted_json(raw_assembly)
            if (
                assembly.version != ASSEMBLY_EVIDENCE_VERSION
                or persisted_assembly_digest != assembly_evidence_digest(assembly)
                or assembly.tenant != request.tenant
                or assembly.accepted_agent_revision_digest != accepted.agent_revision.digest
            ):
                _error("evidence_cross_link_invalid")
        except RunEvidenceBundleError:
            raise
        except (AssemblyEvidenceError, TypeError, ValueError) as exc:
            raise RunEvidenceBundleError("evidence_legacy_unbound") from exc

        events = await self._events(request)
        high_water = events[-1].get("seq")
        if type(high_water) is not int:
            _error("evidence_cross_link_invalid")
        terminal_events = [event for event in events if event.get("event_type") == RUN_TERMINAL_EVENT.event_type]
        if len(terminal_events) != 1:
            _error("evidence_incomplete")
        terminal, terminal_digest = _validated_terminal_event(
            terminal_events[0],
            status=status,
            row_stop_reason=row.get("stop_reason"),
        )
        artifact_paths = _presented_artifacts(events)

        catalog = accepted.agent_revision.subagent_catalog
        scopes = accepted.agent_revision.skill_scopes
        trusted = accepted.trusted_context
        credential = None if trusted is None else trusted.credential
        tool_plane = accepted.tool_plane_revision
        if catalog is None or scopes is None or credential is None or tool_plane is None:
            _error("evidence_legacy_unbound")
        if accepted.tool_receipt_evidence_version != 2:
            _error("evidence_legacy_unbound")
        extension_evidence = (
            accepted.extension_manifest_digest,
            accepted.extension_artifact_manifest_digest,
            accepted.extension_configuration_digest,
        )
        if any(item is not None for item in extension_evidence) and any(item is None for item in extension_evidence):
            _error("evidence_legacy_unbound")

        receipt_refs, receipt_outcomes = _receipt_references(
            events,
            request=request,
            accepted=accepted,
            assembly=assembly,
        )
        retrieval_refs = _retrieval_references(
            events,
            request=request,
            receipt_outcomes=receipt_outcomes,
        )
        mcp_refs = await self._mcp_items(request, receipt_outcomes)
        batch_refs = await self._batch_items(
            request,
            accepted=accepted,
            assembly=assembly,
            receipt_outcomes=receipt_outcomes,
        )

        sections: list[EvidenceSectionV1] = [
            EvidenceSectionV1.complete("accepted_invocation", (accepted.runtime_identity_digest,)),
            EvidenceSectionV1.complete("actor_credential", (credential.digest,)),
            EvidenceSectionV1.complete("assembly", (persisted_assembly_digest,)),
            EvidenceSectionV1.complete("subagent_catalog", (catalog.digest,)),
            EvidenceSectionV1.complete("skill_material", (scopes.digest,)),
            (
                EvidenceSectionV1.complete(
                    "extension_material",
                    (
                        accepted.extension_manifest_digest,
                        accepted.extension_artifact_manifest_digest.removeprefix("sha256:"),
                        accepted.extension_configuration_digest.removeprefix("sha256:"),
                    ),
                )
                if accepted.extension_manifest_digest is not None and accepted.extension_artifact_manifest_digest is not None and accepted.extension_configuration_digest is not None
                else EvidenceSectionV1.absent_by_design("extension_material")
            ),
            EvidenceSectionV1.complete("tool_plane", (tool_plane["effective_digest"],)),
            EvidenceSectionV1.complete("lifecycle", (terminal_digest,)),
            EvidenceSectionV1.complete("tool_receipts", receipt_refs),
            (EvidenceSectionV1.complete("mcp_tasks", mcp_refs) if mcp_refs else EvidenceSectionV1.absent_by_design("mcp_tasks")),
            (EvidenceSectionV1.complete("subagent_batches", batch_refs) if batch_refs else EvidenceSectionV1.absent_by_design("subagent_batches")),
        ]

        raw_execution = row.get("execution_evidence_json")
        sandbox: AcceptedExecutionEvidenceV2 | None = None
        if raw_execution is None and row.get("execution_evidence_digest") is None:
            sections.append(EvidenceSectionV1.absent_by_design("sandbox_execution"))
        else:
            try:
                decoded = decode_accepted_execution_evidence(raw_execution)
            except (TypeError, ValueError) as exc:
                raise RunEvidenceBundleError("evidence_legacy_unbound") from exc
            if not isinstance(decoded, AcceptedExecutionEvidenceV2):
                _error("evidence_legacy_unbound")
            sandbox = decoded
            if (
                row.get("execution_evidence_digest") != sandbox.digest
                or sandbox.run_id != request.run_id
                or sandbox.tenant != request.tenant
                or sandbox.accepted_invocation_digest != accepted.runtime_identity_digest
                or sandbox.tool_plane_effective_digest != tool_plane["effective_digest"]
            ):
                _error("evidence_cross_link_invalid")
            sections.append(
                EvidenceSectionV1.complete(
                    "sandbox_execution",
                    (
                        sandbox.digest,
                        sandbox.provider_resource_commitment,
                        sandbox.runtime_image_digest,
                        sandbox.skill_snapshot_digest,
                        sandbox.skill_scope_digest,
                        sandbox.materialization_digest,
                        sandbox.read_only_proof_digest,
                    ),
                )
            )
        sections.append(EvidenceSectionV1.complete("retrieval_observations", retrieval_refs) if retrieval_refs else EvidenceSectionV1.absent_by_design("retrieval_observations"))
        sections.append(EvidenceSectionV1.complete("qualification", (sandbox.qualification_evidence_digest,)) if sandbox is not None else EvidenceSectionV1.unqualified("qualification"))

        accepted_version = trusted.to_persisted_json().get("version") if trusted is not None else None
        if type(accepted_version) is not int:
            _error("evidence_legacy_unbound")
        counts = _lifecycle_counts(events)
        source = EvidenceSnapshotSourceV1(
            tenant=request.tenant,
            thread_id=request.thread_id,
            run_id=request.run_id,
            terminal_status=status,
            safe_stop_reason=terminal.stop_reason or status,
            accepted_at=_as_datetime(row.get("created_at")),
            completed_at=_as_datetime(terminal_events[0].get("created_at")),
            accepted_invocation_digest=accepted.runtime_identity_digest,
            accepted_invocation_version=accepted_version,
            accepted_context_digest=accepted.accepted_context_digest,
            agent_revision_digest=accepted.agent_revision.digest,
            assembly_evidence_digest=persisted_assembly_digest,
            assembly_fingerprint=assembly.fingerprint,
            lifecycle_high_water_mark=high_water,
            terminal_event_digest=terminal_digest,
            lifecycle_event_count=len(events),
            lifecycle_counts=counts,
            sections=tuple(sections),
            artifact_paths=artifact_paths,
            subagent_catalog_digest=catalog.digest,
            subagent_catalog_entry_count=len(catalog.entries),
            skill_scopes_digest=scopes.digest,
            skill_scope_count=len(scopes.scopes),
            capability_manifest_digest=accepted.extension_manifest_digest,
            extension_artifact_manifest_digest=(accepted.extension_artifact_manifest_digest),
            extension_configuration_digest=(accepted.extension_configuration_digest),
            tool_plane_revision_digest=tool_plane["effective_digest"],
            credential_evidence_ref=credential.credential_ref,
            credential_evidence_digest=credential.digest,
        )
        self._fences[id(source)] = {
            "state_version": row.get("state_version"),
            "status": status,
            "updated_at": row.get("updated_at"),
            "high_water": high_water,
            "mcp_refs": mcp_refs,
            "batch_refs": batch_refs,
            "fence_digest": _snapshot_fence(
                {
                    "state_version": row.get("state_version"),
                    "status": status,
                    "updated_at": row.get("updated_at"),
                    "high_water": high_water,
                    "mcp_refs": mcp_refs,
                    "batch_refs": batch_refs,
                }
            ),
        }
        return source

    async def revalidate(
        self,
        request: EvidenceSnapshotRequest,
        source: EvidenceSnapshotSourceV1,
    ) -> bool:
        fence = self._fences.pop(id(source), None)
        if fence is None:
            return False
        row = await self._run_store.get(request.run_id, user_id=request.owner_id)
        if not isinstance(row, Mapping):
            return False
        if any(row.get(key) != fence[key] for key in ("state_version", "status", "updated_at")):
            return False
        tail = await self._event_store.list_events(
            request.thread_id,
            request.run_id,
            limit=1,
            after_seq=fence["high_water"],
            user_id=request.owner_id,
        )
        if tail:
            return False
        # External tasks and batches were required to be terminal in ``read``;
        # their rows can no longer make a valid transition.  Re-read their
        # bounded pages to detect storage corruption or an illegal rewrite.
        receipt_outcomes: dict[str, Mapping[str, object]] = {}
        events = await self._event_store.list_events(
            request.thread_id,
            request.run_id,
            event_types=[TOOL_RECEIPT_OUTCOME_EVENT],
            limit=MAX_EVIDENCE_REFERENCES,
            user_id=request.owner_id,
        )
        for event in events:
            try:
                receipt = parse_tool_receipt_event(event).receipt
            except ToolEvidenceError:
                return False
            receipt_outcomes[receipt.receipt_id] = receipt.to_event_body()
        try:
            mcp_refs = await self._mcp_items(request, receipt_outcomes)
            # Batch cross-links need accepted/assembly objects, already strictly
            # recoverable from the same unchanged run row.
            accepted = AcceptedInvocation.from_persisted(row)
            raw_assembly = row.get("assembly_evidence_json")
            if accepted is None or not isinstance(raw_assembly, Mapping):
                return False
            assembly = AssemblyEvidenceV1.from_persisted_json(raw_assembly)
            batch_refs = await self._batch_items(
                request,
                accepted=accepted,
                assembly=assembly,
                receipt_outcomes=receipt_outcomes,
            )
        except (
            AssemblyEvidenceError,
            RunEvidenceBundleError,
            SubagentCatalogError,
            TypeError,
            ValueError,
        ):
            return False
        current_digest = _snapshot_fence(
            {
                "state_version": row.get("state_version"),
                "status": row.get("status"),
                "updated_at": row.get("updated_at"),
                "high_water": fence["high_water"],
                "mcp_refs": mcp_refs,
                "batch_refs": batch_refs,
            }
        )
        return current_digest == fence["fence_digest"]


async def build_gateway_run_evidence_snapshot(
    *,
    request: EvidenceSnapshotRequest,
    run_store: Any,
    event_store: Any,
    mcp_task_repo: Any | None = None,
    subagent_batch_repo: Any | None = None,
) -> RunEvidenceSnapshotV1:
    reader = GatewayRunEvidenceSnapshotReader(
        run_store=run_store,
        event_store=event_store,
        mcp_task_repo=mcp_task_repo,
        subagent_batch_repo=subagent_batch_repo,
    )
    return await RunEvidenceSnapshotService(reader).build(request)


__all__ = [
    "GatewayRunEvidenceSnapshotReader",
    "build_gateway_run_evidence_snapshot",
]
