"""Immutable, tenant-bound lineage for durable MCP tasks.

This is the only module that constructs or decodes persisted MCP task lineage.
Callers provide trusted, typed execution facts and a Project 03 safe request
projection; persisted records contain commitments and safe references only.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, Self

from deerflow_extension_api import InvocationIdentityV1, TenantReferenceV1

LINEAGE_VERSION = 1
MAX_REQUEST_PROJECTION_BYTES = 8 * 1024
MAX_LINEAGE_BYTES = 16 * 1024

_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_PRINCIPAL_REF_RE = re.compile(r"^principal-[0-9a-f]{24}$", re.ASCII)
_RECEIPT_ID_RE = re.compile(r"^tr_[0-9a-f]{64}$", re.ASCII)
_TOOL_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$", re.ASCII)
_JSON_TYPES = frozenset({"null", "boolean", "integer", "number", "string", "object", "array"})

McpTaskLineageKind = Literal["agent_tool", "standalone_api", "internal"]
McpTaskParentExecutionKind = Literal["lead", "subagent"]


class McpTaskLineageError(ValueError):
    """A stable, non-sensitive lineage validation failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _invalid() -> None:
    raise McpTaskLineageError("mcp_task_lineage_invalid")


def _json_value(value: object, *, depth: int = 0) -> object:
    if depth > 16:
        _invalid()
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            _invalid()
        return value
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            _invalid()
        return {str(key): _json_value(item, depth=depth + 1) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item, depth=depth + 1) for item in value]
    _invalid()


def _canonical_json_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            _json_value(value),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise McpTaskLineageError("mcp_task_lineage_invalid") from exc


def _canonical_digest(value: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _validate_projected_argument(value: object) -> None:
    """Require the structural projection emitted by Project 03."""

    if not isinstance(value, Mapping):
        _invalid()
    classification = value.get("classification")
    value_type = value.get("type")
    if classification not in {"secret_handle", "evidence_safe", "shape"}:
        _invalid()
    if value_type not in _JSON_TYPES:
        _invalid()
    keys = set(value)
    if classification == "secret_handle":
        if keys != {"classification", "type"}:
            _invalid()
        return
    if classification == "evidence_safe":
        if keys != {"classification", "type", "value"}:
            _invalid()
        safe_value = value.get("value")
        if safe_value is not None and not isinstance(safe_value, (str, bool, int, float)):
            _invalid()
        _canonical_json_bytes(safe_value)
        return
    if value_type == "object":
        if keys != {"classification", "type", "fields"}:
            _invalid()
        fields = value.get("fields")
        if not isinstance(fields, Mapping):
            _invalid()
        for field_name, child in fields.items():
            _require_text(field_name, max_bytes=128)
            _validate_projected_argument(child)
        return
    if value_type == "array":
        if keys != {"classification", "type", "length", "items"}:
            _invalid()
        length = value.get("length")
        items = value.get("items")
        if type(length) is not int or length < 0 or not isinstance(items, (list, tuple)) or len(items) != length:
            _invalid()
        for child in items:
            _validate_projected_argument(child)
        return
    expected = {"classification", "type"}
    if value_type == "string":
        expected.add("utf8_bytes")
        if type(value.get("utf8_bytes")) is not int or value["utf8_bytes"] < 0:
            _invalid()
    if keys != expected:
        _invalid()


def _validate_safe_request_projection(
    value: Mapping[str, object],
    *,
    expected_server_name: str,
    expected_tool_name: str,
) -> bytes:
    if not isinstance(value, Mapping):
        _invalid()
    keys = set(value)
    if not {"version", "tool_name", "arguments"}.issubset(keys) or keys - {
        "version",
        "tool_name",
        "arguments",
        "task_mode",
    }:
        _invalid()
    if value.get("version") != 1 or value.get("tool_name") != expected_tool_name:
        _invalid()
    arguments = value.get("arguments")
    if not isinstance(arguments, Mapping):
        _invalid()
    for field_name, child in arguments.items():
        _require_text(field_name, max_bytes=128)
        _validate_projected_argument(child)
    task_mode = value.get("task_mode")
    if task_mode is not None:
        if not isinstance(task_mode, Mapping) or set(task_mode) != {
            "notification_requested",
            "task_name",
        }:
            _invalid()
        if type(task_mode.get("notification_requested")) is not bool:
            _invalid()
        _require_text(task_mode.get("task_name"), max_bytes=255)
    _require_text(expected_server_name, max_bytes=128)
    encoded = _canonical_json_bytes(
        {
            **value,
            "mcp_server_name": expected_server_name,
        }
    )
    if len(encoded) > MAX_REQUEST_PROJECTION_BYTES:
        _invalid()
    return encoded


def _require_text(
    value: object,
    *,
    max_bytes: int,
    optional: bool = False,
) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > max_bytes or any(ord(character) < 32 or ord(character) == 127 for character in value):
        _invalid()
    return value


def _require_digest(value: object, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        _invalid()
    return value


def derive_principal_ref(
    *,
    tenant: TenantReferenceV1,
    identity: InvocationIdentityV1,
) -> str:
    """Derive a bounded pseudonymous reference from authenticated identity."""

    if not isinstance(tenant, TenantReferenceV1) or not isinstance(
        identity,
        InvocationIdentityV1,
    ):
        raise McpTaskLineageError("mcp_task_lineage_unavailable")
    digest = _canonical_digest(
        {
            "version": 1,
            "tenant_digest": tenant.digest,
            "principal_identity": identity.to_json(),
        }
    )
    return f"principal-{digest[:24]}"


@dataclass(frozen=True, slots=True)
class CredentialSelector:
    """Non-secret configured credential-binding identity."""

    binding_id: str
    version: int

    def __post_init__(self) -> None:
        _require_text(self.binding_id, max_bytes=256)
        if type(self.version) is not int or self.version < 1:
            _invalid()

    def safe_reference(
        self,
        *,
        tenant_digest: str,
        principal_ref: str,
        server_name: str,
    ) -> str:
        _require_digest(tenant_digest)
        if _PRINCIPAL_REF_RE.fullmatch(principal_ref) is None:
            _invalid()
        _require_text(server_name, max_bytes=128)
        return _canonical_digest(
            {
                "version": 1,
                "tenant_digest": tenant_digest,
                "principal_ref": principal_ref,
                "mcp_server_name": server_name,
                "credential_binding_id": self.binding_id,
                "credential_version": self.version,
            }
        )


def configured_credential_selector(
    server_name: str,
    server_config: object,
) -> CredentialSelector | None:
    """Resolve only the non-secret identity of a configured credential path."""

    _require_text(server_name, max_bytes=128)
    explicit = getattr(server_config, "credential_binding_id", None)
    user_auth = getattr(server_config, "user_auth", None)
    oauth = getattr(server_config, "oauth", None)
    if explicit is not None:
        binding_id = explicit
    else:
        mechanisms: list[str] = []
        if bool(getattr(server_config, "headers", None)):
            mechanisms.append("static-http")
        if oauth is not None and bool(getattr(oauth, "enabled", True)):
            mechanisms.append("oauth")
        if user_auth is not None and bool(getattr(user_auth, "enabled", True)) and getattr(server_config, "type", None) in ("http", "sse"):
            mechanisms.append("user-auth")
        if not mechanisms:
            return None
        binding_id = f"auto:{'+'.join(mechanisms)}:{server_name}"
    version = getattr(server_config, "credential_version", 1)
    try:
        return CredentialSelector(binding_id=binding_id, version=version)
    except (TypeError, ValueError) as exc:
        raise McpTaskLineageError("mcp_task_credential_binding_unavailable") from exc


def require_current_credential_selector(
    lineage: McpTaskLineageV1,
    server_config: object,
) -> None:
    """Fail recovery when configured binding identity/version has drifted."""

    if not isinstance(lineage, McpTaskLineageV1):
        raise McpTaskLineageError("mcp_task_credential_binding_unavailable")
    selector = configured_credential_selector(
        lineage.mcp_server_name,
        server_config,
    )
    expected = (
        None
        if selector is None
        else selector.safe_reference(
            tenant_digest=lineage.tenant.digest,
            principal_ref=lineage.principal_ref,
            server_name=lineage.mcp_server_name,
        )
    )
    expected_version = None if selector is None else selector.version
    if expected != lineage.credential_selector_ref or expected_version != lineage.credential_selector_version:
        raise McpTaskLineageError("mcp_task_credential_binding_unavailable")


@dataclass(frozen=True, slots=True)
class TrustedMcpSubmissionContext:
    """Server-owned facts required while an Agent tool receipt is active."""

    tenant: TenantReferenceV1
    principal_identity: InvocationIdentityV1
    parent_run_id: str
    parent_execution_task_id: str
    parent_execution_kind: McpTaskParentExecutionKind
    parent_subagent_name: str | None
    parent_tool_receipt_id: str
    agent_revision_digest: str
    assembly_fingerprint: str
    subagent_catalog_digest: str
    subagent_definition_digest: str | None
    extension_generation: int
    extension_manifest_digest: str
    accepted_origin_digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.tenant, TenantReferenceV1) or not isinstance(
            self.principal_identity,
            InvocationIdentityV1,
        ):
            raise McpTaskLineageError("mcp_task_lineage_unavailable")
        _require_text(self.parent_run_id, max_bytes=64)
        _require_text(self.parent_execution_task_id, max_bytes=128)
        if self.parent_execution_kind not in ("lead", "subagent"):
            raise McpTaskLineageError("mcp_task_lineage_unavailable")
        if not isinstance(self.parent_tool_receipt_id, str) or _RECEIPT_ID_RE.fullmatch(self.parent_tool_receipt_id) is None:
            raise McpTaskLineageError("mcp_task_lineage_unavailable")
        for value in (
            self.agent_revision_digest,
            self.assembly_fingerprint,
            self.subagent_catalog_digest,
            self.extension_manifest_digest,
            self.accepted_origin_digest,
        ):
            try:
                _require_digest(value)
            except McpTaskLineageError as exc:
                raise McpTaskLineageError("mcp_task_lineage_unavailable") from exc
        if type(self.extension_generation) is not int or self.extension_generation < 0:
            raise McpTaskLineageError("mcp_task_lineage_unavailable")
        if self.parent_execution_kind == "lead":
            if self.parent_subagent_name is not None or self.subagent_definition_digest is not None:
                raise McpTaskLineageError("mcp_task_lineage_unavailable")
        else:
            try:
                _require_text(self.parent_subagent_name, max_bytes=128)
                _require_digest(self.subagent_definition_digest)
            except McpTaskLineageError as exc:
                raise McpTaskLineageError("mcp_task_lineage_unavailable") from exc

    @classmethod
    def from_runtime_context(
        cls,
        context: object,
        *,
        expected_tool_name: str,
    ) -> TrustedMcpSubmissionContext:
        """Bind accepted invocation facts to the currently started receipt.

        Both inputs are installed by the host.  Missing, stale, or mutually
        inconsistent anchors fail closed before a durable MCP driver can make
        its remote submit call.
        """

        from deerflow.extensions.mcp import mcp_invocation_facts_from_context
        from deerflow.runtime.tool_evidence import get_active_tool_receipt

        facts = mcp_invocation_facts_from_context(context)
        receipt = get_active_tool_receipt()
        trusted = facts.trusted_context if facts is not None else None
        receipt_context = receipt.context if receipt is not None else None
        if (
            facts is None
            or trusted is None
            or receipt is None
            or receipt.phase != "started"
            or receipt.tool_name != expected_tool_name
            or receipt_context is None
            or trusted.run_id != facts.run_id
            or receipt_context.run_id != facts.run_id
            or trusted.thread_id != facts.thread_id
            or trusted.identity != facts.principal.identity
            or trusted.origin != facts.origin
            or trusted.tenant is None
            or receipt_context.tenant != trusted.tenant
            or trusted.agent_revision.digest != facts.agent_revision.digest
            or receipt_context.agent_revision_digest != facts.agent_revision.digest
            or trusted.extension_generation != facts.extension_generation
            or receipt_context.extension_generation != facts.extension_generation
            or trusted.extension_manifest_digest is None
            or facts.extension_manifest_digest != trusted.extension_manifest_digest
            or not facts.origin.digest
        ):
            raise McpTaskLineageError("mcp_task_lineage_unavailable")
        try:
            return cls(
                tenant=trusted.tenant,
                principal_identity=trusted.identity,
                parent_run_id=facts.run_id,
                parent_execution_task_id=receipt_context.execution_task_id,
                parent_execution_kind=receipt_context.execution_kind,
                parent_subagent_name=receipt_context.subagent_name,
                parent_tool_receipt_id=receipt.receipt_id,
                agent_revision_digest=receipt_context.agent_revision_digest,
                assembly_fingerprint=receipt_context.assembly_fingerprint,
                subagent_catalog_digest=receipt_context.subagent_catalog_digest,
                subagent_definition_digest=receipt_context.subagent_definition_digest,
                extension_generation=receipt_context.extension_generation,
                extension_manifest_digest=trusted.extension_manifest_digest,
                accepted_origin_digest=facts.origin.digest,
            )
        except (TypeError, ValueError) as exc:
            raise McpTaskLineageError("mcp_task_lineage_unavailable") from exc


@dataclass(frozen=True, slots=True)
class McpTaskLineageV1:
    """Canonical persisted commitments for one durable MCP task submission."""

    version: Literal[1]
    kind: McpTaskLineageKind
    tenant: TenantReferenceV1
    principal_ref: str
    parent_run_id: str | None
    parent_execution_task_id: str | None
    parent_execution_kind: McpTaskParentExecutionKind | None
    parent_subagent_name: str | None
    parent_tool_receipt_id: str | None
    agent_revision_digest: str | None
    assembly_fingerprint: str | None
    subagent_catalog_digest: str | None
    subagent_definition_digest: str | None
    extension_generation: int
    extension_manifest_digest: str | None
    accepted_origin_digest: str | None
    mcp_server_name: str
    mcp_tool_name: str
    request_projection_digest: str
    credential_selector_ref: str | None
    credential_selector_version: int | None
    digest: str

    def __post_init__(self) -> None:
        if self.version != LINEAGE_VERSION or self.kind not in (
            "agent_tool",
            "standalone_api",
            "internal",
        ):
            _invalid()
        if not isinstance(self.tenant, TenantReferenceV1):
            _invalid()
        if _PRINCIPAL_REF_RE.fullmatch(self.principal_ref) is None:
            _invalid()
        _require_text(self.mcp_server_name, max_bytes=128)
        if _TOOL_NAME_RE.fullmatch(self.mcp_tool_name) is None:
            _invalid()
        _require_digest(self.request_projection_digest)
        _require_digest(self.credential_selector_ref, optional=True)
        if (self.credential_selector_ref is None) != (self.credential_selector_version is None):
            _invalid()
        if self.credential_selector_version is not None and (type(self.credential_selector_version) is not int or self.credential_selector_version < 1):
            _invalid()
        if type(self.extension_generation) is not int or self.extension_generation < 0:
            _invalid()

        parent_fields = (
            self.parent_run_id,
            self.parent_execution_task_id,
            self.parent_execution_kind,
            self.parent_tool_receipt_id,
            self.agent_revision_digest,
            self.assembly_fingerprint,
            self.subagent_catalog_digest,
        )
        if self.kind == "agent_tool":
            if any(value is None for value in parent_fields):
                _invalid()
            _require_text(self.parent_run_id, max_bytes=64)
            _require_text(self.parent_execution_task_id, max_bytes=128)
            if self.parent_execution_kind not in ("lead", "subagent"):
                _invalid()
            if not isinstance(self.parent_tool_receipt_id, str) or _RECEIPT_ID_RE.fullmatch(self.parent_tool_receipt_id) is None:
                _invalid()
            for value in (
                self.agent_revision_digest,
                self.assembly_fingerprint,
                self.subagent_catalog_digest,
                self.extension_manifest_digest,
                self.accepted_origin_digest,
            ):
                _require_digest(value)
            if self.parent_execution_kind == "lead":
                if self.parent_subagent_name is not None or self.subagent_definition_digest is not None:
                    _invalid()
            else:
                _require_text(self.parent_subagent_name, max_bytes=128)
                _require_digest(self.subagent_definition_digest)
        else:
            if any(value is not None for value in parent_fields) or any(
                value is not None
                for value in (
                    self.parent_subagent_name,
                    self.subagent_definition_digest,
                )
            ):
                _invalid()
            _require_digest(self.extension_manifest_digest, optional=True)
            _require_digest(self.accepted_origin_digest, optional=True)

        expected = _canonical_digest(self._without_digest())
        if self.digest != expected:
            _invalid()
        if len(_canonical_json_bytes(self.to_persisted_json())) > MAX_LINEAGE_BYTES:
            _invalid()

    def _without_digest(self) -> dict[str, object]:
        return {
            "version": self.version,
            "kind": self.kind,
            "tenant": self.tenant.to_json(),
            "principal_ref": self.principal_ref,
            "parent_run_id": self.parent_run_id,
            "parent_execution_task_id": self.parent_execution_task_id,
            "parent_execution_kind": self.parent_execution_kind,
            "parent_subagent_name": self.parent_subagent_name,
            "parent_tool_receipt_id": self.parent_tool_receipt_id,
            "agent_revision_digest": self.agent_revision_digest,
            "assembly_fingerprint": self.assembly_fingerprint,
            "subagent_catalog_digest": self.subagent_catalog_digest,
            "subagent_definition_digest": self.subagent_definition_digest,
            "extension_generation": self.extension_generation,
            "extension_manifest_digest": self.extension_manifest_digest,
            "accepted_origin_digest": self.accepted_origin_digest,
            "mcp_server_name": self.mcp_server_name,
            "mcp_tool_name": self.mcp_tool_name,
            "request_projection_digest": self.request_projection_digest,
            "credential_selector_ref": self.credential_selector_ref,
            "credential_selector_version": self.credential_selector_version,
        }

    def to_persisted_json(self) -> dict[str, object]:
        """Return the exact JSON-safe representation stored with the task."""

        return {**self._without_digest(), "digest": self.digest}

    @classmethod
    def from_persisted_json(cls, value: Mapping[str, object]) -> Self:
        """Decode and validate an exact persisted lineage representation."""

        expected = {
            "version",
            "kind",
            "tenant",
            "principal_ref",
            "parent_run_id",
            "parent_execution_task_id",
            "parent_execution_kind",
            "parent_subagent_name",
            "parent_tool_receipt_id",
            "agent_revision_digest",
            "assembly_fingerprint",
            "subagent_catalog_digest",
            "subagent_definition_digest",
            "extension_generation",
            "extension_manifest_digest",
            "accepted_origin_digest",
            "mcp_server_name",
            "mcp_tool_name",
            "request_projection_digest",
            "credential_selector_ref",
            "credential_selector_version",
            "digest",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            _invalid()
        try:
            tenant = TenantReferenceV1.from_json(value["tenant"])
            return cls(
                version=value["version"],  # type: ignore[arg-type]
                kind=value["kind"],  # type: ignore[arg-type]
                tenant=tenant,
                principal_ref=value["principal_ref"],  # type: ignore[arg-type]
                parent_run_id=value["parent_run_id"],  # type: ignore[arg-type]
                parent_execution_task_id=value["parent_execution_task_id"],  # type: ignore[arg-type]
                parent_execution_kind=value["parent_execution_kind"],  # type: ignore[arg-type]
                parent_subagent_name=value["parent_subagent_name"],  # type: ignore[arg-type]
                parent_tool_receipt_id=value["parent_tool_receipt_id"],  # type: ignore[arg-type]
                agent_revision_digest=value["agent_revision_digest"],  # type: ignore[arg-type]
                assembly_fingerprint=value["assembly_fingerprint"],  # type: ignore[arg-type]
                subagent_catalog_digest=value["subagent_catalog_digest"],  # type: ignore[arg-type]
                subagent_definition_digest=value["subagent_definition_digest"],  # type: ignore[arg-type]
                extension_generation=value["extension_generation"],  # type: ignore[arg-type]
                extension_manifest_digest=value["extension_manifest_digest"],  # type: ignore[arg-type]
                accepted_origin_digest=value["accepted_origin_digest"],  # type: ignore[arg-type]
                mcp_server_name=value["mcp_server_name"],  # type: ignore[arg-type]
                mcp_tool_name=value["mcp_tool_name"],  # type: ignore[arg-type]
                request_projection_digest=value["request_projection_digest"],  # type: ignore[arg-type]
                credential_selector_ref=value["credential_selector_ref"],  # type: ignore[arg-type]
                credential_selector_version=value["credential_selector_version"],  # type: ignore[arg-type]
                digest=value["digest"],  # type: ignore[arg-type]
            )
        except McpTaskLineageError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise McpTaskLineageError("mcp_task_lineage_invalid") from exc


class McpTaskLineageBinder:
    """Construct lineage only from server-authenticated typed facts."""

    @staticmethod
    def _build(**values: Any) -> McpTaskLineageV1:
        digest = _canonical_digest(
            {
                **values,
                "tenant": values["tenant"].to_json(),
            }
        )
        return McpTaskLineageV1(**values, digest=digest)

    def for_agent_tool(
        self,
        *,
        trusted_runtime: TrustedMcpSubmissionContext,
        server_name: str,
        tool_name: str,
        safe_request_projection: Mapping[str, object],
        credential_selector: CredentialSelector | None,
    ) -> McpTaskLineageV1:
        """Bind an Agent submission to accepted execution and receipt facts."""

        if not isinstance(trusted_runtime, TrustedMcpSubmissionContext):
            raise McpTaskLineageError("mcp_task_lineage_unavailable")
        projection_bytes = _validate_safe_request_projection(
            safe_request_projection,
            expected_server_name=server_name,
            expected_tool_name=tool_name,
        )
        principal_ref = derive_principal_ref(
            tenant=trusted_runtime.tenant,
            identity=trusted_runtime.principal_identity,
        )
        selector_ref = (
            None
            if credential_selector is None
            else credential_selector.safe_reference(
                tenant_digest=trusted_runtime.tenant.digest,
                principal_ref=principal_ref,
                server_name=server_name,
            )
        )
        return self._build(
            version=LINEAGE_VERSION,
            kind="agent_tool",
            tenant=trusted_runtime.tenant,
            principal_ref=principal_ref,
            parent_run_id=trusted_runtime.parent_run_id,
            parent_execution_task_id=trusted_runtime.parent_execution_task_id,
            parent_execution_kind=trusted_runtime.parent_execution_kind,
            parent_subagent_name=trusted_runtime.parent_subagent_name,
            parent_tool_receipt_id=trusted_runtime.parent_tool_receipt_id,
            agent_revision_digest=trusted_runtime.agent_revision_digest,
            assembly_fingerprint=trusted_runtime.assembly_fingerprint,
            subagent_catalog_digest=trusted_runtime.subagent_catalog_digest,
            subagent_definition_digest=trusted_runtime.subagent_definition_digest,
            extension_generation=trusted_runtime.extension_generation,
            extension_manifest_digest=trusted_runtime.extension_manifest_digest,
            accepted_origin_digest=trusted_runtime.accepted_origin_digest,
            mcp_server_name=server_name,
            mcp_tool_name=tool_name,
            request_projection_digest=hashlib.sha256(projection_bytes).hexdigest(),
            credential_selector_ref=selector_ref,
            credential_selector_version=(None if credential_selector is None else credential_selector.version),
        )

    def for_standalone_api(
        self,
        *,
        tenant: TenantReferenceV1,
        principal_identity: InvocationIdentityV1,
        extension_generation: int,
        extension_manifest_digest: str | None,
        accepted_origin_digest: str | None,
        server_name: str,
        tool_name: str,
        safe_request_projection: Mapping[str, object],
        credential_selector: CredentialSelector | None,
    ) -> McpTaskLineageV1:
        """Bind a standalone submission without fabricating parent facts."""

        projection_bytes = _validate_safe_request_projection(
            safe_request_projection,
            expected_server_name=server_name,
            expected_tool_name=tool_name,
        )
        principal_ref = derive_principal_ref(
            tenant=tenant,
            identity=principal_identity,
        )
        selector_ref = (
            None
            if credential_selector is None
            else credential_selector.safe_reference(
                tenant_digest=tenant.digest,
                principal_ref=principal_ref,
                server_name=server_name,
            )
        )
        return self._build(
            version=LINEAGE_VERSION,
            kind="standalone_api",
            tenant=tenant,
            principal_ref=principal_ref,
            parent_run_id=None,
            parent_execution_task_id=None,
            parent_execution_kind=None,
            parent_subagent_name=None,
            parent_tool_receipt_id=None,
            agent_revision_digest=None,
            assembly_fingerprint=None,
            subagent_catalog_digest=None,
            subagent_definition_digest=None,
            extension_generation=extension_generation,
            extension_manifest_digest=extension_manifest_digest,
            accepted_origin_digest=accepted_origin_digest,
            mcp_server_name=server_name,
            mcp_tool_name=tool_name,
            request_projection_digest=hashlib.sha256(projection_bytes).hexdigest(),
            credential_selector_ref=selector_ref,
            credential_selector_version=(None if credential_selector is None else credential_selector.version),
        )


__all__ = [
    "CredentialSelector",
    "LINEAGE_VERSION",
    "MAX_REQUEST_PROJECTION_BYTES",
    "McpTaskLineageBinder",
    "McpTaskLineageError",
    "McpTaskLineageV1",
    "TrustedMcpSubmissionContext",
    "configured_credential_selector",
    "derive_principal_ref",
    "require_current_credential_selector",
]
