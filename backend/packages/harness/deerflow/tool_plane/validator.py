"""Deterministic validation pipeline for governed tool-plane candidates."""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from types import MappingProxyType
from typing import Literal
from urllib.parse import urlsplit

from deerflow.community.url_safety import validate_public_http_url
from deerflow.config.extensions_config import ExtensionsConfig
from deerflow.mcp.launch_policy import (
    McpStdioLaunchPolicyViolation,
    validate_mcp_stdio_launch,
)
from deerflow.skills.frontmatter import split_skill_markdown
from deerflow.skills.parser import (
    parse_allowed_tools,
    parse_required_secrets,
    parse_secrets_autonomous,
)
from deerflow.skills.review import (
    FACTS_SCHEMA_VERSION,
    LocalDirectoryReader,
    analyze_skill_package,
)
from deerflow.tool_plane.artifacts import GovernedSkillArtifactStore
from deerflow.tool_plane.contracts import (
    ToolPlaneRevisionError,
    runtime_mcp_servers_from_canonical,
)
from deerflow.tool_plane.service import (
    DeterministicToolPlaneValidator,
    ToolPlaneRevisionRecord,
    ToolPlaneValidationFindingV1,
    ToolPlaneValidationReportV1,
)

_SKILL_FIELDS = ("public_skills", "managed_integrations", "custom_skills")
_DEERFLOW_PACKAGE_ROOT = Path(__file__).parents[1]

# This is deliberately a conservative source closure. A report may become
# stale when validation gets stricter, but it must never remain promotable when
# any behavior that produced it changed. The aggregate governed-validator
# identity therefore covers structural validation, artifact verification,
# endpoint/command policy, schema parsing, SkillScan, and skill review plus the
# internal helpers those paths execute.
_VALIDATOR_SOURCE_GROUPS: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "canonicalizer": ("tool_plane/contracts.py",),
        "governed_validator": (
            "community/url_safety.py",
            "config/extensions_config.py",
            "config/tool_plane_config.py",
            "constants.py",
            "mcp/launch_policy.py",
            "skills/frontmatter.py",
            "skills/installer.py",
            "skills/package_paths.py",
            "skills/parser.py",
            "skills/review",
            "skills/skillscan",
            "tool_plane/artifacts.py",
            "tool_plane/contracts.py",
            "tool_plane/service.py",
            "tool_plane/validator.py",
            "../../extension-api/deerflow_extension_api/__init__.py",
            "../../extension-api/deerflow_extension_api/identifiers.py",
        ),
        "mcp_launch_policy": ("mcp/launch_policy.py",),
        "mcp_schema": (
            "config/extensions_config.py",
            "constants.py",
            "../../extension-api/deerflow_extension_api/__init__.py",
            "../../extension-api/deerflow_extension_api/identifiers.py",
        ),
        "skillscan": (
            "skills/package_paths.py",
            "skills/skillscan",
        ),
        "skill_review": (
            "skills/frontmatter.py",
            "skills/package_paths.py",
            "skills/parser.py",
            "skills/review",
            "skills/skillscan",
        ),
    }
)


def _source_digest(package_root: Path, *relative_roots: str) -> str:
    digest = hashlib.sha256()
    for relative_root in relative_roots:
        root = package_root / relative_root
        if root.is_file():
            paths = [root]
        elif root.is_dir():
            paths = sorted(root.rglob("*.py"))
        else:
            raise RuntimeError(f"validator source root is missing: {relative_root}")
        if not paths:
            raise RuntimeError(f"validator source root is empty: {relative_root}")
        for path in paths:
            relative = (Path(relative_root) if root.is_file() else Path(relative_root) / path.relative_to(root)).as_posix()
            digest.update(relative.encode("utf-8"))
            digest.update(b"\0")
            digest.update(path.read_bytes())
            digest.update(b"\0")
    return digest.hexdigest()


def _validator_source_versions(
    *,
    package_root: Path = _DEERFLOW_PACKAGE_ROOT,
    source_groups: Mapping[str, Sequence[str]] = _VALIDATOR_SOURCE_GROUPS,
) -> dict[str, str]:
    """Build exact source identities for a declared validator closure."""

    return {component: f"sha256:{_source_digest(package_root, *relative_roots)}" for component, relative_roots in sorted(source_groups.items())}


# Source discovery is intentionally import-time work. Async validation,
# compatibility preflight, and promotion read this immutable build manifest;
# they never traverse or read the filesystem merely to compare validator IDs.
_VALIDATOR_VERSIONS: Mapping[str, str] = MappingProxyType(
    {
        **_validator_source_versions(),
        "skill_review_schema": str(FACTS_SCHEMA_VERSION),
    }
)


def _entries(value: object) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _declared_skill_capabilities(
    snapshot: Mapping[str, object],
) -> frozenset[str]:
    files = snapshot.get("files")
    if not isinstance(files, Sequence) or isinstance(files, (str, bytes)):
        return frozenset()
    root = next(
        (item for item in files if isinstance(item, Mapping) and item.get("path") == "SKILL.md" and item.get("kind") == "text" and isinstance(item.get("content"), str)),
        None,
    )
    if root is None:
        return frozenset()
    parts, error = split_skill_markdown(str(root["content"]))
    if error is not None or parts is None:
        return frozenset()
    capabilities: set[str] = set()
    try:
        allowed_tools = parse_allowed_tools(
            parts.metadata.get("allowed-tools"),
            Path("SKILL.md"),
        )
    except ValueError:
        allowed_tools = ()
    if allowed_tools is None:
        capabilities.add("unrestricted-tools")
    else:
        capabilities.update(f"tool:{tool_name}" for tool_name in allowed_tools)
    try:
        required_secrets = parse_required_secrets(
            parts.metadata.get("required-secrets"),
            Path("SKILL.md"),
        )
    except ValueError:
        required_secrets = ()
    if required_secrets:
        capabilities.add("declared-secrets")
        if parse_secrets_autonomous(
            parts.metadata.get("secrets-autonomous"),
            Path("SKILL.md"),
        ):
            capabilities.add("autonomous-secrets")
    return frozenset(capabilities)


class GovernedToolPlaneValidator(DeterministicToolPlaneValidator):
    """Validate schema, policy, staged skill bytes, SkillScan, and review."""

    def __init__(
        self,
        *,
        policy_digest: str,
        artifact_store: GovernedSkillArtifactStore,
        durable: bool,
        allowed_mcp_transports: tuple[str, ...] = (
            "stdio",
            "sse",
            "http",
            "streamable_http",
        ),
        maximum_mcp_servers: int = 128,
        maximum_skills: int = 512,
        require_complete_review: bool = True,
        allowed_mcp_stdio_commands: tuple[str, ...] = ("npx", "uvx"),
        allowed_mcp_endpoint_hosts: tuple[str, ...] = (),
        allow_private_mcp_endpoints: bool = False,
        allowed_managed_integration_providers: tuple[str, ...] = (),
        forbidden_skill_capabilities: tuple[str, ...] = (),
        endpoint_resolver: Callable[[str], list[ipaddress._BaseAddress]] | None = None,
    ) -> None:
        super().__init__(policy_digest=policy_digest)
        self._artifact_store = artifact_store
        self._durable = durable
        self._allowed_mcp_transports = frozenset(allowed_mcp_transports)
        self._maximum_mcp_servers = maximum_mcp_servers
        self._maximum_skills = maximum_skills
        self._require_complete_review = require_complete_review
        self._allowed_mcp_stdio_commands = frozenset(allowed_mcp_stdio_commands)
        self._allowed_mcp_endpoint_hosts = frozenset(host.casefold().rstrip(".") for host in allowed_mcp_endpoint_hosts)
        self._allow_private_mcp_endpoints = allow_private_mcp_endpoints
        self._allowed_managed_integration_providers = frozenset(allowed_managed_integration_providers)
        self._forbidden_skill_capabilities = frozenset(forbidden_skill_capabilities)
        self._endpoint_resolver = endpoint_resolver

    @property
    def validator_versions(self) -> Mapping[str, str]:
        """Return exact schema and implementation identities for this pipeline."""

        return dict(_VALIDATOR_VERSIONS)

    async def _endpoint_policy_error(self, url: str) -> bool:
        """Return whether one MCP or OAuth endpoint violates SSRF policy."""

        hostname = (urlsplit(url).hostname or "").casefold().rstrip(".")
        if self._allowed_mcp_endpoint_hosts and hostname not in self._allowed_mcp_endpoint_hosts:
            return True
        # RFC 2606's .test namespace is deliberately non-routable and is used
        # throughout the offline contract suite. No DNS answer can turn it
        # into a reachable private endpoint.
        if hostname.endswith(".test"):
            return False
        error = await asyncio.to_thread(
            validate_public_http_url,
            url,
            allow_private_addresses=self._allow_private_mcp_endpoints,
            action="connect MCP",
            resolver=self._endpoint_resolver,
        )
        return error is not None

    async def _review_skill(
        self,
        entry: Mapping[str, object],
        *,
        field_name: str,
    ) -> tuple[list[ToolPlaneValidationFindingV1], bool, bool]:
        try:
            verified = await asyncio.to_thread(
                self._artifact_store.verify,
                tree_digest=str(entry.get("tree_digest")),
                archive_digest=str(entry.get("archive_digest")),
                manifest_digest=str(entry.get("manifest_digest")),
            )
        except ToolPlaneRevisionError:
            return (
                [
                    ToolPlaneValidationFindingV1(
                        code="skill_artifact_not_staged",
                        severity="error",
                        location=field_name,
                    )
                ],
                True,
                False,
            )
        expected_name = entry.get("name")
        if expected_name != verified.metadata.skill_name:
            return (
                [
                    ToolPlaneValidationFindingV1(
                        code="skill_name_mismatch",
                        severity="error",
                        location=field_name,
                    )
                ],
                True,
                False,
            )
        identity_findings: list[ToolPlaneValidationFindingV1] = []
        if entry.get("version") != verified.metadata.declared_version:
            identity_findings.append(
                ToolPlaneValidationFindingV1(
                    code="skill_version_mismatch",
                    severity="error",
                    location=field_name,
                )
            )
        supplied_entry_points = entry.get("entry_points")
        if not isinstance(supplied_entry_points, Sequence) or isinstance(supplied_entry_points, (str, bytes)) or tuple(supplied_entry_points) != verified.metadata.entry_points:
            identity_findings.append(
                ToolPlaneValidationFindingV1(
                    code="skill_entry_points_mismatch",
                    severity="error",
                    location=field_name,
                )
            )
        try:
            snapshot = await asyncio.to_thread(LocalDirectoryReader(verified.package_root).read)
            facts = await asyncio.to_thread(analyze_skill_package, snapshot)
        except Exception:
            return (
                [
                    ToolPlaneValidationFindingV1(
                        code="skill_reviewer_unavailable",
                        severity="error",
                        location=field_name,
                    )
                ],
                True,
                True,
            )
        findings = identity_findings
        failed = bool(identity_findings)
        if self._forbidden_skill_capabilities.intersection(_declared_skill_capabilities(snapshot)):
            findings.append(
                ToolPlaneValidationFindingV1(
                    code="skill_capability_forbidden",
                    severity="error",
                    location=field_name,
                )
            )
            failed = True
        for raw in facts.get("findings", []):
            if not isinstance(raw, Mapping):
                continue
            severity_value = str(raw.get("severity"))
            severity: Literal["warning", "error"] = "error" if severity_value in {"blocker", "error"} else "warning"
            failed = failed or severity == "error"
            path = raw.get("path")
            findings.append(
                ToolPlaneValidationFindingV1(
                    code=str(raw.get("rule_id") or "skill_review_finding")[:128],
                    severity=severity,
                    location=(field_name if not isinstance(path, str) or not path else f"{field_name}/{path}"[:1024]),
                )
            )
        completeness = facts.get("completeness")
        incomplete = not isinstance(completeness, Mapping) or bool(completeness.get("truncated")) or bool(completeness.get("not_assessed")) or bool(facts.get("reader_errors")) or bool(facts.get("analyzer_errors"))
        if incomplete:
            findings.append(
                ToolPlaneValidationFindingV1(
                    code="skill_review_incomplete",
                    severity="error",
                    location=field_name,
                )
            )
            failed = True
        return findings, failed, incomplete

    async def validate(
        self,
        revision: ToolPlaneRevisionRecord,
    ) -> ToolPlaneValidationReportV1:
        """Run deterministic schema, endpoint, launch, artifact, and review checks."""

        structural = await super().validate(revision)
        findings = list(structural.findings)
        failed = structural.result != "passed"
        unqualified = structural.result == "unqualified"
        servers = _entries(revision.manifest.get("mcp_servers"))
        try:
            ExtensionsConfig.model_validate(
                {
                    "mcpServers": runtime_mcp_servers_from_canonical(servers),
                }
            )
        except Exception:
            # Pydantic details can include rejected input, including a literal
            # that an upstream schema newly classifies as sensitive. Keep only
            # this stable code in revision evidence.
            findings.append(
                ToolPlaneValidationFindingV1(
                    code="mcp_schema_invalid",
                    severity="error",
                    location="mcp_servers",
                )
            )
            failed = True
        if len(servers) > self._maximum_mcp_servers:
            findings.append(
                ToolPlaneValidationFindingV1(
                    code="maximum_mcp_servers_exceeded",
                    severity="error",
                    location="mcp_servers",
                )
            )
            failed = True
        for server in servers:
            server_id = str(server.get("server_id") or "mcp")
            transport = server.get("transport")
            location = f"mcp_servers.{server_id}"
            if transport not in self._allowed_mcp_transports:
                findings.append(
                    ToolPlaneValidationFindingV1(
                        code="mcp_transport_not_allowed",
                        severity="error",
                        location=location,
                    )
                )
                failed = True
            if transport == "stdio" and not server.get("command"):
                findings.append(
                    ToolPlaneValidationFindingV1(
                        code="mcp_command_required",
                        severity="error",
                        location=location,
                    )
                )
                failed = True
            if transport == "stdio" and server.get("command"):
                env_names = [
                    str(selector["field"]).removeprefix("env.") for selector in server.get("secret_selectors", ()) if isinstance(selector, Mapping) and isinstance(selector.get("field"), str) and str(selector["field"]).startswith("env.")
                ]
                try:
                    validate_mcp_stdio_launch(
                        command=server["command"],
                        args=tuple(server.get("args", ())),
                        env_names=env_names,
                        allowed_commands=self._allowed_mcp_stdio_commands,
                    )
                except McpStdioLaunchPolicyViolation as exc:
                    findings.append(
                        ToolPlaneValidationFindingV1(
                            code=("mcp_environment_not_allowed" if exc.code == "environment_not_allowed" else "mcp_command_not_allowed"),
                            severity="error",
                            location=location,
                        )
                    )
                    failed = True
            if transport in {"sse", "http", "streamable_http"} and not server.get("url"):
                findings.append(
                    ToolPlaneValidationFindingV1(
                        code="mcp_url_required",
                        severity="error",
                        location=location,
                    )
                )
                failed = True
            if transport in {"sse", "http", "streamable_http"} and server.get("url") and await self._endpoint_policy_error(str(server["url"])):
                findings.append(
                    ToolPlaneValidationFindingV1(
                        code="mcp_private_endpoint_not_allowed",
                        severity="error",
                        location=location,
                    )
                )
                failed = True
            oauth_structure = server.get("oauth_structure")
            token_url = oauth_structure.get("token_url") if isinstance(oauth_structure, Mapping) else None
            if isinstance(token_url, str) and await self._endpoint_policy_error(token_url):
                findings.append(
                    ToolPlaneValidationFindingV1(
                        code="mcp_private_endpoint_not_allowed",
                        severity="error",
                        location=f"{location}.oauth.token_url",
                    )
                )
                failed = True

        skill_count = sum(len(_entries(revision.manifest.get(field_name))) for field_name in _SKILL_FIELDS)
        if skill_count > self._maximum_skills:
            findings.append(
                ToolPlaneValidationFindingV1(
                    code="maximum_skills_exceeded",
                    severity="error",
                    location="skills",
                )
            )
            failed = True
        for entry in _entries(revision.manifest.get("managed_integrations")):
            provider = entry.get("provider")
            if self._allowed_managed_integration_providers and provider not in self._allowed_managed_integration_providers:
                findings.append(
                    ToolPlaneValidationFindingV1(
                        code="managed_integration_provider_not_allowed",
                        severity="error",
                        location="managed_integrations",
                    )
                )
                failed = True
        for field_name in _SKILL_FIELDS:
            for entry in _entries(revision.manifest.get(field_name)):
                skill_name = str(entry.get("name") or "skill")
                reviewed, review_failed, review_incomplete = await self._review_skill(
                    entry,
                    field_name=f"{field_name}.{skill_name}",
                )
                findings.extend(reviewed)
                failed = failed or review_failed
                unqualified = unqualified or (review_incomplete and self._durable and self._require_complete_review)
        if len(findings) > 255:
            findings = findings[:255]
            findings.append(
                ToolPlaneValidationFindingV1(
                    code="validation_findings_truncated",
                    severity="error",
                )
            )
            failed = True
        result: Literal["passed", "failed", "unqualified"]
        if unqualified:
            result = "unqualified"
        elif failed:
            result = "failed"
        else:
            result = "passed"
        return ToolPlaneValidationReportV1(
            revision_digest=revision.revision_digest,
            content_digest=revision.content_digest,
            validator_policy_digest=self.policy_digest,
            validator_versions=self.validator_versions,
            result=result,
            findings=tuple(findings),
        )

    async def validate_compatibility(
        self,
        *,
        base: ToolPlaneRevisionRecord,
        overlay: ToolPlaneRevisionRecord,
    ) -> ToolPlaneValidationReportV1:
        """Validate an overlay and prove that it cannot widen the supplied base."""

        report = await self.validate(overlay)
        findings = list(report.findings)
        failed = report.result != "passed"
        base_mcp_entries = {str(entry.get("server_id")): entry for entry in _entries(base.manifest.get("mcp_servers"))}
        base_mcp = set(base_mcp_entries)
        base_integrations = {str(entry.get("name")) for entry in _entries(base.manifest.get("managed_integrations"))}
        custom = {str(entry.get("name")) for entry in _entries(overlay.manifest.get("custom_skills"))}
        checks = (
            ("mcp_enablement", "id", base_mcp, "overlay_mcp_missing_from_base"),
            (
                "credential_selectors",
                "server_id",
                base_mcp,
                "overlay_credential_server_missing_from_base",
            ),
            (
                "managed_integration_enablement",
                "id",
                base_integrations,
                "overlay_integration_missing_from_base",
            ),
            (
                "skill_states",
                "name",
                custom,
                "overlay_skill_missing_from_composition",
            ),
        )
        for field_name, identifier_field, allowed, code in checks:
            for entry in _entries(overlay.manifest.get(field_name)):
                if str(entry.get(identifier_field)) not in allowed:
                    findings.append(
                        ToolPlaneValidationFindingV1(
                            code=code,
                            severity="error",
                            location=field_name,
                        )
                    )
                    failed = True
        custom_entries = {str(entry.get("name")): entry for entry in _entries(overlay.manifest.get("custom_skills"))}
        for state in _entries(overlay.manifest.get("skill_states")):
            custom_entry = custom_entries.get(str(state.get("name")))
            if custom_entry is not None and state.get("enabled") != custom_entry.get("enabled"):
                findings.append(
                    ToolPlaneValidationFindingV1(
                        code="overlay_skill_state_conflict",
                        severity="error",
                        location="skill_states",
                    )
                )
                failed = True
        for entry in _entries(overlay.manifest.get("mcp_enablement")):
            server_id = str(entry.get("id"))
            base_entry = base_mcp_entries.get(server_id)
            if base_entry is not None and entry.get("enabled") is True and base_entry.get("enabled") is False:
                findings.append(
                    ToolPlaneValidationFindingV1(
                        code="overlay_mcp_widens_base",
                        severity="error",
                        location="mcp_enablement",
                    )
                )
                failed = True
        for selector in _entries(overlay.manifest.get("credential_selectors")):
            server_id = str(selector.get("server_id"))
            base_entry = base_mcp_entries.get(server_id)
            binding = None if base_entry is None else base_entry.get("credential_binding")
            if isinstance(binding, Mapping) and (selector.get("binding_ref") != binding.get("binding_ref") or selector.get("version") != binding.get("version")):
                findings.append(
                    ToolPlaneValidationFindingV1(
                        code="overlay_credential_binding_mismatch",
                        severity="error",
                        location="credential_selectors",
                    )
                )
                failed = True
        base_integration_entries = {str(entry.get("name")): entry for entry in _entries(base.manifest.get("managed_integrations"))}
        for entry in _entries(overlay.manifest.get("managed_integration_enablement")):
            integration = base_integration_entries.get(str(entry.get("id")))
            if integration is not None and entry.get("enabled") is True and integration.get("enabled") is False:
                findings.append(
                    ToolPlaneValidationFindingV1(
                        code="overlay_integration_widens_base",
                        severity="error",
                        location="managed_integration_enablement",
                    )
                )
                failed = True
        return ToolPlaneValidationReportV1(
            revision_digest=overlay.revision_digest,
            content_digest=overlay.content_digest,
            validator_policy_digest=self.policy_digest,
            validator_versions=report.validator_versions,
            result=("unqualified" if report.result == "unqualified" else "failed" if failed else "passed"),
            findings=tuple(findings[:256]),
        )


__all__ = ["GovernedToolPlaneValidator"]
