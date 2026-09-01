"""Config-driven extension loading.

Entry points are named as `module.path:install`, resolved through the same
`resolve_variable` helper the guardrails provider already uses. Load order is
the config list order — explicit and reproducible, which matters because the
middleware stack is position-sensitive.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, packages_distributions, version
from itertools import count
from pathlib import Path
from typing import Any, Literal

from deerflow_extension_api import API_VERSION
from pydantic import BaseModel, ConfigDict, Field

from deerflow.deployment.topology import (
    DeploymentProfile,
    coerce_deployment_profile,
)
from deerflow.diagnostics import BoundedDiagnostic, bounded_diagnostic
from deerflow.extensions.artifacts import (
    UNVERIFIED_EXTENSION_ARTIFACT_MANIFEST_DIGEST,
    ExtensionArtifactManifestV1,
    ExtensionArtifactVerificationError,
    ExtensionSourceLockV1,
    extension_configuration_digest,
    normalize_distribution_name,
    read_artifact_manifest,
    read_source_lock,
    verify_installed_artifact_manifest,
)
from deerflow.extensions.registry import ExtensionRegistry, LoadedExtensions
from deerflow.persistence.migrations._env_filters import register_extension_table_prefix
from deerflow.reflection import resolve_variable

logger = logging.getLogger(__name__)
_extension_generations = count(1)
_missing_manifest_warning_emitted = False

DiagnosticLevel = Literal["debug", "info", "warning", "error"]


class ExtensionSpec(BaseModel):
    """One entry of the `plugins:` list in config.yaml."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(
        default=True,
        description="When false, skip the extension without resolving or importing it",
    )
    name: str | None = Field(
        default=None,
        description="Stable operator-facing name recorded by the extension manager",
    )
    package: str | None = Field(
        default=None,
        description="Installed Python distribution recorded by the extension manager",
    )
    use: str = Field(description="Entry point path, e.g. 'my_extension:install'")
    config: dict[str, Any] = Field(
        default_factory=dict,
        description="Extension-private configuration, passed to install() verbatim",
    )
    required: bool = Field(
        default=False,
        description="When true, a load failure aborts startup instead of being skipped",
    )
    table_prefix: str | None = Field(
        default=None,
        min_length=1,
        description=(
            "Table-name prefix this extension owns, if it persists data under its own "
            "MetaData and migration chain. Registered with "
            "deerflow.persistence.migrations._env_filters so alembic revision --autogenerate "
            "excludes those tables instead of reflecting them from a live database and "
            "proposing to drop them. Registered from two processes: here for the Gateway, "
            "and from migrations/env.py -- reading this declaration, never importing the "
            "extension -- for alembic, which never starts a Gateway. Omit the key to "
            "declare no prefix; an empty string is rejected here rather than treated "
            "as absent, so that one declaration cannot mean 'no prefix' to one of "
            "those two processes and 'a prefix matching every table' to the other."
        ),
    )


@dataclass(frozen=True)
class Diagnostic:
    """A load- or run-time problem attributed to a specific extension.

    The repository has no structured diagnostics channel today; this is a
    deliberately minimal one whose only job is keeping failures attributable.
    """

    level: DiagnosticLevel
    source: str
    message: str
    code: str | None = None
    error_class: str | None = None
    correlation_id: str | None = None
    contribution_id: str | None = None
    operation: str | None = None

    @classmethod
    def error(cls, source: str, message: str) -> Diagnostic:
        return cls("error", source, message)

    @classmethod
    def warning(cls, source: str, message: str) -> Diagnostic:
        return cls("warning", source, message)

    @classmethod
    def info(cls, source: str, message: str) -> Diagnostic:
        return cls("info", source, message)

    @classmethod
    def debug(cls, source: str, message: str) -> Diagnostic:
        return cls("debug", source, message)

    @classmethod
    def failure(
        cls,
        source: str,
        *,
        code: str,
        error: BaseException,
        message: str | None = None,
    ) -> Diagnostic:
        """Build a bounded failure diagnostic without retaining provider text."""

        diagnostic = bounded_diagnostic(
            code=code,
            operation="extension_operation",
            error=error,
            contribution_id=source,
        )
        return cls.from_bounded(source, diagnostic, message=message)

    @classmethod
    def from_bounded(
        cls,
        source: str,
        diagnostic: BoundedDiagnostic,
        *,
        message: str | None = None,
    ) -> Diagnostic:
        """Adapt the shared bounded diagnostic without changing its identity."""

        return cls(
            "error",
            source,
            message or f"{diagnostic.code}:{diagnostic.operation}",
            code=diagnostic.code,
            error_class=diagnostic.error_class,
            correlation_id=diagnostic.correlation_id,
            contribution_id=diagnostic.contribution_id,
            operation=diagnostic.operation,
        )


class ExtensionLoadError(RuntimeError):
    """Raised when an extension marked `required: true` fails to load."""


@dataclass(frozen=True, slots=True)
class _VerifiedArtifactContext:
    artifact_manifest_digest: str | None
    configuration_digest: str
    source_entries_by_plugin: Mapping[tuple[str, str, str], str]


def _profile_value(value: object | None) -> DeploymentProfile:
    if value is not None:
        return coerce_deployment_profile(value)
    try:
        from deerflow.config.app_config import get_app_config

        return coerce_deployment_profile(get_app_config().deployment.profile)
    except (FileNotFoundError, OSError, ValueError):
        return DeploymentProfile.local_development


def _default_provenance_paths() -> tuple[Path, Path]:
    configured_root = os.environ.get("DEER_FLOW_PROJECT_ROOT")
    root = Path(configured_root).expanduser().resolve() if configured_root else Path.cwd().resolve()
    backend = root / "backend" if (root / "backend" / "pyproject.toml").is_file() else root
    app_root = backend.parent if backend.name == "backend" else root
    source_lock = Path(
        os.environ.get(
            "DEER_FLOW_EXTENSION_SOURCE_LOCK_PATH",
            str(backend / "extensions.lock.json"),
        )
    )
    artifact_manifest = Path(
        os.environ.get(
            "DEER_FLOW_EXTENSION_ARTIFACT_MANIFEST_PATH",
            str(app_root / "hartmesh" / "extension-artifacts.json"),
        )
    )
    return source_lock, artifact_manifest


def _manifest_source_entries(
    source_lock: ExtensionSourceLockV1,
    manifest: ExtensionArtifactManifestV1,
) -> Mapping[tuple[str, str, str], str]:
    by_digest = {entry.entry_digest: entry for entry in source_lock.entries}
    result: dict[tuple[str, str, str], str] = {}
    for artifact in manifest.entries:
        source = by_digest.get(artifact.source_entry_digest)
        if source is None:
            raise ExtensionArtifactVerificationError(
                "extension_artifact_digest_mismatch",
                distribution=artifact.distribution,
            )
        result[
            (
                normalize_distribution_name(source.distribution),
                source.entry_point_name,
                source.entry_point_value,
            )
        ] = source.entry_digest
    if len(result) != len(source_lock.entries):
        raise ExtensionArtifactVerificationError("extension_artifact_digest_mismatch")
    return result


def _verify_artifacts_before_import(
    specs: Sequence[ExtensionSpec],
    *,
    deployment_profile: object | None,
    source_lock_path: str | Path | None,
    artifact_manifest_path: str | Path | None,
    expected_artifact_manifest_digest: str | None,
    expected_configuration_digest: str | None,
) -> _VerifiedArtifactContext:
    global _missing_manifest_warning_emitted

    profile = _profile_value(deployment_profile)
    default_source_lock, default_artifact_manifest = _default_provenance_paths()
    lock_path = Path(source_lock_path) if source_lock_path is not None else default_source_lock
    manifest_path = Path(artifact_manifest_path) if artifact_manifest_path is not None else default_artifact_manifest
    configuration_digest = extension_configuration_digest(specs)
    configured_expected_artifact = expected_artifact_manifest_digest or os.environ.get("DEER_FLOW_EXTENSION_ARTIFACT_MANIFEST_DIGEST")
    configured_expected_config = expected_configuration_digest or os.environ.get("DEER_FLOW_EXTENSION_CONFIGURATION_DIGEST")
    if configured_expected_config is not None and configured_expected_config != configuration_digest:
        raise ExtensionArtifactVerificationError(
            "extension_configuration_digest_mismatch",
            expected_digest=configured_expected_config,
            actual_digest=configuration_digest,
        )
    try:
        source_lock = read_source_lock(lock_path)
        manifest = read_artifact_manifest(manifest_path)
        verified = verify_installed_artifact_manifest(
            source_lock,
            manifest,
            backend_dir=lock_path.parent,
            expected_extension_api_version=API_VERSION,
        )
    except ExtensionArtifactVerificationError as exc:
        if profile.is_durable or configured_expected_artifact is not None:
            raise
        if not _missing_manifest_warning_emitted:
            logger.warning(
                "Extension artifact provenance is not verified in local development: diagnostic_code=%s",
                exc.code,
            )
            _missing_manifest_warning_emitted = True
        return _VerifiedArtifactContext(
            UNVERIFIED_EXTENSION_ARTIFACT_MANIFEST_DIGEST,
            configuration_digest,
            {},
        )
    if configured_expected_artifact is not None and configured_expected_artifact != verified.digest:
        raise ExtensionArtifactVerificationError(
            "extension_artifact_expected_digest_mismatch",
            expected_digest=configured_expected_artifact,
            actual_digest=verified.digest,
        )
    entries = _manifest_source_entries(source_lock, verified)
    for spec in specs:
        if not spec.enabled:
            continue
        if spec.package is None or spec.name is None:
            if profile.is_durable:
                raise ExtensionArtifactVerificationError("extension_artifact_manifest_invalid")
            continue
        identity = (normalize_distribution_name(spec.package), spec.name, spec.use)
        if identity not in entries:
            raise ExtensionArtifactVerificationError(
                "extension_artifact_digest_mismatch",
                distribution=spec.package,
            )
    return _VerifiedArtifactContext(verified.digest, configuration_digest, entries)


def _distribution_provenance(install: object) -> tuple[str | None, str | None]:
    """Resolve installed distribution provenance without trusting plugin input."""
    module_name = getattr(install, "__module__", None)
    if not isinstance(module_name, str) or not module_name:
        return None, None
    top_level = module_name.partition(".")[0]
    candidates = sorted(packages_distributions().get(top_level, ()))
    if not candidates:
        return None, None
    package_name = candidates[0]
    try:
        package_version = version(package_name)
    except PackageNotFoundError:
        package_version = None
    return package_name, package_version


def _parse_version(version: object) -> tuple[int, ...] | None:
    if not isinstance(version, str):
        return None
    try:
        return tuple(int(part) for part in str.split(version, "."))
    except ValueError:
        return None


def _compatible(declared: str, current: str) -> bool:
    """One-directional, with the semver window for the contract's life stage.

    Pre-1.0 minors may break, so the window is same major.minor with patches
    additive: host >= declared.
    From 1.0 on contracts only grow within a major, so a newer host stays
    compatible with older extensions while an extension written against a
    newer minor is refused — it would reach for contract additions the host
    does not implement. Unparseable versions are refused, not waved through."""
    declared_parts = _parse_version(declared)
    current_parts = _parse_version(current)
    if not declared_parts or not current_parts:
        return False
    width = max(len(declared_parts), len(current_parts), 2)
    declared_padded = declared_parts + (0,) * (width - len(declared_parts))
    current_padded = current_parts + (0,) * (width - len(current_parts))
    if declared_padded[0] != current_padded[0]:
        return False
    if declared_padded[0] == 0 and declared_padded[1] != current_padded[1]:
        return False
    return current_padded >= declared_padded


def _range_for(declared: str) -> str:
    """The pip window matching ``_compatible``'s rules, for the actionable
    refusal message. Falls back to an exact request when the declared version
    is unparseable — the message must survive the version that caused it."""
    parts = _parse_version(declared)
    if not parts:
        return f"=={declared}"
    if parts[0] == 0:
        minor = parts[1] if len(parts) > 1 else 0
        return f">={declared},<0.{minor + 1}"
    return f">={declared},<{parts[0] + 1}.0"


def load_extensions(
    specs: Sequence[ExtensionSpec],
    *,
    deployment_profile: object | None = None,
    source_lock_path: str | Path | None = None,
    artifact_manifest_path: str | Path | None = None,
    expected_artifact_manifest_digest: str | None = None,
    expected_configuration_digest: str | None = None,
) -> tuple[LoadedExtensions, list[Diagnostic]]:
    """Resolve and install every configured extension.

    Fail-open by default: a broken extension is skipped with a diagnostic so
    the Gateway still starts. `required: true` flips that to fail-closed for
    extensions whose absence changes behaviour rather than just observability.
    """
    try:
        artifact_context = _verify_artifacts_before_import(
            specs,
            deployment_profile=deployment_profile,
            source_lock_path=source_lock_path,
            artifact_manifest_path=artifact_manifest_path,
            expected_artifact_manifest_digest=expected_artifact_manifest_digest,
            expected_configuration_digest=expected_configuration_digest,
        )
    except ExtensionArtifactVerificationError as exc:
        logger.error(
            "Extension provenance verification failed diagnostic_code=%s correlation_id=%s distribution=%s",
            exc.code,
            exc.correlation_id,
            exc.distribution or "none",
        )
        raise ExtensionLoadError(str(exc)) from None

    registry = ExtensionRegistry()
    diagnostics: list[Diagnostic] = []
    loaded_sources: list[str] = []

    def record_failure(
        spec: ExtensionSpec,
        *,
        code: str,
        error: BaseException,
        message: str | None = None,
    ) -> Diagnostic:
        diagnostic = Diagnostic.failure(spec.use, code=code, error=error, message=message)
        diagnostics.append(diagnostic)
        logger.error(
            "Extension load failure source=%s diagnostic_code=%s error_class=%s correlation_id=%s",
            diagnostic.source,
            diagnostic.code,
            diagnostic.error_class,
            diagnostic.correlation_id,
        )
        return diagnostic

    for spec in specs:
        if spec.table_prefix:
            # Registered unconditionally -- even for a disabled or later-failing
            # spec -- because the tables it names may already exist in the
            # database from a previous run. Excluding them from alembic's view
            # is the safe direction; the risk this guards against is
            # autogenerate proposing to drop them, not registering one prefix
            # too many.
            #
            # A prefix that collides with a host table name is not a
            # per-extension failure `required: false` can shrug off: it
            # corrupts the shared alembic filter for that host table for the
            # life of the process, regardless of whether this extension ever
            # loads. It always aborts startup.
            try:
                register_extension_table_prefix(spec.table_prefix)
            except ValueError as exc:
                message = str(exc)
                diagnostics.append(Diagnostic.error(spec.use, message))
                logger.error("Extension %s: %s", spec.use, message)
                raise ExtensionLoadError(message) from exc

        if not spec.enabled:
            continue

        try:
            install = resolve_variable(spec.use)
        except Exception as exc:
            record_failure(spec, code="entry_point_resolution_failed", error=exc)
            if spec.required:
                raise ExtensionLoadError(f"required extension {spec.use} failed to load") from None
            continue

        if not callable(install):
            message = f"extension entry point is not callable: {type(install).__name__}"
            diagnostics.append(Diagnostic.error(spec.use, message))
            logger.error("Extension %s: %s", spec.use, message)
            if spec.required:
                raise ExtensionLoadError(f"required extension {spec.use} is not callable")
            continue

        try:
            declared = getattr(install, "__deerflow_api__", None)
        except Exception as exc:
            record_failure(
                spec,
                code="api_marker_inspection_failed",
                error=exc,
                message="could not inspect extension-api version marker",
            )
            if spec.required:
                raise ExtensionLoadError(f"required extension {spec.use} could not inspect api marker") from None
            continue
        if declared is not None and _parse_version(declared) is None:
            message = f"extension declares invalid extension-api version marker of type {type(declared).__name__}; expected a dotted numeric string such as '0.1'"
            diagnostics.append(Diagnostic.error(spec.use, message))
            logger.error("Extension %s: %s", spec.use, message)
            if spec.required:
                raise ExtensionLoadError(f"required extension {spec.use} declares invalid api marker")
            continue
        if declared is not None:
            # ``isinstance(..., str)`` also accepts subclasses whose
            # ``__str__``/``__format__`` methods can execute plugin code while
            # we build an incompatibility diagnostic. Normalize with the base
            # implementation before compatibility checks and rendering.
            declared = str.__str__(declared)
        if declared is not None and not _compatible(declared, API_VERSION):
            message = f"extension requires extension-api {declared}, host provides {API_VERSION}. Install a matching version: pip install 'deerflow-extension-api{_range_for(declared)}'"
            diagnostics.append(Diagnostic.error(spec.use, message))
            logger.error("Extension %s: %s", spec.use, message)
            if spec.required:
                raise ExtensionLoadError(f"required extension {spec.use} declares incompatible api {declared}")
            continue

        # Positional rollback, not registry.discard(spec.use): two specs may
        # legitimately share the same `use` with different config, and
        # discard-by-source would also erase an earlier, successfully
        # installed instance that happens to share this spec's `use`.
        mark = registry.mark()
        package_name, package_version = _distribution_provenance(install)
        source_entry_digest = None
        if spec.package is not None and spec.name is not None:
            source_entry_digest = artifact_context.source_entries_by_plugin.get((normalize_distribution_name(spec.package), spec.name, spec.use))
        try:
            with registry.attributed_to(
                spec.use,
                package_name=package_name,
                package_version=package_version,
                source_entry_digest=source_entry_digest,
            ):
                install(registry, _frozen_config(spec.config))
        except Exception as exc:
            registry.rollback_to(mark)
            record_failure(spec, code="install_failed", error=exc)
            if spec.required:
                raise ExtensionLoadError(f"required extension {spec.use} failed to install") from None
            continue

        registry.record_loaded_plugin(
            package_name=package_name,
            package_version=package_version,
            required=spec.required,
            source_entry_digest=source_entry_digest,
        )
        loaded_sources.append(spec.use)

    # Loading third-party code is exactly the event an operator needs positive
    # confirmation of, and every other branch here is failure-only — so without
    # this line a fully successful load is indistinguishable from a `plugins:`
    # block the host never read. The x/y count names the difference between
    # "all loaded" and "some were skipped" without repeating the per-failure
    # errors already logged above.
    if specs:
        logger.info(
            "Extensions loaded: %d/%d (%s)",
            len(loaded_sources),
            len(specs),
            ", ".join(loaded_sources) or "none",
        )
    else:
        # Debug, not info: no configured plugins is the default state for almost
        # every deployment, and an unconditional line would be pure boot noise.
        logger.debug("No extensions configured")

    return (
        registry.build(
            generation=next(_extension_generations),
            artifact_manifest_digest=artifact_context.artifact_manifest_digest,
            extension_configuration_digest=artifact_context.configuration_digest,
        ),
        diagnostics,
    )


def _frozen_config(config: dict[str, Any]) -> Mapping[str, Any]:
    """Hand extensions a shallow copy of their config block.

    This is a shallow copy: it stops an extension from reassigning
    top-level keys on another extension's (or the caller's) config dict, but
    nested structures (lists, dicts) are still shared by reference and can be
    mutated in place. Use plain, top-level config values if this guarantee
    matters to you.
    """
    return dict(config)
