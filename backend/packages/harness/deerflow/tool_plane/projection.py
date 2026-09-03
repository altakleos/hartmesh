"""Crash-observable filesystem projection for governed tool-plane material."""

from __future__ import annotations

import asyncio
import copy
import json
import os
import shutil
import tempfile
from collections.abc import Callable, Mapping
from contextlib import nullcontext
from pathlib import Path
from typing import Any

from deerflow.config.extensions_config import (
    ExtensionsConfig,
    atomic_write_extensions_config,
    extensions_config_file_lock,
    extensions_config_write_lock,
    reload_extensions_config,
)
from deerflow.config.runtime_paths import runtime_home
from deerflow.skills.projection import skill_projection_mutation
from deerflow.skills.storage import (
    get_or_new_skill_storage,
    get_or_new_user_skill_storage,
)
from deerflow.skills.storage.local_skill_storage import LocalSkillStorage
from deerflow.tool_plane.artifacts import compute_skill_tree_digest
from deerflow.tool_plane.contracts import (
    ToolPlaneRevisionError,
    ToolPlaneRevisionScopeV1,
    canonical_tool_plane_digest,
    runtime_mcp_servers_from_canonical,
)

_DRIFT_DIGEST = canonical_tool_plane_digest({"version": 1, "state": "unmanaged_drift"})


def _selector_value(selector: object) -> str:
    if not isinstance(selector, str) or not selector.startswith("env:"):
        raise ToolPlaneRevisionError("projection_failed")
    return f"${selector.removeprefix('env:')}"


def _mcp_runtime_projection(servers: object) -> dict[str, dict[str, object]]:
    try:
        return runtime_mcp_servers_from_canonical(servers)
    except ToolPlaneRevisionError as exc:
        raise ToolPlaneRevisionError("projection_failed") from exc


def _skill_runtime_projection(skills: object) -> dict[str, dict[str, bool]]:
    if not isinstance(skills, list):
        raise ToolPlaneRevisionError("projection_failed")
    result: dict[str, dict[str, bool]] = {}
    for raw in skills:
        if not isinstance(raw, Mapping) or not isinstance(raw.get("name"), str):
            raise ToolPlaneRevisionError("projection_failed")
        result[str(raw["name"])] = {"enabled": bool(raw.get("enabled", True))}
    return result


class LockedFileToolPlaneProjection:
    """Project complete revisions under the repository's established locks.

    A safe manifest and exact runtime projection are written to a private,
    content-addressed directory.  The runtime config is replaced first and a
    single active sidecar pointer is replaced last.  SQL ``prepared`` state is
    the recovery journal for the intentional cross-store crash window.
    """

    def __init__(
        self,
        *,
        config_path: Path | None = None,
        state_root: Path | None = None,
        skills_root: Path | None = None,
        integrations_root: Path | None = None,
        artifact_store: Any | None = None,
        user_storage_factory: Callable[[str], Any] | None = None,
    ) -> None:
        resolved = config_path or ExtensionsConfig.resolve_config_path()
        self._config_path = Path(resolved) if resolved is not None else runtime_home().parent / "extensions_config.json"
        self._state_root = state_root or runtime_home() / "tool-plane"
        self._skills_root = skills_root
        self._integrations_root = integrations_root
        self._artifact_store = artifact_store
        self._user_storage_factory = user_storage_factory or get_or_new_user_skill_storage

    @staticmethod
    def _scope_token(scope: ToolPlaneRevisionScopeV1) -> str:
        return canonical_tool_plane_digest(scope.to_json())

    def _active_path(self, scope: ToolPlaneRevisionScopeV1) -> Path:
        return self._state_root / "active" / f"{self._scope_token(scope)}.json"

    def _revision_path(
        self,
        scope: ToolPlaneRevisionScopeV1,
        digest: str,
    ) -> Path:
        return self._state_root / "projections" / self._scope_token(scope) / digest / "projection.json"

    @staticmethod
    def _runtime_projection(
        scope: ToolPlaneRevisionScopeV1,
        manifest: Mapping[str, object],
    ) -> dict[str, object]:
        if scope.kind == "deployment_base":
            global_skills = [
                *list(manifest.get("public_skills", [])),
                *list(manifest.get("managed_integrations", [])),
            ]
            return {
                "mcpServers": _mcp_runtime_projection(manifest.get("mcp_servers")),
                "skills": _skill_runtime_projection(global_skills),
            }
        return {
            "skill_states": _skill_runtime_projection(manifest.get("skill_states")),
            "mcp_enablement": copy.deepcopy(manifest.get("mcp_enablement", [])),
            "managed_integration_enablement": copy.deepcopy(manifest.get("managed_integration_enablement", [])),
        }

    async def project(
        self,
        scope: ToolPlaneRevisionScopeV1,
        manifest: Mapping[str, object],
        *,
        desired_digest: str,
    ) -> str:
        return await asyncio.to_thread(
            self._project_sync,
            scope,
            copy.deepcopy(dict(manifest)),
            desired_digest,
            None,
        )

    async def project_for_actor(
        self,
        scope: ToolPlaneRevisionScopeV1,
        manifest: Mapping[str, object],
        *,
        desired_digest: str,
        storage_subject_id: str,
    ) -> str:
        return await asyncio.to_thread(
            self._project_sync,
            scope,
            copy.deepcopy(dict(manifest)),
            desired_digest,
            storage_subject_id,
        )

    async def has_existing_projection(self) -> bool:
        """Return whether mutable pre-governance material needs adoption."""

        return await asyncio.to_thread(self._has_existing_projection_sync)

    async def has_existing_user_projection(
        self,
        subject_ids: tuple[str, ...],
    ) -> bool:
        """Detect indexed nonempty stores without deriving identities from paths."""

        return await asyncio.to_thread(
            self._has_existing_user_projection_sync,
            subject_ids,
        )

    async def has_unindexed_user_projection(
        self,
        subject_ids: tuple[str, ...],
    ) -> bool:
        """Fail-closed signal for material outside the authoritative index."""

        return await asyncio.to_thread(
            self._has_unindexed_user_projection_sync,
            subject_ids,
        )

    async def capture_current_deployment(
        self,
        *,
        validation_policy_digest: str,
        artifact_store: Any,
    ) -> tuple[dict[str, object], str]:
        """Capture current non-secret base material under the projection locks."""

        return await asyncio.to_thread(
            self._capture_current_deployment_sync,
            validation_policy_digest,
            artifact_store,
        )

    async def capture_current_user(
        self,
        *,
        storage_subject_id: str,
        base_revision_digest: str,
        artifact_store: Any,
    ) -> tuple[dict[str, object], str]:
        """Capture one authoritative user store without exposing its identity."""

        return await asyncio.to_thread(
            self._capture_current_user_sync,
            storage_subject_id,
            base_revision_digest,
            artifact_store,
        )

    @staticmethod
    def _package_roots(root: Path) -> list[Path]:
        if not root.exists():
            return []
        packages: list[Path] = []
        for current_root, directory_names, file_names in os.walk(
            root,
            followlinks=False,
        ):
            current = Path(current_root)
            directory_names[:] = sorted(name for name in directory_names if not name.startswith("."))
            if "SKILL.md" in file_names:
                packages.append(current)
                directory_names.clear()
        return packages

    def _capture_current_deployment_sync(
        self,
        validation_policy_digest: str,
        artifact_store: Any,
    ) -> tuple[dict[str, object], str]:
        from deerflow.config.paths import get_paths
        from deerflow.tool_plane.contracts import canonicalize_deployment_candidate

        if self._skills_root is None:
            try:
                storage = get_or_new_skill_storage()
            except FileNotFoundError:
                storage = LocalSkillStorage(host_path=str(runtime_home() / "skills"))
        else:
            storage = LocalSkillStorage(host_path=str(self._skills_root))
        skills_root = storage.get_skills_root_path()
        integrations_root = self._integrations_root or get_paths().integration_skills_dir()
        inventory_lock = self._state_root / "inventory.lock"
        projection_lock = skill_projection_mutation(storage, "public") if isinstance(storage, LocalSkillStorage) else nullcontext()
        with projection_lock:
            with extensions_config_write_lock, extensions_config_file_lock(self._config_path), extensions_config_file_lock(inventory_lock):
                try:
                    raw = json.loads(self._config_path.read_text(encoding="utf-8")) if self._config_path.exists() else {}
                except (OSError, json.JSONDecodeError) as exc:
                    raise ToolPlaneRevisionError("validation_failed") from exc
                if not isinstance(raw, Mapping):
                    raise ToolPlaneRevisionError("validation_failed")
                raw_skill_states = raw.get("skills", {})
                if not isinstance(raw_skill_states, Mapping):
                    raise ToolPlaneRevisionError("validation_failed")

                def capture(root: Path) -> dict[str, dict[str, object]]:
                    result: dict[str, dict[str, object]] = {}
                    for package_root in self._package_roots(root):
                        artifact = artifact_store.stage_directory(package_root)
                        if artifact.skill_name in result:
                            raise ToolPlaneRevisionError("validation_failed")
                        state = raw_skill_states.get(artifact.skill_name, {})
                        enabled = state.get("enabled", True) if isinstance(state, Mapping) else True
                        result[artifact.skill_name] = {
                            "enabled": enabled,
                            "archive_digest": artifact.archive_digest,
                            "tree_digest": artifact.tree_digest,
                            "manifest_digest": artifact.manifest_digest,
                            "entry_points": list(artifact.entry_points),
                        }
                    return result

                candidate: dict[str, object] = {
                    "version": 1,
                    "mcp_servers": copy.deepcopy(raw.get("mcpServers", raw.get("mcp_servers", {}))),
                    "public_skills": capture(skills_root / "public"),
                    "managed_integrations": capture(Path(integrations_root)),
                    "validation_policy_digest": validation_policy_digest,
                    "parent_revision_digest": None,
                    "change_summary": "Adopt the current tool-plane projection",
                }
                material = canonicalize_deployment_candidate(candidate)
                return candidate, material.digest

    def _capture_current_user_sync(
        self,
        storage_subject_id: str,
        base_revision_digest: str,
        artifact_store: Any,
    ) -> tuple[dict[str, object], str]:
        from deerflow.tool_plane.contracts import canonicalize_user_overlay_candidate

        storage = self._user_storage_factory(storage_subject_id)
        custom_root = Path(storage.get_user_custom_root())
        states_path = getattr(storage, "_skill_states_file", None)
        integrations_root = Path(self._integrations_root or storage.get_integrations_root())
        with skill_projection_mutation(storage, "user"):
            states: dict[str, dict[str, bool]] = {}
            if isinstance(states_path, Path) and states_path.exists():
                try:
                    raw_states = json.loads(states_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError) as exc:
                    raise ToolPlaneRevisionError("validation_failed") from exc
                if not isinstance(raw_states, Mapping):
                    raise ToolPlaneRevisionError("validation_failed")
                for raw_name, raw_state in raw_states.items():
                    if not isinstance(raw_name, str) or not isinstance(raw_state, Mapping) or set(raw_state) != {"enabled"} or type(raw_state.get("enabled")) is not bool:
                        raise ToolPlaneRevisionError("validation_failed")
                    states[raw_name] = {"enabled": bool(raw_state["enabled"])}

            custom_package_roots = self._package_roots(custom_root)
            if not custom_package_roots:
                # Before per-user storage existed, global custom skills were
                # exposed as a read-only LEGACY fallback. Bootstrap must bind
                # the exact visible bytes into each indexed user's overlay;
                # recording only their state would create an overlay that can
                # never prove package compatibility. Promotion projects these
                # bytes into the user root, completing the legacy migration.
                legacy_root = getattr(storage, "_global_custom_root", None)
                if isinstance(legacy_root, Path):
                    custom_package_roots = self._package_roots(legacy_root)

            custom_skills: dict[str, dict[str, object]] = {}
            for package_root in custom_package_roots:
                artifact = artifact_store.stage_directory(package_root)
                if artifact.skill_name in custom_skills:
                    raise ToolPlaneRevisionError("validation_failed")
                custom_skills[artifact.skill_name] = {
                    "enabled": states.get(artifact.skill_name, {}).get("enabled", True),
                    "archive_digest": artifact.archive_digest,
                    "tree_digest": artifact.tree_digest,
                    "manifest_digest": artifact.manifest_digest,
                    "entry_points": list(artifact.entry_points),
                }

            integration_names = {package_root.name for package_root in self._package_roots(integrations_root)}
            managed_enablement = {name: state["enabled"] for name, state in states.items() if name in integration_names}
            other_states = {name: state for name, state in states.items() if name not in custom_skills and name not in integration_names}

            # Existing managed-integration credential trees contain resolved
            # secret values. Their mere presence blocks adoption; this adapter
            # never reads, hashes, or copies those bytes into evidence.
            user_root = custom_root.parent.parent
            credential_root = user_root / "integrations"
            try:
                has_credential_files = credential_root.exists() and any(path.is_file() for path in credential_root.rglob("*"))
            except OSError as exc:
                raise ToolPlaneRevisionError("validation_failed") from exc
            if has_credential_files:
                raise ToolPlaneRevisionError(
                    "secret_value_present",
                    safe_details={"field": "user_integration_credentials"},
                )

            candidate: dict[str, object] = {
                "version": 1,
                "base_revision_digest": base_revision_digest,
                "custom_skills": custom_skills,
                "mcp_enablement": {},
                "managed_integration_enablement": managed_enablement,
                "credential_selectors": {},
                "skill_states": other_states,
                "parent_revision_digest": None,
                "change_summary": "Adopt the current user tool-plane projection",
            }
            material = canonicalize_user_overlay_candidate(candidate)
            return candidate, material.digest

    def _has_existing_projection_sync(self) -> bool:
        if not self._config_path.exists():
            return False
        try:
            raw = json.loads(self._config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            # An unreadable configured projection is material and must never be
            # mistaken for a fresh empty installation.
            return True
        if not isinstance(raw, Mapping):
            return True
        return bool(raw.get("mcpServers") or raw.get("mcp_servers") or raw.get("skills"))

    def _has_existing_user_projection_sync(
        self,
        subject_ids: tuple[str, ...],
    ) -> bool:
        if self._has_unindexed_user_projection_sync(subject_ids):
            return True
        for subject_id in subject_ids:
            storage = self._user_storage_factory(subject_id)
            custom_root = Path(storage.get_user_custom_root())
            states_path = getattr(storage, "_skill_states_file", None)
            with skill_projection_mutation(storage, "user"):
                if self._package_roots(custom_root):
                    return True
                if isinstance(states_path, Path) and states_path.exists():
                    try:
                        raw_states = json.loads(states_path.read_text(encoding="utf-8"))
                    except (OSError, json.JSONDecodeError):
                        return True
                    if raw_states:
                        return True
                credential_root = custom_root.parent.parent / "integrations"
                try:
                    if credential_root.exists() and any(path.is_file() for path in credential_root.rglob("*")):
                        return True
                except OSError:
                    return True
        return False

    @staticmethod
    def _user_bucket_has_tool_plane_material(bucket: Path) -> bool:
        if bucket.is_symlink() or not bucket.is_dir():
            return True
        custom_root = bucket / "skills" / "custom"
        states_path = bucket / "skills" / "_skill_states.json"
        credential_root = bucket / "integrations"
        try:
            if custom_root.exists() and any(custom_root.iterdir()):
                return True
            if states_path.exists():
                if states_path.is_symlink() or not states_path.is_file():
                    return True
                raw_states = json.loads(states_path.read_text(encoding="utf-8"))
                if not isinstance(raw_states, Mapping) or raw_states:
                    return True
            if credential_root.exists() and any(path.is_file() or path.is_symlink() for path in credential_root.rglob("*")):
                return True
        except (OSError, json.JSONDecodeError):
            return True
        return False

    def _has_unindexed_user_projection_sync(
        self,
        subject_ids: tuple[str, ...],
    ) -> bool:
        """Detect unknown buckets without treating path names as identities."""

        from deerflow.config.paths import get_paths, make_safe_user_id

        users_root = get_paths().base_dir / "users"
        if not users_root.exists():
            return False
        known_buckets = {make_safe_user_id(subject_id) for subject_id in subject_ids}
        try:
            buckets = tuple(users_root.iterdir())
        except OSError:
            return True
        return any(bucket.name not in known_buckets and self._user_bucket_has_tool_plane_material(bucket) for bucket in buckets)

    def _project_sync(
        self,
        scope: ToolPlaneRevisionScopeV1,
        manifest: Mapping[str, object],
        desired_digest: str,
        storage_subject_id: str | None,
    ) -> str:
        if canonical_tool_plane_digest(manifest) != desired_digest:
            raise ToolPlaneRevisionError("projection_digest_mismatch")
        runtime_projection = self._runtime_projection(scope, manifest)
        revision_path = self._revision_path(scope, desired_digest)
        revision_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "scope": scope.to_json(),
            "content_digest": desired_digest,
            "manifest": dict(manifest),
            "runtime_projection": runtime_projection,
        }
        if scope.kind == "user_overlay":
            if not isinstance(storage_subject_id, str) or not storage_subject_id:
                raise ToolPlaneRevisionError("projection_failed")
            payload["storage_subject_id"] = storage_subject_id
        if revision_path.exists():
            try:
                existing = json.loads(revision_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ToolPlaneRevisionError("recovery_required") from exc
            if existing != payload:
                raise ToolPlaneRevisionError("projection_digest_mismatch")
        else:
            atomic_write_extensions_config(revision_path, payload)

        active_path = self._active_path(scope)
        active_path.parent.mkdir(parents=True, exist_ok=True)
        if scope.kind == "deployment_base":
            try:
                storage = get_or_new_skill_storage()
            except FileNotFoundError:
                # Config-free harnesses still exercise the real projection
                # lock with an isolated runtime-home skill root.
                storage = LocalSkillStorage(
                    host_path=str(runtime_home() / "skills"),
                )
            projection_lock = skill_projection_mutation(storage, "public") if isinstance(storage, LocalSkillStorage) else nullcontext()
            with projection_lock:
                with extensions_config_write_lock, extensions_config_file_lock(self._config_path), extensions_config_file_lock(active_path):
                    if self._artifact_store is not None:
                        skills_root = self._skills_root or storage.get_skills_root_path()
                        if self._integrations_root is None:
                            from deerflow.config.paths import get_paths

                            integrations_root = get_paths().integration_skills_dir()
                        else:
                            integrations_root = self._integrations_root
                        self._project_skill_packages(
                            manifest.get("public_skills"),
                            Path(skills_root) / "public",
                        )
                        self._project_skill_packages(
                            manifest.get("managed_integrations"),
                            Path(integrations_root),
                        )
                    if self._config_path.exists():
                        try:
                            raw = json.loads(self._config_path.read_text(encoding="utf-8"))
                        except (OSError, json.JSONDecodeError) as exc:
                            raise ToolPlaneRevisionError("projection_failed") from exc
                    else:
                        raw = {}
                    if not isinstance(raw, dict):
                        raise ToolPlaneRevisionError("projection_failed")
                    raw["mcpServers"] = runtime_projection["mcpServers"]
                    raw.pop("mcp_servers", None)
                    raw["skills"] = runtime_projection["skills"]
                    # Validate the exact unresolved selector form before it
                    # becomes active; do not call from_file(), which resolves
                    # environment values and would pull secrets into this path.
                    ExtensionsConfig.model_validate(raw)
                    atomic_write_extensions_config(self._config_path, raw)
                    atomic_write_extensions_config(
                        active_path,
                        {"version": 1, "content_digest": desired_digest},
                    )
                    reload_extensions_config()
        else:
            storage = self._user_storage_factory(storage_subject_id)
            with skill_projection_mutation(storage, "user"):
                with extensions_config_file_lock(active_path):
                    if self._artifact_store is not None:
                        self._project_skill_packages(
                            manifest.get("custom_skills"),
                            storage.get_user_custom_root(),
                        )
                    states: dict[str, dict[str, bool]] = {}
                    for field_name, identifier_field in (
                        ("custom_skills", "name"),
                        ("managed_integration_enablement", "id"),
                        ("skill_states", "name"),
                    ):
                        values = manifest.get(field_name, [])
                        if not isinstance(values, list):
                            raise ToolPlaneRevisionError("projection_failed")
                        for value in values:
                            if not isinstance(value, Mapping):
                                raise ToolPlaneRevisionError("projection_failed")
                            identifier = value.get(identifier_field)
                            if not isinstance(identifier, str):
                                raise ToolPlaneRevisionError("projection_failed")
                            states[identifier] = {"enabled": bool(value.get("enabled", True))}
                    storage._write_skill_states(states)
                    atomic_write_extensions_config(
                        active_path,
                        {"version": 1, "content_digest": desired_digest},
                    )
        observed = self._observed_digest_sync(scope, storage_subject_id)
        if observed != desired_digest:
            raise ToolPlaneRevisionError("projection_digest_mismatch")
        return observed

    async def observed_digest(
        self,
        scope: ToolPlaneRevisionScopeV1,
    ) -> str | None:
        return await asyncio.to_thread(self._observed_digest_sync, scope)

    async def observed_digest_for_actor(
        self,
        scope: ToolPlaneRevisionScopeV1,
        *,
        storage_subject_id: str,
    ) -> str | None:
        """Observe an overlay only when its protected subject binding matches."""

        if scope.kind != "user_overlay":
            raise ToolPlaneRevisionError("projection_failed")
        return await asyncio.to_thread(
            self._observed_digest_sync,
            scope,
            storage_subject_id,
        )

    def _observed_digest_sync(
        self,
        scope: ToolPlaneRevisionScopeV1,
        expected_storage_subject_id: str | None = None,
    ) -> str | None:
        active_path = self._active_path(scope)
        if not active_path.exists():
            return None
        try:
            pointer = json.loads(active_path.read_text(encoding="utf-8"))
            digest = pointer["content_digest"]
            if not isinstance(digest, str):
                return _DRIFT_DIGEST
            projection_path = self._revision_path(scope, digest)
            payload = json.loads(projection_path.read_text(encoding="utf-8"))
            manifest = payload["manifest"]
            if canonical_tool_plane_digest(manifest) != digest:
                return _DRIFT_DIGEST
            expected_runtime = payload["runtime_projection"]
            if scope.kind == "deployment_base":
                current = json.loads(self._config_path.read_text(encoding="utf-8"))
                if not isinstance(current, Mapping):
                    return _DRIFT_DIGEST
                current_runtime = {
                    "mcpServers": current.get(
                        "mcpServers",
                        current.get("mcp_servers", {}),
                    ),
                    "skills": current.get("skills", {}),
                }
                # Do not hash or retain mismatching bytes: a direct edit may
                # contain a credential value.  Equality decides only whether
                # to return the promoted digest or the constant drift marker.
                if current_runtime != expected_runtime:
                    return _DRIFT_DIGEST
                if self._artifact_store is not None:
                    try:
                        if self._skills_root is None:
                            storage = get_or_new_skill_storage()
                            skills_root = storage.get_skills_root_path()
                        else:
                            skills_root = self._skills_root
                        if self._integrations_root is None:
                            from deerflow.config.paths import get_paths

                            integrations_root = get_paths().integration_skills_dir()
                        else:
                            integrations_root = self._integrations_root
                        if not self._skill_packages_match(
                            manifest.get("public_skills"),
                            Path(skills_root) / "public",
                        ) or not self._skill_packages_match(
                            manifest.get("managed_integrations"),
                            Path(integrations_root),
                        ):
                            return _DRIFT_DIGEST
                    except Exception:
                        return _DRIFT_DIGEST
            else:
                storage_subject_id = payload.get("storage_subject_id")
                if not isinstance(storage_subject_id, str):
                    return _DRIFT_DIGEST
                if expected_storage_subject_id is not None and storage_subject_id != expected_storage_subject_id:
                    return _DRIFT_DIGEST
                storage = self._user_storage_factory(storage_subject_id)
                if self._artifact_store is not None and not self._skill_packages_match(
                    manifest.get("custom_skills"),
                    storage.get_user_custom_root(),
                ):
                    return _DRIFT_DIGEST
                expected_states: dict[str, dict[str, bool]] = {}
                for field_name, identifier_field in (
                    ("custom_skills", "name"),
                    ("managed_integration_enablement", "id"),
                    ("skill_states", "name"),
                ):
                    values = manifest.get(field_name, [])
                    if not isinstance(values, list):
                        return _DRIFT_DIGEST
                    for value in values:
                        if not isinstance(value, Mapping):
                            return _DRIFT_DIGEST
                        identifier = value.get(identifier_field)
                        if not isinstance(identifier, str):
                            return _DRIFT_DIGEST
                        expected_states[identifier] = {"enabled": bool(value.get("enabled", True))}
                if storage._read_skill_states() != expected_states:
                    return _DRIFT_DIGEST
            return digest
        except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
            return _DRIFT_DIGEST

    def _project_skill_packages(self, entries: object, live_root: Path) -> None:
        if not isinstance(entries, list) or self._artifact_store is None:
            raise ToolPlaneRevisionError("projection_failed")
        live_root.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix=f".{live_root.name}.governed-",
            dir=live_root.parent,
        ) as temporary:
            staged_root = Path(temporary)
            for raw in entries:
                if not isinstance(raw, Mapping):
                    raise ToolPlaneRevisionError("projection_failed")
                name = raw.get("name")
                if not isinstance(name, str):
                    raise ToolPlaneRevisionError("projection_failed")
                try:
                    verified = self._artifact_store.verify(
                        tree_digest=str(raw.get("tree_digest")),
                        archive_digest=str(raw.get("archive_digest")),
                        manifest_digest=str(raw.get("manifest_digest")),
                    )
                except ToolPlaneRevisionError as exc:
                    raise ToolPlaneRevisionError("projection_failed") from exc
                shutil.copytree(verified.package_root, staged_root / name)
            live_root.mkdir(parents=True, exist_ok=True)
            desired = {path.name for path in staged_root.iterdir()}
            for existing in live_root.iterdir():
                if existing.name.startswith("."):
                    continue
                if existing.name not in desired:
                    if existing.is_dir() and not existing.is_symlink():
                        shutil.rmtree(existing)
                    else:
                        existing.unlink()
            for staged in staged_root.iterdir():
                target = live_root / staged.name
                if target.exists() or target.is_symlink():
                    if target.is_dir() and not target.is_symlink():
                        shutil.rmtree(target)
                    else:
                        target.unlink()
                os.replace(staged, target)

    @staticmethod
    def _skill_packages_match(entries: object, live_root: Path) -> bool:
        if not isinstance(entries, list):
            return False
        expected: dict[str, str] = {}
        for raw in entries:
            if not isinstance(raw, Mapping):
                return False
            name = raw.get("name")
            digest = raw.get("tree_digest")
            if not isinstance(name, str) or not isinstance(digest, str):
                return False
            expected[name] = digest
        if not live_root.exists():
            return not expected
        observed_names = {path.name for path in live_root.iterdir() if not path.name.startswith(".")}
        if observed_names != set(expected):
            return False
        return all(compute_skill_tree_digest(live_root / name) == digest for name, digest in expected.items())


__all__ = ["LockedFileToolPlaneProjection"]
