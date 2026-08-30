"""Subagent registry for managing available subagents."""

import logging
import threading
import time
from collections.abc import Hashable, Mapping
from dataclasses import asdict, dataclass, replace
from types import MappingProxyType
from typing import Any, Literal

from deerflow.persistence.managed_subagents import ManagedSubagentDefinition, get_managed_subagent_store
from deerflow.sandbox.security import is_host_bash_allowed
from deerflow.subagents.builtins import BUILTIN_SUBAGENTS
from deerflow.subagents.config import SubagentConfig

logger = logging.getLogger(__name__)
_MANAGED_SIGNATURE_TTL_SECONDS = 1.0
_managed_definitions_cache_lock = threading.RLock()
_managed_definitions_cache: dict[Hashable, tuple[float, Hashable, tuple[ManagedSubagentDefinition, ...]]] = {}

type SubagentSourceKind = Literal["builtin", "config", "managed"]


@dataclass(frozen=True)
class _ResolvedSubagentRegistryEntry:
    """Typed provenance returned by the registry's admission seam."""

    name: str
    config: SubagentConfig
    source_kind: SubagentSourceKind
    source_material: Mapping[str, Any]

    def __post_init__(self) -> None:
        if self.config.name != self.name:
            raise ValueError("resolved subagent name does not match its config")
        object.__setattr__(
            self,
            "source_material",
            MappingProxyType(dict(self.source_material)),
        )


def _resolve_subagents_app_config(app_config: Any | None = None):
    if app_config is None:
        from deerflow.config.subagents_config import get_subagents_app_config

        return get_subagents_app_config()
    return getattr(app_config, "subagents", app_config)


def _build_custom_subagent_config(name: str, *, app_config: Any | None = None) -> SubagentConfig | None:
    """Build a SubagentConfig from config.yaml custom_agents section.

    Args:
        name: The name of the custom subagent.
        app_config: Optional AppConfig or SubagentsAppConfig to resolve from.

    Returns:
        SubagentConfig if found in custom_agents, None otherwise.
    """
    subagents_config = _resolve_subagents_app_config(app_config)
    custom = subagents_config.custom_agents.get(name)
    if custom is None:
        return None

    return SubagentConfig(
        name=name,
        description=custom.description,
        system_prompt=custom.system_prompt,
        tools=custom.tools,
        disallowed_tools=custom.disallowed_tools,
        skills=custom.skills,
        model=custom.model,
        max_turns=custom.max_turns,
        timeout_seconds=custom.timeout_seconds,
    )


def _clear_managed_definitions_cache() -> None:
    """Clear process-local registry snapshots (primarily for tests)."""
    with _managed_definitions_cache_lock:
        _managed_definitions_cache.clear()


def _managed_definitions(
    *,
    app_config: Any | None = None,
    force_refresh: bool = False,
) -> tuple[ManagedSubagentDefinition, ...]:
    """Load and cache deployment-managed definitions until their signature changes."""
    store_config = app_config if hasattr(app_config, "agent_storage") else None
    store = get_managed_subagent_store(store_config)
    if force_refresh:
        # Durable admission is the prospective-effect boundary. It must take
        # one current store snapshot even when an ordinary registry read primed
        # the short process-local cache immediately before an administrator
        # committed an edit (including from another Gateway process).
        return tuple(store.list())
    cache_key = store.cache_identity()

    with _managed_definitions_cache_lock:
        checked_at = time.monotonic()
        cached = _managed_definitions_cache.get(cache_key)
        # A prompt/catalog pass can resolve every managed name separately.
        # Avoid repeating the file stat sweep or SQL signature query for each
        # lookup while keeping cross-process changes visible within one second.
        if cached is not None and checked_at - cached[0] < _MANAGED_SIGNATURE_TTL_SECONDS:
            return cached[2]

        signature = store.signature()
        if cached is not None and cached[1] == signature:
            _managed_definitions_cache[cache_key] = (checked_at, signature, cached[2])
            return cached[2]

        definitions = tuple(store.list())
        _managed_definitions_cache[cache_key] = (checked_at, signature, definitions)
        return definitions


def _resolve_subagent_config(
    name: str,
    *,
    app_config: Any | None = None,
    _managed_snapshot: tuple[ManagedSubagentDefinition, ...] | None = None,
) -> _ResolvedSubagentRegistryEntry | None:
    """Resolve one definition plus its winning live source.

    This is the registry's internal provenance seam for durable admission.  It
    deliberately shares the exact precedence and override code used by
    :func:`get_subagent_config`, so snapshot callers never reproduce it.
    """

    source_kind: SubagentSourceKind
    source_material: dict[str, Any]
    config = BUILTIN_SUBAGENTS.get(name)
    if config is not None:
        source_kind = "builtin"
        source_material = asdict(config)
    else:
        subagents_config = _resolve_subagents_app_config(app_config)
        custom = subagents_config.custom_agents.get(name)
        if custom is not None:
            config = _build_custom_subagent_config(name, app_config=app_config)
            source_kind = "config"
            source_material = {"name": name, **custom.model_dump(mode="json")}
        else:
            managed_definitions = _managed_snapshot
            if managed_definitions is None:
                managed_definitions = _managed_definitions(app_config=app_config)
            managed = next(
                (definition for definition in managed_definitions if definition.name == name and definition.enabled),
                None,
            )
            if managed is None:
                return None
            config = SubagentConfig(
                name=managed.name,
                description=managed.description,
                system_prompt=managed.system_prompt,
                tools=managed.tools,
                disallowed_tools=managed.disallowed_tools,
                skills=managed.skills,
                model=managed.model,
                max_turns=managed.max_turns,
                timeout_seconds=managed.timeout_seconds,
            )
            source_kind = "managed"
            source_material = managed.model_dump(
                mode="json",
                exclude={"display_name"},
            )

    assert config is not None

    # Apply per-agent overrides from config.yaml. Only explicit per-agent
    # overrides apply to custom/managed definitions; global timeout/max-turn
    # defaults retain their historical built-in-only semantics.
    subagents_config = _resolve_subagents_app_config(app_config)
    is_builtin = source_kind == "builtin"
    agent_override = subagents_config.agents.get(name)
    overrides = {}

    if agent_override is not None and agent_override.timeout_seconds is not None:
        if agent_override.timeout_seconds != config.timeout_seconds:
            logger.debug("Subagent '%s': timeout overridden (%ss -> %ss)", name, config.timeout_seconds, agent_override.timeout_seconds)
            overrides["timeout_seconds"] = agent_override.timeout_seconds
    elif is_builtin and subagents_config.timeout_seconds != config.timeout_seconds:
        logger.debug("Subagent '%s': timeout from global default (%ss -> %ss)", name, config.timeout_seconds, subagents_config.timeout_seconds)
        overrides["timeout_seconds"] = subagents_config.timeout_seconds

    if agent_override is not None and agent_override.max_turns is not None:
        if agent_override.max_turns != config.max_turns:
            logger.debug("Subagent '%s': max_turns overridden (%s -> %s)", name, config.max_turns, agent_override.max_turns)
            overrides["max_turns"] = agent_override.max_turns
    elif is_builtin and subagents_config.max_turns is not None and subagents_config.max_turns != config.max_turns:
        logger.debug("Subagent '%s': max_turns from global default (%s -> %s)", name, config.max_turns, subagents_config.max_turns)
        overrides["max_turns"] = subagents_config.max_turns

    effective_model = subagents_config.get_model_for(name)
    if effective_model is not None and effective_model != config.model:
        logger.debug("Subagent '%s': model overridden (%s -> %s)", name, config.model, effective_model)
        overrides["model"] = effective_model

    effective_skills = subagents_config.get_skills_for(name)
    if effective_skills is not None and effective_skills != config.skills:
        logger.debug("Subagent '%s': skills overridden (%s -> %s)", name, config.skills, effective_skills)
        overrides["skills"] = effective_skills

    if overrides:
        config = replace(config, **overrides)
    return _ResolvedSubagentRegistryEntry(
        name=name,
        config=config,
        source_kind=source_kind,
        source_material=source_material,
    )


def get_subagent_config(name: str, *, app_config: Any | None = None) -> SubagentConfig | None:
    """Get a subagent configuration by name, with config.yaml overrides applied.

    Resolution order (mirrors Codex's config layering):
    1. Built-in subagents (general-purpose, bash)
    2. Custom subagents from config.yaml custom_agents section
    3. Enabled administrator-managed subagents
    4. Per-agent overrides from config.yaml agents section (timeout, max_turns, model, skills)

    Args:
        name: The name of the subagent.
        app_config: Optional AppConfig or SubagentsAppConfig to resolve overrides from.

    Returns:
        SubagentConfig if found (with any config.yaml overrides applied), None otherwise.
    """
    resolved = _resolve_subagent_config(name, app_config=app_config)
    return None if resolved is None else resolved.config


def list_subagents(*, app_config: Any | None = None, allowed_subagents: list[str] | None = None) -> list[SubagentConfig]:
    """List all available subagent configurations (with config.yaml overrides applied).

    Returns:
        List of all registered SubagentConfig instances (built-in + custom).
    """
    configs = []
    for name in get_subagent_names(app_config=app_config, allowed_subagents=allowed_subagents):
        config = get_subagent_config(name, app_config=app_config)
        if config is not None:
            configs.append(config)
    return configs


def _subagent_names_from_snapshot(
    *,
    app_config: Any | None,
    allowed_subagents: list[str] | None,
    managed_definitions: tuple[ManagedSubagentDefinition, ...],
) -> list[str]:
    names = list(BUILTIN_SUBAGENTS.keys())

    # Merge custom_agents from config.yaml
    subagents_config = _resolve_subagents_app_config(app_config)
    for custom_name in subagents_config.custom_agents:
        if custom_name not in names:
            names.append(custom_name)

    # Built-in and config.yaml definitions have operator-controlled precedence.
    # A managed definition that later conflicts remains persisted for the
    # Settings UI, but is excluded from runtime discovery.
    for definition in managed_definitions:
        if not definition.enabled:
            continue
        if definition.name in names:
            logger.debug("Managed subagent '%s' conflicts with a built-in or config.yaml definition and is excluded from runtime", definition.name)
            continue
        names.append(definition.name)

    if allowed_subagents is not None:
        allowed = set(allowed_subagents)
        names = [name for name in names if name in allowed]

    return names


def get_subagent_names(*, app_config: Any | None = None, allowed_subagents: list[str] | None = None) -> list[str]:
    """Get registered subagent names, optionally restricted by the caller policy.

    Returns:
        List of subagent names.
    """
    return _subagent_names_from_snapshot(
        app_config=app_config,
        allowed_subagents=allowed_subagents,
        managed_definitions=_managed_definitions(app_config=app_config),
    )


def _filter_available_subagent_names(
    names: list[str],
    *,
    app_config: Any | None,
) -> list[str]:
    try:
        host_bash_allowed = is_host_bash_allowed(app_config) if hasattr(app_config, "sandbox") else is_host_bash_allowed()
    except Exception:
        logger.debug("Could not determine host bash availability; exposing all subagents")
        return names

    if not host_bash_allowed:
        names = [name for name in names if name != "bash"]
    return names


def get_available_subagent_names(*, app_config: Any | None = None, allowed_subagents: list[str] | None = None) -> list[str]:
    """Get subagent names that should be exposed to the active runtime.

    Returns:
        List of subagent names visible to the current sandbox configuration.
    """
    return _filter_available_subagent_names(
        get_subagent_names(
            app_config=app_config,
            allowed_subagents=allowed_subagents,
        ),
        app_config=app_config,
    )


def _snapshot_resolved_subagent_configs(
    *,
    app_config: Any,
    allowed_subagents: list[str] | None,
) -> tuple[
    tuple[str, ...],
    tuple[_ResolvedSubagentRegistryEntry, ...],
]:
    """Resolve one coherent, cache-independent registry view for admission."""

    managed_definitions = _managed_definitions(
        app_config=app_config,
        force_refresh=True,
    )
    registered_names = _subagent_names_from_snapshot(
        app_config=app_config,
        allowed_subagents=allowed_subagents,
        managed_definitions=managed_definitions,
    )
    available_names = _filter_available_subagent_names(
        list(registered_names),
        app_config=app_config,
    )
    resolved: list[_ResolvedSubagentRegistryEntry] = []
    for name in available_names:
        definition = _resolve_subagent_config(
            name,
            app_config=app_config,
            _managed_snapshot=managed_definitions,
        )
        if definition is not None:
            resolved.append(definition)
    return tuple(registered_names), tuple(resolved)
