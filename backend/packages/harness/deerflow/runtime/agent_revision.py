"""Resolve immutable lead-agent factory material at invocation acceptance."""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

from deerflow.config.agents_config import AgentConfig, load_agent_soul, validate_agent_name
from deerflow.config.app_config import AppConfig
from deerflow.persistence.agents import make_agent_store
from deerflow.runtime.accepted_invocation import ResolvedAgentMaterialV1, ResolvedAgentRevision, canonical_digest
from deerflow.runtime.skill_snapshot import snapshot_effective_skills

RESOLVED_AGENT_MATERIAL_CONTEXT_KEY = "__deerflow_resolved_agent_material_v1"

# Guarded split: every AgentConfig field is explicitly either a graph-factory
# input or excluded as descriptive/source-delivery metadata.
AGENT_CONFIG_FACTORY_INCLUDED_FIELDS = frozenset(
    {
        "model",
        "tool_groups",
        "skills",
        "model_settings",
        "thinking_enabled",
        "reasoning_effort",
    }
)
AGENT_CONFIG_FACTORY_EXCLUDED_FIELDS = frozenset({"name", "description", "github"})

APP_CONFIG_FACTORY_INCLUDED_FIELDS = frozenset(
    {
        "token_usage",
        "token_budget",
        "models",
        "sandbox",
        "tools",
        "tool_groups",
        "skills",
        "skill_scan",
        "skill_evolution",
        "extensions",
        "tool_output",
        "tool_search",
        "title",
        "summarization",
        "memory",
        "acp_agents",
        "subagents",
        "guardrails",
        "authorization",
        "circuit_breaker",
        "llm_call",
        "loop_detection",
        "tool_progress",
        "read_before_write",
        "safety_finish_reason",
        "database",
        "checkpointer",
    }
)
APP_CONFIG_FACTORY_EXCLUDED_FIELDS = frozenset(
    {
        "log_level",
        "logging",
        "plugins",
        "required_capabilities",
        "max_recursion_limit",
        "agents_api",
        "input_polish",
        "suggestions",
        "channel_connections",
        "auth",
        "run_events",
        "deployment",
        "agent_storage",
        "scheduler",
        "mcp_tasks",
        "stream_bridge",
        "run_ownership",
        "dedupe_storage",
    }
)


def assert_agent_config_projection_complete() -> None:
    """Assert every agent-config field has an explicit revision classification."""

    classified = AGENT_CONFIG_FACTORY_INCLUDED_FIELDS | AGENT_CONFIG_FACTORY_EXCLUDED_FIELDS
    actual = frozenset(AgentConfig.model_fields)
    if classified != actual:
        missing = sorted(actual - classified)
        stale = sorted(classified - actual)
        raise AssertionError(f"AgentConfig revision projector classification drifted; missing={missing}, stale={stale}")


def assert_app_config_projection_complete() -> None:
    """Assert every app-config field has an explicit revision classification."""

    classified = APP_CONFIG_FACTORY_INCLUDED_FIELDS | APP_CONFIG_FACTORY_EXCLUDED_FIELDS
    actual = frozenset(AppConfig.model_fields)
    if classified != actual:
        missing = sorted(actual - classified)
        stale = sorted(classified - actual)
        raise AssertionError(f"AppConfig revision projector classification drifted; missing={missing}, stale={stale}")


def _runtime_config(config: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(config.get("configurable", {}) or {})
    context = config.get("context", {}) or {}
    if isinstance(context, Mapping):
        result.update(context)
    return result


def _is_secret_key(key: str) -> bool:
    normalized = key.lower()
    return any(
        marker in normalized
        for marker in (
            "api_key",
            "apikey",
            "password",
            "secret",
            "credential",
            "access_token",
            "auth_token",
            "github_token",
        )
    )


def _safe_settings(value: Any, *, path: str = "config") -> Any:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for raw_key, item in sorted(value.items(), key=lambda pair: str(pair[0])):
            key = str(raw_key)
            child_path = f"{path}.{key}"
            if _is_secret_key(key):
                result[key] = {"secret_handle_id": child_path}
            else:
                result[key] = _safe_settings(item, path=child_path)
        return result
    if isinstance(value, (list, tuple)):
        return [_safe_settings(item, path=f"{path}[]") for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _skills(app_config: AppConfig, *, user_id: str | None) -> tuple[tuple[Any, ...], tuple[Any, ...]]:
    from deerflow.skills.storage import get_or_new_skill_storage, get_or_new_user_skill_storage

    storage = get_or_new_user_skill_storage(user_id, app_config=app_config) if user_id else get_or_new_skill_storage(app_config=app_config)
    all_skills = tuple(storage.load_skills(enabled_only=False))
    enabled = tuple(skill for skill in all_skills if skill.enabled)
    return enabled, all_skills


def resolve_agent_revision(
    config: Mapping[str, Any],
    *,
    app_config: AppConfig,
    user_id: str | None,
) -> ResolvedAgentRevision:
    """Resolve current material once and return the exact captured object."""
    assert_agent_config_projection_complete()
    assert_app_config_projection_complete()
    cfg = _runtime_config(config)
    is_bootstrap = bool(cfg.get("is_bootstrap", False))
    agent_name = validate_agent_name(cfg.get("agent_name"))
    agent_id = "bootstrap" if is_bootstrap else (agent_name or "default")

    agent_config: AgentConfig | None = None
    soul: str | None = None
    if agent_name and not is_bootstrap:
        snapshot = make_agent_store(app_config).snapshot(agent_name, user_id=user_id)
        agent_config = snapshot.config
        soul = snapshot.soul
        storage_source = snapshot.source
        storage_version = snapshot.version
    else:
        soul = load_agent_soul(None, user_id=user_id)
        storage_source = "builtin"
        storage_version = canonical_digest({"agent_id": agent_id, "soul": soul or ""})

    # Re-validate independent copies so later config reloads or caller mutation
    # cannot alter the material used by the worker.
    pinned_app_config = type(app_config).model_validate(copy.deepcopy(app_config.model_dump(mode="python")))
    pinned_agent_config = None if agent_config is None else AgentConfig.model_validate(copy.deepcopy(agent_config.model_dump(mode="python")))

    requested_model = cfg.get("model_name") or cfg.get("model")
    agent_model = pinned_agent_config.model if pinned_agent_config and pinned_agent_config.model else None
    selected_model = requested_model or agent_model or (pinned_app_config.models[0].name if pinned_app_config.models else None)
    model_config = pinned_app_config.get_model_config(selected_model) if selected_model else None
    if model_config is None and pinned_app_config.models:
        model_config = pinned_app_config.models[0]
    model_profile = _safe_settings(model_config or {}, path="models.selected")
    model_profile["app_execution_digest"] = canonical_digest(
        {
            "version": 1,
            "app_config": {
                field_name: _safe_settings(
                    getattr(pinned_app_config, field_name),
                    path=f"app_config.{field_name}",
                )
                for field_name in sorted(APP_CONFIG_FACTORY_INCLUDED_FIELDS)
            },
        }
    )

    groups = tuple(pinned_agent_config.tool_groups or ()) if pinned_agent_config else ()
    configured_tools = tuple(sorted(tool.name for tool in pinned_app_config.tools if not groups or tool.group in groups))

    enabled_skills, _ = _skills(pinned_app_config, user_id=user_id)
    if is_bootstrap:
        available_names: set[str] | None = {"bootstrap"}
    elif pinned_agent_config and pinned_agent_config.skills is not None:
        available_names = set(pinned_agent_config.skills)
    else:
        available_names = None
    live_effective_skills = tuple(skill for skill in enabled_skills if available_names is None or skill.name in available_names)
    skill_snapshot = snapshot_effective_skills(
        live_effective_skills,
        user_id=user_id,
    )
    effective_skills = skill_snapshot.skills if skill_snapshot is not None else ()
    skill_projection = tuple(projection.to_json() for projection in skill_snapshot.projections) if skill_snapshot is not None else ()

    agent_projection = None
    if pinned_agent_config is not None:
        raw = pinned_agent_config.model_dump(mode="json")
        agent_projection = {key: _safe_settings(raw.get(key), path=f"agent_config.{key}") for key in sorted(AGENT_CONFIG_FACTORY_INCLUDED_FIELDS)}

    def _option(key: str, agent_value: Any, default: Any) -> Any:
        if key in cfg:
            return cfg[key]
        return agent_value if agent_value is not None else default

    runtime_defaults = {
        "agent_name": agent_name,
        "is_bootstrap": is_bootstrap,
        "thinking_enabled": bool(
            _option(
                "thinking_enabled",
                pinned_agent_config.thinking_enabled if pinned_agent_config else None,
                True,
            )
        ),
        "reasoning_effort": _option(
            "reasoning_effort",
            pinned_agent_config.reasoning_effort if pinned_agent_config else None,
            None,
        ),
        "is_plan_mode": bool(cfg.get("is_plan_mode", False)),
        "subagent_enabled": bool(cfg.get("subagent_enabled", False)),
        "max_concurrent_subagents": int(cfg.get("max_concurrent_subagents", 3)),
        "max_total_subagents": int(cfg.get("max_total_subagents", pinned_app_config.subagents.max_total_per_run)),
        "non_interactive": bool(cfg.get("non_interactive", False)),
        "channel_name": cfg.get("channel_name"),
    }
    try:
        material = ResolvedAgentMaterialV1(
            agent_id=agent_id,
            storage_source=storage_source,
            storage_version=storage_version,
            agent_config=agent_projection,
            soul=soul or "",
            model_profile=model_profile,
            tool_groups=groups,
            tools=configured_tools,
            skills=skill_projection,
            runtime_defaults=runtime_defaults,
            app_config=pinned_app_config,
            agent_config_object=pinned_agent_config,
            enabled_skill_objects=effective_skills,
            # An accepted invocation intentionally exposes only effective skills.
            # Disabled/live registry entries are not executable revision material.
            all_skill_objects=effective_skills,
            user_id=user_id,
            skill_snapshot=skill_snapshot,
        )
        return ResolvedAgentRevision.from_material(material)
    except Exception:
        if skill_snapshot is not None:
            skill_snapshot.release()
        raise


__all__ = [
    "AGENT_CONFIG_FACTORY_EXCLUDED_FIELDS",
    "AGENT_CONFIG_FACTORY_INCLUDED_FIELDS",
    "APP_CONFIG_FACTORY_EXCLUDED_FIELDS",
    "APP_CONFIG_FACTORY_INCLUDED_FIELDS",
    "RESOLVED_AGENT_MATERIAL_CONTEXT_KEY",
    "assert_agent_config_projection_complete",
    "assert_app_config_projection_complete",
    "resolve_agent_revision",
]
