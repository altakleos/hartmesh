"""Canonical and secret-safe tool-plane revision contracts."""

from __future__ import annotations

import json

import pytest

from deerflow.config.app_config import AppConfig
from deerflow.config.extensions_config import ExtensionsConfig
from deerflow.tool_plane import (
    EffectiveToolPlaneRevisionV1,
    ToolPlaneRevisionError,
    ToolPlaneRevisionScopeV1,
    canonicalize_deployment_candidate,
    canonicalize_user_overlay_candidate,
    resolve_tool_plane_runtime,
)

_DIGEST = "a" * 64


def test_deployment_candidate_digest_is_stable_across_input_order() -> None:
    first = canonicalize_deployment_candidate(
        {
            "validation_policy_digest": _DIGEST,
            "mcp_servers": {
                "search": {
                    "type": "http",
                    "url": "https://mcp.example.test/api",
                    "enabled": True,
                    "headers": {"X-API-Key": "$SEARCH_API_KEY"},
                    "tools": {"lookup": {}, "crawl": {}},
                }
            },
            "public_skills": {
                "research": {
                    "enabled": True,
                    "tree_digest": "b" * 64,
                    "manifest_digest": "c" * 64,
                    "entry_points": ["SKILL.md"],
                }
            },
            "managed_integrations": [],
            "change_summary": "Enable reviewed search material",
        }
    )
    second = canonicalize_deployment_candidate(
        {
            "managed_integrations": [],
            "public_skills": {
                "research": {
                    "entry_points": ["SKILL.md"],
                    "manifest_digest": "c" * 64,
                    "tree_digest": "b" * 64,
                    "enabled": True,
                }
            },
            "change_summary": "Enable reviewed search material",
            "mcp_servers": {
                "search": {
                    "tools": {"crawl": {}, "lookup": {}},
                    "headers": {"X-API-Key": "$SEARCH_API_KEY"},
                    "enabled": True,
                    "url": "https://mcp.example.test/api",
                    "transport": "http",
                }
            },
            "validation_policy_digest": _DIGEST,
        }
    )

    assert first.digest == second.digest
    assert first.to_json() == second.to_json()
    assert first.to_json()["mcp_servers"][0]["secret_selectors"] == [{"field": "headers.x-api-key", "selector": "env:SEARCH_API_KEY"}]


@pytest.mark.parametrize(
    "field,value",
    [
        ("headers", {"Authorization": "Bearer super-secret-token"}),
        ("env", {"API_KEY": "super-secret-token"}),
        ("oauth", {"client_secret": "super-secret-token"}),
        ("user_auth", {"users": {"user-1": "super-secret-token"}}),
    ],
)
def test_literal_mcp_secret_is_rejected_without_echoing_value(
    field: str,
    value: object,
) -> None:
    with pytest.raises(ToolPlaneRevisionError) as caught:
        canonicalize_deployment_candidate(
            {
                "validation_policy_digest": _DIGEST,
                "mcp_servers": {
                    "search": {
                        "type": "http",
                        "url": "https://mcp.example.test/api",
                        field: value,
                    }
                },
            }
        )

    assert caught.value.code == "secret_value_present"
    assert "super-secret-token" not in str(caught.value)
    assert "super-secret-token" not in json.dumps(caught.value.safe_details)


def test_user_overlay_is_bound_to_base_and_contains_only_selector_references() -> None:
    overlay = canonicalize_user_overlay_candidate(
        {
            "base_revision_digest": "d" * 64,
            "custom_skills": {
                "my-helper": {
                    "enabled": True,
                    "tree_digest": "e" * 64,
                    "manifest_digest": "f" * 64,
                    "entry_points": ["SKILL.md"],
                }
            },
            "mcp_enablement": {"search": True},
            "managed_integration_enablement": {"github": False},
            "credential_selectors": {
                "search": {
                    "binding_ref": "credential-binding:search-primary",
                    "version": 3,
                }
            },
            "skill_states": {"my-helper": {"enabled": True}},
        }
    )

    assert overlay.base_revision_digest == "d" * 64
    assert overlay.to_json()["credential_selectors"] == [
        {
            "binding_ref": "credential-binding:search-primary",
            "server_id": "search",
            "version": 3,
        }
    ]


def test_scope_rejects_user_reference_on_deployment_base() -> None:
    with pytest.raises(ValueError, match="user_ref"):
        ToolPlaneRevisionScopeV1(kind="deployment_base", user_ref="user-forged")


def test_mcp_canonicalization_preserves_safe_runtime_structure() -> None:
    revision = canonicalize_deployment_candidate(
        {
            "validation_policy_digest": _DIGEST,
            "mcp_servers": {
                "reports": {
                    "type": "http",
                    "url": "https://reports.example.test/mcp",
                    "description": "Reviewed reporting tools",
                    "routing": {
                        "mode": "prefer",
                        "priority": 50,
                        "keywords": ["report", "analysis"],
                    },
                    "tools": {
                        "submit_report": {
                            "routing": {
                                "mode": "prefer",
                                "priority": 80,
                                "keywords": ["submit"],
                            }
                        }
                    },
                    "tool_name_prefix": False,
                    "tool_call_timeout": 30,
                    "session_init_timeout": 15,
                    "credential_binding_id": "reports-primary",
                    "credential_version": 4,
                    "task_toolsets": [
                        {
                            "name": "report",
                            "submit_tool": "submit_report",
                            "status_tool": "report_status",
                            "cancel_tool": "cancel_report",
                        }
                    ],
                }
            },
        }
    )

    server = revision.to_json()["mcp_servers"][0]
    assert server["description"] == "Reviewed reporting tools"
    assert server["routing"]["priority"] == 50
    assert server["tool_overrides"]["submit_report"]["routing"]["priority"] == 80
    assert server["tool_name_prefix"] is False
    assert server["tool_call_timeout"] == 30.0
    assert server["session_init_timeout"] == 15.0
    assert server["credential_binding"] == {
        "binding_ref": "reports-primary",
        "version": 4,
    }
    assert server["task_toolsets"][0]["submit_tool"] == "submit_report"


def test_mcp_canonicalization_preserves_argument_order_and_duplicates() -> None:
    revision = canonicalize_deployment_candidate(
        {
            "validation_policy_digest": _DIGEST,
            "mcp_servers": {
                "local": {
                    "type": "stdio",
                    "command": "npx",
                    "args": ["--package", "zeta", "--package", "alpha"],
                }
            },
        }
    )

    assert revision.to_json()["mcp_servers"][0]["args"] == [
        "--package",
        "zeta",
        "--package",
        "alpha",
    ]


def test_effective_runtime_resolves_selectors_without_changing_evidence(
    monkeypatch,
) -> None:
    monkeypatch.setenv("SEARCH_TOKEN", "runtime-only-secret")
    base = canonicalize_deployment_candidate(
        {
            "validation_policy_digest": _DIGEST,
            "mcp_servers": {
                "search": {
                    "type": "http",
                    "url": "https://mcp.example.test",
                    "headers": {"Authorization": "$SEARCH_TOKEN"},
                    "tools": {"lookup": {}},
                }
            },
        }
    )
    effective = EffectiveToolPlaneRevisionV1(
        base_revision_digest="b" * 64,
        user_overlay_digest="c" * 64,
        base_generation=1,
        overlay_generation=0,
        projection_digest="d" * 64,
        effective_mcp_server_ids=("search",),
        effective_mcp_servers=base.mcp_servers,
    )
    app_config = AppConfig.model_construct(extensions=ExtensionsConfig())

    runtime = resolve_tool_plane_runtime(app_config, effective)

    assert runtime.app_config.extensions.mcp_servers["search"].headers["authorization"] == "runtime-only-secret"
    assert runtime.allowed_mcp_tools_by_server == {"search": frozenset({"lookup"})}
    serialized = json.dumps(effective.to_json(), sort_keys=True)
    assert "runtime-only-secret" not in serialized
    assert "env:SEARCH_TOKEN" in serialized


@pytest.mark.parametrize(
    "server_patch",
    [
        {"api_key": "literal-secret"},
        {"url": "https://user:password@example.test/mcp"},
        {"args": ["--token=literal-secret"]},
        {"oauth": {"token_url": "https://auth.example.test", "extra_token_params": {"api_key": "literal-secret"}}},
    ],
)
def test_mcp_unmodeled_or_embedded_secret_material_is_rejected(server_patch) -> None:
    server = {
        "type": "http",
        "url": "https://mcp.example.test",
        **server_patch,
    }
    with pytest.raises(ToolPlaneRevisionError) as caught:
        canonicalize_deployment_candidate(
            {
                "validation_policy_digest": _DIGEST,
                "mcp_servers": {"search": server},
            }
        )

    assert caught.value.code == "secret_value_present"
    assert "literal-secret" not in str(caught.value.safe_details)


def test_mcp_header_names_are_unique_case_insensitively() -> None:
    with pytest.raises(ToolPlaneRevisionError) as caught:
        canonicalize_deployment_candidate(
            {
                "validation_policy_digest": _DIGEST,
                "mcp_servers": {
                    "search": {
                        "type": "http",
                        "url": "https://mcp.example.test",
                        "headers": {
                            "Authorization": "$SEARCH_TOKEN",
                            "authorization": "$OTHER_TOKEN",
                        },
                    }
                },
            }
        )

    assert caught.value.code == "validation_failed"
    assert caught.value.safe_details["reason"] == "duplicate_identifier"
