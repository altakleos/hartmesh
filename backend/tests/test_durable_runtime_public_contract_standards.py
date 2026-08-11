"""Standards closure for durable-runtime harness exports and test providers."""

from __future__ import annotations

import ast
from pathlib import Path

_BACKEND_ROOT = Path(__file__).resolve().parents[1]

_DOCUMENTED_EXPORTS = {
    "packages/harness/deerflow/runtime/accepted_invocation.py": {
        "AcceptedInvocation",
        "InvocationOrigin",
        "PrincipalProjection",
        "ResolvedAgentRevision",
        "canonical_digest",
    },
    "packages/harness/deerflow/runtime/agent_revision.py": {
        "assert_agent_config_projection_complete",
        "assert_app_config_projection_complete",
    },
    "packages/harness/deerflow/extensions/mcp.py": {
        "McpInterceptorDiagnostic",
        "McpInterceptorRuntime",
        "mcp_invocation_facts_from_context",
        "reset_mcp_interceptor_runtime",
    },
    "packages/harness/deerflow/extensions/capabilities.py": {
        "CapabilityHealthSnapshot",
        "CapabilityManifest",
        "CapabilityManifestEntry",
        "CapabilityPluginManifestEntry",
        "CapabilityReadinessSnapshot",
    },
    "packages/harness/deerflow/runtime/runs/lifecycle_query.py": {
        "LifecyclePage",
        "decode_lifecycle_cursor",
        "encode_lifecycle_cursor",
        "invocation_source_kind",
        "validate_cursor_window",
    },
}

_FIXTURE_SIGNATURES = {
    ("CountingAuthorizationProvider", "authorize"): (
        {"request": "AuthzRequest"},
        "AuthzDecision",
    ),
    ("CountingAuthorizationProvider", "aauthorize"): (
        {"request": "AuthzRequest"},
        "AuthzDecision",
    ),
    ("CountingAuthorizationProvider", "filter_resources"): (
        {
            "principal": "Principal",
            "resource_type": "str",
            "candidates": "list[str]",
        },
        "list[str]",
    ),
    ("_PreparedMcpInterceptor", "prepare_call"): (
        {"request": "McpCallProjectionV1"},
        "PreparedMcpCallV1",
    ),
    ("_ConstraintsProviderV2", "project"): (
        {"request": "ConstraintProjectionRequestV2"},
        "Never",
    ),
}


def _module(path: str) -> ast.Module:
    return ast.parse((_BACKEND_ROOT / path).read_text(encoding="utf-8"))


def test_durable_runtime_explicit_exports_have_contract_docstrings() -> None:
    for path, expected_exports in _DOCUMENTED_EXPORTS.items():
        module = _module(path)
        definitions = {node.name: node for node in module.body if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))}
        all_assignment = next(node for node in module.body if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets))
        assert isinstance(all_assignment.value, (ast.List, ast.Tuple))
        exports = {element.value for element in all_assignment.value.elts if isinstance(element, ast.Constant) and isinstance(element.value, str)}

        assert expected_exports <= exports
        assert {name for name in expected_exports if ast.get_docstring(definitions[name]) is None} == set()


def test_demo_extension_protocol_fixtures_have_exact_annotations() -> None:
    module = _module("extension_test_fixtures/demo_extensions.py")
    classes = {node.name: node for node in module.body if isinstance(node, ast.ClassDef)}

    for (class_name, method_name), (parameters, return_annotation) in _FIXTURE_SIGNATURES.items():
        method = next(node for node in classes[class_name].body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == method_name)
        assert [argument.arg for argument in method.args.args] == [
            "self",
            *parameters,
        ]
        annotations = {argument.arg: ast.unparse(argument.annotation) for argument in method.args.args if argument.annotation is not None}
        assert annotations == parameters
        assert method.returns is not None
        assert ast.unparse(method.returns) == return_annotation
