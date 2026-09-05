"""Boundary check: raw provider handles are resolved in a short list of modules.

Every sandbox operation crosses the session an execution declared (rule 5 of
``docs/ACCEPTED_SANDBOX_EXECUTION.md``). That holds only while the raw
provider verbs, ``get``, ``acquire`` and ``acquire_async``, are called from
the modules that own resolution: the session provider, upstream's registry,
middleware and tools, the Gateway request lease, providers themselves, and
the ordinary fallbacks that first ask ``declared_sandbox()``. This test scans
the harness and the Gateway and fails on any other caller. Adding a module to
the allowlist is the opt-out, and it is visible in the diff.
"""

from __future__ import annotations

import ast
from pathlib import Path

BACKEND = Path(__file__).resolve().parent.parent
SCANNED_ROOTS = (BACKEND / "packages" / "harness" / "deerflow", BACKEND / "app")

PROVIDER_VERBS = frozenset({"get", "acquire", "acquire_async"})
PROVIDER_FACTORIES = frozenset(
    {
        "get_sandbox_provider",
        "get_initialized_sandbox_provider",
        "sandbox_session_provider",
        "unwrap_sandbox_provider",
        "lifecycle_sandbox_provider",
    }
)
PROVIDER_FIELDS = frozenset({"_provider", "provider", "_backing", "sandbox_provider"})
PROVIDER_PARAMETERS = frozenset({"sandbox_provider"})
PROVIDER_TYPES = frozenset({"SandboxProvider", "SessionProvider"})

ALLOWED_CALLERS = frozenset(
    {
        "packages/harness/deerflow/sandbox/session.py",  # the session provider dispatches by Kind
        "packages/harness/deerflow/sandbox/lease.py",  # upstream's registry
        "packages/harness/deerflow/sandbox/middleware.py",  # upstream's eager acquisition and release
        "packages/harness/deerflow/sandbox/tools.py",  # upstream's lazy initialization
        "packages/harness/deerflow/subagents/executor.py",  # upstream's persistent-shell tri-state read
        "packages/harness/deerflow/agents/middlewares/tool_output_budget_middleware.py",  # ordinary fallback after declared_sandbox()
        "app/gateway/authz.py",  # upstream's request lease
    }
)
ALLOWED_TREES = (
    "packages/harness/deerflow/sandbox/local/",  # providers own their handles
    "packages/harness/deerflow/community/",  # providers and their materializers
)


def _annotation_names(annotation: ast.expr | None) -> set[str]:
    names: set[str] = set()
    if annotation is None:
        return names
    for node in ast.walk(annotation):
        if isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            names.add(node.attr)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            names.add(node.value.split("|")[0].strip())
    return names


def _is_factory_call(node: ast.expr) -> bool:
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    name = func.id if isinstance(func, ast.Name) else func.attr if isinstance(func, ast.Attribute) else None
    return name in PROVIDER_FACTORIES


def _provider_names(scope: ast.AST) -> set[str]:
    """Names bound to a provider inside ``scope``: factory results and provider parameters."""
    names: set[str] = set()
    if isinstance(scope, ast.FunctionDef | ast.AsyncFunctionDef):
        arguments = scope.args
        for argument in [*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs]:
            if argument.arg in PROVIDER_PARAMETERS or _annotation_names(argument.annotation) & PROVIDER_TYPES:
                names.add(argument.arg)
    for node in ast.walk(scope):
        if isinstance(node, ast.Assign) and _is_factory_call(node.value):
            names.update(target.id for target in node.targets if isinstance(target, ast.Name))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if _is_factory_call(node.value) or _annotation_names(node.annotation) & PROVIDER_TYPES:
                names.add(node.target.id)
    return names


def _is_provider_receiver(node: ast.expr, provider_names: set[str]) -> bool:
    if _is_factory_call(node):
        return True
    if isinstance(node, ast.Name):
        return node.id in provider_names
    if isinstance(node, ast.Attribute):
        return node.attr in PROVIDER_FIELDS and isinstance(node.value, ast.Name) and node.value.id == "self"
    return False


def provider_verb_calls(source: str) -> list[int]:
    """Line numbers of raw provider verb calls in ``source``."""
    tree = ast.parse(source)
    scopes: list[ast.AST] = [tree, *(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef))]
    lines: set[int] = set()
    for scope in scopes:
        provider_names = _provider_names(scope)
        body = scope.body if isinstance(scope, ast.Module) else scope
        for node in ast.walk(body) if not isinstance(scope, ast.Module) else (child for statement in body for child in ast.walk(statement)):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr in PROVIDER_VERBS and _is_provider_receiver(node.func.value, provider_names):
                lines.add(node.lineno)
    return sorted(lines)


def _scan() -> dict[str, list[int]]:
    found: dict[str, list[int]] = {}
    for root in SCANNED_ROOTS:
        for path in sorted(root.rglob("*.py")):
            relative = path.relative_to(BACKEND).as_posix()
            lines = provider_verb_calls(path.read_text(encoding="utf-8"))
            if lines:
                found[relative] = lines
    return found


def _is_allowed(relative: str) -> bool:
    return relative in ALLOWED_CALLERS or relative.startswith(ALLOWED_TREES)


def test_the_scanner_recognizes_provider_receivers() -> None:
    source = """
from deerflow.sandbox.sandbox_provider import SandboxProvider, get_sandbox_provider

def direct(sandbox_id):
    return get_sandbox_provider().get(sandbox_id)

def bound(thread_id):
    provider = get_sandbox_provider()
    return provider.acquire(thread_id, user_id="u")

async def typed(provider: SandboxProvider | None, thread_id):
    return await provider.acquire_async(thread_id)

class Wrapper:
    def get(self, sandbox_id):
        return self._backing.get(sandbox_id)

async def request_lease(sandbox_provider, sandbox_id):
    return sandbox_provider.get(sandbox_id)
"""
    assert provider_verb_calls(source) == [5, 9, 12, 16, 19]


def test_the_scanner_ignores_dictionaries_locks_and_contexts() -> None:
    source = """
def other(by_provider, lock, context, provider):
    by_provider.get("x")
    lock.acquire()
    context.get("thread_id")
    provider.get("looks like one but was never bound")
"""
    assert provider_verb_calls(source) == []


def test_raw_provider_handles_are_resolved_only_in_the_allowed_modules() -> None:
    violations = [f"  {relative}:{','.join(map(str, lines))}" for relative, lines in _scan().items() if not _is_allowed(relative)]

    assert not violations, "Raw provider verbs outside the resolution modules; resolve through the declared session instead, or add the module to ALLOWED_CALLERS:\n" + "\n".join(violations)


def test_the_allowlist_names_only_modules_that_still_call_provider_verbs() -> None:
    found = _scan()

    stale = sorted(relative for relative in ALLOWED_CALLERS if relative not in found)
    assert not stale, f"Allowlisted modules no longer call provider verbs; remove them: {stale}"
