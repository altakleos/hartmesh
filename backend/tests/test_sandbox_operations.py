"""The operations module is the single declaration point for sandbox verbs.

Every public method on ``Sandbox`` must be declared exactly once in
``deerflow.sandbox.operations``. The declaration generates the operation kind,
the process-local envelope constructor, and the fenced facade method, so a new
upstream verb cannot arrive on the accepted facade as a silent passthrough.
"""

from __future__ import annotations

import abc
import dataclasses
import inspect

import pytest

from deerflow.sandbox import operations
from deerflow.sandbox.accepted_material import (
    AcceptedSandboxOperationKind,
    AcceptedSandboxOperationV1,
    _AcceptedSandboxFacade,
)
from deerflow.sandbox.operations import (
    SandboxOperationKind,
    assert_facade_covers_sandbox,
    assert_operations_cover_sandbox,
    fenced_sandbox_facade,
    sandbox_operations,
    sandbox_public_methods,
)
from deerflow.sandbox.sandbox import Sandbox

SAMPLE_CALLS: dict[str, tuple[tuple[object, ...], dict[str, object]]] = {
    "execute_command": (("echo hi",), {"timeout": 3.0}),
    "execute_command_in_scope": (("echo hi",), {"scope_id": "scope-1"}),
    "release_command_scope": (("scope-1",), {}),
    "read_file": (("/f",), {"start_line": 1}),
    "download_file": (("/f",), {}),
    "list_dir": (("/d",), {"max_depth": 3}),
    "write_file": (("/f", "text"), {"append": True}),
    "glob": (("/d", "*.md"), {"include_dirs": True}),
    "grep": (("/d", "needle"), {"glob": "*.py", "literal": True}),
    "update_file": (("/f", b"bytes"), {}),
}


def _parameters(signature: inspect.Signature, *, drop_self: bool) -> list[tuple[str, inspect._ParameterKind, object]]:
    params = list(signature.parameters.values())
    if drop_self:
        params = params[1:]
    return [(param.name, param.kind, param.default) for param in params]


def _bound(name: str, args: tuple[object, ...], kwargs: dict[str, object]) -> dict[str, object]:
    bound = inspect.signature(getattr(Sandbox, name)).bind(None, *args, **kwargs)
    bound.apply_defaults()
    return {key: value for key, value in bound.arguments.items() if key != "self"}


def _make_recording_sandbox() -> Sandbox:
    class Recording(Sandbox):
        def __init__(self) -> None:
            super().__init__("recording")
            self.calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    for name in sandbox_public_methods():

        def recorder(self, *args, __name=name, **kwargs):
            self.calls.append((__name, args, kwargs))
            return f"delegated:{__name}"

        setattr(Recording, name, recorder)
    abc.update_abstractmethods(Recording)
    return Recording()


class _CapturingBridge:
    persistent_shell_sessions = False
    safe_reference = "accepted-session-test"

    def __init__(self) -> None:
        self.operations: list[AcceptedSandboxOperationV1] = []

    def execute_sync(self, operation: AcceptedSandboxOperationV1) -> object:
        self.operations.append(operation)
        return f"result:{operation.kind.value}"


def test_every_public_sandbox_method_is_a_declared_operation():
    assert set(sandbox_operations()) == sandbox_public_methods()
    assert {"execute_command_in_scope", "release_command_scope"} <= set(sandbox_operations())
    assert_operations_cover_sandbox()


def test_sample_calls_cover_every_declared_operation():
    assert set(SAMPLE_CALLS) == set(sandbox_operations())


def test_declared_signatures_match_the_base_class():
    for name, spec in sandbox_operations().items():
        base = inspect.signature(getattr(Sandbox, name))
        assert _parameters(base, drop_self=True) == _parameters(spec.signature, drop_self=False), name


def test_operation_kind_enum_is_generated_from_the_declarations():
    assert AcceptedSandboxOperationKind is SandboxOperationKind
    assert {member.value for member in SandboxOperationKind} == set(sandbox_operations())
    assert SandboxOperationKind.EXECUTE_COMMAND == "execute_command"
    assert SandboxOperationKind.EXECUTE_COMMAND_IN_SCOPE == "execute_command_in_scope"


@pytest.mark.parametrize(
    ("name", "expected_args", "expected_kwargs"),
    [
        ("execute_command", ("echo forbidden",), {"env": None, "timeout": None}),
        ("read_file", ("/safe",), {"start_line": None, "end_line": None}),
        ("download_file", ("/f",), {}),
        ("list_dir", ("/d",), {"max_depth": 2}),
        ("write_file", ("/f", "text"), {"append": False}),
        ("glob", ("/d", "*.md"), {"include_dirs": False, "max_results": 200}),
        ("grep", ("/d", "needle"), {"glob": None, "literal": False, "case_sensitive": False, "max_results": 100}),
        ("update_file", ("/f", b"bytes"), {}),
        ("execute_command_in_scope", ("ls",), {"env": None, "timeout": None, "scope_id": None}),
        ("release_command_scope", ("scope-1",), {}),
    ],
)
def test_generated_envelope_constructors_keep_the_historical_argument_split(name, expected_args, expected_kwargs):
    """Parameters without defaults travel as args, parameters with defaults as
    kwargs. This is the split the hand-written constructors used, pinned so
    the envelope shape does not drift under generation."""
    operation = getattr(AcceptedSandboxOperationV1, name)(*expected_args)
    assert operation.kind == name
    assert operation.args == expected_args
    assert operation.kwargs == expected_kwargs
    assert operation is not AcceptedSandboxOperationV1.for_operation(name, *expected_args)


def test_envelope_constructor_binds_keyword_arguments():
    operation = AcceptedSandboxOperationV1.grep("/d", "needle", glob="*.py", max_results=5)
    assert operation.kwargs == {"glob": "*.py", "literal": False, "case_sensitive": False, "max_results": 5}


def test_envelope_constructor_rejects_unknown_arguments():
    with pytest.raises(TypeError):
        AcceptedSandboxOperationV1.read_file("/f", nonsense=1)
    with pytest.raises(ValueError, match="unknown sandbox operation"):
        AcceptedSandboxOperationV1.for_operation("brand_new_verb")


def test_every_facade_method_routes_through_the_bridge_and_delegates_faithfully():
    bridge = _CapturingBridge()
    facade = _AcceptedSandboxFacade(bridge)
    target = _make_recording_sandbox()
    for name, (args, kwargs) in SAMPLE_CALLS.items():
        bridge.operations.clear()
        target.calls.clear()
        result = getattr(facade, name)(*args, **kwargs)
        assert result == f"result:{name}", name
        assert len(bridge.operations) == 1, name
        operation = bridge.operations[0]
        assert operation.kind == name
        assert operation.delegate(target) == f"delegated:{name}"
        recorded_name, recorded_args, recorded_kwargs = target.calls[0]
        assert recorded_name == name
        assert _bound(name, recorded_args, recorded_kwargs) == _bound(name, args, kwargs), name


def test_facade_overrides_every_public_sandbox_method_with_the_base_signature():
    assert_facade_covers_sandbox(_AcceptedSandboxFacade)
    for name in sandbox_public_methods():
        assert name in vars(_AcceptedSandboxFacade), name
        facade_signature = inspect.signature(getattr(_AcceptedSandboxFacade, name))
        base_signature = inspect.signature(getattr(Sandbox, name))
        assert _parameters(facade_signature, drop_self=True) == _parameters(base_signature, drop_self=True), name


def test_facade_coverage_check_names_a_base_class_passthrough():
    class Partial(Sandbox):
        def execute_command(self, command, env=None, timeout=None):
            return ""

    with pytest.raises(RuntimeError, match="execute_command_in_scope"):
        assert_facade_covers_sandbox(Partial)


def test_operation_coverage_check_names_an_undeclared_sandbox_method():
    class Wider(Sandbox):
        def brand_new_verb(self) -> None:
            return None

    with pytest.raises(RuntimeError, match="brand_new_verb"):
        assert_operations_cover_sandbox(sandbox_cls=Wider)


def test_operation_coverage_check_names_an_operation_without_a_sandbox_method():
    phantom = dataclasses.replace(sandbox_operations()["read_file"], name="phantom")
    with pytest.raises(RuntimeError, match="phantom"):
        assert_operations_cover_sandbox(operations={**sandbox_operations(), "phantom": phantom})


def test_declaring_the_same_operation_twice_is_an_error():
    before = dict(sandbox_operations())
    with pytest.raises(RuntimeError, match="read_file"):

        @operations.sandbox_operation
        def read_file(self, path: str) -> str:
            return ""

    assert dict(sandbox_operations()) == before


def test_fenced_facade_decorator_installs_methods_and_refreshes_abstract_methods():
    @fenced_sandbox_facade
    class Facade(Sandbox):
        def __init__(self) -> None:
            super().__init__("facade")
            self.seen: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

        def _execute_fenced_operation(self, name: str, args: tuple[object, ...], kwargs: dict[str, object]) -> object:
            self.seen.append((name, args, kwargs))
            return name

    facade = Facade()
    assert facade.execute_command("ls", timeout=1) == "execute_command"
    assert facade.release_command_scope("scope-1") == "release_command_scope"
    assert facade.seen == [("execute_command", ("ls",), {"timeout": 1}), ("release_command_scope", ("scope-1",), {})]


def test_fenced_facade_decorator_requires_the_execution_hook():
    with pytest.raises(TypeError, match="_execute_fenced_operation"):

        @fenced_sandbox_facade
        class Hookless(Sandbox):
            pass
