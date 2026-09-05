"""The single declaration point for sandbox operations.

Every public method on :class:`~deerflow.sandbox.sandbox.Sandbox` is declared
here exactly once. The declaration is the source of three generated things:

* the operation kind (:data:`SandboxOperationKind`), the closed set of verbs an
  accepted session may admit;
* the process-local envelope constructor on the accepted operation record; and
* the fenced facade method that routes the verb through an accepted session.

The module asserts at import time that the declared set equals the base
class's public method set, so a verb added upstream fails loudly here instead
of arriving on the accepted facade as a passthrough that skips the fence.
"""

from __future__ import annotations

import abc
import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType

from deerflow.sandbox.sandbox import Sandbox

FENCED_EXECUTION_HOOK = "_execute_fenced_operation"


@dataclass(frozen=True, slots=True)
class SandboxOperationSpec:
    """One declared verb: the base-class method name and its call shape."""

    name: str
    signature: inspect.Signature
    positional: tuple[str, ...]
    keyword: tuple[str, ...]
    doc: str

    def envelope_arguments(
        self,
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
    ) -> tuple[tuple[object, ...], dict[str, object]]:
        """Bind a call to the declared signature and split it for the envelope.

        Parameters without a default travel positionally; parameters with a
        default travel by keyword with defaults applied. Unknown or missing
        arguments raise ``TypeError`` exactly as the base method would.
        """
        bound = self.signature.bind(*args, **kwargs)
        bound.apply_defaults()
        return (
            tuple(bound.arguments[name] for name in self.positional),
            {name: bound.arguments[name] for name in self.keyword},
        )


_DECLARED: dict[str, SandboxOperationSpec] = {}


def _spec_from(func: Callable[..., object]) -> SandboxOperationSpec:
    signature = inspect.signature(func)
    params = list(signature.parameters.values())
    if not params or params[0].name != "self":
        raise TypeError(f"sandbox operation {func.__name__!r} must be declared as a method with a leading self")
    call_params = params[1:]
    for param in call_params:
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            raise TypeError(f"sandbox operation {func.__name__!r} cannot declare variadic parameters")
    positional = tuple(param.name for param in call_params if param.default is inspect.Parameter.empty)
    keyword = tuple(param.name for param in call_params if param.default is not inspect.Parameter.empty)
    return SandboxOperationSpec(
        name=func.__name__,
        signature=signature.replace(parameters=call_params),
        positional=positional,
        keyword=keyword,
        doc=inspect.getdoc(func) or "",
    )


def sandbox_operation(func: Callable[..., object]) -> Callable[..., object]:
    """Declare one sandbox verb. The body is documentation; it never runs."""
    spec = _spec_from(func)
    if spec.name in _DECLARED:
        raise RuntimeError(f"sandbox operation {spec.name!r} is already declared")
    _DECLARED[spec.name] = spec
    return func


def sandbox_operations() -> Mapping[str, SandboxOperationSpec]:
    """Declared operations in declaration order; a read-only view."""
    return MappingProxyType(_DECLARED)


def sandbox_public_methods(sandbox_cls: type = Sandbox) -> frozenset[str]:
    """Public functions defined on the base class: the set a facade must fence."""
    names: set[str] = set()
    for name in dir(sandbox_cls):
        if name.startswith("_"):
            continue
        if inspect.isfunction(inspect.getattr_static(sandbox_cls, name)):
            names.add(name)
    return frozenset(names)


def assert_operations_cover_sandbox(
    *,
    sandbox_cls: type = Sandbox,
    operations: Mapping[str, SandboxOperationSpec] | None = None,
) -> None:
    """Raise unless the declared verbs and the base-class public methods agree."""
    declared = set(sandbox_operations() if operations is None else operations)
    public = set(sandbox_public_methods(sandbox_cls))
    undeclared = sorted(public - declared)
    phantom = sorted(declared - public)
    if undeclared or phantom:
        raise RuntimeError(
            "sandbox operations do not match the Sandbox public methods: "
            f"undeclared on the operations module: {undeclared}; "
            f"declared without a Sandbox method: {phantom}. "
            "Declare every public Sandbox verb in deerflow.sandbox.operations so it is fenced on the accepted facade."
        )


def assert_facade_covers_sandbox(facade_cls: type, *, sandbox_cls: type = Sandbox) -> None:
    """Raise unless ``facade_cls`` itself overrides every public base method."""
    passthroughs = sorted(name for name in sandbox_public_methods(sandbox_cls) if name not in vars(facade_cls))
    if passthroughs:
        raise RuntimeError(f"{facade_cls.__name__} inherits Sandbox methods as unfenced passthroughs: {passthroughs}")


def _fenced_method(spec: SandboxOperationSpec) -> Callable[..., object]:
    def fenced(self, *args, **kwargs):
        return getattr(self, FENCED_EXECUTION_HOOK)(spec.name, args, kwargs)

    fenced.__name__ = spec.name
    fenced.__qualname__ = spec.name
    fenced.__doc__ = spec.doc
    fenced.__signature__ = spec.signature.replace(
        parameters=[inspect.Parameter("self", inspect.Parameter.POSITIONAL_OR_KEYWORD), *spec.signature.parameters.values()],
    )
    return fenced


def fenced_sandbox_facade[T: type](facade_cls: T) -> T:
    """Install one fenced method per declared operation on a Sandbox subclass.

    Each generated method forwards ``(name, args, kwargs)`` to the class's
    ``_execute_fenced_operation`` hook, which is where the accepted session
    builds the envelope and validates its authorities. The decorator refreshes
    the abstract-method set and then asserts that nothing on the base class is
    left as a passthrough.
    """
    if not (isinstance(facade_cls, type) and issubclass(facade_cls, Sandbox)):
        raise TypeError("fenced_sandbox_facade requires a Sandbox subclass")
    if not callable(getattr(facade_cls, FENCED_EXECUTION_HOOK, None)):
        raise TypeError(f"{facade_cls.__name__} must define {FENCED_EXECUTION_HOOK}(name, args, kwargs) before it can be a fenced facade")
    for spec in sandbox_operations().values():
        setattr(facade_cls, spec.name, _fenced_method(spec))
    abc.update_abstractmethods(facade_cls)
    assert_facade_covers_sandbox(facade_cls)
    return facade_cls


# ---------------------------------------------------------------------------
# Declarations. Order is the order of the generated kind enum. Bodies are
# documentation only and never run.
# ---------------------------------------------------------------------------


@sandbox_operation
def execute_command(
    self,
    command: str,
    env: dict[str, str] | None = None,
    timeout: float | None = None,
) -> str:
    """Run a shell command and return its combined output."""


@sandbox_operation
def execute_command_in_scope(
    self,
    command: str,
    env: dict[str, str] | None = None,
    timeout: float | None = None,
    *,
    scope_id: str | None = None,
) -> str:
    """Run a shell command inside one execution's shell scope."""


@sandbox_operation
def release_command_scope(self, scope_id: str) -> None:
    """Release provider-side shell state held for one execution scope."""


@sandbox_operation
def read_file(
    self,
    path: str,
    start_line: int | None = None,
    end_line: int | None = None,
) -> str:
    """Read a text file, optionally bounded to a line range."""


@sandbox_operation
def download_file(self, path: str) -> bytes:
    """Read a file as bytes."""


@sandbox_operation
def list_dir(self, path: str, max_depth: int = 2) -> list[str]:
    """List a directory tree to a bounded depth."""


@sandbox_operation
def write_file(self, path: str, content: str, append: bool = False) -> None:
    """Write or append text to a file."""


@sandbox_operation
def glob(
    self,
    path: str,
    pattern: str,
    *,
    include_dirs: bool = False,
    max_results: int = 200,
) -> tuple[list[str], bool]:
    """Match paths under a directory against a glob pattern."""


@sandbox_operation
def grep(
    self,
    path: str,
    pattern: str,
    *,
    glob: str | None = None,
    literal: bool = False,
    case_sensitive: bool = False,
    max_results: int = 100,
) -> tuple[list[object], bool]:
    """Search one file or a directory tree for a pattern."""


@sandbox_operation
def update_file(self, path: str, content: bytes) -> None:
    """Replace a file's bytes."""


SandboxOperationKind = StrEnum(
    "SandboxOperationKind",
    {name.upper(): name for name in _DECLARED},
)
SandboxOperationKind.__doc__ = "Closed set of sandbox verbs, generated from the declarations above."
SandboxOperationKind.__module__ = __name__

assert_operations_cover_sandbox()

__all__ = [
    "FENCED_EXECUTION_HOOK",
    "SandboxOperationKind",
    "SandboxOperationSpec",
    "assert_facade_covers_sandbox",
    "assert_operations_cover_sandbox",
    "fenced_sandbox_facade",
    "sandbox_operation",
    "sandbox_operations",
    "sandbox_public_methods",
]
