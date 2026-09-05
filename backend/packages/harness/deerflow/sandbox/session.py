"""Sandbox sessions: one declared kind per execution, resolved by the provider.

Every caller that resolves a sandbox, including upstream's lease manager once it
is merged, already goes through the provider's ``acquire``, ``get`` and
``release``. This module wraps the configured provider and dispatches those
three verbs by the executing session's declaration:

* a declared execution acquires its own public ref and never provisions;
* a public ref resolves to its handle only for the declaring execution, so a
  fork, a Gateway request or a channel with no declaration gets nothing back;
* an ordinary acquire is refused while a retire-terminal session holds the
  mount scope, which is what makes "destroy with a co-holder" impossible; and
* releasing a public ref runs the declaration's terminal, once.

The real provider identifier never leaves the declaring session: the provider
hooks that are keyed by sandbox id (the network policy hooks) receive the
public ref and translate it inside, for the declaring execution only. Ordinary
sessions are the default kind: everything not declared is forwarded to the
backing provider unchanged. The declaration travels with the execution as a
context variable; ``declared_sandbox`` is the one resolver the tools and
middleware consult, and it is the only carrier: nothing in a runtime context
dict can stand in for it.
"""

from __future__ import annotations

import inspect
import logging
import threading
import weakref
from collections import deque
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from deerflow.sandbox.sandbox import Sandbox
from deerflow.sandbox.sandbox_provider import SandboxProvider

logger = logging.getLogger(__name__)

# How many retired public refs the registry remembers so a stale checkpoint
# cannot hand a dead ref to the backing provider as if it were a container id.
_RETIRED_REF_MEMORY = 4096


class SandboxSessionKind(StrEnum):
    """What the session is made of. Ordinary is the default kind."""

    ORDINARY = "ordinary"
    ACCEPTED = "accepted"


class SandboxSessionTerminal(StrEnum):
    """What the last holder does with the container."""

    PARK = "park"
    RETIRE = "retire"


class SandboxSessionConflict(RuntimeError):
    """A sandbox request that the current declarations cannot admit."""

    code = "sandbox_session_conflict"

    def __init__(
        self,
        message: str,
        *,
        mount_scope: tuple[str, str] | None = None,
        public_ref: str | None = None,
    ) -> None:
        super().__init__(message)
        self.mount_scope = mount_scope
        self.public_ref = public_ref


@dataclass(frozen=True, slots=True)
class SandboxSessionDeclaration:
    """One execution's declared session.

    ``public_ref`` is the only identifier that leaves the provider; it is what
    LangGraph state, logs and evidence carry. ``mount_scope`` is whose thread
    the material is projected from, ``(user_id, thread_id)``, and is what an
    ordinary acquire is keyed by. ``is_live`` and ``retire`` are supplied by the
    session owner so the registry never holds provider objects itself.
    """

    public_ref: str
    mount_scope: tuple[str, str] | None
    kind: SandboxSessionKind
    terminal: SandboxSessionTerminal
    handle: Sandbox
    is_live: Callable[[], bool]
    retire: Callable[[], None]
    # The backing provider's own identifier for the container. It never leaves
    # the session provider: provider hooks that are keyed by sandbox id (the
    # network policy hooks) receive the public ref and translate to it inside.
    provider_ref: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.public_ref, str) or not self.public_ref:
            raise ValueError("public_ref must be a non-empty string")
        if self.provider_ref is not None and (not isinstance(self.provider_ref, str) or not self.provider_ref):
            raise ValueError("provider_ref must be a non-empty string or None")
        if self.mount_scope is not None and (not isinstance(self.mount_scope, tuple) or len(self.mount_scope) != 2 or not all(isinstance(part, str) and part for part in self.mount_scope)):
            raise ValueError("mount_scope must be a (user_id, thread_id) pair or None")
        if not isinstance(self.kind, SandboxSessionKind) or not isinstance(self.terminal, SandboxSessionTerminal):
            raise TypeError("kind and terminal must be session enums")
        if not isinstance(self.handle, Sandbox):
            raise TypeError("handle must be a Sandbox")
        if not callable(self.is_live) or not callable(self.retire):
            raise TypeError("is_live and retire must be callables")


_CURRENT: ContextVar[SandboxSessionDeclaration | None] = ContextVar(
    "deerflow_sandbox_session",
    default=None,
)


def current_sandbox_session() -> SandboxSessionDeclaration | None:
    """The declaration bound to the executing task, thread or request."""
    return _CURRENT.get()


def set_current_sandbox_session(declaration: SandboxSessionDeclaration | None) -> None:
    """Bind (or clear) the executing context's declaration without a scope.

    Used by session installers that outlive one function call, such as the
    durable worker installing a session for a whole run. Child tasks and
    threads created afterwards inherit the binding.
    """
    _CURRENT.set(declaration)


@contextmanager
def bind_sandbox_session(declaration: SandboxSessionDeclaration) -> Iterator[SandboxSessionDeclaration]:
    """Bind ``declaration`` to the executing context for the block."""
    token = _CURRENT.set(declaration)
    try:
        yield declaration
    finally:
        _CURRENT.reset(token)


def declared_sandbox() -> Sandbox | None:
    """The executing context's declared handle, or ``None`` on the ordinary path.

    This is the one resolver the sandbox tools and middleware consult before
    they touch runtime state or the provider. Liveness is the handle's own
    business: a fenced handle raises its typed authority error on use, and the
    tools propagate that untouched, so resolving never converts a lost session
    into a different error class or into an ordinary acquire.
    """
    current = _CURRENT.get()
    return None if current is None else current.handle


def sandbox_mount_scope(user_id: object, thread_id: object) -> tuple[str, str] | None:
    """The ``(user_id, thread_id)`` an ordinary acquire is keyed by, or ``None``.

    A session keyed by something else, such as a batch child keyed by its
    attempt, declares no mount scope: no ordinary acquire can collide with it,
    so none is refused because of it.
    """
    if isinstance(user_id, str) and user_id and isinstance(thread_id, str) and thread_id:
        return (user_id, thread_id)
    return None


class SandboxSessionRegistry:
    """Process-local map from public ref to live declaration."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._by_ref: dict[str, SandboxSessionDeclaration] = {}
        self._retired: deque[str] = deque(maxlen=_RETIRED_REF_MEMORY)

    def declare(self, declaration: SandboxSessionDeclaration) -> SandboxSessionDeclaration | None:
        """Register ``declaration``; return the declaration it replaced, if any.

        The newest declaration wins. Replacing a live one is logged because it
        means two sessions in one process minted the same public ref; the
        older execution's handle stops resolving, which fails typed rather
        than sharing a container.
        """
        if not isinstance(declaration, SandboxSessionDeclaration):
            raise TypeError("declaration must be a SandboxSessionDeclaration")
        with self._lock:
            previous = self._by_ref.get(declaration.public_ref)
            if previous is declaration:
                return None
            if previous is not None and previous.is_live():
                logger.warning("Replacing a live sandbox session declaration for %s", declaration.public_ref)
            self._by_ref[declaration.public_ref] = declaration
            return previous

    def revoke(self, public_ref: str) -> None:
        with self._lock:
            if self._by_ref.pop(public_ref, None) is not None:
                self._retired.append(public_ref)

    def lookup(self, public_ref: str) -> SandboxSessionDeclaration | None:
        """The live declaration for ``public_ref``; dead entries are dropped."""
        with self._lock:
            declaration = self._by_ref.get(public_ref)
            if declaration is None:
                return None
            if not declaration.is_live():
                self._by_ref.pop(public_ref, None)
                self._retired.append(public_ref)
                return None
            return declaration

    def was_declared(self, public_ref: str) -> bool:
        """Whether ``public_ref`` is, or recently was, a declared session ref."""
        with self._lock:
            return public_ref in self._by_ref or public_ref in self._retired

    def retire_terminal_holder(self, mount_scope: tuple[str, str]) -> SandboxSessionDeclaration | None:
        """A live retire-terminal declaration holding ``mount_scope``, if any."""
        with self._lock:
            for public_ref, declaration in list(self._by_ref.items()):
                if not declaration.is_live():
                    self._by_ref.pop(public_ref, None)
                    self._retired.append(public_ref)
                    continue
                if declaration.terminal is SandboxSessionTerminal.RETIRE and declaration.mount_scope == mount_scope:
                    return declaration
        return None

    def live(self) -> tuple[SandboxSessionDeclaration, ...]:
        with self._lock:
            return tuple(declaration for declaration in self._by_ref.values() if declaration.is_live())


_REGISTRY = SandboxSessionRegistry()


def get_sandbox_session_registry() -> SandboxSessionRegistry:
    """The process registry every session provider consults by default."""
    return _REGISTRY


_NETWORK_HOOKS = (
    "consume_network_policy_events",
    "deny_pending_network_policy_events",
    "decide_network_policy_request",
)
_OVERRIDDEN = frozenset({"acquire", "acquire_async", "get", "release", *_NETWORK_HOOKS, *(f"{name}_async" for name in _NETWORK_HOOKS)})


def _forwarded_names() -> tuple[tuple[str, bool], ...]:
    names: list[tuple[str, bool]] = []
    for name in dir(SandboxProvider):
        if name.startswith("_") or name in _OVERRIDDEN:
            continue
        member = inspect.getattr_static(SandboxProvider, name)
        if inspect.isfunction(member):
            names.append((name, inspect.iscoroutinefunction(member)))
    return tuple(names)


def _forwarded_attributes() -> tuple[str, ...]:
    return tuple(name for name in dir(SandboxProvider) if not name.startswith("_") and not callable(inspect.getattr_static(SandboxProvider, name)))


class SessionProvider(SandboxProvider):
    """The configured provider, dispatching acquire, get and release by declaration.

    Everything the base class defines and everything the backing provider adds
    (capability hooks, ``destroy``, ``shutdown``, duck-typed flags) is forwarded
    unchanged, so a wrapped provider behaves exactly like the backing one for
    every ordinary session.
    """

    def __init__(self, backing: SandboxProvider, *, registry: SandboxSessionRegistry | None = None) -> None:
        if isinstance(backing, SessionProvider):
            raise TypeError("backing provider is already a SessionProvider")
        # Duck-typed and mock providers are accepted on purpose: the installer
        # has always taken "a custom or mock provider", and every verb below is
        # forwarded by name, not by type.
        if backing is None or not all(callable(getattr(backing, verb, None)) for verb in ("acquire", "get", "release")):
            raise TypeError("backing must provide acquire, get and release")
        self._backing = backing
        self._registry = registry if registry is not None else _REGISTRY

    @property
    def backing(self) -> SandboxProvider:
        return self._backing

    @property
    def __class__(self):  # type: ignore[override]
        """Answer ``isinstance`` checks for the configured provider class.

        ``type(self)`` stays ``SessionProvider``; the fallback ``__class__``
        lookup is what lets callers that check for a concrete provider keep
        working while the session provider stands in front of it.
        """
        return type(self._backing)

    @property
    def registry(self) -> SandboxSessionRegistry:
        return self._registry

    # -- declaration-aware verbs -------------------------------------------

    def _declared(self) -> SandboxSessionDeclaration | None:
        current = _CURRENT.get()
        if current is None:
            return None
        if not current.is_live():
            raise SandboxSessionConflict(
                f"declared sandbox session {current.public_ref} is no longer live",
                mount_scope=current.mount_scope,
                public_ref=current.public_ref,
            )
        return current

    def _refuse_held_mount_scope(self, thread_id: str | None, user_id: str | None) -> None:
        mount_scope = sandbox_mount_scope(user_id, thread_id)
        if mount_scope is None:
            return
        holder = self._registry.retire_terminal_holder(mount_scope)
        if holder is not None:
            raise SandboxSessionConflict(
                f"sandbox for user {user_id!r} thread {thread_id!r} is held by accepted session {holder.public_ref}",
                mount_scope=mount_scope,
                public_ref=holder.public_ref,
            )

    def acquire(self, thread_id: str | None = None, *, user_id: str | None = None) -> str:
        current = self._declared()
        if current is not None:
            return current.public_ref
        self._refuse_held_mount_scope(thread_id, user_id)
        return self._backing.acquire(thread_id, user_id=user_id)

    async def acquire_async(self, thread_id: str | None = None, *, user_id: str | None = None) -> str:
        current = self._declared()
        if current is not None:
            return current.public_ref
        self._refuse_held_mount_scope(thread_id, user_id)
        return await self._backing.acquire_async(thread_id, user_id=user_id)

    def get(self, sandbox_id: str) -> Sandbox | None:
        declaration = self._registry.lookup(sandbox_id)
        if declaration is not None:
            return declaration.handle if _CURRENT.get() is declaration else None
        if self._registry.was_declared(sandbox_id):
            return None
        return self._backing.get(sandbox_id)

    def release(self, sandbox_id: str) -> None:
        declaration = self._registry.lookup(sandbox_id)
        if declaration is not None:
            if _CURRENT.get() is not declaration:
                logger.warning("Ignoring release of sandbox session %s from an execution that did not declare it", sandbox_id)
                return
            try:
                declaration.retire()
            finally:
                self._registry.revoke(sandbox_id)
            return
        if self._registry.was_declared(sandbox_id):
            return
        self._backing.release(sandbox_id)

    # -- provider hooks keyed by sandbox id: translate the public ref ---------

    def _backing_ref(self, sandbox_id: str) -> str | None:
        """The backing id behind ``sandbox_id``, for the declaring execution only.

        A public ref resolves to its container id only for the execution that
        declared it, exactly as ``get`` resolves the handle; a stranger, a
        retired ref, or a declaration without a provider ref resolves to
        nothing. An ordinary id passes through unchanged.
        """
        declaration = self._registry.lookup(sandbox_id)
        if declaration is not None:
            return declaration.provider_ref if _CURRENT.get() is declaration else None
        if self._registry.was_declared(sandbox_id):
            return None
        return sandbox_id

    def consume_network_policy_events(self, sandbox_id: str) -> list[dict[str, object]]:
        backing_ref = self._backing_ref(sandbox_id)
        if backing_ref is None:
            return []
        return self._backing.consume_network_policy_events(backing_ref)

    async def consume_network_policy_events_async(self, sandbox_id: str) -> list[dict[str, object]]:
        backing_ref = self._backing_ref(sandbox_id)
        if backing_ref is None:
            return []
        return await self._backing.consume_network_policy_events_async(backing_ref)

    def deny_pending_network_policy_events(self, sandbox_id: str) -> bool:
        backing_ref = self._backing_ref(sandbox_id)
        if backing_ref is None:
            return False
        return self._backing.deny_pending_network_policy_events(backing_ref)

    async def deny_pending_network_policy_events_async(self, sandbox_id: str) -> bool:
        backing_ref = self._backing_ref(sandbox_id)
        if backing_ref is None:
            return False
        return await self._backing.deny_pending_network_policy_events_async(backing_ref)

    def decide_network_policy_request(self, sandbox_id: str, request_id: str, decision: str) -> bool:
        backing_ref = self._backing_ref(sandbox_id)
        if backing_ref is None:
            return False
        return self._backing.decide_network_policy_request(backing_ref, request_id, decision)

    async def decide_network_policy_request_async(self, sandbox_id: str, request_id: str, decision: str) -> bool:
        backing_ref = self._backing_ref(sandbox_id)
        if backing_ref is None:
            return False
        return await self._backing.decide_network_policy_request_async(backing_ref, request_id, decision)

    # -- everything else is the backing provider ----------------------------

    def __getattr__(self, name: str) -> Any:
        backing = self.__dict__.get("_backing")
        if backing is None or name.startswith("__"):
            raise AttributeError(name)
        return getattr(backing, name)


def _install_forwarders() -> None:
    for name, is_async in _forwarded_names():
        if is_async:

            async def forward(self, *args, __name=name, **kwargs):
                return await getattr(self._backing, __name)(*args, **kwargs)

        else:

            def forward(self, *args, __name=name, **kwargs):
                return getattr(self._backing, __name)(*args, **kwargs)

        forward.__name__ = name
        forward.__qualname__ = f"SessionProvider.{name}"
        forward.__doc__ = f"Forward ``{name}`` to the backing provider."
        setattr(SessionProvider, name, forward)
    for name in _forwarded_attributes():
        setattr(
            SessionProvider,
            name,
            property(lambda self, __name=name: getattr(self._backing, __name)),
        )


_install_forwarders()


_wrappers_lock = threading.Lock()
# One wrapper per backing provider object. Lifecycle registries (the lease
# manager) are keyed by provider identity, so a backing provider and the
# wrapper installed in front of it must always resolve to the same wrapper.
# Values are weak: a wrapper lives exactly as long as something holds it, and
# it holds its backing strongly, so an id cannot be reused while its entry is
# still present.
_wrappers: weakref.WeakValueDictionary[int, SessionProvider] = weakref.WeakValueDictionary()


def sandbox_session_provider(provider: SandboxProvider, *, registry: SandboxSessionRegistry | None = None) -> SessionProvider:
    """Wrap ``provider`` once; an already wrapped provider is returned as is.

    The same backing provider always yields the same wrapper, so identity-keyed
    lifecycle state cannot fork between callers holding the backing provider
    and callers holding the installed session provider.
    """
    if isinstance(provider, SessionProvider):
        return provider
    with _wrappers_lock:
        existing = _wrappers.get(id(provider))
        if existing is not None and existing.backing is provider and (registry is None or existing.registry is registry):
            return existing
        wrapper = SessionProvider(provider, registry=registry)
        _wrappers[id(provider)] = wrapper
        return wrapper


def unwrap_sandbox_provider(provider: SandboxProvider) -> SandboxProvider:
    """The backing provider behind a session provider, or ``provider`` itself."""
    return provider.backing if isinstance(provider, SessionProvider) else provider


__all__ = [
    "SandboxSessionConflict",
    "SandboxSessionDeclaration",
    "SandboxSessionKind",
    "SandboxSessionRegistry",
    "SandboxSessionTerminal",
    "SessionProvider",
    "bind_sandbox_session",
    "current_sandbox_session",
    "declared_sandbox",
    "get_sandbox_session_registry",
    "sandbox_mount_scope",
    "sandbox_session_provider",
    "set_current_sandbox_session",
    "unwrap_sandbox_provider",
]
