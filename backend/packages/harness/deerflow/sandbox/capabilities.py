"""Optional sandbox provider capabilities, negotiated at runtime.

A sandbox provider is required to implement four verbs: ``acquire``, its async
twin, ``get`` and ``release`` (see
:class:`~deerflow.sandbox.sandbox_provider.SandboxProvider`). Anything a caller
needs beyond that is a capability: a contract class the provider offers through
``SandboxProvider.capability(protocol)`` and a caller discovers through
:func:`sandbox_capability`. A provider offers a capability by inheriting its
contract, in which case the base negotiation answers the provider itself, or by
answering a companion object that inherits it. A provider that offers nothing
answers ``None``, and callers fail closed with a typed error rather than
probing attribute names.

The contracts themselves are declared by the code that needs them; this module
owns only the negotiation.
"""

from __future__ import annotations


def sandbox_capability[CapabilityT](provider: object, protocol: type[CapabilityT]) -> CapabilityT | None:
    """The object through which ``provider`` offers ``protocol``, or ``None``.

    A provider negotiates through its ``capability`` method when it has one;
    a duck-typed double without it offers exactly the contracts it inherits.
    Whatever is answered must itself inherit the contract: a capability is a
    declaration, never an accident of attribute names.
    """
    negotiate = getattr(provider, "capability", None)
    if callable(negotiate):
        found = negotiate(protocol)
    else:
        found = provider if isinstance(provider, protocol) else None
    if found is not None and not isinstance(found, protocol):
        raise TypeError(f"{type(provider).__name__}.capability answered an object that does not implement {protocol.__name__}")
    return found
