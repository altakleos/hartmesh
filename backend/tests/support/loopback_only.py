"""Keep a test's network on the loopback interface, whatever the code under test does.

``install`` wraps the public ``socket`` seams and calls through for loopback:

- **Connect.** A TCP connect to an address that is not loopback raises and is
  recorded in ``blocked``; the fixture that installed the guard fails the test
  at teardown. A test can route one public ``(address, port)`` to a loopback
  endpoint instead (``routes``): the connect then goes there over a real
  socket and the intended destination is recorded in ``routed``.
- **Name lookup.** No lookup leaves the process. An address written as numbers
  and ``localhost`` are answered natively. A scripted name (``names``) takes
  its next scripted answer, the last one repeating, and is recorded in
  ``lookups``. Any other name does not resolve.

A scripted answer is resolved natively as a numeric address in the family the
caller asked for, so an unsupported family fails instead of receiving a
fabricated answer. Tests under the strict blocking-IO gate pass
``resolver_module=_socket`` so the instrumented ``socket.getaddrinfo`` wrapper
stays in place above the guard.
"""

from __future__ import annotations

import errno
import ipaddress
import socket
from dataclasses import dataclass, field
from types import ModuleType
from typing import Any


@dataclass
class LoopbackOnly:
    names: dict[str, list[str]] = field(default_factory=dict)
    lookups: list[tuple[str, str]] = field(default_factory=list)
    routes: dict[tuple[str, int], tuple[str, int]] = field(default_factory=dict)
    routed: list[tuple[str, int]] = field(default_factory=list)
    blocked: list[Any] = field(default_factory=list)

    def answers(self, name: str) -> list[str]:
        """The answers one scripted name was given so far, in order."""
        return [answer for looked_up, answer in self.lookups if looked_up == name]

    def assert_nothing_blocked(self) -> None:
        assert not self.blocked, f"the test tried to connect outside the loopback interface: {self.blocked}"


def _numeric(host: str) -> ipaddress._BaseAddress | None:
    try:
        return ipaddress.ip_address(host.split("%", 1)[0])
    except ValueError:
        return None


def install(monkeypatch, *, resolver_module: ModuleType = socket) -> LoopbackOnly:
    guard = LoopbackOnly()
    native_lookup = resolver_module.getaddrinfo
    native_connect = socket.socket.connect
    native_connect_ex = socket.socket.connect_ex

    def getaddrinfo(host, port, *args, **kwargs):
        name = host.decode("ascii", "replace") if isinstance(host, bytes) else host
        if name is None or name == "localhost" or _numeric(name) is not None:
            return native_lookup(host, port, *args, **kwargs)
        name = name.rstrip(".").lower()
        script = guard.names.get(name)
        if not script:
            raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")
        answer = script[min(len(guard.answers(name)), len(script) - 1)]
        guard.lookups.append((name, answer))
        return native_lookup(answer, port, *args, **kwargs)

    def destination(address):
        if not isinstance(address, tuple) or len(address) < 2 or not isinstance(address[0], str):
            return address
        target = _numeric(address[0])
        if target is not None and target.is_loopback:
            return address
        route = guard.routes.get((str(target), address[1])) if target is not None else None
        if route is None:
            guard.blocked.append(address)
            raise OSError(errno.ENETUNREACH, "tests may only connect to the loopback interface")
        guard.routed.append((str(target), address[1]))
        return route

    def connect(self, address):
        return native_connect(self, destination(address))

    def connect_ex(self, address):
        return native_connect_ex(self, destination(address))

    monkeypatch.setattr(resolver_module, "getaddrinfo", getaddrinfo)
    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket.socket, "connect_ex", connect_ex)
    return guard
