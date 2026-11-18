"""Endpoint rules for deployments whose connections are written by tenants.

A deployment declares that the people who administer it are tenants, not its
operator, with ``DEER_FLOW_TENANT_WRITTEN_ENDPOINTS=on`` in the process
environment. Only the exact value ``on`` counts. On such a deployment,
endpoints configured through the product must use public HTTP(S) addresses.

The provider can allow named private endpoints with
``DEER_FLOW_PRIVATE_ENDPOINTS``: comma-separated ``host:port`` pairs, compared
exactly (hosts without regard to case), with the port always written. The list
is read only when the deployment declared itself. An entry that is not of this
form is ignored and logged; it never allows anything and never stops a start.

Both names are read at the moment of use, not at import.
"""

from __future__ import annotations

import ipaddress
import logging
import os
import re
from functools import lru_cache
from urllib.parse import urlsplit

from deerflow.community.url_safety import validate_public_http_url

logger = logging.getLogger(__name__)

TENANT_WRITTEN_ENV = "DEER_FLOW_TENANT_WRITTEN_ENDPOINTS"
PRIVATE_ENDPOINTS_ENV = "DEER_FLOW_PRIVATE_ENDPOINTS"

_DEFAULT_PORTS = {"http": 80, "https": 443}
_ENTRY = re.compile(r"(?P<host>\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?):(?P<port>[0-9]{1,5})")


def tenant_written() -> bool:
    """Whether this deployment's connections are written by tenants."""
    return os.environ.get(TENANT_WRITTEN_ENV) == "on"


def private_endpoints() -> frozenset[tuple[str, int]]:
    """The provider's allowed private ``(host, port)`` pairs; empty unless :func:`tenant_written`."""
    if not tenant_written():
        return frozenset()
    return _parse_private_endpoints(os.environ.get(PRIVATE_ENDPOINTS_ENV, ""))


def is_private_endpoint(scheme: str, host: str | None, port: int | None) -> bool:
    """Whether this origin is a listed pair; an unwritten port is the scheme's default."""
    if not host:
        return False
    port = port if port is not None else _DEFAULT_PORTS.get(scheme.lower())
    return port is not None and (_normalized_host(host), port) in private_endpoints()


def address_problem(url: str) -> str | None:
    """Return ``None`` when *url* may be used, otherwise a short reason.

    A listed private endpoint is accepted; any other address must be a public
    HTTP(S) one. Resolves a host name synchronously, so async callers run it
    in a worker thread.
    """
    try:
        parsed = urlsplit(url)
        host, port = parsed.hostname, parsed.port
    except (ValueError, AttributeError, TypeError):
        return "Error: URL could not be parsed"
    if parsed.scheme in _DEFAULT_PORTS and is_private_endpoint(parsed.scheme, host, port):
        return None
    return validate_public_http_url(url, action="connect to")


def _normalized_host(host: str) -> str:
    """One form for a host wherever it is compared: lower case, and ASCII for a name outside it.

    A list entry is ASCII. A URL can carry the same name in Unicode, and an
    HTTP client reports it decoded, so both are brought to the encoded form
    the client puts on the wire. A name that cannot be encoded matches nothing.
    """
    host = host.strip().lower()
    try:
        return str(ipaddress.ip_address(host))
    except ValueError:
        pass
    if host.isascii():
        return host
    try:
        import idna

        return idna.encode(host).decode("ascii")
    except Exception:  # noqa: BLE001 - any name the encoder refuses is simply not listed
        return host


def _parse_entry(entry: str) -> tuple[str, int] | None:
    match = _ENTRY.fullmatch(entry)
    if match is None or not 1 <= int(match["port"]) <= 65535:
        return None
    host = match["host"]
    if host.startswith("["):
        try:
            return str(ipaddress.IPv6Address(host[1:-1])), int(match["port"])
        except ValueError:
            return None
    return _normalized_host(host), int(match["port"])


@lru_cache(maxsize=8)
def _parse_private_endpoints(raw: str) -> frozenset[tuple[str, int]]:
    """Parse one environment value; cached so a malformed entry is logged once per value."""
    endpoints: set[tuple[str, int]] = set()
    for position, entry in enumerate(raw.split(","), start=1):
        if not entry.strip():
            continue
        endpoint = _parse_entry(entry.strip())
        if endpoint is None:
            # The position, not the text: a pasted value can be anything.
            logger.warning("Ignoring entry %d of %s: expected host:port", position, PRIVATE_ENDPOINTS_ENV)
            continue
        endpoints.add(endpoint)
    return frozenset(endpoints)
