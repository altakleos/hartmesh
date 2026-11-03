"""Shared URL safety checks for server-side web tools."""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Callable
from urllib.parse import urlparse

_BLOCKED_HOSTNAMES = {"localhost", "metadata.google.internal"}
_IDNA_DOT_TRANSLATION = str.maketrans("\u3002\uff0e\uff61", "...")


def safe_public_result_url(value: object) -> str:
    """Screen an untrusted result URL without doing DNS or network I/O.

    This only filters URLs returned to callers. Downloaders must still check
    the resolved address at connection time.
    """
    if not isinstance(value, str):
        return ""
    url = value.strip()
    try:
        parsed = urlparse(url)
        host = parsed.hostname
    except ValueError:
        return ""
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or not host:
        return ""
    # IDNA clients treat these Unicode label separators as ASCII dots. Screen
    # the normalized host so an IP literal cannot masquerade as a DNS name.
    host = host.translate(_IDNA_DOT_TRANSLATION).lower().rstrip(".")
    if not host or host in _BLOCKED_HOSTNAMES or host.endswith(".localhost"):
        return ""

    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        address = _decode_result_ipv4(host)
        if address is None:
            return url
    if isinstance(address, ipaddress.IPv6Address):
        embedded = address.ipv4_mapped or address.sixtofour
        if embedded is None and address in ipaddress.ip_network("64:ff9b::/96"):
            embedded = ipaddress.IPv4Address(int(address) & 0xFFFFFFFF)
        if embedded is None and 1 < int(address) < 1 << 32:
            embedded = ipaddress.IPv4Address(int(address) & 0xFFFFFFFF)
        if embedded is not None and is_blocked_address(embedded):
            return ""
    return "" if is_blocked_address(address) else url


def _decode_result_ipv4(host: str) -> ipaddress.IPv4Address | None:
    """Recognize decimal, octal and hex IPv4 spellings accepted by clients."""
    parts = host.split(".")
    if not 1 <= len(parts) <= 4:
        return None
    values: list[int] = []
    for part in parts:
        try:
            base = 16 if part.startswith(("0x", "0X")) else 8 if part.startswith("0") and len(part) > 1 else 10
            values.append(int(part, base))
        except ValueError:
            return None
    *leading, last = values
    if any(not 0 <= value <= 255 for value in leading):
        return None
    remaining_bits = 8 * (4 - len(leading))
    if not 0 <= last < 1 << remaining_bits:
        return None
    result = 0
    for value in leading:
        result = result << 8 | value
    return ipaddress.IPv4Address(result << remaining_bits | last)


def resolve_host_addresses(hostname: str) -> list[ipaddress._BaseAddress]:
    """Resolve a hostname to all IP addresses for SSRF screening."""
    addresses: list[ipaddress._BaseAddress] = []
    try:
        infos = socket.getaddrinfo(hostname, None)
    except (socket.gaierror, UnicodeError):
        return addresses
    for info in infos:
        sockaddr = info[4]
        try:
            addresses.append(ipaddress.ip_address(sockaddr[0]))
        except ValueError:
            continue
    return addresses


async def aresolve_host_addresses(hostname: str) -> list[ipaddress._BaseAddress]:
    """The same screening resolution, without stalling the event loop.

    ``socket.getaddrinfo`` is a blocking syscall whose own timeout is the
    platform resolver's, not ours. Called straight from async fetch code it
    holds the Gateway's single event loop -- every other tenant's stream
    included -- for the length of one slow lookup, and ``asyncio.timeout``
    cannot preempt it because nothing awaits. The loop's own ``getaddrinfo``
    runs it in the default executor and yields, so the surrounding budget
    applies and other work continues.
    """
    try:
        infos = await asyncio.get_running_loop().getaddrinfo(hostname, None)
    except (socket.gaierror, UnicodeError):
        return []
    addresses: list[ipaddress._BaseAddress] = []
    for info in infos:
        try:
            addresses.append(ipaddress.ip_address(info[4][0]))
        except ValueError:
            continue
    return addresses


def is_blocked_address(address: ipaddress._BaseAddress) -> bool:
    """Return True for addresses web tools should not reach by default.

    ``not is_global`` catches special-purpose ranges the individual flags miss,
    notably the 100.64.0.0/10 shared address space (CGNAT, Tailscale, and
    Alibaba Cloud's ``100.100.100.200`` instance metadata endpoint). The flags
    stay because some non-public forms still report ``is_global``, such as the
    NAT64 spelling of a metadata address (``64:ff9b::a9fe:a9fe``).
    """
    return not address.is_global or address.is_private or address.is_loopback or address.is_link_local or address.is_reserved or address.is_multicast or address.is_unspecified


def validate_public_http_url(
    url: str,
    *,
    allow_private_addresses: bool = False,
    action: str = "fetch",
    resolver: Callable[[str], list[ipaddress._BaseAddress]] | None = None,
) -> str | None:
    """Validate an http(s) URL before a server-side web tool fetches it.

    Returns an ``"Error: ..."`` string when the URL should be rejected, or
    ``None`` when the caller may proceed.  The check is intentionally conservative
    for self-hosted fetch/render services because those services run inside the
    deployment network and can otherwise reach cloud metadata or private hosts.
    """
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return "Error: Only http:// and https:// URLs are supported"

    if allow_private_addresses:
        return None

    hostname = parsed.hostname
    if not hostname:
        return "Error: URL host could not be parsed"

    normalized_host = hostname.strip().rstrip(".").lower()
    if normalized_host in _BLOCKED_HOSTNAMES:
        return f"Error: Refusing to {action} a private or loopback address"

    try:
        literal_ip = ipaddress.ip_address(normalized_host)
    except ValueError:
        literal_ip = None

    if literal_ip is not None:
        candidates = [literal_ip]
    else:
        resolve = resolver or resolve_host_addresses
        candidates = resolve(hostname)
        if not candidates:
            return "Error: URL host could not be resolved"

    if any(is_blocked_address(addr) for addr in candidates):
        return f"Error: Refusing to {action} a private, loopback, or metadata address"
    return None
