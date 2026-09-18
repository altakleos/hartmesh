"""Fetch a public web page from the Gateway, through controls this repo owns.

Why
---
The tenant profile's keyless ``web_fetch`` sent every page through a hosted
reader that answered a tenant's server address with a deterministic 401 for
every URL (hartmesh-tenancy/DF21). A basic capability rested on a third
party's anonymous tier, and the profile's comment asserting "keyless fetch"
was a claim nothing verified. This client makes it true by construction: the
page is fetched by the Gateway itself, under the address, redirect, size,
content-type and time controls below, with no key and no provider to refuse.

What is checked
---------------
Every address the request would touch, before it is touched:

* the URL is ``http`` or ``https`` with a host (``validate_public_http_url``);
* the host resolves, and *every* resolved address is public: none in the
  canonical ``NEVER_ALLOWED_NETWORKS`` set the sandbox egress policy and the
  provisioner share (private, loopback, link-local, carrier NAT, multicast,
  documentation, cloud metadata), none the stdlib flags as non-global;
* the connection is made to the address that was checked, not to a second
  resolution the client would do on its own: the request goes to the IP
  literal with ``Host`` and the TLS ``sni_hostname`` set to the name, so the
  certificate is still verified against the name (measured: a wrong or
  missing SNI is rejected by the verifier) and a resolver that answers
  differently the second time gains nothing;
* redirects are not followed by the client. Each ``Location`` is resolved
  against the current URL and goes through the same checks, up to
  :data:`MAX_REDIRECTS` hops (a site the tenant asked for needed six);
* the body is read in chunks up to :data:`MAX_BODY_BYTES` and refused past
  it, so a page cannot fill the Gateway's memory; only HTML, XHTML and plain
  text are read at all;
* one budget, :data:`DEFAULT_TIMEOUT_SECONDS`, covers the whole chain.

What comes back is typed: a :class:`FetchedPage`, or a :class:`FetchRefusal`
that says who refused (``origin``: that address; ``provider``: this path, for
every address) and what kind of refusal it was, in the vocabulary the tool
result metadata already uses. Nothing here reads the refusal back out of a
sentence.

What is not done: no cookies, no credentials, no retries, and no attempt to
pass a challenge a site puts in front of automated clients -- a 403 is
reported as the origin's refusal and the model is told to use another source.
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

import httpx

from deerflow.community.url_safety import aresolve_host_addresses, is_blocked_address, validate_public_http_url
from deerflow.community.web_fetch_outcome import FetchRefusal
from deerflow.sandbox.egress import NEVER_ALLOWED_NETWORKS

logger = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_TIMEOUT_SECONDS",
    "MAX_BODY_BYTES",
    "MAX_REDIRECTS",
    "READABLE_CONTENT_TYPES",
    "USER_AGENT",
    "DirectFetchClient",
    "FetchRefusal",
    "FetchedPage",
]

#: The whole chain -- every hop, the body included -- within one budget.
DEFAULT_TIMEOUT_SECONDS = 10.0
#: A page the model reads is text; past this it is a download, not a source.
MAX_BODY_BYTES = 2 * 1024 * 1024
#: Hops a redirect chain may take. One site the tenant asked for took six
#: (http to https to www to a trailing slash and on); eight leaves room
#: without letting a loop run long.
MAX_REDIRECTS = 8
#: What the reader can turn into text. A PDF or an image is refused by type
#: rather than downloaded and discarded.
READABLE_CONTENT_TYPES = frozenset({"text/html", "application/xhtml+xml", "text/plain"})
#: The same honest identity the search client sends. Not a browser string.
USER_AGENT = "Mozilla/5.0 (compatible; DeerFlow/1.0)"

_NEVER_ALLOWED = tuple(ipaddress.ip_network(value) for value in NEVER_ALLOWED_NETWORKS)
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_BODY_CHUNK = 64 * 1024

#: Resolution is awaited, never called: a blocking ``getaddrinfo`` here would
#: hold the Gateway's event loop for every other tenant, and the fetch budget
#: could not preempt it.
Resolver = Callable[[str], Awaitable[list[ipaddress._BaseAddress]]]


@dataclass(frozen=True, slots=True)
class FetchedPage:
    """A page that was read: where it finally came from, and what it was."""

    url: str
    status_code: int
    content_type: str
    text: str


def _blocked(address: ipaddress._BaseAddress) -> bool:
    return is_blocked_address(address) or any(address in network for network in _NEVER_ALLOWED)


async def _pin(url: str, resolver: Resolver) -> tuple[str, str, ipaddress._BaseAddress] | FetchRefusal:
    """The validated URL, its host name, and the one address it will be sent to.

    One resolution: the addresses the validator judges are the addresses the
    request is pinned to, so there is no second lookup for a resolver to
    answer differently.
    """
    parsed = urlsplit(url)
    hostname = parsed.hostname
    if parsed.scheme not in {"http", "https"} or not hostname:
        return FetchRefusal(scope="origin", error_type="permission", reason="only http and https addresses can be fetched")
    try:
        literal = ipaddress.ip_address(hostname.strip("[]"))
    except ValueError:
        literal = None
    addresses = [literal] if literal is not None else list(await resolver(hostname))
    error = validate_public_http_url(url, resolver=lambda _name: addresses)
    if error is not None:
        reason = "the address could not be resolved" if "resolved" in error else "the address is private, loopback, or reserved"
        return FetchRefusal(scope="origin", error_type="permission", reason=reason)
    if any(_blocked(address) for address in addresses):
        return FetchRefusal(scope="origin", error_type="permission", reason="the address is private, loopback, or reserved")
    return url, hostname, addresses[0]


def _pinned_url(url: str, address: ipaddress._BaseAddress) -> str:
    parsed = urlsplit(url)
    host = f"[{address}]" if address.version == 6 else str(address)
    netloc = f"{host}:{parsed.port}" if parsed.port else host
    return urlunsplit((parsed.scheme, netloc, parsed.path or "/", parsed.query, ""))


def _host_header(url: str) -> str:
    parsed = urlsplit(url)
    hostname = parsed.hostname or ""
    if ":" in hostname:
        hostname = f"[{hostname}]"
    return f"{hostname}:{parsed.port}" if parsed.port else hostname


def _media_type(content_type: str) -> str:
    return content_type.split(";", 1)[0].strip().lower()


def _refusal_for_status(status_code: int) -> FetchRefusal:
    """The origin's status, as the kind of refusal the model can act on.

    All of these are the origin's: a paywall's 401 says nothing about the next
    address, so none is ``provider`` scope and all are recoverable by choosing
    another source. The ``error_type`` values are the ones the keyword
    classifier already emits, so downstream readers need no new vocabulary.
    """
    if status_code == 401:
        return FetchRefusal("origin", "auth", "this page requires a login", status_code)
    if status_code == 403:
        return FetchRefusal("origin", "permission", "this site refuses automated readers", status_code)
    if status_code in {404, 410}:
        return FetchRefusal("origin", "not_found", "this page does not exist", status_code)
    if status_code == 429:
        return FetchRefusal("origin", "rate_limited", "this site is rate-limiting requests", status_code)
    if 500 <= status_code < 600:
        return FetchRefusal("origin", "transient", "this site answered with a server error", status_code)
    return FetchRefusal("origin", "unknown", f"this site answered with HTTP {status_code}", status_code)


class DirectFetchClient:
    """One fetch, pinned to the addresses it was checked against."""

    def __init__(
        self,
        *,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        max_body_bytes: int = MAX_BODY_BYTES,
        max_redirects: int = MAX_REDIRECTS,
        readable_content_types: Iterable[str] = READABLE_CONTENT_TYPES,
        resolver: Resolver | None = None,
        trust_env: bool = True,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._timeout = float(timeout_seconds)
        self._max_body_bytes = int(max_body_bytes)
        self._max_redirects = int(max_redirects)
        self._readable = frozenset(_media_type(value) for value in readable_content_types)
        self._resolver = resolver or aresolve_host_addresses
        self._trust_env = trust_env
        self._transport = transport

    async def fetch(self, url: str) -> FetchedPage | FetchRefusal:
        client_kwargs: dict[str, Any] = {"follow_redirects": False, "trust_env": self._trust_env, "timeout": self._timeout}
        if self._transport is not None:
            client_kwargs["transport"] = self._transport
        try:
            async with asyncio.timeout(self._timeout):
                async with httpx.AsyncClient(**client_kwargs) as client:
                    return await self._follow(client, url)
        except TimeoutError:
            return FetchRefusal("origin", "transient", "this site did not answer in time")
        except (httpx.InvalidURL, ValueError):
            # The address is the model's text; one it cannot even parse is
            # refused by name rather than raised out of the tool.
            return FetchRefusal("origin", "permission", "this is not a valid web address")
        except httpx.HTTPError as exc:
            logger.info("Direct fetch failed: %s", type(exc).__name__)
            return FetchRefusal("origin", "transient", "this site could not be reached")

    async def _follow(self, client: httpx.AsyncClient, url: str) -> FetchedPage | FetchRefusal:
        current = url
        for _hop in range(self._max_redirects + 1):
            pinned = await _pin(current, self._resolver)
            if isinstance(pinned, FetchRefusal):
                return pinned
            current, hostname, address = pinned
            headers = {"Host": _host_header(current), "User-Agent": USER_AGENT, "Accept": "text/html, application/xhtml+xml, text/plain;q=0.9, */*;q=0.1"}
            extensions: dict[str, Any] = {}
            if current.startswith("https://"):
                extensions["sni_hostname"] = hostname
            request = client.build_request("GET", _pinned_url(current, address), headers=headers, extensions=extensions)
            response = await client.send(request, stream=True)
            try:
                if response.status_code in _REDIRECT_STATUSES:
                    location = response.headers.get("location")
                    if not location:
                        return FetchRefusal("origin", "transient", "this site redirected nowhere")
                    current = urljoin(current, location)
                    continue
                if response.status_code != 200:
                    return _refusal_for_status(response.status_code)
                media_type = _media_type(response.headers.get("content-type", ""))
                if media_type not in self._readable:
                    return FetchRefusal("origin", "unsupported", f"this address serves {media_type or 'an unknown type'}, not a readable page", 200)
                body = bytearray()
                async for chunk in response.aiter_bytes(_BODY_CHUNK):
                    body.extend(chunk)
                    if len(body) > self._max_body_bytes:
                        return FetchRefusal("origin", "unsupported", "this page is larger than a readable source", 200)
                encoding = response.encoding or "utf-8"
                try:
                    text = bytes(body).decode(encoding, errors="replace")
                except LookupError:
                    text = bytes(body).decode("utf-8", errors="replace")
                return FetchedPage(url=current, status_code=200, content_type=media_type, text=text)
            finally:
                await response.aclose()
        return FetchRefusal("origin", "transient", f"this address redirected more than {self._max_redirects} times")
