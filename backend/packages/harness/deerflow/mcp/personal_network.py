"""Public-network policy for HTTP MCP connections that must use public endpoints.

Two client factories share one transport. ``personal_httpx_client_factory`` is
for a non-operator personal connection on any deployment: public addresses
only. ``tenant_written_httpx_client_factory`` is for every other remote
connection and OAuth token request on a deployment whose connections are
written by tenants (``community/endpoint_policy.py``): the same rule, except
that a provider-listed private endpoint is reached by ordinary name lookup.
Neither follows a redirect or reads proxy and trust settings from the
environment.
"""

import asyncio
import ipaddress
import ssl

import httpx

from deerflow.community.endpoint_policy import is_private_endpoint
from deerflow.community.url_safety import resolve_host_addresses, validate_public_http_url


def _public_addresses(url: httpx.URL) -> list[str]:
    addresses = []

    def resolve(host):
        candidates = resolve_host_addresses(host)
        addresses.extend(str(address) for address in candidates)
        return candidates

    if validate_public_http_url(str(url), action="connect to", resolver=resolve):
        raise ValueError("MCP connections require a public HTTP(S) endpoint")
    # Literal IP URLs are validated without calling the resolver.
    return list(dict.fromkeys(addresses)) if addresses else [str(ipaddress.ip_address(url.host))]


class _PersonalTransport(httpx.AsyncBaseTransport):
    def __init__(self, *, allow_listed_private_endpoints: bool = False):
        self._allow_listed_private_endpoints = allow_listed_private_endpoints
        self._ssl_context: ssl.SSLContext | None = None
        self._ssl_context_lock = asyncio.Lock()
        self._transports: dict[tuple[bytes, bytes, int | None], httpx.AsyncHTTPTransport] = {}

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        # A provider-listed private endpoint skips the public-address check and
        # the pinning that depends on it; every other request is vetted again.
        listed = self._allow_listed_private_endpoints and is_private_endpoint(request.url.scheme, request.url.host, request.url.port)
        addresses = [] if listed else await asyncio.to_thread(_public_addresses, request.url)
        # The synchronous client factory can run on the event loop. Load TLS
        # trust lazily off-loop, sharing one context across concurrent requests.
        if self._ssl_context is None:
            async with self._ssl_context_lock:
                if self._ssl_context is None:
                    self._ssl_context = await asyncio.to_thread(httpx.create_ssl_context, trust_env=False)
        origin = (request.url.raw_scheme, request.url.raw_host, request.url.port)
        # Pin TCP to the vetted IP, but isolate pools by original origin so two
        # names sharing an IP cannot reuse a connection authenticated for one.
        if origin not in self._transports:
            self._transports[origin] = httpx.AsyncHTTPTransport(verify=self._ssl_context, trust_env=False)
        transport = self._transports[origin]
        if listed:
            return await transport.handle_async_request(request)
        for index, address in enumerate(addresses):
            pinned = httpx.Request(
                request.method,
                request.url.copy_with(host=address),
                headers=request.headers,
                stream=request.stream,
                extensions={**request.extensions, "sni_hostname": request.url.raw_host.decode("ascii")},
            )
            try:
                return await transport.handle_async_request(pinned)
            except (httpx.ConnectError, httpx.ConnectTimeout):
                if index == len(addresses) - 1:
                    raise
        raise AssertionError("Public address validation returned no addresses")

    async def aclose(self):
        for transport in self._transports.values():
            await transport.aclose()


def _client(transport: _PersonalTransport, headers, timeout, auth) -> httpx.AsyncClient:
    kwargs = {"headers": headers, "auth": auth, "follow_redirects": False, "trust_env": False, "transport": transport}
    if timeout is not None:
        kwargs["timeout"] = timeout
    return httpx.AsyncClient(**kwargs)


def personal_httpx_client_factory(headers=None, timeout=None, auth=None):
    """Public addresses only."""
    return _client(_PersonalTransport(), headers, timeout, auth)


def tenant_written_httpx_client_factory(headers=None, timeout=None, auth=None):
    """Public addresses, or a private endpoint the provider listed."""
    return _client(_PersonalTransport(allow_listed_private_endpoints=True), headers, timeout, auth)
