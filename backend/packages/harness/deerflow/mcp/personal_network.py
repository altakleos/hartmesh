"""Public-network policy for HTTP MCP connections that must use public endpoints.

Two client factories share one transport. ``personal_httpx_client_factory`` is
for a non-operator personal connection on any deployment: public addresses
only. ``tenant_written_httpx_client_factory`` is for every other remote
connection and OAuth token request on a deployment whose connections are
written by tenants (``community/endpoint_policy.py``): the same rule, except
that a provider-listed private endpoint is reached by ordinary name lookup.
Both clients have redirect following off and read no proxy or trust settings
from the environment, so a listed HTTPS endpoint needs a publicly trusted
certificate. The MCP transports follow a redirect that stays on the endpoint's
origin themselves, through this same transport; a redirect to another origin
is not followed.
"""

import asyncio
import ipaddress
import os
import socket
import ssl
import threading
from functools import lru_cache

import httpx

from deerflow.community.endpoint_policy import is_private_endpoint
from deerflow.community.url_safety import resolve_host_addresses, validate_public_http_url
from deerflow.mcp.discovery_errors import EndpointAddressRefused


def _public_addresses(url: httpx.URL, refusal: str) -> list[str]:
    addresses = []

    def resolve(host):
        candidates = resolve_host_addresses(host)
        addresses.extend(str(address) for address in candidates)
        return candidates

    if validate_public_http_url(str(url), action="connect to", resolver=resolve):
        raise EndpointAddressRefused(refusal)
    # Literal IP URLs are validated without calling the resolver.
    return list(dict.fromkeys(addresses)) if addresses else [str(ipaddress.ip_address(url.host))]


class _PersonalTransport(httpx.AsyncBaseTransport):
    def __init__(self, *, allow_listed_private_endpoints: bool = False):
        self._allow_listed_private_endpoints = allow_listed_private_endpoints
        # A personal connection keeps the text it always had, on every deployment.
        self._refusal = "MCP connections require a public HTTP(S) endpoint" if allow_listed_private_endpoints else "Personal MCP connections require a public HTTP(S) endpoint"
        self._ssl_context: ssl.SSLContext | None = None
        self._ssl_context_lock = asyncio.Lock()
        self._transports: dict[tuple[bytes, bytes, int | None], httpx.AsyncHTTPTransport] = {}

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        # A provider-listed private endpoint skips the public-address check and
        # the pinning that depends on it; every other request is vetted again.
        # The host is compared in the encoded form that goes on the wire.
        listed = self._allow_listed_private_endpoints and is_private_endpoint(request.url.scheme, request.url.raw_host.decode("ascii"), request.url.port)
        addresses = [] if listed else await asyncio.to_thread(_public_addresses, request.url, self._refusal)
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
            try:
                return await transport.handle_async_request(_pinned(request, address))
            except (httpx.ConnectError, httpx.ConnectTimeout):
                if index == len(addresses) - 1:
                    raise
        raise AssertionError("Public address validation returned no addresses")

    async def aclose(self):
        for transport in self._transports.values():
            await transport.aclose()


def _pinned(request: httpx.Request, address: str) -> httpx.Request:
    """The same request addressed to one vetted IP, keeping the name for TLS and the Host header."""
    return httpx.Request(
        request.method,
        request.url.copy_with(host=address),
        headers=request.headers,
        stream=request.stream,
        extensions={**request.extensions, "sni_hostname": request.url.raw_host.decode("ascii")},
    )


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


# ── Custom model endpoints on a tenant-written deployment ────────────────────
#
# The same rule for a model whose address a tenant chose: public addresses or
# a provider-listed private endpoint, checked at every request and pinned to
# the checked address, with redirect following off. A model is built for every
# reply, is called both the plain and the asynchronous way from more than one
# event loop and thread, and never closes a client it was handed. These
# transports therefore keep nothing: each request gets a connection of its
# own, closed with its response.

_MODEL_REFUSAL = "Custom model endpoints require a public HTTP(S) endpoint"


@lru_cache(maxsize=1)
def _supported_model_socket_options() -> tuple[tuple[int, int, int], ...]:
    """The keepalive profile the model library gives the clients it builds itself, less what this system refuses.

    It notices a silent peer within about two minutes. The values are fixed:
    the library's tuning variables are not read, only its switch
    (:func:`_model_socket_options`). The library's own helpers are private, so
    the options are probed here, once: one this system does not know is
    dropped rather than failing every connection. A probe socket that cannot
    be made raises, and an error is not kept, so the next call probes again.
    """
    wanted = [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)]
    for name, value in (("TCP_KEEPIDLE", 60), ("TCP_KEEPINTVL", 10), ("TCP_KEEPCNT", 3), ("TCP_USER_TIMEOUT", 120_000)):
        option = getattr(socket, name, None)
        if option is not None:
            wanted.append((socket.IPPROTO_TCP, option, value))
    supported: list[tuple[int, int, int]] = []
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        for option in wanted:
            try:
                probe.setsockopt(*option)
            except OSError:
                continue
            supported.append(option)
    return tuple(supported)


def _model_socket_options() -> list[tuple[int, int, int]] | None:
    """The options for one model connection; the model library's own switch turns them off here too."""
    if os.environ.get("LANGCHAIN_OPENAI_TCP_KEEPALIVE", "1") == "0":
        return None
    try:
        return list(_supported_model_socket_options()) or None
    except Exception:  # noqa: BLE001 - as the library does: a system can refuse the probe socket with any error
        # This connection goes without the options; the next one probes again.
        return None


_model_trust_lock = threading.Lock()
_model_trust: ssl.SSLContext | None = None


def _shared_model_trust() -> ssl.SSLContext:
    """One TLS trust for every model request; loading it blocks, so async callers use a worker thread."""
    global _model_trust
    with _model_trust_lock:
        if _model_trust is None:
            _model_trust = httpx.create_ssl_context(trust_env=False)
        return _model_trust


# A custom model has no organization or project setting, so no request of
# these clients carries either header.
_MODEL_HEADERS_NOT_SENT = ("OpenAI-Organization", "OpenAI-Project")


def _model_request_candidates(request: httpx.Request) -> list[httpx.Request]:
    """The requests to try in order, less the headers above: a listed endpoint at its own address, else one per vetted address. May resolve a name."""
    for name in _MODEL_HEADERS_NOT_SENT:
        request.headers.pop(name, None)
    # The host is compared in the encoded form that goes on the wire.
    if is_private_endpoint(request.url.scheme, request.url.raw_host.decode("ascii"), request.url.port):
        return [request]
    return [_pinned(request, address) for address in _public_addresses(request.url, _MODEL_REFUSAL)]


class _ClosingStream(httpx.SyncByteStream):
    def __init__(self, stream, transport: httpx.HTTPTransport):
        self._stream = stream
        self._transport = transport

    def __iter__(self):
        yield from self._stream

    def close(self) -> None:
        try:
            self._stream.close()
        finally:
            self._transport.close()


class _ClosingAsyncStream(httpx.AsyncByteStream):
    def __init__(self, stream, transport: httpx.AsyncHTTPTransport):
        self._stream = stream
        self._transport = transport

    async def __aiter__(self):
        async for chunk in self._stream:
            yield chunk

    async def aclose(self) -> None:
        try:
            await self._stream.aclose()
        finally:
            await self._transport.aclose()


class _ModelTransport(httpx.BaseTransport):
    def handle_request(self, request: httpx.Request) -> httpx.Response:
        candidates = _model_request_candidates(request)
        trust = _shared_model_trust()
        for index, candidate in enumerate(candidates):
            transport = httpx.HTTPTransport(verify=trust, trust_env=False, socket_options=_model_socket_options())
            try:
                response = transport.handle_request(candidate)
            except BaseException as exc:
                transport.close()
                if isinstance(exc, httpx.ConnectError | httpx.ConnectTimeout) and index < len(candidates) - 1:
                    continue
                raise
            response.stream = _ClosingStream(response.stream, transport)
            return response
        raise AssertionError("Public address validation returned no addresses")


class _ModelAsyncTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        candidates = await asyncio.to_thread(_model_request_candidates, request)
        trust = _model_trust or await asyncio.to_thread(_shared_model_trust)
        for index, candidate in enumerate(candidates):
            transport = httpx.AsyncHTTPTransport(verify=trust, trust_env=False, socket_options=_model_socket_options())
            try:
                response = await transport.handle_async_request(candidate)
            except BaseException as exc:
                await transport.aclose()
                if isinstance(exc, httpx.ConnectError | httpx.ConnectTimeout) and index < len(candidates) - 1:
                    continue
                raise
            response.stream = _ClosingAsyncStream(response.stream, transport)
            return response
        raise AssertionError("Public address validation returned no addresses")


def public_endpoint_model_clients() -> tuple[httpx.Client, httpx.AsyncClient]:
    """The plain and the asynchronous HTTP client of one custom model.

    Building them does no I/O, and they hold nothing open between requests,
    so nobody has to close them.
    """
    return (
        httpx.Client(transport=_ModelTransport(), follow_redirects=False, trust_env=False),
        httpx.AsyncClient(transport=_ModelAsyncTransport(), follow_redirects=False, trust_env=False),
    )


def public_endpoint_model_async_client(*, timeout: float) -> httpx.AsyncClient:
    """An asynchronous client under the same rule, for a caller that closes it (model discovery)."""
    return httpx.AsyncClient(transport=_ModelAsyncTransport(), timeout=timeout, follow_redirects=False, trust_env=False)
