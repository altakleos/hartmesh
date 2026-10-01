"""A generic OpenID Connect provider the tests start themselves.

One loopback HTTP server, any number of issuers under it (``/a``, ``/b``), each
with its own RSA signing key. Discovery, authorization, token, JWKS and
userinfo, and nothing vendor-specific. The authorization endpoint has no
login page: the test names who is signing in with two extra query parameters
(``test_subject``, ``test_email``), which a real provider would learn from a
password form. Everything a real provider would check is checked and
recorded so a test can assert it happened: the redirect URI, the PKCE
verifier against the S256 challenge, the client's credentials in the method
it was registered with, and the nonce that goes into the ID token.

The test also says what else the token and userinfo carry (``test_claims``,
``test_userinfo_claims``, JSON): that is how a membership claim of any shape
is put into the ID token, into userinfo, or into both, under any name.
"""

from __future__ import annotations

import base64
import hashlib
import json
import secrets
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa

DISCOVERY = "/.well-known/openid-configuration"


@dataclass
class Issuer:
    name: str
    private_key: Any
    kid: str
    # Codes issued and not yet redeemed: code -> what the authorization request carried.
    pending: dict[str, dict[str, str]] = field(default_factory=dict)
    # Access tokens issued: token -> subject/email, for userinfo.
    sessions: dict[str, dict[str, Any]] = field(default_factory=dict)
    # What the last token exchange presented, for the tests to assert on.
    exchanges: list[dict[str, Any]] = field(default_factory=list)
    # Test hook: emit a token whose nonce is not the one the request carried.
    wrong_nonce: bool = False


class OIDCTestProvider:
    """Starts on ``127.0.0.1:0``; ``issuer_url(name)`` is what a Gateway configures."""

    def __init__(self, issuers: tuple[str, ...] = ("a",), *, client_id: str, client_secret: str) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.issuers: dict[str, Issuer] = {name: Issuer(name=name, private_key=rsa.generate_private_key(public_exponent=65537, key_size=2048), kid=f"{name}-key-1") for name in issuers}
        provider = self
        self.log: list[str] = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - BaseHTTPRequestHandler's signature
                provider.log.append(format % args)

            def do_GET(self) -> None:  # noqa: N802
                provider._get(self)

            def do_POST(self) -> None:  # noqa: N802
                provider._post(self)

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, name="oidc-test-provider", daemon=True)

    # ── lifecycle ────────────────────────────────────────────────────────

    def start(self) -> OIDCTestProvider:
        self._thread.start()
        return self

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    @property
    def base_url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    def issuer_url(self, name: str = "a") -> str:
        return f"{self.base_url}/{name}"

    # ── what a test does at the "login page" ─────────────────────────────

    @staticmethod
    def sign_in_url(authorization_url: str, *, subject: str, email: str, email_verified: bool = True, claims: dict[str, Any] | None = None, userinfo_claims: dict[str, Any] | None = None) -> str:
        """The authorization URL with the test's answer to the login form appended.

        ``claims`` go into the ID token only and ``userinfo_claims`` into the
        userinfo response only, so a test controls which source carries what.
        """
        joiner = "&" if "?" in authorization_url else "?"
        answer = {"test_subject": subject, "test_email": email, "test_email_verified": "1" if email_verified else "0"}
        if claims:
            answer["test_claims"] = json.dumps(claims)
        if userinfo_claims:
            answer["test_userinfo_claims"] = json.dumps(userinfo_claims)
        return authorization_url + joiner + urlencode(answer)

    # ── routing ──────────────────────────────────────────────────────────

    def _issuer_for(self, path: str) -> tuple[Issuer | None, str]:
        parts = path.split("/", 2)
        name = parts[1] if len(parts) > 1 else ""
        rest = "/" + parts[2] if len(parts) > 2 else "/"
        return self.issuers.get(name), rest

    def _get(self, handler: BaseHTTPRequestHandler) -> None:
        url = urlparse(handler.path)
        issuer, rest = self._issuer_for(url.path)
        if issuer is None:
            return self._json(handler, 404, {"error": "unknown issuer"})
        if rest == DISCOVERY:
            base = self.issuer_url(issuer.name)
            return self._json(
                handler,
                200,
                {
                    "issuer": base,
                    "authorization_endpoint": f"{base}/authorize",
                    "token_endpoint": f"{base}/token",
                    "userinfo_endpoint": f"{base}/userinfo",
                    "jwks_uri": f"{base}/jwks",
                    "response_types_supported": ["code"],
                    "subject_types_supported": ["public"],
                    "id_token_signing_alg_values_supported": ["RS256"],
                    "code_challenge_methods_supported": ["S256"],
                    "token_endpoint_auth_methods_supported": ["client_secret_post", "client_secret_basic"],
                },
            )
        if rest == "/jwks":
            public = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(issuer.private_key.public_key()))
            public.update({"kid": issuer.kid, "use": "sig", "alg": "RS256"})
            return self._json(handler, 200, {"keys": [public]})
        if rest == "/authorize":
            return self._authorize(handler, issuer, parse_qs(url.query))
        if rest == "/userinfo":
            token = handler.headers.get("Authorization", "").removeprefix("Bearer ").strip()
            session = issuer.sessions.get(token)
            if session is None:
                return self._json(handler, 401, {"error": "invalid_token"})
            return self._json(handler, 200, {"sub": session["sub"], "email": session["email"], "email_verified": session["email_verified"], "name": session["sub"].title(), **session["userinfo_claims"]})
        return self._json(handler, 404, {"error": "not found"})

    def _authorize(self, handler: BaseHTTPRequestHandler, issuer: Issuer, query: dict[str, list[str]]) -> None:
        def one(name: str) -> str:
            values = query.get(name) or [""]
            return values[0]

        if one("response_type") != "code" or one("client_id") != self.client_id:
            return self._json(handler, 400, {"error": "unauthorized_client"})
        if not one("redirect_uri") or not one("state") or one("code_challenge_method") not in ("", "S256"):
            return self._json(handler, 400, {"error": "invalid_request"})
        if not one("test_subject"):
            # No login page: a request nobody answered stays here.
            return self._json(handler, 200, {"login": "who is signing in? add test_subject and test_email"})
        code = secrets.token_urlsafe(24)
        issuer.pending[code] = {
            "client_id": one("client_id"),
            "redirect_uri": one("redirect_uri"),
            "scope": one("scope"),
            "nonce": one("nonce"),
            "code_challenge": one("code_challenge"),
            "sub": one("test_subject"),
            "email": one("test_email"),
            "email_verified": one("test_email_verified") != "0",
            "claims": json.loads(one("test_claims") or "{}"),
            "userinfo_claims": json.loads(one("test_userinfo_claims") or "{}"),
        }
        location = one("redirect_uri") + ("&" if "?" in one("redirect_uri") else "?") + urlencode({"code": code, "state": one("state")})
        handler.send_response(302)
        handler.send_header("Location", location)
        handler.send_header("Content-Length", "0")
        handler.end_headers()

    def _post(self, handler: BaseHTTPRequestHandler) -> None:
        url = urlparse(handler.path)
        issuer, rest = self._issuer_for(url.path)
        if issuer is None or rest != "/token":
            return self._json(handler, 404, {"error": "not found"})
        length = int(handler.headers.get("Content-Length") or 0)
        form = {key: values[0] for key, values in parse_qs(handler.rfile.read(length).decode("utf-8")).items()}

        # Client authentication, in whichever method the client used.
        auth_header = handler.headers.get("Authorization", "")
        if auth_header.startswith("Basic "):
            method = "client_secret_basic"
            decoded = base64.b64decode(auth_header.removeprefix("Basic ")).decode("utf-8")
            presented_id, _, presented_secret = decoded.partition(":")
        else:
            method = "client_secret_post"
            presented_id, presented_secret = form.get("client_id", ""), form.get("client_secret", "")
        exchange: dict[str, Any] = {"method": method, "client_id": presented_id, "pkce_verified": False, "ok": False}
        issuer.exchanges.append(exchange)
        if presented_id != self.client_id or not secrets.compare_digest(presented_secret, self.client_secret):
            return self._json(handler, 401, {"error": "invalid_client"})
        pending = issuer.pending.pop(form.get("code", ""), None)
        if pending is None or form.get("grant_type") != "authorization_code" or form.get("redirect_uri") != pending["redirect_uri"]:
            return self._json(handler, 400, {"error": "invalid_grant"})
        if pending["code_challenge"]:
            verifier = form.get("code_verifier", "")
            challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest()).rstrip(b"=").decode("ascii")
            if not verifier or not secrets.compare_digest(challenge, pending["code_challenge"]):
                return self._json(handler, 400, {"error": "invalid_grant", "error_description": "PKCE verification failed"})
            exchange["pkce_verified"] = True

        now = int(time.time())
        access_token = secrets.token_urlsafe(24)
        issuer.sessions[access_token] = {"sub": pending["sub"], "email": pending["email"], "email_verified": pending["email_verified"], "userinfo_claims": pending["userinfo_claims"]}
        claims: dict[str, Any] = {
            "iss": self.issuer_url(issuer.name),
            "sub": pending["sub"],
            "aud": self.client_id,
            "exp": now + 300,
            "iat": now,
            "email": pending["email"],
            "email_verified": pending["email_verified"],
        }
        claims.update(pending["claims"])
        if pending["nonce"]:
            claims["nonce"] = ("not-" + pending["nonce"]) if issuer.wrong_nonce else pending["nonce"]
        id_token = jwt.encode(claims, issuer.private_key, algorithm="RS256", headers={"kid": issuer.kid})
        exchange["ok"] = True
        exchange["nonce"] = claims.get("nonce")
        return self._json(handler, 200, {"access_token": access_token, "token_type": "Bearer", "expires_in": 300, "id_token": id_token, "scope": pending["scope"]})

    @staticmethod
    def _json(handler: BaseHTTPRequestHandler, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)
