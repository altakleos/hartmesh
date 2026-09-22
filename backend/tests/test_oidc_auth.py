from unittest.mock import ANY, AsyncMock

import pytest
from fastapi import HTTPException

from app.gateway.auth.models import User
from app.gateway.auth.oidc import OIDCError, OIDCIdentity, OIDCMetadata, OIDCService, OIDCValidationError
from app.gateway.auth.user_provisioning import get_or_provision_oidc_user
from deerflow.config.auth_config import OIDCProviderConfig


def _provider_config(**overrides):
    return OIDCProviderConfig(
        display_name="Test SSO",
        issuer="https://issuer.example.com",
        client_id="deer-flow",
        **overrides,
    )


def _identity(**overrides):
    values = {
        "provider": "keycloak",
        "subject": "oidc-subject",
        "email": "user@example.com",
        "email_verified": True,
        "name": "Test User",
        "claims": {},
    }
    values.update(overrides)
    return OIDCIdentity(**values)


@pytest.mark.asyncio
async def test_oidc_existing_local_account_blocks_sso_login_even_when_unverified():
    local_user = User(email="user@example.com", password_hash="hash")
    local_provider = AsyncMock()
    local_provider.is_identity_disabled.return_value = False
    local_provider.record_sign_in.return_value = None
    local_provider.get_user_by_oauth.return_value = None
    local_provider.get_user_by_email.return_value = local_user

    with pytest.raises(HTTPException) as exc_info:
        await get_or_provision_oidc_user(
            provider_id="keycloak",
            provider_config=_provider_config(
                require_verified_email=False,
                auto_create_users=False,
            ),
            identity=_identity(email_verified=False),
            local_provider=local_provider,
        )

    assert exc_info.value.status_code == 409
    local_provider.update_user.assert_not_called()


@pytest.mark.asyncio
async def test_oidc_existing_local_account_blocks_sso_login_even_when_verified():
    local_user = User(email="user@example.com", password_hash="hash")
    local_provider = AsyncMock()
    local_provider.is_identity_disabled.return_value = False
    local_provider.get_user_by_oauth.return_value = None
    local_provider.get_user_by_email.return_value = local_user

    with pytest.raises(HTTPException) as exc_info:
        await get_or_provision_oidc_user(
            provider_id="keycloak",
            provider_config=_provider_config(auto_create_users=False),
            identity=_identity(subject="verified-subject"),
            local_provider=local_provider,
        )

    assert exc_info.value.status_code == 409
    local_provider.update_user.assert_not_called()
    local_provider.create_oauth_user.assert_not_called()


@pytest.mark.asyncio
async def test_oidc_auto_create_assigns_admin_role_from_configured_email():
    local_provider = AsyncMock()
    local_provider.is_identity_disabled.return_value = False
    local_provider.get_user_by_oauth.return_value = None
    local_provider.get_user_by_email.return_value = None
    created_user = User(
        email="admin@example.com",
        password_hash=None,
        system_role="admin",
        oauth_provider="keycloak",
        oauth_id="admin-subject",
    )
    local_provider.create_oauth_user.return_value = created_user

    result = await get_or_provision_oidc_user(
        provider_id="keycloak",
        provider_config=_provider_config(admin_emails=["ADMIN@example.com"]),
        identity=_identity(subject="admin-subject", email="admin@example.com"),
        local_provider=local_provider,
    )

    assert result == {"user": created_user, "created": True}
    local_provider.create_oauth_user.assert_awaited_once_with(
        email="admin@example.com",
        oauth_provider="keycloak",
        oauth_id="admin-subject",
        system_role="admin",
        oauth_issuer="https://issuer.example.com",
        last_sign_in_at=ANY,
    )


@pytest.mark.asyncio
async def test_oidc_validate_id_token_refreshes_jwks_once_on_kid_miss(monkeypatch):
    service = OIDCService()
    metadata = OIDCMetadata(
        issuer="https://issuer.example.com",
        authorization_endpoint="https://issuer.example.com/auth",
        token_endpoint="https://issuer.example.com/token",
        userinfo_endpoint=None,
        jwks_uri="https://issuer.example.com/jwks",
    )
    load_calls = []
    resolve_results = [None, "signing-key"]

    async def load_jwks(jwks_uri, force_refresh=False):
        load_calls.append(force_refresh)
        return {"keys": []}

    async def resolve_signing_key(jwks_data, kid, algorithm, jwks_uri):
        return resolve_results.pop(0)

    monkeypatch.setattr(service, "_load_jwks", load_jwks)
    monkeypatch.setattr(service, "_resolve_signing_key", resolve_signing_key)
    monkeypatch.setattr("app.gateway.auth.oidc.jwt.get_unverified_header", lambda token: {"kid": "new-kid", "alg": "RS256"})
    monkeypatch.setattr(
        "app.gateway.auth.oidc.jwt.decode",
        lambda *args, **kwargs: {"iss": metadata.issuer, "sub": "subject", "aud": "deer-flow", "exp": 9999999999},
    )

    claims = await service.validate_id_token(metadata, "deer-flow", "id-token")

    assert claims["sub"] == "subject"
    assert load_calls == [False, True]
    await service.close()


@pytest.mark.asyncio
async def test_oidc_validate_id_token_rejects_hmac_algorithms(monkeypatch):
    service = OIDCService()
    metadata = OIDCMetadata(
        issuer="https://issuer.example.com",
        authorization_endpoint="https://issuer.example.com/auth",
        token_endpoint="https://issuer.example.com/token",
        userinfo_endpoint=None,
        jwks_uri="https://issuer.example.com/jwks",
    )

    async def load_jwks(jwks_uri, force_refresh=False):
        return {"keys": [{"kid": "kid", "kty": "oct", "k": "secret"}]}

    async def resolve_signing_key(jwks_data, kid, algorithm, jwks_uri):
        return "secret"

    def decode(*args, **kwargs):
        assert "HS256" not in kwargs["algorithms"]
        raise OIDCValidationError("HMAC algorithms must not be accepted")

    monkeypatch.setattr(service, "_load_jwks", load_jwks)
    monkeypatch.setattr(service, "_resolve_signing_key", resolve_signing_key)
    monkeypatch.setattr("app.gateway.auth.oidc.jwt.get_unverified_header", lambda token: {"kid": "kid", "alg": "HS256"})
    monkeypatch.setattr("app.gateway.auth.oidc.jwt.decode", decode)

    with pytest.raises(OIDCValidationError, match="unsupported algorithm"):
        await service.validate_id_token(metadata, "deer-flow", "id-token")

    await service.close()


@pytest.mark.asyncio
async def test_oidc_existing_account_lookup_uses_normalized_email():
    local_user = User(email="user@example.com", password_hash="hash")
    local_provider = AsyncMock()
    local_provider.is_identity_disabled.return_value = False
    local_provider.get_user_by_oauth.return_value = None
    local_provider.get_user_by_email.return_value = local_user

    with pytest.raises(HTTPException) as exc_info:
        await get_or_provision_oidc_user(
            provider_id="keycloak",
            provider_config=_provider_config(auto_create_users=False),
            identity=_identity(email="User@Example.COM"),
            local_provider=local_provider,
        )

    assert exc_info.value.status_code == 409
    local_provider.get_user_by_email.assert_awaited_once_with("user@example.com")


@pytest.mark.asyncio
async def test_oidc_auto_create_uses_normalized_email():
    local_provider = AsyncMock()
    local_provider.is_identity_disabled.return_value = False
    local_provider.get_user_by_oauth.return_value = None
    local_provider.get_user_by_email.return_value = None
    created_user = User(email="user@example.com", password_hash=None, oauth_provider="keycloak", oauth_id="subject")
    local_provider.create_oauth_user.return_value = created_user

    await get_or_provision_oidc_user(
        provider_id="keycloak",
        provider_config=_provider_config(),
        identity=_identity(subject="subject", email="User@Example.COM"),
        local_provider=local_provider,
    )

    local_provider.create_oauth_user.assert_awaited_once_with(
        email="user@example.com",
        oauth_provider="keycloak",
        oauth_id="subject",
        system_role="user",
        oauth_issuer="https://issuer.example.com",
        last_sign_in_at=ANY,
    )


@pytest.mark.asyncio
async def test_oidc_metadata_from_dict_accepts_missing_overrides():
    service = OIDCService()

    metadata = service._metadata_from_dict(
        {
            "issuer": "https://issuer.example.com",
            "authorization_endpoint": "https://issuer.example.com/auth",
            "token_endpoint": "https://issuer.example.com/token",
            "userinfo_endpoint": "https://issuer.example.com/userinfo",
            "jwks_uri": "https://issuer.example.com/jwks",
        },
        None,
    )

    assert metadata.jwks_uri == "https://issuer.example.com/jwks"
    await service.close()


@pytest.mark.asyncio
async def test_oidc_authenticate_callback_treats_string_false_email_verified_as_unverified(monkeypatch):
    service = OIDCService()
    metadata = OIDCMetadata(
        issuer="https://issuer.example.com",
        authorization_endpoint="https://issuer.example.com/auth",
        token_endpoint="https://issuer.example.com/token",
        userinfo_endpoint=None,
        jwks_uri="https://issuer.example.com/jwks",
    )

    async def exchange_code(**kwargs):
        return {"id_token": "id-token"}

    async def validate_id_token(**kwargs):
        return {"sub": "subject", "email": "user@example.com", "email_verified": "false"}

    monkeypatch.setattr(service, "exchange_code", exchange_code)
    monkeypatch.setattr(service, "validate_id_token", validate_id_token)

    identity = await service.authenticate_callback(
        provider_id="keycloak",
        metadata=metadata,
        client_id="deer-flow",
        client_secret=None,
        code="code",
        redirect_uri="https://app.example.com/callback",
    )

    assert identity.email_verified is False
    await service.close()


@pytest.mark.asyncio
async def test_oidc_provision_recovers_existing_user_on_create_race():
    """A concurrent create that loses the unique index re-resolves to the winner's row."""
    created_user = User(email="user@example.com", password_hash=None, oauth_provider="keycloak", oauth_id="subject")
    local_provider = AsyncMock()
    local_provider.is_identity_disabled.return_value = False
    # First lookup (by oauth) misses, then create races and raises, then re-lookup wins.
    local_provider.get_user_by_oauth.side_effect = [None, created_user]
    local_provider.get_user_by_email.return_value = None
    local_provider.create_oauth_user.side_effect = ValueError("Email already registered: user@example.com")

    result = await get_or_provision_oidc_user(
        provider_id="keycloak",
        provider_config=_provider_config(),
        identity=_identity(subject="subject"),
        local_provider=local_provider,
    )

    assert result == {"user": created_user, "created": False}
    assert local_provider.get_user_by_oauth.await_count == 2


@pytest.mark.asyncio
async def test_oidc_provision_create_race_on_email_only_raises_409():
    """A create race that collides on email (different identity) surfaces a clean 409, not a 500."""
    local_provider = AsyncMock()
    local_provider.is_identity_disabled.return_value = False
    # No existing oauth link before or after the race (email collision, not same subject).
    local_provider.get_user_by_oauth.return_value = None
    local_provider.get_user_by_email.return_value = None
    local_provider.create_oauth_user.side_effect = ValueError("Email already registered: user@example.com")

    with pytest.raises(HTTPException) as exc_info:
        await get_or_provision_oidc_user(
            provider_id="keycloak",
            provider_config=_provider_config(),
            identity=_identity(subject="subject"),
            local_provider=local_provider,
        )

    assert exc_info.value.status_code == 409


@pytest.mark.asyncio
async def test_oidc_discover_rejects_mismatched_issuer(monkeypatch):
    service = OIDCService()

    class _Resp:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "issuer": "https://evil.example.com",
                "authorization_endpoint": "https://issuer.example.com/auth",
                "token_endpoint": "https://issuer.example.com/token",
                "jwks_uri": "https://issuer.example.com/jwks",
            }

    async def fake_get(url):
        return _Resp()

    monkeypatch.setattr(service._http, "get", fake_get)

    with pytest.raises(OIDCError, match="does not match configured issuer"):
        await service.discover("https://issuer.example.com")

    await service.close()


@pytest.mark.asyncio
async def test_oidc_discover_accepts_issuer_with_trailing_slash_difference(monkeypatch):
    service = OIDCService()

    class _Resp:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "issuer": "https://issuer.example.com/",
                "authorization_endpoint": "https://issuer.example.com/auth",
                "token_endpoint": "https://issuer.example.com/token",
                "jwks_uri": "https://issuer.example.com/jwks",
            }

    async def fake_get(url):
        return _Resp()

    monkeypatch.setattr(service._http, "get", fake_get)

    metadata = await service.discover("https://issuer.example.com")

    assert metadata.issuer == "https://issuer.example.com/"
    await service.close()


def _redirect_request(headers: dict, scheme: str = "http", netloc: str = "localhost:8001"):
    from unittest.mock import MagicMock

    req = MagicMock()
    req.headers = headers
    req.url.scheme = scheme
    req.url.netloc = netloc
    return req


def test_oidc_redirect_uri_prefers_configured_value():
    from app.gateway.routers.auth import _resolve_oidc_redirect_uri

    cfg = _provider_config(redirect_uri="https://app.example.com/api/v1/auth/callback/keycloak")
    req = _redirect_request({"host": "attacker.example.com"})

    assert _resolve_oidc_redirect_uri(req, "keycloak", cfg) == "https://app.example.com/api/v1/auth/callback/keycloak"


def test_oidc_redirect_uri_fallback_uses_forwarded_headers_not_raw_host():
    from app.gateway.routers.auth import _resolve_oidc_redirect_uri

    cfg = _provider_config()
    # Raw Host is attacker-controlled; proxy-set X-Forwarded-* must win.
    req = _redirect_request(
        {
            "host": "attacker.example.com",
            "x-forwarded-host": "app.example.com",
            "x-forwarded-proto": "https",
        }
    )

    result = _resolve_oidc_redirect_uri(req, "keycloak", cfg)

    assert result == "https://app.example.com/api/v1/auth/callback/keycloak"


def test_oidc_redirect_uri_fallback_plain_host_when_no_proxy_headers():
    from app.gateway.routers.auth import _resolve_oidc_redirect_uri

    cfg = _provider_config()
    req = _redirect_request({"host": "localhost:8001"})

    result = _resolve_oidc_redirect_uri(req, "keycloak", cfg)

    assert result == "http://localhost:8001/api/v1/auth/callback/keycloak"


# ── Clock skew between the Gateway and the identity provider ──────────────
#
# The product validated the ID token with no leeway at all, so a Gateway
# whose clock had fallen behind the provider refused every token as "not yet
# valid (iat)" -- on a measured tenant, a 1.5 s lag between NTP polls was a
# complete sign-in outage for the whole company, showing nothing but a
# generic `sso_failed`. A VM that is a second or two behind is ordinary; the
# tolerance is bounded so a real replay is still refused.
#
# These tests sign real tokens and let PyJWT decide. Stubbing `jwt.decode`
# would assert the stub, and the whole question here is what PyJWT does with
# a timestamp on either side of the bound.


def _signing_keypair():
    from cryptography.hazmat.primitives.asymmetric import ec

    private_key = ec.generate_private_key(ec.SECP256R1())
    return private_key, private_key.public_key()


def _skew_service(monkeypatch, public_key):
    """An OIDCService whose JWKS lookup answers with ``public_key``."""
    service = OIDCService()

    async def load_jwks(jwks_uri, force_refresh=False):
        return {"keys": []}

    async def resolve_signing_key(jwks_data, kid, algorithm, jwks_uri):
        return public_key

    monkeypatch.setattr(service, "_load_jwks", load_jwks)
    monkeypatch.setattr(service, "_resolve_signing_key", resolve_signing_key)
    return service


def _skew_metadata():
    return OIDCMetadata(
        issuer="https://issuer.example.com",
        authorization_endpoint="https://issuer.example.com/auth",
        token_endpoint="https://issuer.example.com/token",
        userinfo_endpoint=None,
        jwks_uri="https://issuer.example.com/jwks",
    )


def _id_token(private_key, *, iat_offset: float = 0.0, exp_offset: float = 300.0):
    """A real ES256 ID token whose timestamps sit where the caller asks."""
    import time

    import jwt as pyjwt

    now = time.time()
    return pyjwt.encode(
        {
            "iss": "https://issuer.example.com",
            "sub": "subject",
            "aud": "deer-flow",
            "iat": now + iat_offset,
            "exp": now + exp_offset,
        },
        private_key,
        algorithm="ES256",
        headers={"kid": "skew-kid"},
    )


@pytest.mark.asyncio
async def test_a_token_issued_seconds_ahead_of_this_gateways_clock_is_accepted(monkeypatch):
    """The measured outage: a guest clock behind the provider by a second or two."""
    private_key, public_key = _signing_keypair()
    service = _skew_service(monkeypatch, public_key)

    claims = await service.validate_id_token(
        _skew_metadata(),
        "deer-flow",
        _id_token(private_key, iat_offset=5),
        leeway=60,
    )

    assert claims["sub"] == "subject"
    await service.close()


@pytest.mark.asyncio
async def test_a_token_issued_far_ahead_is_still_refused_and_the_refusal_names_the_skew(monkeypatch):
    """Outside the bound the answer is still no, and the operator is told why.

    "Not yet valid" alone cannot be told apart from a replayed or forged
    token; the number of seconds and its direction is what distinguishes a
    clock that needs NTP from a token that needs refusing.
    """
    private_key, public_key = _signing_keypair()
    service = _skew_service(monkeypatch, public_key)

    with pytest.raises(OIDCValidationError) as refusal:
        await service.validate_id_token(
            _skew_metadata(),
            "deer-flow",
            _id_token(private_key, iat_offset=120),
            leeway=60,
        )

    message = str(refusal.value)
    assert "120" in message, f"the refusal must carry the measured skew in seconds: {message}"
    assert "60" in message, f"the refusal must name the leeway it was measured against: {message}"
    await service.close()


@pytest.mark.asyncio
async def test_a_token_that_expired_seconds_ago_is_accepted_within_the_leeway(monkeypatch):
    """The same clock lag in the other direction: a Gateway running ahead."""
    private_key, public_key = _signing_keypair()
    service = _skew_service(monkeypatch, public_key)

    claims = await service.validate_id_token(
        _skew_metadata(),
        "deer-flow",
        _id_token(private_key, iat_offset=-300, exp_offset=-5),
        leeway=60,
    )

    assert claims["sub"] == "subject"
    await service.close()


@pytest.mark.asyncio
async def test_a_token_that_expired_long_ago_is_refused_with_its_age(monkeypatch):
    private_key, public_key = _signing_keypair()
    service = _skew_service(monkeypatch, public_key)

    with pytest.raises(OIDCValidationError) as refusal:
        await service.validate_id_token(
            _skew_metadata(),
            "deer-flow",
            _id_token(private_key, iat_offset=-300, exp_offset=-120),
            leeway=60,
        )

    message = str(refusal.value)
    assert "expired" in message.lower()
    assert "120" in message, f"the refusal must carry how far past expiry the token is: {message}"
    await service.close()


@pytest.mark.asyncio
async def test_the_leeway_is_a_bound_the_caller_sets_not_a_constant(monkeypatch):
    """A zero leeway must still refuse, or the configured value does nothing.

    Every test above passes against a hardcoded 60 s. This one fails unless
    the number the caller passes is the number PyJWT is given.
    """
    private_key, public_key = _signing_keypair()
    service = _skew_service(monkeypatch, public_key)
    token = _id_token(private_key, iat_offset=5)

    with pytest.raises(OIDCValidationError):
        await service.validate_id_token(_skew_metadata(), "deer-flow", token, leeway=0)

    assert (await service.validate_id_token(_skew_metadata(), "deer-flow", token, leeway=300))["sub"] == "subject"
    await service.close()


@pytest.mark.asyncio
async def test_the_orchestrated_callback_carries_the_leeway_down_to_the_token(monkeypatch):
    """``authenticate_callback`` is the entry the route uses, so the bound has
    to survive the hop from it to the validation underneath."""
    private_key, public_key = _signing_keypair()
    service = _skew_service(monkeypatch, public_key)
    token = _id_token(private_key, iat_offset=5)

    async def exchange_code(**kwargs):
        return {"id_token": token, "access_token": "at"}

    monkeypatch.setattr(service, "exchange_code", exchange_code)
    call = dict(provider_id="sso", metadata=_skew_metadata(), client_id="deer-flow", client_secret=None, code="c", redirect_uri="https://tenant.example.com/cb")

    with pytest.raises(OIDCValidationError):
        await service.authenticate_callback(**call, leeway=0)

    identity = await service.authenticate_callback(**call, leeway=60)
    assert identity.subject == "subject"
    await service.close()


@pytest.mark.asyncio
async def test_the_callback_route_hands_the_service_the_configured_leeway(monkeypatch):
    """The knob has to be wired, not merely present.

    Every other test here passes with a leeway hardcoded anywhere below the
    route. This one drives the route itself and reads back what it passed, so
    a configured value that stopped at the config model -- looking adjustable
    while every deployment stayed on the default -- fails here.
    """
    from starlette.requests import Request

    from app.gateway.auth import oidc_state
    from app.gateway.routers import auth as auth_router

    configured_leeway = 137.0
    provider_id = "sso"

    class _Config:
        class auth:  # noqa: N801
            class oidc:  # noqa: N801
                enabled = True
                frontend_base_url = "https://tenant.example.com"
                clock_skew_leeway_seconds = configured_leeway
                providers = {provider_id: _provider_config()}

    monkeypatch.setattr("deerflow.config.app_config.get_app_config", lambda: _Config)

    passed: dict = {}

    class _Service:
        async def discover(self, issuer, overrides=None):
            return _skew_metadata()

        async def authenticate_callback(self, **kwargs):
            passed.update(kwargs)
            raise OIDCError("stop here: the recorded arguments are the subject of this test")

    monkeypatch.setattr(auth_router, "_get_oidc_service", lambda: _Service())

    state_value = "state-value"
    payload = oidc_state.OIDCStatePayload(provider=provider_id, state=state_value, nonce=None, code_verifier=None)
    monkeypatch.setattr(auth_router, "get_state_cookie", lambda request, provider: payload)

    request = Request({"type": "http", "method": "GET", "path": f"/callback/{provider_id}", "headers": [], "query_string": b"", "scheme": "https", "server": ("tenant.example.com", 443)})

    await auth_router.oauth_callback(request=request, provider=provider_id, code="auth-code", state=state_value)

    assert passed.get("leeway") == configured_leeway, f"the route passed {passed.get('leeway')!r}, not the configured {configured_leeway!r}"


def test_the_leeway_is_bounded_at_the_config_the_gateway_starts_from():
    """A tolerance wide enough to accept a stale token is not a clock tolerance.

    Asserted through the real ``AppConfig -> AuthAppConfig -> OIDCAuthConfig``
    chain rather than the model alone, because the requirement is that a bad
    value stops a Gateway from starting, not merely that some model would
    have refused it.
    """
    import pydantic
    import pytest as _pytest

    from deerflow.config.app_config import AppConfig

    auth_config = AppConfig.model_fields["auth"].annotation
    oidc_config = auth_config.model_fields["oidc"].annotation

    assert oidc_config().clock_skew_leeway_seconds == 60.0, "the default is a tolerance, not none"

    for refused in (301, -1, float("inf"), float("nan")):
        with _pytest.raises(pydantic.ValidationError):
            auth_config(oidc={"enabled": True, "providers": {}, "clock_skew_leeway_seconds": refused})

    # 0 is a real setting: it restores the behaviour that had no tolerance.
    assert auth_config(oidc={"enabled": True, "providers": {}, "clock_skew_leeway_seconds": 0}).oidc.clock_skew_leeway_seconds == 0
    assert auth_config(oidc={"enabled": True, "providers": {}, "clock_skew_leeway_seconds": 300}).oidc.clock_skew_leeway_seconds == 300
