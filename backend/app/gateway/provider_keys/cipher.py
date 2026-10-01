"""The wrapping of provider keys at rest.

A stored key is Fernet-encrypted (AES-128-CBC with HMAC-SHA256) under a key
derived (SHA-256) from ``HARTMESH_PROVIDER_KEYS_SECRET``, which the deployer
supplies through the environment: it is never written to the database it
protects, so a backup of that database is not a copy of the company's keys.
The plaintext wrapped is the key *together with its variable*, so a row
copied onto another provider does not become that provider's key.

Rotation: set the new secret in ``HARTMESH_PROVIDER_KEYS_SECRET`` and the
old one in ``HARTMESH_PROVIDER_KEYS_SECRET_PREVIOUS``, and restart; the
Gateway reads under either and rewraps every stored key under the new one
at start, after which the previous secret can go. Under a secret that wraps
nothing stored -- a restore into a deployment with a different one -- a
stored key is *unreadable*, never guessed at, and its provider has no key
until an administrator sets one again.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from dataclasses import dataclass, field

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

WRAPPING_KEY_ENV = "HARTMESH_PROVIDER_KEYS_SECRET"
PREVIOUS_WRAPPING_KEY_ENV = "HARTMESH_PROVIDER_KEYS_SECRET_PREVIOUS"
# A secret short enough to type is short enough to guess; `openssl rand -base64 32` gives 44.
WRAPPING_KEY_MIN_LENGTH = 32
_PREFIX = "fernet:v1:"


class WrappingKeyInvalid(ValueError):
    """The configured wrapping key cannot be used; the message names the variable, never the value."""


class UnreadableProviderKey(ValueError):
    """A stored key this deployment's wrapping key does not open."""


@dataclass(frozen=True)
class UnwrappedProviderKey:
    key: str = field(repr=False)
    # Opened with the previous wrapping key: a rotation should rewrap it.
    under_previous: bool = False


# The secret is the deployer's; the label makes the key derived from it this
# use's alone, should the same secret ever serve something else too.
_KEY_LABEL = b"hartmesh-provider-keys/v1"


def _fernet(secret: str) -> Fernet:
    derived = HKDF(algorithm=hashes.SHA256(), length=32, salt=None, info=_KEY_LABEL).derive(secret.encode("utf-8"))
    return Fernet(base64.urlsafe_b64encode(derived))


def _secret(environ: Mapping[str, str], name: str) -> str | None:
    raw = environ.get(name, "").strip()
    if not raw:
        return None
    if len(raw) < WRAPPING_KEY_MIN_LENGTH:
        raise WrappingKeyInvalid(f"{name} must be at least {WRAPPING_KEY_MIN_LENGTH} characters (for example the output of `openssl rand -base64 32`)")
    return raw


class ProviderKeyCipher:
    def __init__(self, current: str, previous: str | None = None) -> None:
        self._current = _fernet(current)
        self._previous = _fernet(previous) if previous else None

    def __repr__(self) -> str:
        return f"ProviderKeyCipher(previous={'set' if self._previous else 'unset'})"

    @classmethod
    def from_environ(cls, environ: Mapping[str, str]) -> ProviderKeyCipher | None:
        """The cipher the environment configures, or None when it configures none."""
        current = _secret(environ, WRAPPING_KEY_ENV)
        previous = _secret(environ, PREVIOUS_WRAPPING_KEY_ENV)
        if current is None:
            return None
        return cls(current, previous)

    def wrap(self, variable: str, key: str) -> str:
        plaintext = json.dumps({"variable": variable, "key": key}, separators=(",", ":")).encode("utf-8")
        return _PREFIX + self._current.encrypt(plaintext).decode("ascii")

    def unwrap(self, variable: str, stored: str) -> UnwrappedProviderKey:
        if not stored.startswith(_PREFIX):
            raise UnreadableProviderKey("the stored value is not a wrapped key")
        token = stored[len(_PREFIX) :].encode("ascii", errors="replace")
        under_previous = False
        try:
            plaintext = self._current.decrypt(token)
        except InvalidToken:
            if self._previous is None:
                raise UnreadableProviderKey("the wrapping key does not open it") from None
            try:
                plaintext = self._previous.decrypt(token)
            except InvalidToken:
                raise UnreadableProviderKey("neither wrapping key opens it") from None
            under_previous = True
        try:
            document = json.loads(plaintext)
        except ValueError:
            raise UnreadableProviderKey("the wrapped value is not a provider key") from None
        if not isinstance(document, dict) or document.get("variable") != variable or not isinstance(document.get("key"), str):
            raise UnreadableProviderKey("the wrapped key belongs to another provider")
        return UnwrappedProviderKey(key=document["key"], under_previous=under_previous)
