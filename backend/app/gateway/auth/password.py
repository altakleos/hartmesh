"""Password hashing utilities with versioned hash format.

Hash format: ``$dfv<N>$<bcrypt_hash>`` where ``<N>`` is the version.

- **v1** (legacy): ``bcrypt(password)`` — plain bcrypt, susceptible to
  72-byte silent truncation.
- **v2** (current): ``bcrypt(b64(sha256(password)))`` — SHA-256 pre-hash
  avoids the 72-byte truncation limit so the full password contributes
  to the hash.

Verification auto-detects the version and falls back to v1 for hashes
without a prefix, so existing deployments upgrade transparently on next
login.
"""

import asyncio
import base64
import hashlib
import secrets

import bcrypt

_CURRENT_VERSION = 2
_PREFIX_V2 = "$dfv2$"
_PREFIX_V1 = "$dfv1$"


def _pre_hash_v2(password: str) -> bytes:
    """SHA-256 pre-hash to bypass bcrypt's 72-byte limit."""
    return base64.b64encode(hashlib.sha256(password.encode("utf-8")).digest())


def hash_password(password: str) -> str:
    """Hash a password (current version: v2 — SHA-256 + bcrypt)."""
    raw = bcrypt.hashpw(_pre_hash_v2(password), bcrypt.gensalt()).decode("utf-8")
    return f"{_PREFIX_V2}{raw}"


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password, auto-detecting the hash version.

    Accepts v2 (``$dfv2$…``), v1 (``$dfv1$…``), and bare bcrypt hashes
    (treated as v1 for backward compatibility with pre-versioning data).
    """
    try:
        if hashed_password.startswith(_PREFIX_V2):
            bcrypt_hash = hashed_password[len(_PREFIX_V2) :]
            return bcrypt.checkpw(_pre_hash_v2(plain_password), bcrypt_hash.encode("utf-8"))

        if hashed_password.startswith(_PREFIX_V1):
            bcrypt_hash = hashed_password[len(_PREFIX_V1) :]
        else:
            bcrypt_hash = hashed_password

        return bcrypt.checkpw(plain_password.encode("utf-8"), bcrypt_hash.encode("utf-8"))
    except ValueError:
        # bcrypt raises ValueError for malformed or corrupt hashes (e.g., invalid salt).
        # Fail closed rather than crashing the request.
        return False


def needs_rehash(hashed_password: str) -> bool:
    """Return True if the hash uses an older version and should be rehashed."""
    return not hashed_password.startswith(_PREFIX_V2)


async def hash_password_async(password: str) -> str:
    """Hash a password using bcrypt (non-blocking).

    Wraps the blocking bcrypt operation in a thread pool to avoid
    blocking the event loop during password hashing.
    """
    return await asyncio.to_thread(hash_password, password)


async def verify_password_async(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash (non-blocking).

    Wraps the blocking bcrypt operation in a thread pool to avoid
    blocking the event loop during password verification.
    """
    return await asyncio.to_thread(verify_password, plain_password, hashed_password)


#: A real hash of an unguessable value, computed once per process. Verifying
#: against it costs what verifying a real user's password costs, and can never
#: succeed.
_TIMING_EQUALIZER_HASH: str | None = None


def _timing_equalizer_hash() -> str:
    global _TIMING_EQUALIZER_HASH
    if _TIMING_EQUALIZER_HASH is None:
        _TIMING_EQUALIZER_HASH = hash_password(secrets.token_urlsafe(32))
    return _TIMING_EQUALIZER_HASH


async def equalize_password_timing() -> None:
    """Spend one password verification without having a password to check.

    The login path must answer in the same time whether the account exists,
    does not exist, or is locked out. Returning early on any of those makes
    response time an account-enumeration oracle: bcrypt is deliberately slow,
    so "no user row" is tens of milliseconds faster than "wrong password" and
    the difference is trivially measurable over a WAN.

    This is an equal-cost stand-in, not a constant-time guarantee: the work
    factor is the same, the scheduler is not.
    """
    await verify_password_async(secrets.token_urlsafe(16), _timing_equalizer_hash())
