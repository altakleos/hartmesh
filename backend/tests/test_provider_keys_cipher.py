"""The wrapping of provider keys at rest.

The wrapping key comes from the deployment's environment and never from the
database it protects. Without it nothing is wrapped (and nothing accepted);
under a different one a stored key is unreadable rather than wrong; a
rotation reads under the previous key and writes under the current one.
"""

from __future__ import annotations

import pytest

from app.gateway.provider_keys.cipher import (
    PREVIOUS_WRAPPING_KEY_ENV,
    WRAPPING_KEY_ENV,
    ProviderKeyCipher,
    UnreadableProviderKey,
    WrappingKeyInvalid,
)

SENTINEL = "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY"
CURRENT = "c" * 43
OTHER = "o" * 43


def test_there_is_no_cipher_without_a_wrapping_key() -> None:
    assert ProviderKeyCipher.from_environ({}) is None
    assert ProviderKeyCipher.from_environ({WRAPPING_KEY_ENV: "   "}) is None


def test_a_short_wrapping_key_is_refused_and_not_echoed() -> None:
    with pytest.raises(WrappingKeyInvalid) as refused:
        ProviderKeyCipher.from_environ({WRAPPING_KEY_ENV: "short-secret"})
    assert "short-secret" not in str(refused.value)
    assert WRAPPING_KEY_ENV in str(refused.value)
    with pytest.raises(WrappingKeyInvalid):
        ProviderKeyCipher.from_environ({WRAPPING_KEY_ENV: CURRENT, PREVIOUS_WRAPPING_KEY_ENV: "short"})


def test_a_wrapped_key_is_not_the_key_and_reads_back_only_for_its_own_variable() -> None:
    cipher = ProviderKeyCipher.from_environ({WRAPPING_KEY_ENV: CURRENT})
    wrapped = cipher.wrap("OPENAI_API_KEY", SENTINEL)
    assert SENTINEL not in wrapped
    assert wrapped.startswith("fernet:v1:")
    assert cipher.unwrap("OPENAI_API_KEY", wrapped).key == SENTINEL
    # A row copied onto another provider's variable does not become that provider's key.
    with pytest.raises(UnreadableProviderKey):
        cipher.unwrap("ANTHROPIC_API_KEY", wrapped)


def test_under_a_different_wrapping_key_a_stored_key_is_unreadable() -> None:
    wrapped = ProviderKeyCipher.from_environ({WRAPPING_KEY_ENV: CURRENT}).wrap("OPENAI_API_KEY", SENTINEL)
    other = ProviderKeyCipher.from_environ({WRAPPING_KEY_ENV: OTHER})
    with pytest.raises(UnreadableProviderKey) as unreadable:
        other.unwrap("OPENAI_API_KEY", wrapped)
    assert SENTINEL not in str(unreadable.value)
    with pytest.raises(UnreadableProviderKey):
        other.unwrap("OPENAI_API_KEY", "not-a-wrapped-value")


def test_a_rotation_reads_under_the_previous_key_and_rewraps_under_the_current_one() -> None:
    old = ProviderKeyCipher.from_environ({WRAPPING_KEY_ENV: OTHER})
    wrapped = old.wrap("OPENAI_API_KEY", SENTINEL)

    rotating = ProviderKeyCipher.from_environ({WRAPPING_KEY_ENV: CURRENT, PREVIOUS_WRAPPING_KEY_ENV: OTHER})
    unwrapped = rotating.unwrap("OPENAI_API_KEY", wrapped)
    assert unwrapped.key == SENTINEL
    assert unwrapped.under_previous is True

    rewrapped = rotating.wrap("OPENAI_API_KEY", unwrapped.key)
    assert rotating.unwrap("OPENAI_API_KEY", rewrapped).under_previous is False
    # Once rewrapped, the previous key can go.
    assert ProviderKeyCipher.from_environ({WRAPPING_KEY_ENV: CURRENT}).unwrap("OPENAI_API_KEY", rewrapped).key == SENTINEL


def test_nothing_the_cipher_prints_carries_a_key() -> None:
    cipher = ProviderKeyCipher.from_environ({WRAPPING_KEY_ENV: CURRENT})
    unwrapped = cipher.unwrap("OPENAI_API_KEY", cipher.wrap("OPENAI_API_KEY", SENTINEL))
    assert SENTINEL not in repr(unwrapped)
    assert CURRENT not in repr(cipher)


def test_the_secret_is_turned_into_a_key_for_this_use_alone() -> None:
    """A secret reused for something else, hashed the obvious way, does not open what it wrapped."""
    import base64
    import hashlib

    from cryptography.fernet import Fernet, InvalidToken

    wrapped = ProviderKeyCipher.from_environ({WRAPPING_KEY_ENV: CURRENT}).wrap("OPENAI_API_KEY", SENTINEL)
    obvious = Fernet(base64.urlsafe_b64encode(hashlib.sha256(CURRENT.encode("utf-8")).digest()))
    with pytest.raises(InvalidToken):
        obvious.decrypt(wrapped.removeprefix("fernet:v1:").encode("ascii"))
