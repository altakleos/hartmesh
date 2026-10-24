"""Offline wrapping compatibility and separation of Telegram bot credentials."""

from __future__ import annotations

import base64
import json

import pytest
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from app.gateway.provider_keys.cipher import (
    PREVIOUS_WRAPPING_KEY_ENV,
    WRAPPING_KEY_ENV,
    ProviderKeyCipher,
    UnreadableProviderKey,
    WrappingKeyInvalid,
)
from app.telegram_bots.cipher import TOKEN_LABEL, token_cipher, unwrap_token, wrap_token

CURRENT = "current-wrapping-secret-" + "c" * 32
PREVIOUS = "previous-wrapping-secret-" + "p" * 32
BOT_ID = 123456789


def _token() -> str:
    return str(BOT_ID) + ":" + "synthetic_" * 4


def _legacy_fernet(secret: str) -> Fernet:
    # Independent published provider-key v1 format, before the label parameter.
    derived = HKDF(algorithm=hashes.SHA256(), length=32, salt=None, info=b"hartmesh-provider-keys/v1").derive(secret.encode("utf-8"))
    return Fernet(base64.urlsafe_b64encode(derived))


def test_token_wrapped_under_telegram_label() -> None:
    assert TOKEN_LABEL == b"hartmesh-telegram-bot-tokens/v1"
    cipher = token_cipher({WRAPPING_KEY_ENV: CURRENT})
    assert cipher is not None
    token = _token()
    stored = wrap_token(cipher, BOT_ID, token)
    assert stored.startswith("fernet:v1:")
    assert token not in stored
    assert unwrap_token(cipher, BOT_ID, stored).key == token
    assert unwrap_token(cipher, BOT_ID, stored).under_previous is False
    direct = ProviderKeyCipher.from_environ({WRAPPING_KEY_ENV: CURRENT}, label=TOKEN_LABEL)
    assert direct is not None
    assert direct.unwrap(f"telegram-bot:{BOT_ID}", stored).key == token


def test_provider_key_cipher_cannot_open_bot_token() -> None:
    provider = ProviderKeyCipher(CURRENT)
    telegram = token_cipher({WRAPPING_KEY_ENV: CURRENT})
    assert telegram is not None
    # Use the same variable to establish key-label separation independently
    # of the additional bot/provider identity bound inside the plaintext.
    variable = f"telegram-bot:{BOT_ID}"
    token = _token()
    bot_stored = wrap_token(telegram, BOT_ID, token)
    provider_stored = provider.wrap(variable, token)
    with pytest.raises(UnreadableProviderKey):
        provider.unwrap(variable, bot_stored)
    with pytest.raises(UnreadableProviderKey):
        unwrap_token(telegram, BOT_ID, provider_stored)


def test_row_copied_to_another_bot_is_unreadable() -> None:
    cipher = token_cipher({WRAPPING_KEY_ENV: CURRENT})
    assert cipher is not None
    with pytest.raises(UnreadableProviderKey) as refused:
        unwrap_token(cipher, BOT_ID + 1, wrap_token(cipher, BOT_ID, _token()))
    assert _token() not in str(refused.value)


@pytest.mark.parametrize("factory", [lambda: ProviderKeyCipher(CURRENT), lambda: ProviderKeyCipher.from_environ({WRAPPING_KEY_ENV: CURRENT})], ids=["constructor", "environment"])
def test_default_label_leaves_provider_keys_unchanged(factory) -> None:
    variable, key = "OPENAI_API_KEY", "synthetic-provider-credential"
    plaintext = json.dumps({"variable": variable, "key": key}, separators=(",", ":")).encode("utf-8")
    legacy_stored = "fernet:v1:" + _legacy_fernet(CURRENT).encrypt_at_time(plaintext, current_time=1_700_000_000).decode("ascii")
    cipher = factory()
    assert cipher is not None
    assert cipher.unwrap(variable, legacy_stored).key == key
    wrapped = cipher.wrap(variable, key)
    assert json.loads(_legacy_fernet(CURRENT).decrypt(wrapped.removeprefix("fernet:v1:").encode("ascii"))) == {"variable": variable, "key": key}


def test_bot_rotation_uses_only_the_bot_label_under_both_secrets() -> None:
    old = token_cipher({WRAPPING_KEY_ENV: PREVIOUS})
    rotating = token_cipher({WRAPPING_KEY_ENV: CURRENT, PREVIOUS_WRAPPING_KEY_ENV: PREVIOUS})
    assert old is not None and rotating is not None
    unwrapped = unwrap_token(rotating, BOT_ID, wrap_token(old, BOT_ID, _token()))
    assert unwrapped.key == _token()
    assert unwrapped.under_previous is True
    current = token_cipher({WRAPPING_KEY_ENV: CURRENT})
    assert current is not None
    rewrapped = wrap_token(rotating, BOT_ID, unwrapped.key)
    assert unwrap_token(current, BOT_ID, rewrapped).under_previous is False
    for secret in (CURRENT, PREVIOUS):
        with pytest.raises(UnreadableProviderKey):
            ProviderKeyCipher(secret).unwrap(f"telegram-bot:{BOT_ID}", rewrapped)


@pytest.mark.parametrize("environ", [{}, {WRAPPING_KEY_ENV: ""}, {WRAPPING_KEY_ENV: " " * 32}, {PREVIOUS_WRAPPING_KEY_ENV: PREVIOUS}])
def test_no_bot_cipher_without_a_current_secret(environ) -> None:
    assert token_cipher(environ) is None


@pytest.mark.parametrize("variable", [WRAPPING_KEY_ENV, PREVIOUS_WRAPPING_KEY_ENV])
def test_bot_cipher_rejects_short_secrets_without_echoing_them(variable: str) -> None:
    environ = {WRAPPING_KEY_ENV: CURRENT, variable: "short-synthetic-secret"}
    with pytest.raises(WrappingKeyInvalid) as refused:
        token_cipher(environ)
    assert variable in str(refused.value)
    assert environ[variable] not in str(refused.value)


def test_secret_normalization_and_minimum_length_match_provider_keys() -> None:
    secret = "s" * 32
    cipher = token_cipher({WRAPPING_KEY_ENV: f"  {secret}  "})
    assert cipher is not None
    direct = ProviderKeyCipher(secret, label=TOKEN_LABEL)
    assert unwrap_token(direct, BOT_ID, wrap_token(cipher, BOT_ID, _token())).key == _token()


def test_bot_cipher_preserves_previous_secret_validation_when_current_is_absent() -> None:
    with pytest.raises(WrappingKeyInvalid):
        token_cipher({PREVIOUS_WRAPPING_KEY_ENV: "short"})


def test_bot_cipher_repr_and_errors_do_not_contain_tokens_or_wrapping_secrets() -> None:
    cipher = token_cipher({WRAPPING_KEY_ENV: CURRENT, PREVIOUS_WRAPPING_KEY_ENV: PREVIOUS})
    assert cipher is not None
    value = unwrap_token(cipher, BOT_ID, wrap_token(cipher, BOT_ID, _token()))
    with pytest.raises(UnreadableProviderKey) as refused:
        unwrap_token(cipher, BOT_ID, "not-a-wrapped-value")
    printed = repr(cipher) + repr(value) + str(refused.value)
    assert all(secret not in printed for secret in (CURRENT, PREVIOUS, _token()))
