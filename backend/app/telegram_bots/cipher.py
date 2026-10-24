"""Bot-specific wrappers for the deployment's configured credential cipher."""

from collections.abc import Mapping

from app.gateway.provider_keys.cipher import ProviderKeyCipher, UnwrappedProviderKey

TOKEN_LABEL = b"hartmesh-telegram-bot-tokens/v1"


def token_cipher(environ: Mapping[str, str]) -> ProviderKeyCipher | None:
    """Return the token cipher, or None without a configured current secret."""
    return ProviderKeyCipher.from_environ(environ, label=TOKEN_LABEL)


def wrap_token(cipher: ProviderKeyCipher, bot_id: int, token: str) -> str:
    """Wrap one token bound to its bot identity."""
    return cipher.wrap(variable=f"telegram-bot:{bot_id}", key=token)


def unwrap_token(cipher: ProviderKeyCipher, bot_id: int, stored: str) -> UnwrappedProviderKey:
    """Open the stored token only for the same bot and credential label."""
    return cipher.unwrap(f"telegram-bot:{bot_id}", stored)
