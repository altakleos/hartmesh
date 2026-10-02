"""Ask a model provider whether it accepts a key, without storing or applying the key.

The probe builds the chat client exactly as a run would -- the catalog's own
entry for the provider, through the model factory, so a provider that needs a
patched adapter or its own endpoint gets it -- with the candidate key in place
of the catalog's reference to the provider's variable. It sends one short
message and stops at the first piece of the answer: a provider that starts
answering has accepted the key.

Nothing here reads or writes the process environment or the loaded
configuration, and nothing it returns or logs carries the key or the
provider's own error text, which can quote the key it was sent.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

logger = logging.getLogger(__name__)

PROBE_TIMEOUT_SECONDS = 20.0

# What a provider that answers "this key is not valid" with a status other than
# 401 or 403 says instead. Matched against the error text, never returned.
_REJECTED_MARKERS = ("API_KEY_INVALID", "API key not valid", "invalid_api_key", "Incorrect API key")


@dataclass(frozen=True)
class ProbeOutcome:
    #: ``accepted``: the provider started answering. ``rejected``: it refused
    #: the key. ``inconclusive``: it could not be asked, or refused for a
    #: reason that is not the key.
    result: Literal["accepted", "rejected", "inconclusive"]
    #: Why an inconclusive probe was inconclusive, in fixed words: ``timeout``,
    #: ``http_<status>``, ``no_answer``, or ``key_not_used`` when no client
    #: could be built that asks with this key. Never the provider's text.
    reason: str | None = None


def with_key(value: Any, variable: str, key: str) -> Any:
    """*value* with every reference to ``$variable`` replaced by *key*."""
    if isinstance(value, str):
        return key if value == f"${variable}" else value
    if isinstance(value, Mapping):
        return {name: with_key(item, variable, key) for name, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [with_key(item, variable, key) for item in value]
    return value


class _ConfigWithOneModel:
    """The loaded configuration with one model in place of its own.

    Every other setting the model factory reads stays what the deployment runs
    with, and the loaded configuration itself is never touched.
    """

    def __init__(self, base: Any, model: Any) -> None:
        self._base = base
        self.models = [model]

    def get_model_config(self, name: str) -> Any:
        return self.models[0] if name == self.models[0].name else None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._base, name)


class _ClientIgnoresTheKey(Exception):
    """The client the catalog entry builds does not carry the candidate key."""


def carries(client: Any, key: str) -> bool:
    """Whether *client* holds *key* in one of its own fields."""
    for value in vars(client).values():
        if value == key or (hasattr(value, "get_secret_value") and value.get_secret_value() == key):
            return True
    return False


def _build_client(entry: Mapping[str, Any], key: str):
    """The client a run would build from *entry*, proven to carry *key*.

    A catalog entry may name its credential in a field the client class does
    not read; such a provider works in a run only because its SDK takes the
    key from the environment. Built that way here, the client would be asking
    with the environment's key, or with none, and the answer would be about
    the wrong key. So the client is checked for the candidate, built again
    with the key under ``api_key`` if it lacks it, and refused if it still
    does.
    """
    from deerflow.config.app_config import get_app_config
    from deerflow.config.model_config import ModelConfig
    from deerflow.models.factory import create_chat_model

    model = ModelConfig(**entry)
    config = _ConfigWithOneModel(get_app_config(), model)

    def build(**overrides: Any):
        return create_chat_model(model.name, app_config=config, attach_tracing=False, model_overrides={"max_retries": 0, **overrides})

    try:
        client = build()
    except Exception:  # noqa: BLE001 - a class that refuses to be built without a key it can read gets the second attempt below
        client = None
    if client is None or not carries(client, key):
        client = build(api_key=key)
        if not carries(client, key):
            raise _ClientIgnoresTheKey(type(client).__name__)
    return client


def _status_of(error: BaseException) -> int | None:
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        for name in ("status_code", "code", "http_status"):
            value = getattr(current, name, None)
            if isinstance(value, int) and not isinstance(value, bool) and 100 <= value <= 599:
                return value
        value = getattr(getattr(current, "response", None), "status_code", None)
        if isinstance(value, int) and not isinstance(value, bool) and 100 <= value <= 599:
            return value
        current = current.__cause__ or current.__context__
    return None


def classify(error: BaseException) -> ProbeOutcome:
    """What a failed probe says about the key."""
    if isinstance(error, TimeoutError):
        return ProbeOutcome("inconclusive", "timeout")
    if isinstance(error, _ClientIgnoresTheKey):
        return ProbeOutcome("inconclusive", "key_not_used")
    status = _status_of(error)
    if status in (401, 403):
        return ProbeOutcome("rejected")
    text = str(error)
    if any(marker in text for marker in _REJECTED_MARKERS):
        return ProbeOutcome("rejected")
    if status is not None:
        return ProbeOutcome("inconclusive", f"http_{status}")
    return ProbeOutcome("inconclusive", "no_answer")


async def probe_model(entry: Mapping[str, Any], key: str) -> ProbeOutcome:
    """Send one short message with the client *entry* describes; *entry* already carries *key*."""
    from langchain_core.messages import HumanMessage

    try:
        async with asyncio.timeout(PROBE_TIMEOUT_SECONDS):
            client = await asyncio.to_thread(_build_client, entry, key)
            async for _ in client.astream([HumanMessage(content="ping")], config={"callbacks": []}):
                break
    except Exception as error:  # noqa: BLE001 - every failure is an answer about the key, and its text is never passed on
        outcome = classify(error)
        logger.info("provider keys: a connection test ended %s (%s, %s)", outcome.result, outcome.reason or "the provider refused the key", type(error).__name__)
        return outcome
    return ProbeOutcome("accepted")
