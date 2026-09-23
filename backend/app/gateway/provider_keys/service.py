"""Provider keys an administrator sets in the product, applied over the deployment's environment.

How a key reaches the model. Every place that uses a provider key reads it
from the Gateway's process environment: ``config.yaml`` carries
``api_key: $OPENAI_API_KEY`` and the config loader resolves the reference,
and the search tools read their variable at call time. So the product's
keys are applied *to that environment* -- for each catalog variable, the
product's key when one is stored, the value the process started with when
none is -- and the profile's own renderer renders ``config.yaml`` again
from it, which is what includes the fragment of a provider that had no key
and drops one whose key is gone. The render goes to a second file beside
the one ``gateway/run.sh`` wrote (``config.effective.yaml``), the Gateway's
``DEER_FLOW_CONFIG_PATH`` is pointed at it and the config is reloaded. The
first file stays what the environment alone renders, so every command the
deployer runs inside the container, with that environment, still loads it.

When. At start, before anything is built from the config, and after every
write -- no restart. A run already going keeps the configuration it
started with -- its models and their keys -- to its end;
the next run uses the new key, and so does a search tool's next call, since
those read their variable when they are called.

Precedence. A stored key outranks the environment's for its provider at
every start, so it survives a restart, a redeploy whose ``.env`` still
carries the seed, and a restore of a database taken after it was set. A
restore taken *before* it was set has no stored key, and the environment's
applies again. A stored key the wrapping key does not open leaves its
provider with **no** key: never the environment's, which is the key the
company believes it replaced.

The operator model file (``HARTMESH_MODELS_FILE``) makes the deployer the
curator of the tenant's models: writes are refused, and nothing stored is
applied while it is set.
"""

from __future__ import annotations

import asyncio
import logging
import os
import tempfile
from collections.abc import Callable, Mapping, MutableMapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.gateway.provider_keys.cipher import (
    PREVIOUS_WRAPPING_KEY_ENV,
    WRAPPING_KEY_ENV,
    ProviderKeyCipher,
    UnreadableProviderKey,
    WrappingKeyInvalid,
)
from app.gateway.provider_keys.profile import CatalogProvider, ProfileRenderer, RenderRefused

logger = logging.getLogger(__name__)

CONFIG_PATH_ENV = "DEER_FLOW_CONFIG_PATH"
EFFECTIVE_CONFIG_NAME = "config.effective.yaml"
KEY_MAX_LENGTH = 4096


class ProviderKeysRefused(Exception):
    """A write refused; ``code`` is stable, ``message`` names the cause and never a key."""

    def __init__(self, code: str, message: str, status: int = 409) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


@dataclass(frozen=True)
class _Reading:
    """What one stored row means under this deployment's wrapping key."""

    key: str | None
    unreadable: bool = False
    under_previous: bool = False

    def __repr__(self) -> str:
        return f"_Reading(unreadable={self.unreadable}, under_previous={self.under_previous})"


def _default_reload() -> None:
    from deerflow.config.app_config import reload_app_config

    reload_app_config()


def _check_key(key: str) -> str:
    candidate = key.strip()
    if not candidate or len(candidate) > KEY_MAX_LENGTH or not candidate.isprintable() or any(character.isspace() for character in candidate):
        raise ProviderKeysRefused("key_invalid", f"a provider key is one token of at most {KEY_MAX_LENGTH} printable characters, with no spaces or line breaks", status=422)
    return candidate


class ProviderKeyService:
    def __init__(
        self,
        *,
        renderer: ProfileRenderer,
        repository: Any,
        environ: MutableMapping[str, str],
        reload: Callable[[], None] = _default_reload,
    ) -> None:
        self._renderer = renderer
        self._repository = repository
        self._environ = environ
        self._reload = reload
        self._providers = {provider.id: provider for provider in renderer.providers}
        # What the process started with, for every catalog variable: what a
        # provider falls back to when no key is stored for it.
        self._seed = {provider.variable: environ.get(provider.variable, "") for provider in renderer.providers}
        self._base_path = Path(environ[CONFIG_PATH_ENV])
        self._effective_path = self._base_path.with_name(EFFECTIVE_CONFIG_NAME)
        # Whether the config path names the effective file this service wrote.
        self._installed = False
        self._cipher: ProviderKeyCipher | None = None
        self._cipher_problem: str | None = None
        try:
            self._cipher = ProviderKeyCipher.from_environ(environ)
        except WrappingKeyInvalid as exc:
            self._cipher_problem = str(exc)
        self._lock = asyncio.Lock()

    @classmethod
    def from_environ(cls, repository: Any, *, environ: MutableMapping[str, str] | None = None) -> ProviderKeyService | None:
        """The service for this deployment, or None where keys are not managed in the product."""
        environ = os.environ if environ is None else environ
        renderer = ProfileRenderer.from_environ(environ)
        if renderer is None or not environ.get(CONFIG_PATH_ENV, "").strip():
            return None
        return cls(renderer=renderer, repository=repository, environ=environ)

    # ── reading ────────────────────────────────────────────────────────────

    def refusal(self) -> dict[str, str] | None:
        """Why a write would be refused now, or None when writes are accepted."""
        models_file = self._renderer.operator_models_file(self._environ)
        if models_file is not None:
            return {
                "code": "operator_model_file",
                "message": f"{self._renderer.operator_models_variable} is set: the deployer curates this deployment's models, so provider keys are managed in its environment, not here",
            }
        if self._cipher_problem is not None:
            return {"code": "wrapping_key_invalid", "message": self._cipher_problem}
        if self._cipher is None:
            return {
                "code": "no_wrapping_key",
                "message": f"{WRAPPING_KEY_ENV} is not set: a key set here is stored encrypted under it, and without it none can be stored. The deployer sets it in the deployment's environment",
            }
        return None

    def _read(self, variable: str, ciphertext: str) -> _Reading:
        if self._cipher is None:
            return _Reading(key=None, unreadable=True)
        try:
            unwrapped = self._cipher.unwrap(variable, ciphertext)
        except UnreadableProviderKey:
            return _Reading(key=None, unreadable=True)
        return _Reading(key=unwrapped.key, under_previous=unwrapped.under_previous)

    def _describe(self, provider: CatalogProvider, stored: Mapping[str, Any], applying: bool) -> dict[str, Any]:
        row = stored.get(provider.variable)
        reading = self._read(provider.variable, row.ciphertext) if row is not None else None
        seeded = bool(self._seed.get(provider.variable, "").strip())
        if applying and reading is not None:
            source = "none" if reading.unreadable else "product"
        else:
            source = "environment" if seeded else "none"
        return {
            "provider": provider.id,
            "variable": provider.variable,
            "kind": provider.kind,
            "source": source,
            "product_key": "absent" if reading is None else ("unreadable" if reading.unreadable else "set"),
            "wrapped_with": None if reading is None or reading.unreadable else ("previous" if reading.under_previous else "current"),
            "changed_at": row.changed_at.isoformat() if row is not None else None,
            "changed_by": row.changed_by if row is not None else None,
        }

    def _applying(self) -> bool:
        return self._renderer.operator_models_file(self._environ) is None

    async def status(self) -> dict[str, Any]:
        """Every catalog provider's key source; never a key."""
        stored = await self._repository.stored()
        applying = self._applying()
        return {
            "available": True,
            "refusal": self.refusal(),
            "wrapping_key": "invalid" if self._cipher_problem else ("set" if self._cipher else "absent"),
            "providers": [self._describe(provider, stored, applying) for provider in self._renderer.providers],
        }

    async def events(self, *, limit: int = 50) -> list[dict]:
        return await self._repository.events(limit=limit)

    # ── applying ───────────────────────────────────────────────────────────

    def _effective(self, readings: Mapping[str, _Reading]) -> dict[str, str | None]:
        """Each catalog variable's value: the stored key, nothing for an unreadable one, else the seed."""
        values: dict[str, str | None] = {}
        for provider in self._renderer.providers:
            reading = readings.get(provider.variable)
            if reading is None:
                values[provider.variable] = self._seed.get(provider.variable) or None
            else:
                values[provider.variable] = reading.key
        return values

    def _render(self, values: Mapping[str, str | None]) -> str:
        environ = dict(self._environ)
        for variable, value in values.items():
            if value:
                environ[variable] = value
            else:
                environ.pop(variable, None)
        return self._renderer.render(environ)

    def _install(self, values: Mapping[str, str | None], rendered: str) -> None:
        """Put the values in the environment, write the render, point the config at it and reload.

        In the order a config read in between still resolves: a variable the
        new file names is set before the file is in place, and one only the
        old file named is dropped after the reload. A failure before the
        file is in place leaves the environment as it was.
        """
        self._effective_path.parent.mkdir(parents=True, exist_ok=True)
        handle = tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=self._effective_path.parent, prefix=".config.effective.", delete=False)
        try:
            with handle:
                handle.write(rendered)
            os.chmod(handle.name, 0o640)
            before = {variable: self._environ.get(variable) for variable in values}
            try:
                for variable, value in values.items():
                    if value:
                        self._environ[variable] = value
                os.replace(handle.name, self._effective_path)
            except BaseException:
                self._set_environ(before)
                raise
        finally:
            Path(handle.name).unlink(missing_ok=True)
        self._installed = True
        self._environ[CONFIG_PATH_ENV] = str(self._effective_path)
        self._reload()
        for variable, value in values.items():
            if not value:
                self._environ.pop(variable, None)

    def _set_environ(self, values: Mapping[str, str | None]) -> None:
        for variable, value in values.items():
            if value:
                self._environ[variable] = value
            else:
                self._environ.pop(variable, None)

    def _install_base(self) -> None:
        """Back on the file ``gateway/run.sh`` rendered and the environment the process started with."""
        self._set_environ({variable: seed or None for variable, seed in self._seed.items()})
        self._environ[CONFIG_PATH_ENV] = str(self._base_path)
        self._reload()
        self._discard_effective()

    async def start(self) -> bool:
        """Apply what is stored, before anything is built from the config; True when anything was applied."""
        async with self._lock:
            stored = await self._repository.stored()
            if not self._applying():
                if stored:
                    logger.warning(
                        "provider keys: %s is set, so the keys stored in the product for %s are not applied",
                        self._renderer.operator_models_variable,
                        ", ".join(sorted(stored)),
                    )
                self._discard_effective()
                return False
            known = {provider.variable for provider in self._renderer.providers}
            readings = {variable: self._read(variable, row.ciphertext) for variable, row in stored.items() if variable in known}
            if not readings:
                self._discard_effective()
                return False
            await self._rewrap(stored, readings)
            values = self._effective(readings)
            try:
                rendered = await asyncio.to_thread(self._render, values)
            except RenderRefused as exc:
                # Never the base file in its place: that is the environment's keys.
                logger.error("provider keys: the configuration does not render with the stored keys, so the Gateway does not start: %s", exc)
                raise
            await asyncio.to_thread(self._install, values, rendered)
            unreadable = sorted(variable for variable, reading in readings.items() if reading.unreadable)
            logger.info("provider keys: set in the product for %s", ", ".join(sorted(variable for variable, reading in readings.items() if not reading.unreadable)) or "none")
            if unreadable:
                if self._cipher is not None:
                    why = f"{WRAPPING_KEY_ENV} does not open them"
                else:
                    why = self._cipher_problem or f"{WRAPPING_KEY_ENV} is not set"
                logger.warning(
                    "provider keys: the keys stored for %s cannot be read (%s), so those providers have no key until the right %s is supplied or a key is set again in the product",
                    ", ".join(unreadable),
                    why,
                    WRAPPING_KEY_ENV,
                )
            return True

    def _discard_effective(self) -> None:
        # A file a previous start left is never read (the config path points
        # at the base file), but a second config.yaml on the disk is a question.
        self._effective_path.unlink(missing_ok=True)
        self._installed = False

    async def _rewrap(self, stored: Mapping[str, Any], readings: Mapping[str, _Reading]) -> None:
        if self._cipher is None:
            return
        for variable, reading in readings.items():
            if reading.under_previous and reading.key is not None:
                if await self._repository.rewrap(variable, expected=stored[variable].ciphertext, ciphertext=self._cipher.wrap(variable, reading.key)):
                    logger.info("provider keys: rewrapped %s under %s; %s can be removed once every stored key is rewrapped", variable, WRAPPING_KEY_ENV, PREVIOUS_WRAPPING_KEY_ENV)

    # ── writing ────────────────────────────────────────────────────────────

    def _provider(self, provider_id: str) -> CatalogProvider:
        provider = self._providers.get(provider_id)
        if provider is None:
            raise ProviderKeysRefused("unknown_provider", "no provider of that name is in this release's catalog", status=404)
        return provider

    def _refuse_unless_managed(self, *, needs_cipher: bool) -> None:
        refusal = self.refusal()
        if refusal is None:
            return
        if not needs_cipher and refusal["code"] in {"no_wrapping_key", "wrapping_key_invalid"}:
            return
        raise ProviderKeysRefused(refusal["code"], refusal["message"])

    async def _apply_change(self, variable: str, reading: _Reading | None, commit: Callable[[], Any]) -> Any:
        """Render with the change (a refusal changes nothing), install it, then commit it.

        Installed before it is committed, so a change the Gateway could not
        apply is never recorded as made -- a key reported removed while it is
        still in use is the failure this ordering rules out. A commit that
        fails puts the Gateway back on what is stored.
        """
        stored = await self._repository.stored()
        readings = {name: self._read(name, row.ciphertext) for name, row in stored.items() if name in self._seed}
        previous = self._effective(readings)
        was_installed = self._installed
        readings = dict(readings)
        if reading is None:
            readings.pop(variable, None)
        else:
            readings[variable] = reading
        values = self._effective(readings)
        try:
            rendered = await asyncio.to_thread(self._render, values)
        except RenderRefused as exc:
            raise ProviderKeysRefused("render_refused", f"the deployment's configuration would not render with this change: {exc}", status=422) from None
        try:
            await asyncio.to_thread(self._install, values, rendered)
            return await commit()
        except BaseException:
            await self._restore(previous, was_installed)
            raise

    async def _restore(self, previous: Mapping[str, str | None], was_installed: bool) -> None:
        try:
            if was_installed:
                rendered = await asyncio.to_thread(self._render, previous)
                await asyncio.to_thread(self._install, previous, rendered)
            else:
                await asyncio.to_thread(self._install_base)
        except Exception:
            logger.exception("provider keys: a change was not made, and the Gateway could not be put back on the stored keys; restart the Gateway to apply them")

    async def put(self, provider_id: str, key: str, *, actor_id: str, actor_email: str | None) -> dict[str, Any]:
        self._refuse_unless_managed(needs_cipher=True)
        provider = self._provider(provider_id)
        key = _check_key(key)
        assert self._cipher is not None
        cipher = self._cipher
        async with self._lock:
            action = await self._apply_change(
                provider.variable,
                _Reading(key=key),
                lambda: self._repository.put(provider.variable, cipher.wrap(provider.variable, key), actor_id=actor_id, actor_email=actor_email),
            )
            logger.info("provider keys: %s the key for %s (%s) by %s", action, provider.id, provider.variable, actor_email or actor_id)
            return {"action": action, "provider": self._describe(provider, await self._repository.stored(), True)}

    async def remove(self, provider_id: str, *, actor_id: str, actor_email: str | None) -> dict[str, Any]:
        # Removing needs no wrapping key: it is how an administrator puts an
        # unreadable key's provider back on the environment's key.
        self._refuse_unless_managed(needs_cipher=False)
        provider = self._provider(provider_id)
        async with self._lock:
            if provider.variable not in await self._repository.stored():
                return {"action": "none", "provider": self._describe(provider, {}, True)}
            removed = await self._apply_change(
                provider.variable,
                None,
                lambda: self._repository.remove(provider.variable, actor_id=actor_id, actor_email=actor_email),
            )
            action = "removed" if removed else "none"
            logger.info("provider keys: %s the key for %s (%s) by %s", action, provider.id, provider.variable, actor_email or actor_id)
            return {"action": action, "provider": self._describe(provider, await self._repository.stored(), True)}
