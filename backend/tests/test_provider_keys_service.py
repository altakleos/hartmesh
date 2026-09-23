"""Provider keys an administrator sets in the product, applied over the deployment's environment.

What these tests pin, each against the compose profile's own renderer and
catalog and the Gateway's own config loader:

* precedence -- the product's key outranks the environment's for its
  provider, at start (so across a restart, a redeploy that rewrites the
  ``.env`` with the seed, and a restore taken after the key was set), and a
  restore taken before it leaves the environment's key in use;
* no restart -- a write re-renders and reloads, so a provider that had no
  key gets its models and a replacement is what the next model is built with;
* removal -- back to the environment's key, or to none;
* at rest -- no wrapping key refuses a write, a different one leaves a
  stored key unreadable and its provider with no key, never the seed;
* the operator model file -- writes refused, nothing stored applied;
* nothing else changes -- no stored key, no re-render, no second file.
"""

from __future__ import annotations

import importlib.util
import os
import shutil
import sys
from pathlib import Path

import pytest
import pytest_asyncio
import yaml

# Loading a rendered profile with the real config loader writes process-wide singletons.
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture

from app.gateway.provider_keys.cipher import WRAPPING_KEY_ENV
from app.gateway.provider_keys.profile import PROFILE_DIR_ENV
from app.gateway.provider_keys.service import ProviderKeyService, ProviderKeysRefused

REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE = REPO_ROOT / "deploy" / "compose"
SEED = "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY-SEED-A"
OWN = "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY-OWN-B"
NEWER = "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY-NEWER-C"
WRAPPING = "w" * 44
ADMIN = {"actor_id": "admin-1", "actor_email": "admin@example.com"}


def _catalog_variables() -> list[str]:
    return sorted({yaml.safe_load(path.read_text(encoding="utf-8"))["env"] for path in (PROFILE / "providers").rglob("*.yaml")})


def _load_renderer():
    spec = importlib.util.spec_from_file_location("hartmesh_render_config_provider_keys_test", PROFILE / "gateway" / "render_config.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def environ(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """The Gateway's process environment as the profile starts it, with the base config rendered as run.sh renders it."""
    for name in _catalog_variables():
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("HARTMESH_MODELS_FILE", raising=False)
    # Set by the rotation test; registered here so it never outlives one.
    monkeypatch.delenv("HARTMESH_PROVIDER_KEYS_SECRET_PREVIOUS", raising=False)
    values = {
        "DATABASE_URL": "postgresql://deerflow:x@postgres:5432/deerflow",
        "DEER_FLOW_STREAM_BRIDGE_REDIS_URL": "redis://:x@redis:6379/0",
        "HARTMESH_LOCAL_PASSWORDS": "allowed",
        "HARTMESH_LOCAL_REGISTRATION": "closed",
        PROFILE_DIR_ENV: str(PROFILE),
        WRAPPING_KEY_ENV: WRAPPING,
        "DEER_FLOW_CONFIG_PATH": str(tmp_path / "home" / "config.yaml"),
        "OPENAI_API_KEY": SEED,
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    _render_base()
    return os.environ


def _render_base() -> None:
    renderer = _load_renderer()
    rendered, _ = renderer.render_text((PROFILE / "config.yaml").read_text(encoding="utf-8"), renderer.load_catalog(PROFILE / "providers"), os.environ)
    base = Path(os.environ["DEER_FLOW_CONFIG_PATH"])
    base.parent.mkdir(parents=True, exist_ok=True)
    base.write_text(rendered, encoding="utf-8")


@pytest_asyncio.fixture
async def database(tmp_path: Path):
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine
    from deerflow.persistence.provider_keys import ProviderKeyRepository

    path = tmp_path / "db" / "keys.db"
    path.parent.mkdir()
    await init_engine("sqlite", url=f"sqlite+aiosqlite:///{path}", sqlite_dir=str(path.parent))
    try:
        yield path, ProviderKeyRepository(get_session_factory())
    finally:
        await close_engine()


def _backup(database: Path, destination: Path) -> None:
    """A copy of the database as a backup holds it: SQLite keeps recent writes in its write-ahead log."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        source = Path(f"{database}{suffix}")
        if source.exists():
            shutil.copyfile(source, Path(f"{destination}{suffix}"))


def _service(repository, environ) -> ProviderKeyService:
    service = ProviderKeyService.from_environ(repository, environ=environ)
    assert service is not None
    return service


def _live_config():
    from deerflow.config.app_config import get_app_config

    return get_app_config()


def _model_keys(config) -> dict[str, str]:
    return {model.name: model.model_extra.get("api_key") if model.model_extra else getattr(model, "api_key", None) for model in config.models}


def _client_key(name: str) -> str:
    """The key on the chat model client a run would build now, read off the client itself."""
    from deerflow.models.factory import create_chat_model

    client = create_chat_model(name=name, attach_tracing=False)
    secret = getattr(client, "anthropic_api_key", None) or getattr(client, "openai_api_key", None)
    return secret.get_secret_value()


def _source(document: dict, variable: str) -> str:
    return next(provider["source"] for provider in document["providers"] if provider["variable"] == variable)


# ── nothing set in the product: nothing changes ─────────────────────────────


@pytest.mark.asyncio
async def test_with_no_key_stored_a_start_changes_nothing(environ, database) -> None:
    _, repository = database
    base = Path(environ["DEER_FLOW_CONFIG_PATH"])
    before = base.read_bytes()
    listing = sorted(path.name for path in base.parent.iterdir())

    service = _service(repository, environ)
    assert await service.start() is False

    assert base.read_bytes() == before
    assert sorted(path.name for path in base.parent.iterdir()) == listing
    assert environ["DEER_FLOW_CONFIG_PATH"] == str(base)
    assert environ["OPENAI_API_KEY"] == SEED
    document = await service.status()
    assert _source(document, "OPENAI_API_KEY") == "environment"
    assert _source(document, "ANTHROPIC_API_KEY") == "none"


def test_outside_the_profile_there_is_no_service(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(PROFILE_DIR_ENV, raising=False)
    assert ProviderKeyService.from_environ(object(), environ={}) is None


# ── precedence ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_the_products_key_is_used_over_the_environments_and_keeps_winning_at_every_start(environ, database, tmp_path: Path) -> None:
    _, repository = database
    service = _service(repository, environ)
    await service.start()

    result = await service.put("openai", OWN, **ADMIN)
    assert result["action"] == "added"
    assert result["provider"]["source"] == "product"
    assert environ["OPENAI_API_KEY"] == OWN
    assert _model_keys(_live_config())["gpt-4"] == OWN

    # A restart, or a redeploy that rewrote the .env with the seed: a fresh
    # process whose environment carries the seed again, and a base config
    # rendered from that environment alone.
    environ["OPENAI_API_KEY"] = SEED
    environ["DEER_FLOW_CONFIG_PATH"] = str(tmp_path / "home" / "config.yaml")
    _render_base()
    restarted = _service(repository, environ)
    assert await restarted.start() is True
    assert environ["OPENAI_API_KEY"] == OWN
    assert _model_keys(_live_config())["gpt-4"] == OWN
    assert _source(await restarted.status(), "OPENAI_API_KEY") == "product"

    # The base file is still what the environment alone renders, so every
    # command the deployer runs with that environment still loads it.
    assert "$OPENAI_API_KEY" in Path(tmp_path / "home" / "config.yaml").read_text(encoding="utf-8")
    assert OWN not in Path(tmp_path / "home" / "config.yaml").read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_a_restore_taken_after_the_key_keeps_it_and_one_taken_before_leaves_the_environments(environ, database, tmp_path: Path) -> None:
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine
    from deerflow.persistence.provider_keys import ProviderKeyRepository

    db_path, repository = database
    before_backup = tmp_path / "before.db"
    _backup(db_path, before_backup)
    await _service(repository, environ).put("openai", OWN, **ADMIN)
    after_backup = tmp_path / "after.db"
    _backup(db_path, after_backup)
    await close_engine()

    async def start_on(backup: Path) -> ProviderKeyService:
        environ["OPENAI_API_KEY"] = SEED
        environ["DEER_FLOW_CONFIG_PATH"] = str(tmp_path / "home" / "config.yaml")
        restored = tmp_path / f"restored-{backup.stem}" / "keys.db"
        _backup(backup, restored)
        await init_engine("sqlite", url=f"sqlite+aiosqlite:///{restored}", sqlite_dir=str(restored.parent))
        service = _service(ProviderKeyRepository(get_session_factory()), environ)
        await service.start()
        return service

    service = await start_on(after_backup)
    assert environ["OPENAI_API_KEY"] == OWN
    assert _source(await service.status(), "OPENAI_API_KEY") == "product"
    await close_engine()

    service = await start_on(before_backup)
    assert environ["OPENAI_API_KEY"] == SEED
    assert _model_keys(_live_config())["gpt-4"] == SEED
    assert _source(await service.status(), "OPENAI_API_KEY") == "environment"


# ── no restart ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_a_key_for_a_provider_that_had_none_brings_its_models_and_a_replacement_is_what_the_next_model_uses(environ, database) -> None:
    _, repository = database
    service = _service(repository, environ)
    await service.start()
    assert "claude-sonnet-4" not in _model_keys(_live_config())

    await service.put("anthropic", OWN, **ADMIN)
    assert _model_keys(_live_config())["claude-sonnet-4"] == OWN
    assert _client_key("claude-sonnet-4") == OWN

    replaced = await service.put("anthropic", NEWER, **ADMIN)
    assert replaced["action"] == "replaced"
    assert _model_keys(_live_config())["claude-sonnet-4"] == NEWER
    # The client the next run builds carries the replacement; nothing is sent anywhere.
    assert _client_key("claude-sonnet-4") == NEWER
    await service.put("openai", NEWER, **ADMIN)
    assert _client_key("gpt-4") == NEWER


@pytest.mark.asyncio
async def test_a_search_providers_key_reaches_the_tools_and_the_environment_they_read(environ, database) -> None:
    _, repository = database
    service = _service(repository, environ)
    await service.put("tavily", OWN, **ADMIN)
    assert environ["TAVILY_API_KEY"] == OWN
    tool = next(tool for tool in _live_config().tools if tool.name == "web_search")
    assert tool.use.startswith("deerflow.community.tavily")


# ── removal ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_removing_the_products_key_falls_back_to_the_environments_or_to_none(environ, database) -> None:
    _, repository = database
    service = _service(repository, environ)
    await service.put("openai", OWN, **ADMIN)
    await service.put("anthropic", OWN, **ADMIN)

    removed = await service.remove("openai", **ADMIN)
    assert removed["action"] == "removed"
    assert removed["provider"]["source"] == "environment"
    assert environ["OPENAI_API_KEY"] == SEED
    assert _model_keys(_live_config())["gpt-4"] == SEED

    removed = await service.remove("anthropic", **ADMIN)
    assert removed["provider"]["source"] == "none"
    assert "ANTHROPIC_API_KEY" not in environ
    assert "claude-sonnet-4" not in _model_keys(_live_config())

    again = await service.remove("anthropic", **ADMIN)
    assert again["action"] == "none"
    assert [event["action"] for event in await repository.events()] == ["removed", "removed", "added", "added"]


@pytest.mark.asyncio
async def test_a_change_the_renderer_refuses_changes_nothing(environ, database) -> None:
    _, repository = database
    service = _service(repository, environ)
    await service.start()
    base = environ["DEER_FLOW_CONFIG_PATH"]
    # The profile's own renderer refuses this environment: no sign-in mode it knows.
    environ["HARTMESH_LOCAL_PASSWORDS"] = "bogus"
    with pytest.raises(ProviderKeysRefused) as refused:
        await service.put("openai", OWN, **ADMIN)
    assert (refused.value.code, refused.value.status) == ("render_refused", 422)
    assert OWN not in refused.value.message
    assert await repository.stored() == {}
    assert await repository.events() == []
    assert environ["OPENAI_API_KEY"] == SEED
    assert environ["DEER_FLOW_CONFIG_PATH"] == base
    assert not Path(base).with_name("config.effective.yaml").exists()


@pytest.mark.asyncio
async def test_a_start_with_nothing_stored_discards_a_second_file_an_earlier_start_left(environ, database) -> None:
    _, repository = database
    base = Path(environ["DEER_FLOW_CONFIG_PATH"])
    service = _service(repository, environ)
    await service.put("openai", OWN, **ADMIN)
    await service.remove("openai", **ADMIN)
    effective = base.with_name("config.effective.yaml")
    assert effective.exists()

    environ["DEER_FLOW_CONFIG_PATH"] = str(base)
    assert await _service(repository, environ).start() is False
    assert not effective.exists()


@pytest.mark.asyncio
async def test_a_key_is_one_printable_token_up_to_the_limit_and_surrounding_whitespace_is_not_part_of_it(environ, database) -> None:
    _, repository = database
    service = _service(repository, environ)
    for refused_key in ("abc\x07def", "two tokens", "k" * 4097, "   "):
        with pytest.raises(ProviderKeysRefused) as refused:
            await service.put("openai", refused_key, **ADMIN)
        assert (refused.value.code, refused.value.status) == ("key_invalid", 422)
    assert await repository.stored() == {}
    assert (await service.put("openai", "k" * 4096, **ADMIN))["action"] == "added"
    await service.put("openai", f"  {NEWER}\n", **ADMIN)
    assert environ["OPENAI_API_KEY"] == NEWER


# ── a change that cannot be completed ──────────────────────────────────────


def _fail_the_file_swap(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(source, destination):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(os, "replace", refuse)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["replace", "remove"])
async def test_a_change_the_gateway_cannot_apply_is_not_recorded_and_the_applied_key_stays_the_stored_one(environ, database, change) -> None:
    _, repository = database
    service = _service(repository, environ)
    await service.put("openai", OWN, **ADMIN)

    with pytest.MonkeyPatch.context() as disk_full, pytest.raises(OSError):
        _fail_the_file_swap(disk_full)
        if change == "replace":
            await service.put("openai", NEWER, **ADMIN)
        else:
            await service.remove("openai", **ADMIN)

    # What the page and the command say is what the Gateway uses: a revoked
    # key reported removed while it is still billed is the failure to avoid.
    assert _source(await service.status(), "OPENAI_API_KEY") == "product"
    assert [event["action"] for event in await repository.events()] == ["added"]
    assert environ["OPENAI_API_KEY"] == OWN
    assert _model_keys(_live_config())["gpt-4"] == OWN
    assert (await service.remove("openai", **ADMIN))["action"] == "removed"


@pytest.mark.asyncio
async def test_a_change_the_database_does_not_take_puts_the_gateway_back_on_what_is_stored(environ, database, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _, repository = database
    service = _service(repository, environ)

    async def refuse(*args, **kwargs):
        raise RuntimeError("the database went away")

    monkeypatch.setattr(repository, "put", refuse)
    with pytest.raises(RuntimeError):
        await service.put("anthropic", OWN, **ADMIN)

    # Back on the base config, byte for byte what it was: nothing is stored.
    assert "ANTHROPIC_API_KEY" not in environ
    assert environ["DEER_FLOW_CONFIG_PATH"] == str(tmp_path / "home" / "config.yaml")
    assert not (tmp_path / "home" / "config.effective.yaml").exists()
    assert "claude-sonnet-4" not in _model_keys(_live_config())
    assert _model_keys(_live_config())["gpt-4"] == SEED


@pytest.mark.asyncio
async def test_a_config_read_while_a_key_is_being_applied_finds_every_variable_it_names(environ, database, monkeypatch: pytest.MonkeyPatch) -> None:
    from deerflow.config.app_config import reload_app_config

    _, repository = database
    service = _service(repository, environ)
    await service.put("openai", OWN, **ADMIN)

    swap = os.replace
    reads: list[BaseException | None] = []

    def swap_then_read(source, destination):
        swap(source, destination)
        # Another request loading the config the instant the new file is in place.
        try:
            reload_app_config()
            reads.append(None)
        except BaseException as exc:  # noqa: BLE001 -- the read is what is under test
            reads.append(exc)

    monkeypatch.setattr(os, "replace", swap_then_read)
    await service.put("anthropic", OWN, **ADMIN)
    assert reads == [None]


# ── the source signal ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_the_signal_names_product_environment_and_none_at_once_and_never_a_key(environ, database) -> None:
    _, repository = database
    service = _service(repository, environ)
    await service.put("anthropic", OWN, **ADMIN)

    document = await service.status()
    assert _source(document, "ANTHROPIC_API_KEY") == "product"
    assert _source(document, "OPENAI_API_KEY") == "environment"
    assert _source(document, "DEEPSEEK_API_KEY") == "none"
    assert {provider["variable"] for provider in document["providers"]} == set(_catalog_variables())
    anthropic = next(provider for provider in document["providers"] if provider["variable"] == "ANTHROPIC_API_KEY")
    assert anthropic["product_key"] == "set"
    assert anthropic["changed_by"] == "admin@example.com"
    assert anthropic["kind"] == "models"
    assert document["refusal"] is None
    assert OWN not in repr(document) and SEED not in repr(document)


# ── at rest ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_the_stored_row_is_not_the_key(environ, database) -> None:
    db_path, repository = database
    await _service(repository, environ).put("openai", OWN, **ADMIN)
    # The main file and its write-ahead log: a recent write is in the log.
    for suffix in ("", "-wal"):
        on_disk = Path(f"{db_path}{suffix}")
        assert on_disk.exists() or suffix, on_disk
        assert not on_disk.exists() or OWN.encode() not in on_disk.read_bytes()
    stored = (await repository.stored())["OPENAI_API_KEY"]
    assert OWN not in stored.ciphertext


@pytest.mark.asyncio
async def test_without_a_wrapping_key_a_write_is_refused_and_says_why(environ, database) -> None:
    _, repository = database
    del environ[WRAPPING_KEY_ENV]
    service = _service(repository, environ)
    with pytest.raises(ProviderKeysRefused) as refused:
        await service.put("openai", OWN, **ADMIN)
    assert refused.value.code == "no_wrapping_key"
    assert WRAPPING_KEY_ENV in refused.value.message
    assert await repository.stored() == {}
    assert (await service.status())["refusal"]["code"] == "no_wrapping_key"


@pytest.mark.asyncio
@pytest.mark.parametrize(("secret", "wrapping_key", "refusal"), [(None, "absent", "no_wrapping_key"), ("too-short", "invalid", "wrapping_key_invalid")], ids=["unset", "too-short"])
async def test_with_no_usable_wrapping_key_a_stored_key_leaves_its_provider_with_no_key_and_can_still_be_removed(environ, database, secret, wrapping_key, refusal) -> None:
    _, repository = database
    await _service(repository, environ).put("openai", OWN, **ADMIN)

    # A later start whose command did not carry the secret, or carried a bad one.
    if secret is None:
        del environ[WRAPPING_KEY_ENV]
    else:
        environ[WRAPPING_KEY_ENV] = secret
    environ["OPENAI_API_KEY"] = SEED
    environ["DEER_FLOW_CONFIG_PATH"] = str(Path(environ["DEER_FLOW_CONFIG_PATH"]).with_name("config.yaml"))
    _render_base()
    service = _service(repository, environ)
    assert await service.start() is True

    assert "OPENAI_API_KEY" not in environ
    assert "gpt-4" not in _model_keys(_live_config())
    document = await service.status()
    assert (document["wrapping_key"], document["refusal"]["code"]) == (wrapping_key, refusal)
    openai = next(provider for provider in document["providers"] if provider["variable"] == "OPENAI_API_KEY")
    assert (openai["source"], openai["product_key"]) == ("none", "unreadable")
    with pytest.raises(ProviderKeysRefused) as refused:
        await service.put("openai", NEWER, **ADMIN)
    assert refused.value.code == refusal

    # Removing needs no wrapping key: the way back to the environment's key.
    removed = await service.remove("openai", **ADMIN)
    assert (removed["action"], removed["provider"]["source"]) == ("removed", "environment")
    assert environ["OPENAI_API_KEY"] == SEED
    assert _model_keys(_live_config())["gpt-4"] == SEED


@pytest.mark.asyncio
async def test_under_a_different_wrapping_key_the_provider_has_no_key_and_never_the_seed(environ, database) -> None:
    _, repository = database
    await _service(repository, environ).put("openai", OWN, **ADMIN)

    # A restore into a deployment with another wrapping key, whose .env still carries the seed.
    environ[WRAPPING_KEY_ENV] = "x" * 44
    environ["OPENAI_API_KEY"] = SEED
    environ["DEER_FLOW_CONFIG_PATH"] = str(Path(environ["DEER_FLOW_CONFIG_PATH"]).with_name("config.yaml"))
    _render_base()
    service = _service(repository, environ)
    await service.start()

    assert "OPENAI_API_KEY" not in environ
    assert "gpt-4" not in _model_keys(_live_config())
    openai = next(provider for provider in (await service.status())["providers"] if provider["variable"] == "OPENAI_API_KEY")
    assert (openai["source"], openai["product_key"]) == ("none", "unreadable")

    # Setting it again is the remedy, and removing it the other one.
    await service.put("openai", NEWER, **ADMIN)
    assert environ["OPENAI_API_KEY"] == NEWER


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("secret", "logged"),
    [(None, f"{WRAPPING_KEY_ENV} is not set"), ("too-short", f"{WRAPPING_KEY_ENV} must be at least 32 characters")],
    ids=["absent", "too-short"],
)
async def test_the_start_log_names_why_the_stored_keys_cannot_be_read(environ, database, caplog: pytest.LogCaptureFixture, secret, logged) -> None:
    _, repository = database
    await _service(repository, environ).put("openai", OWN, **ADMIN)

    if secret is None:
        del environ[WRAPPING_KEY_ENV]
    else:
        environ[WRAPPING_KEY_ENV] = secret
    environ["DEER_FLOW_CONFIG_PATH"] = str(Path(environ["DEER_FLOW_CONFIG_PATH"]).with_name("config.yaml"))
    _render_base()
    with caplog.at_level("WARNING", logger="app.gateway.provider_keys.service"):
        await _service(repository, environ).start()

    unreadable = [record.getMessage() for record in caplog.records if "cannot be read" in record.getMessage()]
    assert len(unreadable) == 1
    # A deployer told "not set" about a secret that is set goes looking for the wrong mistake.
    assert logged in unreadable[0]
    assert (f"{WRAPPING_KEY_ENV} is not set" in unreadable[0]) == (secret is None)


@pytest.mark.asyncio
async def test_a_rotation_rewraps_every_stored_key_at_start(environ, database) -> None:
    from app.gateway.provider_keys.cipher import PREVIOUS_WRAPPING_KEY_ENV, ProviderKeyCipher

    _, repository = database
    await _service(repository, environ).put("openai", OWN, **ADMIN)

    environ[PREVIOUS_WRAPPING_KEY_ENV] = WRAPPING
    environ[WRAPPING_KEY_ENV] = "n" * 44
    environ["OPENAI_API_KEY"] = SEED
    service = _service(repository, environ)
    await service.start()
    assert environ["OPENAI_API_KEY"] == OWN

    stored = (await repository.stored())["OPENAI_API_KEY"]
    only_new = ProviderKeyCipher("n" * 44)
    assert only_new.unwrap("OPENAI_API_KEY", stored.ciphertext).key == OWN
    assert next(provider for provider in (await service.status())["providers"] if provider["variable"] == "OPENAI_API_KEY")["wrapped_with"] == "current"


# ── the operator model file ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_with_an_operator_model_file_writes_are_refused_and_nothing_stored_is_applied(environ, database, tmp_path: Path) -> None:
    _, repository = database
    await _service(repository, environ).put("openai", OWN, **ADMIN)

    models = tmp_path / "operator" / "models.yaml"
    models.parent.mkdir()
    models.write_text("models: []\n", encoding="utf-8")
    environ["HARTMESH_MODELS_FILE"] = str(models)
    environ["OPENAI_API_KEY"] = SEED
    environ["DEER_FLOW_CONFIG_PATH"] = str(tmp_path / "home" / "config.yaml")
    _render_base()
    base = Path(environ["DEER_FLOW_CONFIG_PATH"]).read_bytes()

    service = _service(repository, environ)
    assert await service.start() is False
    assert environ["OPENAI_API_KEY"] == SEED
    assert environ["DEER_FLOW_CONFIG_PATH"] == str(tmp_path / "home" / "config.yaml")
    assert Path(environ["DEER_FLOW_CONFIG_PATH"]).read_bytes() == base

    for write in (service.put("openai", NEWER, **ADMIN), service.remove("openai", **ADMIN)):
        with pytest.raises(ProviderKeysRefused) as refused:
            await write
        assert refused.value.code == "operator_model_file"
        assert "HARTMESH_MODELS_FILE" in refused.value.message
    document = await service.status()
    assert document["refusal"]["code"] == "operator_model_file"
    assert _source(document, "OPENAI_API_KEY") == "environment"


# ── what is refused ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_a_provider_outside_the_catalog_and_an_unusable_key_are_refused(environ, database) -> None:
    _, repository = database
    service = _service(repository, environ)
    with pytest.raises(ProviderKeysRefused) as refused:
        await service.put("acme", OWN, **ADMIN)
    assert (refused.value.code, refused.value.status) == ("unknown_provider", 404)
    # A variable is not a provider name, even a catalog one.
    with pytest.raises(ProviderKeysRefused):
        await service.put("OPENAI_API_KEY", OWN, **ADMIN)
    for bad in ("", "   ", "two words", "line\nbreak", "x" * 4097):
        with pytest.raises(ProviderKeysRefused) as refused:
            await service.put("openai", bad, **ADMIN)
        assert refused.value.code == "key_invalid"
        assert bad.strip() == "" or bad not in refused.value.message
    assert await repository.stored() == {}
