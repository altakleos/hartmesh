"""The deployer's reading of where each provider's key comes from: one JSON document, never a key.

Run as the deployer runs it: a separate process with the deployment's
environment, the base config ``gateway/run.sh`` rendered, and the database
the Gateway writes.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture

from app.gateway.provider_keys.cipher import WRAPPING_KEY_ENV
from app.gateway.provider_keys.profile import PROFILE_DIR_ENV, ProfileRenderer

BACKEND = Path(__file__).resolve().parents[1]
PROFILE = BACKEND.parent / "deploy" / "compose"
SEED = "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY-SEED"
OWN = "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY-OWN"
ADMIN = {"actor_id": "admin-1", "actor_email": "admin@example.com"}


def _catalog_variables() -> list[str]:
    return sorted({yaml.safe_load(path.read_text(encoding="utf-8"))["env"] for path in (PROFILE / "providers").rglob("*.yaml")})


@pytest.fixture
def deployment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """The profile's base config as run.sh renders it, on a sqlite database the command opens itself."""
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
        WRAPPING_KEY_ENV: "w" * 44,
        "DEER_FLOW_CONFIG_PATH": str(tmp_path / "home" / "config.yaml"),
        "DEER_FLOW_HOME": str(tmp_path / "home"),
        "OPENAI_API_KEY": SEED,
        "TAVILY_API_KEY": SEED,
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    spec = importlib.util.spec_from_file_location("hartmesh_render_config_provider_keys_command", PROFILE / "gateway" / "render_config.py")
    renderer = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = renderer
    spec.loader.exec_module(renderer)
    rendered, _ = renderer.render_text((PROFILE / "config.yaml").read_text(encoding="utf-8"), renderer.load_catalog(PROFILE / "providers"), os.environ)
    document = yaml.safe_load(rendered)
    # The profile's PostgreSQL, swapped for a file both processes can open.
    document["database"] = {"backend": "sqlite", "sqlite_dir": str(tmp_path / "db")}
    document.pop("checkpointer", None)
    (tmp_path / "home").mkdir()
    (tmp_path / "home" / "config.yaml").write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return tmp_path


def _set_in_the_product(tmp_path: Path, provider: str, key: str) -> None:
    """What an administrator's PUT does, against the same database; the environment is a copy, as the Gateway's is its own."""
    from app.gateway.provider_keys.service import ProviderKeyService
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine
    from deerflow.persistence.provider_keys import ProviderKeyRepository

    async def put() -> None:
        await init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path / 'db' / 'deerflow.db'}", sqlite_dir=str(tmp_path / "db"))
        try:
            service = ProviderKeyService(renderer=ProfileRenderer(PROFILE), repository=ProviderKeyRepository(get_session_factory()), environ=dict(os.environ), reload=lambda: None)
            await service.put(provider, key, **ADMIN)
        finally:
            await close_engine()

    asyncio.run(put())


def _command(env: dict[str, str] | None = None, *args: str) -> tuple[int, dict, str]:
    environment = {**(env if env is not None else os.environ), "PYTHONPATH": f"{BACKEND}{os.pathsep}{BACKEND / 'tests'}"}
    completed = subprocess.run([sys.executable, "-m", "app.gateway.provider_keys.status", *args], cwd=BACKEND, env=environment, capture_output=True, text=True, timeout=120, check=False)
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    assert len(lines) == 1, f"one JSON document on stdout, got: {completed.stdout!r} / {completed.stderr[-800:]!r}"
    return completed.returncode, json.loads(lines[0]), completed.stdout + completed.stderr


def test_the_command_names_product_environment_and_none_at_once_and_never_a_key(deployment: Path) -> None:
    _set_in_the_product(deployment, "anthropic", OWN)
    _set_in_the_product(deployment, "openai", OWN)

    code, document, output = _command()
    assert code == 0, document
    assert document["command"] == "provider-keys-status"
    assert document["refusal"] is None and document["wrapping_key"] == "set"
    by_variable = {provider["variable"]: provider for provider in document["providers"]}
    assert set(by_variable) == set(_catalog_variables())
    # The product's key outranks the environment's seed for the same provider.
    assert (by_variable["OPENAI_API_KEY"]["source"], by_variable["OPENAI_API_KEY"]["product_key"]) == ("product", "set")
    assert by_variable["ANTHROPIC_API_KEY"]["source"] == "product"
    assert by_variable["TAVILY_API_KEY"]["source"] == "environment"
    assert by_variable["DEEPSEEK_API_KEY"]["source"] == "none"
    assert by_variable["OPENAI_API_KEY"]["changed_by"] == "admin@example.com"
    assert by_variable["OPENAI_API_KEY"]["wrapped_with"] == "current"
    assert OWN not in output and SEED not in output


def test_under_another_wrapping_key_the_command_says_unreadable_and_no_key(deployment: Path) -> None:
    _set_in_the_product(deployment, "openai", OWN)
    code, document, output = _command({**os.environ, WRAPPING_KEY_ENV: "x" * 44})
    assert code == 0
    openai = next(provider for provider in document["providers"] if provider["variable"] == "OPENAI_API_KEY")
    assert (openai["source"], openai["product_key"]) == ("none", "unreadable")
    assert OWN not in output


def test_outside_the_profile_the_command_refuses_with_a_document(deployment: Path) -> None:
    env = {name: value for name, value in os.environ.items() if name != PROFILE_DIR_ENV}
    code, document, _ = _command(env)
    assert code == 1
    assert document["error"] == "not_available"


def test_a_command_line_it_cannot_read_is_a_document_too(deployment: Path) -> None:
    code, document, output = _command(None, "--key", OWN)
    assert (code, document["error"]) == (1, "usage")
    assert OWN not in output


def test_a_failure_is_named_by_its_kind_and_never_by_its_text(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """An exception's text can carry a connection string's password; the document names only its kind."""
    from app.gateway.provider_keys import status

    async def fail():
        raise RuntimeError(f"could not connect to postgresql://deerflow:{OWN}@postgres/deerflow")

    monkeypatch.setattr(status, "_run", fail)
    assert status.main([]) == 1
    output = capsys.readouterr().out
    assert json.loads(output) == {"command": "provider-keys-status", "error": "failed", "message": "RuntimeError"}
    assert OWN not in output
