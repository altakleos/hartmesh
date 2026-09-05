"""Offline contracts for the tenant VM compose profile under deploy/compose.

The profile is a released deployment path beside the Helm chart: one KVM
guest per customer, the whole stack under Docker Compose, sandboxes created
by the Gateway's local Docker backend. These tests pin what the profile
promises without a Docker daemon: the .env contract, the published surface,
the memory budget, the config render, the nginx render, and the release
pinning that makes ``images.txt`` and the profile agree byte for byte.
"""

from __future__ import annotations

import importlib.util
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE = REPO_ROOT / "deploy" / "compose"
COMPOSE = PROFILE / "compose.yaml"
TEMPLATE = PROFILE / "config.yaml"
CATALOG = PROFILE / "providers"
IMAGES = PROFILE / "images.txt"
CONTRACT_KEYS = {
    "HARTMESH_TENANT",
    "HARTMESH_PUBLIC_HOST",
    "HARTMESH_TRUSTED_PROXIES",
    "HARTMESH_LISTEN",
    "HARTMESH_DATA_DIR",
    "SANDBOX_RUNTIME",
    "SANDBOX_EGRESS",
    "POSTGRES_PASSWORD",
    "REDIS_PASSWORD",
    "AUTH_JWT_SECRET",
}
SERVICES = {"gateway", "frontend", "nginx", "postgres", "redis"}
MEMORY_MIB = {"gateway": 1536, "frontend": 384, "nginx": 128, "postgres": 768, "redis": 256}
NGINX_VARIABLES = {
    "$forwarded_proto",
    "$remote_addr",
    "$proxy_add_x_forwarded_for",
    "$http_host",
    "$gateway_upstream",
    "$http_upgrade",
    "$connection_upgrade",
    "$scheme",
    "$frontend_upstream",
    "$provisioner_upstream",
    "$http_x_forwarded_proto",
}
_BARE_VARIABLE = re.compile(r"\$[a-z_]+")
_ENV_REFERENCE = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)")


def _load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def render_config() -> Iterator[ModuleType]:
    module = _load_module("hartmesh_render_config_test", PROFILE / "gateway" / "render_config.py")
    try:
        yield module
    finally:
        sys.modules.pop("hartmesh_render_config_test", None)


@pytest.fixture(scope="module")
def pin_images() -> Iterator[ModuleType]:
    module = _load_module("hartmesh_pin_compose_images_test", REPO_ROOT / "scripts" / "pin_compose_images.py")
    try:
        yield module
    finally:
        sys.modules.pop("hartmesh_pin_compose_images_test", None)


@pytest.fixture(scope="module")
def compose() -> dict:
    return yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))


def _mib(value: str) -> int:
    assert value.endswith("m"), value
    return int(value[:-1])


def _base_environ() -> dict[str, str]:
    return {"DATABASE_URL": "postgresql://deerflow:x@postgres:5432/deerflow", "DEER_FLOW_STREAM_BRIDGE_REDIS_URL": "redis://:x@redis:6379/0"}


# ── compose.yaml ─────────────────────────────────────────────────────────────


def test_profile_declares_exactly_the_five_services_and_no_named_volumes(compose: dict) -> None:
    assert compose["name"] == "hartmesh"
    assert set(compose["services"]) == SERVICES
    assert "volumes" not in compose, "named volumes would live on the root disk, which is not tenant data"
    for name, service in compose["services"].items():
        assert service.get("restart") == "unless-stopped", name
        assert "logging" not in service, f"{name}: the daemon's journald default must not be overridden"


def test_only_nginx_publishes_a_port_and_the_contract_carries_the_bind(compose: dict) -> None:
    published = {name: service.get("ports") for name, service in compose["services"].items() if service.get("ports")}
    assert set(published) == {"nginx"}
    (mapping,) = published["nginx"]
    assert mapping.startswith("${HARTMESH_LISTEN") and mapping.endswith(":2026")
    assert compose["services"]["nginx"].get("user") == "101:101"


def test_memory_limits_sum_to_three_gib_with_equal_swap(compose: dict) -> None:
    total = 0
    for name, expected in MEMORY_MIB.items():
        service = compose["services"][name]
        assert _mib(service["mem_limit"]) == expected, name
        assert service["memswap_limit"] == service["mem_limit"], name
        total += expected
    assert total == 3072


def test_only_the_gateway_reads_the_env_file_and_the_others_get_explicit_environment(compose: dict) -> None:
    services = compose["services"]
    with_env_file = {name for name, service in services.items() if service.get("env_file")}
    assert with_env_file == {"gateway"}
    (env_file,) = services["gateway"]["env_file"]
    assert env_file == {"path": "${HARTMESH_DATA_DIR}/.env", "required": False}
    assert set(services["frontend"]["environment"]) == {"NODE_ENV", "DEER_FLOW_INTERNAL_GATEWAY_BASE_URL"}
    assert services["frontend"]["environment"]["DEER_FLOW_INTERNAL_GATEWAY_BASE_URL"] == "http://gateway:8001"
    assert set(services["nginx"]["environment"]) == {"HARTMESH_PUBLIC_HOST", "HARTMESH_TRUSTED_PROXIES"}
    for name in ("frontend", "nginx", "postgres", "redis"):
        assert "AUTH_JWT_SECRET" not in yaml.safe_dump(services[name]), name


def test_bind_mounts_stay_under_the_data_directory_or_the_read_only_bundle(compose: dict) -> None:
    for name, service in compose["services"].items():
        for volume in service.get("volumes", []):
            if isinstance(volume, dict):
                assert volume["type"] == "tmpfs", (name, volume)
                continue
            source, _, rest = volume.partition(":")
            if source == "/var/run/docker.sock":
                assert name == "gateway"
                continue
            if source.startswith("./"):
                assert rest.endswith(":ro") and rest.startswith("/opt/hartmesh/"), (name, volume)
                continue
            assert source.startswith("${HARTMESH_DATA_DIR"), (name, volume)


def test_gateway_wiring_follows_the_contract(compose: dict) -> None:
    gateway = compose["services"]["gateway"]
    env = gateway["environment"]
    assert "user" not in gateway, "the entrypoint drops privileges itself after reading the socket's group"
    assert gateway["command"] == ["sh", "/opt/hartmesh/gateway/entrypoint.sh"]
    assert env["DEER_FLOW_TENANT_ID"].startswith("${HARTMESH_TENANT")
    assert env["DEER_FLOW_SANDBOX_RUNTIME"].startswith("${SANDBOX_RUNTIME")
    assert env["DEER_FLOW_HOME"] == "${HARTMESH_DATA_DIR}/home"
    assert env["DEER_FLOW_HOST_BASE_DIR"] == env["DEER_FLOW_HOME"]
    assert env["DEER_FLOW_CONFIG_PATH"] == "${HARTMESH_DATA_DIR}/home/config.yaml"
    assert env["DEER_FLOW_EXTENSIONS_CONFIG_PATH"] == "${HARTMESH_DATA_DIR}/home/extensions_config.json"
    assert "${HARTMESH_DATA_DIR}/home:${HARTMESH_DATA_DIR}/home" in gateway["volumes"], "the data dir must be mounted at its host path"
    assert env["DATABASE_URL"] == "postgresql://deerflow:${POSTGRES_PASSWORD}@postgres:5432/deerflow"
    assert env["DEER_FLOW_STREAM_BRIDGE_REDIS_URL"] == "redis://:${REDIS_PASSWORD}@redis:6379/0"
    assert env["DEER_FLOW_SANDBOX_HOST"] == "host.docker.internal"
    assert "host.docker.internal:host-gateway" in gateway["extra_hosts"]
    assert env["DEER_FLOW_SANDBOX_NETWORK"] == "hartmesh_sandbox"
    assert env["DEER_FLOW_SANDBOX_MEMORY"] == "640m"
    assert env["DEER_FLOW_SANDBOX_CPUS"] == "1"
    assert int(env["DEER_FLOW_SANDBOX_PIDS_LIMIT"]) > 0
    assert env["DEER_FLOW_SANDBOX_PROXY_MEMORY"].endswith("m")
    assert env["DEER_FLOW_SANDBOX_CONTAINER_USER"] == "1000:1000"
    assert env["DEER_FLOW_SANDBOX_IMAGE_STARTUP_CAPS"] == "0"
    assert env["DEER_FLOW_SANDBOX_SECCOMP_UNCONFINED"] == "0"
    assert "DEER_FLOW_SANDBOX_SECCOMP_PROFILE" not in env
    assert "DEER_FLOW_INTERNAL_AUTH_TOKEN" not in env, "one worker keeps the per-process token coherent without a second key"
    assert "BETTER_AUTH_SECRET" not in COMPOSE.read_text(encoding="utf-8")
    subnet = compose["networks"]["app"]["ipam"]["config"][0]["subnet"]
    assert env["AUTH_TRUSTED_PROXIES"] == subnet
    assert compose["services"]["gateway"]["healthcheck"]["test"][-1].count("/health/ready") == 1


def test_sandbox_network_is_declared_and_joined_by_no_service(compose: dict) -> None:
    assert set(compose["networks"]) == {"app", "sandbox"}
    for name, service in compose["services"].items():
        assert service["networks"] == ["app"], name
    run = (PROFILE / "gateway" / "run.sh").read_text(encoding="utf-8")
    assert 'docker network create --driver bridge "$DEER_FLOW_SANDBOX_NETWORK"' in run
    assert 'docker network inspect "$DEER_FLOW_SANDBOX_NETWORK"' in run


def test_datastores_run_as_the_data_directory_owner_with_relaxed_durability(compose: dict) -> None:
    postgres = compose["services"]["postgres"]
    redis = compose["services"]["redis"]
    assert postgres["user"] == "1000:1000" and redis["user"] == "1000:1000"
    assert postgres["command"] == ["postgres", "-c", "synchronous_commit=off", "-c", "wal_writer_delay=200ms"]
    assert postgres["stop_grace_period"] == "60s"
    assert "--appendfsync everysec" in redis["command"][-1]
    assert "--maxmemory 200mb --maxmemory-policy allkeys-lru" in redis["command"][-1]
    assert "$$REDIS_PASSWORD" in redis["command"][-1]
    assert compose["services"]["gateway"]["depends_on"] == {"postgres": {"condition": "service_healthy"}, "redis": {"condition": "service_healthy"}}


# ── the .env contract ────────────────────────────────────────────────────────


def test_env_example_lists_exactly_the_fixed_contract_keys() -> None:
    lines = (PROFILE / ".env.example").read_text(encoding="utf-8").splitlines()
    keys = {line.split("=", 1)[0] for line in lines if line and not line.startswith("#")}
    assert keys == CONTRACT_KEYS
    comments = [line for line in lines if line.startswith("#")]
    assert len(comments) == 1 and "verbatim" in comments[0] and "subset" in comments[0]
    values = dict(line.split("=", 1) for line in lines if line and not line.startswith("#"))
    assert values["HARTMESH_TRUSTED_PROXIES"] == "192.0.2.10,192.0.2.11"
    assert values["HARTMESH_PUBLIC_HOST"] == "tenant.example.com"
    assert values["HARTMESH_LISTEN"] == "0.0.0.0:2026"
    assert values["SANDBOX_EGRESS"] in {"allowlist", "open"}
    assert not (PROFILE / ".env").exists()
    assert ".env" in (PROFILE / ".gitignore").read_text(encoding="utf-8").split()


def test_profile_consumes_no_key_outside_the_contract() -> None:
    referenced = set(_ENV_REFERENCE.findall(COMPOSE.read_text(encoding="utf-8")))
    assert referenced <= CONTRACT_KEYS, referenced - CONTRACT_KEYS
    assert referenced >= CONTRACT_KEYS - {"SANDBOX_EGRESS"}, "every fixed key but SANDBOX_EGRESS is interpolated by compose.yaml"
    contract_like = re.compile(r"\b(HARTMESH_[A-Z_]+|SANDBOX_[A-Z_]+|POSTGRES_PASSWORD|REDIS_PASSWORD|AUTH_JWT_SECRET)\b")
    seams = {"HARTMESH_RENDER_ONLY", "HARTMESH_NGINX_SOURCE", "HARTMESH_NGINX_TARGET"}
    for path in (PROFILE / "gateway" / "run.sh", PROFILE / "gateway" / "entrypoint.sh", PROFILE / "gateway" / "render_config.py", PROFILE / "nginx" / "render.sh"):
        names = set(contract_like.findall(path.read_text(encoding="utf-8"))) - seams
        assert names <= CONTRACT_KEYS, (path.name, names - CONTRACT_KEYS)


def test_gateway_entrypoint_drops_to_uid_1000_with_the_socket_group_and_runs_one_worker() -> None:
    entrypoint = (PROFILE / "gateway" / "entrypoint.sh").read_text(encoding="utf-8")
    run = (PROFILE / "gateway" / "run.sh").read_text(encoding="utf-8")
    assert 'docker_gid="$(stat -c %g "$SOCKET")"' in entrypoint
    assert 'setpriv --reuid=1000 --regid=1000 --groups="$docker_gid" --inh-caps=-all --no-new-privs sh "$RUN"' in entrypoint
    assert "uvicorn app.gateway.app:app --host 0.0.0.0 --port 8001 --workers 1" in run
    assert 'render_config.py" \\' in run and '--output "$DEER_FLOW_CONFIG_PATH"' in run
    assert 'if [ ! -f "$DEER_FLOW_EXTENSIONS_CONFIG_PATH" ]; then' in run
    assert 'cp "$PROFILE/extensions_config.json" "$DEER_FLOW_EXTENSIONS_CONFIG_PATH"' in run


# ── nginx ────────────────────────────────────────────────────────────────────


def test_profile_nginx_conf_is_a_verbatim_copy_of_the_compose_nginx_conf() -> None:
    assert (PROFILE / "nginx" / "nginx.conf").read_bytes() == (REPO_ROOT / "docker" / "nginx" / "nginx.conf").read_bytes()
    source = (PROFILE / "nginx" / "nginx.conf").read_text(encoding="utf-8")
    assert "${" not in source
    assert set(_BARE_VARIABLE.findall(source)) == NGINX_VARIABLES
    assert len(_BARE_VARIABLE.findall(source)) == 99


def _render_nginx(tmp_path: Path, environ: dict[str, str], *, source: Path | None = None) -> subprocess.CompletedProcess[str]:
    target = tmp_path / "nginx.conf"
    env = {"PATH": os.environ["PATH"], "HARTMESH_RENDER_ONLY": "1", "HARTMESH_NGINX_SOURCE": str(source or PROFILE / "nginx" / "nginx.conf"), "HARTMESH_NGINX_TARGET": str(target), **environ}
    return subprocess.run(["sh", str(PROFILE / "nginx" / "render.sh")], env=env, capture_output=True, text=True, timeout=30, check=False)


def test_nginx_render_substitutes_only_the_server_name_and_adds_real_ip_directives(tmp_path: Path) -> None:
    result = _render_nginx(tmp_path, {"HARTMESH_PUBLIC_HOST": "tenant.example.com", "HARTMESH_TRUSTED_PROXIES": "192.0.2.10, 192.0.2.11,2001:db8::/32"})
    assert result.returncode == 0, result.stderr
    rendered = (tmp_path / "nginx.conf").read_text(encoding="utf-8")
    source = (PROFILE / "nginx" / "nginx.conf").read_text(encoding="utf-8")
    assert _BARE_VARIABLE.findall(rendered) == _BARE_VARIABLE.findall(source), "every nginx variable must survive the render"
    assert "server_name _;" not in rendered
    assert "        server_name tenant.example.com;\n" in rendered
    for address in ("192.0.2.10", "192.0.2.11", "2001:db8::/32"):
        assert f"        set_real_ip_from {address};\n" in rendered
    assert "        real_ip_header X-Forwarded-For;\n" in rendered
    assert "        real_ip_recursive on;\n" in rendered
    assert rendered.count("set_real_ip_from ") == 3
    assert rendered.count("real_ip_header ") == 1


def test_nginx_render_refuses_bad_hosts_bad_proxies_and_a_missing_anchor(tmp_path: Path) -> None:
    bad_host = _render_nginx(tmp_path, {"HARTMESH_PUBLIC_HOST": "evil; }", "HARTMESH_TRUSTED_PROXIES": "192.0.2.10"})
    assert bad_host.returncode == 1 and "HARTMESH_PUBLIC_HOST" in bad_host.stderr
    bad_proxy = _render_nginx(tmp_path, {"HARTMESH_PUBLIC_HOST": "tenant.example.com", "HARTMESH_TRUSTED_PROXIES": "192.0.2.10;x"})
    assert bad_proxy.returncode == 1 and "HARTMESH_TRUSTED_PROXIES" in bad_proxy.stderr
    empty = _render_nginx(tmp_path, {"HARTMESH_PUBLIC_HOST": "tenant.example.com", "HARTMESH_TRUSTED_PROXIES": " , "})
    assert empty.returncode == 1
    anchorless = tmp_path / "anchorless.conf"
    anchorless.write_text((PROFILE / "nginx" / "nginx.conf").read_text(encoding="utf-8").replace("server_name _;", "server_name x;"), encoding="utf-8")
    missing = _render_nginx(tmp_path, {"HARTMESH_PUBLIC_HOST": "tenant.example.com", "HARTMESH_TRUSTED_PROXIES": "192.0.2.10"}, source=anchorless)
    assert missing.returncode == 1 and "anchor" in missing.stderr


# ── config.yaml render ───────────────────────────────────────────────────────


def _example_config() -> str:
    return (REPO_ROOT / "config.example.yaml").read_text(encoding="utf-8")


def test_template_matches_the_example_version_provider_and_local_backend(render_config: ModuleType) -> None:
    template = yaml.safe_load(TEMPLATE.read_text(encoding="utf-8"))
    example = yaml.safe_load(_example_config())
    assert template["config_version"] == example["config_version"]
    provider_line = re.search(r"^#\s+use: (deerflow\.community\.aio_sandbox:AioSandboxProvider)$", _example_config(), flags=re.MULTILINE)
    assert provider_line is not None
    assert template["sandbox"]["use"] == provider_line.group(1)
    assert "provisioner_url" not in template["sandbox"]
    assert template["sandbox"]["replicas"] == 2
    assert template["sandbox"]["image"] == "ghcr.io/altakleos/hartmesh-sandbox:v2.1.0-hartmesh.4" or "@sha256:" in template["sandbox"]["image"]
    assert template["sandbox"]["network"]["allow_domains"] == ["pypi.org", "files.pythonhosted.org", "registry.npmjs.org", "github.com"]
    assert template["sandbox"]["network"]["approval"] == "prompt"
    assert template["skills"]["path"].startswith("/srv/hartmesh/")
    assert template["deployment"]["profile"] == "local_development"
    assert template["run_events"]["backend"] == "db"
    assert template["database"]["postgres_url"] == "$DATABASE_URL"


def test_catalog_fragments_are_derived_from_the_example_and_reference_only_their_own_key(render_config: ModuleType) -> None:
    fragments = render_config.load_catalog(CATALOG)
    assert len(fragments) >= 20
    example = _example_config()
    for fragment in fragments:
        assert fragment.env in example, fragment.source
        assert fragment.env.endswith(("_API_KEY", "_APIKEY")), fragment.source
        referenced: set[str] = set()
        render_config._references({"models": list(fragment.models), "tools": list(fragment.tools)}, referenced)
        assert referenced <= {fragment.env}, (fragment.source, referenced)
        for model in fragment.models:
            assert isinstance(model.get("use"), str) and isinstance(model.get("model"), str), fragment.source
        for tool in fragment.tools:
            assert tool["name"] in {"web_search", "web_fetch", "image_search"}, fragment.source
    envs = [fragment.env for fragment in fragments]
    assert len(envs) == len(set(envs))


@pytest.mark.parametrize(
    ("label", "keys", "expected_models", "expected_search"),
    [
        ("none", set(), [], "deerflow.community.ddg_search.tools:web_search_tool"),
        ("one", {"GEMINI_API_KEY"}, ["gemini-2.5-pro"], "deerflow.community.ddg_search.tools:web_search_tool"),
        ("several", {"OPENAI_API_KEY", "ANTHROPIC_API_KEY", "TAVILY_API_KEY", "SERPER_API_KEY"}, ["gpt-4", "gpt-5-responses", "claude-sonnet-4"], "deerflow.community.tavily.tools:web_search_tool"),
    ],
)
def test_render_includes_only_present_providers_and_leaves_no_absent_reference(render_config: ModuleType, label: str, keys: set[str], expected_models: list[str], expected_search: str) -> None:
    environ = {**_base_environ(), **{key: "secret" for key in keys}}
    rendered, included = render_config.render_text(TEMPLATE.read_text(encoding="utf-8"), render_config.load_catalog(CATALOG), environ)
    document = yaml.safe_load(rendered)
    assert [model["name"] for model in document["models"]] == expected_models, label
    tools = {tool["name"]: tool for tool in document["tools"]}
    assert tools["web_search"]["use"] == expected_search
    assert {"web_fetch", "image_search", "ls", "read_file", "glob", "grep", "write_file", "str_replace", "bash"} <= set(tools)
    referenced: set[str] = set()
    render_config._references(document, referenced)
    assert referenced == {"DATABASE_URL", *keys}, label
    assert "secret" not in rendered
    assert {fragment.env for fragment in included} == keys
    assert document["sandbox"]["network"]["mode"] == "allowlist"
    assert "provisioner_url" not in document["sandbox"]


def test_render_selects_the_network_block_from_sandbox_egress(render_config: ModuleType) -> None:
    fragments = render_config.load_catalog(CATALOG)
    template = TEMPLATE.read_text(encoding="utf-8")
    allowlist, _ = render_config.render_text(template, fragments, _base_environ())
    assert yaml.safe_load(allowlist)["sandbox"]["network"]["mode"] == "allowlist"
    absent, _ = render_config.render_text(template, fragments, {**_base_environ(), "SANDBOX_EGRESS": ""})
    assert yaml.safe_load(absent)["sandbox"]["network"]["mode"] == "allowlist"
    opened, _ = render_config.render_text(template, fragments, {**_base_environ(), "SANDBOX_EGRESS": "open"})
    assert yaml.safe_load(opened)["sandbox"]["network"] == {"mode": "open"}
    for bad in ("isolated", "ALLOWLIST", "yes"):
        with pytest.raises(render_config.RenderError, match="SANDBOX_EGRESS"):
            render_config.render_text(template, fragments, {**_base_environ(), "SANDBOX_EGRESS": bad})


def test_render_refuses_a_template_reference_to_an_unset_variable(render_config: ModuleType) -> None:
    with pytest.raises(render_config.RenderError, match="DATABASE_URL"):
        render_config.render_text(TEMPLATE.read_text(encoding="utf-8"), render_config.load_catalog(CATALOG), {})


def test_render_output_is_a_valid_app_config(render_config: ModuleType, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from deerflow.config.app_config import AppConfig

    keys = {"OPENAI_API_KEY", "TAVILY_API_KEY"}
    environ = {**_base_environ(), **{key: "secret" for key in keys}}
    for name, value in environ.items():
        monkeypatch.setenv(name, value)
    rendered, _ = render_config.render_text(TEMPLATE.read_text(encoding="utf-8"), render_config.load_catalog(CATALOG), environ)
    path = tmp_path / "config.yaml"
    path.write_text(rendered, encoding="utf-8")
    config = AppConfig.from_file(str(path))
    assert [model.name for model in config.models] == ["gpt-4", "gpt-5-responses"]
    assert config.sandbox.use == "deerflow.community.aio_sandbox:AioSandboxProvider"
    assert config.sandbox.replicas == 2
    assert config.sandbox.network.mode == "allowlist"
    assert config.run_events.backend == "db"


# ── release pinning ──────────────────────────────────────────────────────────


def test_images_txt_lists_exactly_the_references_the_profile_uses(pin_images: ModuleType) -> None:
    references = pin_images.read_references(IMAGES)
    used = pin_images.yaml_references(COMPOSE) + pin_images.yaml_references(TEMPLATE)
    assert sorted(references) == sorted(set(used))
    assert len(references) == 7
    repositories = {pin_images.repository_of(reference) for reference in references}
    assert repositories == {
        "ghcr.io/altakleos/hartmesh-backend",
        "ghcr.io/altakleos/hartmesh-frontend",
        "ghcr.io/altakleos/hartmesh-sandbox",
        "ghcr.io/altakleos/hartmesh-sandbox-network-proxy",
        "postgres",
        "redis",
        "nginx",
    }
    for repository in repositories:
        assert re.fullmatch(r"[a-z0-9./_-]+", repository), repository


def _fake_resolver(calls: list[str]):
    def resolve(reference: str) -> str:
        calls.append(reference)
        import hashlib

        return "sha256:" + hashlib.sha256(reference.encode()).hexdigest()

    return resolve


def test_pin_rewrites_every_reference_to_a_digest_and_check_refuses_tag_form(pin_images: ModuleType, tmp_path: Path) -> None:
    copy = tmp_path / "compose"
    shutil.copytree(PROFILE, copy)
    files = pin_images.ProfileFiles.under(copy)
    if any(pin_images.PINNED_REFERENCE.fullmatch(reference) is None for reference in pin_images.read_references(files.images)):
        with pytest.raises(pin_images.PinError, match="tag-form"):
            pin_images.verify(files)
    calls: list[str] = []
    pinned = pin_images.pin(files, _fake_resolver(calls))
    assert len(pinned) == 7
    assert all(pin_images.PINNED_REFERENCE.fullmatch(reference) for reference in pinned)
    assert files.images.read_text(encoding="utf-8") == "".join(f"{reference}\n" for reference in pinned)
    assert set(pin_images.yaml_references(files.compose)) | set(pin_images.yaml_references(files.config)) == set(pinned)
    assert pin_images.verify(files) == pinned
    assert (copy / "compose.yaml").read_text(encoding="utf-8").count("#") == COMPOSE.read_text(encoding="utf-8").count("#"), "comments survive the rewrite"
    again = pin_images.pin(files, _fake_resolver(calls_again := []))
    assert again == pinned and calls_again == []
    with pytest.raises(pin_images.PinError, match="not a sha256 digest"):
        garbage = tmp_path / "garbage"
        shutil.copytree(PROFILE, garbage)
        pin_images.pin(pin_images.ProfileFiles.under(garbage), lambda reference: "latest")


def test_pin_check_mode_refuses_a_profile_that_disagrees_with_images_txt(pin_images: ModuleType, tmp_path: Path) -> None:
    copy = tmp_path / "compose"
    shutil.copytree(PROFILE, copy)
    files = pin_images.ProfileFiles.under(copy)
    pin_images.pin(files, _fake_resolver([]))
    text = files.compose.read_text(encoding="utf-8")
    files.compose.write_text(text.replace("postgres@sha256:", "postgres:16@sha256:", 1), encoding="utf-8")
    with pytest.raises(pin_images.PinError, match="not a line of"):
        pin_images.verify(files)
    result = subprocess.run([sys.executable, str(REPO_ROOT / "scripts" / "pin_compose_images.py"), "--check", "--profile", str(copy)], capture_output=True, text=True, check=False)
    assert result.returncode == 1 and "pin_compose_images:" in result.stderr


def test_release_workflows_reference_the_compose_profile() -> None:
    manifest = (REPO_ROOT / ".github" / "workflows" / "release-manifest.yaml").read_text(encoding="utf-8")
    assert "deploy/compose/images.txt" in manifest
    assert "scripts/pin_compose_images.py --check" in manifest
    releasing = (REPO_ROOT / "RELEASING.md").read_text(encoding="utf-8")
    assert "scripts/pin_compose_images.py" in releasing
    assert "deploy/compose/images.txt" in releasing
