"""Disposable real-Gateway fixture for tenant-presentation-live.spec.ts.

Run with the prepared backend Python. Requires an already loaded, immutable
Gateway image, Docker, and the checkout mounted read-only over that image's app
and harness. This qualifies current source on that runtime, not a new release.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from io import BytesIO
from pathlib import Path

from PIL import Image

ROOT = Path(__file__).resolve().parents[3]
MARKER = "disposable tenant presentation test fixture v1\n"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")).hexdigest()


def docker(*args):
    result = subprocess.run(["docker", *args], text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return (result.stdout + (result.stderr if args[0] == "logs" else "")).strip()


def container_name(directory):
    return "hartmesh-tenant-test-" + hashlib.sha256(str(directory).encode()).hexdigest()[:12]


def stop(directory):
    name = container_name(directory)
    inspect = subprocess.run(["docker", "inspect", name], capture_output=True, text=True)
    if inspect.returncode == 0:
        info = json.loads(inspect.stdout)[0]
        if info["Config"]["Labels"].get("hartmesh.tenant-presentation-test") != str(directory):
            raise RuntimeError("Container ownership mismatch")
        if info["State"]["Running"]:
            docker("stop", "--time", "10", name)
        docker("rm", name)


def prepare(directory):
    directory.mkdir(parents=True, exist_ok=True)
    if any(directory.iterdir()):
        raise ValueError("Fixture directory must be empty")
    (directory / ".tenant-presentation-test").write_text(MARKER)
    for child in ("active", "home", "data", "skills", "product", "product/assets", "profiles"):
        (directory / child).mkdir(exist_ok=True)
    (directory / "extensions.json").write_text('{"mcpServers":{},"skills":{}}')
    for name, color, size in (("product", "navy", (40, 40)), ("provider", "green", (40, 40)), ("icon", "navy", (32, 32))):
        Image.new("RGBA", size, color).save(directory / "product/assets" / (name + ".png"))
    payload = {
        "schema_version": 1,
        "public": True,
        "product": {"name": "Example Product", "logo": "assets/product.png", "browser_icon": "assets/icon.png"},
        "provider": {"name": "Example Provider", "logo": "assets/provider.png", "support_url": "https://support.example.test/help"},
    }
    (directory / "product/product.json").write_text(json.dumps(payload))
    base = {
        "models": [],
        "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
        "database": {"backend": "sqlite", "sqlite_dir": "/qualify/data"},
        "ui": {"profile": "business"},
        "memory": {"enabled": False},
        "skills": {"path": "/qualify/skills"},
    }
    (directory / "base-config.json").write_text(json.dumps(base))
    (directory / "config.yaml").write_text(json.dumps(base))
    # The actual image encoder determines the public product projection to which
    # strict tenant manifests bind. This subprocess is offline and read-only.
    image = os.environ["HARTMESH_TENANT_GATEWAY_IMAGE"]
    if "@sha256:" not in image:
        raise ValueError("An immutable Gateway image digest is required")
    projection = docker(
        "run",
        "--rm",
        "--pull",
        "never",
        "--network",
        "none",
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "-v",
        f"{directory}:/qualify:ro",
        "-v",
        f"{ROOT / 'backend/packages/harness/deerflow'}:/app/backend/packages/harness/deerflow:ro",
        "-v",
        f"{ROOT / 'backend/packages/extension-api/deerflow_extension_api'}:/app/backend/packages/extension-api/deerflow_extension_api:ro",
        "-e",
        "HARTMESH_PRODUCT_PAYLOAD=/qualify/product",
        "-e",
        "DEER_FLOW_CONFIG_PATH=/qualify/config.yaml",
        "-e",
        "DEER_FLOW_EXTENSIONS_CONFIG_PATH=/qualify/extensions.json",
        "--entrypoint",
        "/app/backend/.venv/bin/python",
        image,
        "-m",
        "deerflow.config.product_identity_export",
    )
    product = json.loads(projection)["identity"]
    (directory / "product-projection.json").write_text(json.dumps(product))
    for case, name, color, size in (("A", "Example Tenant One", "red", (1024, 1024)), ("B", "Example Tenant One Revised " + "X" * 53, "blue", (1024, 1)), ("Two", "Example Tenant Two", "purple", (1, 1024)), ("Only", None, "red", (32, 32))):
        folder = directory / "profiles" / case
        folder.mkdir()
        out = BytesIO()
        Image.new("RGBA", size, color).save(out, format="PNG")
        raw = out.getvalue()
        (folder / "logo.png").write_bytes(raw)
        manifest = {"schema_version": 1, "mode": "tenant" if name else "product-only", "product_sha256": digest(product), "tenant": {"name": name, "logo": {"path": "logo.png", "sha256": hashlib.sha256(raw).hexdigest()}} if name else None}
        (folder / "presentation.json").write_text(json.dumps(manifest))
    # Non-presentation resources deliberately share the mount and survive A/B/A.
    (directory / "active/starters.json").write_text(json.dumps([{"id": "starter", "title": "Original starter", "prompt": "Prepare a summary"}]))
    (directory / "active/roles.json").write_text("[]")
    (directory / "active/report-profile.json").write_text('{"independent":"original"}')


def activate(directory, case, port):
    stop(directory)
    active = directory / "active"
    for member in ("presentation.json", "logo.png"):
        target = active / member
        target.unlink(missing_ok=True)
    config = json.loads((directory / "base-config.json").read_text())
    if case != "Default":
        config["tenant_bundle"] = {"path": "/qualify/active"}
    if case not in {"Default", "Legacy"}:
        source_case = case if case in {"A", "B", "Two", "Only"} else "A"
        source = directory / "profiles" / source_case
        for member in ("presentation.json", "logo.png"):
            shutil.copyfile(source / member, active / member)
        manifest = json.loads((active / "presentation.json").read_text())
        if case == "Unsupported":
            manifest["schema_version"] = 0
        if case == "Invalid":
            manifest["unknown"] = True
        if case == "ProductMismatch":
            manifest["product_sha256"] = "0" * 64
        (active / "presentation.json").write_text(json.dumps(manifest))
        config["tenant_bundle"].update(presentation="strict-v1", presentation_sha256=digest(manifest))
        if case == "Missing":
            (active / "presentation.json").unlink()
        if case == "Unreadable":
            (active / "presentation.json").chmod(0)
        if case == "Mismatch":
            shutil.copyfile(directory / "profiles/Two/logo.png", active / "logo.png")
    else:
        shutil.copyfile(directory / "profiles/A/logo.png", active / "logo.png")
        (active / "brand.json").write_text('{"company_name":"Example Legacy Tenant","logo":"logo.png","colors":{"primary":"#123456"}}')
    if case == "B":
        (active / "starters.json").write_text(json.dumps([{"id": "starter", "title": "Revised starter", "prompt": "Prepare a summary"}]))
        (active / "report-profile.json").write_text('{"independent":"revised"}')
    (directory / "config.yaml").write_text(json.dumps(config))
    image = os.environ["HARTMESH_TENANT_GATEWAY_IMAGE"]
    validated = None
    if case in {"A", "B", "Two", "Only", "Default", "Legacy"}:
        validation = [
            "run",
            "--rm",
            "--pull",
            "never",
            "--network",
            "none",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "-v",
            f"{directory}:/qualify:ro",
            "-v",
            f"{ROOT / 'backend/packages/harness/deerflow'}:/app/backend/packages/harness/deerflow:ro",
            "-v",
            f"{ROOT / 'backend/packages/extension-api/deerflow_extension_api'}:/app/backend/packages/extension-api/deerflow_extension_api:ro",
            "-e",
            "DEER_FLOW_CONFIG_PATH=/qualify/config.yaml",
            "-e",
            "DEER_FLOW_EXTENSIONS_CONFIG_PATH=/qualify/extensions.json",
        ]
        if case not in {"Default", "Legacy"}:
            validation += ["-e", "HARTMESH_PRODUCT_PAYLOAD=/qualify/product"]
        validated = json.loads(docker(*validation, "--entrypoint", "/app/backend/.venv/bin/python", image, "-m", "deerflow.config.tenant_presentation"))
    name = container_name(directory)
    secret = "synthetic-qualification-" + hashlib.sha256(str(directory).encode()).hexdigest()
    command = [
        "run",
        "-d",
        "--init",
        "--workdir",
        "/app/backend",
        "--entrypoint",
        "/app/backend/.venv/bin/python",
        "--pull",
        "never",
        "--name",
        name,
        "--label",
        f"hartmesh.tenant-presentation-test={directory}",
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges",
        "-p",
        f"127.0.0.1:{port}:8001",
        "-v",
        f"{directory}:/qualify:ro",
        "-v",
        f"{directory / 'home'}:/qualify/home",
        "-v",
        f"{directory / 'data'}:/qualify/data",
        "-v",
        f"{ROOT / 'backend/app'}:/app/backend/app:ro",
        "-v",
        f"{ROOT / 'backend/packages/harness/deerflow'}:/app/backend/packages/harness/deerflow:ro",
        "-v",
        f"{ROOT / 'backend/packages/extension-api/deerflow_extension_api'}:/app/backend/packages/extension-api/deerflow_extension_api:ro",
        "-e",
        "DEER_FLOW_HOME=/qualify/home",
        "-e",
        "DEER_FLOW_CONFIG_PATH=/qualify/config.yaml",
        "-e",
        "DEER_FLOW_EXTENSIONS_CONFIG_PATH=/qualify/extensions.json",
        "-e",
        "AUTH_JWT_SECRET=" + secret,
        "-e",
        "DEER_FLOW_AUTH_DISABLED=0",
        "-e",
        "UV_CACHE_DIR=/tmp/uv-cache",
    ]
    if case not in {"Default", "Legacy"}:
        command += ["-e", "HARTMESH_PRODUCT_PAYLOAD=/qualify/product"]
    docker(*command, image, "-m", "uvicorn", "app.gateway.app:app", "--host", "0.0.0.0", "--port", "8001")
    try:
        for _ in range(180):
            if docker("inspect", "--format", "{{.State.Running}}", name) != "true":
                raise RuntimeError("Gateway startup failed: " + docker("logs", name))
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1) as response:
                    if response.status == 200:
                        assert docker("exec", name, "/app/backend/.venv/bin/python", "-c", "import os; assert os.statvfs('/qualify/active').f_flag & os.ST_RDONLY; print('readonly')") == "readonly"
                        print(json.dumps(validated), flush=True)
                        return
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(0.2)
        raise RuntimeError("Gateway did not become healthy")
    finally:
        (directory / ("gateway-" + case + ".log")).write_text(docker("logs", name))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "activate", "stop"))
    parser.add_argument("case", nargs="?", choices=("A", "B", "Two", "Only", "Default", "Legacy", "Invalid", "Missing", "Unreadable", "Mismatch", "ProductMismatch", "Unsupported"))
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--port", type=int, default=18111)
    args = parser.parse_args()
    directory = args.directory.resolve()
    if args.command == "prepare":
        prepare(directory)
    else:
        if (directory / ".tenant-presentation-test").read_text() != MARKER:
            raise ValueError("Fixture ownership mismatch")
        if args.command == "stop":
            stop(directory)
        else:
            activate(directory, args.case, args.port)


if __name__ == "__main__":
    main()
