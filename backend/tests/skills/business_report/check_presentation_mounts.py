#!/usr/bin/env python3
"""Real Docker bind/recreation proof. This operator test driver uses Docker;
production report commands remain Python-only and offline inside the sandbox.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import uuid
from pathlib import Path

LABEL = "hartmesh.report-presentation-proof"


def docker(*args, expected=0, include_stderr=False):
    result = subprocess.run(["docker", *map(str, args)], capture_output=True, text=True, encoding="utf-8", timeout=180)
    assert result.returncode == expected, (result.returncode, result.stdout, result.stderr)
    return (result.stdout + result.stderr if include_stderr else result.stdout).strip()


def retained(directory):
    return {str(p.relative_to(directory)): hashlib.sha256(p.read_bytes()).hexdigest() for p in directory.rglob("*") if p.is_file()}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", required=True)
    parser.add_argument("--skill", required=True, type=Path)
    parser.add_argument("--fixtures", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=True)
    assert not any(root.iterdir()), "Use a fresh task-owned output directory"
    output = root / "results"
    output.mkdir(mode=0o777)
    output.chmod(0o777)
    skill = args.skill.resolve()
    fixtures = args.fixtures.resolve()
    profiles = fixtures / "tenant-presentation"
    a = json.loads((profiles / "a.json").read_text(encoding="utf-8"))
    b = json.loads((profiles / "b.json").read_text(encoding="utf-8"))
    product_path = fixtures / "product-identity/one.json"
    product = json.loads(product_path.read_text(encoding="utf-8"))
    image = json.loads(docker("image", "inspect", args.image))[0]
    image_id = image["Id"]
    active = root / "active.json"
    active.write_bytes((profiles / "a.json").read_bytes())
    name = "hm-report-presentation-" + uuid.uuid4().hex[:12]
    owner = str(root)
    container = None

    def stop():
        nonlocal container
        if container is not None:
            info = json.loads(docker("inspect", container))[0]
            assert info["Config"]["Labels"].get(LABEL) == owner
            docker("rm", "--force", container)
            container = None

    def start(selected):
        nonlocal container
        stop()
        container = docker(
            "run",
            "--detach",
            "--init",
            "--network",
            "none",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--user",
            "1000:1000",
            "--label",
            f"{LABEL}={owner}",
            "--name",
            name,
            "--entrypoint",
            "python3",
            "--volume",
            f"{skill}:/mnt/skill:ro",
            "--volume",
            f"{fixtures.parent}:/mnt/qualification:ro",
            "--volume",
            f"{product_path}:/mnt/product/identity.json:ro",
            "--volume",
            f"{active}:/mnt/tenant/presentation.json:ro",
            "--volume",
            f"{output}:/mnt/output",
            "--env",
            "PYTHONDONTWRITEBYTECODE=1",
            "--env",
            "HARTMESH_REPORT_PRODUCT_IDENTITY=/mnt/product/identity.json",
            "--env",
            f"HARTMESH_REPORT_PRODUCT_SHA256={product['sha256']}",
            "--env",
            "HARTMESH_REPORT_TENANT_PRESENTATION=/mnt/tenant/presentation.json",
            "--env",
            f"HARTMESH_REPORT_TENANT_SHA256={selected['snapshot_id']}",
            image_id,
            "-c",
            "import time; time.sleep(1200)",
        )

    def probe():
        code = """import json,os,sys
sys.path.insert(0, '/mnt/skill/scripts')
from business_report_presentation import capture_presentation
p='/mnt/tenant/presentation.json'
assert os.statvfs(p).f_flag & os.ST_RDONLY
try:
    admitted = capture_presentation(None).source_id
except ValueError:
    admitted = None
with open(p, encoding='utf-8') as stream:
    source = json.load(stream)['snapshot_id']
print(json.dumps({'source':source, 'expected':os.environ['HARTMESH_REPORT_TENANT_SHA256'], 'admitted':admitted}))
"""
        return json.loads(docker("exec", container, "python3", "-c", code))

    def command(*args, expected=0, pin=None):
        prefix = ["exec"]
        if pin:
            prefix += ["--env", f"HARTMESH_REPORT_TENANT_SHA256={pin}"]
        text = docker(*prefix, container, "python3", "/mnt/skill/scripts/report.py", *args, expected=expected, include_stderr=True)
        if expected:
            assert "Report presentation" in text and "no new output" in text
        with (root / "commands.log").open("a", encoding="utf-8") as log:
            log.write(text + "\n")
        return text

    def current():
        return docker("exec", container, "python3", "-c", "from pathlib import Path; print(Path('/mnt/output/report/.cache/business-report/report.current.json').read_text())")

    def activate_host(profile):
        staged = root / "next.json"
        staged.write_bytes((profiles / f"{profile}.json").read_bytes())
        os.replace(staged, active)

    try:
        start(a)
        assert probe() == {"source": a["snapshot_id"], "expected": a["snapshot_id"], "admitted": a["snapshot_id"]}
        command("build", "/mnt/qualification/fixtures/example_services_export_small.csv", "--period", "2026-08", "--company", "Example Report Subject", "--out", "/mnt/output/report", "--bundle-id", "a", "--render", "html,pdf")
        old = output / "report/drafts/a"
        before = retained(old)
        old_pointer = current()
        activate_host("b")
        assert json.loads(active.read_text(encoding="utf-8"))["snapshot_id"] == b["snapshot_id"]
        # A real file bind pins the old inode: changed host bytes are not proof
        # that this still-running sandbox has acquired the intended selection.
        stale = probe()
        assert stale["source"] == a["snapshot_id"] and stale["expected"] == a["snapshot_id"]
        assert stale["admitted"] != b["snapshot_id"]
        refused = command("render", "/mnt/output/report/drafts/a/report.report.json", "--to", "html,pdf", "--bundle-id", "stale", pin=b["snapshot_id"], expected=1)
        assert "Rendered" not in refused and current() == old_pointer
        assert retained(old) == before and not (output / "report/drafts/stale").exists()
        start(b)
        assert probe() == {"source": b["snapshot_id"], "expected": b["snapshot_id"], "admitted": b["snapshot_id"]}
        command("render", "/mnt/output/report/drafts/a/report.report.json", "--to", "html,pdf", "--bundle-id", "b")
        revised = output / "report/drafts/b"
        assert b["tenant"]["name"] in (revised / "report.html").read_text(encoding="utf-8")
        assert json.loads((revised / "report.report.json").read_text(encoding="utf-8")) == json.loads((old / "report.report.json").read_text(encoding="utf-8"))
        assert retained(old) == before
        revised_bytes = retained(revised)
        pointer_b = current()
        activate_host("a")
        assert probe()["source"] == b["snapshot_id"]
        command("render", "/mnt/output/report/drafts/b/report.report.json", "--to", "html,pdf", "--bundle-id", "stale-restore", pin=a["snapshot_id"], expected=1)
        assert current() == pointer_b and retained(revised) == revised_bytes
        start(a)
        assert probe() == {"source": a["snapshot_id"], "expected": a["snapshot_id"], "admitted": a["snapshot_id"]}
        command("render", "/mnt/output/report/drafts/b/report.report.json", "--to", "html,pdf", "--bundle-id", "restored-a")
        restored = output / "report/drafts/restored-a"
        assert a["tenant"]["name"] in (restored / "report.html").read_text(encoding="utf-8")
        assert retained(old) == before and retained(revised) == revised_bytes
        result = {
            "image_id": image_id,
            "platform": image["Os"] + "/" + image["Architecture"],
            "read_only_file_bind": True,
            "stale_runtime_refused": True,
            "recreation_and_restoration": True,
            "a": a["snapshot_id"],
            "b": b["snapshot_id"],
            "required_cases_skipped": 0,
        }
        (root / "qualification.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(result), flush=True)
    finally:
        stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
