import os
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_snapshot_qualification_uses_its_own_dispatch_and_authenticates_the_installed_backend():
    workflow = yaml.safe_load((ROOT / ".github/workflows/compose-instances.yml").read_text())
    inputs = workflow[True]["workflow_dispatch"]["inputs"]
    assert inputs["verify_shared_snapshots"] == {"description": "Interrupt and recover all three publications in a shared conversation", "type": "boolean", "default": False}
    assert "inputs.verify_shared_snapshots && '-shared-snapshots'" in workflow["concurrency"]["group"]
    steps = {s.get("name"): s for s in workflow["jobs"]["bundle-instances"]["steps"]}
    mode = steps["Check snapshot qualification mode"]
    assert mode["if"] == "github.event_name == 'workflow_dispatch' && inputs.verify_shared_snapshots"
    verify = steps["Verify the signed snapshot backend"]
    assert "inputs.backend_image != ''" in verify["if"] and "inputs.verify_shared_snapshots" in verify["if"]
    assert "verify-candidate" in verify["run"] and "deploy/compose/images.txt" in verify["run"] and "--revision" in verify["run"]
    run = steps["Recover each interrupted snapshot publication"]
    assert run["if"] == mode["if"]
    assert run["env"]["HARTMESH_REQUIRE_BUNDLE_INSTANCES"] == "1"
    assert run["env"]["HARTMESH_VERIFY_SHARED_SNAPSHOTS"] == "1"
    assert "test_compose_snapshot_publications.py" in run["run"] and "snapshot-publications.xml" in run["run"]
    assert "inputs.verify_shared_snapshots" in steps["13 Run the acceptance tests"]["if"]


def test_snapshot_only_profile_collects_the_real_mounted_case():
    case = "tests/test_compose_snapshot_publications.py::test_the_same_share_survives_interruption_of_an_output_transcript_and_manifest"
    environment = {**os.environ, "GITHUB_ACTIONS": "true", "HARTMESH_REQUIRE_BUNDLE_INSTANCES": "1", "HARTMESH_VERIFY_SHARED_SNAPSHOTS": "1"}
    args = [sys.executable, "-m", "pytest", "--collect-only", "-q", "-p", "no:cacheprovider", case.split("::")[0]]
    result = subprocess.run(args, cwd=ROOT / "backend", env=environment, capture_output=True, text=True)
    assert result.returncode == 0 and case in result.stdout, result.stdout + result.stderr
