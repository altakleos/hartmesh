import os
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_snapshot_qualification_uses_its_own_dispatch_and_authenticates_the_installed_backend():
    workflow = yaml.safe_load((ROOT / ".github/workflows/compose-instances.yml").read_text())
    inputs = workflow[True]["workflow_dispatch"]["inputs"]
    assert inputs["verify_shared_snapshots"] == {"description": "Qualify ordinary and employee conversation sharing, continuation and recovery", "type": "boolean", "default": False}
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
    assert "test_compose_employee_sharing.py" in run["run"] and "agent-sharing.json" in run["run"]
    assert "SNAPSHOT_BROWSER_IMAGE" in verify["run"] and "hartmesh-frontend" in verify["run"]
    assert "inputs.verify_shared_snapshots" in steps["13 Run the acceptance tests"]["if"]


def test_snapshot_only_profile_collects_the_real_mounted_case():
    cases = (
        "tests/test_compose_snapshot_publications.py::test_the_same_share_survives_interruption_of_an_output_transcript_and_manifest",
        "tests/test_compose_employee_sharing.py::test_manager_shares_a_colleagues_words_and_home_output",
    )
    environment = {**os.environ, "GITHUB_ACTIONS": "true", "HARTMESH_REQUIRE_BUNDLE_INSTANCES": "1", "HARTMESH_VERIFY_SHARED_SNAPSHOTS": "1"}
    args = [sys.executable, "-m", "pytest", "--collect-only", "-q", "-p", "no:cacheprovider", *(case.split("::")[0] for case in cases)]
    result = subprocess.run(args, cwd=ROOT / "backend", env=environment, capture_output=True, text=True)
    assert result.returncode == 0 and all(case in result.stdout for case in cases), result.stdout + result.stderr
