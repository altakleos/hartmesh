"""The browser rehearsal must bind both running services to the qualified images."""

from pathlib import Path

import pytest
import yaml
from _compose_workspace_file_timing import observe, summary

ROOT = Path(__file__).resolve().parents[2]


def test_signed_and_source_browser_rehearsals_are_distinct_and_bound():
    workflow = yaml.safe_load((ROOT / ".github/workflows/compose-instances.yml").read_text(encoding="utf-8"))
    assert "inputs.verify_file_browser && '-file-browser' || ''" in workflow["concurrency"]["group"]
    steps = workflow["jobs"]["bundle-instances"]["steps"]
    signed = next(step for step in steps if step.get("name") == "Verify the signed workspace browser pair")
    assert "inputs.verify_file_browser" in signed["if"] and "inputs.backend_image != ''" in signed["if"]
    for required in ("deploy/compose/images.txt", "verify-candidate", "crane config", "--revision", "github.token", "FILES_BROWSER_REVISION"):
        assert required in str(signed)
    source = next(step for step in steps if "frontend-hm/Dockerfile" in step.get("run", ""))
    assert "inputs.verify_file_browser" in source["if"] and "inputs.backend_image == ''" in source["if"]
    backend = next(step for step in steps if "docker build" in step.get("run", "") and "--file backend/Dockerfile" in step["run"])
    for build in (source, backend):
        assert "org.opencontainers.image.revision=$GITHUB_SHA" in build["run"] and "org.opencontainers.image.source=https://github.com/$GITHUB_REPOSITORY" in build["run"]
    gate = next(step for step in steps if "test_compose_workspace_files.py" in step.get("run", ""))
    assert gate["if"] == "github.event_name == 'workflow_dispatch' && inputs.verify_file_browser"
    assert "workspace-files.xml" in gate["run"] and "workspace-files.json" in gate["run"]


def test_navigation_percentile_requires_twenty_samples_and_preserves_a_failed_check():
    samples = [float(n) for n in range(20, 0, -1)]
    assert summary(samples) == {"sample_count": 20, "maximum_seconds": 20, "p95_seconds": 19}
    assert summary(samples[:3]) == {"sample_count": 3, "maximum_seconds": 20}
    record = {"folder": {}}
    observe(record, "folder", lambda: None)

    def refused():
        raise AssertionError("folder did not open")

    with pytest.raises(AssertionError, match="did not open"):
        observe(record, "folder", refused)
    assert record["folder"]["sample_count"] == len(record["folder"]["seconds"]) == 1
    assert record["folder"]["failed_after_seconds"] >= 0 and "p95_seconds" not in record["folder"]
