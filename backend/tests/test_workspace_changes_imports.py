"""Workspace scanner imports cannot depend on a preloaded worker runtime."""

import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("statement", ["from deerflow.workspace_changes.scanner import scan_workspace_roots", "from deerflow.workspace_changes import capture_output_snapshot"])
def test_workspace_api_imports_in_a_fresh_python_process(statement):
    environment = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    result = subprocess.run([sys.executable, "-c", statement], cwd=Path(__file__).resolve().parents[1], env=environment, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
