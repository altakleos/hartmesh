"""File primitives and runtime capture must support either cold import order."""

import subprocess
import sys

import pytest


@pytest.mark.parametrize("first", ["deerflow.files.store", "deerflow.workspace_changes.scanner"])
def test_file_and_workspace_read_modules_import_in_a_fresh_process(first):
    script = "import importlib, sys; importlib.import_module(sys.argv[1]); import deerflow.files.shared_removals; import deerflow.workspace_changes.scanner"
    result = subprocess.run([sys.executable, "-c", script, first], capture_output=True, text=True, timeout=45, check=False)
    assert result.returncode == 0, result.stderr
