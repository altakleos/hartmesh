"""Missing optional dependencies must not launch package installers."""

import builtins
import runpy
import subprocess
from pathlib import Path

import pytest

SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "skills/public/data-analysis/scripts/analyze.py"
)


@pytest.mark.parametrize("dependency", ["duckdb", "openpyxl"])
def test_missing_dependency_reports_explicit_setup(monkeypatch, capsys, dependency):
    if dependency == "openpyxl":
        pytest.importorskip("duckdb")
    original_import = builtins.__import__

    def missing(name, *args, **kwargs):
        if name == dependency:
            raise ModuleNotFoundError(f"No module named '{dependency}'")
        return original_import(name, *args, **kwargs)

    def forbidden_install(*args, **kwargs):
        pytest.fail("analysis must not launch dependency installers")

    monkeypatch.setattr(builtins, "__import__", missing)
    monkeypatch.setattr(subprocess, "run", forbidden_install)
    if dependency == "openpyxl":
        # HartMesh reads workbooks through pandas, which imports openpyxl only
        # when an OOXML file is loaded: the script itself must still start.
        runpy.run_path(str(SCRIPT))
        return
    # The sandbox image ships duckdb; its absence names the image to use.
    with pytest.raises(SystemExit) as error:
        runpy.run_path(str(SCRIPT))
    assert error.value.code == 2
    assert "duckdb is not installed in this sandbox image" in capsys.readouterr().err
