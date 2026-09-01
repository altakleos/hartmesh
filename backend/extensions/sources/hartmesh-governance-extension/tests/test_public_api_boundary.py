from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_package_imports_with_only_the_public_extension_api() -> None:
    example_root = Path(__file__).resolve().parents[1]
    extension_api = example_root.parents[1] / "backend" / "packages" / "extension-api"
    script = "; ".join(
        (
            "import sys",
            f"sys.path[:0] = [{str(example_root)!r}, {str(extension_api)!r}]",
            "import hartmesh_governance_extension",
            "assert 'deerflow' not in sys.modules",
            "assert 'app' not in sys.modules",
        )
    )

    result = subprocess.run(
        [sys.executable, "-I", "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
