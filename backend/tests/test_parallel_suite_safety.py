"""The offline suite runs one worker per core, so tests must not collide.

`make test` distributes by file (`--dist loadfile`), which is what keeps a
module that owns a process-external resource by a fixed name safe: all of its
tests stay in one worker, in order. What that distribution cannot protect is a
resource two *different* modules would claim at once, and the one such resource
a test process can claim from the operating system is a listening port.

Every listener in the suite today asks the kernel for a free one. This pins
that, because a fixed port is invisible until two workers happen to want it at
the same moment, and then it is an intermittent failure in whichever test lost
the race rather than in the one that took the port.
"""

from __future__ import annotations

import ast
from pathlib import Path

_TESTS = Path(__file__).resolve().parent


def _bind_ports(tree: ast.AST) -> list[ast.AST]:
    """Every literal port passed to a ``.bind((host, port))`` call."""
    found: list[ast.AST] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute) or func.attr != "bind":
            continue
        if not node.args or not isinstance(node.args[0], ast.Tuple):
            continue
        address = node.args[0]
        if len(address.elts) != 2:
            continue
        found.append(address.elts[1])
    return found


def _removes_containers_by_prefix_filter(source: str) -> bool:
    """Whether the module removes Docker containers it did not individually name.

    The signature is a name *filter* feeding a removal: a module that only
    removes the exact names it started (or that asserts on recorded commands
    without running them) names no filter and is not caught here.
    """
    return '"--filter"' in source and '"docker", "rm"' in source


def _declares_xdist_group(tree: ast.AST) -> bool:
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr == "xdist_group":
            return True
    return False


def test_a_module_that_reaps_containers_by_prefix_stays_in_one_worker() -> None:
    """Scatter is per test, so a prefix reaper must say it owns that prefix.

    The autouse cleanup in such a module removes every container matching its
    prefix, including one a sibling test started moments ago. Declaring an
    xdist group is what keeps those siblings sequential in one worker while the
    rest of the suite still scatters.
    """
    offenders: list[str] = []
    for path in sorted(_TESTS.rglob("*.py")):
        if path == Path(__file__).resolve():
            continue
        source = path.read_text(encoding="utf-8")
        if not _removes_containers_by_prefix_filter(source):
            continue
        try:
            tree = ast.parse(source)
        except SyntaxError:  # pragma: no cover - a broken file fails elsewhere
            continue
        if not _declares_xdist_group(tree):
            offenders.append(str(path.relative_to(_TESTS.parent)))

    assert not offenders, f"these remove containers by a name filter rather than by the exact names they started, so parallel siblings would reap each other; declare pytestmark = pytest.mark.xdist_group(...) in them: {offenders}"


def test_no_test_module_binds_a_fixed_port() -> None:
    offenders: list[str] = []
    for path in sorted(_TESTS.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:  # pragma: no cover - a broken file fails elsewhere
            continue
        for port in _bind_ports(tree):
            if isinstance(port, ast.Constant) and port.value == 0:
                continue
            offenders.append(f"{path.relative_to(_TESTS.parent)}:{port.lineno}")

    assert not offenders, f"these bind a port the kernel did not choose, so two parallel workers can collide on it; bind port 0 and read the assigned port back: {offenders}"
