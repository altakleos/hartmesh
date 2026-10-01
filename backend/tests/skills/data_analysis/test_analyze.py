"""Contracts for the public data-analysis skill script.

The script runs inside the sandbox image, so it has to work with the libraries
that image ships and must never install anything at runtime: a fresh sandbox
would otherwise pay for a download before the first query, and a deployment
whose egress allowlist stops at package indexes would fail outright.
"""

from __future__ import annotations

import ast
import csv
import importlib.util
import logging
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb
import openpyxl
import pytest

from deerflow.workspace_changes.scanner import EXCLUDED_DIR_NAMES

REPO_ROOT = Path(__file__).resolve().parents[4]
SKILL_DIR = REPO_ROOT / "skills" / "public" / "data-analysis"
SCRIPT = SKILL_DIR / "scripts" / "analyze.py"
SKILL_DOC = SKILL_DIR / "SKILL.md"
XLS_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "example_orders.xls"


def _load_script():
    spec = importlib.util.spec_from_file_location("data_analysis_analyze", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    # Never leave a __pycache__ inside the public skill package: the skill reviewer
    # treats every file there as package content.
    previous = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = previous
    return module


@pytest.fixture(scope="module")
def analyze():
    return _load_script()


@pytest.fixture
def con():
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()


@pytest.fixture
def cache_dir(analyze, tmp_path, monkeypatch):
    path = tmp_path / "cache"
    monkeypatch.setattr(analyze, "CACHE_DIR", str(path))
    return path


def _types(con: duckdb.DuckDBPyConnection, table: str) -> dict[str, str]:
    return {row[0]: row[1] for row in con.execute(f'DESCRIBE "{table}"').fetchall()}


def _run_main(analyze, monkeypatch, capsys, *args: str) -> str:
    monkeypatch.setattr(sys, "argv", ["analyze.py", "--files", str(XLS_FIXTURE), *args])
    analyze.main()
    return capsys.readouterr().out


def _write_orders_xlsx(path: Path) -> None:
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "Orders"
    sheet.append(["order_id", "customer", "total"])
    sheet.append([1, "Harbor Cafe", 120.5])
    sheet.append([2, "Pine St Co", 80.0])
    workbook.create_sheet("Empty")
    workbook.save(path)


def test_script_installs_nothing_at_runtime(analyze) -> None:
    source = SCRIPT.read_text(encoding="utf-8")

    assert "subprocess" not in source
    assert re.search(r"\bpip\b", source) is None
    # DuckDB extension installs are downloads too (extensions.duckdb.org).
    assert re.search(r"\bINSTALL\b", source) is None
    assert not hasattr(analyze, "subprocess")


def test_script_parses_as_python_310() -> None:
    """The sandbox image ships Python 3.10; the backend test environment is newer.

    ``feature_version`` rejects newer statements (``except*``) but not every newer
    detail (nested f-string quotes); the smoke workflow running the script on the
    real image is the complete guard.
    """
    ast.parse(SCRIPT.read_text(encoding="utf-8"), feature_version=(3, 10))


def test_missing_duckdb_exits_with_a_message_naming_the_image(tmp_path) -> None:
    shadow = tmp_path / "duckdb"
    shadow.mkdir()
    (shadow / "__init__.py").write_text("raise ImportError('shadowed for the test')\n", encoding="utf-8")
    env = {**os.environ, "PYTHONPATH": str(tmp_path), "PYTHONDONTWRITEBYTECODE": "1"}

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--files", str(XLS_FIXTURE), "--action", "inspect"],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 2
    assert "docker/sandbox/Dockerfile" in result.stderr
    assert "nothing is installed at runtime" in result.stderr
    assert re.search(r"\bpip\b", result.stderr) is None


def test_legacy_xls_workbook_loads_every_sheet_with_types(analyze, con) -> None:
    table_map = analyze.load_files(con, [str(XLS_FIXTURE)])

    assert table_map == {"Orders": "Orders", "Payments 2026": "Payments_2026"}
    assert con.execute('SELECT COUNT(*) FROM "Orders"').fetchone()[0] == 12
    assert con.execute('SELECT COUNT(*) FROM "Payments_2026"').fetchone()[0] == 9

    types = _types(con, "Orders")
    assert types["order_id"] == "BIGINT"
    assert types["amount"] == "DOUBLE"
    assert types["customer"] == "VARCHAR"
    assert types["date"].startswith("TIMESTAMP")

    assert con.execute('SELECT COUNT(*) FROM "Orders" WHERE amount IS NULL').fetchone()[0] == 1
    paid = con.execute("SELECT ROUND(SUM(amount), 2) FROM \"Orders\" WHERE status = 'paid'").fetchone()[0]
    assert paid == 5345.0
    months = con.execute('SELECT DISTINCT DATE_TRUNC(\'month\', "date") FROM "Orders"').fetchall()
    assert len(months) == 1
    joined = con.execute('SELECT COUNT(*) FROM "Orders" o JOIN "Payments_2026" p ON o.order_id = p.order_id').fetchone()[0]
    assert joined == 9


def test_xlsx_workbook_loads_without_a_duckdb_extension(analyze, con, tmp_path) -> None:
    path = tmp_path / "orders.xlsx"
    _write_orders_xlsx(path)

    table_map = analyze.load_files(con, [str(path)])

    # The empty sheet is skipped rather than becoming an empty table.
    assert table_map == {"Orders": "Orders"}
    assert con.execute('SELECT COUNT(*) FROM "Orders"').fetchone()[0] == 2
    assert _types(con, "Orders")["total"] == "DOUBLE"
    loaded = {row[0] for row in con.execute("SELECT extension_name FROM duckdb_extensions() WHERE loaded").fetchall()}
    assert "spatial" not in loaded


def test_workbook_reader_follows_content_not_extension(analyze, con, tmp_path) -> None:
    # An OOXML workbook saved under the legacy extension is a common export shape.
    path = tmp_path / "renamed.xls"
    _write_orders_xlsx(path)

    table_map = analyze.load_files(con, [str(path)])

    assert table_map == {"Orders": "Orders"}
    assert con.execute('SELECT SUM(total) FROM "Orders"').fetchone()[0] == 200.5


def test_csv_file_name_with_an_apostrophe_loads(analyze, con, tmp_path) -> None:
    path = tmp_path / "Ann's orders.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["item", "amount"])
        writer.writerow(["Widget", 240])
        writer.writerow(["Gadget", 1450])

    table_map = analyze.load_files(con, [str(path)])

    assert table_map == {"Ann's orders": "Ann_s_orders"}
    assert con.execute('SELECT SUM(amount) FROM "Ann_s_orders"').fetchone()[0] == 1690


def test_cache_lives_under_the_workspace_cache_dir(analyze, tmp_path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    assert analyze.resolve_cache_dir(env={}, workspace_dir=str(workspace)) == str(workspace / ".cache" / "data-analysis")
    assert analyze.resolve_cache_dir(env={"DATA_ANALYSIS_CACHE_DIR": str(tmp_path / "elsewhere")}, workspace_dir=str(workspace)) == str(tmp_path / "elsewhere")
    fallback = analyze.resolve_cache_dir(env={}, workspace_dir=str(tmp_path / "missing"))
    assert fallback == str(Path(tempfile.gettempdir()) / ".cache" / "data-analysis")
    # The workspace-change scanner prunes this directory, so the cache never shows up as an edited file.
    assert Path(analyze.CACHE_DIR_NAME).parts[0] in EXCLUDED_DIR_NAMES


def test_skill_doc_names_the_same_cache_location(analyze) -> None:
    documented = "/mnt/user-data/workspace/.cache/data-analysis/"

    assert documented in SKILL_DOC.read_text(encoding="utf-8")
    assert f"{analyze.DEFAULT_WORKSPACE_DIR}/{analyze.CACHE_DIR_NAME}/" == documented


def test_main_inspects_then_reuses_the_cache(analyze, cache_dir, monkeypatch, capsys, caplog) -> None:
    # The script's basicConfig is a no-op under pytest (the root logger already has handlers).
    caplog.set_level(logging.INFO)

    inspected = _run_main(analyze, monkeypatch, capsys, "--action", "inspect")
    assert "Rows: 12" in inspected
    assert 'Table: Payments 2026 (SQL name: "Payments_2026")' in inspected

    caplog.clear()
    queried = _run_main(analyze, monkeypatch, capsys, "--action", "query", "--sql", "SELECT status, COUNT(*) AS n FROM Orders GROUP BY status ORDER BY n DESC")
    assert "paid" in queried
    assert "(3 rows)" in queried
    assert "Cache hit" in caplog.text
    assert sorted(p.suffix for p in cache_dir.iterdir()) == [".duckdb", ".json"]


def test_query_accepts_a_sheet_name_quoted_or_bare(analyze, con, capsys) -> None:
    table_map = analyze.load_files(con, [str(XLS_FIXTURE)])
    capsys.readouterr()

    bare = analyze.action_query(con, "SELECT COUNT(*) AS n FROM Payments 2026", table_map)
    quoted = analyze.action_query(con, 'SELECT COUNT(*) AS n FROM "Payments 2026"', table_map)
    sql_name = analyze.action_query(con, 'SELECT COUNT(*) AS n FROM "Payments_2026"', table_map)

    assert "9" in bare and "SQL Error" not in bare
    assert "9" in quoted and "SQL Error" not in quoted
    assert "9" in sql_name and "SQL Error" not in sql_name


def test_interrupted_load_does_not_leave_a_partial_cache(analyze, cache_dir, monkeypatch, capsys) -> None:
    # A database with a table already in it but no table map is what a load cut
    # off part-way leaves behind (the map is written last).
    files_hash = analyze.compute_files_hash([str(XLS_FIXTURE)])
    stale_db = Path(analyze.get_cache_db_path(files_hash))
    stale = duckdb.connect(str(stale_db))
    stale.execute('CREATE TABLE "Orders" AS SELECT 1 AS order_id')
    stale.close()
    stale_wal = Path(f"{stale_db}.wal")
    stale_wal.write_bytes(b"not a real write-ahead log")

    inspected = _run_main(analyze, monkeypatch, capsys, "--action", "inspect")

    assert "Rows: 12" in inspected
    assert not stale_wal.exists()
    queried = _run_main(analyze, monkeypatch, capsys, "--action", "query", "--sql", 'SELECT COUNT(*) FROM "Orders"')
    assert "12" in queried


def test_damaged_cache_entry_is_rebuilt(analyze, cache_dir, monkeypatch, capsys, caplog) -> None:
    caplog.set_level(logging.INFO)
    files_hash = analyze.compute_files_hash([str(XLS_FIXTURE)])
    Path(analyze.get_cache_db_path(files_hash)).write_bytes(b"garbage, not a database")
    analyze.save_table_map(files_hash, {"Orders": "Orders"})

    inspected = _run_main(analyze, monkeypatch, capsys, "--action", "inspect")

    assert "Rows: 12" in inspected
    assert "Cached database unusable" in caplog.text
    assert "Cache hit" in _run_main_log(analyze, monkeypatch, capsys, caplog)


def _run_main_log(analyze, monkeypatch, capsys, caplog) -> str:
    caplog.clear()
    _run_main(analyze, monkeypatch, capsys, "--action", "inspect")
    return caplog.text


def test_unusable_cache_dir_falls_back_to_memory(analyze, tmp_path, monkeypatch, capsys, caplog) -> None:
    caplog.set_level(logging.INFO)
    blocker = tmp_path / "blocker"
    blocker.write_text("a file where the cache directory's parent should be", encoding="utf-8")
    monkeypatch.setattr(analyze, "CACHE_DIR", str(blocker / "cache"))

    inspected = _run_main(analyze, monkeypatch, capsys, "--action", "inspect")

    assert "Rows: 12" in inspected
    assert "Cache unavailable" in caplog.text
    assert not (blocker / "cache").exists()


def test_older_cache_entries_are_pruned(analyze, cache_dir, monkeypatch, capsys) -> None:
    cache_dir.mkdir()
    for index in range(4):
        fake_hash = f"{index:064x}"
        (cache_dir / f"{fake_hash}.duckdb").write_bytes(b"")
        (cache_dir / f"{fake_hash}{analyze.TABLE_MAP_SUFFIX}").write_text("{}", encoding="utf-8")
        stamp = 1_600_000_000 + index * 60
        os.utime(cache_dir / f"{fake_hash}.duckdb", (stamp, stamp))

    _run_main(analyze, monkeypatch, capsys, "--action", "inspect")

    databases = sorted(p.name for p in cache_dir.glob("*.duckdb"))
    assert len(databases) == analyze.CACHE_ENTRIES_KEPT
    assert f"{2:064x}.duckdb" in databases
    assert f"{3:064x}.duckdb" in databases
    assert not (cache_dir / f"{0:064x}{analyze.TABLE_MAP_SUFFIX}").exists()
    assert not (cache_dir / f"{1:064x}{analyze.TABLE_MAP_SUFFIX}").exists()
