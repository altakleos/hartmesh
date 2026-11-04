"""
Data Analysis Script using DuckDB.

Analyzes Excel (.xlsx/.xlsm/.xls) and CSV files using DuckDB's in-process SQL engine.
Supports schema inspection, SQL queries, statistical summaries, and result export.

Everything this script imports (duckdb, pandas, openpyxl, xlrd) ships in the
sandbox image built from docker/sandbox/Dockerfile; nothing is installed or
downloaded at runtime, so a fresh sandbox answers its first query without
network access.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import tempfile
from collections import Counter
from collections.abc import Mapping

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

MISSING_DUCKDB_MESSAGE = (
    "duckdb is not installed in this sandbox image. The data-analysis skill needs the sandbox image "
    "built from docker/sandbox/Dockerfile (see backend/docs/CONFIGURATION.md, 'Building a Custom AIO "
    "Sandbox Image'); nothing is installed at runtime."
)

try:
    import duckdb
except ImportError:
    sys.stderr.write(MISSING_DUCKDB_MESSAGE + "\n")
    sys.exit(2)

# Parsed data is cached next to the chat's other working files. The workspace
# is a per-chat bind mount that outlives the sandbox container, whereas the
# sandbox's temporary directory is container-scoped and discarded with it
# (under gVisor it may also be an in-memory tmpfs). The cache sits under
# ``.cache`` so the workspace-change scanner never reports it as an edited
# file. Outside a sandbox (tests, local runs) it falls back to the temporary
# directory.
DEFAULT_WORKSPACE_DIR = "/mnt/user-data/workspace"
CACHE_DIR_NAME = ".cache/data-analysis"
CACHE_DIR_ENV = "DATA_ANALYSIS_CACHE_DIR"
TABLE_MAP_SUFFIX = ".table_map.json"
# Older cache entries in the same chat are pruned down to this many.
CACHE_ENTRIES_KEPT = 3

EXCEL_EXTENSIONS = (".xlsx", ".xlsm", ".xls")


def resolve_cache_dir(
    env: Mapping[str, str] | None = None, workspace_dir: str = DEFAULT_WORKSPACE_DIR
) -> str:
    """Pick the cache directory: explicit override, else the chat workspace, else the temp dir."""
    env = os.environ if env is None else env
    override = env.get(CACHE_DIR_ENV)
    if override:
        return override
    if os.path.isdir(workspace_dir):
        return os.path.join(workspace_dir, CACHE_DIR_NAME)
    return os.path.join(tempfile.gettempdir(), CACHE_DIR_NAME)


CACHE_DIR = resolve_cache_dir()


def compute_files_hash(files: list[str]) -> str:
    """Hash the ordered names and contents that determine the loaded table map."""
    # A new namespace also prevents reuse of maps built by the old allocator.
    hasher = hashlib.sha256(b"data-analysis-inputs-v2\0")
    for file_path in files:
        name = os.fsencode(os.path.basename(file_path))
        hasher.update(len(name).to_bytes(8, "big"))
        hasher.update(name)
        file_hasher = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                while chunk := f.read(8192):
                    file_hasher.update(chunk)
        except OSError:
            # Keep unreadable paths distinct, including POSIX surrogate escapes.
            hasher.update(b"missing\0")
            file_hasher = hashlib.sha256(os.fsencode(file_path))
        else:
            hasher.update(b"readable\0")
        # Fixed-width digests preserve file boundaries, even for empty files.
        hasher.update(file_hasher.digest())
    return hasher.hexdigest()


def get_cache_db_path(files_hash: str) -> str:
    """Get the path to the cached DuckDB database file, creating the cache directory."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, f"{files_hash}.duckdb")


def get_table_map_path(files_hash: str) -> str:
    """Get the path to the cached table map JSON file."""
    return os.path.join(CACHE_DIR, f"{files_hash}{TABLE_MAP_SUFFIX}")


def save_table_map(files_hash: str, table_map: dict[str, str]) -> None:
    """Save the table map atomically alongside the cached DB.

    The map is what marks an entry as complete, so it is written last and in
    one step: a half-written map would make a partial database look valid.
    """
    path = get_table_map_path(files_hash)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(table_map, f, ensure_ascii=False)
    os.replace(tmp_path, path)


def load_table_map(files_hash: str) -> dict[str, str] | None:
    """Load table map from cache. Returns None if not found."""
    path = get_table_map_path(files_hash)
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def discard_cache_entry(files_hash: str) -> None:
    """Remove one cache entry: database, write-ahead log and table map, whichever exist."""
    db_path = os.path.join(CACHE_DIR, f"{files_hash}.duckdb")
    map_path = get_table_map_path(files_hash)
    for path in (db_path, f"{db_path}.wal", map_path, f"{map_path}.tmp"):
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def prune_cache(keep_hash: str) -> None:
    """Keep the newest CACHE_ENTRIES_KEPT entries, always including keep_hash; discard the rest."""
    try:
        names = os.listdir(CACHE_DIR)
    except OSError:
        return
    entries = []
    for name in names:
        if not name.endswith(".duckdb"):
            continue
        files_hash = name[: -len(".duckdb")]
        try:
            mtime = os.path.getmtime(os.path.join(CACHE_DIR, name))
        except OSError:
            continue
        entries.append((files_hash == keep_hash, mtime, files_hash))
    entries.sort(reverse=True)
    for _, _, files_hash in entries[CACHE_ENTRIES_KEPT:]:
        discard_cache_entry(files_hash)
        logger.info(f"Removed an older cache entry ({files_hash[:12]}...)")


def open_cache(files: list[str]) -> tuple[duckdb.DuckDBPyConnection, dict[str, str]]:
    """Return a connection with the files loaded, reusing the chat's cache when it is intact.

    A cache hit reopens the stored database read-only. A miss, or an entry
    whose database is damaged or has no table map (the leftover of an
    interrupted load), is rebuilt from nothing so no partial table survives.
    If the cache directory cannot be used at all, the files are loaded into
    memory for this call only.
    """
    files_hash = compute_files_hash(files)
    db_path: str | None = None
    try:
        db_path = get_cache_db_path(files_hash)
        cached_table_map = load_table_map(files_hash)
        if cached_table_map and os.path.exists(db_path):
            try:
                con = duckdb.connect(db_path, read_only=True)
            except duckdb.Error as e:
                logger.warning(f"Cached database unusable ({e}); rebuilding it")
            else:
                logger.info(f"Cache hit! Using cached database: {db_path}")
                logger.info(
                    f"Loaded {len(cached_table_map)} table(s) from cache: {', '.join(cached_table_map.keys())}"
                )
                return con, cached_table_map
        discard_cache_entry(files_hash)
        con = duckdb.connect(db_path)
    except (OSError, duckdb.Error) as e:
        logger.warning(f"Cache unavailable ({e}); loading files without a cache")
        db_path = None
        con = duckdb.connect(":memory:")

    logger.info("Loading files (first time, will cache for future use)...")
    table_map = load_files(con, files)
    if not table_map:
        logger.error("No tables were loaded. Check file paths and formats.")
        con.close()
        if db_path is not None:
            discard_cache_entry(files_hash)
        sys.exit(1)

    logger.info(f"\nLoaded {len(table_map)} table(s): {', '.join(table_map.keys())}")
    if db_path is not None:
        save_table_map(files_hash, table_map)
        prune_cache(files_hash)
        logger.info(f"Cached database saved to: {db_path}")
    return con, table_map


def sanitize_table_name(name: str) -> str:
    """Sanitize a sheet/file name into a valid SQL table name."""
    sanitized = re.sub(r"[^\w]", "_", name)
    if sanitized and sanitized[0].isdigit():
        sanitized = f"t_{sanitized}"
    return sanitized


def _quote_identifier(name: str) -> str:
    """Quote a SQL identifier, including embedded double quotes."""
    return '"' + name.replace('"', '""') + '"'


def _allocate_table_names(name: str, table_map: dict[str, str]) -> tuple[str, str]:
    """Keep lookup aliases and SQL names unique, including case-only collisions."""
    reserved = {n.casefold() for n in (*table_map.keys(), *table_map.values())}
    base = sanitize_table_name(name)
    table_name = base
    counter = 1
    while table_name.casefold() in reserved:
        table_name = f"{base}_{counter}"
        counter += 1
    # An original name must never redirect an earlier canonical SQL name.
    alias = table_name if name.casefold() in reserved else name
    return alias, table_name


def load_files(con: duckdb.DuckDBPyConnection, files: list[str]) -> dict[str, str]:
    """
    Load Excel/CSV files into DuckDB tables.

    Returns unique lookup aliases mapped to their canonical SQL table names.
    """
    table_map: dict[str, str] = {}

    for file_path in files:
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            continue

        ext = os.path.splitext(file_path)[1].lower()

        if ext in EXCEL_EXTENSIONS:
            _load_excel(con, file_path, table_map)
        elif ext == ".csv":
            _load_csv(con, file_path, table_map)
        else:
            logger.warning(f"Unsupported file format: {ext} ({file_path})")

    return table_map


def _load_excel(
    con: duckdb.DuckDBPyConnection, file_path: str, table_map: dict[str, str]
) -> None:
    """Load all sheets from an Excel file into DuckDB tables.

    pandas picks the reader from the file's content (xlrd for the legacy BIFF
    format, openpyxl for OOXML), so a mislabelled export still loads, and
    DuckDB copies each sheet into a table; no DuckDB extension is needed.
    """
    import pandas as pd

    try:
        sheets = pd.read_excel(file_path, sheet_name=None)
    except Exception as e:
        logger.warning(f"  Failed to read workbook '{file_path}': {e}")
        return

    for sheet_name, frame in sheets.items():
        if frame.shape[1] == 0:
            logger.warning(f"  Skipped empty sheet '{sheet_name}'")
            continue

        alias, table_name = _allocate_table_names(sheet_name, table_map)

        try:
            con.register("sheet_source", frame)
            con.execute(
                f"CREATE TABLE {_quote_identifier(table_name)} AS SELECT * FROM sheet_source"
            )
            con.unregister("sheet_source")
            table_map[alias] = table_name
            row_count = con.execute(
                f"SELECT COUNT(*) FROM {_quote_identifier(table_name)}"
            ).fetchone()[0]
            logger.info(
                f"  Loaded sheet '{sheet_name}' -> table '{table_name}' ({row_count} rows)"
            )
        except Exception as e:
            logger.warning(f"  Failed to load sheet '{sheet_name}': {e}")


def _load_csv(
    con: duckdb.DuckDBPyConnection, file_path: str, table_map: dict[str, str]
) -> None:
    """Load a CSV file into a DuckDB table."""
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    alias, table_name = _allocate_table_names(base_name, table_map)

    try:
        con.execute(
            f"""
            CREATE TABLE {_quote_identifier(table_name)} AS
            SELECT * FROM read_csv_auto(?)
        """,
            [file_path],
        )
        table_map[alias] = table_name
        row_count = con.execute(
            f"SELECT COUNT(*) FROM {_quote_identifier(table_name)}"
        ).fetchone()[0]
        logger.info(
            f"  Loaded CSV '{base_name}' -> table '{table_name}' ({row_count} rows)"
        )
    except Exception as e:
        logger.warning(f"  Failed to load CSV '{base_name}': {e}")


def action_inspect(con: duckdb.DuckDBPyConnection, table_map: dict[str, str]) -> str:
    """Inspect the schema of all loaded tables."""
    output_parts = []

    for original_name, table_name in table_map.items():
        output_parts.append(f"\n{'=' * 60}")
        output_parts.append(f'Table: {original_name} (SQL name: "{table_name}")')
        output_parts.append(f"{'=' * 60}")

        # Get row count
        row_count = con.execute(
            f"SELECT COUNT(*) FROM {_quote_identifier(table_name)}"
        ).fetchone()[0]
        output_parts.append(f"Rows: {row_count}")

        # Get column info
        columns = con.execute(f"DESCRIBE {_quote_identifier(table_name)}").fetchall()
        output_parts.append(f"\nColumns ({len(columns)}):")
        output_parts.append(f"{'Name':<30} {'Type':<15} {'Nullable'}")
        output_parts.append(f"{'-' * 30} {'-' * 15} {'-' * 8}")
        for col in columns:
            col_name, col_type, nullable = col[0], col[1], col[2]
            output_parts.append(f"{col_name:<30} {col_type:<15} {nullable}")

        # Get non-null counts per column
        col_names = [col[0] for col in columns]
        non_null_parts = []
        for c in col_names:
            non_null_parts.append(f"COUNT({_quote_identifier(c)})")
        non_null_sql = (
            f"SELECT {', '.join(non_null_parts)} FROM {_quote_identifier(table_name)}"
        )
        try:
            non_null_counts = con.execute(non_null_sql).fetchone()
            output_parts.append("\nNon-null counts:")
            for i, c in enumerate(col_names):
                output_parts.append(f"  {c}: {non_null_counts[i]} / {row_count}")
        except Exception:
            pass

        # Sample data (first 5 rows)
        output_parts.append("\nSample data (first 5 rows):")
        try:
            sample = con.execute(
                f"SELECT * FROM {_quote_identifier(table_name)} LIMIT 5"
            ).fetchdf()
            output_parts.append(sample.to_string(index=False))
        except Exception:
            sample = con.execute(
                f"SELECT * FROM {_quote_identifier(table_name)} LIMIT 5"
            ).fetchall()
            header = [col[0] for col in columns]
            output_parts.append("  " + " | ".join(header))
            for row in sample:
                output_parts.append("  " + " | ".join(str(v) for v in row))

    result = "\n".join(output_parts)
    print(result)
    return result


_SQL_SKIPPED_SEGMENT = re.compile(
    r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|--[^\n]*|/\*.*?\*/", re.S
)
_BARE_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _quote_bare_multiword_names(sql: str, names: list[str]) -> str:
    """Quote bare sheet/file names that are not plain identifiers (``Payments 2026``).

    Only plain SQL text is touched: string literals, quoted identifiers and
    comments pass through unchanged, so literals and column names keep their
    meaning. Plain identifiers need no help; their temporary views resolve.
    """
    needing = sorted(
        (name for name in names if not _BARE_IDENTIFIER.fullmatch(name)),
        key=len,
        reverse=True,
    )
    if not needing:
        return sql
    pattern = re.compile(
        "|".join(rf"(?<![\w\"]){re.escape(name)}(?![\w\"])" for name in needing)
    )

    def quote(text: str) -> str:
        return pattern.sub(lambda match: _quote_identifier(match.group(0)), text)

    parts, position = [], 0
    for segment in _SQL_SKIPPED_SEGMENT.finditer(sql):
        parts.append(quote(sql[position : segment.start()]))
        parts.append(segment.group(0))
        position = segment.end()
    parts.append(quote(sql[position:]))
    return "".join(parts)


def action_query(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    table_map: dict[str, str],
    output_file: str | None = None,
) -> str:
    """Execute a SQL query and return/export results."""
    try:
        # Let DuckDB resolve identifiers without rewriting SQL literals, column
        # names, comments, or CTEs. Temporary views also work on read-only caches.
        for original_name, table_name in table_map.items():
            if original_name != table_name:
                con.execute(
                    f"CREATE OR REPLACE TEMP VIEW {_quote_identifier(original_name)} "
                    f"AS SELECT * FROM main.{_quote_identifier(table_name)}"
                )
        result = con.execute(_quote_bare_multiword_names(sql, list(table_map)))
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
    except Exception as e:
        error_msg = f"SQL Error: {e}\n\nAvailable tables:\n"
        for orig, tbl in table_map.items():
            cols = con.execute(f"DESCRIBE {_quote_identifier(tbl)}").fetchall()
            col_names = [c[0] for c in cols]
            error_msg += f'  "{tbl}" ({orig}): {", ".join(col_names)}\n'
        print(error_msg)
        return error_msg

    # Format output
    if output_file:
        return _export_results(columns, rows, output_file)

    # Print as table
    return _format_table(columns, rows)


def _format_table(columns: list[str], rows: list[tuple]) -> str:
    """Format query results as a readable table."""
    if not rows:
        msg = "Query returned 0 rows."
        print(msg)
        return msg

    # Calculate column widths
    col_widths = [len(str(c)) for c in columns]
    for row in rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val)))

    # Cap column width
    max_width = 40
    col_widths = [min(w, max_width) for w in col_widths]

    # Build table
    parts = []
    header = " | ".join(str(c).ljust(col_widths[i]) for i, c in enumerate(columns))
    separator = "-+-".join("-" * col_widths[i] for i in range(len(columns)))
    parts.append(header)
    parts.append(separator)
    for row in rows:
        row_str = " | ".join(
            str(v)[:max_width].ljust(col_widths[i]) for i, v in enumerate(row)
        )
        parts.append(row_str)

    parts.append(f"\n({len(rows)} rows)")
    result = "\n".join(parts)
    print(result)
    return result


def _markdown_cell(value: object) -> str:
    """Keep table delimiters and line breaks inside a single Markdown cell."""
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace("|", "\\|")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
        .replace("\n", "<br>")
    )


def _export_results(columns: list[str], rows: list[tuple], output_file: str) -> str:
    """Export query results to a file (CSV, JSON, or Markdown)."""
    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
    ext = os.path.splitext(output_file)[1].lower()

    if ext == ".csv":
        import csv

        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)

    elif ext == ".json":
        duplicates = [name for name, count in Counter(columns).items() if count > 1]
        if duplicates:
            raise ValueError(
                "JSON export requires unique column names; duplicates: "
                + ", ".join(repr(name) for name in duplicates)
                + ". Use SQL aliases (AS) to give each result column a distinct name."
            )
        records = []
        for row in rows:
            record = {}
            for i, col in enumerate(columns):
                val = row[i]
                # Handle non-JSON-serializable types
                if hasattr(val, "isoformat"):
                    val = val.isoformat()
                elif isinstance(val, (bytes, bytearray)):
                    val = val.hex()
                record[col] = val
            records.append(record)
        # Reject non-JSON numbers, including nested values, before touching the
        # destination so a failed export cannot overwrite an existing result.
        serialized = json.dumps(
            records, indent=2, ensure_ascii=False, default=str, allow_nan=False
        )
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(serialized)

    elif ext == ".md":
        with open(output_file, "w", encoding="utf-8") as f:
            # Header
            f.write("| " + " | ".join(_markdown_cell(c) for c in columns) + " |\n")
            f.write("| " + " | ".join("---" for _ in columns) + " |\n")
            # Rows
            for row in rows:
                f.write("| " + " | ".join(_markdown_cell(v) for v in row) + " |\n")
    else:
        msg = f"Unsupported output format: {ext}. Use .csv, .json, or .md"
        print(msg)
        return msg

    msg = f"Results exported to {output_file} ({len(rows)} rows)"
    print(msg)
    return msg


def action_summary(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    table_map: dict[str, str],
) -> str:
    """Generate statistical summary for a table."""
    # Resolve table name
    resolved = _quote_identifier(table_map.get(table_name, table_name))

    try:
        columns = con.execute(f"DESCRIBE {resolved}").fetchall()
    except Exception:
        available = ", ".join(f'"{t}" ({o})' for o, t in table_map.items())
        msg = f"Table '{table_name}' not found. Available tables: {available}"
        print(msg)
        return msg

    row_count = con.execute(f"SELECT COUNT(*) FROM {resolved}").fetchone()[0]

    output_parts = []
    output_parts.append(f"\nStatistical Summary: {table_name}")
    output_parts.append(f"Total rows: {row_count}")
    output_parts.append(f"{'=' * 70}")

    numeric_types = {
        "BIGINT",
        "INTEGER",
        "SMALLINT",
        "TINYINT",
        "DOUBLE",
        "FLOAT",
        "DECIMAL",
        "HUGEINT",
        "REAL",
        "NUMERIC",
    }

    for col in columns:
        col_name, col_type = col[0], col[1].upper()
        quoted_col = _quote_identifier(col_name)
        output_parts.append(f"\n--- {col_name} ({col[1]}) ---")

        # Check base type (strip parameterized parts)
        base_type = re.sub(r"\(.*\)", "", col_type).strip()

        if base_type in numeric_types:
            try:
                stats = con.execute(f"""
                    SELECT
                        COUNT({quoted_col}) as count,
                        AVG({quoted_col})::DOUBLE as mean,
                        STDDEV({quoted_col})::DOUBLE as std,
                        MIN({quoted_col}) as min,
                        QUANTILE_CONT({quoted_col}, 0.25) as q25,
                        MEDIAN({quoted_col}) as median,
                        QUANTILE_CONT({quoted_col}, 0.75) as q75,
                        MAX({quoted_col}) as max,
                        COUNT(*) - COUNT({quoted_col}) as null_count
                    FROM {resolved}
                """).fetchone()
                labels = [
                    "count",
                    "mean",
                    "std",
                    "min",
                    "25%",
                    "50%",
                    "75%",
                    "max",
                    "nulls",
                ]
                for label, val in zip(labels, stats):
                    if isinstance(val, float):
                        output_parts.append(f"  {label:<8}: {val:,.4f}")
                    else:
                        output_parts.append(f"  {label:<8}: {val}")
            except Exception as e:
                output_parts.append(f"  Error computing stats: {e}")
        else:
            try:
                stats = con.execute(f"""
                    SELECT
                        COUNT({quoted_col}) as count,
                        COUNT(DISTINCT {quoted_col}) as unique_count,
                        MODE({quoted_col}) as mode_val,
                        COUNT(*) - COUNT({quoted_col}) as null_count
                    FROM {resolved}
                """).fetchone()
                output_parts.append(f"  count   : {stats[0]}")
                output_parts.append(f"  unique  : {stats[1]}")
                output_parts.append(f"  top     : {stats[2]}")
                output_parts.append(f"  nulls   : {stats[3]}")

                # Show top 5 values
                top_vals = con.execute(f"""
                    SELECT {quoted_col}, COUNT(*) as freq
                    FROM {resolved}
                    WHERE {quoted_col} IS NOT NULL
                    GROUP BY {quoted_col}
                    ORDER BY freq DESC
                    LIMIT 5
                """).fetchall()
                if top_vals:
                    output_parts.append("  top values:")
                    for val, freq in top_vals:
                        pct = (freq / row_count * 100) if row_count > 0 else 0
                        output_parts.append(f"    {val}: {freq} ({pct:.1f}%)")
            except Exception as e:
                output_parts.append(f"  Error computing stats: {e}")

    result = "\n".join(output_parts)
    print(result)
    return result


def main():
    parser = argparse.ArgumentParser(description="Analyze Excel/CSV files using DuckDB")
    parser.add_argument(
        "--files",
        nargs="+",
        required=True,
        help="Paths to Excel (.xlsx/.xlsm/.xls) or CSV files",
    )
    parser.add_argument(
        "--action",
        required=True,
        choices=["inspect", "query", "summary"],
        help="Action to perform: inspect, query, or summary",
    )
    parser.add_argument(
        "--sql",
        type=str,
        default=None,
        help="SQL query to execute (required for 'query' action)",
    )
    parser.add_argument(
        "--table",
        type=str,
        default=None,
        help="Table name for summary (required for 'summary' action)",
    )
    parser.add_argument(
        "--output-file",
        type=str,
        default=None,
        help="Path to export results (CSV/JSON/MD)",
    )
    args = parser.parse_args()

    # Validate arguments
    if args.action == "query" and not args.sql:
        parser.error("--sql is required for 'query' action")
    if args.action == "summary" and not args.table:
        parser.error("--table is required for 'summary' action")

    con, table_map = open_cache(args.files)

    try:
        if args.action == "inspect":
            action_inspect(con, table_map)
        elif args.action == "query":
            action_query(con, args.sql, table_map, args.output_file)
        elif args.action == "summary":
            action_summary(con, args.table, table_map)
    except ValueError as exc:
        sys.exit(f"Error: {exc}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
