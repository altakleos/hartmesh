#!/usr/bin/env python3
"""Turn a tabular export (CSV, XLSX, XLS) into a management report.

Commands (see SKILL.md for the workflow):

    report.py inspect <files…>                       schema, roles, period and currency guesses
    report.py build   <files…> --period P --out DIR  report.json, charts/*.png, checks.json
    report.py show    <report.json>                  the figures, checks and notes, without the rows
    report.py prose   <report.json> --from prose.json  model-written text, numbers verified
    report.py render  <report.json> --to pdf,docx,xlsx   every named format in one run
    report.py checks  <report.json> <files…>         re-run the checks on their own

Everything the renderers show comes from one report.json, so the HTML, PDF,
DOCX and XLSX agree by construction. The script runs on the libraries the
sandbox image ships (docker/sandbox/Dockerfile), installs nothing, never
calls a network service, never shells out and never modifies an input file.
A file is addressed as ``path`` or ``path::Sheet`` for workbooks.
"""

from __future__ import annotations

import argparse
import copy
import csv
import datetime as dt
import hashlib
import io
import json
import math
import os
import re
import sys
import warnings
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation, localcontext
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    import business_report_settings as settings
    import openpyxl  # noqa: F401  (pandas' xlsx engine; asserted so a missing engine fails here, not mid-build)
    import pandas as pd
    from business_report_common import (
        CURRENCY_CODES,
        DEFAULT_PROFILE,
        DEFAULT_REPORTS_DIR,
        DEFAULT_TENANT_DIR,
        EXIT_DECISION_NEEDED,
        EXIT_MISSING_LIBRARY,
        EXIT_OK,
        EXIT_WITHHELD,
        PRESENTED_TARGETS,
        PROFILES_DIR,
        RENDER_TARGETS,
        RENDERS_MANIFEST,
        REPORT_SUFFIX,
        REQUIRED_ROLES,
        ROLES,
        SYMBOL_TO_CODE,
        TARGET_ORDER,
        TEXT_ROLES,
        BuildContext,
        BuildOptions,
        DecisionNeeded,
        InputError,
        LoadedTable,
        Mapping,
        checks_line,
        format_value,
        in_period,
        is_are,
        is_color,
        is_missing,
        load_brand,
        month_period,
        one_line,
        parse_period,
        plural,
        previous_period,
        same_period_last_year,
        slugify,
        suggest_period,
        to_text,
        utc_now,
        vocab,
    )
    from business_report_publish import STATE_DIR, bundle_manifest, current_identity, current_report, publication_lock, publication_root, publish_bundle, require_current, sync_directory, write_json
    from business_report_render import chart_path, draw_charts, fetch_inline_only, render
    from business_report_sections import SECTION_BUILDERS, TABLE_ROW_LIMIT, build_report
except ImportError as error:
    from business_report_common import MISSING_LIBRARY_MESSAGE  # importable without the libraries

    sys.stderr.write(f"{MISSING_LIBRARY_MESSAGE} ({error})\n")
    sys.exit(EXIT_MISSING_LIBRARY if "EXIT_MISSING_LIBRARY" in dir() else 2)

__all__ = ["DEFAULT_REPORTS_DIR", "SECTION_BUILDERS", "TABLE_ROW_LIMIT", "checks_line", "fetch_inline_only", "format_value", "main", "parse_period", "previous_period", "same_period_last_year"]

SAMPLE_ROWS = 200
NUMBER_TOKEN = re.compile(r"(?<![A-Za-z0-9.])([$€£]?)(-?\d{1,3}(?:,\d{3})+(?:\.\d+)?|-?\d+(?:\.\d+)?)([%kK])?(?![A-Za-z0-9])")
SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")
DMY_PATTERN = re.compile(r"^\s*(\d{1,2})[/.\-](\d{1,2})[/.\-](\d{2,4})\b")
CURRENCY_TOKENS = {**SYMBOL_TO_CODE, "US$": "USD", "CA$": "CAD", "A$": "AUD", "NZ$": "NZD", **{code: code for code in CURRENCY_CODES}}
CURRENCY_TOKEN_PATTERN = "(?:" + "|".join(re.escape(token) for token in sorted(CURRENCY_TOKENS, key=len, reverse=True)) + ")"
CURRENCY_TOKEN = re.compile(r"(?<![A-Za-z])" + CURRENCY_TOKEN_PATTERN + r"(?![A-Za-z])", re.IGNORECASE)
DOLLAR_CURRENCIES = {"USD", "CAD", "AUD", "NZD", "MXN", "SGD", "HKD"}
MONEY_CELL = re.compile(
    rf"(?P<leading>[+-]?)\s*(?:{CURRENCY_TOKEN_PATTERN}\s*){{0,2}}(?P<inner>[+-]?)\s*"
    rf"(?P<mantissa>[0-9][0-9., ']*|[.,][0-9]+)(?P<exponent>[eE][+-]?[0-9]+)?\s*"
    rf"(?:{CURRENCY_TOKEN_PATTERN}\s*){{0,2}}(?P<trailing>-?)",
    re.IGNORECASE,
)


# --- profile, preferences ------------------------------------------------------


def load_profile(name_or_path: str | None, tenant_dir: str | None) -> dict:
    """A profile by path, by name from the tenant bundle, or by name from the skill."""

    if name_or_path and name_or_path.endswith(".json"):
        candidates = [Path(name_or_path)]
    else:
        name = name_or_path or DEFAULT_PROFILE
        candidates = []
        if tenant_dir:
            candidates.append(Path(tenant_dir) / "report-profiles" / f"{name}.json")
        candidates.append(PROFILES_DIR / f"{name}.json")
    for candidate in candidates:
        if candidate.is_file():
            profile = json.loads(candidate.read_text(encoding="utf-8"))
            for key in ("name", "vocabulary", "aliases", "sections"):
                if key not in profile:
                    raise InputError(f"Profile {candidate} has no '{key}' key.")
            missing_roles = [role for role in ROLES if role not in profile["aliases"]]
            if missing_roles:
                raise InputError(f"Profile {candidate} has no aliases for: {', '.join(missing_roles)}.")
            profile.setdefault("status_groups", {})
            profile.setdefault("top_n", 10)
            profile.setdefault("title", "{period} Business Review")
            return profile
    raise InputError(f"No report profile named {name_or_path or DEFAULT_PROFILE!r} (looked in {', '.join(str(c) for c in candidates)}).")


def resolve_tenant_dir(tenant_dir: str | None) -> str | None:
    if tenant_dir:
        return tenant_dir
    return DEFAULT_TENANT_DIR if Path(DEFAULT_TENANT_DIR).is_dir() else None


def apply_preferences(options: BuildOptions, prefs: dict) -> tuple[BuildOptions, list[str]]:
    """Merge preferences.json into build options; pure, returns what was applied."""

    prefs = settings.validate(prefs)
    merged = copy.deepcopy(options)
    applied: list[str] = []
    brand = prefs.get("brand")
    if isinstance(brand, dict):
        for key in ("primary", "secondary"):
            if is_color(brand.get(key)):
                merged.brand[key] = brand[key]
                applied.append(f"brand.{key}")
    exclusions = prefs.get("exclusions")
    if isinstance(exclusions, list):
        for index, exclusion in enumerate(exclusions):
            normalized = normalize_exclusion(exclusion)
            if normalized is not None:
                merged.exclusions.append(normalized)
                applied.append(f"exclusions[{index}]")
    if prefs.get("summary_length") in ("short", "standard"):
        merged.summary_length = prefs["summary_length"]
        applied.append("summary_length")
    comparisons = prefs.get("comparisons")
    if isinstance(comparisons, list) and all(item in ("previous_period", "same_period_last_year") for item in comparisons):
        merged.comparisons = list(comparisons)
        applied.append("comparisons")
    charts = prefs.get("charts")
    if isinstance(charts, list) and all(isinstance(item, str) for item in charts):
        merged.charts = list(charts)
        applied.append("charts")
    currency = prefs.get("currency")
    if isinstance(currency, str) and re.fullmatch(r"[A-Z]{3}", currency):
        merged.currency = currency
        applied.append("currency")
    return merged, applied


def normalize_exclusion(value) -> dict | None:
    if isinstance(value, str):
        key, separator, equals = value.partition("=")
        if not separator:
            return None
        key = key.strip()
        target = "role" if key in ROLES else "column"
        return {target: key, "equals": equals.strip()}
    if isinstance(value, dict) and "equals" in value and ("role" in value or "column" in value):
        target = "role" if "role" in value else "column"
        return {target: str(value[target]), "equals": str(value["equals"])}
    return None


# --- reading inputs ------------------------------------------------------------


def parse_source(source: str) -> tuple[Path, str | None]:
    path_text, separator, sheet = source.rpartition("::")
    if separator and path_text:
        return Path(path_text), sheet
    return Path(source), None


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def usable_header(name) -> bool:
    text = to_text(name)
    return bool(text) and re.fullmatch(r"Unnamed: \d+", text) is None


def _named_columns(columns: list[str]) -> list[str]:
    """At most ``MAX_NAMED_COLUMNS`` names, with a count standing for the rest."""
    if len(columns) <= MAX_NAMED_COLUMNS:
        return list(columns)
    return [*columns[:MAX_NAMED_COLUMNS], f"… {len(columns) - MAX_NAMED_COLUMNS} more"]


def _read_csv(path: Path, *, headerless: bool = False) -> pd.DataFrame:
    """The file as a table. ``headerless`` reads every line as data, so a title
    row above the real header cannot decide the shape of the frame: a header of
    one field over rows of three leaves pandas a one-column table, and nothing
    downstream can recover the columns from that."""
    raw = path.read_bytes()
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise InputError(f"{path.name} is not UTF-8 or Windows-1252 text.")
    try:
        delimiter = csv.Sniffer().sniff(text[:8192], delimiters=",;\t|").delimiter
    except csv.Error:
        delimiter = ","
    if headerless:
        # The widest row decides the shape. Without this pandas takes the first
        # line's field count as the width and refuses the rest of the file,
        # which is the shape of every export that opens with a title.
        width = max((len(row) for row in csv.reader(io.StringIO(text), delimiter=delimiter)), default=1)
        names = list(range(width))
    frame = pd.read_csv(
        io.StringIO(text),
        sep=delimiter,
        dtype=str,
        keep_default_na=False,
        skip_blank_lines=True,
        **({"header": None, "names": names} if headerless else {}),
    )
    frame.columns = [str(column) for column in frame.columns]
    return frame


def _read_workbook(path: Path) -> dict[str, pd.DataFrame]:
    try:
        sheets = pd.read_excel(path, sheet_name=None)
    except Exception as error:  # xlrd/openpyxl raise their own families
        raise InputError(f"{path.name} could not be read as a workbook: {error}") from error
    result: dict[str, pd.DataFrame] = {}
    for name, frame in sheets.items():
        frame.columns = [str(column) for column in frame.columns]
        result[str(name)] = frame
    return result


#: How far down a sheet a real header row may sit. An export that opens with a
#: company name and a blank line is ordinary; a header below this is not, and
#: guessing further would start reading data as column names.
MAX_HEADER_SCAN_ROWS = 10

#: How many unreadable amounts a check quotes back. The count says how much
#: revenue moved; an example says which cells to go and fix.
MAX_UNREADABLE_EXAMPLES = 3

#: How many column names a question or a digest line carries. A sheet may hold
#: thousands; a person answering "which column holds the date" is choosing from
#: the front of the file, and the rest would only crowd the turn.
MAX_NAMED_COLUMNS = 40


def reheadered(frame: pd.DataFrame, profile: dict) -> pd.DataFrame | None:
    """The same table read from the header row it actually has, or None.

    An export whose first rows are a title and a blank line reaches pandas with
    ``Unnamed: N`` columns and its real header sitting in the body. The header
    is the first row whose cells are all usable, distinct names *and* which
    resolves the roles the report requires; nothing weaker is accepted, so a
    file that simply has no amount column is still refused by name rather than
    rebuilt on a row that happened to look like a header.
    """
    limit = min(MAX_HEADER_SCAN_ROWS, int(frame.shape[0]))
    for index in range(limit):
        names = [to_text(value) for value in frame.iloc[index]]
        if len(set(names)) != len(names) or not all(usable_header(name) for name in names):
            continue
        candidate = frame.iloc[index + 1 :].reset_index(drop=True)
        candidate.columns = names
        if not suggest_mapping(candidate, profile).missing:
            return candidate
    return None


def read_tables(path: Path, profile: dict) -> tuple[list[tuple[str | None, pd.DataFrame]], list[dict], bool]:
    """Every data table in a file, the sheets skipped and why, and whether it is a workbook."""

    if not path.is_file():
        raise InputError(f"No such file: {path}")
    if path.suffix.lower() in (".csv", ".txt", ".tsv"):
        frame = _read_csv(path)
        if suggest_mapping(frame, profile).missing:
            repaired = reheadered(_read_csv(path, headerless=True), profile)
            if repaired is not None:
                frame = repaired
        return [(None, frame)], [], False
    if path.suffix.lower() not in (".xlsx", ".xlsm", ".xls"):
        raise InputError(f"{path.name}: only .csv, .xlsx, .xlsm and .xls files are supported.")
    tables: list[tuple[str | None, pd.DataFrame]] = []
    skipped: list[dict] = []
    for sheet, frame in _read_workbook(path).items():
        if frame.shape[0] == 0 or frame.shape[1] == 0:
            skipped.append({"sheet": sheet, "reason": "empty"})
            continue
        mapping = suggest_mapping(frame, profile)
        if mapping.missing:
            repaired = reheadered(frame, profile)
            if repaired is None:
                skipped.append({"sheet": sheet, "reason": f"no {' and '.join(mapping.missing)} column"})
                continue
            frame = repaired
        tables.append((sheet, frame))
    return tables, skipped, True


def read_table(source: str, profile: dict | None = None) -> LoadedTable:
    """One data table from ``path`` or ``path::Sheet``."""

    profile = profile or load_profile(None, None)
    path, sheet = parse_source(source)
    tables, skipped, is_workbook = read_tables(path, profile)
    if sheet is not None:
        chosen = [table for table in tables if table[0] == sheet]
        if not chosen:
            reason = next((entry["reason"] for entry in skipped if entry["sheet"] == sheet), None)
            if reason:
                raise InputError(f"Sheet {sheet!r} in {path.name} has no data table ({reason}).")
            names = [table[0] for table in tables] + [entry["sheet"] for entry in skipped]
            raise InputError(f"No sheet named {sheet!r} in {path.name} (sheets: {', '.join(names)}).")
        sheet, frame = chosen[0]
    elif len(tables) == 1:
        sheet, frame = tables[0]
    elif not tables:
        detail = "; ".join(f"{entry['sheet']}: {entry['reason']}" for entry in skipped)
        raise InputError(f"{path.name} has no sheet with a date and an amount column ({detail}).")
    else:
        names = [table[0] for table in tables]
        raise DecisionNeeded(
            f"{path.name} has several sheets with data: {', '.join(names)}. Which one is the export? Pass it as {path.name}::<sheet>.",
            {"file": path.name, "sheets": names},
        )
    return LoadedTable(
        name=path.name,
        path=str(path),
        sheet=sheet,
        frame=frame,
        sha256=sha256_of(path),
        rows=int(frame.shape[0]),
        uploaded=dt.date.fromtimestamp(path.stat().st_mtime).isoformat(),
        is_workbook=is_workbook,
        skipped_sheets=skipped,
    )


# --- column roles --------------------------------------------------------------


def normalize_header(name) -> str:
    return re.sub(r"[^a-z0-9]+", " ", to_text(name).lower()).strip()


def _sample(series: pd.Series) -> list:
    return [value for value in series.head(SAMPLE_ROWS).tolist() if not is_missing(value) and to_text(value) != ""]


def looks_like_dates(series: pd.Series) -> bool:
    if pd.api.types.is_datetime64_any_dtype(series):
        return True
    values = _sample(series)
    if not values:
        return False
    texts = [to_text(value) for value in values]
    if sum(1 for text in texts if re.fullmatch(r"-?\d+(\.\d+)?", text)) > len(texts) / 2:
        return False
    parsed = parse_dates(pd.Series(texts))
    return int(parsed.notna().sum()) >= 0.9 * len(texts)


def looks_numeric(series: pd.Series) -> bool:
    if pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series):
        return True
    values = _sample(series)
    if not values:
        return False
    texts = [to_text(value) for value in values]
    style = detect_number_style(texts)
    parsed = sum(1 for text in texts if not math.isnan(parse_amount(text, style)))
    return parsed >= 0.9 * len(texts)


def suggest_mapping(frame: pd.DataFrame, profile: dict) -> Mapping:
    """Header names first, then value types; two equally good columns are a question, not a guess."""

    columns = [column for column in frame.columns if usable_header(column)]
    normalized = {column: normalize_header(column) for column in columns}
    date_like = {column: looks_like_dates(frame[column]) for column in columns}
    roles: dict[str, str | None] = {role: None for role in ROLES}
    confidence: dict[str, str] = {}
    ambiguous: dict[str, list[str]] = {}
    alternatives: dict[str, list[str]] = {}
    assigned: set[str] = set()

    def free(candidates: list[str]) -> list[str]:
        return [column for column in candidates if column not in assigned]

    for role in ROLES:
        aliases = [normalize_header(alias) for alias in profile["aliases"][role]]
        pool = [column for column in columns if column not in assigned and (role == "date" or not date_like[column])]
        exact = [column for column in pool if normalized[column] in aliases]
        if len(exact) > 1:
            ambiguous[role] = exact
            continue
        if exact:
            roles[role], confidence[role] = exact[0], "high"
            assigned.add(exact[0])
            continue
        partial = [column for column in pool if any(re.search(rf"(^| ){re.escape(alias)}( |$)", normalized[column]) for alias in aliases)]
        if len(partial) > 1:
            ambiguous[role] = partial
            continue
        if partial:
            roles[role], confidence[role] = partial[0], "medium"
            assigned.add(partial[0])

    if roles["date"] is None and "date" not in ambiguous:
        candidates = free([column for column in columns if date_like[column]])
        if len(candidates) == 1:
            roles["date"], confidence["date"] = candidates[0], "low"
            assigned.add(candidates[0])
        elif candidates:
            ambiguous["date"] = candidates
    if roles["amount"] is None and "amount" not in ambiguous:
        candidates = free([column for column in columns if not date_like[column] and looks_numeric(frame[column])])
        if len(candidates) == 1:
            roles["amount"], confidence["amount"] = candidates[0], "low"
            assigned.add(candidates[0])
        elif candidates:
            ambiguous["amount"] = candidates
    for role in ("date", "amount"):
        if roles[role] is not None:
            others = free([column for column in columns if (date_like[column] if role == "date" else looks_numeric(frame[column]))])
            if others:
                alternatives[role] = others
    missing = [role for role in REQUIRED_ROLES if roles[role] is None and role not in ambiguous]
    return Mapping(roles=roles, confidence=confidence, ambiguous=ambiguous, missing=missing, alternatives=alternatives)


def resolve_mapping(frame: pd.DataFrame, profile: dict, override: dict | None, table_name: str) -> Mapping:
    """The suggested mapping with the agent's mapping file applied; an explicit column displaces a guessed one."""

    mapping = suggest_mapping(frame, profile)
    if not override:
        return mapping
    explicit: dict[str, str] = {}
    for role, column in override.items():
        if role not in ROLES:
            raise InputError(f"Mapping names an unknown role {role!r}; roles are {', '.join(ROLES)}.")
        mapping.ambiguous.pop(role, None)
        if column is None:
            mapping.roles[role] = None
            mapping.confidence.pop(role, None)
            continue
        if column not in frame.columns:
            raise InputError(f"Mapping puts {role!r} on column {column!r}, which {table_name} does not have.")
        if column in explicit.values():
            other = next(name for name, taken in explicit.items() if taken == column)
            raise InputError(f"Mapping uses column {column!r} for two roles, {other} and {role}; give one of them another column or null.")
        for other, taken in list(mapping.roles.items()):
            if taken == column and other != role and other not in explicit:
                mapping.roles[other] = None  # the guess yields to the explicit choice
                mapping.confidence.pop(other, None)
        mapping.roles[role] = column
        mapping.confidence[role] = "mapping"
        explicit[role] = column
    mapping.missing = [role for role in REQUIRED_ROLES if mapping.roles[role] is None and role not in mapping.ambiguous]
    return mapping


def mapping_question(mapping: Mapping, table_name: str, profile: dict) -> str | None:
    """One question that covers every open role, so the user is asked once."""

    vocabulary = profile["vocabulary"]

    def describe(role: str) -> str:
        return {"date": "date", "amount": vocabulary.get("amount", "amount")}.get(role, vocabulary.get(role, role))

    parts = [f"the {describe(role)} ({' or '.join(candidates)})" for role, candidates in mapping.ambiguous.items()]
    parts += [f"the {describe(role)} (no column matched)" for role in mapping.missing]
    if not parts:
        return None
    what = "column holds" if len(parts) == 1 else "columns hold"
    return f"Which {what} {', and '.join(parts)} in {table_name}?"


# --- parsing values ------------------------------------------------------------


EU_STRONG = re.compile(r"\d,\d{1,2}$")
EU_WEAK = re.compile(r"\d\.\d{3}$")
US_STRONG = re.compile(r"\d\.(?:\d{1,2}|\d{4,})$")
US_GROUPED = re.compile(r"\d,\d{3}(?:\D|$)")


def _money_parts(value) -> tuple[str, str, str] | None:
    """Validate the whole cell before extracting its sign, mantissa and exponent."""
    text = str(value).strip().replace("−", "-").replace("\u00a0", " ").replace("\u202f", " ").replace("’", "'")
    accounting = text.startswith("(") and text.endswith(")")
    if accounting:
        text = text[1:-1].strip()
    match = MONEY_CELL.fullmatch(text)
    if match is None:
        return None
    signs = [match[name] for name in ("leading", "inner", "trailing") if match[name]]
    if len(signs) + int(accounting) > 1:
        return None
    sign = "-" if accounting or signs == ["-"] else ""
    return sign, match["mantissa"].strip(), match["exponent"] or ""


def detect_number_style(values) -> str | None:
    """Decide once per column whether ',' or '.' is the decimal separator ("us", "eu", or None when nothing says)."""

    eu_strong = us_strong = eu_weak = us_grouped = plain = double_dot = 0
    for value in values:
        parts = _money_parts(value)
        if parts is None or parts[2] or (math.isnan(parse_amount(value, "us")) and math.isnan(parse_amount(value, "eu"))):
            continue
        digits = parts[1].replace(" ", "").replace("'", "")
        if EU_STRONG.search(digits):
            eu_strong += 1
        elif US_STRONG.search(digits):
            us_strong += 1
        elif EU_WEAK.search(digits) and "," not in digits:
            eu_weak += 1
        if US_GROUPED.search(digits) and "." in digits:
            us_grouped += 1
        if re.fullmatch(r"\d+", digits):
            plain += 1
        if re.search(r"\d\.\d{3}\.\d{3}", digits):
            double_dot += 1
    if eu_strong > us_strong + us_grouped:
        return "eu"
    if us_strong or us_grouped:
        return "us"
    # "3.000" alone could be three thousand or three with three decimals; a column
    # that also holds plain integers ("850") or "1.234.567" is grouping thousands,
    # one where every value carries three decimals is not.
    if eu_weak and (double_dot or plain):
        return "eu"
    if eu_weak:
        return "us"
    return None


def _amount_literal(value, style: str | None) -> str | None:
    """A validated decimal literal; separators are removed only after grouping is checked."""
    if is_missing(value) or isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    parts = _money_parts(value)
    if parts is None:
        return None
    sign, mantissa, exponent = parts
    if exponent:
        # Scientific mantissas have a decimal separator, never thousands
        # grouping, even when ordinary money elsewhere uses another locale.
        if re.fullmatch(r"(?:[0-9]+(?:[.,][0-9]+)?|[.,][0-9]+)", mantissa) is None:
            return None
        return sign + mantissa.replace(",", ".") + exponent
    if style is None:
        if "," in mantissa and "." in mantissa:
            style = "eu" if mantissa.rfind(",") > mantissa.rfind(".") else "us"
        elif "," in mantissa:
            style = "us" if re.fullmatch(r"[0-9]{1,3}(?:,[0-9]{3})+", mantissa) else "eu"
        else:
            style = "us"
    decimal, grouping = (",", ".") if style == "eu" else (".", ",")
    integer, separator, fractional = mantissa.partition(decimal)
    if separator and (not fractional or not re.fullmatch(r"[0-9]+", fractional)):
        return None
    # One grouping convention per value, always in groups of three. Arbitrary
    # spaces, repeated separators and embedded signs cannot invent new digits.
    group_marks = [mark for mark in (grouping, " ", "'") if mark in integer]
    if len(group_marks) > 1:
        return None
    if group_marks:
        mark = group_marks[0]
        if not re.fullmatch(r"[0-9]{1,3}(?:" + re.escape(mark) + r"[0-9]{3})+", integer):
            return None
        integer = integer.replace(mark, "")
    elif integer and not re.fullmatch(r"[0-9]+", integer):
        return None
    if not integer and not fractional:
        return None
    return sign + (integer or "0") + ("." + fractional if separator else "") + exponent


def parse_amount(value, style: str | None = None) -> float:
    """A money cell as a float; NaN when it cannot be read (the caller counts those)."""

    literal = _amount_literal(value, style)
    if literal is None:
        return math.nan
    try:
        result = float(literal)
        if not math.isfinite(result) or (result == 0 and Decimal(literal) != 0):
            return math.nan
    except (ValueError, OverflowError, InvalidOperation):
        return math.nan
    return result


def _decimal_from_cell(value, style: str | None = None) -> Decimal | None:
    """A second reading of a money cell with Decimal arithmetic, used only by the reconciliation check."""

    literal = _amount_literal(value, style)
    if literal is None:
        return None
    try:
        parsed = Decimal(literal)
        as_float = float(parsed)
        return parsed if parsed.is_finite() and math.isfinite(as_float) and (as_float != 0 or parsed == 0) else None
    except (InvalidOperation, ValueError, OverflowError):
        return None


def independent_amount_total(raw_values: list, style: str | None = None, *, styles: list[str | None] | None = None) -> float:
    source_styles = styles if styles is not None else [style] * len(raw_values)
    parsed = [amount for value, source_style in zip(raw_values, source_styles, strict=True) if (amount := _decimal_from_cell(value, source_style)) is not None and amount != 0]
    with localcontext() as context:
        if parsed:
            context.prec = max(context.prec, max(amount.adjusted() for amount in parsed) - min(amount.as_tuple().exponent for amount in parsed) + len(str(len(parsed))) + 2)
        total = sum(parsed, Decimal("0"))
    return float(total)


def detect_date_order(texts: list[str]) -> str | None:
    """Day-first or month-first, decided once per column from the numeric d/m/y values in it."""

    first_over_12 = second_over_12 = seen = 0
    for text in texts:
        match = DMY_PATTERN.match(text) if isinstance(text, str) else None
        if not match:
            continue
        seen += 1
        first, second = int(match.group(1)), int(match.group(2))
        if first > 12:
            first_over_12 += 1
        if second > 12:
            second_over_12 += 1
    if not seen:
        return None
    if first_over_12 and not second_over_12:
        return "day-first"
    if second_over_12 and not first_over_12:
        return "month-first"
    if first_over_12 and second_over_12:
        return "mixed"
    return "ambiguous"


def _naive(parsed: pd.Series) -> pd.Series:
    if isinstance(parsed.dtype, pd.DatetimeTZDtype):
        return parsed.dt.tz_convert(None)
    return parsed


def _to_datetime(texts: pd.Series, **kwargs) -> pd.Series | None:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # the inferred-format attempt warns when it falls back; the next attempt handles that
            parsed = pd.to_datetime(texts, errors="coerce", **kwargs)
    except (ValueError, TypeError):
        return None
    if not pd.api.types.is_datetime64_any_dtype(parsed):
        return None  # pandas 2 hands back object dtype for mixed offsets instead of raising
    return _naive(parsed)


def parse_dates(series: pd.Series, order: str | None = None) -> pd.Series:
    """Dates for a whole column with one reading of the day/month order, never one guess per row."""

    if pd.api.types.is_datetime64_any_dtype(series):
        return _naive(pd.to_datetime(series, errors="coerce"))
    texts = series.map(lambda value: to_text(value) or None)
    present = int(texts.notna().sum())
    if present == 0:
        return pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    order = order or detect_date_order([text for text in texts.tolist() if isinstance(text, str)])
    dayfirst = order == "day-first"
    attempts = (
        {"format": "ISO8601"},
        {"format": "ISO8601", "utc": True},
        {"dayfirst": dayfirst},
        {"dayfirst": dayfirst, "utc": True},
        {"format": "mixed", "dayfirst": dayfirst},
        {"format": "mixed", "dayfirst": dayfirst, "utc": True},
    )
    result = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    for kwargs in attempts:
        # A majority format must not hide valid minority dates. Later parsers
        # only fill unresolved cells, preserving earlier ISO/timezone readings.
        unresolved = texts[result.isna() & texts.notna()]
        if unresolved.empty:
            break
        parsed = _to_datetime(unresolved, **kwargs)
        if parsed is None:
            continue
        # pandas 3 may return microseconds for dates outside the nanosecond
        # range. Keep the report's stable datetime64[ns] contract; such values
        # remain unresolved instead of overflowing during the merge.
        valid = parsed.loc[parsed.between(pd.Timestamp.min, pd.Timestamp.max)].astype("datetime64[ns]")
        result.loc[valid.index] = valid
    return result


def _currency_evidence(frame: pd.DataFrame, amount_column: str | None) -> dict[str, str]:
    """All explicit evidence in the amount column and clearly named currency columns."""
    evidence: dict[str, str] = {}
    if amount_column is None:
        return evidence

    def collect(value, source):
        for match in CURRENCY_TOKEN.finditer(to_text(value)):
            token = match[0].upper()
            # A bare dollar sign is ambiguous among dollar currencies. An
            # explicit code, qualified symbol or preference can disambiguate it.
            code = "$" if token == "$" else CURRENCY_TOKENS[token]
            evidence.setdefault(code, source)

    collect(amount_column, "header")
    for value in frame[amount_column]:
        collect(value, "values")
    for column in frame.columns:
        if normalize_header(column) not in ("currency", "currency code"):
            continue
        for value in frame[column]:
            text = to_text(value)
            if not text:
                continue
            if re.fullmatch(CURRENCY_TOKEN_PATTERN, text, re.IGNORECASE) is None:
                raise InputError(f"Unrecognized currency {text!r} in column {column!r}; use a supported currency code before building the report.")
            collect(text, "values")
    return evidence


def _resolve_currency(evidence: dict[str, str], preferred: str | None = None) -> tuple[str, str]:
    evidence = dict(evidence)
    if "$" in evidence:
        explicit = set(evidence) - {"$"}
        dollar_code = next(iter(explicit)) if len(explicit) == 1 and explicit <= DOLLAR_CURRENCIES else preferred if not explicit and preferred in DOLLAR_CURRENCIES else "USD"
        evidence.setdefault(dollar_code, evidence.pop("$"))
    if len(evidence) > 1:
        raise InputError(
            f"Mixed currencies ({', '.join(sorted(evidence))}) in the input amount headers, values or currency columns. "
            "Separate the inputs by currency or convert the source amounts to one currency before building; --currency does not convert amounts."
        )
    if preferred and evidence and preferred not in evidence:
        raise InputError(f"Requested currency {preferred} conflicts with the input currency {next(iter(evidence))}. Convert the source amounts first or choose the input currency; --currency does not convert amounts.")
    if preferred:
        return preferred, "preferences"
    return next(iter(evidence.items())) if evidence else ("USD", "assumed")


def detect_currency(frame: pd.DataFrame, amount_column: str | None) -> tuple[str, str]:
    return _resolve_currency(_currency_evidence(frame, amount_column))


@dataclass
class CleanFrame:
    frame: pd.DataFrame
    unparsed_dates: int
    unparsed_amounts: int
    parsed_amounts: int
    unmapped_columns: list[str]
    number_style: str | None
    date_order: str | None
    ambiguous_date_example: str | None
    #: A few of the amount cells that could not be read, as the file wrote them.
    unparsed_amount_examples: list[str] = field(default_factory=list)


def apply_mapping(frame: pd.DataFrame, roles: dict[str, str | None]) -> CleanFrame:
    """Role columns with parsed values, plus the amount cells as read, for the checks."""

    columns: dict[str, pd.Series] = {}
    date_column, amount_column = roles.get("date"), roles.get("amount")
    if date_column is None or amount_column is None:
        raise InputError("A date column and an amount column are required.")
    date_texts = frame[date_column].map(to_text).tolist() if not pd.api.types.is_datetime64_any_dtype(frame[date_column]) else []
    date_order = detect_date_order(date_texts)
    ambiguous_example = next((text for text in date_texts if DMY_PATTERN.match(text)), None) if date_order == "ambiguous" else None
    dates = parse_dates(frame[date_column], date_order)
    columns["date"] = dates
    style = detect_number_style(_sample(frame[amount_column])) if not pd.api.types.is_numeric_dtype(frame[amount_column]) else None
    amounts = frame[amount_column].map(lambda value: parse_amount(value, style)).astype(float)
    columns["amount"] = amounts
    columns["amount_raw"] = frame[amount_column]
    columns["amount_style"] = pd.Series(style, index=frame.index, dtype=object)
    for role in TEXT_ROLES:
        column = roles.get(role)
        if column is not None:
            columns[role] = frame[column].map(to_text)
    if roles.get("quantity") is not None:
        columns["quantity"] = frame[roles["quantity"]].map(lambda value: parse_amount(value, style)).astype(float)
    clean = pd.DataFrame(columns, index=frame.index)
    non_empty = frame[amount_column].map(lambda value: to_text(value) != "")
    used = {column for column in roles.values() if column}
    unmapped = [column for column in frame.columns if usable_header(column) and column not in used]
    return CleanFrame(
        frame=clean,
        unparsed_dates=int(dates.isna().sum()),
        unparsed_amounts=int((amounts.isna() & non_empty).sum()),
        parsed_amounts=int(amounts.notna().sum()),
        unmapped_columns=unmapped,
        number_style=style,
        date_order=date_order,
        ambiguous_date_example=ambiguous_example,
        unparsed_amount_examples=[to_text(value) for value in frame.loc[amounts.isna() & non_empty, amount_column].map(to_text).drop_duplicates().head(MAX_UNREADABLE_EXAMPLES)],
    )


# --- periods -------------------------------------------------------------------


# --- building the report -------------------------------------------------------


#: A period with no rows names this many of the latest months, each with its row count.
MONTHS_LISTED = 24


def _rows_per_month(dates, counted: str = "") -> str:
    """The rows in each month ``dates`` covers, oldest first; ``counted`` qualifies the heading."""
    counts = dates.dropna().dt.strftime("%Y-%m").value_counts().sort_index()
    if counts.empty:
        return f", no rows {counted}".rstrip() if counted else ""
    listed = counts.iloc[-MONTHS_LISTED:]
    earlier = len(counts) - len(listed)
    heading = " ".join(part for part in ("rows per month", counted) if part)
    if earlier:
        heading += f" (the latest {len(listed)}; {earlier} earlier {plural(earlier, 'month', 'months')} not listed)"
    return f", {heading}: " + ", ".join(f"{month}: {int(count)}" for month, count in listed.items())


def prepare(sources: list[str], period_text: str | None, options: BuildOptions, mapping_override: dict | None, profile: dict, *, resolved_mappings: list[dict] | None = None) -> BuildContext:
    """Read, map, clean and filter the inputs; raises DecisionNeeded when a question is due.

    ``period_text`` of ``None`` means the caller named no period -- the common
    "make me a report from this file" -- and the file answers it: the month
    holding most of its rows. Asking instead would cost the person a round trip
    for a question the data already answers, and the report says in its checks
    which period it chose so they can name another.
    """

    if not sources:
        raise InputError("Give at least one CSV, XLSX or XLS file.")
    profile = copy.deepcopy(profile)
    tables = [read_table(source, profile) for source in sources]
    if resolved_mappings is not None and len(resolved_mappings) != len(tables):
        raise InputError("The saved mappings do not match the number of inputs; rebuild the report.")
    mappings: list[Mapping] = []
    for index, table in enumerate(tables):
        mapping = resolve_mapping(table.frame, profile, resolved_mappings[index] if resolved_mappings is not None else mapping_override, table.name)
        question = mapping_question(mapping, table.name, profile)
        if question:
            # The columns the file does have travel with the question. Without
            # them a missing role reads as "no column matched" and the only way
            # to see what the file holds is a second read of it, which is the
            # round trip this exit exists to avoid.
            raise DecisionNeeded(
                question,
                {
                    "file": table.name,
                    "sheet": table.sheet,
                    "columns": _named_columns([column for column in table.frame.columns if usable_header(column)]),
                    "ambiguous": mapping.ambiguous,
                    "missing": mapping.missing,
                    "mapping": mapping.roles,
                },
            )
        mappings.append(mapping)
    for role, column in (mapping_override or {}).items():
        if isinstance(column, str) and role in ("category", "person", "customer", "source", "status"):
            profile["vocabulary"][role] = column.strip().lower()  # the user's own word for it heads the section
    cleaned = [apply_mapping(table.frame, mapping.roles) for table, mapping in zip(tables, mappings)]
    frames = []
    unmapped: list[str] = []
    for table, clean in zip(tables, cleaned):
        extra = table.frame[clean.unmapped_columns].map(to_text) if clean.unmapped_columns else pd.DataFrame(index=table.frame.index)
        extra.columns = [f"extra:{column}" for column in extra.columns]
        frames.append(pd.concat([clean.frame, extra], axis=1).assign(source_file=table.name))
        for column in clean.unmapped_columns:
            if column not in unmapped:
                unmapped.append(column)
    all_rows = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0].reset_index(drop=True)
    for role in TEXT_ROLES:
        if role in all_rows.columns:
            all_rows[role] = all_rows[role].fillna("").astype(str)
    if period_text is None:
        period = suggest_period(all_rows["date"])
        if period is None:
            raise InputError("No period was given and no row has a usable date, so there is nothing to report on.")
    else:
        period = parse_period(period_text)
    currency_evidence: dict[str, str] = {}
    for table, mapping in zip(tables, mappings):
        for code, source in _currency_evidence(table.frame, mapping.roles["amount"]).items():
            currency_evidence.setdefault(code, source)
    currency, currency_source = _resolve_currency(currency_evidence, options.currency)
    within = in_period(all_rows["date"], period)
    excluded_mask = pd.Series(False, index=all_rows.index)
    exclusion_texts: list[str] = []
    records, record = vocab(profile, "records", "rows"), vocab(profile, "record", "row")
    for exclusion in options.exclusions:
        column = exclusion.get("role") if "role" in exclusion else f"extra:{exclusion.get('column')}"
        if column not in all_rows.columns:
            mapped = [role for role, name in mappings[0].roles.items() if name == exclusion.get("column")]
            column = mapped[0] if mapped else column
        if column not in all_rows.columns:
            raise InputError(f"Exclusion names {exclusion.get('role') or exclusion.get('column')!r}, which the files do not have.")
        matches = (all_rows[column].astype(str).str.strip().str.lower() == str(exclusion["equals"]).strip().lower()) & ~excluded_mask
        counted = matches & within
        label = vocab(profile, exclusion["role"], exclusion["role"]) if "role" in exclusion else exclusion["column"]
        count = int(counted.sum())
        amount = float(all_rows.loc[counted, "amount"].fillna(0).sum())
        exclusion_texts.append(f"Excluded {count} {plural(count, record, records)} where {label} = {exclusion['equals']} ({format_value(amount, 'currency', currency)})")
        excluded_mask |= matches
    excluded_in_period = int((excluded_mask & within).sum())
    kept = all_rows.loc[~excluded_mask].reset_index(drop=True)
    if not int(in_period(kept["date"], period).sum()):
        valid = all_rows["date"].dropna()
        span = f"the files cover {valid.min().strftime('%Y-%m-%d')} to {valid.max().strftime('%Y-%m-%d')}" if len(valid) else "no row has a usable date"
        if excluded_in_period:
            # The period is in the files; the exclusions emptied it. The months
            # listed are the ones another period could still be built from.
            excluded = f"{excluded_in_period} {plural(excluded_in_period, record, records)} in {period.label} {'was' if excluded_in_period == 1 else 'were'} excluded"
            raise InputError(f"No rows are left in {period.label}: {excluded}; {span}{_rows_per_month(kept['date'], 'after the exclusions')}.")
        # The files have no row in the period at all: count every row, as reading the file would.
        raise InputError(f"No rows fall in {period.label}; {span}{_rows_per_month(all_rows['date'])}.")
    date_warnings = []
    for table, clean in zip(tables, cleaned):
        source = f"{table.name} ({table.sheet})" if table.sheet else table.name
        if clean.date_order == "ambiguous" and clean.ambiguous_date_example:
            date_warnings.append(f"{source}: Dates such as {clean.ambiguous_date_example} were read as month/day/year; say if they are day/month/year.")
        elif clean.date_order == "mixed":
            date_warnings.append(f"{source}: The date column mixes day/month and month/day values; rows were read one by one and some may sit in the wrong month.")
    return BuildContext(
        tables=tables,
        mappings=mappings,
        all_rows=kept,
        unmapped_columns=unmapped,
        period=period,
        profile=profile,
        options=options,
        currency=currency,
        currency_source=currency_source,
        number_style=cleaned[0].number_style,
        date_order_warnings=sorted(set(date_warnings)),
        excluded_rows=excluded_in_period,
        unparsed_dates=sum(clean.unparsed_dates for clean in cleaned),
        period_was_given=period_text is not None,
        unparsed_amounts=sum(clean.unparsed_amounts for clean in cleaned),
        unparsed_amount_examples=list(dict.fromkeys(example for clean in cleaned for example in clean.unparsed_amount_examples))[:MAX_UNREADABLE_EXAMPLES],
        parsed_amounts=sum(clean.parsed_amounts for clean in cleaned),
        exclusion_texts=exclusion_texts,
    )


# --- sections ------------------------------------------------------------------


# --- checks --------------------------------------------------------------------


def _like(examples: list[str]) -> str:
    """`` (like "n/a", "see invoice")``, or nothing when there is no example."""
    return f" (like {', '.join(chr(34) + example + chr(34) for example in examples)})" if examples else ""


def _months_beyond_the_files(dates, period) -> list[str]:
    """Whole months of a longer period that lie before the files' first date or after their last.

    Whole months, not days: a file whose last job fell on the 30th covers its
    month, and a warning for that day would be one the user learns to ignore.
    """
    valid = dates.dropna()
    if valid.empty or period.start.replace(day=1) == period.end.replace(day=1):
        return []
    first, last = valid.min().date(), valid.max().date()
    beyond: list[str] = []
    year, month = period.start.year, period.start.month
    while (year, month) <= (period.end.year, period.end.month):
        whole = month_period(year, month)
        if whole.end < first or whole.start > last:
            beyond.append(whole.label)
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return beyond


def _join_words(items: list[str]) -> str:
    return items[0] if len(items) == 1 else ", ".join(items[:-1]) + " and " + items[-1]


def compute_checks(ctx: BuildContext, rows: pd.DataFrame, revenue: float, count: int, sections: list[dict]) -> list[dict]:
    profile, period, currency = ctx.profile, ctx.period, ctx.currency
    records, record = vocab(profile, "records", "rows"), vocab(profile, "record", "row")
    checks: list[dict] = []
    total_rows = sum(table.rows for table in ctx.tables)
    outside = total_rows - count - ctx.unparsed_dates - ctx.excluded_rows
    used_text = (
        f"Used {format_value(count, 'integer')} of {format_value(total_rows, 'integer')} rows: {format_value(outside, 'integer')} {is_are(outside)} outside {period.label}, {format_value(ctx.unparsed_dates, 'integer')} had no usable date"
    )
    used_text += f", {format_value(ctx.excluded_rows, 'integer')} {'was' if ctx.excluded_rows == 1 else 'were'} excluded." if ctx.excluded_rows else "."
    checks.append({"id": "rows_used", "status": "warn" if ctx.unparsed_dates else "pass", "text": used_text})
    if not ctx.period_was_given:
        checks.append({"id": "period_choice", "status": "warn", "text": f"No period was asked for, so this report covers {period.label}, where most of the {records} in the file fall. Say another period to change it."})

    beyond = _months_beyond_the_files(ctx.all_rows["date"], period)
    if beyond:
        dates = ctx.all_rows["date"].dropna()
        run = f"{dates.min().strftime('%Y-%m-%d')} to {dates.max().strftime('%Y-%m-%d')}"
        checks.append({"id": "period_coverage", "status": "warn", "text": f"{period.label} includes {_join_words(beyond)}, which the files do not reach (they run {run}); its figures and comparisons cover the rest of the period only."})

    independent = independent_amount_total(rows["amount_raw"].tolist(), styles=[style if pd.notna(style) else None for style in rows["amount_style"]])
    mismatches = []
    if not math.isclose(independent, revenue, rel_tol=1e-12, abs_tol=1e-9):
        mismatches.append(f"the amount column sums to {format_value(independent, 'currency', currency)}")
    for section in sections:
        table = section.get("table")
        if not table or not table.get("totals") or "Revenue" not in table["columns"] or section["id"] == "customers":
            continue
        index = table["columns"].index("Revenue")
        section_total = sum(row[index] or 0 for row in table["rows"])
        if not math.isclose(section_total, revenue, rel_tol=1e-12, abs_tol=1e-9):
            mismatches.append(f"the {section['heading'].lower()} table sums to {format_value(section_total, 'currency', currency)}")
    if mismatches:
        checks.append({"id": "totals_reconcile", "status": "fail", "text": f"Totals do not match your file: the report says {format_value(revenue, 'currency', currency)} but {', '.join(mismatches)}. The report was withheld."})
    else:
        checks.append({"id": "totals_reconcile", "status": "pass", "text": f"Totals match your file: {format_value(revenue, 'currency', currency)} across {format_value(count, 'integer')} {plural(count, record, records)}."})

    if ctx.date_order_warnings:
        checks.append({"id": "date_order", "status": "warn", "text": " ".join(ctx.date_order_warnings)})

    if "id" in rows.columns:
        ids = rows.loc[rows["id"].str.strip() != "", "id"]
        duplicates = int((ids.value_counts() > 1).sum())
        if duplicates:
            checks.append({"id": "duplicate_ids", "status": "warn", "text": f"{format_value(duplicates, 'integer')} {record} {'ID appears' if duplicates == 1 else 'IDs appear'} more than once."})
        else:
            checks.append({"id": "duplicate_ids", "status": "pass", "text": f"No {record} ID appears more than once."})
    else:
        checks.append({"id": "duplicate_ids", "status": "not_checked", "text": f"No ID column, so duplicate {records} were not checked."})

    unmapped_parts = []
    if "person" in rows.columns:
        unassigned = int((rows["person"] == "Unassigned").sum())
        if unassigned:
            unmapped_parts.append(f"{format_value(unassigned, 'integer')} {plural(unassigned, record, records)} had no {vocab(profile, 'person', 'person')} and {is_are(unassigned)} listed as Unassigned")
    if "category" in rows.columns:
        uncategorized = int((rows["category"] == "Uncategorized").sum())
        if uncategorized:
            unmapped_parts.append(f"{format_value(uncategorized, 'integer')} had no {vocab(profile, 'category', 'category')} and {is_are(uncategorized)} listed as Uncategorized")
    if "person" in rows.columns or "category" in rows.columns:
        if unmapped_parts:
            checks.append({"id": "unmapped_rows", "status": "warn", "text": "; ".join(unmapped_parts) + "."})
        else:
            checks.append({"id": "unmapped_rows", "status": "pass", "text": f"Every {record} has a {vocab(profile, 'person', 'person') if 'person' in rows.columns else vocab(profile, 'category', 'category')}."})

    if ctx.parsed_amounts == 0:
        checks.append({"id": "unparsed_amounts", "status": "warn", "text": f"No row has a usable amount, so every total is {format_value(0, 'currency', currency)}; check the amount column."})
    elif ctx.unparsed_amounts:
        checks.append(
            {
                "id": "unparsed_amounts",
                "status": "warn",
                "text": (
                    f"{format_value(ctx.unparsed_amounts, 'integer')} {plural(ctx.unparsed_amounts, 'row')} had no usable amount "
                    f"and {'counts' if ctx.unparsed_amounts == 1 else 'count'} as {format_value(0, 'currency', currency)}"
                    f"{_like(ctx.unparsed_amount_examples)}."
                ),
            }
        )
    else:
        checks.append({"id": "unparsed_amounts", "status": "pass", "text": "Every row has a usable amount."})

    if ctx.currency_source == "assumed":
        checks.append({"id": "currency", "status": "warn", "text": f"Amounts are assumed to be in {currency}; say if they are not."})
    else:
        origin = {"header": "from the column header", "values": "from the values", "preferences": "from your preferences", "mapping": "from the mapping", "profile": "from the profile"}[ctx.currency_source]
        checks.append({"id": "currency", "status": "pass", "text": f"Amounts are in {currency} ({origin})."})

    if ctx.exclusion_texts:
        checks.append({"id": "exclusions", "status": "pass", "text": "; ".join(ctx.exclusion_texts) + "."})
    if any(table.is_workbook for table in ctx.tables):
        checks.append({"id": "external_links", "status": "not_checked", "text": "External workbook links were not checked."})
    return checks


# --- prose verification --------------------------------------------------------


def _numbers_in(value, found: set[float]) -> None:
    if isinstance(value, bool):
        return
    if isinstance(value, (int, float)):
        if not (isinstance(value, float) and math.isnan(value)):
            found.add(float(value))
    elif isinstance(value, dict):
        for item in value.values():
            _numbers_in(item, found)
    elif isinstance(value, list):
        for item in value:
            _numbers_in(item, found)


def allowed_numbers(report: dict) -> set[float]:
    """The figures a summary may cite: KPIs, tables and checks, the periods named, never individual rows."""

    found: set[float] = set()
    for key in ("kpis", "sections"):
        _numbers_in(report.get(key), found)
    for check in report.get("checks", []):
        for match in NUMBER_TOKEN.finditer(check["text"]):
            found.add(float(match.group(2).replace(",", "")))
    period = parse_period(report["meta"]["period"]["key"])
    for named in (period, previous_period(period), same_period_last_year(period)):
        found.add(float(named.start.year))
        found.add(float(named.end.year))
    found.update(float(day) for day in range(1, min(31, (period.end - period.start).days + 1) + 1))
    found.add(float(report["meta"]["draft"]))
    for entry in report["meta"]["inputs"]:
        found.add(float(entry["rows"]))
    return found


def _token_matches(text: str, suffix: str | None, allowed: set[float]) -> bool:
    value = Decimal(text.replace(",", ""))
    decimals = len(text.split(".")[1]) if "." in text else 0
    quantum = Decimal(1).scaleb(-decimals)
    scale = Decimal(1000) if suffix in ("k", "K") else Decimal(1)
    for candidate in allowed:
        scaled = Decimal(str(candidate)) / scale
        # A delta of -8.2 is written "8.2% below", so the sign is not compared.
        if scaled.quantize(quantum, rounding=ROUND_HALF_UP) == value or (-scaled).quantize(quantum, rounding=ROUND_HALF_UP) == value:
            return True
    return False


def verify_prose_numbers(report: dict, text: str) -> tuple[str, list[str]]:
    """Keep only sentences whose every number is in the report; return the text and what was removed."""

    allowed = allowed_numbers(report)
    kept: list[str] = []
    removed: list[str] = []
    for sentence in SENTENCE_SPLIT.split(text.strip()):
        if not sentence:
            continue
        bad = [match.group(0) for match in NUMBER_TOKEN.finditer(sentence) if not _token_matches(match.group(2), match.group(3), allowed)]
        if bad:
            removed.extend(bad)
        else:
            kept.append(sentence)
    return " ".join(kept), removed


def apply_prose(report: dict, summary: list[str] | None, actions: list[str] | None) -> tuple[dict, list[str], list[str]]:
    """Model-written text into the report, numbers verified; pure. Returns the report, what was removed, and notes."""

    updated = copy.deepcopy(report)
    removed_all: list[str] = []
    notes: list[str] = []
    sections = {section["id"]: section for section in updated["sections"]}
    if summary is not None:
        paragraphs = []
        for paragraph in summary:
            clean, removed = verify_prose_numbers(updated, paragraph)
            removed_all += removed
            if clean:
                paragraphs.append(clean)
        section = sections.get("summary")
        if section is None:
            section = {"id": "summary", "heading": "Summary"}
            updated["sections"].insert(0, section)
        if paragraphs:
            section["paragraphs"] = paragraphs
        else:
            notes.append("No sentence of the new summary survived the number check; kept the built summary.")
    if actions is not None:
        bullets = []
        for bullet in actions:
            clean, removed = verify_prose_numbers(updated, bullet)
            removed_all += removed
            if clean and not removed:
                bullets.append(clean)
        section = sections.get("actions")
        if section is None:
            section = {"id": "actions", "heading": "What to act on"}
            updated["sections"].append(section)
        if bullets:
            section["bullets"] = bullets
        else:
            notes.append("No action survived the number check; kept the built actions.")
    if removed_all:
        check = {"id": "prose_numbers", "status": "warn", "text": f"Removed {len(removed_all)} {plural(len(removed_all), 'number')} from the written text that {is_are(len(removed_all))} not in the report: {', '.join(removed_all)}."}
    else:
        check = {"id": "prose_numbers", "status": "pass", "text": "Every number in the written text matches the report."}
    updated["checks"] = [entry for entry in updated["checks"] if entry["id"] != "prose_numbers"] + [check]
    updated["meta"]["draft"] = int(updated["meta"]["draft"]) + 1
    updated["meta"]["generated_at"] = utc_now()
    return updated, removed_all, notes


# --- inspect and show ----------------------------------------------------------


def inspect_sources(sources: list[str], profile: dict) -> dict:
    files = []
    question = None
    for source in sources:
        path, sheet = parse_source(source)
        tables, skipped, is_workbook = read_tables(path, profile)
        if sheet is not None:
            tables = [table for table in tables if table[0] == sheet]
        entries = []
        for sheet_name, frame in tables:
            mapping = suggest_mapping(frame, profile)
            date_texts = frame[mapping.roles["date"]].map(to_text).tolist() if mapping.roles["date"] and not pd.api.types.is_datetime64_any_dtype(frame[mapping.roles["date"]]) else []
            date_order = detect_date_order(date_texts)
            dates = parse_dates(frame[mapping.roles["date"]], date_order) if mapping.roles["date"] else pd.Series(dtype="datetime64[ns]")
            suggestion = suggest_period(dates)
            valid = dates.dropna()
            columns = []
            for column in frame.columns:
                if not usable_header(column):
                    continue
                series = frame[column]
                kind = "date" if looks_like_dates(series) else "number" if looks_numeric(series) else "text"
                columns.append({"name": column, "type": kind, "sample": [to_text(value) for value in _sample(series)[:3]], "distinct": int(series.map(to_text).nunique())})
            entries.append(
                {
                    "sheet": sheet_name,
                    "rows": int(frame.shape[0]),
                    "columns": columns,
                    "mapping": mapping.roles,
                    "confidence": mapping.confidence,
                    "ambiguous": mapping.ambiguous,
                    "missing": mapping.missing,
                    "alternatives": mapping.alternatives,
                    "period_suggestion": {"key": suggestion.key, "label": suggestion.label, "rows": int(in_period(dates, suggestion).sum())} if suggestion else None,
                    "date_range": {"start": valid.min().strftime("%Y-%m-%d"), "end": valid.max().strftime("%Y-%m-%d")} if len(valid) else None,
                    "date_order": date_order,
                    "months": {str(key): int(value) for key, value in sorted(valid.dt.to_period("M").value_counts().items())} if len(valid) else {},
                    "currency": dict(zip(("code", "source"), detect_currency(frame, mapping.roles["amount"]))),
                }
            )
            if question is None:
                question = mapping_question(mapping, f"{path.name}::{sheet_name}" if sheet_name else path.name, profile)
        if question is None and len(entries) > 1:
            question = f"{path.name} has several sheets with data: {', '.join(entry['sheet'] for entry in entries)}. Which one is the export?"
        files.append({"name": path.name, "path": str(path), "sha256": sha256_of(path), "uploaded": dt.date.fromtimestamp(path.stat().st_mtime).isoformat(), "workbook": is_workbook, "tables": entries, "skipped_sheets": skipped})
    return {"profile": profile["name"], "files": files, "question": question}


def show_report(report: dict) -> str:
    """The figures, tables, checks and notes as text for the agent; the rows stay in the file."""

    currency = report["meta"]["currency"]["code"]
    meta = report["meta"]
    lines = [f"{meta['title']} (draft {meta['draft']}, {meta['period']['label']}, {currency})"]
    for kpi in report["kpis"]:
        delta = kpi.get("delta")
        change = f" ({'+' if (delta['pct'] or 0) > 0 else ''}{format_value(delta['pct'], 'percent')} vs {delta['vs']})" if delta and delta.get("pct") is not None else ""
        lines.append(f"  {kpi['label']}: {format_value(kpi['value'], kpi['format'], currency)}{change}")
    for section in report["sections"]:
        lines.append("")
        lines.append(section["heading"])
        for paragraph in section.get("paragraphs", []):
            lines.append(f"  {paragraph}")
        for bullet in section.get("bullets", []):
            lines.append(f"  - {bullet}")
        table = section.get("table")
        if table:
            lines.append("  " + " | ".join(table["columns"]))
            for row_index, row in enumerate(table["rows"][:12]):
                lines.append("  " + " | ".join(format_value(value, table["formats"][index] if not table.get("row_formats") else table["row_formats"][row_index][index], currency) for index, value in enumerate(row)))
            if len(table["rows"]) > 12:
                lines.append(f"  … {len(table['rows']) - 12} more rows in the file")
            if table.get("totals"):
                lines.append("  " + " | ".join(format_value(value, table["formats"][index], currency) for index, value in enumerate(table["totals"])))
    lines.append("")
    lines.append(f"Checks: {checks_line(report)}")
    for check in report["checks"]:
        lines.append(f"  [{check['status']}] {check['text']}")
    lines.append("")
    lines.append("Not included: " + (" ".join(report["notes"]) if report["notes"] else "nothing; every section the profile lists is in the report."))
    inputs = ", ".join(f"{entry['name']} (uploaded {entry['uploaded']}, {entry['rows']} rows)" for entry in meta["inputs"])
    lines.append(f"Inputs: {inputs}")
    # What the report did not read. The rows table carries these columns, but
    # `show` never prints rows, so without this line the only way to learn that
    # a `Treatment` column existed is to read the file again.
    unused = meta.get("build", {}).get("unmapped") or []
    if unused:
        lines.append("Unused columns: " + ", ".join(_named_columns(unused)))
    # A cell, a column name or a profile word must not be able to start a line:
    # the model is told to act on whole lines of this digest.
    return "\n".join(one_line(line) for line in lines)


# --- command line --------------------------------------------------------------


def _report_base_name(out_dir: Path, override: str | None) -> str:
    """The report is named after its directory, so every path a run will write
    is known before the run: the model names them under the bash tool's
    ``present`` argument in the same call. ``--name`` overrides."""

    if override:
        return slugify(override)
    return slugify(out_dir.resolve().name) or "report"


def _existing_report(path: Path) -> dict | None:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        int(data["meta"]["draft"])
        return data
    except (ValueError, KeyError, TypeError, json.JSONDecodeError):
        return None


def _write_json(path: Path, data) -> None:
    write_json(path, data)


def _load_json_file(path: str, what: str) -> dict:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise InputError(f"Could not read {what} {path}: {error}") from error
    if not isinstance(data, dict):
        raise InputError(f"{what} {path} must hold a JSON object.")
    return data


def _load_report(path: Path) -> dict:
    report = _load_json_file(str(path), "report")
    if report.get("version") != 1 or not isinstance(report.get("meta"), dict):
        raise InputError(f"{path.name} is not a version 1 report produced by this skill.")
    return report


def parse_targets(values: list[str] | None, flag: str) -> list[str]:
    """`pdf,docx,xlsx`, repeated flags, or `all`; validated before anything is written."""

    named: list[str] = []
    for value in values or []:
        for item in str(value).split(","):
            item = item.strip().lower()
            if not item:
                continue
            if item == "all":
                named.extend(PRESENTED_TARGETS)
            else:
                named.append(item)
    unknown = [item for item in named if item not in RENDER_TARGETS]
    if unknown:
        raise InputError(f"{flag} does not know {', '.join(sorted(set(unknown)))}; use {', '.join(RENDER_TARGETS)}, a comma-separated list of them, or all.")
    return [target for target in TARGET_ORDER if target in named]


def _render_base(report_path: Path) -> tuple[Path, str]:
    name = report_path.name
    return report_path.parent, name[: -len(REPORT_SUFFIX)] if name.endswith(REPORT_SUFFIX) else report_path.stem


def _render_name(report_path: Path, target: str) -> str:
    return f"{_render_base(report_path)[1]}.{target}"


def read_render_manifest(report_path: Path) -> list[str]:
    """The renders this skill recorded writing beside *report_path*.

    Ownership is recorded rather than inferred from the file name, because the
    name says nothing about who wrote the file. A report directory with no
    manifest — one this skill has not rendered into, or a bundle the user
    re-uploaded — owns nothing, so nothing in it is ever removed.
    """

    directory, base = _render_base(report_path)
    path = directory / RENDERS_MANIFEST
    if not path.is_file():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    if not isinstance(data, dict) or data.get("base") != base:
        return []
    names = data.get("files")
    if not isinstance(names, list):
        return []
    known = [f"{base}.{target}" for target in TARGET_ORDER]
    return [name for name in known if name in names]


def unowned_renders(report_path: Path) -> list[str]:
    """Files named like renders of this report that this skill did not write."""

    directory, base = _render_base(report_path)
    owned = set(read_render_manifest(report_path))
    return [f"{base}.{target}" for target in TARGET_ORDER if f"{base}.{target}" not in owned and (directory / f"{base}.{target}").is_file()]


def print_unowned_renders(report_path: Path) -> None:
    """Warn about a file named like one of this report's renders that this skill did not write."""

    for name in unowned_renders(report_path):
        print(f"Note: {name} sits beside this report but was not written by it; it may show a different draft.")


def print_rendered(report_path: Path, targets: list[str]) -> None:
    view_path = report_path.parent / f"{report_path.name.removesuffix(REPORT_SUFFIX)}.view.json"
    if view_path.is_file():
        print(f"Preview: {view_path}")
    else:
        print(f"Preview unavailable; ordinary report: {report_path}")
    for target in targets:
        print(f"Rendered {target}: {report_path.parent / _render_name(report_path, target)}")


def command_inspect(args) -> int:
    tenant_dir = resolve_tenant_dir(args.tenant)
    profile = load_profile(args.profile, tenant_dir)
    print(json.dumps(inspect_sources(args.files, profile), indent=2, ensure_ascii=False))
    return EXIT_OK


def command_build(args) -> int:
    tenant_dir = resolve_tenant_dir(args.tenant)
    profile = load_profile(args.profile, tenant_dir)
    options = BuildOptions(brand=load_brand(tenant_dir))
    applied: list[str] = []
    snapshot = settings.read(args.prefs) if args.prefs else None
    if snapshot:
        if snapshot["revision"] == "missing":
            raise InputError("The specified preferences file is missing; use preferences save first or omit --prefs.")
        options, applied = apply_preferences(options, snapshot["preferences"])
    override = None
    if args.prefs_override:
        override = settings.read(args.prefs_override)
        if override["revision"] == "missing":
            raise InputError("The temporary preferences override file is missing.")
        # Fields and collections replace saved choices for this build only.
        effective = {**(snapshot["preferences"] if snapshot else {"version": 1}), **override["preferences"]}
        options, applied = apply_preferences(BuildOptions(brand=load_brand(tenant_dir)), effective)
    for text in args.exclude or []:
        exclusion = normalize_exclusion(text)
        if exclusion is None:
            raise InputError(f"Exclusion {text!r} must look like role=value or column=value.")
        options.exclusions.append(exclusion)
    options.title = args.title
    options.company = args.company
    options.currency = args.currency or options.currency
    options.draft = args.draft
    options.name = args.name
    if args.short:
        options.summary_length = "short"
    if args.summary_length:
        options.summary_length = args.summary_length
    mapping_override = _load_json_file(args.mapping, "mapping file") if args.mapping else None
    targets = parse_targets(args.render, "--render")
    ctx = prepare(args.files, args.period, options, mapping_override, profile)
    out_dir = Path(args.out)
    base = _report_base_name(out_dir, options.name)
    with publication_lock(out_dir, base):
        previous_path = current_report(out_dir, base, verify=False)
        previous = _existing_report(previous_path) if previous_path else None
        if previous and previous["meta"]["period"]["key"] != ctx.period.key:
            raise InputError(f"{out_dir} holds the {previous['meta']['period']['label']} report; build {ctx.period.label} into its own --out directory.")
        expected = current_identity(out_dir, base, verify=False)
    report, charts = build_report(ctx, int(previous["meta"]["draft"]) if previous else 0, compute_checks)
    report["meta"]["preferences_applied"] = applied
    if any(check["status"] == "fail" for check in report["checks"]):
        _write_json(out_dir / "checks.json", report["checks"])
        failed = next(check for check in report["checks"] if check["status"] == "fail")
        sys.stderr.write(f"Report withheld: {failed['text']}\n")
        return EXIT_WITHHELD

    def stage_charts(stage):
        draw_charts(charts, stage, options.brand, ctx.currency)
        if snapshot or override:
            # Supporting bundle member; report and passive-view schemas stay unchanged.
            write_json(
                stage / "preferences-used.json",
                {
                    "source": args.prefs,
                    **(snapshot or {"preferences": {"version": 1}, "revision": "missing", "sha256": None}),
                    "override_source": args.prefs_override,
                    "override": override,
                    "effective": effective if args.prefs_override else snapshot["preferences"],
                    "cli_summary_length": args.summary_length or ("short" if args.short else None),
                    "cli_currency": args.currency,
                    "cli_exclusions": args.exclude or [],
                },
            )

    report_path = publish_bundle(out_dir, base, report, targets, tenant_dir, render, stage_charts, bundle_id=args.bundle_id, expected_current=expected, verify_current=False)
    print(f"Built draft {report['meta']['draft']}: {report['meta']['title']} -> {report_path}")
    if previous and any(check["id"] == "prose_numbers" for check in previous.get("checks", [])):
        print("The previous draft carried written text (summary or actions); this rebuild replaced it with the computed text. Run prose again if it still applies.")
    # The figures the next step is written from, so reading them is not a second
    # run: this is exactly what `show` prints, and the rows stay in the file.
    print()
    print(show_report(report))
    if report["charts"]:
        print("Charts: " + ", ".join(chart["png"] for chart in report["charts"]))
    print_rendered(report_path, targets)
    if previous_path:
        print_unowned_renders(previous_path)
    return EXIT_OK


def command_show(args) -> int:
    print(show_report(_load_report(Path(args.report))))
    return EXIT_OK


def _stage_report_charts(report: dict, source: Path, stage: Path) -> None:
    manifest = bundle_manifest(source) if source.parent.parent.name == "drafts" else None
    if manifest and "preferences-used.json" in manifest["sha256"]:
        captured = (source.parent / "preferences-used.json").read_bytes()
        if hashlib.sha256(captured).hexdigest() != manifest["sha256"]["preferences-used.json"]:
            raise InputError("The retained preferences snapshot changed. Restore the verified draft.")
        (stage / "preferences-used.json").write_bytes(captured)
    for chart in report["charts"]:
        png = chart_path(source, chart["png"])
        if png is None:
            raise InputError(f"Report chart {chart['png']} is missing. Restore it before publishing a new bundle.")
        content = png.read_bytes()
        if manifest and hashlib.sha256(content).hexdigest() != manifest["sha256"].get(chart["png"]):
            raise InputError("A report chart changed while copying. Retry from a verified draft.")
        destination = stage / chart["png"]
        destination.parent.mkdir(exist_ok=True)
        destination.write_bytes(content)


def _source_report(path: Path):
    root = publication_root(path)
    base = _render_base(path)[1]
    with publication_lock(root, base):
        require_current(root, base, path)
        manifest = bundle_manifest(path) if path.parent.parent.name == "drafts" else None
        content = path.read_bytes()
        if manifest and hashlib.sha256(content).hexdigest() != manifest["sha256"][path.name]:
            raise InputError("The report changed while reading. Retry from a verified draft.")
        report = json.loads(content)
        if not isinstance(report, dict) or report.get("version") != 1 or not isinstance(report.get("meta"), dict):
            raise InputError("This is not a version 1 report produced by this skill.")
        expected = (str(path.resolve()), hashlib.sha256(content).hexdigest()) if current_report(root, base) else None
        return report, root, base, expected


def command_prose(args) -> int:
    report_path = Path(args.report)
    summary: list[str] | None = None
    actions: list[str] | None = None
    if args.source:
        data = _load_json_file(args.source, "prose file")
        if "summary" in data:
            summary = [str(item) for item in data["summary"]] if isinstance(data["summary"], list) else [str(data["summary"])]
        if "actions" in data:
            actions = [str(item) for item in data["actions"]]
    if args.summary:
        summary = list(args.summary)
    if args.action:
        actions = list(args.action)
    if summary is None and actions is None:
        raise InputError("Give --from prose.json, --summary text or --action text.")
    targets = parse_targets(args.render, "--render")
    report, root, base, expected = _source_report(report_path)
    updated, removed, notes = apply_prose(report, summary, actions)
    published = publish_bundle(root, base, updated, targets, resolve_tenant_dir(args.tenant), render, lambda stage: _stage_report_charts(updated, report_path, stage), bundle_id=args.bundle_id, expected_current=expected)
    print(f"Draft {updated['meta']['draft']}: {published}")
    if removed:
        print(f"Removed {len(removed)} {plural(len(removed), 'number')} not in the report: {', '.join(removed)}. Sentences with them were dropped; say so to the user.")
    else:
        print("Every number in the written text matches the report.")
    for note in notes:
        print(note)
    # The report as it now stands, read out of the draft that was just written
    # rather than out of the text that was handed in: a sentence the number
    # check dropped is not in here, and the KPI strip, the checks line and the
    # not-included items are what the user has to be told about this draft.
    print()
    print(show_report(updated))
    print_rendered(published, targets)
    print_unowned_renders(report_path)
    return EXIT_OK


def command_render(args) -> int:
    targets = parse_targets(args.to, "--to")
    if not targets:
        raise InputError("--to must name at least one format: pdf, docx, xlsx or html.")
    if args.out and (len(targets) != 1 or args.bundle_id):
        raise InputError("--out names one export; use one --to target without --bundle-id.")
    report_path = Path(args.report)
    tenant_dir = resolve_tenant_dir(args.tenant)
    report, root, base, expected = _source_report(report_path)
    if args.out:
        # A chosen export is one atomically replaced file, outside bundle ownership.
        import tempfile

        destination = Path(args.out)
        resolved = destination.resolve()
        if resolved == report_path.resolve() or resolved.is_relative_to((root / "drafts").resolve()) or any(parent.parent.name == "drafts" and (parent / RENDERS_MANIFEST).exists() for parent in resolved.parents):
            raise InputError("Choose an export path outside the published report bundle.")
        destination.parent.mkdir(parents=True, exist_ok=True)
        private = destination.parent / STATE_DIR
        private.mkdir(parents=True, exist_ok=True, mode=0o700)
        with tempfile.TemporaryDirectory(prefix="export-", dir=private) as temporary:
            stage = Path(temporary)
            _stage_report_charts(report, report_path, stage)
            staged_report = stage / report_path.name
            _write_json(staged_report, report)
            staged = render(report, staged_report, targets[0], stage / destination.name, tenant_dir)
            staged.chmod(0o644)
            with staged.open("rb") as stream:
                os.fsync(stream.fileno())
            with publication_lock(root, base):
                if current_identity(root, base) != expected:
                    raise InputError("The current report changed while the export was prepared. Retry from the current draft.")
                os.replace(staged, destination)
                sync_directory(destination.parent)
        print(f"Rendered {targets[0]}: {destination}")
        return EXIT_OK
    # Only hash-verified bundle members may be carried. Legacy filename-only
    # manifests do not establish which report supplied a format's numbers.
    carry = read_render_manifest(report_path) if report_path.parent.parent.name == "drafts" else []
    carry = [name for name in carry if name not in {_render_name(report_path, target) for target in targets}]
    published = publish_bundle(root, base, report, targets, tenant_dir, render, lambda stage: _stage_report_charts(report, report_path, stage), bundle_id=args.bundle_id, expected_current=expected, source=report_path, carry=carry)
    print(f"Report: {published}")
    print_rendered(published, targets)
    print_unowned_renders(report_path)
    return EXIT_OK


def command_checks(args) -> int:
    report_path = Path(args.report)
    report = _load_report(report_path)
    tenant_dir = resolve_tenant_dir(args.tenant)
    build = report["meta"]["build"]
    try:
        recipe = build.get("recheck")
        profile = recipe["profile"] if recipe else load_profile(report["meta"].get("profile"), tenant_dir)
        options = BuildOptions(exclusions=list(build.get("exclusions", [])), currency=report["meta"]["currency"]["code"] if report["meta"]["currency"]["source"] == "preferences" else None)
        if recipe:
            options.comparisons = recipe["comparisons"]
            options.top_n = recipe["top_n"]
            options.summary_length = recipe["summary_length"]
        ctx = prepare(args.files or build["sources"], report["meta"]["period"]["key"], options, None if recipe else build.get("mapping"), profile, resolved_mappings=recipe["mappings"] if recipe else None)
        if recipe:
            ctx.period_was_given = recipe["period_was_given"]
        rebuilt, _charts = build_report(ctx, int(report["meta"]["draft"]) - 1, compute_checks)

        # Uploaded timestamps may change when the identical export is copied.
        # Hash, sheet and row count identify the input bytes actually checked.
        def provenance(document):
            return [(entry["sha256"], entry.get("sheet"), entry["rows"]) for entry in document["meta"]["inputs"]]

        if provenance(report) != provenance(rebuilt):
            checks = [{"id": "input_provenance", "status": "fail", "text": "The inputs differ from those recorded in this saved draft. Rebuild the report explicitly before using the new data."}]
        elif _saved_figures(report, legacy=not recipe) != _saved_figures(rebuilt, legacy=not recipe):
            checks = [{"id": "saved_figures", "status": "fail", "text": "The saved figures do not match the recorded inputs and build choices. Rebuild the report; this check has not changed it."}]
        else:
            checks = rebuilt["checks"]
            # Recheck actual saved prose against independently computed figures,
            # never numbers admitted by a previous prose-check message.
            generated = {section["id"]: section.get("paragraphs", []) + section.get("bullets", []) for section in rebuilt["sections"]}
            bad = [number for section in report["sections"] for text in section.get("paragraphs", []) + section.get("bullets", []) if text not in generated.get(section["id"], []) for number in verify_prose_numbers(rebuilt, text)[1]]
            if bad or any(check["id"] == "prose_numbers" for check in report.get("checks", [])):
                checks.append(
                    {
                        "id": "prose_numbers",
                        "status": "fail" if bad else "pass",
                        "text": f"Saved text contains numbers absent from the report: {', '.join(dict.fromkeys(bad))}. Correct the text explicitly." if bad else "Every number in the saved text matches the report.",
                    }
                )
    except (InputError, DecisionNeeded, KeyError, TypeError, ValueError, OSError) as error:
        checks = [{"id": "saved_figures", "status": "fail", "text": f"Could not verify this saved draft: {error}. Restore its inputs and build choices or rebuild explicitly."}]
    _write_json(publication_root(report_path) / "checks.json", checks)
    print(checks_line({"checks": checks}))
    return EXIT_WITHHELD if any(check["status"] == "fail" for check in checks) else EXIT_OK


def _saved_figures(report: dict, *, legacy: bool) -> dict:
    """Computed facts, excluding editable prose and presentation preferences."""

    def table(value):
        # Early reports did not retain the effective profile. Re-resolving their
        # explicit column mappings can change headings but not the figures.
        return {key: item for key, item in value.items() if key != "columns" or not legacy} if value is not None else None

    return {
        "period": report["meta"]["period"],
        "currency": report["meta"]["currency"]["code"],
        "kpis": [{key: value for key, value in kpi.items() if key != "label" or not legacy} for kpi in report["kpis"]],
        "tables": [{"id": section["id"], "table": table(section["table"])} for section in report["sections"] if "table" in section],
        "rows": table(report["rows"]),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="report.py", description="Turn a tabular export into a management report (see SKILL.md).")
    commands = parser.add_subparsers(dest="command", required=True)

    inspect_parser = commands.add_parser("inspect", help="Describe the files: columns, suggested roles, period and currency.")
    inspect_parser.add_argument("files", nargs="+", help="CSV, XLSX or XLS files; a workbook sheet as path::Sheet")
    inspect_parser.add_argument("--profile", help="Profile name or path (default: services-generic)")
    inspect_parser.add_argument("--tenant", help="Tenant bundle directory (default: /mnt/tenant when present)")
    inspect_parser.set_defaults(handler=command_inspect)

    build_parser_ = commands.add_parser("build", help="Compute the report: report.json, charts and checks.")
    build_parser_.add_argument("files", nargs="+", help="CSV, XLSX or XLS files; earlier periods in the same files or extra files feed the comparisons")
    build_parser_.add_argument("--period", help="YYYY-MM, YYYY-Qn, YYYY or YYYY-MM-DD..YYYY-MM-DD; omitted, the file's busiest month, named in the checks")
    build_parser_.add_argument("--out", required=True, help="Directory for the report, its charts and checks.json")
    build_parser_.add_argument("--mapping", help="JSON file: {role: column or null} to settle a question from inspect")
    build_parser_.add_argument("--profile", help="Profile name or path (default: services-generic)")
    build_parser_.add_argument("--prefs", help="Validated preferences.json to apply")
    build_parser_.add_argument("--prefs-override", help="Temporary v1 preferences; fields replace saved choices for this build only")
    build_parser_.add_argument("--summary-length", choices=("short", "standard"), help="Override saved summary length for this build only")
    build_parser_.add_argument("--tenant", help="Tenant bundle directory (default: /mnt/tenant when present)")
    build_parser_.add_argument("--exclude", action="append", help="role=value or column=value; repeatable")
    build_parser_.add_argument("--title", help="Report title (default from the profile)")
    build_parser_.add_argument("--company", help="Company name shown on the report")
    build_parser_.add_argument("--currency", help="Three-letter code when the file does not say")
    build_parser_.add_argument("--name", help="Base file name (default: the name of the --out directory)")
    build_parser_.add_argument("--draft", type=int, help="Draft number (default: previous draft in --out plus one)")
    build_parser_.add_argument("--short", action="store_true", help="One-sentence summary")
    build_parser_.add_argument("--render", action="append", help="Render in the same run: pdf,docx,xlsx or all; repeatable")
    build_parser_.add_argument("--bundle-id", help="New immutable draft directory name; choose a unique name to know presented paths in advance")
    build_parser_.set_defaults(handler=command_build)

    show_parser = commands.add_parser("show", help="Print the figures, tables, checks and notes of a report (not its rows).")
    show_parser.add_argument("report", help="The .report.json to show")
    show_parser.set_defaults(handler=command_show)

    prose_parser = commands.add_parser("prose", help="Put model-written summary and actions into the report; numbers are verified.")
    prose_parser.add_argument("report", help="The .report.json to update")
    prose_parser.add_argument("--from", dest="source", help='JSON file: {"summary": [paragraphs], "actions": [bullets]}')
    prose_parser.add_argument("--summary", action="append", help="A summary paragraph; repeatable")
    prose_parser.add_argument("--action", action="append", help="An action bullet; repeatable")
    prose_parser.add_argument("--render", action="append", help="Render the new draft in the same run: pdf,docx,xlsx or all; repeatable")
    prose_parser.add_argument("--tenant", help="Tenant bundle directory for the logo and colours (default: /mnt/tenant when present)")
    prose_parser.add_argument("--bundle-id", help="New immutable draft directory name; choose a unique name to know presented paths in advance")
    prose_parser.set_defaults(handler=command_prose)

    render_parser = commands.add_parser("render", help="Render report.json to html, pdf, docx or xlsx; several formats in one run.")
    render_parser.add_argument("report", help="The .report.json to render")
    render_parser.add_argument("--to", required=True, action="append", help="html, pdf, docx, xlsx, a comma-separated list of them, or all (pdf, docx and xlsx); repeatable")
    render_parser.add_argument("--out", help="Output path (default: next to the report)")
    render_parser.add_argument("--tenant", help="Tenant bundle directory for the logo and colours (default: /mnt/tenant when present)")
    render_parser.add_argument("--bundle-id", help="New immutable draft directory name; choose a unique name to know presented paths in advance")
    render_parser.set_defaults(handler=command_render)

    checks_parser = commands.add_parser("checks", help="Re-run the checks against the inputs and write checks.json.")
    checks_parser.add_argument("report", help="The .report.json whose checks to re-run")
    checks_parser.add_argument("files", nargs="*", help="The input files (default: the ones recorded in the report)")
    checks_parser.add_argument("--tenant", help="Tenant bundle directory (default: /mnt/tenant when present)")
    checks_parser.set_defaults(handler=command_checks)
    preferences_parser = commands.add_parser("preferences", help="Validate/read/save/patch/reset consumer-owned preferences")
    preferences_parser.add_argument("arguments", nargs=argparse.REMAINDER)
    preferences_parser.set_defaults(handler=lambda args: settings.main(args.arguments))
    return parser


# argparse's own status for a command line it refuses. It shares 2 with
# EXIT_MISSING_LIBRARY; stderr tells the two apart, and SKILL.md gives each
# its own action.
EXIT_USAGE = 2

PRESENT_REFUSAL = (
    "report.py: error: --present is not an option of this script, and nothing was run. "
    "Make the call again with both arguments: this command line without --present and the paths after it as `command`, "
    "and the files it writes (the report, then each render) in the bash tool's `present` argument.\n"
)


def _misplaced_present(argv: list[str]) -> bool:
    """Whether the command line carries ``present``, which belongs to the bash tool.

    Checked before parsing, because argparse's own refusal says only that the
    option is unknown -- and before the subcommand it does not even say that,
    since the next word is taken as the subcommand. Words after ``--`` are
    file names.
    """
    for arg in argv:
        if arg == "--":
            return False
        if arg == "--present" or arg.startswith("--present="):
            return True
    return False


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if _misplaced_present(argv):
        sys.stderr.write(PRESENT_REFUSAL)
        return EXIT_USAGE
    args = build_parser().parse_args(argv)
    try:
        return args.handler(args)
    except DecisionNeeded as decision:
        sys.stderr.write(f"Decision needed: {decision.question}\n{json.dumps(decision.details, indent=2, ensure_ascii=False)}\n")
        return EXIT_DECISION_NEEDED
    except (InputError, settings.SettingsError) as error:
        sys.stderr.write(f"Error: {error}\n")
        return EXIT_WITHHELD


if __name__ == "__main__":
    sys.exit(main())
